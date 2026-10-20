//! Host-owned YouTube adapters for the public Orchestrator seam.
//! Shared between the runnable example and the deterministic integration tests.
//! Requires yt-dlp on PATH (or YTDLP_BIN). No shell is invoked.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use orchestrator_agent_rs::{
    ArtifactInput, ArtifactRole, HostTool, ToolContext, ToolDescriptor, ToolEffect, ToolError,
    ToolOutcome,
};
use serde_json::{json, Value};
use tokio::process::Command;

const MAX_RAW_VTT_BYTES: usize = 1024 * 1024;
const MAX_TRANSCRIPT_BYTES: usize = 128 * 1024;
const MAX_MARKDOWN_BYTES: usize = 256 * 1024;

fn error(code: &str, message: &str) -> ToolError {
    ToolError {
        code: code.to_string(),
        message: message.to_string(),
    }
}

/// Keep yt-dlp on an allowlisted video URL, never on an arbitrary URL or playlist.
/// Only the 11-character video ID is forwarded to the external process.
fn canonical_video_url(input: &str) -> Result<String, ToolError> {
    let input = input.trim();
    let video_id = if let Some(rest) = input
        .strip_prefix("https://www.youtube.com/watch?v=")
        .or_else(|| input.strip_prefix("https://youtube.com/watch?v="))
        .or_else(|| input.strip_prefix("https://m.youtube.com/watch?v="))
        .or_else(|| input.strip_prefix("https://youtu.be/"))
    {
        rest.split(['&', '?', '#', '/']).next().unwrap_or_default()
    } else {
        return Err(error(
            "INVALID_URL",
            "expected a YouTube watch or youtu.be video URL",
        ));
    };

    if video_id.len() != 11
        || !video_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(error(
            "INVALID_URL",
            "YouTube video ID must be 11 URL-safe characters",
        ));
    }

    Ok(format!("https://www.youtube.com/watch?v={video_id}"))
}

/// Parse YouTube/WebVTT captions into a plain transcript. Remove cue timestamps,
/// simple inline markup and duplicated consecutive rolling captions.
fn parse_vtt(source: &str) -> String {
    let mut lines = Vec::<String>::new();
    for raw in source.lines() {
        let line = raw.trim();
        if line.is_empty()
            || line == "WEBVTT"
            || line.starts_with("Kind:")
            || line.starts_with("Language:")
            || line.starts_with("NOTE")
            || line.starts_with("STYLE")
            || line.contains("-->")
            || line.chars().all(|c| c.is_ascii_digit())
        {
            continue;
        }

        let mut plain = String::new();
        let mut in_tag = false;
        for ch in line.chars() {
            match ch {
                '<' => in_tag = true,
                '>' if in_tag => in_tag = false,
                _ if !in_tag => plain.push(ch),
                _ => {}
            }
        }
        let plain = plain
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&#39;", "'")
            .replace("&nbsp;", " ");
        let plain = plain.trim();
        if !plain.is_empty() && lines.last().is_none_or(|last| last != plain) {
            lines.push(plain.to_string());
        }
    }
    lines.join("\n")
}

#[derive(Debug, Clone)]
pub struct FetchYoutubeTranscript {
    executable: PathBuf,
}

impl FetchYoutubeTranscript {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
        }
    }
}

#[async_trait::async_trait]
impl HostTool for FetchYoutubeTranscript {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "fetch_youtube_transcript".into(),
            description: "Get the available YouTube subtitles with yt-dlp and return the complete plain transcript. Use the user-provided video URL.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {"url": {"type": "string"}},
                "required": ["url"]
            }),
            effect: ToolEffect::Mutating, // publishes an intermediate artifact
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        let url = canonical_video_url(
            args.get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| error("INVALID_INPUT", "url must be a string"))?,
        )?;
        if ctx.is_cancelled() {
            return Err(error("CANCELLED", "transcript request was cancelled"));
        }

        // yt-dlp writes only captions into a private temporary directory.
        let temp = tempfile::tempdir()
            .map_err(|_| error("YTDLP_IO", "could not create a temporary directory"))?;
        ctx.progress("yt-dlp: downloading available captions");
        let status = Command::new(&self.executable)
            .arg("--ignore-config")
            .arg("--no-playlist")
            .arg("--skip-download")
            .arg("--write-subs")
            .arg("--write-auto-subs")
            .arg("--sub-langs")
            .arg("vi,en,en-US")
            .arg("--sub-format")
            .arg("vtt")
            .arg("--no-warnings")
            .arg("--quiet")
            .arg("-P")
            .arg(format!("home:{}", temp.path().display()))
            .arg("-o")
            .arg("%(id)s.%(ext)s")
            .arg("--")
            .arg(&url)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .status()
            .await
            .map_err(|_| error("YTDLP_UNAVAILABLE", "cannot execute yt-dlp"))?;

        if !status.success() {
            return Err(error("YTDLP_FAILED", "yt-dlp could not retrieve subtitles"));
        }
        if ctx.is_cancelled() {
            return Err(error("CANCELLED", "transcript request was cancelled"));
        }

        let (language, path) = pick_caption(temp.path()).await?;
        let raw = tokio::fs::read(&path)
            .await
            .map_err(|_| error("YTDLP_IO", "cannot read subtitle file"))?;
        if raw.len() > MAX_RAW_VTT_BYTES {
            return Err(error("TRANSCRIPT_TOO_LARGE", "subtitle file exceeds 1 MiB"));
        }
        let source = std::str::from_utf8(&raw)
            .map_err(|_| error("INVALID_CAPTIONS", "subtitle file is not UTF-8"))?;
        let transcript = parse_vtt(source);
        if transcript.is_empty() {
            return Err(error(
                "NO_TRANSCRIPT",
                "the subtitle file has no spoken text",
            ));
        }
        if transcript.len() > MAX_TRANSCRIPT_BYTES {
            return Err(error("TRANSCRIPT_TOO_LARGE", "transcript exceeds 128 KiB"));
        }

        ctx.progress("Publishing verified transcript to ArtifactStore");
        let artifact = ctx
            .artifacts()
            .put(ArtifactInput::from_bytes(
                "youtube-transcript.txt",
                "text/plain",
                ArtifactRole::Intermediate,
                transcript.as_bytes().to_vec(),
            ))
            .await?;

        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: json!({
                "source_url": url,
                "language": language,
                "transcript": transcript,
                "transcript_artifact_id": artifact.id
            })
            .to_string(),
            metadata: json!({"artifact_ids": [artifact.id]}),
        })
    }
}

async fn pick_caption(dir: &Path) -> Result<(String, PathBuf), ToolError> {
    let mut entries = tokio::fs::read_dir(dir)
        .await
        .map_err(|_| error("YTDLP_IO", "cannot inspect subtitle directory"))?;
    let mut captions = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|_| error("YTDLP_IO", "cannot inspect subtitle file"))?
    {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".vtt")
            || !entry
                .file_type()
                .await
                .map(|ft| ft.is_file())
                .unwrap_or(false)
        {
            continue;
        }
        let (priority, language) = if name.ends_with(".vi.vtt") {
            (0, "vi")
        } else if name.ends_with(".en.vtt") {
            (1, "en")
        } else if name.ends_with(".en-US.vtt") {
            (2, "en-US")
        } else {
            (3, "unknown")
        };
        captions.push((priority, path, language.to_string()));
    }
    captions.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    captions
        .into_iter()
        .next()
        .map(|(_, path, lang)| (lang, path))
        .ok_or_else(|| error("NO_TRANSCRIPT", "video has no available vi/en subtitles"))
}

pub struct PublishYoutubeMarkdown;

#[async_trait::async_trait]
impl HostTool for PublishYoutubeMarkdown {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "publish_youtube_markdown".into(),
            description: "Publish the completed video summary as Markdown. Requires the transcript_artifact_id returned by fetch_youtube_transcript and the full markdown report.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "markdown": {"type": "string"},
                    "transcript_artifact_id": {"type": "string"}
                },
                "required": ["markdown", "transcript_artifact_id"]
            }),
            effect: ToolEffect::Mutating,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        let markdown = args
            .get("markdown")
            .and_then(Value::as_str)
            .ok_or_else(|| error("INVALID_INPUT", "markdown must be a string"))?;
        if markdown.trim().is_empty() || markdown.len() > MAX_MARKDOWN_BYTES {
            return Err(error("INVALID_MARKDOWN", "markdown must be 1..=256 KiB"));
        }
        if ctx.is_cancelled() {
            return Err(error("CANCELLED", "Markdown publication was cancelled"));
        }
        // Verify provenance through the library's scoped artifact capability.
        // An arbitrary ID from the model cannot authorize another session's data.
        let transcript_id = args
            .get("transcript_artifact_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| error("INVALID_INPUT", "transcript_artifact_id is required"))?;
        let reader = ctx.artifacts().open(transcript_id).await?;
        if reader.metadata().name != "youtube-transcript.txt" {
            return Err(error(
                "INVALID_INPUT",
                "expected a YouTube transcript artifact",
            ));
        }
        if reader.read_to_string().await?.trim().is_empty() {
            return Err(error("NO_TRANSCRIPT", "transcript artifact is empty"));
        }
        let artifact = ctx
            .artifacts()
            .put(ArtifactInput::primary_bytes(
                "youtube-summary.md",
                "text/markdown",
                markdown.as_bytes().to_vec(),
            ))
            .await?;

        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: json!({"artifact_id": artifact.id, "name": artifact.name}).to_string(),
            metadata: json!({"artifact_ids": [artifact.id]}),
        })
    }
}
