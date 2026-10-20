//! Flow hoàn chỉnh: URL video → transcript (yt-dlp) → tóm tắt (LLM) → file Markdown.
//! Chạy: cargo run --example video_summary -- <URL> [--lang vi,en] [--env-file .env.live]
//!                                              [--output-dir target/video-summary]
//! Yêu cầu: `yt-dlp` trong PATH (brew install yt-dlp ffmpeg) và cấu hình G0_OPENAI_* như live_flow.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, CallerContext, FeedItem, HostTool, LlmBackend, LlmConfig,
    LlmProviderConfig, Orchestrator, OrchestratorConfig, ProviderAuth, RunStatus, SecretString,
    ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Giới hạn ký tự transcript trả về cho model để không tràn context.
const MAX_TRANSCRIPT_CHARS: usize = 24_000;

fn tool_err(code: &str, message: impl Into<String>) -> ToolError {
    ToolError {
        code: code.into(),
        message: message.into(),
    }
}

fn outcome(ctx: &ToolContext, content: String, metadata: Value) -> ToolOutcome {
    ToolOutcome {
        call_id: ctx.scope().tool_call_id.clone(),
        content,
        metadata,
    }
}

fn str_arg<'a>(args: &'a Value, key: &str) -> std::result::Result<&'a str, ToolError> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| tool_err("INVALID_ARGUMENT", format!("Thiếu hoặc rỗng: {key}")))
}

// ---------------------------------------------------------------------------
// Tool: get_transcript (yt-dlp)
// ---------------------------------------------------------------------------

struct GetTranscript {
    work_dir: PathBuf,
    langs: String,
}

/// Chuyển WebVTT thành văn bản thuần: bỏ header, timestamp, tag và dòng lặp (auto-sub).
fn vtt_to_text(vtt: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    for raw in vtt.lines() {
        let line = raw.trim();
        if line.is_empty()
            || line == "WEBVTT"
            || line.contains("-->")
            || line.starts_with("Kind:")
            || line.starts_with("Language:")
            || line.starts_with("NOTE")
            || line.chars().all(|c| c.is_ascii_digit())
        {
            continue;
        }
        let mut text = String::new();
        let mut in_tag = false;
        for c in line.chars() {
            match c {
                '<' => in_tag = true,
                '>' => in_tag = false,
                _ if !in_tag => text.push(c),
                _ => {}
            }
        }
        let text = text
            .replace("&amp;", "&")
            .replace("&gt;", ">")
            .replace("&lt;", "<")
            .replace("&#39;", "'")
            .replace("&quot;", "\"")
            .trim()
            .to_string();
        if !text.is_empty() && out.last() != Some(&text) {
            out.push(text);
        }
    }
    out.join(" ")
}

/// Chạy yt-dlp (blocking) và trả về (tiêu đề, transcript). Không qua shell; URL đứng sau `--`.
fn fetch_transcript(
    work_dir: &Path,
    url: &str,
    langs: &str,
) -> std::result::Result<(String, String), ToolError> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(tool_err("INVALID_URL", "Chỉ hỗ trợ URL http(s)"));
    }
    // Phụ đề auto-dịch (`en`) hay bị YouTube trả 429; bản gốc `<lang>-orig` ổn định hơn.
    let langs: String = langs
        .split(',')
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .flat_map(|l| [format!("{l}-orig"), l.to_string()])
        .collect::<Vec<_>>()
        .join(",");
    let langs = langs.as_str();
    let dir = work_dir.join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&dir).map_err(|e| tool_err("IO", e.to_string()))?;
    let output = std::process::Command::new("yt-dlp")
        .args([
            "--skip-download",
            "--no-playlist",
            "--write-subs",
            "--write-auto-subs",
            "--sub-langs",
            langs,
            "--sub-format",
            "vtt",
            // `--print` ngầm bật simulate (không ghi phụ đề) nên cần --no-simulate.
            "--no-simulate",
            "--print",
            "title",
            "-o",
        ])
        .arg(dir.join("%(id)s.%(ext)s"))
        .arg("--")
        .arg(url)
        .output()
        .map_err(|e| {
            tool_err(
                "YTDLP_UNAVAILABLE",
                format!("Không chạy được yt-dlp (brew install yt-dlp): {e}"),
            )
        })?;
    let has_sub = std::fs::read_dir(&dir)
        .map(|d| {
            d.filter_map(|e| e.ok())
                .any(|e| e.path().extension().is_some_and(|x| x == "vtt"))
        })
        .unwrap_or(false);
    // yt-dlp thoát lỗi khi một biến thể phụ đề bị 429; vẫn dùng nếu đã có file .vtt.
    if !output.status.success() && !has_sub {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let tail: String = stderr
            .chars()
            .rev()
            .take(600)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        return Err(tool_err("YTDLP_FAILED", tail));
    }
    let title = String::from_utf8_lossy(&output.stdout)
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    let mut subs: Vec<PathBuf> = std::fs::read_dir(&dir)
        .map_err(|e| tool_err("IO", e.to_string()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "vtt"))
        .collect();
    // Ưu tiên theo thứ tự ngôn ngữ người dùng yêu cầu, rồi tên file cho ổn định.
    let order: Vec<&str> = langs
        .split(',')
        .map(|l| l.trim().trim_end_matches(".*"))
        .collect();
    subs.sort_by_key(|p| {
        let name = p
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        let rank = order
            .iter()
            .position(|l| name.contains(&format!(".{l}")))
            .unwrap_or(usize::MAX);
        (rank, name)
    });
    let sub = subs.first().ok_or_else(|| {
        tool_err(
            "NO_TRANSCRIPT",
            format!("Video không có phụ đề cho: {langs}"),
        )
    })?;
    let text =
        vtt_to_text(&std::fs::read_to_string(sub).map_err(|e| tool_err("IO", e.to_string()))?);
    if text.is_empty() {
        return Err(tool_err("NO_TRANSCRIPT", "Phụ đề rỗng"));
    }
    Ok((title, text))
}

#[async_trait::async_trait]
impl HostTool for GetTranscript {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "get_transcript".into(),
            description: "Download the transcript (subtitles) of a video URL using yt-dlp. Returns the video title and plain-text transcript.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {"url": {"type": "string", "description": "http(s) video URL"}},
                "required": ["url"],
                "additionalProperties": false
            }),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(
        &self,
        ctx: ToolContext,
        args: Value,
    ) -> std::result::Result<ToolOutcome, ToolError> {
        let url = str_arg(&args, "url")?.to_string();
        ctx.progress("Đang tải transcript bằng yt-dlp...");
        let (work_dir, langs, u) = (self.work_dir.clone(), self.langs.clone(), url.clone());
        let (title, text) =
            tokio::task::spawn_blocking(move || fetch_transcript(&work_dir, &u, &langs))
                .await
                .map_err(|e| tool_err("INTERNAL", e.to_string()))??;
        let total = text.chars().count();
        let truncated = total > MAX_TRANSCRIPT_CHARS;
        let body: String = text.chars().take(MAX_TRANSCRIPT_CHARS).collect();
        ctx.progress(&format!("Đã lấy transcript: {total} ký tự"));
        Ok(outcome(
            &ctx,
            format!("Title: {title}\nTruncated: {truncated}\n\nTranscript:\n{body}"),
            json!({"url": url, "title": title, "chars": total, "truncated": truncated}),
        ))
    }
}

// ---------------------------------------------------------------------------
// Tools Markdown: markdown_create / markdown_append_section / markdown_read
// ---------------------------------------------------------------------------

struct MarkdownTools {
    dir: PathBuf,
    kind: MdKind,
}

#[derive(Clone, Copy)]
enum MdKind {
    Create,
    Append,
    Read,
}

/// Chỉ nhận tên file đơn giản (không có `/`, `..`) và ép đuôi `.md`; file luôn nằm trong `dir`.
fn md_path(dir: &Path, filename: &str) -> std::result::Result<PathBuf, ToolError> {
    let name = filename.trim();
    let valid = !name.is_empty()
        && name.len() <= 100
        && !name.starts_with('.')
        && name
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if !valid {
        return Err(tool_err(
            "INVALID_FILENAME",
            "filename chỉ gồm chữ, số, '-', '_', '.' (không có đường dẫn)",
        ));
    }
    let name = if name.ends_with(".md") {
        name.to_string()
    } else {
        format!("{name}.md")
    };
    Ok(dir.join(name))
}

fn render_document(args: &Value) -> std::result::Result<String, ToolError> {
    let title = str_arg(args, "title")?;
    let summary = str_arg(args, "summary")?;
    let mut doc = format!("# {title}\n\n");
    if let Some(url) = args.get("source_url").and_then(Value::as_str) {
        doc.push_str(&format!("> Nguồn: <{url}>\n\n"));
    }
    doc.push_str(&format!("## Tóm tắt\n\n{summary}\n"));
    let points: Vec<&str> = args
        .get("key_points")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    if !points.is_empty() {
        doc.push_str("\n## Điểm chính\n\n");
        for p in points {
            doc.push_str(&format!("- {p}\n"));
        }
    }
    Ok(doc)
}

#[async_trait::async_trait]
impl HostTool for MarkdownTools {
    fn descriptor(&self) -> ToolDescriptor {
        let filename = json!({"type": "string", "description": "File name, e.g. summary.md"});
        match self.kind {
            MdKind::Create => ToolDescriptor {
                name: "markdown_create".into(),
                description: "Create a new Markdown file (title, source link, summary, bullet key points). Fails if the file exists.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "filename": filename,
                        "title": {"type": "string"},
                        "source_url": {"type": "string"},
                        "summary": {"type": "string", "description": "Paragraph summary in Markdown"},
                        "key_points": {"type": "array", "items": {"type": "string"}}
                    },
                    "required": ["filename", "title", "summary"],
                    "additionalProperties": false
                }),
                effect: ToolEffect::Mutating,
            },
            MdKind::Append => ToolDescriptor {
                name: "markdown_append_section".into(),
                description: "Append a `## heading` section with body text to an existing Markdown file.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "filename": filename,
                        "heading": {"type": "string"},
                        "body": {"type": "string"}
                    },
                    "required": ["filename", "heading", "body"],
                    "additionalProperties": false
                }),
                effect: ToolEffect::Mutating,
            },
            MdKind::Read => ToolDescriptor {
                name: "markdown_read".into(),
                description: "Read back a Markdown file previously created.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"filename": filename},
                    "required": ["filename"],
                    "additionalProperties": false
                }),
                effect: ToolEffect::ReadOnly,
            },
        }
    }

    async fn execute(
        &self,
        ctx: ToolContext,
        args: Value,
    ) -> std::result::Result<ToolOutcome, ToolError> {
        let path = md_path(&self.dir, str_arg(&args, "filename")?)?;
        let io_err = |e: io::Error| tool_err("IO", e.to_string());
        let content = match self.kind {
            MdKind::Create => {
                let doc = render_document(&args)?;
                tokio::fs::create_dir_all(&self.dir).await.map_err(io_err)?;
                // create_new: không ghi đè file có sẵn.
                let mut f = tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .await
                    .map_err(|e| match e.kind() {
                        io::ErrorKind::AlreadyExists => tool_err(
                            "ALREADY_EXISTS",
                            "File đã tồn tại; dùng markdown_append_section",
                        ),
                        _ => io_err(e),
                    })?;
                tokio::io::AsyncWriteExt::write_all(&mut f, doc.as_bytes())
                    .await
                    .map_err(io_err)?;
                format!("Created {} ({} bytes)", path.display(), doc.len())
            }
            MdKind::Append => {
                let section = format!(
                    "\n## {}\n\n{}\n",
                    str_arg(&args, "heading")?,
                    str_arg(&args, "body")?
                );
                let mut f = tokio::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .await
                    .map_err(|e| match e.kind() {
                        io::ErrorKind::NotFound => tool_err("NOT_FOUND", "File chưa tồn tại"),
                        _ => io_err(e),
                    })?;
                tokio::io::AsyncWriteExt::write_all(&mut f, section.as_bytes())
                    .await
                    .map_err(io_err)?;
                format!("Appended section to {}", path.display())
            }
            MdKind::Read => tokio::fs::read_to_string(&path)
                .await
                .map_err(|e| match e.kind() {
                    io::ErrorKind::NotFound => tool_err("NOT_FOUND", "File chưa tồn tại"),
                    _ => io_err(e),
                })?,
        };
        Ok(outcome(&ctx, content, json!({"path": path})))
    }
}

// ---------------------------------------------------------------------------
// Host: cấu hình, log, chạy một turn
// ---------------------------------------------------------------------------

fn read_config(path: &Path) -> Result<HashMap<String, String>> {
    let text = std::fs::read_to_string(path)?;
    Ok(text
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            let value = value.trim();
            let value = value
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
                .unwrap_or(value);
            Some((key.trim().into(), value.into()))
        })
        .collect())
}

struct Log {
    file: std::fs::File,
    start: Instant,
    secret: String,
    /// true: stream dễ đọc ra terminal; false (`--json`): in nguyên dòng JSONL.
    pretty: bool,
    /// Tag của nguồn text.delta gần nhất (để biết khi nào cần in lại tag).
    last_text_tag: Option<String>,
}

impl Log {
    /// Hiển thị event dạng stream. Mỗi event gắn tag theo `session_id` (`root` hoặc
    /// `sub:<8 ký tự đầu>`); khi nguồn đổi giữa các delta, xuống dòng và in tag mới.
    fn pretty_print(&mut self, kind: &str, data: &Value) {
        let mut out = io::stdout().lock();
        if kind != "event" {
            let _ = match kind {
                "result" => writeln!(out, "\n[kết quả] {}", data["status"].as_str().unwrap_or("")),
                "failed" => writeln!(out, "\n[lỗi] {}", data["message"].as_str().unwrap_or("")),
                "verified" => writeln!(out, "[xác minh] đã tạo {}", data["markdown"]),
                _ => Ok(()),
            };
            return;
        }
        let payload = &data["payload"];
        let session = data["session_id"].as_str().unwrap_or("");
        let tag = if session == data["root_session_id"].as_str().unwrap_or("") {
            "root".to_string()
        } else {
            format!("sub:{}", session.chars().take(8).collect::<String>())
        };
        let tool = payload["tool_name"].as_str().unwrap_or("?");
        let event_type = data["event_type"].as_str().unwrap_or("");
        if event_type == "text.delta" {
            if self.last_text_tag.as_deref() != Some(tag.as_str()) {
                let _ = write!(out, "\n[{tag}] ");
                self.last_text_tag = Some(tag);
            }
            let _ = write!(out, "{}", payload["delta"].as_str().unwrap_or(""));
            let _ = out.flush();
            return;
        }
        let line = match event_type {
            "tool.started" => format!("[{tag}] [tool ▶] {tool}"),
            "tool.progress" => format!(
                "[{tag}] [tool …] {}",
                payload["message"].as_str().unwrap_or("")
            ),
            "tool.completed" => format!("[{tag}] [tool ✔] {tool}"),
            "tool.failed" => format!(
                "[{tag}] [tool ✘] {tool} ({})",
                payload["error_code"].as_str().unwrap_or("")
            ),
            _ => return,
        };
        // Dòng tool ngắt mạch text đang stream; delta kế tiếp sẽ in lại tag.
        self.last_text_tag = None;
        let _ = writeln!(out, "\n{line}");
    }

    fn write(&mut self, kind: &str, data: Value) -> io::Result<()> {
        let line =
            json!({"elapsed_ms": self.start.elapsed().as_millis(), "kind": kind, "data": data})
                .to_string()
                .replace(&self.secret, "[redacted]");
        writeln!(self.file, "{line}")?;
        self.file.flush()?;
        if self.pretty {
            self.pretty_print(kind, &data);
        } else {
            println!("{line}");
        }
        Ok(())
    }
}

/// Root chỉ điều phối: không có tool nghiệp vụ, chỉ gọi subagent.
const COORDINATOR_PROMPT: &str = "You only coordinate; never summarize or write files yourself. \
Call the subagent tool exactly twice, in order. Each call has ONLY these arguments: agent, \
description (a short label, REQUIRED) and prompt (a single string holding everything the \
subagent needs); no other arguments, in particular no session_id. \
Call 1: agent=explorer, prompt must contain the video URL and the target language. \
Call 2: agent=writer, prompt must be exactly: 'filename: <FILENAME>', 'source_url: <URL>', then the \
explorer's title, summary and key points verbatim. <FILENAME> and <URL> are the values from the user \
request, copied unchanged (never invent or rename the file). \
Then reply briefly with the file name the writer reports. \
If the explorer reports an error, do not call writer: explain the error and stop.";

/// Explorer: khám phá video (transcript) và tóm tắt, không ghi file.
const EXPLORER_PROMPT: &str =
    "You explore a video. Call get_transcript with the given URL, then reply \
with: 'Title: <video title>', a faithful summary (3-6 sentences) in the requested language, and \
3-7 key points as '- ' bullets, using only the transcript. If get_transcript fails, reply \
'ERROR: <reason>' and do not invent content.";

/// Writer: ghi và xác minh file Markdown từ nội dung được giao.
const WRITER_PROMPT: &str = "You write Markdown files. Call markdown_create ONCE with the given \
filename from the 'filename:' line (use it EXACTLY, never choose another name), title, source_url, summary and key_points exactly as provided, then call markdown_read to \
verify, then reply briefly with the file name.";

async fn flow(
    o: &Orchestrator,
    log: &mut Log,
    url: &str,
    md_dir: &Path,
    filename: &str,
) -> Result<()> {
    let caller = CallerContext::new("demo", "video-summary");
    let sid = o.create_session(&caller, "coordinator").await?;
    let mut sub = o.subscribe(&caller, &sid).await?;
    let prompt = format!(
        "Summarize this video in Vietnamese and save it as Markdown.\nURL: {url}\nfilename: {filename}"
    );
    log.write("submit", json!({"session_id": sid, "prompt": prompt}))?;
    let turn = o.submit(&caller, &sid, prompt).await?;
    let deadline = tokio::time::sleep(Duration::from_secs(600));
    tokio::pin!(deadline);
    let mut refresh = tokio::time::interval(o.snapshot_refresh_interval());
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            item = sub.recv() => match item {
                Some(FeedItem::Event(event)) => log.write("event", event.to_json())?,
                Some(FeedItem::ResyncRequired) | None => {
                    log.write("resync", json!({"reason": "feed lagged or closed"}))?;
                    sub = o.subscribe(&caller, &sid).await?;
                }
            },
            _ = refresh.tick() => {
                let result = o.result(&caller, &sid, &turn.turn_id).await?;
                if result.status == RunStatus::Running { continue; }
                log.write("result", json!({
                    "status": result.status.as_str(), "output": result.output,
                    "error_code": result.error.as_ref().map(|e| &e.code),
                }))?;
                if result.status != RunStatus::Completed {
                    return Err(io::Error::other("Turn không completed; xem result trong log").into());
                }
                break;
            },
            _ = &mut deadline => {
                o.cancel(&caller, &sid).await?;
                return Err(io::Error::new(io::ErrorKind::TimedOut, "Turn quá 600 giây; đã cancel").into());
            }
        }
    }
    // Xác minh kết quả thật trên đĩa, không tin lời model.
    let path = md_path(md_dir, filename).map_err(|e| io::Error::other(e.message))?;
    let doc = std::fs::read_to_string(&path).map_err(|e| {
        io::Error::other(format!("Không thấy file Markdown {}: {e}", path.display()))
    })?;
    if !doc.starts_with("# ") || !doc.contains("## Tóm tắt") {
        return Err(io::Error::other("File Markdown thiếu tiêu đề hoặc mục tóm tắt").into());
    }
    log.write("verified", json!({"markdown": path, "bytes": doc.len()}))?;
    println!("\n===== {} =====\n{doc}", path.display());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut url = None;
    let mut langs = "vi,en".to_string();
    let mut pretty = true;
    let mut env_file = std::env::var_os("LIVE_ENV_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".env.live"));
    let mut output = root.join("target/video-summary");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--lang" | "--env-file" | "--output-dir" => {
                let v = args
                    .next()
                    .ok_or_else(|| io::Error::other("Thiếu giá trị option"))?;
                match arg.as_str() {
                    "--lang" => langs = v,
                    "--env-file" => env_file = v.into(),
                    _ => output = v.into(),
                }
            }
            "--json" => pretty = false,
            "--help" | "-h" => {
                println!("cargo run --example video_summary -- <URL> [--lang vi,en] [--env-file .env.live] [--output-dir target/video-summary] [--json]");
                return Ok(());
            }
            s if !s.starts_with('-') && url.is_none() => url = Some(s.to_string()),
            _ => return Err(io::Error::other("Option không hợp lệ; dùng --help").into()),
        }
    }
    let url = url.ok_or_else(|| io::Error::other("Thiếu URL; dùng --help"))?;
    let config = read_config(&env_file)?;
    let required = |key: &str| -> Result<String> {
        config
            .get(key)
            .filter(|s| !s.trim().is_empty())
            .cloned()
            .ok_or_else(|| io::Error::other(format!("Thiếu {key} trong file cấu hình")).into())
    };
    let (endpoint, model, key) = (
        required("G0_OPENAI_BASE_URL")?,
        required("G0_OPENAI_MODEL")?,
        required("G0_OPENAI_API_KEY")?,
    );
    let run_dir = output.join(uuid::Uuid::new_v4().to_string());
    let md_dir = run_dir.join("markdown");
    std::fs::create_dir_all(&md_dir)?;
    let mut log = Log {
        file: std::fs::File::create(run_dir.join("events.jsonl"))?,
        start: Instant::now(),
        secret: key.clone(),
        pretty,
        last_text_tag: None,
    };
    log.write(
        "startup",
        json!({"run_dir": run_dir, "model": model, "url": url, "langs": langs}),
    )?;
    let prompt_file = |name: &str, text: &str| -> Result<PathBuf> {
        let path = run_dir.join(format!("{name}.md"));
        std::fs::write(&path, text)?;
        Ok(path)
    };
    let coordinator_prompt = prompt_file("coordinator", COORDINATOR_PROMPT)?;
    let explorer_prompt = prompt_file("explorer", EXPLORER_PROMPT)?;
    let writer_prompt = prompt_file("writer", WRITER_PROMPT)?;

    let cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "live".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(endpoint),
                auth: ProviderAuth::ApiKey(SecretString::new(key)),
                request_timeout: Duration::from_secs(120),
            }],
        },
        vec![
            AgentDefinition::new("coordinator", "live", &model)
                .with_mode(AgentMode::Primary)
                .with_prompt_path(coordinator_prompt)
                .with_allowed_tools(["subagent"]),
            AgentDefinition::new("explorer", "live", &model)
                .with_mode(AgentMode::Subagent)
                .with_description("Explores a video URL: fetches the transcript and summarizes it")
                .with_prompt_path(explorer_prompt)
                .with_allowed_tools(["get_transcript"]),
            AgentDefinition::new("writer", "live", &model)
                .with_mode(AgentMode::Subagent)
                .with_description("Writes and verifies a Markdown file from a given summary")
                .with_prompt_path(writer_prompt)
                .with_allowed_tools(["markdown_create", "markdown_read"]),
        ],
        run_dir.join("store"),
    );
    let md = |kind| {
        Arc::new(MarkdownTools {
            dir: md_dir.clone(),
            kind,
        })
    };
    let o = Orchestrator::builder(cfg)
        .register_tool(Arc::new(GetTranscript {
            work_dir: run_dir.join("subs"),
            langs,
        }))
        .register_tool(md(MdKind::Create))
        .register_tool(md(MdKind::Append))
        .register_tool(md(MdKind::Read))
        .build()
        .await?;
    let result = flow(&o, &mut log, &url, &md_dir, "summary.md").await;
    let shutdown = o.shutdown().await;
    if let Err(error) = &result {
        log.write("failed", json!({"message": error.to_string()}))?;
    }
    log.write("shutdown", json!({"success": shutdown.is_ok()}))?;
    shutdown?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vtt_is_flattened_and_deduplicated() {
        let vtt = "WEBVTT\nKind: captions\nLanguage: en\n\n1\n00:00:00.000 --> 00:00:02.000\nHello <c>world</c>\n\n00:00:02.000 --> 00:00:03.000\nHello world\nsecond &amp; line\n";
        assert_eq!(vtt_to_text(vtt), "Hello world second & line");
    }

    #[test]
    fn markdown_filename_cannot_escape_dir() {
        let dir = Path::new("/tmp/x");
        assert!(md_path(dir, "../evil").is_err());
        assert!(md_path(dir, "a/b").is_err());
        assert!(md_path(dir, ".hidden").is_err());
        assert_eq!(md_path(dir, "ok").unwrap(), dir.join("ok.md"));
    }

    #[test]
    fn document_renders_sections() {
        let doc = render_document(&json!({
            "title": "T", "summary": "S", "source_url": "https://x", "key_points": ["a", "b"]
        }))
        .unwrap();
        assert!(doc.starts_with("# T\n"));
        assert!(doc.contains("## Tóm tắt\n\nS"));
        assert!(doc.contains("- a\n- b\n"));
    }
}
