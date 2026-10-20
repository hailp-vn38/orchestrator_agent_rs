//! Live end-to-end: YouTube URL -> yt-dlp captions -> LLM -> Markdown artifact -> file.
//!
//! cargo run --example youtube_flow -- "https://www.youtube.com/watch?v=VIDEO_ID" output.md
//! Environment: G0_OPENAI_BASE_URL, G0_OPENAI_MODEL, G0_OPENAI_API_KEY
//! Optional: YTDLP_BIN, LIVE_ENV_FILE. Missing subtitles are reported, never hallucinated.

#[path = "youtube_flow/tools.rs"]
mod tools;

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, ArtifactRole, CallerContext, LlmBackend, LlmConfig,
    LlmProviderConfig, Orchestrator, OrchestratorConfig, ProviderAuth, RunStatus, SecretString,
};
use tools::{FetchYoutubeTranscript, PublishYoutubeMarkdown};

fn setting(key: &str) -> Option<String> {
    if let Ok(value) = std::env::var(key) {
        if !value.trim().is_empty() {
            return Some(value);
        }
    }
    let env_file = std::env::var("LIVE_ENV_FILE")
        .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/.env.live").to_string());
    let content = std::fs::read_to_string(env_file).ok()?;
    content.lines().find_map(|raw| {
        let line = raw.trim();
        if line.starts_with('#') {
            return None;
        }
        let (name, value) = line.split_once('=')?;
        if name.trim() != key {
            return None;
        }
        let value = value.trim().trim_matches('"').trim_matches('\'');
        (!value.is_empty()).then(|| value.to_string())
    })
}

fn required(key: &str) -> Result<String, Box<dyn Error>> {
    setting(key).ok_or_else(|| format!("missing {key} (see .env.live.example)").into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let url = args
        .next()
        .ok_or("usage: cargo run --example youtube_flow -- <youtube-url> [output.md]")?;
    let destination = PathBuf::from(
        args.next()
            .unwrap_or_else(|| "youtube-summary.md".to_string()),
    );
    if args.next().is_some() {
        return Err("expected one YouTube URL and at most one output path".into());
    }

    let base = required("G0_OPENAI_BASE_URL")?;
    let model = required("G0_OPENAI_MODEL")?;
    let key = required("G0_OPENAI_API_KEY")?;
    let binary = setting("YTDLP_BIN").unwrap_or_else(|| "yt-dlp".to_string());

    let temp = tempfile::tempdir()?;
    let prompt = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/youtube_flow/prompt.md");

    let mut config = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "live".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(base),
                auth: ProviderAuth::ApiKey(SecretString::new(key)),
                request_timeout: Duration::from_secs(120),
            }],
        },
        vec![AgentDefinition::new("root", "live", model)
            .with_mode(AgentMode::Primary)
            .with_prompt_path(prompt)
            .with_allowed_tools(["fetch_youtube_transcript", "publish_youtube_markdown"])],
        temp.path().join("store"),
    );
    // Do not truncate the returned subtitles silently before the LLM reads them.
    config.runtime.tool_output_cap_bytes = 512 * 1024;

    let orchestrator = Orchestrator::builder(config)
        .register_tool(Arc::new(FetchYoutubeTranscript::new(binary)))
        .register_tool(Arc::new(PublishYoutubeMarkdown))
        .build()
        .await?;

    let caller = CallerContext::new("youtube-demo", "default");
    let session_id = orchestrator.create_session(&caller, "root").await?;
    let turn = orchestrator
        .submit(
            &caller,
            &session_id,
            format!("Get the transcript of this video, summarize it, and create a Markdown report: {url}"),
        )
        .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
    let result = loop {
        let result = orchestrator
            .result(&caller, &session_id, &turn.turn_id)
            .await?;
        if result.status != RunStatus::Running {
            break result;
        }
        if tokio::time::Instant::now() >= deadline {
            orchestrator.cancel(&caller, &session_id).await?;
            orchestrator.shutdown().await?;
            return Err("workflow exceeded 10 minutes and was cancelled".into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    if result.status != RunStatus::Completed {
        orchestrator.shutdown().await?;
        return Err(format!(
            "run did not complete: {:?} {:?}",
            result.status, result.error
        )
        .into());
    }

    // Completed run alone does not prove the business output exists.
    let report = result
        .artifact_preview
        .iter()
        .find(|a| a.name == "youtube-summary.md" && a.role == ArtifactRole::Primary)
        .ok_or("run finished but no Markdown artifact was published")?;
    let markdown = orchestrator
        .open_artifact(&caller, &report.id)
        .await?
        .into_reader()
        .read_to_string()
        .await?;
    tokio::fs::write(&destination, markdown.as_bytes()).await?;
    println!(
        "Wrote {} ({} bytes, artifact ID {})",
        destination.display(),
        markdown.len(),
        report.id
    );
    println!("Orchestrator: {}", result.output.unwrap_or_default());
    orchestrator.shutdown().await?;
    Ok(())
}
