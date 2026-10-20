//! Public-seam E2E test: URL -> yt-dlp adapter -> transcript -> fake LLM summary
//! -> Markdown ArtifactStore -> verified host download. No network or API key.
//! The fixture is a deterministic fake yt-dlp executable, not a fake HostTool.

#![cfg(unix)]

#[path = "../examples/youtube_flow/tools.rs"]
mod tools;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::test_support::{
    ModelRequest, ModelResponse, ModelRole, ModelToolCall, ScriptedGateway,
};
use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, ArtifactRole, CallerContext, LlmBackend, LlmConfig,
    LlmProviderConfig, MessageRole, Orchestrator, OrchestratorConfig, PageRequest, ProviderAuth,
    RunStatus, SecretString,
};
use serde_json::json;
use tools::{FetchYoutubeTranscript, PublishYoutubeMarkdown};

const VIDEO_URL: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";
const EXPECTED_MD: &str = "# Tóm tắt video\n\nNguồn: https://www.youtube.com/watch?v=dQw4w9WgXcQ\n\n- Orchestrator xử lý phụ đề.\n- Kết quả được lưu dưới dạng Markdown.\n";

const CAPTION_SCRIPT: &str = r#"#!/bin/sh
set -eu
out=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-P" ]; then
    out=$(printf '%s' "$2" | sed 's/^home://')
    shift 2
  else
    shift
  fi
done
test -n "$out"
cat > "$out/clip.vi.vtt" <<'VTT'
WEBVTT
Kind: captions
Language: vi

00:00:00.000 --> 00:00:02.000
<c>Orchestrator xử lý phụ đề.</c>

00:00:02.000 --> 00:00:03.000
Orchestrator xử lý phụ đề.

00:00:03.000 --> 00:00:05.000
Kết quả được lưu dưới dạng Markdown.
VTT
"#;

fn script(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("fake-yt-dlp");
    std::fs::write(&path, body).unwrap();
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&path, permissions).unwrap();
    path
}

fn caller() -> CallerContext {
    CallerContext::new("youtube-test-owner", "youtube-test-workspace")
}

fn config(dir: &Path) -> OrchestratorConfig {
    let prompt = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/youtube_flow/prompt.md");
    let mut config = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "fake".into(),
                backend: LlmBackend::OpenAi,
                endpoint: None,
                auth: ProviderAuth::ApiKey(SecretString::new("not-a-real-secret")),
                request_timeout: Duration::from_secs(30),
            }],
        },
        vec![AgentDefinition::new("root", "fake", "scripted")
            .with_mode(AgentMode::Primary)
            .with_prompt_path(prompt)
            .with_allowed_tools(["fetch_youtube_transcript", "publish_youtube_markdown"])],
        dir.join("store"),
    );
    config.runtime.supervisor_sweep = Duration::from_millis(40);
    config.runtime.tool_output_cap_bytes = 512 * 1024;
    config.runtime.disk_reserve_bytes = 0;
    config
}

async fn build(dir: &Path, gw: &Arc<ScriptedGateway>, binary: &Path) -> Orchestrator {
    Orchestrator::builder(config(dir))
        .with_gateway(gw.clone())
        .register_tool(Arc::new(FetchYoutubeTranscript::new(binary)))
        .register_tool(Arc::new(PublishYoutubeMarkdown))
        .build()
        .await
        .unwrap()
}

async fn terminal(o: &Orchestrator, sid: &str, tid: &str) -> orchestrator_agent_rs::AgentResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let result = o.result(&caller(), sid, tid).await.unwrap();
        if result.status.is_terminal() {
            return result;
        }
        assert!(tokio::time::Instant::now() < deadline, "workflow hung");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn tool_call(id: &str, name: &str, args: serde_json::Value) -> ModelResponse {
    ModelResponse {
        text: String::new(),
        finish_reason: "tool_use".into(),
        tool_calls: vec![ModelToolCall {
            id: id.into(),
            name: name.into(),
            arguments: args,
        }],
        usage: None,
    }
}

fn assert_transcript_reached_model(req: &ModelRequest) {
    let transcript = req
        .messages
        .iter()
        .find(|m| {
            m.role == ModelRole::Tool && m.tool_name.as_deref() == Some("fetch_youtube_transcript")
        })
        .expect("expected real transcript tool result in LLM context");
    assert!(transcript.text.contains("Orchestrator xử lý phụ đề."));
    assert!(transcript
        .text
        .contains("Kết quả được lưu dưới dạng Markdown."));
    assert_eq!(
        transcript
            .text
            .matches("Orchestrator xử lý phụ đề.")
            .count(),
        1
    );
}

#[tokio::test]
async fn url_to_verified_markdown_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let binary = script(tmp.path(), CAPTION_SCRIPT);
    let gw = ScriptedGateway::new();
    gw.push_response(tool_call(
        "download",
        "fetch_youtube_transcript",
        json!({"url": VIDEO_URL}),
    ));
    gw.push_with(|req| {
        assert_transcript_reached_model(req);
        let transcript_result = req.messages.iter().find(|m| {
            m.role == ModelRole::Tool
                && m.tool_name.as_deref() == Some("fetch_youtube_transcript")
        }).expect("transcript tool result is required");
        let payload: serde_json::Value = serde_json::from_str(&transcript_result.text)
            .expect("tool returned JSON");
        let transcript_id = payload["transcript_artifact_id"]
            .as_str()
            .expect("transcript artifact id");
        tool_call(
            "publish",
            "publish_youtube_markdown",
            json!({
                "markdown": EXPECTED_MD,
                "transcript_artifact_id": transcript_id
            }),
        )
    });
    gw.push_with(|req| {
        assert!(req.messages.iter().any(|m| {
            m.role == ModelRole::Tool
                && m.tool_name.as_deref() == Some("publish_youtube_markdown")
                && m.text.contains("artifact_id")
        }));
        ModelResponse {
            text: "Đã lưu Markdown artifact".into(),
            finish_reason: "end_turn".into(),
            tool_calls: vec![],
            usage: None,
        }
    });

    let o = build(tmp.path(), &gw, &binary).await;
    let sid = o.create_session(&caller(), "root").await.unwrap();
    let turn = o.submit(&caller(), &sid, VIDEO_URL.into()).await.unwrap();
    let result = terminal(&o, &sid, &turn.turn_id).await;
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(result.output.as_deref(), Some("Đã lưu Markdown artifact"));
    assert_eq!(gw.agent_requests().len(), 3);

    // Both intermediate transcript and primary report must be durable artifacts.
    assert_eq!(result.artifact_preview.len(), 2);
    let report = result
        .artifact_preview
        .iter()
        .find(|a| a.role == ArtifactRole::Primary && a.name == "youtube-summary.md")
        .expect("Markdown artifact missing");
    let report_id = report.id.clone();
    let transcript = result
        .artifact_preview
        .iter()
        .find(|a| a.name == "youtube-transcript.txt")
        .expect("transcript artifact missing");
    assert_eq!(transcript.role, ArtifactRole::Intermediate);
    let markdown = o
        .open_artifact(&caller(), &report_id)
        .await
        .unwrap()
        .into_reader()
        .read_to_string()
        .await
        .unwrap();
    assert_eq!(markdown, EXPECTED_MD);
    let captured = o
        .open_artifact(&caller(), &transcript.id)
        .await
        .unwrap()
        .into_reader()
        .read_to_string()
        .await
        .unwrap();
    assert_eq!(
        captured,
        "Orchestrator xử lý phụ đề.\nKết quả được lưu dưới dạng Markdown."
    );

    let history = o
        .history(&caller(), &sid, PageRequest::first(30))
        .await
        .unwrap();
    assert_eq!(
        history
            .items
            .iter()
            .filter(|m| m.role == MessageRole::Tool)
            .count(),
        2
    );
    o.shutdown().await.unwrap();
    drop(o);

    // Check the public read seam after restart, without replaying yt-dlp or LLM.
    let gw_after_restart = ScriptedGateway::new();
    let reopened = build(tmp.path(), &gw_after_restart, &binary).await;
    let result_after_restart = reopened
        .result(&caller(), &sid, &turn.turn_id)
        .await
        .unwrap();
    assert_eq!(result_after_restart.status, RunStatus::Completed);
    assert_eq!(result_after_restart.artifact_preview.len(), 2);
    let text = reopened
        .open_artifact(&caller(), &report_id)
        .await
        .unwrap()
        .into_reader()
        .read_to_string()
        .await
        .unwrap();
    assert_eq!(text, EXPECTED_MD);
    assert_eq!(gw_after_restart.call_count(), 0);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn no_subtitles_must_not_publish_a_summary() {
    let tmp = tempfile::tempdir().unwrap();
    let binary = script(tmp.path(), "#!/bin/sh\nexit 0\n");
    let gw = ScriptedGateway::new();
    gw.push_response(tool_call(
        "download",
        "fetch_youtube_transcript",
        json!({"url": VIDEO_URL}),
    ));
    gw.push_text("Không có transcript, không thể tóm tắt.");
    let o = build(tmp.path(), &gw, &binary).await;
    let sid = o.create_session(&caller(), "root").await.unwrap();
    let turn = o.submit(&caller(), &sid, VIDEO_URL.into()).await.unwrap();
    let result = terminal(&o, &sid, &turn.turn_id).await;
    assert_eq!(result.status, RunStatus::Completed);
    assert!(result.artifact_preview.is_empty());
    let history = o
        .history(&caller(), &sid, PageRequest::first(20))
        .await
        .unwrap();
    assert!(history
        .items
        .iter()
        .any(|m| m.role == MessageRole::Tool && m.text.contains("NO_TRANSCRIPT")));
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn untrusted_url_is_rejected_before_external_process() {
    let tmp = tempfile::tempdir().unwrap();
    let binary = tmp.path().join("this-command-does-not-exist");
    let gw = ScriptedGateway::new();
    gw.push_response(tool_call(
        "download",
        "fetch_youtube_transcript",
        json!({"url": "file:///etc/passwd"}),
    ));
    gw.push_text("URL không hợp lệ.");
    let o = build(tmp.path(), &gw, &binary).await;
    let sid = o.create_session(&caller(), "root").await.unwrap();
    let turn = o.submit(&caller(), &sid, "bad URL".into()).await.unwrap();
    let result = terminal(&o, &sid, &turn.turn_id).await;
    assert_eq!(result.status, RunStatus::Completed);
    assert!(result.artifact_preview.is_empty());
    let history = o
        .history(&caller(), &sid, PageRequest::first(20))
        .await
        .unwrap();
    assert!(history
        .items
        .iter()
        .any(|m| m.role == MessageRole::Tool && m.text.contains("INVALID_URL")));
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn publishing_without_a_verified_transcript_is_denied() {
    let tmp = tempfile::tempdir().unwrap();
    let binary = tmp.path().join("not-used");
    let gw = ScriptedGateway::new();
    gw.push_response(tool_call(
        "publish",
        "publish_youtube_markdown",
        json!({
            "markdown": EXPECTED_MD,
            "transcript_artifact_id": "missing-transcript"
        }),
    ));
    gw.push_text("No valid transcript artifact");
    let o = build(tmp.path(), &gw, &binary).await;
    let sid = o.create_session(&caller(), "root").await.unwrap();
    let turn = o.submit(&caller(), &sid, VIDEO_URL.into()).await.unwrap();
    let result = terminal(&o, &sid, &turn.turn_id).await;
    assert_eq!(result.status, RunStatus::Completed);
    assert!(result.artifact_preview.is_empty(), "unverified summary was published");
    let history = o
        .history(&caller(), &sid, PageRequest::first(20))
        .await
        .unwrap();
    assert!(history.items.iter().any(|m| {
        m.role == MessageRole::Tool && m.text.contains("ARTIFACT_NOT_FOUND")
    }));
    o.shutdown().await.unwrap();
}
