//! M7 provider gate (LIVE): Root + Child thật trên OpenAI-compatible LAN provider gọi
//! `produce_report_artifact` (Tool A) và `verify_report_artifact` (Tool B), xuất bản
//! artifact bất biến vào local `ArtifactStore`, cập nhật checkpoint manifest qua CAS,
//! và trả về `AgentResult` có `artifact_preview`, phân trang `list_artifacts` + đọc
//! `open_artifact` khớp SHA-256.
//!
//! Chạy tường minh (cần mạng + credentials trong `.env.live`, xem `.env.live.example`):
//!   cargo test --test m7_live_provider -- --ignored --nocapture
//!
//! Phạm vi evidence (GAP-04):
//! - Chỉ chứng minh thực thi thật qua OpenAI-compatible LAN provider + SQLite + local
//!   `ArtifactStore` qua public `Orchestrator`.
//! - KHÔNG tuyên bố tích hợp dịch vụ ASR/Knowledge Base/Media external thật hay AT-48
//!   multi-provider parity.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, ArtifactInput, ArtifactRole, CallerContext, HostTool, LlmBackend,
    LlmConfig, LlmProviderConfig, Orchestrator, OrchestratorConfig, PageRequest, ProviderAuth,
    RunStatus, SecretString, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};

const REPORT_CONTENT: &str = "# Live M7 Artifact Report\nVerified token: M7-LIVE-9942\n";

fn load_env_file() -> std::collections::HashMap<String, String> {
    let path = std::env::var("LIVE_ENV_FILE")
        .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/.env.live").to_string());
    let Ok(text) = std::fs::read_to_string(path) else {
        return Default::default();
    };
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (k, v) = line.split_once('=')?;
            let v = v.trim();
            let v = v
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .or_else(|| v.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
                .unwrap_or(v);
            Some((k.trim().to_string(), v.to_string()))
        })
        .collect()
}

fn env(k: &str) -> Option<String> {
    static FILE: std::sync::OnceLock<std::collections::HashMap<String, String>> =
        std::sync::OnceLock::new();
    std::env::var(k)
        .ok()
        .or_else(|| FILE.get_or_init(load_env_file).get(k).cloned())
        .filter(|v| !v.trim().is_empty())
}

struct ProduceReportTool {
    calls: Arc<AtomicUsize>,
    last_id: Arc<Mutex<Option<String>>>,
}

#[async_trait::async_trait]
impl HostTool for ProduceReportTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "produce_report_artifact".into(),
            description: "Generate the project artifact report and save it to the ArtifactStore. Returns the artifact_id.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "topic": { "type": "string" }
                },
                "required": ["topic"]
            }),
            effect: ToolEffect::Mutating,
        }
    }

    async fn execute(&self, ctx: ToolContext, _args: Value) -> Result<ToolOutcome, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ctx.progress("writing primary report artifact and checkpoint manifest");
        let report = ctx
            .artifacts()
            .put(ArtifactInput::from_bytes(
                "live-report.md",
                "text/markdown",
                ArtifactRole::Primary,
                REPORT_CONTENT.as_bytes().to_vec(),
            ))
            .await?;
        let manifest = ctx
            .artifacts()
            .put(ArtifactInput::from_bytes(
                "live-checkpoint.json",
                "application/json",
                ArtifactRole::Intermediate,
                serde_json::to_vec(&json!({
                    "report_artifact_id": report.id,
                    "verified_token": "M7-LIVE-9942"
                }))
                .unwrap(),
            ))
            .await?;
        ctx.checkpoint().commit(&manifest.id, None).await?;
        *self.last_id.lock().unwrap() = Some(report.id.clone());
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: serde_json::to_string(&json!({
                "report_artifact_id": report.id,
                "checkpoint_artifact_id": manifest.id,
                "token": "M7-LIVE-9942"
            }))
            .unwrap(),
            metadata: json!({
                "artifact_ids": [report.id, manifest.id],
                "checkpoint_artifact_id": manifest.id,
            }),
        })
    }
}

struct VerifyReportTool {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl HostTool for VerifyReportTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "verify_report_artifact".into(),
            description: "Open a previously produced report artifact by artifact_id and return its verified token.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "artifact_id": { "type": "string" }
                },
                "required": ["artifact_id"]
            }),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let id = args["artifact_id"].as_str().unwrap_or("");
        let reader = ctx.artifacts().open(id).await?;
        let text = reader.read_to_string().await?;
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!("Verified artifact {id} content:\n{text}"),
            metadata: json!({ "artifact_ids": [id] }),
        })
    }
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_provider_publishes_and_reads_artifacts_through_public_orchestrator() {
    let (Some(key), Some(base), Some(model)) = (
        env("G0_OPENAI_API_KEY").or_else(|| env("OPENAI_API_KEY")),
        env("G0_OPENAI_BASE_URL"),
        env("G0_OPENAI_MODEL"),
    ) else {
        panic!("BLOCKED: fill .env.live (see .env.live.example) or set G0_OPENAI_BASE_URL, G0_OPENAI_MODEL, G0_OPENAI_API_KEY");
    };

    let dir = tempfile::tempdir().unwrap();
    let root_prompt = dir.path().join("root.md");
    std::fs::write(
        &root_prompt,
        "You are a strict workflow executor. When asked to produce and verify a report:\n\
         1. Call `produce_report_artifact` with topic=\"m7\".\n\
         2. Read `report_artifact_id` from the tool output and call `verify_report_artifact` with that exact `artifact_id`.\n\
         3. Reply with the verified token from the report.",
    )
    .unwrap();

    let mut cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "live".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(base),
                auth: ProviderAuth::ApiKey(SecretString::new(key)),
                request_timeout: Duration::from_secs(90),
            }],
        },
        vec![AgentDefinition::new("root", "live", model)
            .with_mode(AgentMode::Primary)
            .with_prompt_path(&root_prompt)
            .with_allowed_tools(["produce_report_artifact", "verify_report_artifact"])],
        dir.path().join("store"),
    );
    cfg.runtime.artifact_preview_limit = 1;

    let produce_calls = Arc::new(AtomicUsize::new(0));
    let verify_calls = Arc::new(AtomicUsize::new(0));
    let produced_id = Arc::new(Mutex::new(None));

    let o = Orchestrator::builder(cfg)
        .register_tool(Arc::new(ProduceReportTool {
            calls: produce_calls.clone(),
            last_id: produced_id.clone(),
        }))
        .register_tool(Arc::new(VerifyReportTool {
            calls: verify_calls.clone(),
        }))
        .build()
        .await
        .unwrap();

    let caller = CallerContext::new("live-owner", "live-ws");
    let sid = o.create_session(&caller, "root").await.unwrap();
    let turn = o
        .submit(
            &caller,
            &sid,
            "Please produce the m7 report artifact, then verify it by artifact_id and tell me the token."
                .into(),
        )
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let res = loop {
        let r = o.result(&caller, &sid, &turn.turn_id).await.unwrap();
        if r.status != RunStatus::Running {
            break r;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "live turn timed out waiting for terminal status"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    assert_eq!(res.status, RunStatus::Completed, "live result: {res:?}");
    assert!(produce_calls.load(Ordering::SeqCst) >= 1);
    assert!(verify_calls.load(Ordering::SeqCst) >= 1);
    assert!(
        res.output
            .as_deref()
            .unwrap_or_default()
            .contains("M7-LIVE-9942"),
        "expected token in final output: {:?}",
        res.output
    );

    // `artifact_preview_limit = 1` -> `artifact_preview` có 1 phần tử và `artifacts_next_cursor` là `Some`
    assert_eq!(res.artifact_preview.len(), 1);
    assert!(res.artifacts_next_cursor.is_some());

    let page2 = o
        .list_artifacts(
            &caller,
            &sid,
            &turn.turn_id,
            PageRequest::after(res.artifacts_next_cursor.unwrap(), 10),
        )
        .await
        .unwrap();
    assert!(!page2.items.is_empty());

    let report_id = produced_id.lock().unwrap().clone().unwrap();
    let dl = o.open_artifact(&caller, &report_id).await.unwrap();
    assert_eq!(dl.metadata().name, "live-report.md");
    assert_eq!(dl.metadata().role, ArtifactRole::Primary);
    let content = dl.into_reader().read_to_string().await.unwrap();
    assert_eq!(content, REPORT_CONTENT);

    o.shutdown().await.unwrap();
}
