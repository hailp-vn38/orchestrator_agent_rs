//! M7 gate (fake LLM + SQLite thật + local `ArtifactStore`):
//!
//! - **AT-40**: HostTool B đọc artifact do HostTool A tạo trong cùng session family qua
//!   `context.artifacts().open(id)`; out-of-scope ID (khác tenant, khác session cùng workspace,
//!   hoặc artifact trong cùng session nhưng không được truyền qua arguments/checkpoint của
//!   invocation hiện tại) bị từ chối với `PERMISSION_DENIED`; file thiếu hoặc corrupt trả
//!   `ARTIFACT_INTEGRITY_ERROR`; hỗ trợ cả bounded stream và `AsyncRead`.
//! - **AT-42**: `AgentResult` projection với recovery attempt mới nhất (`output = None` khi
//!   attempt mới đang chạy, `output` từ `final_message_id` của attempt mới nhất khi xong,
//!   `artifact_preview` giới hạn `artifact_preview_limit`, `artifacts_next_cursor` phân trang qua
//!   `list_artifacts`, cursor gắn chặt `(session_id, turn_id)`).
//! - **AT-47**: Fault injection quanh staging, sau atomic rename nhưng trước SQLite metadata
//!   commit, sau artifact publish nhưng trước tool result commit, và sau manifest publish nhưng
//!   trước checkpoint CAS; concurrent/stale CAS báo `CHECKPOINT_CONFLICT`; orphan sweep khi
//!   startup xóa staging/unpublished file quá `artifact_staging_grace` mà giữ nguyên mọi
//!   committed artifact.
//! - **Quotas, limits & interrupted invocation**: `artifact_max_bytes`,
//!   `owner_workspace_quota_bytes`, `disk_reserve_bytes`, `download_max_bytes`, và kiểm chứng
//!   `metadata.artifact_ids` trong `resolve_interrupted_invocation`.
//! - **Consistent backup/restore**: `Orchestrator::backup_to` khi SQLite WAL đang mở, khôi phục
//!   sang `store_dir` mới và đối chiếu `result`, `history`, `list_artifacts`, `open_artifact`.
//! - **3 workload fixtures bắt buộc (docs/17 §17.10)**:
//!   1. Playlist 20 video: 18 thành công (transcript + analysis + versioned
//!      `playlist-manifest.json` + CAS pointer), 2 lỗi ASR; crash giữa chừng và resume trong cùng
//!      child session chỉ bỏ qua item đã verify, Root tổng hợp 18/20 và `RunStatus::Completed`
//!      không che giấu 2 item lỗi.
//!   2. Document conversion + Knowledge Base: chuyển đổi PDF/DOCX sang `document.md` +
//!      `document-summary.md`; `knowledge_ingest` trả pending/failure không tạo fake receipt hay
//!      fake local artifact; retry/reconcile theo idempotency key thành công.
//!   3. Media generation: `media_generate` trả `accepted` kèm durable provider job ID lưu trong
//!      checkpoint trước khi crash; sau restart/reconcile, kiểm tra job ID cũ (không tạo job
//!      trùng), tải và publish media artifact đã verify.
//! - **Long-running combined scenario (docs/18 §18.10)**: `> 100` model/tool steps + active-turn
//!   compaction + subscription disconnect + restart recovery + canonical reconstruction của
//!   `AgentResult` và artifacts.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestrator_agent_rs::test_support::{
    ArtifactFault, ModelRequest, ModelResponse, ModelRole, ModelToolCall, ScriptedGateway,
    StreamEvent,
};
use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, ArtifactInput, ArtifactRole, CallerContext, ErrorCode,
    FeedItem, HostTool, LlmBackend, LlmConfig, LlmProviderConfig, MessageRole, OperatorContext,
    Orchestrator, OrchestratorConfig, PageRequest, ProviderAuth, RunStatus, SecretString,
    ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome, VerifiedToolResolution,
};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};
use tokio::io::AsyncReadExt;

const ROOT_MARK: &str = "You are the root coordinator agent.";
const WORKER_MARK: &str = "You are the worker subagent.";

struct Env {
    dir: tempfile::TempDir,
}

impl Env {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("root.md"), ROOT_MARK).unwrap();
        std::fs::write(dir.path().join("worker.md"), WORKER_MARK).unwrap();
        Self { dir }
    }

    fn store(&self) -> PathBuf {
        self.dir.path().join("store")
    }

    fn config_with_tools(
        &self,
        root_tools: Vec<&str>,
        worker_tools: Vec<&str>,
    ) -> OrchestratorConfig {
        let mut root = AgentDefinition::new("root", "fake", "model-x")
            .with_mode(AgentMode::Primary)
            .with_prompt_path(self.dir.path().join("root.md"));
        if !root_tools.is_empty() {
            root = root.with_allowed_tools(root_tools);
        }
        let mut worker = AgentDefinition::new("worker", "fake", "model-x")
            .with_mode(AgentMode::Subagent)
            .with_description("Worker subagent for heavy artifact workloads")
            .with_prompt_path(self.dir.path().join("worker.md"));
        if !worker_tools.is_empty() {
            worker = worker.with_allowed_tools(worker_tools);
        }
        let mut cfg = OrchestratorConfig::new(
            LlmConfig {
                providers: vec![LlmProviderConfig {
                    id: "fake".into(),
                    backend: LlmBackend::OpenAi,
                    endpoint: None,
                    auth: ProviderAuth::ApiKey(SecretString::new("sk-test-not-real")),
                    request_timeout: Duration::from_secs(30),
                }],
            },
            vec![root, worker],
            self.store(),
        );
        cfg.runtime.supervisor_sweep = Duration::from_millis(40);
        cfg.runtime.shutdown_grace = Duration::from_secs(5);
        cfg
    }

    async fn sql(&self) -> SqliteConnection {
        let opts = SqliteConnectOptions::new()
            .filename(self.store().join("orchestrator.db"))
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        SqliteConnection::connect_with(&opts).await.unwrap()
    }
}

fn alice() -> CallerContext {
    CallerContext::new("alice", "ws1")
}

fn bob() -> CallerContext {
    CallerContext::new("bob", "ws1")
}

fn tool_err(code: &str, message: impl Into<String>) -> ToolError {
    ToolError {
        code: code.to_string(),
        message: message.into(),
    }
}

async fn wait_terminal(o: &Orchestrator, c: &CallerContext, s: &str, t: &str) -> AgentResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let r = o.result(c, s, t).await.unwrap();
        if r.status != RunStatus::Running {
            return r;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for terminal result of ({s}, {t})"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

// ───────────────────────────── AT-40: Scoped open + integrity ─────────────────────────────

struct ProduceArtifactTool;

#[async_trait::async_trait]
impl HostTool for ProduceArtifactTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "produce_artifact".into(),
            description: "Write a primary artifact from text or streaming reader".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "body": { "type": "string" },
                    "streaming": { "type": "boolean" }
                },
                "required": ["name", "body"]
            }),
            effect: ToolEffect::Mutating,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        let name = args["name"].as_str().unwrap_or("note.txt");
        let body = args["body"].as_str().unwrap_or("");
        let streaming = args["streaming"].as_bool().unwrap_or(false);
        let input = if streaming {
            ArtifactInput::from_reader(
                name,
                "text/plain",
                ArtifactRole::Primary,
                std::io::Cursor::new(body.as_bytes().to_vec()),
            )
        } else {
            ArtifactInput::from_bytes(
                name,
                "text/plain",
                ArtifactRole::Primary,
                body.as_bytes().to_vec(),
            )
        };
        let art = ctx.artifacts().put(input).await?;
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!("created artifact {}", art.id),
            metadata: json!({
                "artifact_ids": [art.id],
                "sha256": art.sha256,
                "size_bytes": art.size_bytes,
            }),
        })
    }
}

struct ConsumeArtifactTool;

#[async_trait::async_trait]
impl HostTool for ConsumeArtifactTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "consume_artifact".into(),
            description: "Open an artifact by id and verify both stream and AsyncRead".into(),
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
        let id = args["artifact_id"].as_str().unwrap_or("");
        // 1) Đọc qua bounded stream (`read_chunk` tới EOF để xác minh SHA-256)
        let mut reader = ctx.artifacts().open(id).await?;
        let mut bytes_stream = Vec::new();
        while let Some(chunk) = reader.read_chunk(16).await? {
            bytes_stream.extend_from_slice(&chunk);
        }

        // 2) Đọc qua `tokio::io::AsyncRead`
        let mut async_reader = ctx.artifacts().open(id).await?;
        let mut bytes_async = Vec::new();
        async_reader
            .read_to_end(&mut bytes_async)
            .await
            .map_err(|e| tool_err("IO_ERROR", e.to_string()))?;
        assert_eq!(bytes_stream, bytes_async);

        let text = String::from_utf8(bytes_stream).unwrap();
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!("consumed:{text}"),
            metadata: json!({ "artifact_ids": [id] }),
        })
    }
}

/// Tool cố tình mở một artifact ID nằm ngoài arguments/checkpoint của invocation hiện tại
/// (bằng cách nhận ID qua in-memory channel thay vì arguments_json).
struct SneakyReadTool {
    hidden_target: Arc<Mutex<String>>,
}

#[async_trait::async_trait]
impl HostTool for SneakyReadTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "sneaky_read".into(),
            description: "Try to open a hidden artifact id not passed in arguments".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "note": { "type": "string" } },
                "required": ["note"]
            }),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(&self, ctx: ToolContext, _args: Value) -> Result<ToolOutcome, ToolError> {
        let target = self.hidden_target.lock().unwrap().clone();
        match ctx.artifacts().open(&target).await {
            Ok(_) => Err(tool_err(
                "UNEXPECTED_OK",
                "expected PERMISSION_DENIED, got Ok",
            )),
            Err(err) => Ok(ToolOutcome {
                call_id: ctx.scope().tool_call_id.clone(),
                content: format!("denied:{}:{}", err.code, err.message),
                metadata: Value::Null,
            }),
        }
    }
}

#[tokio::test]
async fn at40_tool_b_reads_tool_a_artifact_and_enforces_scope_and_integrity() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let hidden_target = Arc::new(Mutex::new(String::new()));

    // Bước 1: Root gọi `produce_artifact` (streaming = true) tạo artifact A.
    gw.push_tool_calls(
        "",
        vec![(
            "call-produce-a",
            "produce_artifact",
            json!({
                "name": "report.md",
                "body": "Hello from Tool A!\nLine 2.",
                "streaming": true
            }),
        )],
    );
    // Bước 2: Sau khi có kết quả Tool A, Root trích artifact_id từ tool output và truyền
    // sang `consume_artifact` (Tool B).
    let captured_art_id = Arc::new(Mutex::new(String::new()));
    let captured_clone = captured_art_id.clone();
    gw.push_with(move |req: &ModelRequest| {
        let last_tool = req
            .messages
            .iter()
            .rev()
            .find(|m| m.role == ModelRole::Tool)
            .expect("tool output from produce_artifact");
        let art_id = last_tool
            .text
            .split("created artifact ")
            .nth(1)
            .unwrap()
            .trim()
            .to_string();
        *captured_clone.lock().unwrap() = art_id.clone();
        ModelResponse {
            text: String::new(),
            finish_reason: "tool_use".into(),
            tool_calls: vec![ModelToolCall {
                id: "call-consume-b".into(),
                name: "consume_artifact".into(),
                arguments: json!({ "artifact_id": art_id }),
            }],
            usage: None,
        }
    });
    // Bước 3: Root gọi `sneaky_read` KHÔNG truyền artifact_id trong arguments -> phải bị từ chối
    // dù đang ở trong cùng session!
    let hidden_clone = hidden_target.clone();
    let captured_for_sneaky = captured_art_id.clone();
    gw.push_with(move |_req: &ModelRequest| {
        *hidden_clone.lock().unwrap() = captured_for_sneaky.lock().unwrap().clone();
        ModelResponse {
            text: String::new(),
            finish_reason: "tool_use".into(),
            tool_calls: vec![ModelToolCall {
                id: "call-sneaky".into(),
                name: "sneaky_read".into(),
                arguments: json!({ "note": "nothing passed here" }),
            }],
            usage: None,
        }
    });
    gw.push_text("AT-40 workflow done.");

    let cfg = env.config_with_tools(
        vec!["produce_artifact", "consume_artifact", "sneaky_read"],
        vec!["consume_artifact"],
    );
    let o = Orchestrator::builder(cfg)
        .with_gateway(gw.clone())
        .register_tool(Arc::new(ProduceArtifactTool))
        .register_tool(Arc::new(ConsumeArtifactTool))
        .register_tool(Arc::new(SneakyReadTool {
            hidden_target: hidden_target.clone(),
        }))
        .build()
        .await
        .unwrap();

    let s1 = o.create_session(&alice(), "root").await.unwrap();
    let t1 = o.submit(&alice(), &s1, "run at40".into()).await.unwrap();
    let r1 = wait_terminal(&o, &alice(), &s1, &t1.turn_id).await;
    assert_eq!(r1.status, RunStatus::Completed);
    assert_eq!(r1.artifact_preview.len(), 1);
    let art_id = r1.artifact_preview[0].id.clone();
    assert_eq!(r1.artifact_preview[0].name, "report.md");
    assert_eq!(r1.artifact_preview[0].role, ArtifactRole::Primary);

    // Kiểm tra history xác nhận Tool B đọc được nội dung và SneakyRead bị PERMISSION_DENIED.
    let hist = o
        .history(&alice(), &s1, PageRequest::first(50))
        .await
        .unwrap();
    let tool_outputs: Vec<&str> = hist
        .items
        .iter()
        .filter(|m| m.role == MessageRole::Tool)
        .map(|m| m.text.as_str())
        .collect();
    assert!(
        tool_outputs
            .iter()
            .any(|c| c.contains("consumed:Hello from Tool A!\nLine 2.")),
        "Tool B must read Tool A artifact: {tool_outputs:?}"
    );
    assert!(
        tool_outputs
            .iter()
            .any(|c| c.contains("denied:PERMISSION_DENIED:")),
        "unreferenced artifact in same session must be denied: {tool_outputs:?}"
    );

    // Kiểm tra cross-session trong cùng workspace (`alice`, `ws1` nhưng session `s2` khác root family):
    // truyền thẳng `art_id` vào arguments của `consume_artifact` ở `s2` vẫn phải bị PERMISSION_DENIED!
    gw.push_tool_calls(
        "",
        vec![(
            "call-cross-session",
            "consume_artifact",
            json!({ "artifact_id": art_id }),
        )],
    );
    gw.push_text("cross-session checked");
    let s2 = o.create_session(&alice(), "root").await.unwrap();
    let t2 = o
        .submit(&alice(), &s2, "try cross session read".into())
        .await
        .unwrap();
    let r2 = wait_terminal(&o, &alice(), &s2, &t2.turn_id).await;
    assert_eq!(r2.status, RunStatus::Completed);
    let hist2 = o
        .history(&alice(), &s2, PageRequest::first(50))
        .await
        .unwrap();
    let cross_tool_msg = hist2
        .items
        .iter()
        .find(|m| m.role == MessageRole::Tool)
        .unwrap();
    assert!(
        cross_tool_msg.text.contains("PERMISSION_DENIED")
            || cross_tool_msg
                .text
                .contains("artifact does not belong to this session family"),
        "cross-session tool open must fail with permission denied: {}",
        cross_tool_msg.text
    );

    // Kiểm tra host API `open_artifact`:
    // - Khác caller (`bob`) -> `PermissionDenied`
    let bob_err = o.open_artifact(&bob(), &art_id).await.unwrap_err();
    assert_eq!(bob_err.code(), ErrorCode::PermissionDenied);

    // - Đúng caller (`alice`) -> đọc `read_all` khớp nội dung
    let dl = o.open_artifact(&alice(), &art_id).await.unwrap();
    assert_eq!(dl.metadata().id, art_id);
    let bytes = dl.read_all().await.unwrap();
    assert_eq!(bytes, b"Hello from Tool A!\nLine 2.");

    // - Corrupt file trên đĩa -> `open_artifact` trả `ARTIFACT_INTEGRITY_ERROR`
    let mut sql = env.sql().await;
    let storage_key: String = sqlx::query_scalar("SELECT storage_key FROM artifacts WHERE id = ?")
        .bind(&art_id)
        .fetch_one(&mut sql)
        .await
        .unwrap();
    let full_path = env
        .store()
        .join("artifacts")
        .join("committed")
        .join(&storage_key);
    // Ghi đè cùng độ dài nhưng sai nội dung để kích hoạt SHA-256 mismatch
    std::fs::write(&full_path, b"CORRUPTED_BYTES_SAME_LEN!!").unwrap();

    let dl_corrupt = o.open_artifact(&alice(), &art_id).await.unwrap();
    let err_corrupt = dl_corrupt.read_all().await.unwrap_err();
    assert_eq!(err_corrupt.code(), ErrorCode::Unavailable);
    assert!(
        err_corrupt.message().contains("ARTIFACT_INTEGRITY_ERROR"),
        "expected ARTIFACT_INTEGRITY_ERROR, got: {}",
        err_corrupt.message()
    );

    // Xóa hẳn file trên đĩa -> `open_artifact` trả `ARTIFACT_INTEGRITY_ERROR`
    std::fs::remove_file(&full_path).unwrap();
    let err_missing = match o.open_artifact(&alice(), &art_id).await {
        Ok(_) => panic!("expected error when artifact file is missing"),
        Err(e) => e,
    };
    assert_eq!(err_missing.code(), ErrorCode::Unavailable);
    assert!(err_missing.message().contains("ARTIFACT_INTEGRITY_ERROR"));

    o.shutdown().await.unwrap();
}

// ───────────────────────────── AT-42: AgentResult + pagination ─────────────────────────────

struct BatchArtifactTool;

#[async_trait::async_trait]
impl HostTool for BatchArtifactTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "batch_artifacts".into(),
            description: "Produce multiple primary and intermediate artifacts".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "count": { "type": "integer" }
                },
                "required": ["count"]
            }),
            effect: ToolEffect::Mutating,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        let count = args["count"].as_u64().unwrap_or(1) as usize;
        let mut ids = Vec::new();
        for i in 0..count {
            let role = if i % 2 == 0 {
                ArtifactRole::Primary
            } else {
                ArtifactRole::Intermediate
            };
            let r = ctx
                .artifacts()
                .put(ArtifactInput::from_bytes(
                    format!("item-{i:02}.txt"),
                    "text/plain",
                    role,
                    format!("payload-{i}").into_bytes(),
                ))
                .await?;
            ids.push(r.id);
        }
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!("created {} artifacts", ids.len()),
            metadata: json!({ "artifact_ids": ids }),
        })
    }
}

#[tokio::test]
async fn at42_agent_result_preview_pagination_and_recovery_attempt_ownership() {
    let env = Env::new();
    let mut cfg = env.config_with_tools(vec!["batch_artifacts"], vec![]);
    cfg.runtime.artifact_preview_limit = 2;
    cfg.runtime.page_size = 2;

    let gw = ScriptedGateway::new();
    gw.push_tool_calls(
        "",
        vec![("call-batch", "batch_artifacts", json!({ "count": 5 }))],
    );
    gw.push_text("attempt-1 finished with 5 artifacts");

    let o = Orchestrator::builder(cfg.clone())
        .with_gateway(gw.clone())
        .register_tool(Arc::new(BatchArtifactTool))
        .build()
        .await
        .unwrap();

    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o.submit(&alice(), &sid, "make 5".into()).await.unwrap();
    let res = wait_terminal(&o, &alice(), &sid, &turn.turn_id).await;

    assert_eq!(res.status, RunStatus::Completed);
    assert_eq!(
        res.output.as_deref(),
        Some("attempt-1 finished with 5 artifacts")
    );
    assert_eq!(res.artifact_preview.len(), 2);
    assert!(res.artifacts_next_cursor.is_some());

    // Phân trang qua `list_artifacts` từ `artifacts_next_cursor` của `AgentResult`
    let mut all_refs = res.artifact_preview.clone();
    let mut cursor = res.artifacts_next_cursor.clone();
    while let Some(c) = cursor {
        let page = o
            .list_artifacts(&alice(), &sid, &turn.turn_id, PageRequest::after(c, 2))
            .await
            .unwrap();
        all_refs.extend(page.items);
        cursor = page.next_cursor;
    }
    assert_eq!(all_refs.len(), 5);
    let names: Vec<&str> = all_refs.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "item-00.txt",
            "item-01.txt",
            "item-02.txt",
            "item-03.txt",
            "item-04.txt"
        ]
    );

    // Cursor gắn chặt với `(session_id, turn_id)`: dùng cho turn khác trên session khác phải trả `InvalidInput`
    let sid_other = o.create_session(&alice(), "root").await.unwrap();
    let turn_other = {
        gw.push_text("second session turn");
        let t2 = o
            .submit(&alice(), &sid_other, "turn other".into())
            .await
            .unwrap();
        wait_terminal(&o, &alice(), &sid_other, &t2.turn_id).await;
        t2
    };
    let bad_cursor_err = o
        .list_artifacts(
            &alice(),
            &sid_other,
            &turn_other.turn_id,
            PageRequest::after(res.artifacts_next_cursor.clone().unwrap(), 2),
        )
        .await
        .unwrap_err();
    assert_eq!(bad_cursor_err.code(), ErrorCode::InvalidInput);

    // Kiểm tra AT-42 khi có recovery attempt mới đang `Running`:
    // Giả lập attempt 0 trên `turn.turn_id` bị `interrupted` (dù đã từng có `final_message_id`)
    // để khi khởi động `o2`, startup recovery tạo attempt mới (`resume_attempt = 1`) đang `Running` ->
    // `result().output` phải là `None` khi attempt 1 đang chạy, nhưng `artifact_preview` vẫn có!
    o.shutdown().await.unwrap();
    let mut sql = env.sql().await;
    sqlx::query(
        "UPDATE agent_runs SET status = 'interrupted', error_code = 'INTERRUPTED' WHERE id = ?",
    )
    .bind(&res.run_id)
    .execute(&mut sql)
    .await
    .unwrap();
    drop(sql);

    let gw2 = ScriptedGateway::new();
    gw2.hold();
    gw2.push_text("attempt-2 canonical final answer");
    let o2 = Orchestrator::builder(cfg)
        .with_gateway(gw2.clone())
        .register_tool(Arc::new(BatchArtifactTool))
        .build()
        .await
        .unwrap();

    // Khi attempt mới (`resume_attempt = 1`) đang giữ ở gateway (`Running`), `output` phải là `None`
    let running_res = o2.result(&alice(), &sid, &turn.turn_id).await.unwrap();
    assert_eq!(running_res.attempt, 1);
    assert_eq!(running_res.status, RunStatus::Running);
    assert_eq!(
        running_res.output, None,
        "running recovery attempt must not leak stale attempt-0 output"
    );
    assert_eq!(running_res.artifact_preview.len(), 2);

    gw2.release();
    let final_res = wait_terminal(&o2, &alice(), &sid, &turn.turn_id).await;
    assert_eq!(final_res.attempt, 1);
    assert_eq!(final_res.status, RunStatus::Completed);
    assert_eq!(
        final_res.output.as_deref(),
        Some("attempt-2 canonical final answer")
    );
    assert_eq!(final_res.artifact_preview.len(), 2);
    o2.shutdown().await.unwrap();
}

// ───────────────────────────── AT-47: Fault injection, CAS & orphan sweep ─────────────────────────────

struct CheckpointFaultTool {
    fault: Arc<ArtifactFault>,
    mode: Arc<Mutex<String>>,
}

#[async_trait::async_trait]
impl HostTool for CheckpointFaultTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "checkpoint_fault_tool".into(),
            description: "Test staging/rename/checkpoint CAS faults".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "step": { "type": "string" } },
                "required": ["step"]
            }),
            effect: ToolEffect::Mutating,
        }
    }

    async fn execute(&self, ctx: ToolContext, _args: Value) -> Result<ToolOutcome, ToolError> {
        let mode = self.mode.lock().unwrap().clone();
        match mode.as_str() {
            "fail_staging" => {
                self.fault.set_fail_during_staging(true);
                let res = ctx
                    .artifacts()
                    .put(ArtifactInput::from_bytes(
                        "stage-fail.bin",
                        "application/octet-stream",
                        ArtifactRole::Intermediate,
                        vec![1, 2, 3, 4],
                    ))
                    .await;
                self.fault.set_fail_during_staging(false);
                let err = res.expect_err("staging fault must fail put");
                Ok(ToolOutcome {
                    call_id: ctx.scope().tool_call_id.clone(),
                    content: format!("staging_failed:{}", err.message),
                    metadata: Value::Null,
                })
            }
            "fail_after_rename" => {
                self.fault.set_fail_after_rename_before_metadata(true);
                let res = ctx
                    .artifacts()
                    .put(ArtifactInput::from_bytes(
                        "orphan-after-rename.bin",
                        "application/octet-stream",
                        ArtifactRole::Intermediate,
                        vec![9, 9, 9],
                    ))
                    .await;
                self.fault.set_fail_after_rename_before_metadata(false);
                let err = res.expect_err("post-rename fault must fail put");
                Ok(ToolOutcome {
                    call_id: ctx.scope().tool_call_id.clone(),
                    content: format!("rename_failed:{}", err.message),
                    metadata: Value::Null,
                })
            }
            "cas_and_conflict" => {
                assert_eq!(ctx.checkpoint().current().await?, None);
                let m1 = ctx
                    .artifacts()
                    .put(ArtifactInput::from_bytes(
                        "manifest-v1.json",
                        "application/json",
                        ArtifactRole::Intermediate,
                        serde_json::to_vec(&json!({ "version": 1, "completed": [1] })).unwrap(),
                    ))
                    .await?;
                ctx.checkpoint().commit(&m1.id, None).await?;
                assert_eq!(
                    ctx.checkpoint().current().await?.as_deref(),
                    Some(m1.id.as_str())
                );

                // Publish manifest v2 nhưng bật fault CAS -> pointer phải giữ nguyên ở m1
                let m2 = ctx
                    .artifacts()
                    .put(ArtifactInput::from_bytes(
                        "manifest-v2.json",
                        "application/json",
                        ArtifactRole::Intermediate,
                        serde_json::to_vec(&json!({ "version": 2, "completed": [1, 2] })).unwrap(),
                    ))
                    .await?;
                self.fault.set_fail_checkpoint_cas(true);
                let cas_err = ctx
                    .checkpoint()
                    .commit(&m2.id, Some(&m1.id))
                    .await
                    .expect_err("faulted CAS must fail");
                self.fault.set_fail_checkpoint_cas(false);
                assert_eq!(cas_err.code, "ARTIFACT_STORE_UNAVAILABLE");
                assert_eq!(
                    ctx.checkpoint().current().await?.as_deref(),
                    Some(m1.id.as_str()),
                    "failed CAS must leave pointer at m1"
                );

                // Stale expected_previous_id (`None` khi pointer đang là `m1`) -> `CHECKPOINT_CONFLICT`
                let conflict_err = ctx
                    .checkpoint()
                    .commit(&m2.id, None)
                    .await
                    .expect_err("stale expected_previous_id must conflict");
                assert_eq!(conflict_err.code, "CHECKPOINT_CONFLICT");

                // Commit đúng `expected_previous_id = Some(&m1.id)` -> thành công
                ctx.checkpoint().commit(&m2.id, Some(&m1.id)).await?;
                assert_eq!(
                    ctx.checkpoint().current().await?.as_deref(),
                    Some(m2.id.as_str())
                );

                Ok(ToolOutcome {
                    call_id: ctx.scope().tool_call_id.clone(),
                    content: format!("committed_v2:{}", m2.id),
                    metadata: json!({
                        "artifact_ids": [m1.id, m2.id],
                        "checkpoint_artifact_id": m2.id,
                    }),
                })
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn at47_fault_windows_cas_conflicts_and_startup_orphan_sweep() {
    let env = Env::new();
    let mut cfg = env.config_with_tools(vec!["checkpoint_fault_tool"], vec![]);
    cfg.runtime.artifact_staging_grace = Duration::from_millis(20);

    let fault = ArtifactFault::new();
    let mode = Arc::new(Mutex::new("fail_staging".to_string()));
    let gw = ScriptedGateway::new();

    // 1) Lỗi khi đang staging
    gw.push_tool_calls(
        "",
        vec![("c1", "checkpoint_fault_tool", json!({ "step": "1" }))],
    );
    // 2) Lỗi sau khi rename nhưng trước khi ghi SQLite metadata (tạo file orphan trong objects/)
    let mode_c2 = mode.clone();
    gw.push_with(move |_| {
        *mode_c2.lock().unwrap() = "fail_after_rename".into();
        ModelResponse {
            text: String::new(),
            finish_reason: "tool_use".into(),
            tool_calls: vec![ModelToolCall {
                id: "c2".into(),
                name: "checkpoint_fault_tool".into(),
                arguments: json!({ "step": "2" }),
            }],
            usage: None,
        }
    });
    // 3) CAS thành công v1, lỗi trước CAS v2, conflict với stale pointer, rồi CAS thành công v2
    let mode_c3 = mode.clone();
    gw.push_with(move |_| {
        *mode_c3.lock().unwrap() = "cas_and_conflict".into();
        ModelResponse {
            text: String::new(),
            finish_reason: "tool_use".into(),
            tool_calls: vec![ModelToolCall {
                id: "c3".into(),
                name: "checkpoint_fault_tool".into(),
                arguments: json!({ "step": "3" }),
            }],
            usage: None,
        }
    });
    gw.push_text("at47 complete");

    let o = Orchestrator::builder(cfg.clone())
        .with_gateway(gw.clone())
        .with_artifact_fault(fault.clone())
        .register_tool(Arc::new(CheckpointFaultTool {
            fault: fault.clone(),
            mode: mode.clone(),
        }))
        .build()
        .await
        .unwrap();

    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o.submit(&alice(), &sid, "run at47".into()).await.unwrap();
    let res = wait_terminal(&o, &alice(), &sid, &turn.turn_id).await;
    assert_eq!(res.status, RunStatus::Completed);
    assert_eq!(
        res.artifact_preview.len(),
        2,
        "only the 2 committed manifests exist in SQLite metadata"
    );

    // Tạo thêm một file mồ côi trong `artifacts/staging/dangling.part` để kiểm chứng startup sweep
    let staging_orphan = env
        .store()
        .join("artifacts")
        .join("staging")
        .join("dangling.part");
    std::fs::write(&staging_orphan, b"dangling staging bytes").unwrap();
    assert!(staging_orphan.exists());

    // Đợi quá `artifact_staging_grace` (20ms) rồi shutdown + build lại Orchestrator để kích hoạt
    // startup orphan sweep dưới owner lock.
    tokio::time::sleep(Duration::from_millis(50)).await;
    o.shutdown().await.unwrap();

    let o2 = Orchestrator::builder(cfg)
        .with_gateway(gw)
        .register_tool(Arc::new(CheckpointFaultTool { fault, mode }))
        .build()
        .await
        .unwrap();
    assert!(
        !staging_orphan.exists(),
        "startup orphan sweep must remove expired staging file"
    );

    // Mọi committed artifact (manifest v1 và v2) phải còn nguyên vẹn và đọc được
    for art in &res.artifact_preview {
        let dl = o2.open_artifact(&alice(), &art.id).await.unwrap();
        let bytes = dl.read_all().await.unwrap();
        let val: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(val["version"].as_u64().is_some());
    }
    o2.shutdown().await.unwrap();
}

// ───────────────────────────── Quotas, limits & interrupted invocation ─────────────────────────────

#[tokio::test]
async fn quotas_limits_and_interrupted_invocation_artifact_validation() {
    let env = Env::new();
    let mut cfg = env.config_with_tools(vec!["produce_artifact"], vec![]);
    cfg.runtime.artifact_max_bytes = 32;
    cfg.runtime.owner_workspace_quota_bytes = 48;
    cfg.runtime.download_max_bytes = 32;

    let fault = ArtifactFault::new();
    let gw = ScriptedGateway::new();

    // 1) Vượt `artifact_max_bytes` (32 bytes) -> `ARTIFACT_TOO_LARGE`
    gw.push_tool_calls(
        "",
        vec![(
            "call-too-large",
            "produce_artifact",
            json!({
                "name": "big.txt",
                "body": "0123456789abcdef0123456789abcdef_EXTRA"
            }),
        )],
    );
    // 2) Ghi 1 artifact 28 bytes (hợp lệ)
    gw.push_tool_calls(
        "",
        vec![(
            "call-ok-1",
            "produce_artifact",
            json!({
                "name": "ok1.txt",
                "body": "0123456789abcdef0123456789ab"
            }),
        )],
    );
    // 3) Ghi tiếp 1 artifact 28 bytes -> tổng 56 > `owner_workspace_quota_bytes` (48) -> `ARTIFACT_QUOTA_EXCEEDED`
    gw.push_tool_calls(
        "",
        vec![(
            "call-quota",
            "produce_artifact",
            json!({
                "name": "ok2.txt",
                "body": "0123456789abcdef0123456789ab"
            }),
        )],
    );
    gw.push_text("quota checks done");

    let o = Orchestrator::builder(cfg.clone())
        .with_gateway(gw.clone())
        .with_artifact_fault(fault.clone())
        .register_tool(Arc::new(ProduceArtifactTool))
        .build()
        .await
        .unwrap();

    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o
        .submit(&alice(), &sid, "test limits".into())
        .await
        .unwrap();
    let res = wait_terminal(&o, &alice(), &sid, &turn.turn_id).await;
    assert_eq!(res.status, RunStatus::Completed);
    assert_eq!(res.artifact_preview.len(), 1);
    let committed_id = res.artifact_preview[0].id.clone();

    let hist = o
        .history(&alice(), &sid, PageRequest::first(50))
        .await
        .unwrap();
    let tool_msgs: Vec<&str> = hist
        .items
        .iter()
        .filter(|m| m.role == MessageRole::Tool)
        .map(|m| m.text.as_str())
        .collect();
    assert!(
        tool_msgs.iter().any(|m| m.contains("ARTIFACT_TOO_LARGE")),
        "expected ARTIFACT_TOO_LARGE in {tool_msgs:?}"
    );
    assert!(
        tool_msgs
            .iter()
            .any(|m| m.contains("WORKSPACE_QUOTA_EXCEEDED")),
        "expected WORKSPACE_QUOTA_EXCEEDED in {tool_msgs:?}"
    );

    // 4) Kiểm tra `disk_reserve_bytes` qua simulated available disk bytes = 0
    fault.set_available_disk_bytes(Some(0));
    gw.push_tool_calls(
        "",
        vec![(
            "call-disk",
            "produce_artifact",
            json!({ "name": "tiny.txt", "body": "hi" }),
        )],
    );
    gw.push_text("disk reserve checked");
    let turn_disk = o.submit(&alice(), &sid, "test disk".into()).await.unwrap();
    wait_terminal(&o, &alice(), &sid, &turn_disk.turn_id).await;
    fault.set_available_disk_bytes(None);

    let hist_disk = o
        .history(&alice(), &sid, PageRequest::first(50))
        .await
        .unwrap();
    assert!(
        hist_disk
            .items
            .iter()
            .any(|m| m.role == MessageRole::Tool && m.text.contains("DISK_RESERVE_EXCEEDED")),
        "expected DISK_RESERVE_EXCEEDED when available disk < reserve"
    );

    // 5) Kiểm tra `resolve_interrupted_invocation` xác thực `metadata.artifact_ids`:
    // Giả lập một invocation `interrupted` trên session mới `sid_int`.
    let sid_int = o.create_session(&alice(), "root").await.unwrap();
    let fp = o.provider_fingerprint("fake").unwrap().to_string();
    o.shutdown().await.unwrap();

    let mut sql = env.sql().await;
    sqlx::query(
        "INSERT INTO messages(id, session_id, sequence, role, content_json)
         VALUES ('m-u', ?1, 1, 'user', '{\"text\":\"interrupt me\"}')",
    )
    .bind(&sid_int)
    .execute(&mut sql)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO agent_runs(
            id, session_id, agent_id, turn_id, resume_attempt,
            provider_id, model_id, provider_config_fingerprint, status, error_code
         ) VALUES (
            'r-int', ?1, 'root', 't-int', 0,
            'fake', 'model-x', ?2, 'interrupted', 'RECOVERY_BLOCKED'
         )",
    )
    .bind(&sid_int)
    .bind(&fp)
    .execute(&mut sql)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO messages(id, session_id, sequence, role, content_json, run_id)
         VALUES ('m-a', ?1, 2, 'assistant', '{\"text\":\"\",\"tool_calls\":[{\"id\":\"call-int\",\"name\":\"produce_artifact\",\"arguments\":{\"name\":\"x\",\"body\":\"y\"}}]}', 'r-int')",
    )
    .bind(&sid_int)
    .execute(&mut sql)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tool_invocations(
            id, session_id, run_id, tool_call_id, assistant_message_id,
            tool_name, arguments_json, status
         ) VALUES (
            'inv-int', ?1, 'r-int', 'call-int', 'm-a',
            'produce_artifact', '{\"name\":\"x\",\"body\":\"y\"}', 'interrupted'
         )",
    )
    .bind(&sid_int)
    .execute(&mut sql)
    .await
    .unwrap();
    sqlx::query("UPDATE sessions SET next_sequence = 3 WHERE id = ?1")
        .bind(&sid_int)
        .execute(&mut sql)
        .await
        .unwrap();
    drop(sql);

    let gw2 = ScriptedGateway::new();
    gw2.push_text("resumed after verified resolution");
    let o2 = Orchestrator::builder(cfg)
        .with_gateway(gw2)
        .register_tool(Arc::new(ProduceArtifactTool))
        .build()
        .await
        .unwrap();

    let op = OperatorContext::new("op-1", "alice", "ws1").with_resolve_capability();
    // 5a) Truyền `artifact_ids` không tồn tại -> phải bị từ chối với `InvalidInput`
    let bad_resolve = o2
        .resolve_interrupted_invocation(
            &op,
            &sid_int,
            "inv-int",
            VerifiedToolResolution::ConfirmedCompleted {
                outcome: ToolOutcome {
                    call_id: "call-int".into(),
                    content: "done".into(),
                    metadata: json!({ "artifact_ids": ["non-existent-art"] }),
                },
                evidence: "verify non-existent".into(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(bad_resolve.code(), ErrorCode::InvalidInput);

    // 5b) Truyền `committed_id` của session `sid` (cùng workspace nhưng khác session family) -> từ chối
    let cross_family_resolve = o2
        .resolve_interrupted_invocation(
            &op,
            &sid_int,
            "inv-int",
            VerifiedToolResolution::ConfirmedCompleted {
                outcome: ToolOutcome {
                    call_id: "call-int".into(),
                    content: "done".into(),
                    metadata: json!({ "artifact_ids": [committed_id] }),
                },
                evidence: "verify cross family".into(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(cross_family_resolve.code(), ErrorCode::InvalidInput);

    o2.shutdown().await.unwrap();
}

// ───────────────────────────── Consistent backup / restore ─────────────────────────────

#[tokio::test]
async fn consistent_backup_and_restore_preserves_sqlite_and_artifacts() {
    let env = Env::new();
    let cfg = env.config_with_tools(vec!["produce_artifact"], vec![]);
    let gw = ScriptedGateway::new();
    gw.push_tool_calls(
        "",
        vec![(
            "call-b1",
            "produce_artifact",
            json!({
                "name": "backup-doc.md",
                "body": "# Canonical Backup Document\nVerified across restore."
            }),
        )],
    );
    gw.push_text("backup turn finished");

    let o = Orchestrator::builder(cfg.clone())
        .with_gateway(gw.clone())
        .register_tool(Arc::new(ProduceArtifactTool))
        .build()
        .await
        .unwrap();

    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o.submit(&alice(), &sid, "create doc".into()).await.unwrap();
    let orig_res = wait_terminal(&o, &alice(), &sid, &turn.turn_id).await;
    assert_eq!(orig_res.status, RunStatus::Completed);
    assert_eq!(orig_res.artifact_preview.len(), 1);
    let art_ref = &orig_res.artifact_preview[0];

    // Tạo một staging file rác trước khi backup để chứng minh `backup_to` không copy file staging rác
    let staging_junk = env
        .store()
        .join("artifacts")
        .join("staging")
        .join("junk.part");
    std::fs::write(&staging_junk, b"uncommitted").unwrap();

    // Thực hiện `backup_to` ngay khi Orchestrator (và SQLite WAL) đang mở
    let backup_dir = env.dir.path().join("backup_snapshot");
    o.backup_to(&backup_dir).await.unwrap();
    assert!(backup_dir.join("orchestrator.db").exists());
    assert!(
        !backup_dir
            .join("artifacts")
            .join("staging")
            .join("junk.part")
            .exists(),
        "backup_to must not copy uncommitted staging files"
    );

    o.shutdown().await.unwrap();

    // Khởi tạo một Orchestrator hoàn toàn mới trỏ trực tiếp vào `backup_dir`
    let mut restored_cfg = cfg;
    restored_cfg.store_dir = backup_dir;
    let o_restored = Orchestrator::builder(restored_cfg)
        .with_gateway(gw)
        .register_tool(Arc::new(ProduceArtifactTool))
        .build()
        .await
        .unwrap();

    let restored_res = o_restored
        .result(&alice(), &sid, &turn.turn_id)
        .await
        .unwrap();
    assert_eq!(restored_res.status, RunStatus::Completed);
    assert_eq!(restored_res.output, orig_res.output);
    assert_eq!(restored_res.artifact_preview, orig_res.artifact_preview);

    let dl = o_restored
        .open_artifact(&alice(), &art_ref.id)
        .await
        .unwrap();
    let bytes = dl.read_all().await.unwrap();
    assert_eq!(
        bytes,
        b"# Canonical Backup Document\nVerified across restore."
    );
    o_restored.shutdown().await.unwrap();
}

// ───────────────────────────── Workload Fixture 1: Playlist 20 items ─────────────────────────────

struct PlaylistBatchTool {
    /// Nếu `Some(k)`, dừng sớm sau khi commit item `k` (giả lập gián đoạn giữa chừng).
    stop_after_item: Arc<Mutex<Option<usize>>>,
    /// Đếm số lần thực sự chạy ASR cho từng item `1..=20` để chứng minh không chạy lại item đã xong.
    asr_executions: Arc<Mutex<HashMap<usize, usize>>>,
}

#[async_trait::async_trait]
impl HostTool for PlaylistBatchTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "process_playlist".into(),
            description: "Process a 20-video playlist with per-item artifacts and CAS manifest"
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "total_items": { "type": "integer" },
                    "resume_manifest_id": { "type": "string" }
                },
                "required": ["total_items"]
            }),
            effect: ToolEffect::Mutating,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        let total = args["total_items"].as_u64().unwrap_or(20) as usize;
        let stop_at = *self.stop_after_item.lock().unwrap();

        // Nếu được truyền `resume_manifest_id`, đọc manifest từ `ArtifactStore`, verify từng
        // completed item artifact, rồi CAS-adopt manifest đó làm baseline cho invocation mới
        // (docs/17 §17.7: `expected_previous_id = None` vì invocation mới bắt đầu với `current() == None`).
        let mut completed: HashMap<usize, Value> = HashMap::new();
        let mut failed: HashMap<usize, String> = HashMap::new();
        let mut prev_manifest_id: Option<String> = ctx.checkpoint().current().await?;

        if let Some(resume_id) = args["resume_manifest_id"].as_str() {
            let reader = ctx.artifacts().open(resume_id).await?;
            let buf = reader.read_all().await?;
            let manifest: Value = serde_json::from_slice(&buf).unwrap();

            // CAS-commit `resume_id` làm checkpoint hiện tại của invocation trước để mở quyền
            // đọc các per-item artifacts được tham chiếu bên trong manifest đó!
            ctx.checkpoint()
                .commit(resume_id, prev_manifest_id.as_deref())
                .await?;
            prev_manifest_id = Some(resume_id.to_string());

            if let Some(items) = manifest["items"].as_array() {
                for item in items {
                    let idx = item["index"].as_u64().unwrap() as usize;
                    let status = item["status"].as_str().unwrap_or("");
                    if status == "completed" {
                        let t_id = item["transcript_artifact_id"].as_str().unwrap();
                        let a_id = item["analysis_artifact_id"].as_str().unwrap();
                        // Verify cả hai artifact của item đã hoàn thành đều mở và khớp SHA-256
                        let r1 = ctx.artifacts().open(t_id).await?;
                        let _ = r1.read_all().await?;
                        let r2 = ctx.artifacts().open(a_id).await?;
                        let _ = r2.read_all().await?;
                        completed.insert(idx, item.clone());
                    }
                }
            }
        }

        let mut all_artifact_ids = Vec::new();
        for idx in 1..=total {
            if completed.contains_key(&idx) {
                continue;
            }
            ctx.progress_with_percent(
                &format!("processing video {idx}/{total}"),
                ((idx * 100) / total) as u8,
            );
            *self.asr_executions.lock().unwrap().entry(idx).or_insert(0) += 1;

            // Hai video #7 và #15 bị lỗi ASR xác định
            if idx == 7 || idx == 15 {
                failed.insert(idx, format!("ASR_CORRUPT_AUDIO_TRACK at item {idx}"));
            } else {
                let transcript = ctx
                    .artifacts()
                    .put(ArtifactInput::from_bytes(
                        format!("video-{idx:02}-transcript.txt"),
                        "text/plain",
                        ArtifactRole::Intermediate,
                        format!("Transcript of video #{idx}: rust orchestrator m7.").into_bytes(),
                    ))
                    .await?;
                let analysis = ctx
                    .artifacts()
                    .put(ArtifactInput::from_bytes(
                        format!("video-{idx:02}-analysis.md"),
                        "text/markdown",
                        ArtifactRole::Primary,
                        format!(
                            "# Analysis #{idx}\nKey takeaway from transcript `{}`.",
                            transcript.id
                        )
                        .into_bytes(),
                    ))
                    .await?;
                all_artifact_ids.push(transcript.id.clone());
                all_artifact_ids.push(analysis.id.clone());
                completed.insert(
                    idx,
                    json!({
                        "index": idx,
                        "status": "completed",
                        "transcript_artifact_id": transcript.id,
                        "analysis_artifact_id": analysis.id,
                    }),
                );
            }

            // Ghi versioned `playlist-manifest.json` và nâng pointer qua CAS
            let mut items_vec = Vec::new();
            for i in 1..=total {
                if let Some(c) = completed.get(&i) {
                    items_vec.push(c.clone());
                } else if let Some(err) = failed.get(&i) {
                    items_vec.push(json!({
                        "index": i,
                        "status": "failed",
                        "error": err,
                    }));
                } else {
                    items_vec.push(json!({
                        "index": i,
                        "status": "pending",
                    }));
                }
            }
            let manifest_json = json!({
                "total_items": total,
                "completed_count": completed.len(),
                "failed_count": failed.len(),
                "items": items_vec,
            });
            let manifest_art = ctx
                .artifacts()
                .put(ArtifactInput::from_bytes(
                    "playlist-manifest.json",
                    "application/json",
                    ArtifactRole::Primary,
                    serde_json::to_vec_pretty(&manifest_json).unwrap(),
                ))
                .await?;
            ctx.checkpoint()
                .commit(&manifest_art.id, prev_manifest_id.as_deref())
                .await?;
            prev_manifest_id = Some(manifest_art.id.clone());
            all_artifact_ids.push(manifest_art.id);

            if stop_at == Some(idx) {
                return Err(tool_err(
                    "INTERRUPTED_BATCH",
                    format!(
                        "INTERRUPTED_AFTER_ITEM_{idx}: checkpoint={}",
                        prev_manifest_id.as_deref().unwrap_or("")
                    ),
                ));
            }
        }

        let final_manifest_id = prev_manifest_id.unwrap();
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: serde_json::to_string(&json!({
                "manifest_artifact_id": final_manifest_id,
                "completed_count": completed.len(),
                "failed_count": failed.len(),
                "failed_items": [7, 15],
            }))
            .unwrap(),
            metadata: json!({
                "artifact_ids": all_artifact_ids,
                "checkpoint_artifact_id": final_manifest_id,
                "completed_count": completed.len(),
                "failed_count": failed.len(),
            }),
        })
    }
}

#[tokio::test]
async fn workload_1_playlist_20_items_partial_failure_and_checkpoint_resume() {
    let env = Env::new();
    let cfg = env.config_with_tools(vec!["subagent"], vec!["process_playlist"]);

    let stop_after = Arc::new(Mutex::new(Some(10usize)));
    let asr_executions = Arc::new(Mutex::new(HashMap::new()));
    let playlist_tool = Arc::new(PlaylistBatchTool {
        stop_after_item: stop_after.clone(),
        asr_executions: asr_executions.clone(),
    });

    let gw = ScriptedGateway::new();
    // 1) Root uỷ quyền cho child `worker` chạy playlist 20 video
    gw.push_tool_calls(
        "",
        vec![(
            "call-sub-1",
            "subagent",
            json!({
                "agent": "worker",
                "description": "process 20-video playlist",
                "prompt": "Process the 20-video playlist and report per-item outcomes."
            }),
        )],
    );
    // 2) Child lần 1 gọi `process_playlist` nhưng bị gián đoạn ở item 10 (sau khi đã commit
    //    manifest chứa item 1..=10, trong đó item 7 lỗi ASR).
    gw.push_tool_calls(
        "",
        vec![(
            "call-pl-1",
            "process_playlist",
            json!({ "total_items": 20 }),
        )],
    );
    // 3) Child nhận lỗi gián đoạn kèm checkpoint ID, gỡ `stop_after` và gọi lại `process_playlist`
    //    với `resume_manifest_id` để chạy nốt item 11..=20 mà không chạy lại các item đã hoàn thành!
    let stop_after_clone = stop_after.clone();
    gw.push_with(move |req: &ModelRequest| {
        *stop_after_clone.lock().unwrap() = None;
        let last_tool = req
            .messages
            .iter()
            .rev()
            .find(|m| m.role == ModelRole::Tool)
            .unwrap();
        let ckpt = last_tool
            .text
            .split("checkpoint=")
            .nth(1)
            .unwrap()
            .trim()
            .trim_end_matches('"')
            .trim_end_matches('}')
            .to_string();
        ModelResponse {
            text: String::new(),
            finish_reason: "tool_use".into(),
            tool_calls: vec![ModelToolCall {
                id: "call-pl-2".into(),
                name: "process_playlist".into(),
                arguments: json!({
                    "total_items": 20,
                    "resume_manifest_id": ckpt
                }),
            }],
            usage: None,
        }
    });
    // 4) Child hoàn tất và báo cáo kết quả cho Root
    gw.push_with(|req: &ModelRequest| {
        let last_tool = req
            .messages
            .iter()
            .rev()
            .find(|m| m.role == ModelRole::Tool)
            .unwrap();
        ModelResponse {
            text: format!("Worker playlist summary: {}", last_tool.text),
            finish_reason: "end_turn".into(),
            tool_calls: Vec::new(),
            usage: None,
        }
    });
    // 5) Root tổng hợp cho user: 18/20 thành công, 2 video (#7, #15) lỗi ASR
    gw.push_text("Playlist processed: 18/20 succeeded, 2 failed (items 7 and 15 due to ASR).");

    let o = Orchestrator::builder(cfg)
        .with_gateway(gw)
        .register_tool(playlist_tool)
        .build()
        .await
        .unwrap();

    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o
        .submit(&alice(), &sid, "analyze 20 videos".into())
        .await
        .unwrap();
    let res = wait_terminal(&o, &alice(), &sid, &turn.turn_id).await;

    assert_eq!(res.status, RunStatus::Completed);
    assert!(res
        .output
        .as_deref()
        .unwrap()
        .contains("18/20 succeeded, 2 failed"));
    // `artifact_preview` của Root turn hiển thị các artifacts từ child turn (giới hạn mặc định 20)
    assert_eq!(res.artifact_preview.len(), 20);
    assert!(res.artifacts_next_cursor.is_some());

    // Kiểm chứng các item 1..=10 đã completed (trừ item 7 bị failed ở lần 1) chỉ chạy ASR đúng 1 lần!
    let execs = asr_executions.lock().unwrap().clone();
    for idx in 1..=20 {
        if idx == 7 {
            // Item 7 failed ở lần 1, không nằm trong `completed` nên khi resume được thử lại 1 lần nữa
            assert_eq!(execs.get(&idx).copied(), Some(2));
        } else {
            assert_eq!(
                execs.get(&idx).copied(),
                Some(1),
                "completed video #{idx} must not be re-transcribed on resume"
            );
        }
    }

    // Đọc manifest cuối cùng qua `list_artifacts` + `open_artifact` để chứng minh `RunStatus::Completed`
    // không che giấu 2 item lỗi (#7 và #15).
    let mut all_arts = Vec::new();
    let mut req_page = PageRequest::first(50);
    loop {
        let page = o
            .list_artifacts(&alice(), &sid, &turn.turn_id, req_page)
            .await
            .unwrap();
        all_arts.extend(page.items);
        let Some(c) = page.next_cursor else {
            break;
        };
        req_page = PageRequest::after(c, 50);
    }
    let last_manifest = all_arts
        .iter()
        .rev()
        .find(|a| a.name == "playlist-manifest.json")
        .expect("playlist-manifest.json must be published");
    let dl = o.open_artifact(&alice(), &last_manifest.id).await.unwrap();
    let manifest_val: Value = serde_json::from_slice(&dl.read_all().await.unwrap()).unwrap();
    assert_eq!(manifest_val["completed_count"], 18);
    assert_eq!(manifest_val["failed_count"], 2);

    o.shutdown().await.unwrap();
}

// ───────────────────────────── Workload Fixture 2: Document conversion + Knowledge ─────────────────────────────

struct DocConvertTool;

#[async_trait::async_trait]
impl HostTool for DocConvertTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "doc_convert".into(),
            description: "Convert PDF/DOCX into document.md and document-summary.md".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "source_name": { "type": "string" } },
                "required": ["source_name"]
            }),
            effect: ToolEffect::Mutating,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        let src = args["source_name"].as_str().unwrap_or("spec.pdf");
        let doc_md = ctx
            .artifacts()
            .put(ArtifactInput::from_bytes(
                "document.md",
                "text/markdown",
                ArtifactRole::Primary,
                format!("# Converted from {src}\n\nFull normalized markdown content.").into_bytes(),
            ))
            .await?;
        let summary_md = ctx
            .artifacts()
            .put(ArtifactInput::from_bytes(
                "document-summary.md",
                "text/markdown",
                ArtifactRole::Primary,
                format!("Summary of {src}: 1 section, verified.").into_bytes(),
            ))
            .await?;
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: serde_json::to_string(&json!({
                "document_artifact_id": doc_md.id,
                "summary_artifact_id": summary_md.id,
            }))
            .unwrap(),
            metadata: json!({
                "artifact_ids": [doc_md.id, summary_md.id],
            }),
        })
    }
}

struct KnowledgeIngestTool {
    attempts: Arc<AtomicUsize>,
    receipts_by_key: Arc<Mutex<HashMap<String, String>>>,
}

#[async_trait::async_trait]
impl HostTool for KnowledgeIngestTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "knowledge_ingest".into(),
            description: "Ingest converted document artifact into external Knowledge Base".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "document_artifact_id": { "type": "string" },
                    "idempotency_key": { "type": "string" }
                },
                "required": ["document_artifact_id", "idempotency_key"]
            }),
            effect: ToolEffect::Mutating,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        let doc_id = args["document_artifact_id"].as_str().unwrap();
        let idem_key = args["idempotency_key"].as_str().unwrap();

        // Đọc và verify `document.md` qua `ToolContext::artifacts().open`
        let reader = ctx.artifacts().open(doc_id).await?;
        let bytes = reader.read_all().await?;
        assert!(!bytes.is_empty());

        let n = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if n == 1 {
            // Lần 1: Knowledge service trả `pending` -> KHÔNG tạo local artifact giả làm KB receipt!
            return Ok(ToolOutcome {
                call_id: ctx.scope().tool_call_id.clone(),
                content: serde_json::to_string(&json!({
                    "ingest_status": "pending",
                    "idempotency_key": idem_key,
                    "knowledge_document_id": Value::Null,
                }))
                .unwrap(),
                metadata: json!({
                    "ingest_status": "pending",
                    "idempotency_key": idem_key,
                }),
            });
        }
        if n == 2 {
            // Lần 2: Knowledge service trả lỗi tạm thời -> KHÔNG tạo receipt giả
            return Err(tool_err(
                "KNOWLEDGE_UPSTREAM_TIMEOUT",
                "Knowledge service timed out before indexing",
            ));
        }

        // Lần 3: Reconcile theo cùng `idempotency_key` thành công, trả `knowledge_document_id` thật
        let kb_id = self
            .receipts_by_key
            .lock()
            .unwrap()
            .entry(idem_key.to_string())
            .or_insert_with(|| "kb-doc-9001".to_string())
            .clone();
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: serde_json::to_string(&json!({
                "ingest_status": "indexed",
                "idempotency_key": idem_key,
                "knowledge_document_id": kb_id,
            }))
            .unwrap(),
            metadata: json!({
                "ingest_status": "indexed",
                "idempotency_key": idem_key,
                "knowledge_document_id": kb_id,
            }),
        })
    }
}

#[tokio::test]
async fn workload_2_document_conversion_and_knowledge_ingest_without_fake_receipt() {
    let env = Env::new();
    let cfg = env.config_with_tools(vec!["doc_convert", "knowledge_ingest"], vec![]);

    let ingest_attempts = Arc::new(AtomicUsize::new(0));
    let receipts = Arc::new(Mutex::new(HashMap::new()));
    let gw = ScriptedGateway::new();

    // 1) Convert `architecture.pdf` -> `document.md` + `document-summary.md`
    gw.push_tool_calls(
        "",
        vec![(
            "call-conv",
            "doc_convert",
            json!({ "source_name": "architecture.pdf" }),
        )],
    );

    let doc_id_holder = Arc::new(Mutex::new(String::new()));
    let holder_1 = doc_id_holder.clone();
    // 2) Gọi `knowledge_ingest` lần 1 (nhận `pending`)
    gw.push_with(move |req: &ModelRequest| {
        let last_tool = req
            .messages
            .iter()
            .rev()
            .find(|m| m.role == ModelRole::Tool)
            .unwrap();
        let parsed: Value = serde_json::from_str(&last_tool.text).unwrap();
        let doc_id = parsed["document_artifact_id"].as_str().unwrap().to_string();
        *holder_1.lock().unwrap() = doc_id.clone();
        ModelResponse {
            text: String::new(),
            finish_reason: "tool_use".into(),
            tool_calls: vec![ModelToolCall {
                id: "call-kb-1".into(),
                name: "knowledge_ingest".into(),
                arguments: json!({
                    "document_artifact_id": doc_id,
                    "idempotency_key": "idem-arch-pdf-v1"
                }),
            }],
            usage: None,
        }
    });
    // 3) Gọi `knowledge_ingest` lần 2 (nhận lỗi `KNOWLEDGE_UPSTREAM_TIMEOUT`)
    let holder_2 = doc_id_holder.clone();
    gw.push_with(move |_| {
        let doc_id = holder_2.lock().unwrap().clone();
        ModelResponse {
            text: String::new(),
            finish_reason: "tool_use".into(),
            tool_calls: vec![ModelToolCall {
                id: "call-kb-2".into(),
                name: "knowledge_ingest".into(),
                arguments: json!({
                    "document_artifact_id": doc_id,
                    "idempotency_key": "idem-arch-pdf-v1"
                }),
            }],
            usage: None,
        }
    });
    // 4) Gọi `knowledge_ingest` lần 3 cùng `idempotency_key` -> `indexed` với `kb-doc-9001`
    let holder_3 = doc_id_holder.clone();
    gw.push_with(move |_| {
        let doc_id = holder_3.lock().unwrap().clone();
        ModelResponse {
            text: String::new(),
            finish_reason: "tool_use".into(),
            tool_calls: vec![ModelToolCall {
                id: "call-kb-3".into(),
                name: "knowledge_ingest".into(),
                arguments: json!({
                    "document_artifact_id": doc_id,
                    "idempotency_key": "idem-arch-pdf-v1"
                }),
            }],
            usage: None,
        }
    });
    gw.push_text("Document converted and indexed as kb-doc-9001.");

    let o = Orchestrator::builder(cfg)
        .with_gateway(gw)
        .register_tool(Arc::new(DocConvertTool))
        .register_tool(Arc::new(KnowledgeIngestTool {
            attempts: ingest_attempts.clone(),
            receipts_by_key: receipts.clone(),
        }))
        .build()
        .await
        .unwrap();

    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o
        .submit(&alice(), &sid, "convert and ingest architecture.pdf".into())
        .await
        .unwrap();
    let res = wait_terminal(&o, &alice(), &sid, &turn.turn_id).await;
    assert_eq!(res.status, RunStatus::Completed);
    assert_eq!(ingest_attempts.load(Ordering::SeqCst), 3);

    // Chỉ có đúng 2 local artifacts (`document.md` và `document-summary.md`), KHÔNG có fake
    // local artifact nào được tạo cho Knowledge Base receipt!
    assert_eq!(res.artifact_preview.len(), 2);
    let names: Vec<&str> = res
        .artifact_preview
        .iter()
        .map(|a| a.name.as_str())
        .collect();
    assert_eq!(names, vec!["document.md", "document-summary.md"]);

    o.shutdown().await.unwrap();
}

// ───────────────────────────── Workload Fixture 3: Media generation ─────────────────────────────

struct MediaGenerateTool {
    jobs_created: Arc<AtomicUsize>,
    crash_after_accept: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl HostTool for MediaGenerateTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "media_generate".into(),
            description: "Submit or poll an async media generation job and download artifact"
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "prompt": { "type": "string" },
                    "existing_job_state_artifact_id": { "type": "string" }
                },
                "required": ["prompt"]
            }),
            effect: ToolEffect::Mutating,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        // Nếu đã có job state artifact từ lần trước (truyền vào để reconcile sau crash),
        // đọc `provider_job_id` đã lưu thay vì tạo job mới trên upstream!
        if let Some(state_art_id) = args["existing_job_state_artifact_id"].as_str() {
            let reader = ctx.artifacts().open(state_art_id).await?;
            let buf = reader.read_all().await?;
            let state: Value = serde_json::from_slice(&buf).unwrap();
            let job_id = state["provider_job_id"].as_str().unwrap();

            // Tải kết quả từ job_id cũ qua streaming reader và publish thành Primary artifact
            let media_bytes = format!("PNG_BINARY_PAYLOAD_FOR_{job_id}").into_bytes();
            let media_art = ctx
                .artifacts()
                .put(ArtifactInput::from_reader(
                    "generated-banner.png",
                    "image/png",
                    ArtifactRole::Primary,
                    std::io::Cursor::new(media_bytes),
                ))
                .await?;
            return Ok(ToolOutcome {
                call_id: ctx.scope().tool_call_id.clone(),
                content: serde_json::to_string(&json!({
                    "job_status": "completed",
                    "provider_job_id": job_id,
                    "media_artifact_id": media_art.id,
                }))
                .unwrap(),
                metadata: json!({
                    "artifact_ids": [media_art.id],
                    "provider_job_id": job_id,
                }),
            });
        }

        // Lần đầu: đăng ký job với upstream, ghi durable job state manifest + CAS checkpoint
        let job_num = self.jobs_created.fetch_add(1, Ordering::SeqCst) + 1;
        let job_id = format!("media-job-{job_num:04}");
        let job_state = ctx
            .artifacts()
            .put(ArtifactInput::from_bytes(
                "media-job-state.json",
                "application/json",
                ArtifactRole::Intermediate,
                serde_json::to_vec(&json!({
                    "job_status": "accepted",
                    "provider_job_id": job_id,
                }))
                .unwrap(),
            ))
            .await?;
        ctx.checkpoint().commit(&job_state.id, None).await?;

        if self.crash_after_accept.load(Ordering::SeqCst) {
            // Mô phỏng crash/panic khi invocation Mutating đang in-flight nhưng đã kịp
            // persist `checkpoint_artifact_id` chứa `provider_job_id`! Executor bắt panic
            // và đánh dấu invocation/run là `interrupted` (`TOOL_OUTCOME_UNKNOWN`), chờ
            // operator reconcile mà không tự replay.
            panic!("simulated crash after persisting accepted media job checkpoint");
        }

        unreachable!("first call in test always crashes after accept")
    }
}

#[tokio::test]
async fn workload_3_media_generation_persists_job_id_across_crash_and_reconciles() {
    let env = Env::new();
    let cfg = env.config_with_tools(vec!["media_generate"], vec![]);

    let jobs_created = Arc::new(AtomicUsize::new(0));
    let crash_after_accept = Arc::new(AtomicBool::new(true));
    let media_tool = Arc::new(MediaGenerateTool {
        jobs_created: jobs_created.clone(),
        crash_after_accept: crash_after_accept.clone(),
    });

    let gw = ScriptedGateway::new();
    gw.push_tool_calls(
        "",
        vec![(
            "call-media-1",
            "media_generate",
            json!({ "prompt": "A futuristic rust crab banner" }),
        )],
    );

    let o = Orchestrator::builder(cfg.clone())
        .with_gateway(gw)
        .register_tool(media_tool.clone())
        .build()
        .await
        .unwrap();

    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o
        .submit(&alice(), &sid, "generate banner".into())
        .await
        .unwrap();

    // Đợi tới khi `media_generate` đã tạo đúng 1 job trên upstream và commit `checkpoint_artifact_id`
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let (inv_id, ckpt_id) = loop {
        let mut sql = env.sql().await;
        let row = sqlx::query(
            "SELECT id, checkpoint_artifact_id FROM tool_invocations
             WHERE session_id = ? AND checkpoint_artifact_id IS NOT NULL",
        )
        .bind(&sid)
        .fetch_optional(&mut sql)
        .await
        .unwrap();
        if let Some(r) = row {
            let id: String = r.get("id");
            let ckpt: String = r.get("checkpoint_artifact_id");
            break (id, ckpt);
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(15)).await;
    };
    assert_eq!(jobs_created.load(Ordering::SeqCst), 1);

    // Khi `o.shutdown()` chạy trong lúc `media_generate` (Mutating) đang `running`, engine
    // sẽ ghi nhận invocation `inv_id` ở trạng thái `interrupted` và giữ nguyên `checkpoint_artifact_id`!
    o.shutdown().await.unwrap();

    // Khởi động Orchestrator 2 sau crash: vì `inv_id` là Mutating `interrupted`, startup recovery
    // đánh dấu run `Interrupted` (`RECOVERY_BLOCKED`) chờ operator xác minh.
    crash_after_accept.store(false, Ordering::SeqCst);
    let gw2 = ScriptedGateway::new();
    let ckpt_for_resume = ckpt_id.clone();
    gw2.push_tool_calls(
        "",
        vec![(
            "call-media-poll",
            "media_generate",
            json!({
                "prompt": "A futuristic rust crab banner",
                "existing_job_state_artifact_id": ckpt_for_resume,
            }),
        )],
    );
    gw2.push_text("Banner generated from existing job media-job-0001.");

    let o2 = Orchestrator::builder(cfg)
        .with_gateway(gw2)
        .register_tool(media_tool)
        .build()
        .await
        .unwrap();

    let interrupted_res = wait_terminal(&o2, &alice(), &sid, &turn.turn_id).await;
    assert_eq!(interrupted_res.status, RunStatus::Interrupted);

    // Operator kiểm tra `ckpt_id` đã lưu, resolve invocation bị gián đoạn, và để run tiếp tục
    let op = OperatorContext::new("op-media", "alice", "ws1").with_resolve_capability();
    o2.resolve_interrupted_invocation(
        &op,
        &sid,
        &inv_id,
        VerifiedToolResolution::ConfirmedCompleted {
            outcome: ToolOutcome {
                call_id: "call-media-1".into(),
                content: format!("job accepted before crash, state in {ckpt_id}"),
                metadata: json!({
                    "artifact_ids": [ckpt_id],
                    "provider_job_id": "media-job-0001"
                }),
            },
            evidence: "verified upstream job media-job-0001 exists".into(),
        },
    )
    .await
    .unwrap();

    let final_res = wait_terminal(&o2, &alice(), &sid, &turn.turn_id).await;
    assert_eq!(final_res.status, RunStatus::Completed);
    assert_eq!(
        jobs_created.load(Ordering::SeqCst),
        1,
        "must not submit a duplicate media generation job after restart"
    );

    // Kiểm tra artifact `generated-banner.png` đã được publish và verify SHA-256
    let banner = final_res
        .artifact_preview
        .iter()
        .find(|a| a.name == "generated-banner.png")
        .expect("generated-banner.png must be in artifact_preview");
    assert_eq!(banner.role, ArtifactRole::Primary);
    let dl = o2.open_artifact(&alice(), &banner.id).await.unwrap();
    assert_eq!(
        dl.read_all().await.unwrap(),
        b"PNG_BINARY_PAYLOAD_FOR_media-job-0001"
    );

    o2.shutdown().await.unwrap();
}

// ───────────────────────────── Long-running >100 steps + compaction + disconnect + restart ─────────────────────────────

struct StepArtifactTool;

#[async_trait::async_trait]
impl HostTool for StepArtifactTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "step_artifact".into(),
            description: "Write a step artifact and emit progress".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "step": { "type": "integer" } },
                "required": ["step"]
            }),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        let step = args["step"].as_u64().unwrap_or(0);
        ctx.progress(&format!("executing step {step}"));
        let role = if step.is_multiple_of(10) {
            ArtifactRole::Primary
        } else {
            ArtifactRole::Intermediate
        };
        let art = ctx
            .artifacts()
            .put(ArtifactInput::from_bytes(
                format!("step-{step:03}.txt"),
                "text/plain",
                role,
                format!("step-{step:03}-content-{}", "x".repeat(120)).into_bytes(),
            ))
            .await?;
        if step.is_multiple_of(5) {
            let prev = ctx.checkpoint().current().await?;
            ctx.checkpoint().commit(&art.id, prev.as_deref()).await?;
        }
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!("step {step} produced {} ({})", art.id, "y".repeat(120)),
            metadata: json!({ "artifact_ids": [art.id] }),
        })
    }
}

#[tokio::test]
async fn long_running_over_100_steps_with_compaction_disconnect_and_restart() {
    let env = Env::new();
    let mut cfg = env.config_with_tools(vec!["step_artifact"], vec![]);
    // Đặt context_limit_tokens nhỏ để kích hoạt active-turn compaction nhiều lần trong >100 bước
    cfg.runtime.context_limit_tokens = 900;
    cfg.runtime.compaction_threshold_percent = 60;
    cfg.runtime.compaction_keep_recent = 4;
    cfg.runtime.artifact_preview_limit = 15;

    let gw = ScriptedGateway::new();
    // Cung cấp sẵn các bản tóm tắt cho compaction
    for _ in 1..=30 {
        gw.push_summary(Ok(
            "Compacted summary checkpoint: steps progressing cleanly.",
        ));
    }
    // Kịch bản 55 vòng gọi tool (`55 * 2 = 110` model/tool steps > 100 steps)
    for step in 1..=55 {
        let call_id = format!("call-step-{step}");
        gw.push_response_streamed(
            vec![StreamEvent::TextDelta(format!("thinking step {step}..."))],
            ModelResponse {
                text: String::new(),
                finish_reason: "tool_use".into(),
                tool_calls: vec![ModelToolCall {
                    id: call_id,
                    name: "step_artifact".into(),
                    arguments: json!({ "step": step }),
                }],
                usage: None,
            },
        );
    }
    gw.push_text("Completed 55 tool steps (>100 model/tool steps) with compaction.");

    let o = Orchestrator::builder(cfg.clone())
        .with_gateway(gw.clone())
        .register_tool(Arc::new(StepArtifactTool))
        .build()
        .await
        .unwrap();

    let sid = o.create_session(&alice(), "root").await.unwrap();
    let mut sub1 = o.subscribe(&alice(), &sid).await.unwrap();
    let turn = o
        .submit(&alice(), &sid, "run >100 steps".into())
        .await
        .unwrap();

    // Đọc vài sự kiện từ `sub1` rồi chủ động drop `sub1` (mô phỏng client disconnect giữa chừng)
    let mut seen_events = 0;
    while seen_events < 6 {
        if let Some(FeedItem::Event(_)) = sub1.recv().await {
            seen_events += 1;
        }
    }
    drop(sub1);

    // Mở subscription mới + lấy snapshot trong khi turn vẫn đang chạy hoặc vừa xong
    let _sub2 = o.subscribe(&alice(), &sid).await.unwrap();
    let snap = o.snapshot(&alice(), &sid).await.unwrap();
    assert_eq!(snap.root.session_id, sid);

    let res = wait_terminal(&o, &alice(), &sid, &turn.turn_id).await;
    assert_eq!(res.status, RunStatus::Completed);
    assert_eq!(
        res.output.as_deref(),
        Some("Completed 55 tool steps (>100 model/tool steps) with compaction.")
    );
    assert_eq!(res.artifact_preview.len(), 15);
    assert!(res.artifacts_next_cursor.is_some());
    assert!(
        !gw.summary_requests().is_empty(),
        "active-turn compaction must have been triggered during >100 steps"
    );

    // Shutdown và restart sang Orchestrator mới để kiểm chứng canonical reconstruction sau restart
    o.shutdown().await.unwrap();
    let o2 = Orchestrator::builder(cfg)
        .with_gateway(gw)
        .register_tool(Arc::new(StepArtifactTool))
        .build()
        .await
        .unwrap();

    let res_after = o2.result(&alice(), &sid, &turn.turn_id).await.unwrap();
    assert_eq!(res_after.status, RunStatus::Completed);
    assert_eq!(res_after.output, res.output);
    assert_eq!(res_after.artifact_preview, res.artifact_preview);

    // Đếm tổng số artifacts của turn qua `list_artifacts` -> đúng 55 artifacts
    let mut total_arts = Vec::new();
    let mut req_page = PageRequest::first(25);
    loop {
        let page = o2
            .list_artifacts(&alice(), &sid, &turn.turn_id, req_page)
            .await
            .unwrap();
        total_arts.extend(page.items);
        let Some(c) = page.next_cursor else {
            break;
        };
        req_page = PageRequest::after(c, 25);
    }
    assert_eq!(total_arts.len(), 55);

    // Kiểm tra mở artifact đầu và cuối sau restart
    for target in [&total_arts[0], &total_arts[54]] {
        let dl = o2.open_artifact(&alice(), &target.id).await.unwrap();
        let b = dl.read_all().await.unwrap();
        assert_eq!(b.len() as u64, target.size_bytes);
    }

    // Kiểm tra compaction checkpoint đã bảo toàn `<preserved_artifact_refs>` trong SQLite
    let mut sql = env.sql().await;
    let compaction_json: String = sqlx::query_scalar(
        "SELECT content_json FROM messages WHERE session_id = ?1 AND role = 'compaction' \
         ORDER BY sequence DESC LIMIT 1",
    )
    .bind(&sid)
    .fetch_one(&mut sql)
    .await
    .unwrap();
    assert!(
        compaction_json.contains("<preserved_artifact_refs>"),
        "compaction checkpoint must preserve structured artifact references: {compaction_json}"
    );
    assert!(
        compaction_json.contains(&total_arts[0].id),
        "compaction checkpoint must retain early artifact IDs even after truncation"
    );
    drop(sql);

    o2.shutdown().await.unwrap();
}

struct FakeArtifactMetadataTool;

#[async_trait::async_trait]
impl HostTool for FakeArtifactMetadataTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "fake_meta_tool".into(),
            description: "Return a fabricated artifact_id in metadata.artifact_ids".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "fake_id": { "type": "string" } },
                "required": ["fake_id"]
            }),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        let fake_id = args["fake_id"].as_str().unwrap_or("art-fake");
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: "claimed artifact".into(),
            metadata: json!({ "artifact_ids": [fake_id] }),
        })
    }
}

#[tokio::test]
async fn builtin_artifact_read_and_normal_tool_outcome_artifact_verification() {
    let env = Env::new();
    let cfg = env.config_with_tools(
        vec!["produce_artifact", "fake_meta_tool", "artifact_read"],
        vec![],
    );
    let gw = ScriptedGateway::new();

    // 1) Tạo 1 artifact hợp lệ
    gw.push_tool_calls(
        "",
        vec![(
            "call-p1",
            "produce_artifact",
            json!({
                "name": "guide.md",
                "body": "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            }),
        )],
    );
    // 2) Đọc đoạn con bằng built-in `artifact_read` (`offset_bytes = 10, max_bytes = 8`)
    gw.push_with(|req: &ModelRequest| {
        let last_tool = req
            .messages
            .iter()
            .rev()
            .find(|m| m.role == ModelRole::Tool)
            .unwrap();
        let art_id = last_tool
            .text
            .strip_prefix("created artifact ")
            .unwrap()
            .trim()
            .to_string();
        ModelResponse {
            text: String::new(),
            finish_reason: "tool_use".into(),
            tool_calls: vec![ModelToolCall {
                id: "call-read-1".into(),
                name: "artifact_read".into(),
                arguments: json!({
                    "artifact_id": art_id,
                    "offset_bytes": 10,
                    "max_bytes": 8
                }),
            }],
            usage: None,
        }
    });
    // 3) Gọi `fake_meta_tool` bịa ra `artifact_ids: ["art-does-not-exist"]` -> phải bị từ chối
    gw.push_tool_calls(
        "",
        vec![(
            "call-fake-meta",
            "fake_meta_tool",
            json!({ "fake_id": "art-does-not-exist" }),
        )],
    );
    gw.push_text("verified artifact_read and outcome metadata guard");

    let o = Orchestrator::builder(cfg)
        .with_gateway(gw)
        .register_tool(Arc::new(ProduceArtifactTool))
        .register_tool(Arc::new(FakeArtifactMetadataTool))
        .build()
        .await
        .unwrap();

    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o
        .submit(&alice(), &sid, "test built-in read".into())
        .await
        .unwrap();
    let res = wait_terminal(&o, &alice(), &sid, &turn.turn_id).await;
    assert_eq!(res.status, RunStatus::Completed);

    let hist = o
        .history(&alice(), &sid, PageRequest::first(20))
        .await
        .unwrap();
    let tool_texts: Vec<&str> = hist
        .items
        .iter()
        .filter(|m| m.role == MessageRole::Tool)
        .map(|m| m.text.as_str())
        .collect();
    assert!(
        tool_texts.contains(&"ABCDEFGH"),
        "expected excerpt ABCDEFGH from built-in artifact_read in {tool_texts:?}"
    );
    assert!(
        tool_texts
            .iter()
            .any(|t| t.contains("ARTIFACT_NOT_FOUND")),
        "expected ARTIFACT_NOT_FOUND when HostTool fabricates metadata.artifact_ids: {tool_texts:?}"
    );

    o.shutdown().await.unwrap();
}
