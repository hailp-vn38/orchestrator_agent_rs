//! M5 process harness: crash/lock thật bằng **process riêng** (SIGKILL), không chỉ object mock.
//!
//! Test binary tự re-exec chính nó ở vai "child" (`child_entry`, chỉ chạy khi có `M5_ROLE`):
//! child dựng Orchestrator thật trên store dùng chung, đưa run tới đúng cửa sổ crash rồi ngủ;
//! test cha chờ marker, `kill()` (SIGKILL — không có shutdown, không có Drop), rồi build lại
//! trên cùng store và kiểm tra recovery. OS advisory lock do kernel nhả khi process chết.
//!
//! Mode: fake gateway + SQLite/filesystem thật (không phải evidence provider live). Side effect
//! ở đây là file marker cục bộ: nó chứng minh engine không tự replay, không chứng minh
//! exactly-once ở hệ thống upstream.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::test_support::{ModelResponse, ScriptedGateway};
use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, CallerContext, ErrorCode, HostTool, LlmBackend,
    LlmConfig, LlmProviderConfig, MessageRole, OperatorContext, Orchestrator, OrchestratorConfig,
    PageRequest, ProviderAuth, RunStatus, SecretString, ToolContext, ToolDescriptor, ToolEffect,
    ToolError, ToolOutcome, VerifiedToolResolution,
};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};

const CHILD_MAX_LIFETIME: Duration = Duration::from_secs(120);

/// Tool Mutating: ghi side effect (file marker) rồi chờ — process bị kill trước khi có result.
struct Charge {
    dir: PathBuf,
    runs: Arc<AtomicUsize>,
    /// `true` ở child (ghi marker rồi ngủ); `false` ở cha (chỉ đếm, không được chạy).
    crash_window: bool,
}

#[async_trait::async_trait]
impl HostTool for Charge {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "charge".into(),
            description: "charge a card".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "text": { "type": "string" } },
                "required": ["text"],
                "additionalProperties": false
            }),
            effect: ToolEffect::Mutating,
        }
    }

    async fn execute(&self, ctx: ToolContext, _args: Value) -> Result<ToolOutcome, ToolError> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        if self.crash_window {
            std::fs::write(self.dir.join("effect.marker"), "charged").unwrap();
            tokio::time::sleep(Duration::from_secs(600)).await;
        }
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: "charged".into(),
            metadata: Value::Null,
        })
    }
}

fn agents(dir: &Path) -> Vec<AgentDefinition> {
    vec![
        AgentDefinition::new("root", "fake", "model-x")
            .with_mode(AgentMode::Primary)
            .with_prompt_path(dir.join("root.md"))
            .with_allowed_tools(["charge", "subagent"]),
        AgentDefinition::new("reviewer", "fake", "model-y")
            .with_mode(AgentMode::Subagent)
            .with_description("Reviews code and reports findings")
            .with_prompt_path(dir.join("reviewer.md"))
            .with_allowed_tools(Vec::<String>::new()),
    ]
}

fn config(dir: &Path) -> OrchestratorConfig {
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
        agents(dir),
        dir.join("store"),
    );
    cfg.runtime.supervisor_sweep = Duration::from_millis(50);
    cfg.runtime.shutdown_grace = Duration::from_secs(5);
    cfg
}

fn prepare_dir(dir: &Path) {
    std::fs::write(dir.join("root.md"), "You are the root test agent.").unwrap();
    std::fs::write(dir.join("reviewer.md"), "You are the reviewer test agent.").unwrap();
}

fn alice() -> CallerContext {
    CallerContext::new("alice", "ws1")
}

fn operator() -> OperatorContext {
    OperatorContext::new("op-1", "alice", "ws1").with_resolve_capability()
}

async fn open(
    dir: &Path,
    gw: &Arc<ScriptedGateway>,
    runs: &Arc<AtomicUsize>,
    crash_window: bool,
) -> Result<Orchestrator, orchestrator_agent_rs::OrchestratorError> {
    Orchestrator::builder(config(dir))
        .with_gateway(gw.clone())
        .register_tool(Arc::new(Charge {
            dir: dir.to_path_buf(),
            runs: runs.clone(),
            crash_window,
        }))
        .build()
        .await
}

// ─────────────────────────────── child role ───────────────────────────────

/// Không làm gì khi chạy như test thường; là thân của process con khi có `M5_ROLE`.
#[test]
fn child_entry() {
    let Ok(role) = std::env::var("M5_ROLE") else {
        return;
    };
    let dir = PathBuf::from(std::env::var("M5_DIR").expect("M5_DIR"));
    // Tự dọn nếu test cha chết trước khi kịp kill.
    std::thread::spawn(|| {
        std::thread::sleep(CHILD_MAX_LIFETIME);
        std::process::exit(3);
    });
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let gw = ScriptedGateway::new();
        let runs = Arc::new(AtomicUsize::new(0));
        match role.as_str() {
            "mutation" => {
                gw.push_tool_calls("", vec![("c1", "charge", json!({"text": "10 USD"}))]);
                gw.push_text("never");
            }
            "safe_point" => gw.hold(),
            "child" => {
                gw.push_tool_calls(
                    "",
                    vec![(
                        "c1",
                        "subagent",
                        json!({"agent": "reviewer", "description": "d", "prompt": "review"}),
                    )],
                );
                gw.push_delayed(
                    Duration::from_secs(600),
                    ModelResponse {
                        text: "never".into(),
                        finish_reason: "end_turn".into(),
                        tool_calls: Vec::new(),
                        usage: None,
                    },
                );
            }
            other => panic!("unknown role {other}"),
        }
        let o = open(&dir, &gw, &runs, true).await.expect("child build");
        let sid = o.create_session(&alice(), "root").await.unwrap();
        let t = o
            .submit(&alice(), &sid, "do the task".into())
            .await
            .unwrap();
        wait(|| match role.as_str() {
            "mutation" => dir.join("effect.marker").exists(),
            "safe_point" => gw.call_count() >= 1,
            _ => gw.call_count() >= 2,
        })
        .await;
        std::fs::write(
            dir.join("ids.json"),
            json!({ "session": sid, "turn": t.turn_id }).to_string(),
        )
        .unwrap();
        // Báo sẵn sàng sau cùng; cha kill ngay khi thấy file này.
        std::fs::write(dir.join("ready"), "1").unwrap();
        std::future::pending::<()>().await;
    });
}

async fn wait(mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "condition not met");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ─────────────────────────────── parent helpers ───────────────────────────────

struct Spawned {
    child: Child,
    session: String,
    turn: String,
}

async fn spawn_child(dir: &Path, role: &str) -> Spawned {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_entry", "--nocapture", "--test-threads=1"])
        .env("M5_ROLE", role)
        .env("M5_DIR", dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn child process");
    let ready = dir.join("ready");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !ready.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("child exited early: {status}");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "child never became ready"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let ids: Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("ids.json")).unwrap()).unwrap();
    Spawned {
        child,
        session: ids["session"].as_str().unwrap().to_owned(),
        turn: ids["turn"].as_str().unwrap().to_owned(),
    }
}

impl Spawned {
    /// SIGKILL: không shutdown, không Drop, không flush.
    fn crash(&mut self) {
        self.child.kill().unwrap();
        let status = self.child.wait().unwrap();
        assert!(!status.success(), "child must have been killed: {status}");
    }
}

async fn sql(dir: &Path) -> SqliteConnection {
    let opts = SqliteConnectOptions::new()
        .filename(dir.join("store").join("orchestrator.db"))
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5));
    SqliteConnection::connect_with(&opts).await.unwrap()
}

async fn wait_terminal(o: &Orchestrator, s: &str, t: &str) -> AgentResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let r = o.result(&alice(), s, t).await.unwrap();
        if r.status != RunStatus::Running {
            return r;
        }
        assert!(tokio::time::Instant::now() < deadline, "run stuck: {r:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn tool_texts(o: &Orchestrator, s: &str) -> Vec<String> {
    o.history(&alice(), s, PageRequest::first(100))
        .await
        .unwrap()
        .items
        .into_iter()
        .filter(|m| m.role == MessageRole::Tool)
        .map(|m| m.text)
        .collect()
}

// ─────────────────────────────── scenarios ───────────────────────────────

#[tokio::test]
async fn at12_at24_at34_at49_at50_sigkill_between_effect_and_result() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    prepare_dir(dir);
    let mut crashed = spawn_child(dir, "mutation").await;
    assert!(
        dir.join("effect.marker").exists(),
        "the side effect really happened"
    );

    // AT-50 bằng lock thật của process đang sống.
    let gw = ScriptedGateway::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let denied = open(dir, &gw, &runs, false)
        .await
        .err()
        .expect("lock is held");
    assert_eq!(denied.code(), ErrorCode::StoreAlreadyOwned);

    crashed.crash();

    // Sau khi owner chết, build thành công và recovery chạy dưới owner guard mới.
    let o = open(dir, &gw, &runs, false)
        .await
        .expect("owner released by the OS");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = o
        .result(&alice(), &crashed.session, &crashed.turn)
        .await
        .unwrap();
    assert_eq!(r.status, RunStatus::Interrupted);
    assert_eq!(r.attempt, 0, "no new attempt while the outcome is unknown");
    assert_eq!(r.error.as_ref().unwrap().code, "TOOL_OUTCOME_UNKNOWN");
    assert_eq!(gw.call_count(), 0, "no LLM request with a dangling call");
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "the mutation was not replayed"
    );
    assert!(tool_texts(&o, &crashed.session).await.is_empty());

    // AT-34: invocation chưa resolve hiển thị trung thực; input mới bị chặn.
    let mut c = sql(dir).await;
    let row = sqlx::query("SELECT id, status FROM tool_invocations WHERE tool_name = 'charge'")
        .fetch_one(&mut c)
        .await
        .unwrap();
    let (inv, status): (String, String) = (row.get(0), row.get(1));
    assert_eq!(status, "interrupted");
    let err = o
        .submit(&alice(), &crashed.session, "carry on".into())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::RecoveryBlocked);

    // AT-49: operator reconcile (bằng chứng ngoài hệ thống) rồi mới resume; không replay.
    gw.push_text("understood");
    o.resolve_interrupted_invocation(
        &operator(),
        &crashed.session,
        &inv,
        VerifiedToolResolution::ConfirmedNoEffect {
            evidence: "ledger shows no charge although the marker file exists".into(),
        },
    )
    .await
    .unwrap();
    let r = wait_terminal(&o, &crashed.session, &crashed.turn).await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.attempt, 1);
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert!(tool_texts(&o, &crashed.session).await[0].contains("TOOL_NO_EFFECT_CONFIRMED"));
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn at17_at23_sigkill_at_a_safe_point_resumes_the_same_turn() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    prepare_dir(dir);
    let mut crashed = spawn_child(dir, "safe_point").await;
    crashed.crash();

    let gw = ScriptedGateway::new();
    gw.push_text("resumed after SIGKILL");
    let runs = Arc::new(AtomicUsize::new(0));
    let o = open(dir, &gw, &runs, false).await.unwrap();
    let r = wait_terminal(&o, &crashed.session, &crashed.turn).await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.turn_id, crashed.turn, "turn_id survives the crash");
    assert_eq!(r.attempt, 1);
    assert_eq!(r.output.as_deref(), Some("resumed after SIGKILL"));
    assert_eq!(gw.call_count(), 1);
    let history = o
        .history(&alice(), &crashed.session, PageRequest::first(100))
        .await
        .unwrap()
        .items;
    assert_eq!(
        history.len(),
        2,
        "user input + one final answer, no duplicates"
    );
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn sigkill_while_a_child_is_active_recovers_parent_and_child_with_same_lineage() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    prepare_dir(dir);
    let mut crashed = spawn_child(dir, "child").await;
    let (child_session, child_turn): (String, String) = {
        let mut c = sql(dir).await;
        let row = sqlx::query(
            "SELECT child_session_id, child_turn_id FROM tool_invocations \
             WHERE tool_name = 'subagent'",
        )
        .fetch_one(&mut c)
        .await
        .unwrap();
        (row.get(0), row.get(1))
    };
    crashed.crash();

    // child chạy trước (parent chỉ chờ nó), rồi parent nhận kết quả của child.
    let gw = ScriptedGateway::new();
    gw.push_text("child answer after recovery");
    gw.push_text("parent summary");
    let runs = Arc::new(AtomicUsize::new(0));
    let o = open(dir, &gw, &runs, false).await.unwrap();
    let r = wait_terminal(&o, &crashed.session, &crashed.turn).await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.attempt, 1);
    assert_eq!(
        gw.call_count(),
        2,
        "one request per recovered run, no duplicate child"
    );

    let mut c = sql(dir).await;
    let link = sqlx::query(
        "SELECT child_session_id, child_turn_id FROM tool_invocations WHERE tool_name = 'subagent'",
    )
    .fetch_all(&mut c)
    .await
    .unwrap();
    assert_eq!(link.len(), 1, "same invocation, no second child admission");
    assert_eq!(link[0].get::<String, _>(0), child_session);
    assert_eq!(link[0].get::<String, _>(1), child_turn);
    let attempts = sqlx::query(
        "SELECT resume_attempt, status FROM agent_runs WHERE session_id = ?1 AND turn_id = ?2 \
         ORDER BY resume_attempt",
    )
    .bind(&child_session)
    .bind(&child_turn)
    .fetch_all(&mut c)
    .await
    .unwrap();
    let attempts: Vec<(i64, String)> = attempts.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(
        attempts,
        [(0, "interrupted".to_owned()), (1, "completed".to_owned())],
        "child recovery keeps child_turn_id"
    );
    let texts = tool_texts(&o, &crashed.session).await;
    assert!(
        texts[0].contains("child answer after recovery"),
        "{texts:?}"
    );
    o.shutdown().await.unwrap();
}
