//! M4 gate (fake LLM + SQLite thật): AT-11/19–22/26–27/35–36/44 cho Continuous execution.
//!
//! Mode: deterministic fake gateway (`test-support`) — KHÔNG phải evidence provider live và
//! KHÔNG chứng minh provider chạy bền nhiều giờ. >100 steps / >120 giây ở đây là bằng chứng
//! về *runtime không tự đặt deadline/cap*, dùng clock tokio điều khiển (`start_paused`).

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestrator_agent_rs::test_support::{
    GatewayError, ModelRequest, ModelResponse, ModelRole, ModelToolCall, ScriptedGateway,
    StreamEvent,
};
use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, CallerContext, HostTool, LlmBackend, LlmConfig,
    LlmProviderConfig, MessageRole, MessageView, Orchestrator, OrchestratorConfig, PageRequest,
    ProviderAuth, RunStatus, SecretString, ToolContext, ToolDescriptor, ToolEffect, ToolError,
    ToolOutcome,
};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};

const ROOT_MARK: &str = "You are the root test agent.";
const CHILD_MARK: &str = "You are the worker test agent.";
const OBJECTIVE: &str = "OBJECTIVE-7731: audit every module and report";

#[derive(Clone, Copy)]
enum Behavior {
    Echo,
    Big(usize),
    Sleep(Duration),
}

struct Probe {
    name: &'static str,
    behavior: Behavior,
    runs: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<String>>>,
}

impl Probe {
    fn new(name: &'static str, behavior: Behavior) -> (Arc<Self>, Arc<AtomicUsize>, Log) {
        let runs = Arc::new(AtomicUsize::new(0));
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let probe = Arc::new(Self {
            name,
            behavior,
            runs: runs.clone(),
            log: log.clone(),
        });
        (probe, runs, log)
    }
}

type Log = Arc<Mutex<Vec<String>>>;

#[async_trait::async_trait]
impl HostTool for Probe {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: self.name.into(),
            description: format!("probe {}", self.name),
            input_schema: json!({
                "type": "object",
                "properties": { "text": { "type": "string" } },
                "required": ["text"],
                "additionalProperties": false
            }),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        let text = args["text"].as_str().unwrap_or_default().to_owned();
        self.log.lock().unwrap().push(format!("start:{text}"));
        let content = match self.behavior {
            Behavior::Echo => format!("{}:{text}", self.name),
            Behavior::Big(n) => format!("{text}:{}", "x".repeat(n)),
            Behavior::Sleep(d) => {
                tokio::time::sleep(d).await;
                format!("{}:slept", self.name)
            }
        };
        self.log.lock().unwrap().push(format!("end:{text}"));
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content,
            metadata: Value::Null,
        })
    }
}

struct Env {
    dir: tempfile::TempDir,
}

impl Env {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("root.md"), ROOT_MARK).unwrap();
        std::fs::write(dir.path().join("worker.md"), CHILD_MARK).unwrap();
        Self { dir }
    }

    fn store(&self) -> PathBuf {
        self.dir.path().join("store")
    }

    fn root(&self, tools: &[&str]) -> AgentDefinition {
        AgentDefinition::new("root", "fake", "model-x")
            .with_mode(AgentMode::Primary)
            .with_prompt_path(self.dir.path().join("root.md"))
            .with_allowed_tools(tools.iter().copied())
    }

    fn worker(&self, tools: &[&str]) -> AgentDefinition {
        AgentDefinition::new("worker", "fake", "model-x")
            .with_mode(AgentMode::Subagent)
            .with_description("Does long work")
            .with_prompt_path(self.dir.path().join("worker.md"))
            .with_allowed_tools(tools.iter().copied())
    }

    fn config(&self, agents: Vec<AgentDefinition>) -> OrchestratorConfig {
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
            agents,
            self.store(),
        );
        cfg.runtime.supervisor_sweep = Duration::from_millis(50);
        cfg.runtime.shutdown_grace = Duration::from_secs(5);
        cfg
    }

    /// Cấu hình ép compaction sớm: ngưỡng ≈ 6 KB, giữ 2 exchange gần nhất.
    fn small_context(&self, agents: Vec<AgentDefinition>) -> OrchestratorConfig {
        let mut cfg = self.config(agents);
        cfg.runtime.context_limit_tokens = 3_000;
        cfg.runtime.compaction_threshold_percent = 50;
        cfg.runtime.compaction_keep_recent = 2;
        cfg
    }

    async fn sql(&self) -> SqliteConnection {
        let opts = SqliteConnectOptions::new()
            .filename(self.store().join("orchestrator.db"))
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        SqliteConnection::connect_with(&opts).await.unwrap()
    }

    async fn scalar(&self, sql: &str) -> i64 {
        let mut c = self.sql().await;
        sqlx::query(sql).fetch_one(&mut c).await.unwrap().get(0)
    }
}

fn alice() -> CallerContext {
    CallerContext::new("alice", "ws1")
}

async fn build(
    cfg: OrchestratorConfig,
    gw: &Arc<ScriptedGateway>,
    tools: Vec<Arc<dyn HostTool>>,
) -> Orchestrator {
    let mut b = Orchestrator::builder(cfg).with_gateway(gw.clone());
    for t in tools {
        b = b.register_tool(t);
    }
    b.build().await.unwrap()
}

/// Chờ terminal theo số lần poll + guard thời gian thật (không dùng clock tokio: có test
/// chạy với clock bị pause).
async fn wait_terminal(o: &Orchestrator, s: &str, t: &str) -> AgentResult {
    let started = std::time::Instant::now();
    loop {
        let r = o.result(&alice(), s, t).await.unwrap();
        if r.status != RunStatus::Running {
            return r;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "run stuck: {r:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn run_turn(o: &Orchestrator, sid: &str, text: &str) -> AgentResult {
    let turn = o.submit(&alice(), sid, text.into()).await.unwrap();
    wait_terminal(o, sid, &turn.turn_id).await
}

async fn history(o: &Orchestrator, sid: &str) -> Vec<MessageView> {
    o.history(&alice(), sid, PageRequest::first(500))
        .await
        .unwrap()
        .items
}

async fn root_session(
    env: &Env,
    cfg: OrchestratorConfig,
    gw: &Arc<ScriptedGateway>,
    tools: Vec<Arc<dyn HostTool>>,
) -> (Orchestrator, String) {
    let _ = env;
    let o = build(cfg, gw, tools).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    (o, sid)
}

fn call(id: &str, tool: &str, text: &str) -> (String, String, Value) {
    (id.into(), tool.into(), json!({ "text": text }))
}

fn push_call(gw: &ScriptedGateway, id: &str, tool: &str, text: &str) {
    let (i, t, a) = call(id, tool, text);
    gw.push_tool_calls("", vec![(&i, &t, a)]);
}

/// Mỗi request: mọi Tool message phải ghép với call của assistant đứng trước nó trong cùng
/// request, và mọi call của assistant phải có đủ result (không dangling).
fn assert_pairs_complete(req: &ModelRequest) {
    let mut open: Vec<String> = Vec::new();
    for m in &req.messages {
        match m.role {
            ModelRole::Assistant => {
                assert!(open.is_empty(), "assistant before results of {open:?}");
                open = m.tool_calls.iter().map(|c| c.id.clone()).collect();
            }
            ModelRole::Tool => {
                let id = m.tool_call_id.clone().expect("tool result has call id");
                let pos = open
                    .iter()
                    .position(|o| *o == id)
                    .unwrap_or_else(|| panic!("orphan tool result {id}"));
                open.remove(pos);
            }
            ModelRole::User => assert!(open.is_empty(), "user inside open exchange {open:?}"),
        }
    }
}

// ───────────────────────── AT-19 / AT-20 (steps) ─────────────────────────

/// AT-19: không cấu hình `steps`, model gọi >100 steps → chạy tới final.
#[tokio::test]
async fn at19_no_steps_cap_runs_past_one_hundred_steps() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (echo, runs, _) = Probe::new("echo", Behavior::Echo);
    for i in 0..105 {
        push_call(&gw, &format!("c{i}"), "echo", &format!("n{i}"));
    }
    gw.push_text("all done");
    let (o, sid) = root_session(&env, env.config(vec![env.root(&["echo"])]), &gw, vec![echo]).await;

    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(r.output.as_deref(), Some("all done"));
    assert_eq!(runs.load(Ordering::SeqCst), 105);
    assert_eq!(env.scalar("SELECT steps FROM agent_runs").await, 106);
    assert_eq!(gw.agent_requests().len(), 106);
    // Mọi request đều còn tools: không có cap ngầm.
    assert!(gw.agent_requests().iter().all(|r| !r.tools.is_empty()));
}

fn capped_agent(env: &Env) -> AgentDefinition {
    env.root(&["echo"]).with_steps(5)
}

/// AT-20: `steps=5` → tới cap không còn tool; text-only progress; không tự tạo run mới.
#[tokio::test]
async fn at20_steps_cap_ends_with_text_only_progress_and_no_new_run() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (echo, runs, _) = Probe::new("echo", Behavior::Echo);
    for i in 0..5 {
        push_call(&gw, &format!("c{i}"), "echo", &format!("n{i}"));
    }
    gw.push_text("progress: did 5 things; remaining: the rest");
    let (o, sid) = root_session(&env, env.config(vec![capped_agent(&env)]), &gw, vec![echo]).await;

    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(
        r.status,
        RunStatus::Completed,
        "cap is not a technical error"
    );
    assert_eq!(
        r.output.as_deref(),
        Some("progress: did 5 things; remaining: the rest")
    );
    assert_eq!(runs.load(Ordering::SeqCst), 5);

    let reqs = gw.agent_requests();
    assert_eq!(reqs.len(), 6);
    assert!(reqs[..5].iter().all(|r| !r.tools.is_empty()));
    let last = &reqs[5];
    assert!(last.tools.is_empty(), "no tools once the cap is reached");
    let guidance = last.messages.last().unwrap();
    assert_eq!(guidance.role, ModelRole::User);
    assert!(guidance.text.contains("step limit"), "{}", guidance.text);
    assert_pairs_complete(last);

    assert_eq!(env.scalar("SELECT COUNT(*) FROM agent_runs").await, 1);
    // Guidance chỉ là context tạm: không phải input của user trong transcript.
    let users = history(&o, &sid)
        .await
        .into_iter()
        .filter(|m| m.role == MessageRole::User)
        .count();
    assert_eq!(users, 1);

    // Session vẫn mở: follow-up được nhận như một turn mới.
    gw.push_text("second turn");
    let r2 = run_turn(&o, &sid, "continue").await;
    assert_eq!(r2.status, RunStatus::Completed);
    assert_ne!(r2.turn_id, r.turn_id);
}

/// Cap: model vẫn trả tool call dù không có tool → bỏ call, không execute, không persist.
#[tokio::test]
async fn at20_tool_calls_at_the_cap_are_ignored() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (echo, runs, _) = Probe::new("echo", Behavior::Echo);
    for i in 0..5 {
        push_call(&gw, &format!("c{i}"), "echo", &format!("n{i}"));
    }
    gw.push_tool_calls("still working", vec![("c9", "echo", json!({"text": "x"}))]);
    let (o, sid) = root_session(&env, env.config(vec![capped_agent(&env)]), &gw, vec![echo]).await;

    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(r.output.as_deref(), Some("still working"));
    assert_eq!(runs.load(Ordering::SeqCst), 5, "capped call must not run");
    assert_eq!(env.scalar("SELECT COUNT(*) FROM tool_invocations").await, 5);
}

/// Cap mà model không trả text → text cố định nêu rõ giới hạn, không để output rỗng.
#[tokio::test]
async fn at20_capped_response_without_text_gets_an_explicit_fallback() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (echo, _, _) = Probe::new("echo", Behavior::Echo);
    for i in 0..5 {
        push_call(&gw, &format!("c{i}"), "echo", "n");
    }
    gw.push_text("");
    let (o, sid) = root_session(&env, env.config(vec![capped_agent(&env)]), &gw, vec![echo]).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    assert!(r.output.unwrap().contains("step limit"));
}

// ───────────────────────── AT-21 / AT-22 / AT-44 (compaction) ─────────────────────────

/// Kịch bản: `n` bước tool với output lớn rồi final. Root dùng `small_context`.
async fn long_work(
    env: &Env,
    gw: &Arc<ScriptedGateway>,
    steps: usize,
) -> (Orchestrator, String, Arc<AtomicUsize>) {
    let (big, runs, _) = Probe::new("big", Behavior::Big(2_000));
    for i in 0..steps {
        push_call(gw, &format!("c{i}"), "big", &format!("n{i}"));
    }
    gw.push_text("final report");
    let cfg = env.small_context(vec![env.root(&["big"])]);
    let (o, sid) = root_session(env, cfg, gw, vec![big]).await;
    (o, sid, runs)
}

async fn checkpoints(env: &Env) -> Vec<Value> {
    let mut c = env.sql().await;
    sqlx::query("SELECT content_json FROM messages WHERE role = 'compaction' ORDER BY sequence")
        .fetch_all(&mut c)
        .await
        .unwrap()
        .iter()
        .map(|r| serde_json::from_str(&r.get::<String, _>(0)).unwrap())
        .collect()
}

/// AT-21: gần context limit → persist checkpoint, giữ original history, giữ tool pairs.
#[tokio::test]
async fn at21_near_limit_persists_checkpoint_keeps_history_and_pairs() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (o, sid, runs) = long_work(&env, &gw, 8).await;

    let r = run_turn(&o, &sid, OBJECTIVE).await;
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(runs.load(Ordering::SeqCst), 8);

    let cps = checkpoints(&env).await;
    assert!(!cps.is_empty(), "a checkpoint must be persisted");
    assert_eq!(cps[0]["version"], 1);
    assert!(cps[0]["summary"].as_str().unwrap().contains("SUMMARY"));
    assert_eq!(cps[0]["model_ref"], "fake/model-x");

    // Original history còn nguyên: user + (assistant + tool)×8 + final.
    let h = history(&o, &sid).await;
    assert_eq!(h.len(), 1 + 8 * 2 + 1);
    assert_eq!(
        env.scalar("SELECT COUNT(*) FROM messages WHERE role = 'tool'")
            .await,
        8
    );

    // Request sau checkpoint bắt đầu bằng summary, mang objective, không dangling pair và
    // nhỏ hơn request không nén tương ứng.
    let reqs = gw.agent_requests();
    let after: Vec<_> = reqs
        .iter()
        .filter(|r| r.messages[0].text.contains("<conversation_summary>"))
        .collect();
    assert!(!after.is_empty());
    for req in &reqs {
        assert_pairs_complete(req);
    }
    let first = &after[0].messages[0];
    assert_eq!(first.role, ModelRole::User);
    assert!(first.text.contains("SUMMARY: condensed"));
    assert!(first.text.contains(OBJECTIVE), "objective must survive");

    // Summary request: không tools, chỉ có exchange settled, cùng provider/model.
    let sums = gw.summary_requests();
    assert!(!sums.is_empty());
    assert!(sums[0].tools.is_empty());
    assert_eq!(sums[0].model_id, "model-x");
}

/// AT-44: một active turn vượt threshold nhiều lần → compact prefix lặp lại, objective và
/// tool pairs vẫn đủ, không cần turn hoàn tất trước đó.
#[tokio::test]
async fn at44_repeated_compaction_inside_one_active_turn() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (o, sid, _) = long_work(&env, &gw, 14).await;

    let r = run_turn(&o, &sid, OBJECTIVE).await;
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(
        env.scalar("SELECT COUNT(DISTINCT turn_id) FROM agent_runs")
            .await,
        1
    );

    let cps = checkpoints(&env).await;
    assert!(
        cps.len() >= 2,
        "expected repeated compaction, got {}",
        cps.len()
    );
    let frontiers: Vec<u64> = cps
        .iter()
        .map(|c| c["keep_from_sequence"].as_u64().unwrap())
        .collect();
    assert!(frontiers.windows(2).all(|w| w[0] < w[1]), "{frontiers:?}");
    // Compaction sau gộp summary trước (summarizer nhận <previous_summary>).
    let sums = gw.summary_requests();
    assert!(sums.len() >= 2);
    assert!(sums[1].messages[0].text.contains("<previous_summary>"));

    let reqs = gw.agent_requests();
    for req in &reqs {
        assert_pairs_complete(req);
    }
    let last = reqs.last().unwrap();
    assert!(last.messages[0].text.contains(OBJECTIVE));
    // Context cuối bị chặn bởi checkpoint: ngắn hơn nhiều so với toàn bộ transcript.
    assert!(last.messages.len() < 2 + 14 * 2);
}

/// Compaction chủ động lỗi (summarizer hỏng) không làm hỏng run.
#[tokio::test]
async fn failed_proactive_compaction_does_not_fail_the_run() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    for _ in 0..6 {
        gw.push_summary(Err(GatewayError::Provider));
    }
    let (o, sid, _) = long_work(&env, &gw, 6).await;
    let r = run_turn(&o, &sid, OBJECTIVE).await;
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert!(checkpoints(&env).await.is_empty());
    assert!(!gw.summary_requests().is_empty());
}

/// AT-22: provider báo overflow → compact + retry đúng một lần → thành công.
#[tokio::test]
async fn at22_overflow_compacts_and_retries_once() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (big, _, _) = Probe::new("big", Behavior::Big(10));
    for i in 0..4 {
        push_call(&gw, &format!("c{i}"), "big", &format!("n{i}"));
    }
    gw.push_error(GatewayError::ContextOverflow); // step 5 bị từ chối
    gw.push_text("recovered");
    // Context rất lớn so với ngưỡng nên không compact chủ động; chỉ overflow mới kích hoạt.
    let mut cfg = env.config(vec![env.root(&["big"])]);
    cfg.runtime.compaction_keep_recent = 2;
    let (o, sid) = root_session(&env, cfg, &gw, vec![big]).await;

    let r = run_turn(&o, &sid, OBJECTIVE).await;
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(r.output.as_deref(), Some("recovered"));
    assert_eq!(checkpoints(&env).await.len(), 1);
    assert_eq!(gw.summary_requests().len(), 1);
    let reqs = gw.agent_requests();
    assert_eq!(reqs.len(), 6, "4 steps + rejected + retry");
    assert!(reqs[5].messages[0].text.contains("<conversation_summary>"));
    assert!(reqs[5].messages[0].text.contains(OBJECTIVE));
    assert_pairs_complete(&reqs[5]);
}

/// AT-22: vẫn overflow sau khi compact → lỗi recoverable `CONTEXT_LIMIT`, không retry thêm.
#[tokio::test]
async fn at22_persistent_overflow_fails_recoverably_after_one_retry() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (big, _, _) = Probe::new("big", Behavior::Big(10));
    for i in 0..4 {
        push_call(&gw, &format!("c{i}"), "big", &format!("n{i}"));
    }
    gw.push_error(GatewayError::ContextOverflow);
    gw.push_error(GatewayError::ContextOverflow);
    gw.push_text("never reached");
    let (o, sid) = root_session(&env, env.config(vec![env.root(&["big"])]), &gw, vec![big]).await;

    let r = run_turn(&o, &sid, OBJECTIVE).await;
    assert_eq!(r.status, RunStatus::Failed, "{r:?}");
    assert_eq!(r.error.as_ref().unwrap().code, "CONTEXT_LIMIT");
    assert_eq!(gw.agent_requests().len(), 6, "exactly one retry");
    assert_eq!(gw.summary_requests().len(), 1);

    // Recoverable: session vẫn dùng được cho turn kế tiếp.
    let after = run_turn(&o, &sid, "try again").await;
    assert_eq!(after.output.as_deref(), Some("never reached"));
}

/// AT-22: không có gì để nén (một turn rất ngắn) → fail ngay, không gọi summarizer.
#[tokio::test]
async fn at22_overflow_with_nothing_to_compact_fails_without_summarizing() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_error(GatewayError::ContextOverflow);
    let (o, sid) = root_session(&env, env.config(vec![env.root(&[])]), &gw, vec![]).await;
    let r = run_turn(&o, &sid, "huge input").await;
    assert_eq!(r.status, RunStatus::Failed);
    assert_eq!(r.error.unwrap().code, "CONTEXT_LIMIT");
    assert!(gw.summary_requests().is_empty());
}

// ───────────────────────── AT-26 / AT-27 / AT-35 / AT-36 / usage ─────────────────────────

/// AT-26: 20 fragment live đúng thứ tự; SQLite chỉ có message đã commit.
#[tokio::test]
async fn at26_twenty_fragments_stream_in_order_and_only_the_final_is_stored() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let fragments: Vec<String> = (0..20).map(|i| format!("<{i}>")).collect();
    let refs: Vec<&str> = fragments.iter().map(String::as_str).collect();
    gw.push_stream_text(&refs);
    let (o, sid) = root_session(&env, env.config(vec![env.root(&[])]), &gw, vec![]).await;

    let r = run_turn(&o, &sid, "stream please").await;
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(r.output.as_deref(), Some(fragments.concat().as_str()));

    let emitted: Vec<String> = gw
        .emitted_events()
        .into_iter()
        .map(|e| match e {
            StreamEvent::TextDelta(t) => t,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(emitted, fragments);

    // Không persist từng delta: đúng 2 message (user + final).
    assert_eq!(env.scalar("SELECT COUNT(*) FROM messages").await, 2);
    let h = history(&o, &sid).await;
    assert_eq!(h[1].text, fragments.concat());
}

/// AT-35: ToolCallReady rồi lỗi trước Finished → không execute, không persist invocation
/// và text preview không trở thành message.
#[tokio::test]
async fn at35_tool_call_ready_then_stream_error_runs_nothing_and_persists_nothing() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (echo, runs, _) = Probe::new("echo", Behavior::Echo);
    gw.push_stream_failure(
        vec![
            StreamEvent::TextDelta("partial preview".into()),
            StreamEvent::ToolCallReady {
                id: "c1".into(),
                name: "echo".into(),
            },
        ],
        GatewayError::Transport,
    );
    let (o, sid) = root_session(&env, env.config(vec![env.root(&["echo"])]), &gw, vec![echo]).await;

    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Failed, "{r:?}");
    assert_eq!(r.output, None);
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert_eq!(env.scalar("SELECT COUNT(*) FROM tool_invocations").await, 0);
    let h = history(&o, &sid).await;
    assert_eq!(h.len(), 1, "only the user input is committed: {h:?}");
}

/// AT-27 (runtime): ToolCallReady chỉ là preview; tool chỉ chạy sau response Finished và
/// invocation đã được persist.
#[tokio::test]
async fn at27_ready_preview_does_not_execute_before_finished_and_persist() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (echo, runs, _) = Probe::new("echo", Behavior::Echo);
    gw.push_response_streamed(
        vec![StreamEvent::ToolCallReady {
            id: "c1".into(),
            name: "echo".into(),
        }],
        ModelResponse {
            text: String::new(),
            finish_reason: "tool_use".into(),
            tool_calls: vec![ModelToolCall {
                id: "c1".into(),
                name: "echo".into(),
                arguments: json!({"text": "hi"}),
            }],
            usage: None,
        },
    );
    gw.push_text("done");
    let (o, sid) = root_session(&env, env.config(vec![env.root(&["echo"])]), &gw, vec![echo]).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        env.scalar("SELECT COUNT(*) FROM tool_invocations WHERE status = 'completed'")
            .await,
        1
    );
}

/// AT-36: hai tool trong một response → persist đủ rồi dispatch tuần tự; LLM step kế tiếp
/// chỉ sau cả hai result.
#[tokio::test]
async fn at36_two_tools_run_sequentially_before_the_next_llm_step() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (echo, runs, log) = Probe::new("echo", Behavior::Echo);
    gw.push_tool_calls(
        "",
        vec![
            ("a", "echo", json!({"text": "A"})),
            ("b", "echo", json!({"text": "B"})),
        ],
    );
    gw.push_text("both done");
    let (o, sid) = root_session(&env, env.config(vec![env.root(&["echo"])]), &gw, vec![echo]).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    assert_eq!(
        *log.lock().unwrap(),
        ["start:A", "end:A", "start:B", "end:B"]
    );
    let reqs = gw.agent_requests();
    assert_eq!(reqs.len(), 2);
    let tools: Vec<_> = reqs[1]
        .messages
        .iter()
        .filter(|m| m.role == ModelRole::Tool)
        .map(|m| m.tool_call_id.clone().unwrap())
        .collect();
    assert_eq!(tools, ["a", "b"]);
}

/// Usage: số đo được persist; thiếu usage không phải lỗi và không bị bịa.
#[tokio::test]
async fn usage_is_recorded_when_reported_and_missing_usage_is_not_an_error() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_text_with_usage("measured", 120, 30);
    gw.push_text("unmeasured");
    let (o, sid) = root_session(&env, env.config(vec![env.root(&[])]), &gw, vec![]).await;

    let r1 = run_turn(&o, &sid, "one").await;
    assert_eq!(r1.status, RunStatus::Completed);
    assert_eq!(r1.steps, 1);
    assert_eq!(r1.input_tokens, 120);
    assert_eq!(r1.output_tokens, 30);
    assert_eq!(r1.total_tokens(), 150);
    let r2 = run_turn(&o, &sid, "two").await;
    assert_eq!(
        r2.status,
        RunStatus::Completed,
        "missing usage is not an error"
    );
    assert_eq!(r2.steps, 1);
    assert_eq!(
        (r2.input_tokens, r2.output_tokens, r2.total_tokens()),
        (0, 0, 0)
    );

    let mut c = env.sql().await;
    let rows = sqlx::query("SELECT input_tokens, output_tokens FROM agent_runs ORDER BY rowid")
        .fetch_all(&mut c)
        .await
        .unwrap();
    let pairs: Vec<(i64, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(pairs, [(120, 30), (0, 0)]);
}

// ───────────────────────── AT-11 (clock điều khiển) ─────────────────────────

/// AT-11: child chạy >120 giây (clock tokio bị pause) qua nhiều tool call; không có deadline tổng của
/// child. Mỗi thao tác (100 s) vẫn nằm trong `tool_timeout` (120 s) của riêng nó.
#[tokio::test]
async fn at11_child_longer_than_120s_is_not_cut_by_a_whole_child_timeout() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (slow, runs, _) = Probe::new("slow", Behavior::Sleep(Duration::from_secs(100)));
    // root → child(slow, slow, final) → root final
    gw.push_tool_calls(
        "",
        vec![(
            "r1",
            "subagent",
            json!({ "agent": "worker", "description": "d", "prompt": "do the long job" }),
        )],
    );
    push_call(&gw, "w1", "slow", "one");
    push_call(&gw, "w2", "slow", "two");
    gw.push_text("child finished after a long time");
    gw.push_text("root done");
    let mut cfg = env.config(vec![env.root(&["subagent"]), env.worker(&["slow"])]);
    cfg.runtime.tool_timeout = Duration::from_secs(120);
    let (o, sid) = root_session(&env, cfg, &gw, vec![slow]).await;

    // Pause sau khi store/pool đã mở: clock ảo không được đụng vào bước khởi tạo SQLite.
    tokio::time::pause();
    let t0 = tokio::time::Instant::now();
    let r = run_turn(&o, &sid, "delegate").await;
    let elapsed = t0.elapsed();
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(r.output.as_deref(), Some("root done"));
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    assert!(
        elapsed >= Duration::from_secs(200),
        "virtual clock must have advanced past 120s, got {elapsed:?}"
    );

    // Child kết thúc completed (không bị timeout toàn child).
    let child_status: String = {
        let mut c = env.sql().await;
        sqlx::query(
            "SELECT status FROM agent_runs WHERE session_id = \
             (SELECT child_session_id FROM tool_invocations WHERE tool_name = 'subagent')",
        )
        .fetch_one(&mut c)
        .await
        .unwrap()
        .get(0)
    };
    assert_eq!(child_status, "completed");
}

/// Per-operation timeout vẫn độc lập: một thao tác vượt `tool_timeout` thì bị cắt riêng.
#[tokio::test]
async fn at11_per_operation_timeout_still_applies_to_a_single_tool() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (slow, _, _) = Probe::new("slow", Behavior::Sleep(Duration::from_secs(150)));
    push_call(&gw, "c1", "slow", "x");
    gw.push_text("after timeout");
    let mut cfg = env.config(vec![env.root(&["slow"])]);
    cfg.runtime.tool_timeout = Duration::from_secs(120);
    let (o, sid) = root_session(&env, cfg, &gw, vec![slow]).await;

    tokio::time::pause();
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    let h = history(&o, &sid).await;
    let tool = h.iter().find(|m| m.role == MessageRole::Tool).unwrap();
    assert!(tool.text.contains("TOOL_TIMEOUT"), "{}", tool.text);
}

// ───────────────────────── Per-agent context limit / per-tool timeout ─────────────────────────

/// Tool tự khai báo timeout riêng (vd. tải/transcribe dài) thay cho `tool_timeout` toàn cục.
struct WithTimeout(Arc<Probe>, Duration);

#[async_trait::async_trait]
impl HostTool for WithTimeout {
    fn descriptor(&self) -> ToolDescriptor {
        self.0.descriptor()
    }

    fn timeout(&self) -> Option<Duration> {
        Some(self.1)
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        self.0.execute(ctx, args).await
    }
}

#[tokio::test]
async fn a_tool_with_its_own_timeout_outlives_the_global_tool_timeout() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (slow, _, _) = Probe::new("slow", Behavior::Sleep(Duration::from_secs(150)));
    push_call(&gw, "c1", "slow", "x");
    gw.push_text("done");
    let mut cfg = env.config(vec![env.root(&["slow"])]);
    cfg.runtime.tool_timeout = Duration::from_secs(120);
    let tool = Arc::new(WithTimeout(slow, Duration::from_secs(200)));
    let (o, sid) = root_session(&env, cfg, &gw, vec![tool]).await;

    tokio::time::pause();
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    let h = history(&o, &sid).await;
    let tool = h.iter().find(|m| m.role == MessageRole::Tool).unwrap();
    assert_eq!(tool.text, "slow:slept");
}

#[tokio::test]
async fn a_zero_tool_timeout_is_rejected_at_build() {
    let env = Env::new();
    let (slow, _, _) = Probe::new("slow", Behavior::Echo);
    let tool: Arc<dyn HostTool> = Arc::new(WithTimeout(slow, Duration::ZERO));
    let e = Orchestrator::builder(env.config(vec![env.root(&["slow"])]))
        .with_gateway(ScriptedGateway::new())
        .register_tool(tool)
        .build()
        .await
        .err()
        .unwrap();
    assert_eq!(e.reason().map(|r| r.as_str()), Some("INVALID_TIMEOUT"));
}

#[tokio::test]
async fn an_agent_context_limit_overrides_the_global_one() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (big, _, _) = Probe::new("big", Behavior::Big(2_000));
    for i in 0..8 {
        push_call(&gw, &format!("c{i}"), "big", &format!("n{i}"));
    }
    gw.push_text("final report");
    // Global giữ mặc định (100k token): chỉ giới hạn riêng của agent mới ép compaction.
    let mut cfg = env.config(vec![env.root(&["big"]).with_context_limit_tokens(3_000)]);
    cfg.runtime.compaction_threshold_percent = 50;
    cfg.runtime.compaction_keep_recent = 2;
    let (o, sid) = root_session(&env, cfg, &gw, vec![big]).await;

    let r = run_turn(&o, &sid, OBJECTIVE).await;
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert!(
        !checkpoints(&env).await.is_empty(),
        "the agent limit must trigger compaction"
    );
}

#[tokio::test]
async fn a_zero_agent_context_limit_is_rejected_at_build() {
    let env = Env::new();
    let cfg = env.config(vec![env.root(&[]).with_context_limit_tokens(0)]);
    let e = Orchestrator::builder(cfg)
        .with_gateway(ScriptedGateway::new())
        .build()
        .await
        .err()
        .unwrap();
    assert_eq!(
        e.reason().map(|r| r.as_str()),
        Some("INVALID_RUNTIME_CONFIG")
    );
}
