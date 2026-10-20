//! M5 gate (fake LLM + SQLite thật): recovery & lifecycle.
//!
//! AT-12, AT-17, AT-23, AT-24, AT-41, AT-42, AT-49, AT-50 (mức in-process), bounded recovery
//! attempts, fingerprint/rotation, parent cancel → active child, submit sau shutdown bị từ chối
//! trước commit, shutdown drain/quiesce.
//!
//! Mode: deterministic fake gateway (`test-support`). Trạng thái "process chết" được **dựng
//! bằng SQL** (build → shutdown → ghi dở dang → build lại) để kiểm soát từng cửa sổ crash;
//! crash/lock bằng process thật nằm ở `m5_process.rs`. Fake side effects KHÔNG chứng minh
//! exactly-once ở upstream: ở đây chỉ chứng minh engine không tự replay.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::test_support::{
    ModelRequest, ModelResponse, ModelRole, ScriptedGateway,
};
use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, CallerContext, ErrorCode, HostTool, LlmBackend,
    LlmConfig, LlmProviderConfig, MessageRole, MessageView, OperatorContext, Orchestrator,
    OrchestratorConfig, OrchestratorError, PageRequest, ProviderAuth, RunStatus, SecretString,
    ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome, VerifiedToolResolution,
};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};

const ROOT_MARK: &str = "You are the root test agent.";
const REVIEWER_MARK: &str = "You are the reviewer test agent.";

#[derive(Clone, Copy)]
enum Behavior {
    Echo,
    /// Dừng khi `ctx.is_cancelled()`.
    UntilCancelled,
    /// Bỏ qua cancel: ngủ đủ lâu (non-cooperative).
    Ignore(Duration),
}

struct Probe {
    name: &'static str,
    effect: ToolEffect,
    behavior: Behavior,
    runs: Arc<AtomicUsize>,
}

impl Probe {
    fn new(
        name: &'static str,
        effect: ToolEffect,
        behavior: Behavior,
    ) -> (Arc<Self>, Arc<AtomicUsize>) {
        let runs = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(Self {
                name,
                effect,
                behavior,
                runs: runs.clone(),
            }),
            runs,
        )
    }
}

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
            effect: self.effect,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        let text = args["text"].as_str().unwrap_or_default().to_owned();
        let content = match self.behavior {
            Behavior::Echo => format!("{}:{text}", self.name),
            Behavior::UntilCancelled => {
                while !ctx.is_cancelled() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                "stopped".into()
            }
            Behavior::Ignore(d) => {
                tokio::time::sleep(d).await;
                "finished-late".into()
            }
        };
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
        std::fs::write(dir.path().join("reviewer.md"), REVIEWER_MARK).unwrap();
        Self { dir }
    }

    fn store(&self) -> PathBuf {
        self.dir.path().join("store")
    }

    fn root(&self, model: &str, tools: &[&str]) -> AgentDefinition {
        AgentDefinition::new("root", "fake", model)
            .with_mode(AgentMode::Primary)
            .with_prompt_path(self.dir.path().join("root.md"))
            .with_allowed_tools(tools.iter().copied())
    }

    fn reviewer(&self, tools: &[&str]) -> AgentDefinition {
        AgentDefinition::new("reviewer", "fake", "model-y")
            .with_mode(AgentMode::Subagent)
            .with_description("Reviews code and reports findings")
            .with_prompt_path(self.dir.path().join("reviewer.md"))
            .with_allowed_tools(tools.iter().copied())
    }

    /// Provider `fake`: `endpoint`/`key` quyết định fingerprint / rotation.
    fn config_with(
        &self,
        agents: Vec<AgentDefinition>,
        endpoint: &str,
        key: &str,
    ) -> OrchestratorConfig {
        let mut cfg = OrchestratorConfig::new(
            LlmConfig {
                providers: vec![LlmProviderConfig {
                    id: "fake".into(),
                    backend: LlmBackend::OpenAiCompatible,
                    endpoint: Some(endpoint.into()),
                    auth: ProviderAuth::ApiKey(SecretString::new(key)),
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

    fn config(&self, agents: Vec<AgentDefinition>) -> OrchestratorConfig {
        self.config_with(agents, "http://127.0.0.1:9/v1", "sk-test-not-real")
    }

    fn agents(&self, tools: &[&str]) -> Vec<AgentDefinition> {
        vec![self.root("model-x", tools), self.reviewer(&[])]
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

fn operator() -> OperatorContext {
    OperatorContext::new("op-1", "alice", "ws1").with_resolve_capability()
}

async fn build(
    cfg: OrchestratorConfig,
    gw: &Arc<ScriptedGateway>,
    tools: Vec<Arc<dyn HostTool>>,
) -> Result<Orchestrator, OrchestratorError> {
    let mut b = Orchestrator::builder(cfg).with_gateway(gw.clone());
    for t in tools {
        b = b.register_tool(t);
    }
    b.build().await
}

async fn wait_terminal(o: &Orchestrator, s: &str, t: &str) -> AgentResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let r = o.result(&alice(), s, t).await.unwrap();
        if r.status != RunStatus::Running {
            return r;
        }
        assert!(tokio::time::Instant::now() < deadline, "run stuck: {r:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_until(mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "condition not met");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn history(o: &Orchestrator, s: &str) -> Vec<MessageView> {
    o.history(&alice(), s, PageRequest::first(200))
        .await
        .unwrap()
        .items
}

fn tool_texts(h: &[MessageView]) -> Vec<String> {
    h.iter()
        .filter(|m| m.role == MessageRole::Tool)
        .map(|m| m.text.clone())
        .collect()
}

/// Arguments hợp lệ theo schema của tool được seed.
fn args_of(tool: &str) -> Value {
    if tool == "subagent" {
        sub_new("seeded delegation")
    } else {
        json!({ "text": "x" })
    }
}

/// Dựng dở dang trực tiếp trong SQLite (process "đã chết"): chỉ dùng khi không có
/// Orchestrator nào đang giữ store.
struct Seed<'a> {
    conn: &'a mut SqliteConnection,
    sid: String,
    seq: i64,
    agent: (&'static str, &'static str),
}

impl<'a> Seed<'a> {
    fn new(conn: &'a mut SqliteConnection, sid: &str) -> Self {
        Self {
            conn,
            sid: sid.into(),
            seq: 1,
            agent: ("root", "model-x"),
        }
    }

    /// Run của session này thuộc agent `reviewer` (child).
    fn for_reviewer(mut self) -> Self {
        self.agent = ("reviewer", "model-y");
        self
    }

    async fn user(&mut self, text: &str) {
        self.message(None, "user", json!({ "text": text }), None, None)
            .await;
    }

    async fn message(
        &mut self,
        run: Option<&str>,
        role: &str,
        content: Value,
        call_id: Option<&str>,
        invocation: Option<&str>,
    ) -> String {
        let id = format!("{}-msg-{}", self.sid, self.seq);
        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json, tool_call_id, \
             tool_invocation_id, run_id) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(&id)
        .bind(&self.sid)
        .bind(self.seq)
        .bind(role)
        .bind(content.to_string())
        .bind(call_id)
        .bind(invocation)
        .bind(run)
        .execute(&mut *self.conn)
        .await
        .unwrap();
        self.seq += 1;
        id
    }

    async fn run(
        &mut self,
        id: &str,
        turn: &str,
        attempt: i64,
        fingerprint: &str,
        status: &str,
        code: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO agent_runs(id, session_id, agent_id, turn_id, resume_attempt, \
             provider_id, model_id, provider_config_fingerprint, status, error_code) \
             VALUES(?1, ?2, ?3, ?4, ?5, 'fake', ?6, ?7, ?8, ?9)",
        )
        .bind(id)
        .bind(&self.sid)
        .bind(self.agent.0)
        .bind(turn)
        .bind(attempt)
        .bind(self.agent.1)
        .bind(fingerprint)
        .bind(status)
        .bind(code)
        .execute(&mut *self.conn)
        .await
        .unwrap();
    }

    /// Assistant message với tool calls `(provider call id, tool, text arg)`.
    async fn assistant_calls(&mut self, run: &str, calls: &[(&str, &str)]) -> String {
        let tool_calls: Vec<Value> = calls
            .iter()
            .map(|(id, tool)| json!({ "id": id, "name": tool, "arguments": args_of(tool) }))
            .collect();
        self.message(
            Some(run),
            "assistant",
            json!({ "text": "", "tool_calls": tool_calls }),
            None,
            None,
        )
        .await
    }

    /// `(invocation id, provider call id, tool, status)`.
    async fn invocation(
        &mut self,
        run: &str,
        assistant_msg: &str,
        inv: &str,
        call_id: &str,
        tool: &str,
        status: &str,
    ) {
        sqlx::query(
            "INSERT INTO tool_invocations(id, session_id, run_id, tool_call_id, \
             assistant_message_id, tool_name, arguments_json, status) \
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?8, ?7)",
        )
        .bind(inv)
        .bind(&self.sid)
        .bind(run)
        .bind(call_id)
        .bind(assistant_msg)
        .bind(tool)
        .bind(status)
        .bind(args_of(tool).to_string())
        .execute(&mut *self.conn)
        .await
        .unwrap();
    }

    async fn settle(&mut self, run: &str, inv: &str, call_id: &str, text: &str) {
        sqlx::query(
            "UPDATE tool_invocations SET status = 'completed', \
             result_json = '{\"content\":\"done\"}' WHERE id = ?1",
        )
        .bind(inv)
        .execute(&mut *self.conn)
        .await
        .unwrap();
        self.message(
            Some(run),
            "tool",
            json!({ "text": text, "is_error": false }),
            Some(call_id),
            Some(inv),
        )
        .await;
    }

    async fn finish(self) {
        sqlx::query("UPDATE sessions SET next_sequence = ?2 WHERE id = ?1")
            .bind(&self.sid)
            .bind(self.seq)
            .execute(&mut *self.conn)
            .await
            .unwrap();
    }
}

async fn one(conn: &mut SqliteConnection, sql: &str, bind: &str) -> String {
    sqlx::query(sql)
        .bind(bind)
        .fetch_one(&mut *conn)
        .await
        .unwrap()
        .get::<String, _>(0)
}

async fn count(conn: &mut SqliteConnection, sql: &str, bind: &str) -> i64 {
    sqlx::query(sql)
        .bind(bind)
        .fetch_one(&mut *conn)
        .await
        .unwrap()
        .get(0)
}

/// Tạo session, đóng engine (store rảnh) để seed trạng thái "đã chết"; trả về session id +
/// fingerprint hiện hành.
async fn fresh_session(
    env: &Env,
    tools: Vec<Arc<dyn HostTool>>,
    names: &[&str],
) -> (String, String) {
    let gw = ScriptedGateway::new();
    let o = build(env.config(env.agents(names)), &gw, tools)
        .await
        .unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let fp = o.provider_fingerprint("fake").unwrap().to_owned();
    o.shutdown().await.unwrap();
    (sid, fp)
}

fn has_pair(req: &ModelRequest, call_id: &str, text: &str) -> bool {
    req.messages.iter().any(|m| {
        m.role == ModelRole::Tool
            && m.tool_call_id.as_deref() == Some(call_id)
            && m.text.contains(text)
    })
}

// ───────────────────────── Startup recovery: safe paths ─────────────────────────

#[tokio::test]
async fn at17_at23_stale_run_at_safe_point_resumes_same_turn_with_bounded_attempt() {
    let env = Env::new();
    let (sid, fp) = fresh_session(&env, vec![], &[]).await;
    {
        let mut c = env.sql().await;
        let mut s = Seed::new(&mut c, &sid);
        s.user("hello").await;
        s.run("r0", "turn-1", 0, &fp, "running", None).await;
        s.finish().await;
    }
    let gw = ScriptedGateway::new();
    gw.push_text("recovered");
    let o = build(env.config(env.agents(&[])), &gw, vec![])
        .await
        .unwrap();
    let r = wait_terminal(&o, &sid, "turn-1").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.attempt, 1, "resume_attempt incremented");
    assert_eq!(r.output.as_deref(), Some("recovered"));
    assert_eq!(gw.call_count(), 1);

    let mut c = env.sql().await;
    let old = sqlx::query("SELECT status, error_code FROM agent_runs WHERE id = 'r0'")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(old.get::<String, _>(0), "interrupted");
    assert_eq!(old.get::<String, _>(1), "INTERRUPTED");
    assert_eq!(
        count(
            &mut c,
            "SELECT COUNT(*) FROM agent_runs WHERE turn_id = ?1",
            "turn-1"
        )
        .await,
        2,
        "same turn_id, exactly one new attempt"
    );
    let texts: Vec<_> = history(&o, &sid)
        .await
        .into_iter()
        .map(|m| m.text)
        .collect();
    assert_eq!(texts, ["hello", "recovered"], "no duplicate history");
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn at17_settled_tool_results_are_kept_and_the_tool_is_not_rerun() {
    let env = Env::new();
    let (ro, ro_runs) = Probe::new("ro", ToolEffect::ReadOnly, Behavior::Echo);
    let (mt, mt_runs) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);
    let (sid, fp) = fresh_session(&env, vec![ro.clone(), mt.clone()], &["ro", "mt"]).await;
    {
        let mut c = env.sql().await;
        let mut s = Seed::new(&mut c, &sid);
        s.user("go").await;
        s.run("r0", "turn-1", 0, &fp, "running", None).await;
        let a = s.assistant_calls("r0", &[("c1", "mt")]).await;
        s.invocation("r0", &a, "inv1", "c1", "mt", "running").await;
        s.settle("r0", "inv1", "c1", "mt:settled-before-crash")
            .await;
        s.finish().await;
    }
    let gw = ScriptedGateway::new();
    gw.push_text("all good");
    let o = build(env.config(env.agents(&["ro", "mt"])), &gw, vec![ro, mt])
        .await
        .unwrap();
    let r = wait_terminal(&o, &sid, "turn-1").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(
        mt_runs.load(Ordering::SeqCst),
        0,
        "settled mutation not replayed"
    );
    assert_eq!(ro_runs.load(Ordering::SeqCst), 0);
    assert!(has_pair(
        &gw.agent_requests()[0],
        "c1",
        "mt:settled-before-crash"
    ));
    assert_eq!(tool_texts(&history(&o, &sid).await).len(), 1);
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn replay_safe_and_never_started_invocations_run_once_in_provider_order() {
    let env = Env::new();
    let (ro, ro_runs) = Probe::new("ro", ToolEffect::ReadOnly, Behavior::Echo);
    let (mt, mt_runs) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);
    let (sid, fp) = fresh_session(&env, vec![ro.clone(), mt.clone()], &["ro", "mt"]).await;
    {
        let mut c = env.sql().await;
        let mut s = Seed::new(&mut c, &sid);
        s.user("go").await;
        s.run("r0", "turn-1", 0, &fp, "running", None).await;
        let a = s.assistant_calls("r0", &[("c1", "ro"), ("c2", "mt")]).await;
        // ReadOnly đang chạy lúc crash (replay-safe); Mutating chưa từng bắt đầu.
        s.invocation("r0", &a, "inv1", "c1", "ro", "running").await;
        s.invocation("r0", &a, "inv2", "c2", "mt", "requested")
            .await;
        s.finish().await;
    }
    let gw = ScriptedGateway::new();
    gw.push_text("done");
    let o = build(env.config(env.agents(&["ro", "mt"])), &gw, vec![ro, mt])
        .await
        .unwrap();
    let r = wait_terminal(&o, &sid, "turn-1").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(ro_runs.load(Ordering::SeqCst), 1);
    assert_eq!(mt_runs.load(Ordering::SeqCst), 1);
    assert_eq!(tool_texts(&history(&o, &sid).await), ["ro:x", "mt:x"]);
    let req = &gw.agent_requests()[0];
    assert!(has_pair(req, "c1", "ro:x") && has_pair(req, "c2", "mt:x"));
    // Message tool-result thuộc attempt mới (lineage), assistant gốc thuộc attempt cũ.
    let mut c = env.sql().await;
    let result_run = one(
        &mut c,
        "SELECT run_id FROM messages WHERE session_id = ?1 AND role = 'tool' LIMIT 1",
        &sid,
    )
    .await;
    assert_ne!(result_run, "r0");
    o.shutdown().await.unwrap();
}

// ───────────────────────── Startup recovery: blocked paths ─────────────────────────

/// Dựng `running` Mutating invocation không có result (crash giữa effect và result).
async fn seed_unknown_mutation(env: &Env, sid: &str, fp: &str) {
    let mut c = env.sql().await;
    let mut s = Seed::new(&mut c, sid);
    s.user("charge the card").await;
    s.run("r0", "turn-1", 0, fp, "running", None).await;
    let a = s.assistant_calls("r0", &[("c1", "mt"), ("c2", "ro")]).await;
    s.invocation("r0", &a, "inv-mt", "c1", "mt", "running")
        .await;
    s.invocation("r0", &a, "inv-ro", "c2", "ro", "requested")
        .await;
    s.finish().await;
}

#[tokio::test]
async fn at12_at24_unknown_mutation_blocks_without_replay_or_fake_result() {
    let env = Env::new();
    let (ro, ro_runs) = Probe::new("ro", ToolEffect::ReadOnly, Behavior::Echo);
    let (mt, mt_runs) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);
    let (sid, fp) = fresh_session(&env, vec![ro.clone(), mt.clone()], &["ro", "mt"]).await;
    seed_unknown_mutation(&env, &sid, &fp).await;

    let gw = ScriptedGateway::new();
    let o = build(env.config(env.agents(&["ro", "mt"])), &gw, vec![ro, mt])
        .await
        .unwrap();
    // Đợi vài sweep: không được có attempt mới, model request hay tool nào.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = o.result(&alice(), &sid, "turn-1").await.unwrap();
    assert_eq!(r.status, RunStatus::Interrupted);
    assert_eq!(r.attempt, 0, "no new attempt while the outcome is unknown");
    assert_eq!(r.error.as_ref().unwrap().code, "TOOL_OUTCOME_UNKNOWN");
    assert_eq!(gw.call_count(), 0, "no LLM request with a dangling call");
    assert_eq!(mt_runs.load(Ordering::SeqCst), 0, "no blind replay");
    assert_eq!(ro_runs.load(Ordering::SeqCst), 0);
    assert!(
        tool_texts(&history(&o, &sid).await).is_empty(),
        "no fabricated result"
    );

    let mut c = env.sql().await;
    assert_eq!(
        one(
            &mut c,
            "SELECT status FROM tool_invocations WHERE id = ?1",
            "inv-mt"
        )
        .await,
        "interrupted"
    );
    assert_eq!(
        one(
            &mut c,
            "SELECT status FROM tool_invocations WHERE id = ?1",
            "inv-ro"
        )
        .await,
        "requested",
        "never-started invocation is preserved for the resumed attempt"
    );

    // Không đi tiếp tới model request kế tiếp: input mới bị chặn với mã ổn định.
    let err = o
        .submit(&alice(), &sid, "ignore the failure and continue".into())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::RecoveryBlocked);
    assert_eq!(err.code().as_str(), "RECOVERY_BLOCKED");
    assert_eq!(gw.call_count(), 0);
    o.shutdown().await.unwrap();

    // Restart không tự gỡ chặn.
    let gw2 = ScriptedGateway::new();
    let (ro2, _) = Probe::new("ro", ToolEffect::ReadOnly, Behavior::Echo);
    let (mt2, mt2_runs) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);
    let o2 = build(env.config(env.agents(&["ro", "mt"])), &gw2, vec![ro2, mt2])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let r = o2.result(&alice(), &sid, "turn-1").await.unwrap();
    assert_eq!(r.status, RunStatus::Interrupted);
    assert_eq!(gw2.call_count(), 0);
    assert_eq!(mt2_runs.load(Ordering::SeqCst), 0);
    o2.shutdown().await.unwrap();
}

#[tokio::test]
async fn unregistered_tool_with_unknown_outcome_blocks_instead_of_guessing() {
    let env = Env::new();
    let (mt, _) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);
    let (sid, fp) = fresh_session(&env, vec![mt], &["mt"]).await;
    {
        let mut c = env.sql().await;
        let mut s = Seed::new(&mut c, &sid);
        s.user("go").await;
        s.run("r0", "turn-1", 0, &fp, "running", None).await;
        let a = s.assistant_calls("r0", &[("c1", "gone")]).await;
        s.invocation("r0", &a, "inv", "c1", "gone", "running").await;
        s.finish().await;
    }
    let gw = ScriptedGateway::new();
    let o = build(env.config(env.agents(&[])), &gw, vec![])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let r = o.result(&alice(), &sid, "turn-1").await.unwrap();
    assert_eq!(r.status, RunStatus::Interrupted);
    assert_eq!(r.error.unwrap().code, "TOOL_OUTCOME_UNKNOWN");
    assert_eq!(gw.call_count(), 0);
    o.shutdown().await.unwrap();
}

// ───────────────────────── Operator resolution (AT-49) ─────────────────────────

async fn blocked_env() -> (
    Env,
    String,
    Arc<ScriptedGateway>,
    Orchestrator,
    Arc<AtomicUsize>,
) {
    let env = Env::new();
    let (ro, _) = Probe::new("ro", ToolEffect::ReadOnly, Behavior::Echo);
    let (mt, mt_runs) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);
    let (sid, fp) = fresh_session(&env, vec![ro.clone(), mt.clone()], &["ro", "mt"]).await;
    seed_unknown_mutation(&env, &sid, &fp).await;
    let gw = ScriptedGateway::new();
    let o = build(env.config(env.agents(&["ro", "mt"])), &gw, vec![ro, mt])
        .await
        .unwrap();
    (env, sid, gw, o, mt_runs)
}

#[tokio::test]
async fn at49_confirmed_completed_commits_audit_and_result_then_resumes_without_replay() {
    let (env, sid, gw, o, mt_runs) = blocked_env().await;
    gw.push_text("the card was charged");
    o.resolve_interrupted_invocation(
        &operator(),
        &sid,
        "inv-mt",
        VerifiedToolResolution::ConfirmedCompleted {
            outcome: ToolOutcome {
                call_id: "c1".into(),
                content: "charge receipt R-77".into(),
                metadata: Value::Null,
            },
            evidence: "verified in the payment dashboard".into(),
        },
    )
    .await
    .unwrap();

    let r = wait_terminal(&o, &sid, "turn-1").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.attempt, 1);
    assert_eq!(
        mt_runs.load(Ordering::SeqCst),
        0,
        "mutation is never replayed"
    );
    // Result của operator và result của invocation `requested` còn lại, đúng pairing.
    assert_eq!(
        tool_texts(&history(&o, &sid).await),
        ["charge receipt R-77", "ro:x"]
    );
    let req = &gw.agent_requests()[0];
    assert!(has_pair(req, "c1", "charge receipt R-77"));
    assert!(has_pair(req, "c2", "ro:x"));
    assert_eq!(gw.call_count(), 1);

    let mut c = env.sql().await;
    let row = sqlx::query(
        "SELECT status, resolved_by, resolution_evidence, resolved_at FROM tool_invocations \
         WHERE id = 'inv-mt'",
    )
    .fetch_one(&mut c)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>(0), "completed");
    assert_eq!(row.get::<String, _>(1), "op-1");
    assert_eq!(row.get::<String, _>(2), "verified in the payment dashboard");
    assert!(row.get::<Option<i64>, _>(3).is_some());
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn at49_confirmed_no_effect_is_a_technical_failure_and_not_replayed() {
    let (env, sid, gw, o, mt_runs) = blocked_env().await;
    gw.push_text("understood, it did not run");
    o.resolve_interrupted_invocation(
        &operator(),
        &sid,
        "inv-mt",
        VerifiedToolResolution::ConfirmedNoEffect {
            evidence: "no charge appears in the ledger".into(),
        },
    )
    .await
    .unwrap();
    let r = wait_terminal(&o, &sid, "turn-1").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(mt_runs.load(Ordering::SeqCst), 0, "no automatic replay");
    let texts = tool_texts(&history(&o, &sid).await);
    assert!(texts[0].contains("TOOL_NO_EFFECT_CONFIRMED"), "{texts:?}");
    assert!(has_pair(
        &gw.agent_requests()[0],
        "c1",
        "TOOL_NO_EFFECT_CONFIRMED"
    ));
    let mut c = env.sql().await;
    let row = sqlx::query("SELECT status, error_code FROM tool_invocations WHERE id = 'inv-mt'")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>(0), "failed");
    assert_eq!(row.get::<String, _>(1), "TOOL_NO_EFFECT_CONFIRMED");
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn at49_resolution_is_validated_and_rejected_requests_change_nothing() {
    let (env, sid, gw, o, _) = blocked_env().await;
    let completed =
        |call_id: &str, content: &str, evidence: &str| VerifiedToolResolution::ConfirmedCompleted {
            outcome: ToolOutcome {
                call_id: call_id.into(),
                content: content.into(),
                metadata: Value::Null,
            },
            evidence: evidence.into(),
        };
    let messages_before = {
        let mut c = env.sql().await;
        count(
            &mut c,
            "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
            &sid,
        )
        .await
    };
    let denied = |e: OrchestratorError, code: ErrorCode| {
        assert_eq!(e.code(), code, "{e}");
    };

    // Không có capability của operator.
    let plain = OperatorContext::new("op-1", "alice", "ws1");
    denied(
        o.resolve_interrupted_invocation(&plain, &sid, "inv-mt", completed("c1", "r", "e"))
            .await
            .unwrap_err(),
        ErrorCode::PermissionDenied,
    );
    // Sai owner / workspace.
    for other in [
        OperatorContext::new("op-1", "mallory", "ws1").with_resolve_capability(),
        OperatorContext::new("op-1", "alice", "ws2").with_resolve_capability(),
    ] {
        denied(
            o.resolve_interrupted_invocation(&other, &sid, "inv-mt", completed("c1", "r", "e"))
                .await
                .unwrap_err(),
            ErrorCode::PermissionDenied,
        );
    }
    // Chỉ nhận internal invocation ID: provider call ID / ID lạ → NOT_FOUND.
    for bad in ["c1", "no-such-invocation"] {
        denied(
            o.resolve_interrupted_invocation(&operator(), &sid, bad, completed("c1", "r", "e"))
                .await
                .unwrap_err(),
            ErrorCode::NotFound,
        );
    }
    // Invocation của session khác không resolve được qua session này.
    denied(
        o.resolve_interrupted_invocation(
            &operator(),
            "other-session",
            "inv-mt",
            completed("c1", "r", "e"),
        )
        .await
        .unwrap_err(),
        ErrorCode::NotFound,
    );
    // Outcome không thuộc invocation, evidence rỗng/điều khiển, outcome quá cỡ.
    denied(
        o.resolve_interrupted_invocation(&operator(), &sid, "inv-mt", completed("c2", "r", "e"))
            .await
            .unwrap_err(),
        ErrorCode::InvalidInput,
    );
    for evidence in ["", "   ", "bad\u{7}evidence"] {
        denied(
            o.resolve_interrupted_invocation(
                &operator(),
                &sid,
                "inv-mt",
                completed("c1", "r", evidence),
            )
            .await
            .unwrap_err(),
            ErrorCode::InvalidInput,
        );
    }
    denied(
        o.resolve_interrupted_invocation(
            &operator(),
            &sid,
            "inv-mt",
            completed("c1", &"x".repeat(9 * 1024 * 1024), "e"),
        )
        .await
        .unwrap_err(),
        ErrorCode::InvalidInput,
    );
    // Invocation chưa `interrupted` (requested) không phải đối tượng reconcile.
    denied(
        o.resolve_interrupted_invocation(&operator(), &sid, "inv-ro", completed("c2", "r", "e"))
            .await
            .unwrap_err(),
        ErrorCode::InvalidInput,
    );

    // Không có gì bị ghi, run vẫn bị chặn, model chưa được gọi.
    let mut c = env.sql().await;
    assert_eq!(
        count(
            &mut c,
            "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
            &sid
        )
        .await,
        messages_before
    );
    assert_eq!(
        one(
            &mut c,
            "SELECT status FROM tool_invocations WHERE id = ?1",
            "inv-mt"
        )
        .await,
        "interrupted"
    );
    assert_eq!(
        o.result(&alice(), &sid, "turn-1").await.unwrap().status,
        RunStatus::Interrupted
    );
    assert_eq!(gw.call_count(), 0);

    // Hợp lệ → resolve; resolve lần hai bị từ chối (không còn chờ).
    gw.push_text("ok");
    o.resolve_interrupted_invocation(
        &operator(),
        &sid,
        "inv-mt",
        completed("c1", "receipt", "seen"),
    )
    .await
    .unwrap();
    denied(
        o.resolve_interrupted_invocation(
            &operator(),
            &sid,
            "inv-mt",
            completed("c1", "again", "seen"),
        )
        .await
        .unwrap_err(),
        ErrorCode::InvalidInput,
    );
    wait_terminal(&o, &sid, "turn-1").await;
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn at49_resume_waits_until_every_unknown_mutation_is_resolved() {
    let env = Env::new();
    let (mt, mt_runs) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);
    let (sid, fp) = fresh_session(&env, vec![mt.clone()], &["mt"]).await;
    {
        let mut c = env.sql().await;
        let mut s = Seed::new(&mut c, &sid);
        s.user("two writes").await;
        s.run("r0", "turn-1", 0, &fp, "running", None).await;
        let a = s.assistant_calls("r0", &[("c1", "mt"), ("c2", "mt")]).await;
        s.invocation("r0", &a, "inv1", "c1", "mt", "running").await;
        s.invocation("r0", &a, "inv2", "c2", "mt", "interrupted")
            .await;
        s.finish().await;
    }
    let gw = ScriptedGateway::new();
    gw.push_text("both resolved");
    let o = build(env.config(env.agents(&["mt"])), &gw, vec![mt])
        .await
        .unwrap();
    let resolve = |inv: &'static str, call: &'static str| {
        let o = &o;
        let sid = sid.clone();
        async move {
            o.resolve_interrupted_invocation(
                &operator(),
                &sid,
                inv,
                VerifiedToolResolution::ConfirmedCompleted {
                    outcome: ToolOutcome {
                        call_id: call.into(),
                        content: format!("{inv}-done"),
                        metadata: Value::Null,
                    },
                    evidence: "checked".into(),
                },
            )
            .await
            .unwrap();
        }
    };
    resolve("inv1", "c1").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = o.result(&alice(), &sid, "turn-1").await.unwrap();
    assert_eq!(r.status, RunStatus::Interrupted, "still blocked by inv2");
    assert_eq!(r.attempt, 0);
    assert_eq!(gw.call_count(), 0);

    resolve("inv2", "c2").await;
    let r = wait_terminal(&o, &sid, "turn-1").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(mt_runs.load(Ordering::SeqCst), 0);
    assert_eq!(
        tool_texts(&history(&o, &sid).await),
        ["inv1-done", "inv2-done"]
    );
    o.shutdown().await.unwrap();
}

// ───────────────────────── Bounded attempts ─────────────────────────

#[tokio::test]
async fn recovery_attempts_are_bounded_and_exhaustion_needs_attention() {
    let env = Env::new();
    let (sid, fp) = fresh_session(&env, vec![], &[]).await;
    {
        let mut c = env.sql().await;
        let mut s = Seed::new(&mut c, &sid);
        s.user("loop").await;
        s.run("r2", "turn-1", 2, &fp, "running", None).await;
        s.finish().await;
    }
    let mut cfg = env.config(env.agents(&[]));
    cfg.runtime.max_resume_attempts = 2;
    let gw = ScriptedGateway::new();
    let o = build(cfg, &gw, vec![]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = o.result(&alice(), &sid, "turn-1").await.unwrap();
    assert_eq!(r.status, RunStatus::Interrupted);
    assert_eq!(r.attempt, 2, "no attempt beyond the cap");
    assert_eq!(r.error.unwrap().code, "RECOVERY_ATTEMPTS_EXHAUSTED");
    assert_eq!(gw.call_count(), 0);
    o.shutdown().await.unwrap();

    // Restart không thử lại vô hạn.
    let gw = ScriptedGateway::new();
    let mut cfg = env.config(env.agents(&[]));
    cfg.runtime.max_resume_attempts = 2;
    let o = build(cfg, &gw, vec![]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(o.result(&alice(), &sid, "turn-1").await.unwrap().attempt, 2);
    assert_eq!(gw.call_count(), 0);
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_crash_loop_stops_after_the_configured_number_of_attempts() {
    let env = Env::new();
    let (sid, fp) = fresh_session(&env, vec![], &[]).await;
    {
        let mut c = env.sql().await;
        let mut s = Seed::new(&mut c, &sid);
        s.user("loop").await;
        s.run("r0", "turn-1", 0, &fp, "running", None).await;
        s.finish().await;
    }
    // Mỗi lần khởi động: attempt mới bị "crash" (gateway chặn rồi engine bị drop trong lúc
    // run còn running qua SQL) — mô phỏng bằng cách đánh dấu lại running sau shutdown.
    for expected_attempt in 1..=2 {
        let mut cfg = env.config(env.agents(&[]));
        cfg.runtime.max_resume_attempts = 2;
        let gw = ScriptedGateway::new();
        gw.hold();
        let o = build(cfg, &gw, vec![]).await.unwrap();
        wait_until(|| gw.call_count() == 1).await;
        let r = o.result(&alice(), &sid, "turn-1").await.unwrap();
        assert_eq!(r.attempt, expected_attempt);
        o.shutdown().await.unwrap();
        // Process "chết" trước khi shutdown kịp ghi: run lại ở trạng thái running.
        let mut c = env.sql().await;
        sqlx::query(
            "UPDATE agent_runs SET status = 'running', error_code = NULL, error_message = NULL, \
             finished_at = NULL WHERE session_id = ?1 AND resume_attempt = ?2",
        )
        .bind(&sid)
        .bind(expected_attempt)
        .execute(&mut c)
        .await
        .unwrap();
    }
    let mut cfg = env.config(env.agents(&[]));
    cfg.runtime.max_resume_attempts = 2;
    let gw = ScriptedGateway::new();
    let o = build(cfg, &gw, vec![]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let r = o.result(&alice(), &sid, "turn-1").await.unwrap();
    assert_eq!(r.attempt, 2);
    assert_eq!(r.status, RunStatus::Interrupted);
    assert_eq!(r.error.unwrap().code, "RECOVERY_ATTEMPTS_EXHAUSTED");
    assert_eq!(gw.call_count(), 0);
    o.shutdown().await.unwrap();
}

// ───────────────────────── Fingerprint / rotation ─────────────────────────

async fn stale_with_fingerprint_of(env: &Env, endpoint: &str) -> String {
    let gw = ScriptedGateway::new();
    let o = build(
        env.config_with(env.agents(&[]), endpoint, "key-1"),
        &gw,
        vec![],
    )
    .await
    .unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let fp = o.provider_fingerprint("fake").unwrap().to_owned();
    o.shutdown().await.unwrap();
    let mut c = env.sql().await;
    let mut s = Seed::new(&mut c, &sid);
    s.user("hi").await;
    s.run("r0", "turn-1", 0, &fp, "running", None).await;
    s.finish().await;
    sid
}

#[tokio::test]
async fn changed_routing_identity_blocks_recovery_but_reads_stay_truthful() {
    let env = Env::new();
    let sid = stale_with_fingerprint_of(&env, "http://127.0.0.1:9/v1").await;

    let gw = ScriptedGateway::new();
    gw.push_text("never");
    let o = build(
        env.config_with(env.agents(&[]), "http://127.0.0.1:10/v1", "key-1"),
        &gw,
        vec![],
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = o.result(&alice(), &sid, "turn-1").await.unwrap();
    assert_eq!(r.status, RunStatus::Interrupted);
    assert_eq!(r.attempt, 0);
    let e = r.error.unwrap();
    assert_eq!(e.code, "PROVIDER_CONFIG_CHANGED");
    assert!(
        !e.message.contains("127.0.0.1"),
        "no endpoint in messages: {}",
        e.message
    );
    assert_eq!(gw.call_count(), 0);

    // Turn mới sau restart dùng cấu hình hiện hành (fingerprint mới ghi vào attempt mới).
    let t = o.submit(&alice(), &sid, "new turn".into()).await.unwrap();
    let r = wait_terminal(&o, &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn restoring_the_original_routing_identity_unblocks_recovery() {
    let env = Env::new();
    let sid = stale_with_fingerprint_of(&env, "http://127.0.0.1:9/v1").await;
    let changed = ScriptedGateway::new();
    let o = build(
        env.config_with(env.agents(&[]), "http://127.0.0.1:10/v1", "key-1"),
        &changed,
        vec![],
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        o.result(&alice(), &sid, "turn-1")
            .await
            .unwrap()
            .error
            .unwrap()
            .code,
        "PROVIDER_CONFIG_CHANGED"
    );
    o.shutdown().await.unwrap();

    let gw = ScriptedGateway::new();
    gw.push_text("back on the original endpoint");
    let o = build(
        env.config_with(env.agents(&[]), "http://127.0.0.1:9/v1", "key-1"),
        &gw,
        vec![],
    )
    .await
    .unwrap();
    let r = wait_terminal(&o, &sid, "turn-1").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.attempt, 1);
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn credential_rotation_alone_does_not_block_recovery() {
    let env = Env::new();
    let sid = stale_with_fingerprint_of(&env, "http://127.0.0.1:9/v1").await;
    let gw = ScriptedGateway::new();
    gw.push_text("rotated and resumed");
    let o = build(
        env.config_with(
            env.agents(&[]),
            "http://127.0.0.1:9/v1",
            "a-completely-new-key",
        ),
        &gw,
        vec![],
    )
    .await
    .unwrap();
    let r = wait_terminal(&o, &sid, "turn-1").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.attempt, 1);
    // Fingerprint không đổi vì key không thuộc routing identity.
    let mut c = env.sql().await;
    let fps = sqlx::query(
        "SELECT DISTINCT provider_config_fingerprint FROM agent_runs WHERE session_id = ?1",
    )
    .bind(&sid)
    .fetch_all(&mut c)
    .await
    .unwrap();
    assert_eq!(fps.len(), 1);
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn changed_model_for_the_same_alias_also_blocks_recovery() {
    let env = Env::new();
    let sid = stale_with_fingerprint_of(&env, "http://127.0.0.1:9/v1").await;
    let gw = ScriptedGateway::new();
    let agents = vec![env.root("a-different-model", &[]), env.reviewer(&[])];
    let o = build(env.config(agents), &gw, vec![]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let r = o.result(&alice(), &sid, "turn-1").await.unwrap();
    assert_eq!(r.status, RunStatus::Interrupted);
    assert_eq!(r.error.unwrap().code, "PROVIDER_CONFIG_CHANGED");
    assert_eq!(gw.call_count(), 0);
    o.shutdown().await.unwrap();
}

// ───────────────────────── AT-42: lineage ─────────────────────────

#[tokio::test]
async fn at42_resumed_attempt_never_borrows_the_prior_attempts_assistant_text() {
    let env = Env::new();
    let (ro, _) = Probe::new("ro", ToolEffect::ReadOnly, Behavior::Echo);
    let (sid, fp) = fresh_session(&env, vec![ro.clone()], &["ro"]).await;
    {
        let mut c = env.sql().await;
        let mut s = Seed::new(&mut c, &sid);
        s.user("question").await;
        s.run("r0", "turn-1", 0, &fp, "running", None).await;
        // Attempt cũ đã có assistant message (kèm text) trước khi chết.
        s.message(
            Some("r0"),
            "assistant",
            json!({ "text": "STALE TEXT FROM THE PRIOR ATTEMPT" }),
            None,
            None,
        )
        .await;
        s.finish().await;
    }
    let gw = ScriptedGateway::new();
    gw.hold();
    gw.push_text("fresh final");
    let o = build(env.config(env.agents(&["ro"])), &gw, vec![ro])
        .await
        .unwrap();
    wait_until(|| gw.call_count() == 1).await;
    let r = o.result(&alice(), &sid, "turn-1").await.unwrap();
    assert_eq!(r.status, RunStatus::Running);
    assert_eq!(r.attempt, 1);
    assert_eq!(r.output, None, "attempt in flight has no output");
    assert_eq!(r.final_message_id, None);
    gw.release();
    let r = wait_terminal(&o, &sid, "turn-1").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.output.as_deref(), Some("fresh final"));
    let mut c = env.sql().await;
    let owner_run = one(
        &mut c,
        "SELECT run_id FROM messages WHERE id = ?1",
        r.final_message_id.as_ref().unwrap(),
    )
    .await;
    assert_eq!(
        owner_run, r.run_id,
        "final pointer belongs to the terminal attempt"
    );
    assert_ne!(owner_run, "r0");
    o.shutdown().await.unwrap();
}

// ───────────────────────── Cancel ─────────────────────────

fn sub_new(prompt: &str) -> Value {
    json!({ "agent": "reviewer", "description": "task", "prompt": prompt })
}

#[tokio::test]
async fn cancel_stops_the_active_run_as_cancelled_and_keeps_the_session_usable() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let o = build(env.config(env.agents(&[])), &gw, vec![])
        .await
        .unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    gw.hold();
    let t = o.submit(&alice(), &sid, "slow".into()).await.unwrap();
    wait_until(|| gw.call_count() == 1).await;

    // Người khác không hủy được; idle session thì cancel là no-op.
    let intruder = CallerContext::new("mallory", "ws1");
    assert_eq!(
        o.cancel(&intruder, &sid).await.unwrap_err().code(),
        ErrorCode::PermissionDenied
    );
    o.cancel(&alice(), &sid).await.unwrap();
    let r = wait_terminal(&o, &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Cancelled);
    assert_eq!(r.error.unwrap().code, "CANCELLED");
    assert_eq!(r.output, None);
    o.cancel(&alice(), &sid).await.unwrap(); // idempotent

    gw.release();
    gw.push_text("after cancel");
    let t2 = o.submit(&alice(), &sid, "again".into()).await.unwrap();
    assert_eq!(
        wait_terminal(&o, &sid, &t2.turn_id).await.status,
        RunStatus::Completed
    );
    // Cancel là terminal: restart không tự resume nó.
    o.shutdown().await.unwrap();
    let gw2 = ScriptedGateway::new();
    let o2 = build(env.config(env.agents(&[])), &gw2, vec![])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        o2.result(&alice(), &sid, &t.turn_id).await.unwrap().status,
        RunStatus::Cancelled
    );
    assert_eq!(gw2.call_count(), 0);
    o2.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancel_of_a_parent_propagates_to_the_active_child_without_fake_results() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("long job"))]);
    // Request của child bị chặn ở gateway (tới khi bị hủy).
    gw.push_delayed(
        Duration::from_secs(120),
        ModelResponse {
            text: "never".into(),
            finish_reason: "end_turn".into(),
            tool_calls: Vec::new(),
            usage: None,
        },
    );
    let o = build(env.config(env.agents(&["subagent"])), &gw, vec![])
        .await
        .unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let t = o.submit(&alice(), &sid, "delegate".into()).await.unwrap();
    wait_until(|| gw.call_count() == 2).await;

    o.cancel(&alice(), &sid).await.unwrap();
    let r = wait_terminal(&o, &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Cancelled);

    let mut c = env.sql().await;
    let child = sqlx::query(
        "SELECT r.status, r.error_code FROM agent_runs r JOIN sessions s ON s.id = r.session_id \
         WHERE s.parent_session_id = ?1",
    )
    .bind(&sid)
    .fetch_one(&mut c)
    .await
    .unwrap();
    // Child cũng được hủy (đợi nó ghi terminal).
    let mut child_status: String = child.get(0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while child_status == "running" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "child still running"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        child_status = sqlx::query(
            "SELECT r.status FROM agent_runs r JOIN sessions s ON s.id = r.session_id \
             WHERE s.parent_session_id = ?1",
        )
        .bind(&sid)
        .fetch_one(&mut c)
        .await
        .unwrap()
        .get(0);
    }
    assert_eq!(child_status, "cancelled");
    let inv =
        sqlx::query("SELECT status, child_session_id FROM tool_invocations WHERE session_id = ?1")
            .bind(&sid)
            .fetch_one(&mut c)
            .await
            .unwrap();
    assert_eq!(inv.get::<String, _>(0), "cancelled");
    assert!(inv.get::<Option<String>, _>(1).is_some(), "child link kept");
    assert!(tool_texts(&history(&o, &sid).await).is_empty());
    assert_eq!(gw.call_count(), 2, "no further model request after cancel");
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelling_only_the_child_returns_a_technical_result_to_the_parent() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("job"))]);
    gw.push_delayed(
        Duration::from_secs(120),
        ModelResponse {
            text: "never".into(),
            finish_reason: "end_turn".into(),
            tool_calls: Vec::new(),
            usage: None,
        },
    );
    gw.push_text("parent saw the child cancelled");
    let o = build(env.config(env.agents(&["subagent"])), &gw, vec![])
        .await
        .unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let t = o.submit(&alice(), &sid, "delegate".into()).await.unwrap();
    wait_until(|| gw.call_count() == 2).await;
    let mut c = env.sql().await;
    let child_sid: String = sqlx::query("SELECT id FROM sessions WHERE parent_session_id = ?1")
        .bind(&sid)
        .fetch_one(&mut c)
        .await
        .unwrap()
        .get(0);
    o.cancel(&alice(), &child_sid).await.unwrap();
    let r = wait_terminal(&o, &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    let texts = tool_texts(&history(&o, &sid).await);
    assert!(texts[0].contains("CHILD_CANCELLED"), "{texts:?}");
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancel_lets_a_cooperative_mutating_tool_stop_and_never_replays_it() {
    let env = Env::new();
    let (mt, runs) = Probe::new("mt", ToolEffect::Mutating, Behavior::UntilCancelled);
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "mt", json!({"text": "a"}))]);
    gw.push_text("never");
    let o = build(env.config(env.agents(&["mt"])), &gw, vec![mt])
        .await
        .unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let t = o.submit(&alice(), &sid, "go".into()).await.unwrap();
    wait_until(|| runs.load(Ordering::SeqCst) == 1).await;
    o.cancel(&alice(), &sid).await.unwrap();
    let r = wait_terminal(&o, &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Cancelled);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(gw.call_count(), 1, "no LLM step after cancel");
    o.shutdown().await.unwrap();
}

// ───────────────────────── Shutdown lifecycle ─────────────────────────

#[tokio::test]
async fn submit_after_shutdown_is_rejected_before_any_commit() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let o = build(env.config(env.agents(&[])), &gw, vec![])
        .await
        .unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    o.shutdown().await.unwrap();

    for err in [
        o.submit(&alice(), &sid, "late".into()).await.unwrap_err(),
        o.create_session(&alice(), "root").await.unwrap_err(),
        o.cancel(&alice(), &sid).await.unwrap_err(),
    ] {
        assert_eq!(err.code(), ErrorCode::ShuttingDown);
    }
    let mut c = env.sql().await;
    assert_eq!(
        count(
            &mut c,
            "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
            &sid
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &mut c,
            "SELECT COUNT(*) FROM agent_runs WHERE session_id = ?1",
            &sid
        )
        .await,
        0
    );
    assert_eq!(
        count(&mut c, "SELECT COUNT(*) FROM sessions WHERE ?1 = ?1", &sid).await,
        1
    );
    assert_eq!(gw.call_count(), 0);
}

#[tokio::test]
async fn racing_submits_during_shutdown_are_either_rejected_or_fully_accounted_for() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let o = Arc::new(
        build(env.config(env.agents(&[])), &gw, vec![])
            .await
            .unwrap(),
    );
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let submit = {
        let o = o.clone();
        let sid = sid.clone();
        tokio::spawn(async move { o.submit(&alice(), &sid, "racing".into()).await })
    };
    o.shutdown().await.unwrap();
    let accepted = submit.await.unwrap();
    let mut c = env.sql().await;
    let running = count(
        &mut c,
        "SELECT COUNT(*) FROM agent_runs WHERE session_id = ?1 AND status = 'running'",
        &sid,
    )
    .await;
    assert_eq!(running, 0, "no claim is left running without an owner");
    let runs = count(
        &mut c,
        "SELECT COUNT(*) FROM agent_runs WHERE session_id = ?1",
        &sid,
    )
    .await;
    match accepted {
        Ok(_) => assert_eq!(runs, 1),
        Err(e) => {
            assert_eq!(e.code(), ErrorCode::ShuttingDown);
            assert_eq!(runs, 0, "a rejected submit commits nothing");
        }
    }
}

#[tokio::test]
async fn shutdown_drain_lets_an_in_flight_run_finish_before_cancelling() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_delayed(
        Duration::from_millis(300),
        ModelResponse {
            text: "finished during drain".into(),
            finish_reason: "end_turn".into(),
            tool_calls: Vec::new(),
            usage: None,
        },
    );
    let mut cfg = env.config(env.agents(&[]));
    cfg.runtime.shutdown_drain = Duration::from_secs(5);
    let o = build(cfg, &gw, vec![]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let t = o.submit(&alice(), &sid, "slow".into()).await.unwrap();
    wait_until(|| gw.call_count() == 1).await;
    o.shutdown().await.unwrap();
    drop(o);
    let o = build(env.config(env.agents(&[])), &ScriptedGateway::new(), vec![])
        .await
        .unwrap();
    let r = o.result(&alice(), &sid, &t.turn_id).await.unwrap();
    assert_eq!(
        r.status,
        RunStatus::Completed,
        "drained, not interrupted: {r:?}"
    );
    assert_eq!(r.output.as_deref(), Some("finished during drain"));
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_drain_expiry_falls_back_to_cooperative_cancel() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let mut cfg = env.config(env.agents(&[]));
    cfg.runtime.shutdown_drain = Duration::from_millis(100);
    let o = build(cfg, &gw, vec![]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    gw.hold();
    let t = o.submit(&alice(), &sid, "stuck".into()).await.unwrap();
    wait_until(|| gw.call_count() == 1).await;
    let started = tokio::time::Instant::now();
    o.shutdown().await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "drain was honoured"
    );
    gw.release();
    drop(o);
    let o2 = build(env.config(env.agents(&[])), &gw, vec![])
        .await
        .unwrap();
    // Attempt bị ngắt được phục hồi (không mất turn).
    let r = wait_terminal(&o2, &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(r.attempt, 1);
    o2.shutdown().await.unwrap();
}

#[tokio::test]
async fn at41_non_cooperative_tool_keeps_the_owner_guard_and_shutdown_reports_incomplete() {
    let env = Env::new();
    let (mt, runs) = Probe::new(
        "mt",
        ToolEffect::Mutating,
        Behavior::Ignore(Duration::from_millis(1500)),
    );
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "mt", json!({"text": "a"}))]);
    gw.push_text("never");
    let mut cfg = env.config(env.agents(&["mt"]));
    cfg.runtime.shutdown_grace = Duration::from_millis(200);
    let o = build(cfg, &gw, vec![mt.clone()]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let t = o.submit(&alice(), &sid, "go".into()).await.unwrap();
    wait_until(|| runs.load(Ordering::SeqCst) == 1).await;

    let err = o.shutdown().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unavailable);
    assert!(err.message().contains("incomplete"), "{}", err.message());
    // Chưa báo stopped: run chưa terminal, owner guard còn giữ → engine khác không acquire được.
    {
        let mut c = env.sql().await;
        assert_eq!(
            one(
                &mut c,
                "SELECT status FROM agent_runs WHERE session_id = ?1",
                &sid
            )
            .await,
            "running"
        );
    }
    let (mt2, _) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);
    let other = build(
        env.config(env.agents(&["mt"])),
        &ScriptedGateway::new(),
        vec![mt2.clone()],
    )
    .await
    .err()
    .expect("owner guard must still be held");
    assert_eq!(other.code(), ErrorCode::StoreAlreadyOwned);
    // Engine đang shutdown vẫn từ chối admission mới.
    assert_eq!(
        o.submit(&alice(), &sid, "x".into())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::ShuttingDown
    );

    // Tool kết thúc muộn: retry shutdown thành công, lock được nhả, run interrupted có thể
    // phục hồi và result của tool đã ghi (không mất, không replay).
    tokio::time::sleep(Duration::from_millis(100)).await;
    for _ in 0..40 {
        if o.shutdown().await.is_ok() {
            break;
        }
    }
    o.shutdown().await.unwrap();
    drop(o);
    let (mt3, mt3_runs) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);
    let gw3 = ScriptedGateway::new();
    gw3.push_text("resumed after the late finish");
    let o3 = build(env.config(env.agents(&["mt"])), &gw3, vec![mt3])
        .await
        .unwrap();
    let r = wait_terminal(&o3, &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(
        mt3_runs.load(Ordering::SeqCst),
        0,
        "late-finished mutation is not replayed"
    );
    assert_eq!(tool_texts(&history(&o3, &sid).await), ["finished-late"]);
    o3.shutdown().await.unwrap();
}

#[tokio::test]
async fn at50_second_build_succeeds_only_after_a_safe_release() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let first = build(env.config(env.agents(&[])), &gw, vec![])
        .await
        .unwrap();
    let denied = build(env.config(env.agents(&[])), &gw, vec![])
        .await
        .err()
        .unwrap();
    assert_eq!(denied.code(), ErrorCode::StoreAlreadyOwned);
    first.shutdown().await.unwrap();
    let second = build(env.config(env.agents(&[])), &gw, vec![])
        .await
        .unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn runtime_config_validates_max_resume_attempts() {
    let env = Env::new();
    for bad in [0u32, 101] {
        let mut cfg = env.config(env.agents(&[]));
        cfg.runtime.max_resume_attempts = bad;
        let e = build(cfg, &ScriptedGateway::new(), vec![])
            .await
            .err()
            .unwrap();
        assert_eq!(
            e.reason().map(|r| r.as_str()),
            Some("INVALID_RUNTIME_CONFIG")
        );
    }
    for ok in [1u32, 10, 100] {
        let mut cfg = env.config(env.agents(&[]));
        cfg.runtime.max_resume_attempts = ok;
        let o = build(cfg, &ScriptedGateway::new(), vec![]).await.unwrap();
        o.shutdown().await.unwrap();
    }
}

// ───────────────────────── Parent / child recovery ─────────────────────────

#[tokio::test]
async fn blocked_child_blocks_the_parent_and_one_resolution_resumes_both() {
    let env = Env::new();
    let (mt, mt_runs) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);
    let agents = || vec![env.root("model-x", &["subagent"]), env.reviewer(&["mt"])];
    let (sid, fp) = {
        let o = build(
            env.config(agents()),
            &ScriptedGateway::new(),
            vec![mt.clone()],
        )
        .await
        .unwrap();
        let sid = o.create_session(&alice(), "root").await.unwrap();
        let fp = o.provider_fingerprint("fake").unwrap().to_owned();
        o.shutdown().await.unwrap();
        (sid, fp)
    };
    {
        let mut c = env.sql().await;
        sqlx::query(
            "INSERT INTO sessions(id, parent_session_id, agent_id, owner_id, workspace_id) \
             VALUES('child-1', ?1, 'reviewer', 'alice', 'ws1')",
        )
        .bind(&sid)
        .execute(&mut c)
        .await
        .unwrap();
        let mut child = Seed::new(&mut c, "child-1").for_reviewer();
        child.user("review the change").await;
        child
            .run("cr0", "child-turn", 0, &fp, "running", None)
            .await;
        let a = child.assistant_calls("cr0", &[("k1", "mt")]).await;
        child
            .invocation("cr0", &a, "inv-mt", "k1", "mt", "running")
            .await;
        child.finish().await;
        let mut parent = Seed::new(&mut c, &sid);
        parent.user("delegate").await;
        parent.run("r0", "turn-1", 0, &fp, "running", None).await;
        let a = parent.assistant_calls("r0", &[("c1", "subagent")]).await;
        parent
            .invocation("r0", &a, "inv-sub", "c1", "subagent", "running")
            .await;
        parent.finish().await;
        sqlx::query(
            "UPDATE tool_invocations SET child_session_id = 'child-1', \
             child_turn_id = 'child-turn' WHERE id = 'inv-sub'",
        )
        .execute(&mut c)
        .await
        .unwrap();
    }

    let gw = ScriptedGateway::new();
    let o = build(env.config(agents()), &gw, vec![mt]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let child = o.result(&alice(), "child-1", "child-turn").await.unwrap();
    assert_eq!(child.error.unwrap().code, "TOOL_OUTCOME_UNKNOWN");
    let parent = o.result(&alice(), &sid, "turn-1").await.unwrap();
    assert_eq!(parent.status, RunStatus::Interrupted);
    assert_eq!(parent.error.unwrap().code, "RECOVERY_BLOCKED");
    assert_eq!(gw.call_count(), 0);
    // Parent cũng không nhận input mới cho tới khi child được reconcile.
    assert_eq!(
        o.submit(&alice(), &sid, "more".into())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::RecoveryBlocked
    );

    gw.push_text("child final");
    gw.push_text("parent final");
    o.resolve_interrupted_invocation(
        &operator(),
        "child-1",
        "inv-mt",
        VerifiedToolResolution::ConfirmedCompleted {
            outcome: ToolOutcome {
                call_id: "k1".into(),
                content: "applied".into(),
                metadata: Value::Null,
            },
            evidence: "diff confirmed in the repository".into(),
        },
    )
    .await
    .unwrap();
    let r = wait_terminal(&o, &sid, "turn-1").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.attempt, 1);
    assert_eq!(r.output.as_deref(), Some("parent final"));
    let child = o.result(&alice(), "child-1", "child-turn").await.unwrap();
    assert_eq!((child.status, child.attempt), (RunStatus::Completed, 1));
    assert_eq!(child.output.as_deref(), Some("child final"));
    assert_eq!(
        mt_runs.load(Ordering::SeqCst),
        0,
        "the child's mutation is never replayed"
    );
    let texts = tool_texts(&history(&o, &sid).await);
    assert!(texts[0].contains("child final"), "{texts:?}");
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn resolving_a_child_the_parent_already_gave_up_on_does_not_resume_it() {
    let env = Env::new();
    // Tool Mutating của child vượt timeout → outcome unknown → child `interrupted` ngay trong
    // process; parent nhận `CHILD_FAILED` và tự kết thúc turn (ADR-002).
    let (mt, mt_runs) = Probe::new(
        "mt",
        ToolEffect::Mutating,
        Behavior::Ignore(Duration::from_millis(400)),
    );
    let agents = || vec![env.root("model-x", &["subagent"]), env.reviewer(&["mt"])];
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("job"))]);
    gw.push_tool_calls("", vec![("k1", "mt", json!({"text": "a"}))]);
    gw.push_text("parent handled the failure");
    let mut cfg = env.config(agents());
    cfg.runtime.tool_timeout = Duration::from_millis(100);
    let o = build(cfg, &gw, vec![mt.clone()]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let t = o.submit(&alice(), &sid, "delegate".into()).await.unwrap();
    let r = wait_terminal(&o, &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert!(tool_texts(&history(&o, &sid).await)[0].contains("CHILD_FAILED"));

    let (child_sid, child_turn, child_inv) = {
        let mut c = env.sql().await;
        let row = sqlx::query(
            "SELECT p.child_session_id, p.child_turn_id, k.id FROM tool_invocations p \
             JOIN tool_invocations k ON k.session_id = p.child_session_id \
             WHERE p.session_id = ?1",
        )
        .bind(&sid)
        .fetch_one(&mut c)
        .await
        .unwrap();
        (
            row.get::<String, _>(0),
            row.get::<String, _>(1),
            row.get::<String, _>(2),
        )
    };
    let child = o.result(&alice(), &child_sid, &child_turn).await.unwrap();
    assert_eq!(child.error.unwrap().code, "TOOL_OUTCOME_UNKNOWN");
    assert_eq!(gw.call_count(), 3);

    o.resolve_interrupted_invocation(
        &operator(),
        &child_sid,
        &child_inv,
        VerifiedToolResolution::ConfirmedNoEffect {
            evidence: "nothing was written".into(),
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        gw.call_count(),
        3,
        "no orphan child attempt nobody would consume"
    );
    let child = o.result(&alice(), &child_sid, &child_turn).await.unwrap();
    assert_eq!((child.status, child.attempt), (RunStatus::Interrupted, 0));
    assert_eq!(child.error.unwrap().code, "DELEGATION_CLOSED");

    // Root vẫn tiếp tục được child session đó theo session_id (ADR-003).
    gw.push_tool_calls(
        "",
        vec![(
            "c2",
            "subagent",
            json!({
                "agent": "reviewer",
                "description": "retry",
                "prompt": "try again",
                "session_id": child_sid,
            }),
        )],
    );
    gw.push_text("child done on request");
    gw.push_text("root done");
    let t2 = o.submit(&alice(), &sid, "retry".into()).await.unwrap();
    let r = wait_terminal(&o, &sid, &t2.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    let texts = tool_texts(&history(&o, &sid).await);
    assert!(texts[1].contains("child done on request"), "{texts:?}");
    assert_eq!(
        mt_runs.load(Ordering::SeqCst),
        1,
        "the mutation is never replayed"
    );
    o.shutdown().await.unwrap();
}

// ───────────────────────── Concurrency limit ─────────────────────────

#[tokio::test]
async fn root_runs_beyond_the_concurrency_limit_wait_durably_for_a_slot() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let mut cfg = env.config(env.agents(&[]));
    cfg.runtime.max_concurrent_root_runs = Some(1);
    let o = build(cfg, &gw, vec![]).await.unwrap();
    let s1 = o.create_session(&alice(), "root").await.unwrap();
    let s2 = o.create_session(&alice(), "root").await.unwrap();
    gw.hold();
    let t1 = o.submit(&alice(), &s1, "first".into()).await.unwrap();
    wait_until(|| gw.call_count() == 1).await;
    let t2 = o.submit(&alice(), &s2, "second".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(gw.call_count(), 1, "the second root run waits for a slot");
    assert_eq!(
        o.result(&alice(), &s2, &t2.turn_id).await.unwrap().status,
        RunStatus::Running,
        "accepted and durable while queued"
    );

    gw.release();
    let r1 = wait_terminal(&o, &s1, &t1.turn_id).await;
    let r2 = wait_terminal(&o, &s2, &t2.turn_id).await;
    assert_eq!(
        (r1.status, r2.status),
        (RunStatus::Completed, RunStatus::Completed)
    );
    assert_eq!(r2.output.as_deref(), Some("echo: second"));
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_child_never_waits_behind_the_root_concurrency_limit() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("job"))]);
    gw.push_text("child result");
    gw.push_text("root result");
    let mut cfg = env.config(env.agents(&["subagent"]));
    cfg.runtime.max_concurrent_root_runs = Some(1);
    let o = build(cfg, &gw, vec![]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let t = o.submit(&alice(), &sid, "delegate".into()).await.unwrap();
    let r = wait_terminal(&o, &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.output.as_deref(), Some("root result"));
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_zero_root_concurrency_limit_is_rejected() {
    let env = Env::new();
    let mut cfg = env.config(env.agents(&[]));
    cfg.runtime.max_concurrent_root_runs = Some(0);
    let e = build(cfg, &ScriptedGateway::new(), vec![])
        .await
        .err()
        .unwrap();
    assert_eq!(
        e.reason().map(|r| r.as_str()),
        Some("INVALID_RUNTIME_CONFIG")
    );
}

// ───────────────────────── Persistent store write failure ─────────────────────────

/// Mô phỏng ghi lỗi kéo dài (đĩa đầy, DB read-only một phần) bằng trigger SQLite: commit
/// assistant + invocation luôn thất bại, còn đọc và ghi terminal vẫn được.
async fn break_tool_call_commits(env: &Env) {
    let mut c = env.sql().await;
    sqlx::query(
        "CREATE TRIGGER fail_tool_calls BEFORE INSERT ON tool_invocations \
         BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END",
    )
    .execute(&mut c)
    .await
    .unwrap();
}

async fn heal_tool_call_commits(env: &Env) {
    let mut c = env.sql().await;
    sqlx::query("DROP TRIGGER fail_tool_calls")
        .execute(&mut c)
        .await
        .unwrap();
}

#[tokio::test]
async fn unsettled_run_backs_off_and_stops_calling_the_model_when_writes_keep_failing() {
    let env = Env::new();
    let (mt, mt_runs) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);
    let gw = ScriptedGateway::new();
    for i in 0..50 {
        gw.push_tool_calls("", vec![(&format!("c{i}"), "mt", json!({"text": "a"}))]);
    }
    let mut cfg = env.config(env.agents(&["mt"]));
    cfg.runtime.supervisor_sweep = Duration::from_millis(10);
    let o = build(cfg, &gw, vec![mt.clone()]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    break_tool_call_commits(&env).await;

    let t = o.submit(&alice(), &sid, "go".into()).await.unwrap();
    let r = wait_terminal(&o, &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Interrupted, "{r:?}");
    assert_eq!(r.error.unwrap().code, "STORE_UNAVAILABLE");
    let calls = gw.call_count();
    assert_eq!(
        calls, 5,
        "model calls are bounded while writes keep failing"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        gw.call_count(),
        calls,
        "no further model call after giving up"
    );
    assert_eq!(
        mt_runs.load(Ordering::SeqCst),
        0,
        "no tool ran without a durable call"
    );

    // Store khỏe lại: lần khởi động sau phục hồi đúng turn đó (không mất input).
    heal_tool_call_commits(&env).await;
    o.shutdown().await.unwrap();
    drop(o);
    let gw2 = ScriptedGateway::new();
    gw2.push_text("recovered after the store healed");
    let o2 = build(env.config(env.agents(&["mt"])), &gw2, vec![mt])
        .await
        .unwrap();
    let r = wait_terminal(&o2, &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(r.attempt, 1);
    o2.shutdown().await.unwrap();
}

// ───────────────────────── AT-25 ─────────────────────────

#[tokio::test]
async fn at25_managed_run_continues_after_the_submitting_reader_is_gone() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_delayed(
        Duration::from_millis(300),
        ModelResponse {
            text: "finished while nobody was watching".into(),
            finish_reason: "end_turn".into(),
            tool_calls: Vec::new(),
            usage: None,
        },
    );
    let o = Arc::new(
        build(env.config(env.agents(&[])), &gw, vec![])
            .await
            .unwrap(),
    );
    let sid = o.create_session(&alice(), "root").await.unwrap();
    // "Request handler": nhận 202 rồi kết thúc (disconnect) — không còn ai theo dõi run.
    let turn = {
        let o = o.clone();
        let sid = sid.clone();
        tokio::spawn(async move { o.submit(&alice(), &sid, "long task".into()).await.unwrap() })
            .await
            .unwrap()
    };
    tokio::time::sleep(Duration::from_millis(700)).await;
    let r = o.result(&alice(), &sid, &turn.turn_id).await.unwrap();
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(
        r.output.as_deref(),
        Some("finished while nobody was watching")
    );
    assert_eq!(history(&o, &sid).await.len(), 2);
    o.shutdown().await.unwrap();
}
