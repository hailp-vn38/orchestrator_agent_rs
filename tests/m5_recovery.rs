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
                    limits: orchestrator_agent_rs::RequestLimits::uniform(Duration::from_secs(30)),
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
    let started = std::time::Instant::now();
    loop {
        let r = o.result(&alice(), s, t).await.unwrap();
        if r.status != RunStatus::Running {
            return r;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "run stuck: {r:?}"
        );
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
    assert!(
        texts[0].contains("stop_reason=\"CANCELLED\"") && texts[0].contains("retryable=\"false\""),
        "{texts:?}"
    );
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
async fn at41_forced_shutdown_releases_owner_without_replaying_unknown_mutation() {
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

    o.shutdown()
        .await
        .expect("forced cancellation fits short shutdown grace");
    drop(o);
    let (mt3, mt3_runs) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);
    let o3 = build(
        env.config(env.agents(&["mt"])),
        &ScriptedGateway::new(),
        vec![mt3],
    )
    .await
    .unwrap();
    let r = o3.result(&alice(), &sid, &t.turn_id).await.unwrap();
    assert_eq!(r.status, RunStatus::Interrupted, "{r:?}");
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("TOOL_OUTCOME_UNKNOWN")
    );
    assert_eq!(
        mt3_runs.load(Ordering::SeqCst),
        0,
        "unknown mutation must not replay"
    );
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
    let parent_tool_msg = &tool_texts(&history(&o, &sid).await)[0];
    assert!(
        parent_tool_msg.contains("state=\"failed\""),
        "{parent_tool_msg}"
    );
    assert!(
        parent_tool_msg.contains("stop_reason=\"TOOL_OUTCOME_UNKNOWN\""),
        "{parent_tool_msg}"
    );
    assert!(
        parent_tool_msg.contains("retryable=\"false\""),
        "{parent_tool_msg}"
    );

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

// ───────────────────────── Phase 2: Durable Retry & Waiting States ─────────────────────────

#[tokio::test]
async fn migration_preserves_runs() {
    use orchestrator_agent_rs::test_support::GatewayError;

    // 1. Dựng DB ở schema 0005 (chạy thủ công 0001..0005) có đủ sessions, agent_runs (kèm khoảng
    // trống rowid), messages và tool_invocations, rồi mở Orchestrator để chạy 0006.
    let env = Env::new();
    std::fs::create_dir_all(env.store()).unwrap();
    let db_path = env.store().join("orchestrator.db");
    {
        let opts = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .foreign_keys(true);
        let mut conn = SqliteConnection::connect_with(&opts).await.unwrap();
        sqlx::query(
            "CREATE TABLE _sqlx_migrations (
                version BIGINT PRIMARY KEY,
                description TEXT NOT NULL,
                installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                success BOOLEAN NOT NULL,
                checksum BLOB NOT NULL,
                execution_time BIGINT NOT NULL
            )",
        )
        .execute(&mut conn)
        .await
        .unwrap();

        let migrations: &[(i64, &str, &str)] = &[
            (1, "init", include_str!("../migrations/0001_init.sql")),
            (
                2,
                "session permissions",
                include_str!("../migrations/0002_session_permissions.sql"),
            ),
            (
                3,
                "invocation resolution",
                include_str!("../migrations/0003_invocation_resolution.sql"),
            ),
            (
                4,
                "message turn",
                include_str!("../migrations/0004_message_turn.sql"),
            ),
            (
                5,
                "llm attempts",
                include_str!("../migrations/0005_llm_attempts.sql"),
            ),
        ];
        use sha2::{Digest, Sha384};
        for &(ver, desc, sql) in migrations {
            sqlx::raw_sql(sql).execute(&mut conn).await.unwrap();
            let checksum = Sha384::digest(sql.as_bytes()).to_vec();
            sqlx::query(
                "INSERT INTO _sqlx_migrations(version, description, success, checksum, execution_time) \
                 VALUES(?1, ?2, 1, ?3, 1)",
            )
            .bind(ver)
            .bind(desc)
            .bind(checksum)
            .execute(&mut conn)
            .await
            .unwrap();
        }

        sqlx::query(
            "INSERT INTO sessions(id, owner_id, workspace_id, agent_id, status, next_sequence) \
             VALUES('s1', 'alice', 'ws1', 'root', 'open', 4)",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        for (rowid, rid, tid) in [(1i64, "r1", "t1"), (5i64, "r5", "t2")] {
            sqlx::query(
                "INSERT INTO agent_runs(rowid, id, session_id, agent_id, turn_id, resume_attempt, \
                 provider_id, model_id, provider_config_fingerprint, status, finished_at) \
                 VALUES(?1, ?2, 's1', 'root', ?3, 0, 'fake', 'model-x', 'fp', 'completed', 100)",
            )
            .bind(rowid)
            .bind(rid)
            .bind(tid)
            .execute(&mut conn)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json, turn_id, run_id) \
             VALUES('m1', 's1', 1, 'user', '{\"text\":\"hi\"}', 't1', 'r1'), \
                   ('m2', 's1', 2, 'assistant', '{\"text\":\"ok\"}', 't1', 'r1')",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO tool_invocations(id, session_id, run_id, assistant_message_id, \
             tool_call_id, tool_name, arguments_json, status, finished_at) \
             VALUES('inv1', 's1', 'r1', 'm2', 'c1', 'echo', '{}', 'completed', 100)",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        conn.close().await.unwrap();
    }

    let gw = ScriptedGateway::new();
    let _ = GatewayError::Timeout;
    let o = build(env.config(env.agents(&[])), &gw, vec![])
        .await
        .unwrap();
    o.shutdown().await.unwrap();

    let mut conn = env.sql().await;
    let rowids: Vec<(i64, String)> = sqlx::query("SELECT rowid, id FROM agent_runs ORDER BY rowid")
        .fetch_all(&mut conn)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        rowids,
        vec![(1, "r1".to_owned()), (5, "r5".to_owned())],
        "rowid must be preserved across 0006 rebuild"
    );
    let msg_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let inv_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tool_invocations")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(msg_count, 2);
    assert_eq!(inv_count, 1);
    let fk_violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert!(
        fk_violations.is_empty(),
        "FK check failed: {}",
        fk_violations.len()
    );
    conn.close().await.unwrap();

    // 2. Chạy thêm trên bản sao orchestrator.db thật trong repo (nếu có).
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for rel in [
        "../../data/orchestrator-store/jobs/orchestrator.db",
        "../../data/orchestrator-store/orchestrator.db",
    ] {
        let src_db = manifest_dir.join(rel);
        if !src_db.exists() {
            continue;
        }
        let copy_env = Env::new();
        std::fs::create_dir_all(copy_env.store()).unwrap();
        let dst_db = copy_env.store().join("orchestrator.db");
        {
            let src_opts = SqliteConnectOptions::new()
                .filename(&src_db)
                .read_only(true);
            let mut src_conn = SqliteConnection::connect_with(&src_opts).await.unwrap();
            sqlx::query("VACUUM INTO ?1")
                .bind(dst_db.to_str().unwrap())
                .execute(&mut src_conn)
                .await
                .unwrap();
            src_conn.close().await.unwrap();
        }
        let (before_runs, before_msgs, before_invs) = {
            let mut c = copy_env.sql().await;
            let runs: Vec<(i64, String)> =
                sqlx::query("SELECT rowid, id FROM agent_runs ORDER BY rowid")
                    .fetch_all(&mut c)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|r| (r.get(0), r.get(1)))
                    .collect();
            let msgs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
                .fetch_one(&mut c)
                .await
                .unwrap();
            let invs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tool_invocations")
                .fetch_one(&mut c)
                .await
                .unwrap();
            // Đánh dấu những run đang running (nếu có) về completed để khi mở Orchestrator không spawn recovery.
            sqlx::query("UPDATE agent_runs SET status = 'completed', finished_at = 1 WHERE status = 'running'")
                .execute(&mut c)
                .await
                .unwrap();
            c.close().await.unwrap();
            (runs, msgs, invs)
        };
        let o_real = build(copy_env.config(copy_env.agents(&[])), &gw, vec![])
            .await
            .unwrap();
        o_real.shutdown().await.unwrap();

        let mut c = copy_env.sql().await;
        let after_runs: Vec<(i64, String)> =
            sqlx::query("SELECT rowid, id FROM agent_runs ORDER BY rowid")
                .fetch_all(&mut c)
                .await
                .unwrap()
                .into_iter()
                .map(|r| (r.get(0), r.get(1)))
                .collect();
        let after_msgs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(&mut c)
            .await
            .unwrap();
        let after_invs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tool_invocations")
            .fetch_one(&mut c)
            .await
            .unwrap();
        let fk = sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&mut c)
            .await
            .unwrap();
        assert_eq!(
            after_runs, before_runs,
            "real DB runs/rowids changed for {rel}"
        );
        assert_eq!(
            after_msgs, before_msgs,
            "real DB messages changed for {rel}"
        );
        assert_eq!(
            after_invs, before_invs,
            "real DB invocations changed for {rel}"
        );
        assert!(
            fk.is_empty(),
            "real DB FK check failed for {rel}: {}",
            fk.len()
        );
    }
}

#[tokio::test]
async fn waiting_retry_survives_restart() {
    use orchestrator_agent_rs::test_support::GatewayError;
    let env = Env::new();
    let gw1 = ScriptedGateway::new();
    gw1.push_error(GatewayError::Timeout);

    let mut cfg1 = env.config(env.agents(&[]));
    cfg1.llm.providers[0].limits.fast_retries = 0;
    let o1 = build(cfg1, &gw1, vec![]).await.unwrap();
    let sid = o1.create_session(&alice(), "root").await.unwrap();
    let turn = o1
        .submit(&alice(), &sid, "survive restart".into())
        .await
        .unwrap();

    let r_wait = wait_terminal(&o1, &sid, &turn.turn_id).await;
    assert_eq!(r_wait.status, RunStatus::WaitingRetry);
    let expected_next_at = r_wait.next_attempt_at.expect("next_attempt_at must be set");
    assert_eq!(r_wait.retry_reason.as_deref(), Some("PROVIDER_TIMEOUT"));
    o1.shutdown().await.unwrap();

    // Mở lại process (o2): mark_stale_runs_interrupted không được biến waiting_retry thành interrupted;
    // next_attempt_at phải giữ nguyên cho tới khi đến giờ.
    let gw2 = ScriptedGateway::new();
    gw2.push_text("completed on schedule after restart");
    let mut cfg2 = env.config(env.agents(&[]));
    cfg2.llm.providers[0].limits.fast_retries = 0;
    let o2 = build(cfg2, &gw2, vec![]).await.unwrap();

    let r_after_reopen = o2.result(&alice(), &sid, &turn.turn_id).await.unwrap();
    assert_eq!(r_after_reopen.status, RunStatus::WaitingRetry);
    assert_eq!(r_after_reopen.next_attempt_at, Some(expected_next_at));

    // Tua clock vượt qua lịch chờ 60s để supervisor tự resume.
    tokio::time::pause();
    let started = std::time::Instant::now();
    let r_done = loop {
        let res = o2.result(&alice(), &sid, &turn.turn_id).await.unwrap();
        if res.status.is_terminal() {
            break res;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "waiting_retry did not resume after due time: {res:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    assert_eq!(r_done.status, RunStatus::Completed);
    assert_eq!(
        r_done.output.as_deref(),
        Some("completed on schedule after restart")
    );
    assert_eq!(r_done.attempt, 1);
    assert!(r_done.next_attempt_at.is_none());
    o2.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancel_waiting_run() {
    use orchestrator_agent_rs::test_support::GatewayError;
    let env = Env::new();
    let gw = ScriptedGateway::new();
    // Turn 1: root tự rơi vào waiting_retry (không có worker trong bộ nhớ).
    gw.push_error(GatewayError::Timeout);
    // Turn 2: root gọi subagent, subagent rơi vào waiting_retry -> hủy root phải hủy cả child đang chờ.
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("child job"))]);
    gw.push_error(GatewayError::Timeout);

    let mut cfg = env.config(env.agents(&["subagent"]));
    cfg.llm.providers[0].limits.fast_retries = 0;
    let o = build(cfg, &gw, vec![]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();

    // 1. Hủy root đang ở waiting_retry (không còn worker).
    let t1 = o.submit(&alice(), &sid, "root wait".into()).await.unwrap();
    let r1 = wait_terminal(&o, &sid, &t1.turn_id).await;
    assert_eq!(r1.status, RunStatus::WaitingRetry);
    o.cancel(&alice(), &sid).await.unwrap();
    let r1_cancelled = o.result(&alice(), &sid, &t1.turn_id).await.unwrap();
    assert_eq!(r1_cancelled.status, RunStatus::Cancelled);
    assert!(r1_cancelled.next_attempt_at.is_none());

    // 2. Hủy root khi child đang ở waiting_retry -> cả hai đều Cancelled.
    let t2 = o
        .submit(&alice(), &sid, "delegate wait".into())
        .await
        .unwrap();
    wait_until(|| gw.call_count() == 3).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let child_sid = loop {
        let mut c = env.sql().await;
        let row: Option<String> =
            sqlx::query_scalar("SELECT session_id FROM agent_runs WHERE status = 'waiting_retry'")
                .fetch_optional(&mut c)
                .await
                .unwrap();
        if let Some(csid) = row {
            break csid;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "child not in waiting_retry"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    o.cancel(&alice(), &sid).await.unwrap();
    let r2_cancelled = wait_terminal(&o, &sid, &t2.turn_id).await;
    assert_eq!(r2_cancelled.status, RunStatus::Cancelled);

    let mut c = env.sql().await;
    let child_status: String =
        sqlx::query_scalar("SELECT status FROM agent_runs WHERE session_id = ?1")
            .bind(&child_sid)
            .fetch_one(&mut c)
            .await
            .unwrap();
    assert_eq!(child_status, "cancelled");
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn resume_blocked_on_config_change() {
    use orchestrator_agent_rs::test_support::GatewayError;
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_error(GatewayError::Auth);
    gw.push_text("should not run");

    let mut cfg = env.config(env.agents(&[]));
    cfg.llm.providers[0].limits.fast_retries = 0;
    let o1 = build(cfg, &gw, vec![]).await.unwrap();
    let sid = o1.create_session(&alice(), "root").await.unwrap();
    let turn = o1
        .submit(&alice(), &sid, "check config".into())
        .await
        .unwrap();

    let r_wait = wait_terminal(&o1, &sid, &turn.turn_id).await;
    assert_eq!(r_wait.status, RunStatus::WaitingUser);
    o1.shutdown().await.unwrap();

    // Đổi routing (model-x -> model-z) rồi mở lại và gọi resume_waiting -> phải chặn với PROVIDER_CONFIG_CHANGED.
    let mut cfg2 = env.config(vec![env.root("model-z", &[]), env.reviewer(&[])]);
    cfg2.llm.providers[0].limits.fast_retries = 0;
    let o2 = build(cfg2, &gw, vec![]).await.unwrap();
    let refused = o2.resume_waiting(&alice(), &sid).await.unwrap_err();
    assert!(
        refused.message().contains("blocked or exhausted"),
        "a blocked resume must not report success: {refused:?}"
    );

    let r_blocked = o2.result(&alice(), &sid, &turn.turn_id).await.unwrap();
    assert_eq!(r_blocked.status, RunStatus::Interrupted);
    assert_eq!(
        r_blocked.error.as_ref().map(|e| e.code.as_str()),
        Some("PROVIDER_CONFIG_CHANGED")
    );
    o2.shutdown().await.unwrap();
}

#[tokio::test]
async fn steps_cap_across_attempts() {
    use orchestrator_agent_rs::test_support::GatewayError;
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (echo, _) = Probe::new("echo", ToolEffect::ReadOnly, Behavior::Echo);

    // Attempt 0: chạy 2 tool steps rồi ở step 3 gặp Timeout -> waiting_retry
    gw.push_tool_calls("", vec![("e1", "echo", json!({"text": "1"}))]);
    gw.push_tool_calls("", vec![("e2", "echo", json!({"text": "2"}))]);
    gw.push_error(GatewayError::Timeout);
    // Attempt 1 (sau khi resume): chạy thêm 1 tool step (tổng turn_steps = 3 == max_steps)
    gw.push_tool_calls("", vec![("e3", "echo", json!({"text": "3"}))]);
    // Step kế phải bị ép capped (không truyền tools, có STEP_LIMIT_GUIDANCE)
    gw.push_text("capped summary across attempts");

    let root = env.root("model-x", &["echo"]).with_steps(3);
    let mut cfg = env.config(vec![root, env.reviewer(&[])]);
    cfg.llm.providers[0].limits.fast_retries = 0;
    let o = build(cfg, &gw, vec![echo]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();

    tokio::time::pause();
    let turn = o.submit(&alice(), &sid, "run capped".into()).await.unwrap();
    let started = std::time::Instant::now();
    let r = loop {
        let res = o.result(&alice(), &sid, &turn.turn_id).await.unwrap();
        if res.status.is_terminal() {
            break res;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "run stuck: {res:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };

    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(r.attempt, 1);
    assert_eq!(r.output.as_deref(), Some("capped summary across attempts"));

    let reqs = gw.requests();
    let last_req = reqs.last().unwrap();
    assert!(
        last_req.tools.is_empty(),
        "final request at turn_steps == max_steps must strip tools"
    );
    assert!(
        last_req.messages.iter().any(|m| m
            .text
            .contains("The step limit for this task has been reached")),
        "final request must include STEP_LIMIT_GUIDANCE"
    );
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancel_video_within_grace() {
    let env = Env::new();
    let (slow_video, runs) = Probe::new(
        "video_create",
        ToolEffect::Mutating,
        Behavior::Ignore(Duration::from_secs(600)),
    );
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("v1", "video_create", json!({"text": "render"}))]);
    gw.push_text("never");

    let mut cfg = env.config(env.agents(&["video_create"]));
    cfg.runtime.tool_timeout = Duration::from_secs(1200);
    let o = build(cfg, &gw, vec![slow_video]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o
        .submit(&alice(), &sid, "render video".into())
        .await
        .unwrap();
    wait_until(|| runs.load(Ordering::SeqCst) == 1).await;

    tokio::time::pause();
    let virtual_start = tokio::time::Instant::now();
    o.cancel(&alice(), &sid).await.unwrap();

    let started = std::time::Instant::now();
    let r = loop {
        let res = o.result(&alice(), &sid, &turn.turn_id).await.unwrap();
        if res.status.is_terminal() {
            break res;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "cancel_video_within_grace stuck: {res:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    assert_eq!(r.status, RunStatus::Cancelled, "{r:?}");
    let virtual_elapsed = virtual_start.elapsed();
    assert!(
        virtual_elapsed >= Duration::from_secs(30) && virtual_elapsed <= Duration::from_secs(35),
        "non-cooperative video must be stopped at the 30s grace boundary: {virtual_elapsed:?}"
    );
    assert_eq!(r.error.as_ref().map(|e| e.code.as_str()), Some("CANCELLED"));
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_completes_with_slow_tool() {
    let env = Env::new();
    let (slow_tool, runs) = Probe::new(
        "video_create",
        ToolEffect::Mutating,
        Behavior::Ignore(Duration::from_secs(600)),
    );
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("v1", "video_create", json!({"text": "render"}))]);
    gw.push_text("never");

    let mut cfg = env.config(env.agents(&["video_create"]));
    cfg.runtime.tool_timeout = Duration::from_secs(1200);
    cfg.runtime.shutdown_drain = Duration::from_millis(50);
    cfg.runtime.shutdown_grace = Duration::from_secs(30);
    let o = build(cfg, &gw, vec![slow_tool]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o
        .submit(&alice(), &sid, "render video".into())
        .await
        .unwrap();
    wait_until(|| runs.load(Ordering::SeqCst) == 1).await;

    let _ = turn;
    tokio::time::pause();
    o.shutdown()
        .await
        .expect("shutdown must complete within grace when slow tool is force-dropped at 20s");
    let mut c = env.sql().await;
    assert_eq!(
        one(
            &mut c,
            "SELECT status FROM agent_runs WHERE session_id = ?1",
            &sid
        )
        .await,
        "interrupted"
    );
}

#[tokio::test]
async fn resumable_recovered_not_blocked() {
    struct ResumableProbe {
        seen_invocation_ids: Arc<std::sync::Mutex<Vec<String>>>,
        seen_op_statuses: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    }

    #[async_trait::async_trait]
    impl HostTool for ResumableProbe {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "resumable_tool".into(),
                description: "resumable probe".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": { "text": { "type": "string" } },
                    "required": ["text"],
                    "additionalProperties": false
                }),
                effect: ToolEffect::Resumable,
            }
        }

        async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
            self.seen_invocation_ids
                .lock()
                .unwrap()
                .push(ctx.scope().invocation_id.clone());
            let cur = ctx.operation().current().await?;
            self.seen_op_statuses
                .lock()
                .unwrap()
                .push(cur.as_ref().map(|op| op.status.as_str().to_string()));
            if cur.is_none() {
                ctx.operation().begin(9999999999, "muse").await?;
            }
            ctx.operation()
                .advance(orchestrator_agent_rs::OperationUpdate {
                    status: Some(orchestrator_agent_rs::ToolOperationStatus::Committed),
                    result_json: Some(json!({"ok": true}).to_string()),
                    ..Default::default()
                })
                .await?;
            let text = args["text"].as_str().unwrap_or_default();
            Ok(ToolOutcome {
                call_id: ctx.scope().tool_call_id.clone(),
                content: format!("resumed:{text}"),
                metadata: Value::Null,
            })
        }
    }

    let env = Env::new();
    let seen_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_statuses = Arc::new(std::sync::Mutex::new(Vec::new()));
    let res_tool: Arc<dyn HostTool> = Arc::new(ResumableProbe {
        seen_invocation_ids: seen_ids.clone(),
        seen_op_statuses: seen_statuses.clone(),
    });
    let (mt_tool, mt_runs) = Probe::new("mt", ToolEffect::Mutating, Behavior::Echo);

    let (sid_res, fp) = fresh_session(
        &env,
        vec![res_tool.clone(), mt_tool.clone()],
        &["resumable_tool", "mt"],
    )
    .await;
    let sid_mt = {
        let gw = ScriptedGateway::new();
        let o = build(
            env.config(env.agents(&["resumable_tool", "mt"])),
            &gw,
            vec![res_tool.clone(), mt_tool.clone()],
        )
        .await
        .unwrap();
        let sid = o.create_session(&alice(), "root").await.unwrap();
        o.shutdown().await.unwrap();
        sid
    };

    {
        let mut c = env.sql().await;
        // Session 1: invocation đang `running` với tool Resumable + dòng `tool_operations` ở `running`.
        let mut s1 = Seed::new(&mut c, &sid_res);
        s1.user("render video").await;
        s1.run("r-res-0", "turn-res", 0, &fp, "running", None).await;
        let a1 = s1
            .assistant_calls("r-res-0", &[("call-res", "resumable_tool")])
            .await;
        s1.invocation(
            "r-res-0",
            &a1,
            "inv-res-1",
            "call-res",
            "resumable_tool",
            "running",
        )
        .await;
        s1.finish().await;
        sqlx::query(
            "INSERT INTO tool_operations(id, invocation_id, provider_ref, status, deadline_at) \
             VALUES('op-res-1', 'inv-res-1', 'muse', 'running', 9999999999)",
        )
        .execute(&mut c)
        .await
        .unwrap();

        // Session 2: invocation đang `running` với tool Mutating.
        let mut s2 = Seed::new(&mut c, &sid_mt);
        s2.user("mutate state").await;
        s2.run("r-mt-0", "turn-mt", 0, &fp, "running", None).await;
        let a2 = s2.assistant_calls("r-mt-0", &[("call-mt", "mt")]).await;
        s2.invocation("r-mt-0", &a2, "inv-mt-1", "call-mt", "mt", "running")
            .await;
        s2.finish().await;
    }

    let gw = ScriptedGateway::new();
    gw.push_text("video resumed and finished");
    let o = build(
        env.config(env.agents(&["resumable_tool", "mt"])),
        &gw,
        vec![res_tool, mt_tool],
    )
    .await
    .unwrap();

    // Resumable được tự động phục hồi và gọi lại với cùng `inv-res-1`.
    let r_res = wait_terminal(&o, &sid_res, "turn-res").await;
    assert_eq!(r_res.status, RunStatus::Completed, "{r_res:?}");
    assert_eq!(r_res.attempt, 1);
    assert_eq!(
        seen_ids.lock().unwrap().as_slice(),
        &["inv-res-1".to_string()]
    );
    assert_eq!(
        seen_statuses.lock().unwrap().as_slice(),
        &[Some("running".to_string())]
    );

    // Mutating vẫn bị chặn với `TOOL_OUTCOME_UNKNOWN`.
    let r_mt = o.result(&alice(), &sid_mt, "turn-mt").await.unwrap();
    assert_eq!(r_mt.status, RunStatus::Interrupted);
    assert_eq!(
        r_mt.error.as_ref().map(|e| e.code.as_str()),
        Some("TOOL_OUTCOME_UNKNOWN")
    );
    assert_eq!(mt_runs.load(Ordering::SeqCst), 0);

    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn budget_expired_while_waiting() {
    use orchestrator_agent_rs::test_support::GatewayError;
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (probe, _) = Probe::new("probe", ToolEffect::ReadOnly, Behavior::Echo);

    // Step 1: chạy tool tạo kết quả từng phần.
    gw.push_tool_calls("", vec![("p1", "probe", json!({"text": "partial-work"}))]);
    // Step 2: gặp lỗi Timeout -> rơi vào waiting_retry (backoff 60s), vượt quá total_deadline_at (now + 30s).
    gw.push_error(GatewayError::Timeout);
    gw.push_text("must never be called");

    let mut cfg = env.config(env.agents(&["probe"]));
    cfg.llm.providers[0].limits.fast_retries = 0;
    let o = build(cfg, &gw, vec![probe]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();

    tokio::time::pause();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let budget = orchestrator_agent_rs::RunBudget {
        started_at: now,
        total_deadline_at: now + 30,
        active_limit_secs: 300,
        reserve_secs: 10,
        max_tokens: None,
        waited_secs: 0,
    };
    let turn = o
        .submit_with_options(
            &alice(),
            &sid,
            "run then expire while waiting".into(),
            orchestrator_agent_rs::SubmitOptions {
                budget: Some(budget),
            },
        )
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let r = loop {
        let res = o.result(&alice(), &sid, &turn.turn_id).await.unwrap();
        if res.status.is_terminal() {
            break res;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "budget_expired_while_waiting stuck: {res:?}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    };

    assert_eq!(r.status, RunStatus::Failed, "{r:?}");
    assert_eq!(r.attempt, 0, "must not create a new attempt after deadline");
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("BUDGET_EXHAUSTED")
    );
    assert_eq!(r.partial_output.as_deref(), Some("probe:partial-work"));
    assert_eq!(gw.call_count(), 2, "no third LLM call after budget expiry");
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn budget_kept_across_attempts() {
    use orchestrator_agent_rs::test_support::GatewayError;
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (slow60, _) = Probe::new(
        "slow60",
        ToolEffect::ReadOnly,
        Behavior::Ignore(Duration::from_secs(60)),
    );
    let (slow45, _) = Probe::new(
        "slow45",
        ToolEffect::ReadOnly,
        Behavior::Ignore(Duration::from_secs(45)),
    );

    // Attempt 0: dùng 60s active time rồi gặp Timeout -> waiting_retry (backoff 60s).
    gw.push_tool_calls("", vec![("s1", "slow60", json!({"text": "part1"}))]);
    gw.push_error(GatewayError::Timeout);
    // Attempt 1 (sau khi resume): dùng thêm 45s active time (tổng 105s >= 100s active_limit_secs).
    gw.push_tool_calls("", vec![("s2", "slow45", json!({"text": "part2"}))]);
    // Step kế tiếp của Attempt 1 phải bị ép text-only (capped) vì ngân sách không được cấp lại.
    gw.push_text("capped summary after resume");

    let mut cfg = env.config(env.agents(&["slow60", "slow45"]));
    cfg.llm.providers[0].limits.fast_retries = 0;
    cfg.runtime.tool_timeout = Duration::from_secs(200);
    let o = build(cfg, &gw, vec![slow60, slow45]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();

    tokio::time::pause();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let budget = orchestrator_agent_rs::RunBudget {
        started_at: now,
        total_deadline_at: now + 3600,
        active_limit_secs: 100,
        reserve_secs: 40,
        max_tokens: None,
        waited_secs: 0,
    };
    let turn = o
        .submit_with_options(
            &alice(),
            &sid,
            "keep budget across attempts".into(),
            orchestrator_agent_rs::SubmitOptions {
                budget: Some(budget),
            },
        )
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let r = loop {
        let res = o.result(&alice(), &sid, &turn.turn_id).await.unwrap();
        if res.status.is_terminal() {
            break res;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "budget_kept_across_attempts stuck: {res:?}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    };

    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(r.attempt, 1);
    assert_eq!(r.output.as_deref(), Some("capped summary after resume"));
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("BUDGET_EXHAUSTED")
    );
    let reqs = gw.requests();
    assert!(
        reqs.last().unwrap().tools.is_empty(),
        "final step on attempt 1 must have tools stripped due to cumulative active time"
    );
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn two_hour_random_fault_job() {
    assert_random_fault_job(false).await;
}

#[tokio::test]
async fn random_fault_job_stops_at_budget_limit() {
    assert_random_fault_job(true).await;
}

async fn assert_random_fault_job(short_budget: bool) {
    use orchestrator_agent_rs::test_support::GatewayError;
    let env = Env::new();
    let gw = ScriptedGateway::new();
    // Mỗi lần gọi tool `work_chunk` tốn 10 phút (600s) giả lập.
    let (work_chunk, chunk_runs) = Probe::new(
        "work_chunk",
        ToolEffect::ReadOnly,
        Behavior::Ignore(Duration::from_secs(600)),
    );

    // Sinh kịch bản 2 giờ bằng PRNG tất định với seed cố định:
    // pha trộn 12 bước gọi tool 10 phút (= 2 giờ chạy thực), lỗi mạng/timeout/rate-limit/stalled
    // ngẫu nhiên, và tổng hợp kết quả khi chạm trần ngân sách 2 giờ.
    let mut seed: u64 = 0x2026_1031_CAFE_BEEF;
    let mut next_rand = || -> u64 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    let mut fault_count = 0usize;
    for step in 0..12 {
        let roll = next_rand() % 5;
        if roll == 0 && fault_count < 3 {
            fault_count += 1;
            match fault_count % 3 {
                0 => gw.push_error(GatewayError::Timeout),
                1 => gw.push_error(GatewayError::Stalled),
                _ => gw.push_error(GatewayError::RateLimited {
                    retry_after_secs: Some(120),
                }),
            }
        }
        gw.push_with(move |request| {
            if request.tools.is_empty() {
                ModelResponse {
                    text: "two-hour job completed with summary".into(),
                    finish_reason: "end_turn".into(),
                    tool_calls: vec![],
                    usage: None,
                }
            } else {
                ModelResponse {
                    text: String::new(),
                    finish_reason: "tool_calls".into(),
                    tool_calls: vec![orchestrator_agent_rs::test_support::ModelToolCall {
                        id: format!("w{step}"),
                        name: "work_chunk".into(),
                        arguments: json!({"text":format!("chunk-{step}")}),
                    }],
                    usage: None,
                }
            }
        });
    }
    gw.push_text("two-hour job completed with summary");

    let mut cfg = env.config(env.agents(&["work_chunk"]));
    cfg.llm.providers[0].limits.fast_retries = 1;
    cfg.runtime.max_resume_attempts = 8;
    cfg.runtime.tool_timeout = Duration::from_secs(1800);
    cfg.runtime.supervisor_sweep = Duration::from_secs(30);
    let o = build(cfg, &gw, vec![work_chunk]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();

    tokio::time::pause();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    // Ngân sách 2 giờ chạy (7200s), tổng 24 giờ (86400s), dự trữ 10 phút (600s).
    let budget = orchestrator_agent_rs::RunBudget {
        started_at: now,
        total_deadline_at: now + 24 * 3600,
        active_limit_secs: if short_budget { 1200 } else { 2 * 3600 },
        reserve_secs: 10 * 60,
        max_tokens: None,
        waited_secs: 0,
    };
    let turn = o
        .submit_with_options(
            &alice(),
            &sid,
            "run 2h job under random faults".into(),
            orchestrator_agent_rs::SubmitOptions {
                budget: Some(budget),
            },
        )
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let r = loop {
        let res = o.result(&alice(), &sid, &turn.turn_id).await.unwrap();
        if res.status.is_terminal() {
            break res;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "two_hour_random_fault_job stuck: {res:?}"
        );
        tokio::time::sleep(Duration::from_secs(30)).await;
    };

    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert!(
        r.output
            .as_deref()
            .is_some_and(|s| s.contains("two-hour job completed")),
        "expected final summary output, got: {r:?}"
    );
    if short_budget {
        assert!(chunk_runs.load(Ordering::SeqCst) < 12);
        assert_eq!(
            r.error.as_ref().map(|e| e.code.as_str()),
            Some("BUDGET_EXHAUSTED")
        );
    } else {
        assert!(
            chunk_runs.load(Ordering::SeqCst) >= 12,
            "expected 12 ten-minute chunks"
        );
    }
    // Đạt điều kiện: hoàn tất bình thường hoặc dừng có lý do BUDGET_EXHAUSTED (ánh xạ sang TaskStatus::Stopped).
    if let Some(err) = &r.error {
        assert_eq!(err.code, "BUDGET_EXHAUSTED");
    }
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_preserves_resumable_invocation_for_restart() {
    struct ShutdownVideo {
        ids: Arc<std::sync::Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl HostTool for ShutdownVideo {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "shutdown_video".into(),
                description: "video".into(),
                input_schema: json!({"type":"object", "properties":{}, "additionalProperties":false}),
                effect: ToolEffect::Resumable,
            }
        }
        async fn execute(&self, ctx: ToolContext, _: Value) -> Result<ToolOutcome, ToolError> {
            self.ids
                .lock()
                .unwrap()
                .push(ctx.scope().invocation_id.clone());
            if ctx.operation().current().await?.is_none() {
                ctx.operation().begin(9999999999, "muse").await?;
                ctx.cancel_token().cancelled().await;
                return Err(ToolError {
                    code: "CANCELLED".into(),
                    message: "shutdown".into(),
                });
            }
            Ok(ToolOutcome {
                call_id: ctx.scope().tool_call_id.clone(),
                content: "recovered video".into(),
                metadata: Value::Null,
            })
        }
    }
    let env = Env::new();
    let ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tool: Arc<dyn HostTool> = Arc::new(ShutdownVideo { ids: ids.clone() });
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("v1", "shutdown_video", json!({}))]);
    gw.push_text("video completed after restart");
    let mut cfg = env.config(env.agents(&["shutdown_video"]));
    cfg.runtime.shutdown_drain = Duration::from_millis(10);
    let o = build(cfg, &gw, vec![tool.clone()]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o.submit(&alice(), &sid, "render".into()).await.unwrap();
    wait_until(|| !ids.lock().unwrap().is_empty()).await;
    o.shutdown().await.unwrap();
    let restored = build(env.config(env.agents(&["shutdown_video"])), &gw, vec![tool])
        .await
        .unwrap();
    let result = wait_terminal(&restored, &sid, &turn.turn_id).await;
    assert_eq!(result.status, RunStatus::Completed, "{result:?}");
    let seen = ids.lock().unwrap().clone();
    assert_eq!(
        seen.len(),
        2,
        "shutdown must replay the existing invocation"
    );
    assert_eq!(seen[0], seen[1], "recovery must keep the invocation ID");
    restored.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_with_short_grace_does_not_wait_twenty_seconds() {
    let env = Env::new();
    let (tool, runs) = Probe::new(
        "slow",
        ToolEffect::Mutating,
        Behavior::Ignore(Duration::from_secs(600)),
    );
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("s1", "slow", json!({"text":"render"}))]);
    let mut cfg = env.config(env.agents(&["slow"]));
    cfg.runtime.shutdown_drain = Duration::from_millis(10);
    cfg.runtime.shutdown_grace = Duration::from_secs(1);
    cfg.runtime.tool_timeout = Duration::from_secs(1200);
    let o = build(cfg, &gw, vec![tool]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    o.submit(&alice(), &sid, "render".into()).await.unwrap();
    wait_until(|| runs.load(Ordering::SeqCst) == 1).await;
    tokio::time::pause();
    o.shutdown()
        .await
        .expect("tool cancellation must fit the configured shutdown grace");
}

struct UncertainVideo {
    panic: bool,
}
#[async_trait::async_trait]
impl HostTool for UncertainVideo {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "uncertain_video".into(),
            description: "video".into(),
            input_schema: json!({"type":"object", "properties":{}, "additionalProperties":false}),
            effect: ToolEffect::Resumable,
        }
    }
    async fn execute(&self, ctx: ToolContext, _: Value) -> Result<ToolOutcome, ToolError> {
        ctx.operation().begin(9999999999, "muse").await?;
        if self.panic {
            panic!("lost provider connection");
        }
        Err(ToolError {
            code: "TOOL_OUTCOME_UNKNOWN".into(),
            message: "needs reconciliation".into(),
        })
    }
}

#[tokio::test]
async fn interrupted_operation_is_protected_until_reconciled() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("v1", "uncertain_video", json!({}))]);
    let o = build(
        env.config(env.agents(&["uncertain_video"])),
        &gw,
        vec![Arc::new(UncertainVideo { panic: false })],
    )
    .await
    .unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o.submit(&alice(), &sid, "render".into()).await.unwrap();
    assert_eq!(
        wait_terminal(&o, &sid, &turn.turn_id).await.status,
        RunStatus::Interrupted
    );
    let ops = o.list_tool_operations().await.unwrap();
    assert_eq!(ops.len(), 1);
    assert!(
        ops[0].root_has_active_work,
        "cleanup must retain a recoverable interrupted turn"
    );
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn resumable_panic_with_open_operation_does_not_settle_as_failure() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("v1", "uncertain_video", json!({}))]);
    gw.push_text("must not continue with a new invocation");
    let o = build(
        env.config(env.agents(&["uncertain_video"])),
        &gw,
        vec![Arc::new(UncertainVideo { panic: true })],
    )
    .await
    .unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o.submit(&alice(), &sid, "render".into()).await.unwrap();
    let r = wait_terminal(&o, &sid, &turn.turn_id).await;
    assert_eq!(r.status, RunStatus::Interrupted, "{r:?}");
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("TOOL_OUTCOME_UNKNOWN")
    );
    assert_eq!(
        gw.call_count(),
        1,
        "model must not retry an open operation with a fresh tool call"
    );
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn resumable_retry_later_keeps_invocation_and_waits() {
    struct RetryOnce {
        calls: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl HostTool for RetryOnce {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "retry_once".into(),
                description: "resumable tool hitting a transient failure".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": { "text": { "type": "string" } },
                    "required": ["text"],
                    "additionalProperties": false
                }),
                effect: ToolEffect::Resumable,
            }
        }

        async fn execute(&self, ctx: ToolContext, _args: Value) -> Result<ToolOutcome, ToolError> {
            let first = {
                let mut calls = self.calls.lock().unwrap();
                calls.push(ctx.scope().invocation_id.clone());
                calls.len() == 1
            };
            if first {
                return Err(ToolError {
                    code: orchestrator_agent_rs::CODE_TOOL_RETRY_LATER.into(),
                    message: "PROVIDER_UNAVAILABLE: cooling down".into(),
                });
            }
            Ok(ToolOutcome {
                call_id: ctx.scope().tool_call_id.clone(),
                content: "done".into(),
                metadata: Value::Null,
            })
        }
    }

    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "retry_once", json!({"text": "x"}))]);
    gw.push_text("finished");
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tool: Arc<dyn HostTool> = Arc::new(RetryOnce {
        calls: calls.clone(),
    });
    let o = build(env.config(env.agents(&["retry_once"])), &gw, vec![tool])
        .await
        .unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o.submit(&alice(), &sid, "go".into()).await.unwrap();

    let waiting = wait_terminal(&o, &sid, &turn.turn_id).await;
    assert_eq!(waiting.status, RunStatus::WaitingRetry, "{waiting:?}");
    assert!(waiting.next_attempt_at.is_some());

    o.resume_waiting(&alice(), &sid).await.unwrap();
    let done = wait_terminal(&o, &sid, &turn.turn_id).await;
    assert_eq!(done.status, RunStatus::Completed, "{done:?}");
    let seen = calls.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], seen[1], "the retry must reuse the same invocation");
    o.shutdown().await.unwrap();
}
