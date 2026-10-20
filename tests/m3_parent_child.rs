//! M3 gate (fake LLM + SQLite thật): AT-02–10, AT-13, AT-14, AT-16, AT-38 cho Parent–Child.
//!
//! Mode: deterministic fake gateway (`test-support`) — KHÔNG phải evidence provider live.
//! AT-13 ở đây chỉ chứng minh *routing selection* theo AgentDefinition của child; chứng cứ
//! multi-provider thật nằm ở `m3_live_provider.rs` (provider gate).
//!
//! Kịch bản gateway tuần tự được vì Root đợi child (foreground): thứ tự gọi là
//! root → child → root, nên một queue dùng chung vẫn deterministic.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::test_support::{
    GatewayError, ModelRequest, ModelResponse, ModelRole, ModelToolCall, ScriptedGateway,
};
use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, CallerContext, ErrorCode, HostTool, LlmBackend,
    LlmConfig, LlmProviderConfig, MessageRole, MessageView, Orchestrator, OrchestratorConfig,
    OrchestratorError, PageRequest, ProviderAuth, RunStatus, SecretString, SessionPolicy,
    ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};

const ROOT_MARK: &str = "You are the root test agent.";
const REVIEWER_MARK: &str = "You are the reviewer test agent.";
const PLACEHOLDER: &str = "Subagent completed without a text response.";

#[derive(Clone, Copy)]
enum Behavior {
    Echo,
    Sleep(Duration),
    UntilCancelled,
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
        let probe = Arc::new(Self {
            name,
            effect,
            behavior,
            runs: runs.clone(),
        });
        (probe, runs)
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
            Behavior::Sleep(d) => {
                tokio::time::sleep(d).await;
                format!("{}:slept", self.name)
            }
            Behavior::UntilCancelled => {
                while !ctx.is_cancelled() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                "stopped".into()
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

    fn skills(&self) -> PathBuf {
        self.dir.path().join("skills")
    }

    fn add_skill(&self, id: &str, body: &str) {
        let d = self.skills().join(id);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(
            d.join("SKILL.md"),
            format!("---\nname: {id}\ndescription: desc of {id}\n---\n{body}\n"),
        )
        .unwrap();
    }

    /// Root: provider `fake` / `model-x`, mode Primary.
    fn root(&self, tools: &[&str], skills: &[&str]) -> AgentDefinition {
        AgentDefinition::new("root", "fake", "model-x")
            .with_mode(AgentMode::Primary)
            .with_prompt_path(self.dir.path().join("root.md"))
            .with_allowed_tools(tools.iter().copied())
            .with_allowed_skills(skills.iter().copied())
    }

    /// Child: provider **khác** (`fake2` / `model-y`), mode Subagent.
    fn reviewer(&self, tools: &[&str], skills: &[&str]) -> AgentDefinition {
        AgentDefinition::new("reviewer", "fake2", "model-y")
            .with_mode(AgentMode::Subagent)
            .with_description("Reviews code and reports findings")
            .with_prompt_path(self.dir.path().join("reviewer.md"))
            .with_allowed_tools(tools.iter().copied())
            .with_allowed_skills(skills.iter().copied())
    }

    fn config(&self, agents: Vec<AgentDefinition>) -> OrchestratorConfig {
        let provider = |id: &str| LlmProviderConfig {
            id: id.into(),
            backend: LlmBackend::OpenAi,
            endpoint: None,
            auth: ProviderAuth::ApiKey(SecretString::new("sk-test-not-real")),
            request_timeout: Duration::from_secs(30),
        };
        let mut cfg = OrchestratorConfig::new(
            LlmConfig {
                providers: vec![provider("fake"), provider("fake2")],
            },
            agents,
            self.store(),
        );
        cfg.runtime.supervisor_sweep = Duration::from_millis(50);
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

async fn build_with(
    cfg: OrchestratorConfig,
    gw: &Arc<ScriptedGateway>,
    tools: Vec<Arc<dyn HostTool>>,
    drop_triggers: Option<Arc<AtomicBool>>,
) -> Result<Orchestrator, OrchestratorError> {
    let mut b = Orchestrator::builder(cfg).with_gateway(gw.clone());
    if let Some(flag) = drop_triggers {
        b = b.with_dropped_admission_triggers(flag);
    }
    for t in tools {
        b = b.register_tool(t);
    }
    b.build().await
}

async fn build(
    cfg: OrchestratorConfig,
    gw: &Arc<ScriptedGateway>,
    tools: Vec<Arc<dyn HostTool>>,
) -> Orchestrator {
    build_with(cfg, gw, tools, None).await.unwrap()
}

async fn wait_terminal(o: &Orchestrator, who: &CallerContext, s: &str, t: &str) -> AgentResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let r = o.result(who, s, t).await.unwrap();
        if r.status != RunStatus::Running {
            return r;
        }
        assert!(tokio::time::Instant::now() < deadline, "run stuck: {r:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn run_turn(o: &Orchestrator, sid: &str, text: &str) -> AgentResult {
    let turn = o.submit(&alice(), sid, text.into()).await.unwrap();
    wait_terminal(o, &alice(), sid, &turn.turn_id).await
}

async fn history(o: &Orchestrator, sid: &str) -> Vec<MessageView> {
    o.history(&alice(), sid, PageRequest::first(200))
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

fn sub_new(agent: &str, prompt: &str) -> Value {
    json!({ "agent": agent, "description": "task", "prompt": prompt })
}

fn sub_resume(agent: &str, session_id: &str, prompt: &str) -> Value {
    json!({ "agent": agent, "description": "task", "prompt": prompt, "session_id": session_id })
}

/// `sessionID="..."` của tool-result `subagent` gần nhất trong request.
fn session_id_in(req: &ModelRequest) -> String {
    let text = req
        .messages
        .iter()
        .rev()
        .find(|m| m.role == ModelRole::Tool)
        .map(|m| m.text.clone())
        .expect("a tool result in the request");
    let rest = text
        .split("sessionID=\"")
        .nth(1)
        .unwrap_or_else(|| panic!("no sessionID in: {text}"));
    rest.split('"').next().unwrap().to_owned()
}

fn tool_call_response(id: &str, args: Value) -> ModelResponse {
    ModelResponse {
        text: String::new(),
        finish_reason: "tool_use".into(),
        tool_calls: vec![ModelToolCall {
            id: id.into(),
            name: "subagent".into(),
            arguments: args,
        }],
    }
}

fn is_child_request(r: &ModelRequest) -> bool {
    r.system.contains(REVIEWER_MARK)
}

struct Link {
    child_session_id: Option<String>,
    child_turn_id: Option<String>,
    status: String,
}

async fn links(env: &Env, session_id: &str) -> Vec<Link> {
    let mut c = env.sql().await;
    sqlx::query(
        "SELECT child_session_id, child_turn_id, status FROM tool_invocations \
         WHERE session_id = ?1 AND tool_name = 'subagent' ORDER BY rowid",
    )
    .bind(session_id)
    .fetch_all(&mut c)
    .await
    .unwrap()
    .iter()
    .map(|r| Link {
        child_session_id: r.get(0),
        child_turn_id: r.get(1),
        status: r.get(2),
    })
    .collect()
}

async fn scalar_i64(env: &Env, sql: &str, bind: &str) -> i64 {
    let mut c = env.sql().await;
    sqlx::query(sql)
        .bind(bind)
        .fetch_one(&mut c)
        .await
        .unwrap()
        .get(0)
}

async fn root_with_reviewer(
    env: &Env,
    gw: &Arc<ScriptedGateway>,
    root_tools: &[&str],
    reviewer_tools: &[&str],
    tools: Vec<Arc<dyn HostTool>>,
) -> (Orchestrator, String) {
    let cfg = env.config(vec![
        env.root(root_tools, &[]),
        env.reviewer(reviewer_tools, &[]),
    ]);
    let o = build(cfg, gw, tools).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    (o, sid)
}

// ───────────────────────────── AT-02 / AT-03 ─────────────────────────────

#[tokio::test]
async fn at02_at03_root_calls_reviewer_and_gets_envelope_with_session_and_text() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls(
        "",
        vec![("c1", "subagent", sub_new("reviewer", "Please review X"))],
    );
    gw.push_text("Found 3 issues");
    gw.push_text("root done: 3 issues");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;

    let r = run_turn(&o, &sid, "review it").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.output.as_deref(), Some("root done: 3 issues"));

    // AT-02: child session có parent_session_id đúng; link nằm trên parent invocation.
    let l = links(&env, &sid).await;
    assert_eq!(l.len(), 1);
    assert_eq!(l[0].status, "completed");
    let child = l[0].child_session_id.clone().expect("child link");
    let turn = l[0].child_turn_id.clone().expect("child turn link");
    let mut c = env.sql().await;
    let row = sqlx::query(
        "SELECT parent_session_id, agent_id, owner_id, workspace_id FROM sessions WHERE id = ?1",
    )
    .bind(&child)
    .fetch_one(&mut c)
    .await
    .unwrap();
    assert_eq!(
        row.get::<Option<String>, _>(0).as_deref(),
        Some(sid.as_str())
    );
    assert_eq!(row.get::<String, _>(1), "reviewer");
    assert_eq!(row.get::<String, _>(2), "alice");
    assert_eq!(row.get::<String, _>(3), "ws1");

    // Model requests: root → child → root.
    let reqs = gw.requests();
    assert_eq!(reqs.len(), 3);
    let sub = reqs[0].tools.iter().find(|t| t.name == "subagent").unwrap();
    assert!(sub.description.contains("reviewer"), "{}", sub.description);
    assert!(
        sub.input_schema["properties"].get("background").is_none(),
        "no background in the MVP schema"
    );
    // Child: history riêng (chỉ prompt), prompt của chính nó, không có `subagent` (depth 1).
    assert!(is_child_request(&reqs[1]) && !reqs[1].system.contains(ROOT_MARK));
    assert!(reqs[1].system.contains("Orchestrator base instructions"));
    assert_eq!(reqs[1].messages.len(), 1);
    assert_eq!(reqs[1].messages[0].text, "Please review X");
    assert!(reqs[1].tools.iter().all(|t| t.name != "subagent"));

    // AT-03: Root nhận session_id + completed + output text từ runtime envelope.
    let seen = &reqs[2]
        .messages
        .iter()
        .rev()
        .find(|m| m.role == ModelRole::Tool)
        .unwrap()
        .text;
    assert!(seen.contains(&format!("sessionID=\"{child}\"")), "{seen}");
    assert!(seen.contains("state=\"completed\""), "{seen}");
    assert!(seen.contains("Found 3 issues"), "{seen}");
    assert_eq!(tool_texts(&history(&o, &sid).await), vec![seen.clone()]);

    // Child history đọc được, final của đúng child turn.
    let ch = history(&o, &child).await;
    assert_eq!(ch.len(), 2);
    assert_eq!(ch[0].text, "Please review X");
    assert_eq!(ch[1].text, "Found 3 issues");
    let cr = o.result(&alice(), &child, &turn).await.unwrap();
    assert_eq!(cr.status, RunStatus::Completed);
    assert_eq!(cr.output.as_deref(), Some("Found 3 issues"));

    // Không có bảng `tasks` (TaskManager không có state riêng).
    let tables: Vec<String> = sqlx::query("SELECT name FROM sqlite_master WHERE type='table'")
        .fetch_all(&mut c)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert!(!tables.iter().any(|t| t == "tasks"), "{tables:?}");
}

// ─────────────────────────────── AT-04 ───────────────────────────────

#[tokio::test]
async fn at04_unexpected_format_is_not_rejected_and_root_decides() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls(
        "",
        vec![("c1", "subagent", sub_new("reviewer", "Reply as JSON"))],
    );
    gw.push_text("sure! {not json at all");
    gw.push_text("the reviewer ignored the format; I will report that");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;

    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    // Runtime không tự reject/retry: đúng 3 lượt model, Root nhận nguyên văn.
    assert_eq!(gw.call_count(), 3);
    assert!(tool_texts(&history(&o, &sid).await)[0].contains("sure! {not json at all"));
    assert_eq!(links(&env, &sid).await.len(), 1);
}

// ─────────────────────────────── AT-05 ───────────────────────────────

#[tokio::test]
async fn at05_followup_reuses_child_history_with_new_invocation_and_run() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    // Follow-up trong cùng một Root turn; prompt **giống hệt** lần đầu (AT-38).
    gw.push_tool_calls(
        "",
        vec![("c1", "subagent", sub_new("reviewer", "same prompt"))],
    );
    gw.push_text("answer one");
    gw.push_with(|req| {
        tool_call_response(
            "c2",
            sub_resume("reviewer", &session_id_in(req), "same prompt"),
        )
    });
    gw.push_text("answer two");
    gw.push_text("root done");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;

    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);

    let l = links(&env, &sid).await;
    assert_eq!(l.len(), 2, "mỗi lần gọi là một parent invocation mới");
    assert_eq!(l[0].child_session_id, l[1].child_session_id);
    assert_ne!(
        l[0].child_turn_id, l[1].child_turn_id,
        "prompt trùng ≠ cùng logical turn"
    );
    let child = l[0].child_session_id.clone().unwrap();

    // Child request thứ hai thấy toàn bộ history trước đó.
    let child_reqs: Vec<_> = gw.requests().into_iter().filter(is_child_request).collect();
    assert_eq!(child_reqs.len(), 2);
    let texts: Vec<_> = child_reqs[1]
        .messages
        .iter()
        .map(|m| m.text.as_str())
        .collect();
    assert_eq!(texts, ["same prompt", "answer one", "same prompt"]);

    // Hai child run (mỗi parent invocation một turn), mỗi run đúng final của nó.
    assert_eq!(
        scalar_i64(
            &env,
            "SELECT COUNT(*) FROM agent_runs WHERE session_id = ?1",
            &child
        )
        .await,
        2
    );
    for (link, want) in l.iter().zip(["answer one", "answer two"]) {
        let cr = o
            .result(&alice(), &child, link.child_turn_id.as_ref().unwrap())
            .await
            .unwrap();
        assert_eq!(cr.output.as_deref(), Some(want));
    }
    let tools = tool_texts(&history(&o, &sid).await);
    assert!(tools[0].contains("answer one") && !tools[0].contains("answer two"));
    assert!(tools[1].contains("answer two") && !tools[1].contains("answer one"));
    assert!(tools[1].contains(&format!("sessionID=\"{child}\"")));
}

// ─────────────────────────────── AT-06 ───────────────────────────────

#[tokio::test]
async fn at06_cannot_resume_a_child_of_another_root_or_owner() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls(
        "",
        vec![("c1", "subagent", sub_new("reviewer", "secret task"))],
    );
    gw.push_text("secret child answer");
    gw.push_text("root A done");
    let (o, root_a) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;
    run_turn(&o, &root_a, "go").await;
    let child = links(&env, &root_a).await[0]
        .child_session_id
        .clone()
        .unwrap();
    let before = history(&o, &child).await;

    // Root B (cùng owner) cố resume child của Root A.
    let root_b = o.create_session(&alice(), "root").await.unwrap();
    gw.push_tool_calls(
        "",
        vec![("b1", "subagent", sub_resume("reviewer", &child, "hijack"))],
    );
    gw.push_text("root B done");
    let r = run_turn(&o, &root_b, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    let tools = tool_texts(&history(&o, &root_b).await);
    assert!(tools[0].contains("SESSION_NOT_CHILD"), "{}", tools[0]);
    assert!(!tools[0].contains("secret child answer"));
    assert_eq!(history(&o, &child).await, before, "child history untouched");
    assert!(links(&env, &root_b).await[0].child_session_id.is_none());

    // session_id không tồn tại cũng cho cùng mã (không lộ sự tồn tại).
    gw.push_tool_calls(
        "",
        vec![("b2", "subagent", sub_resume("reviewer", "no-such", "x"))],
    );
    gw.push_text("again");
    run_turn(&o, &root_b, "again").await;
    assert!(tool_texts(&history(&o, &root_b).await)[1].contains("SESSION_NOT_CHILD"));

    // Owner khác không đọc được child (ownership).
    let bob = CallerContext::new("bob", "ws1");
    let e = o
        .history(&bob, &child, PageRequest::first(10))
        .await
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::PermissionDenied);
    let e = o.submit(&bob, &child, "hi".into()).await.unwrap_err();
    assert_eq!(e.code(), ErrorCode::PermissionDenied);
}

// ───────────────────────── AT-07 / AT-08 / AT-09 (child) ─────────────────────────

#[tokio::test]
async fn at07_child_denied_tool_is_blocked_with_no_write() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (write, write_runs) = Probe::new("write_file", ToolEffect::Mutating, Behavior::Echo);
    let (read, read_runs) = Probe::new("read_note", ToolEffect::ReadOnly, Behavior::Echo);
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "do it"))]);
    // Child cố ghi dù chỉ được `read_note`; Root thì được phép `write_file`.
    gw.push_tool_calls("", vec![("k1", "write_file", json!({"text": "x"}))]);
    gw.push_text("child gave up");
    gw.push_text("root done");
    let (o, sid) = root_with_reviewer(
        &env,
        &gw,
        &["subagent", "write_file"],
        &["read_note"],
        vec![write, read],
    )
    .await;

    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(
        write_runs.load(Ordering::SeqCst),
        0,
        "denied tool must not run"
    );
    assert_eq!(read_runs.load(Ordering::SeqCst), 0);
    let child = links(&env, &sid).await[0].child_session_id.clone().unwrap();
    let ch = history(&o, &child).await;
    let tools = tool_texts(&ch);
    assert_eq!(tools.len(), 1);
    assert!(tools[0].contains("TOOL_NOT_ALLOWED"), "{}", tools[0]);
    // Child chỉ được quảng bá tool của chính nó.
    let child_req = gw.requests().into_iter().find(is_child_request).unwrap();
    let names: Vec<_> = child_req.tools.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, ["read_note"]);
    // Root không thấy lỗi tool của child trong history của nó.
    assert!(tool_texts(&history(&o, &sid).await)[0].contains("child gave up"));
    assert!(!tool_texts(&history(&o, &sid).await)[0].contains("TOOL_NOT_ALLOWED"));
}

#[tokio::test]
async fn at08_at09_child_skill_lands_in_child_history_and_denied_skill_errors() {
    let env = Env::new();
    env.add_skill("alpha", "ALPHA-BODY steps");
    env.add_skill("beta", "BETA-SECRET-BODY");
    let gw = ScriptedGateway::new();
    gw.push_tool_calls(
        "",
        vec![("c1", "subagent", sub_new("reviewer", "use skills"))],
    );
    gw.push_tool_calls(
        "",
        vec![
            ("k1", "skill", json!({"id": "alpha"})),
            ("k2", "skill", json!({"id": "beta"})),
        ],
    );
    gw.push_text("child used alpha");
    gw.push_text("root done");
    let mut cfg = env.config(vec![
        env.root(&["subagent"], &[]),
        env.reviewer(&["skill"], &["alpha"]),
    ]);
    cfg.skills_dir = Some(env.skills());
    let o = build(cfg, &gw, vec![]).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();

    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    let child = links(&env, &sid).await[0].child_session_id.clone().unwrap();

    // AT-08: skill content ở child history, không chui vào Root history.
    let child_tools = tool_texts(&history(&o, &child).await);
    assert_eq!(child_tools.len(), 2);
    assert!(child_tools[0].contains("ALPHA-BODY"), "{}", child_tools[0]);
    let root_tools = tool_texts(&history(&o, &sid).await);
    assert!(root_tools.iter().all(|t| !t.contains("ALPHA-BODY")));
    // AT-09: skill bị từ chối → PERMISSION_DENIED, không lộ nội dung/metadata.
    assert!(
        child_tools[1].contains("PERMISSION_DENIED"),
        "{}",
        child_tools[1]
    );
    assert!(!child_tools[1].contains("BETA-SECRET-BODY"));

    let reqs = gw.requests();
    let child_req = reqs.iter().find(|r| is_child_request(r)).unwrap();
    assert!(child_req.system.contains("<id>alpha</id>") && !child_req.system.contains("beta"));
    assert!(!reqs[0].system.contains("alpha"), "root catalog is its own");
}

#[tokio::test]
async fn child_uses_its_own_definition_and_only_the_hosts_child_ceiling() {
    // AT-39 cho child: hai session cùng agent, quyền hiệu lực của child khác nhau theo trần.
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (write, write_runs) = Probe::new("write_file", ToolEffect::Mutating, Behavior::Echo);
    let (read, _) = Probe::new("read_note", ToolEffect::ReadOnly, Behavior::Echo);
    let cfg = env.config(vec![
        env.root(&["subagent", "read_note"], &[]),
        env.reviewer(&["read_note", "write_file"], &[]),
    ]);
    let o = build(cfg, &gw, vec![write, read]).await;

    // Policy của Root chỉ thu hẹp Root: child vẫn dùng tool của chính nó (write_file chạy).
    let root_only = o
        .create_session_with_policy(
            &alice(),
            "root",
            SessionPolicy::default().with_allowed_tools(["subagent"]),
        )
        .await
        .unwrap();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "do it"))]);
    gw.push_tool_calls("", vec![("k1", "write_file", json!({"text": "x"}))]);
    gw.push_text("child done");
    gw.push_text("root done");
    let r = run_turn(&o, &root_only, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(write_runs.load(Ordering::SeqCst), 1);
    let child = links(&env, &root_only).await[0]
        .child_session_id
        .clone()
        .unwrap();
    assert!(tool_texts(&history(&o, &child).await)[0].contains("write_file:x"));

    // Trần child do host đặt: write_file bị chặn, chỉ read_note được quảng bá.
    let capped = o
        .create_session_with_policy(
            &alice(),
            "root",
            SessionPolicy::default().with_child_allowed_tools(["read_note"]),
        )
        .await
        .unwrap();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "do it"))]);
    gw.push_tool_calls("", vec![("k1", "write_file", json!({"text": "y"}))]);
    gw.push_text("child done");
    gw.push_text("root done");
    let r = run_turn(&o, &capped, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(
        write_runs.load(Ordering::SeqCst),
        1,
        "denied under the ceiling"
    );
    let child = links(&env, &capped).await[0]
        .child_session_id
        .clone()
        .unwrap();
    assert!(tool_texts(&history(&o, &child).await)[0].contains("TOOL_NOT_ALLOWED"));
    let child_req = gw.requests().into_iter().rfind(is_child_request).unwrap();
    let names: Vec<_> = child_req.tools.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, ["read_note"]);
}

#[tokio::test]
async fn host_cannot_submit_directly_into_a_child_session() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "work"))]);
    gw.push_text("child final");
    gw.push_text("root done");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;
    run_turn(&o, &sid, "go").await;
    let child = links(&env, &sid).await[0].child_session_id.clone().unwrap();
    let before = history(&o, &child).await;

    // Child đang rảnh vẫn không nhận input trực tiếp từ host.
    let e = o
        .submit(&alice(), &child, "direct".into())
        .await
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::PermissionDenied);
    assert_eq!(history(&o, &child).await, before);
    assert_eq!(gw.call_count(), 3);
    // Đọc vẫn được; child vẫn resume được qua parent.
    let child_for_script = child.clone();
    gw.push_with(move |_| {
        tool_call_response("c2", sub_resume("reviewer", &child_for_script, "more"))
    });
    gw.push_text("child more");
    gw.push_text("root again");
    let r = run_turn(&o, &sid, "again").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(links(&env, &sid).await.len(), 2);
}

// ─────────────────────────────── AT-10 ───────────────────────────────

#[tokio::test]
async fn at10_child_without_final_text_yields_placeholder() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls(
        "",
        vec![("c1", "subagent", sub_new("reviewer", "say nothing"))],
    );
    gw.push_text("");
    gw.push_text("root noticed the empty reply");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;

    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    let tools = tool_texts(&history(&o, &sid).await);
    assert!(tools[0].contains(PLACEHOLDER), "{}", tools[0]);
    let mut c = env.sql().await;
    let result_json: String =
        sqlx::query("SELECT result_json FROM tool_invocations WHERE tool_name = 'subagent'")
            .fetch_one(&mut c)
            .await
            .unwrap()
            .get(0);
    let v: Value = serde_json::from_str(&result_json).unwrap();
    assert_eq!(v["metadata"]["empty_text"], json!(true));
    assert_eq!(v["metadata"]["status"], json!("completed"));
}

// ─────────────────────────────── AT-13 ───────────────────────────────

#[tokio::test]
async fn at13_fake_proves_routing_selection_follows_the_child_definition() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls(
        "",
        vec![("c1", "subagent", sub_new("reviewer", "route me"))],
    );
    gw.push_text("child text");
    gw.push_text("root done");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;
    run_turn(&o, &sid, "go").await;

    let reqs = gw.requests();
    assert_eq!(
        (reqs[0].provider_id.as_str(), reqs[0].model_id.as_str()),
        ("fake", "model-x")
    );
    assert_eq!(
        (reqs[1].provider_id.as_str(), reqs[1].model_id.as_str()),
        ("fake2", "model-y")
    );
    assert_eq!(
        (reqs[2].provider_id.as_str(), reqs[2].model_id.as_str()),
        ("fake", "model-x")
    );
    // Routing + fingerprint của attempt được persist theo từng run.
    let child = links(&env, &sid).await[0].child_session_id.clone().unwrap();
    let mut c = env.sql().await;
    let row = sqlx::query("SELECT provider_id, model_id, provider_config_fingerprint FROM agent_runs WHERE session_id = ?1")
        .bind(&child)
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>(0), "fake2");
    assert_eq!(row.get::<String, _>(1), "model-y");
    assert_eq!(
        row.get::<Option<String>, _>(2).as_deref(),
        o.provider_fingerprint("fake2")
    );
}

// ─────────────────────────────── AT-14 ───────────────────────────────

#[tokio::test]
async fn at14_no_second_run_can_enter_an_active_child_and_history_stays_clean() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (slow, slow_runs) = Probe::new(
        "slow_ro",
        ToolEffect::ReadOnly,
        Behavior::Sleep(Duration::from_millis(900)),
    );
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "work"))]);
    gw.push_tool_calls("", vec![("k1", "slow_ro", json!({"text": "t"}))]);
    gw.push_text("child final");
    gw.push_text("root done");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &["slow_ro"], vec![slow]).await;

    let turn = o.submit(&alice(), &sid, "go".into()).await.unwrap();
    // Chờ child đang chạy (tool chậm) rồi cố submit trực tiếp vào child session: bị từ chối
    // (child chỉ nhận input qua parent). Tranh chấp giữa hai parent invocation được chứng minh
    // ở store (`concurrent_resumes_of_one_child_admit_exactly_one`).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while slow_runs.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "child never ran its tool"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let child = links(&env, &sid).await[0].child_session_id.clone().unwrap();
    let e = o
        .submit(&alice(), &child, "intruder".into())
        .await
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::PermissionDenied);

    let r = wait_terminal(&o, &alice(), &sid, &turn.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    let ch = history(&o, &child).await;
    assert_eq!(ch.len(), 4, "user, assistant(call), tool, final: {ch:?}");
    assert!(ch.iter().all(|m| m.text != "intruder"));
    let seqs: Vec<u64> = ch.iter().map(|m| m.sequence).collect();
    assert_eq!(seqs, [1, 2, 3, 4]);
}

// ─────────────────────────────── AT-16 ───────────────────────────────

#[tokio::test]
async fn at16_history_and_child_link_survive_restart() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls(
        "",
        vec![("c1", "subagent", sub_new("reviewer", "persist me"))],
    );
    gw.push_text("persisted answer");
    gw.push_text("root done");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;
    run_turn(&o, &sid, "go").await;
    let l = links(&env, &sid).await;
    let (child, turn) = (
        l[0].child_session_id.clone().unwrap(),
        l[0].child_turn_id.clone().unwrap(),
    );
    o.shutdown().await.unwrap();
    drop(o);

    let gw2 = ScriptedGateway::new();
    let cfg = env.config(vec![env.root(&["subagent"], &[]), env.reviewer(&[], &[])]);
    let o2 = build(cfg, &gw2, vec![]).await;
    let l2 = links(&env, &sid).await;
    assert_eq!(l2[0].child_session_id.as_deref(), Some(child.as_str()));
    assert_eq!(l2[0].child_turn_id.as_deref(), Some(turn.as_str()));
    let ch = history(&o2, &child).await;
    assert_eq!(
        ch.iter().map(|m| m.text.as_str()).collect::<Vec<_>>(),
        ["persist me", "persisted answer"]
    );
    let cr = o2.result(&alice(), &child, &turn).await.unwrap();
    assert_eq!(cr.output.as_deref(), Some("persisted answer"));
    let rh = history(&o2, &sid).await;
    assert!(tool_texts(&rh)[0].contains(&format!("sessionID=\"{child}\"")));
    assert_eq!(gw2.call_count(), 0, "reopen must not call any model");
}

// ─────────────────────────────── AT-38 ───────────────────────────────

#[tokio::test]
async fn at38_lost_triggers_after_admission_still_run_exactly_one_child_turn() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "once"))]);
    gw.push_text("child once");
    gw.push_text("root done");
    let cfg = env.config(vec![env.root(&["subagent"], &[]), env.reviewer(&[], &[])]);
    // Mọi trigger sau admission (root và child) bị bỏ: claim đã commit nhưng chưa spawn.
    let flag = Arc::new(AtomicBool::new(true));
    let o = build_with(cfg, &gw, vec![], Some(flag)).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();

    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    let child = links(&env, &sid).await[0].child_session_id.clone().unwrap();
    assert_eq!(
        scalar_i64(
            &env,
            "SELECT COUNT(*) FROM agent_runs WHERE session_id = ?1",
            &child
        )
        .await,
        1
    );
    assert_eq!(gw.call_count(), 3);
    assert!(tool_texts(&history(&o, &sid).await)[0].contains("child once"));
}

#[tokio::test]
async fn at38_child_terminal_before_parent_result_commit_is_never_readmitted() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "do once"))]);
    gw.push_text("child final");
    gw.push_text("never reached");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;
    // Fault: parent không commit được tool result (child đã terminal trước đó).
    let mut c = env.sql().await;
    sqlx::query(
        "CREATE TRIGGER block_root_tool_msg BEFORE INSERT ON messages \
         WHEN NEW.role = 'tool' AND NEW.session_id IN \
              (SELECT id FROM sessions WHERE parent_session_id IS NULL) \
         BEGIN SELECT RAISE(ABORT, 'blocked'); END",
    )
    .execute(&mut c)
    .await
    .unwrap();

    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Interrupted);
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("TOOL_OUTCOME_UNKNOWN")
    );
    // Vài chu kỳ sweep: không admit lại, không gọi model lần nữa.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let l = links(&env, &sid).await;
    let (child, turn) = (
        l[0].child_session_id.clone().unwrap(),
        l[0].child_turn_id.clone().unwrap(),
    );
    assert_eq!(
        scalar_i64(
            &env,
            "SELECT COUNT(*) FROM agent_runs WHERE session_id = ?1",
            &child
        )
        .await,
        1
    );
    assert_eq!(gw.call_count(), 2);
    // Child final vẫn đọc được qua link đã persist.
    let cr = o.result(&alice(), &child, &turn).await.unwrap();
    assert_eq!(cr.status, RunStatus::Completed);
    assert_eq!(cr.output.as_deref(), Some("child final"));
}

#[tokio::test]
async fn at38_failed_child_admission_leaves_no_trace_and_parent_gets_a_tool_error() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "x"))]);
    gw.push_text("root saw the failure");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;
    let mut c = env.sql().await;
    sqlx::query(
        "CREATE TRIGGER fail_child_run BEFORE INSERT ON agent_runs \
         WHEN NEW.session_id IN (SELECT id FROM sessions WHERE parent_session_id IS NOT NULL) \
         BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(&mut c)
    .await
    .unwrap();

    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert!(tool_texts(&history(&o, &sid).await)[0].contains("CHILD_FAILED"));
    let n: i64 = sqlx::query("SELECT COUNT(*) FROM sessions WHERE parent_session_id IS NOT NULL")
        .fetch_one(&mut c)
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0, "atomic: no half-created child");
    let l = links(&env, &sid).await;
    assert!(l[0].child_session_id.is_none() && l[0].child_turn_id.is_none());
    assert_eq!(gw.call_count(), 2, "child never ran");
}

// ─────────────────────── Tool errors (docs/06 §6.6) ───────────────────────

#[tokio::test]
async fn policy_errors_create_no_child() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let ask = |id: &str, args: Value| (id.to_owned(), args);
    let calls = [
        ask("e1", sub_new("ghost", "x")),
        ask("e2", sub_new("root", "x")), // Primary-only: không làm child được
        ask("e3", json!({"agent": "reviewer", "description": "d"})), // thiếu prompt
        ask("e4", sub_new("reviewer", "   ")),
        ask(
            "e5",
            json!({"agent": "reviewer", "description": "d", "prompt": "p", "background": true}),
        ),
    ];
    gw.push_tool_calls(
        "",
        calls
            .iter()
            .map(|(i, a)| (i.as_str(), "subagent", a.clone()))
            .collect(),
    );
    gw.push_text("done");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    let t = tool_texts(&history(&o, &sid).await);
    assert!(t[0].contains("AGENT_NOT_FOUND"), "{}", t[0]);
    assert!(t[1].contains("MODE_DENIED"), "{}", t[1]);
    assert!(t[2].contains("INVALID_ARGUMENTS"), "{}", t[2]);
    assert!(t[3].contains("INVALID_ARGUMENTS"), "{}", t[3]);
    assert!(t[4].contains("INVALID_ARGUMENTS"), "{}", t[4]);
    assert_eq!(
        scalar_i64(
            &env,
            "SELECT COUNT(*) FROM sessions WHERE parent_session_id = ?1",
            &sid
        )
        .await,
        0
    );
    assert_eq!(gw.call_count(), 2, "no child model call");
}

#[tokio::test]
async fn subagent_is_hidden_without_permission_and_child_depth_is_limited() {
    // Root không được cấp `subagent` → không quảng bá, gọi bị chặn.
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "x"))]);
    gw.push_text("done");
    let (o, sid) = root_with_reviewer(&env, &gw, &[], &[], vec![]).await;
    run_turn(&o, &sid, "go").await;
    assert!(gw.requests()[0].tools.iter().all(|t| t.name != "subagent"));
    assert!(tool_texts(&history(&o, &sid).await)[0].contains("TOOL_NOT_ALLOWED"));
    o.shutdown().await.unwrap();

    // Child được cấp `subagent` vẫn bị DEPTH_LIMIT (depth=1) và không thấy tool này.
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "outer"))]);
    gw.push_tool_calls("", vec![("k1", "subagent", sub_new("reviewer", "inner"))]);
    gw.push_text("child done");
    gw.push_text("root done");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &["subagent"], vec![]).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    let child = links(&env, &sid).await[0].child_session_id.clone().unwrap();
    assert!(tool_texts(&history(&o, &child).await)[0].contains("DEPTH_LIMIT"));
    let child_req = gw.requests().into_iter().find(is_child_request).unwrap();
    assert!(child_req.tools.iter().all(|t| t.name != "subagent"));
    assert_eq!(
        scalar_i64(
            &env,
            "SELECT COUNT(*) FROM sessions WHERE parent_session_id IS NOT NULL AND id <> ?1",
            &child
        )
        .await,
        0
    );
}

#[tokio::test]
async fn child_failures_become_tool_errors_with_the_child_session_id() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "a"))]);
    gw.push_error(GatewayError::Provider);
    gw.push_with(|req| {
        // Root resume đúng child vừa lỗi (history còn, session dùng lại được).
        let text = req
            .messages
            .iter()
            .rev()
            .find(|m| m.role == ModelRole::Tool)
            .unwrap()
            .text
            .clone();
        let sid = text
            .split("subagent session ")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .expect("child session id in the error")
            .to_owned();
        tool_call_response("c2", sub_resume("reviewer", &sid, "retry"))
    });
    gw.push_text("recovered");
    gw.push_text("root done");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    let t = tool_texts(&history(&o, &sid).await);
    assert!(
        t[0].contains("CHILD_FAILED") && t[0].contains("PROVIDER_UNAVAILABLE"),
        "{}",
        t[0]
    );
    assert!(t[1].contains("recovered"), "{}", t[1]);
    let l = links(&env, &sid).await;
    assert_eq!(l[0].status, "failed");
    assert_eq!(l[0].child_session_id, l[1].child_session_id);
    // Lỗi chỉ chứa mã ổn định, không có secret.
    assert!(!t[0].contains("sk-test-not-real"));
}

#[tokio::test]
async fn child_timeout_and_auth_errors_map_to_stable_codes() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "a"))]);
    gw.push_error(GatewayError::Timeout);
    gw.push_tool_calls("", vec![("c2", "subagent", sub_new("reviewer", "b"))]);
    gw.push_error(GatewayError::Auth);
    gw.push_text("done");
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    let t = tool_texts(&history(&o, &sid).await);
    assert!(t[0].contains("PROVIDER_TIMEOUT"), "{}", t[0]);
    assert!(t[1].contains("MODEL_UNAVAILABLE"), "{}", t[1]);
}

// ─────────────────────────── Shutdown / cancel ───────────────────────────

#[tokio::test]
async fn shutdown_propagates_to_the_active_child_and_keeps_the_link() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (wait, wait_runs) = Probe::new("wait_ro", ToolEffect::ReadOnly, Behavior::UntilCancelled);
    gw.push_tool_calls(
        "",
        vec![("c1", "subagent", sub_new("reviewer", "long job"))],
    );
    gw.push_tool_calls("", vec![("k1", "wait_ro", json!({"text": "t"}))]);
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &["wait_ro"], vec![wait]).await;
    o.submit(&alice(), &sid, "go".into()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while wait_runs.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "child never started its tool"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let child = links(&env, &sid).await[0].child_session_id.clone().unwrap();
    o.shutdown().await.unwrap();

    let mut c = env.sql().await;
    let status = |id: String| {
        sqlx::query("SELECT status, error_code FROM agent_runs WHERE session_id = ?1").bind(id)
    };
    let parent = status(sid.clone()).fetch_one(&mut c).await.unwrap();
    assert_eq!(parent.get::<String, _>(0), "interrupted");
    assert_eq!(
        parent.get::<Option<String>, _>(1).as_deref(),
        Some("SHUTTING_DOWN")
    );
    let childrow = status(child.clone()).fetch_one(&mut c).await.unwrap();
    assert_eq!(childrow.get::<String, _>(0), "interrupted");
    let l = links(&env, &sid).await;
    assert_eq!(l[0].status, "interrupted");
    assert_eq!(l[0].child_session_id.as_deref(), Some(child.as_str()));
    assert!(l[0].child_turn_id.is_some());
    // Không có tool-result giả cho parent.
    let n: i64 =
        sqlx::query("SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND role = 'tool'")
            .bind(&sid)
            .fetch_one(&mut c)
            .await
            .unwrap()
            .get(0);
    assert_eq!(n, 0);
}

#[tokio::test]
async fn shutdown_during_child_admission_creates_no_child() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "subagent", sub_new("reviewer", "x"))]);
    let (o, sid) = root_with_reviewer(&env, &gw, &["subagent"], &[], vec![]).await;
    gw.hold();
    o.submit(&alice(), &sid, "go".into()).await.unwrap();
    // Root bị chặn ở gateway; shutdown hủy trước khi có tool call nào.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while gw.call_count() == 0 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    o.shutdown().await.unwrap();
    assert_eq!(
        scalar_i64(
            &env,
            "SELECT COUNT(*) FROM sessions WHERE parent_session_id IS NOT NULL AND ?1 = ?1",
            &sid
        )
        .await,
        0
    );
}
