//! M2 gate (fake LLM + SQLite thật): AT-07/08/09/15/28/33/35/36/39/43, mandatory base trên
//! mọi request, denied tool không chạy/không ghi, input error thành technical result đúng
//! pairing, output cap tường minh, không double transcript write.
//!
//! Mode: deterministic fake gateway (`test-support`) — KHÔNG phải evidence provider live.
//! Root session cô lập hành vi tool/skill (Child thuộc M3).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestrator_agent_rs::test_support::{ModelRequest, ModelRole, ScriptedGateway};
use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, CallerContext, ConfigReason, ErrorCode, HostTool,
    LlmBackend, LlmConfig, LlmProviderConfig, MessageRole, MessageView, Orchestrator,
    OrchestratorConfig, OrchestratorError, PageRequest, ProviderAuth, RunStatus, SecretString,
    SessionPolicy, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};

const BASE: &str = include_str!("../src/base_instructions.md");
const SESSION_PROJECT: &str = "PROJECT-RULE: always be precise.";

#[derive(Clone, Copy)]
enum Behavior {
    Echo,
    Big(usize),
    Sleep(Duration),
    Panic,
    UntilCancelled,
}

struct Probe {
    name: &'static str,
    effect: ToolEffect,
    behavior: Behavior,
    runs: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<String>>>,
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
            log: Arc::new(Mutex::new(Vec::new())),
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
        self.log.lock().unwrap().push(format!("start:{text}"));
        ctx.progress("working");
        let content = match self.behavior {
            Behavior::Echo => format!("{}:{text}", self.name),
            Behavior::Big(n) => "x".repeat(n),
            Behavior::Sleep(d) => {
                tokio::time::sleep(d).await;
                "late".into()
            }
            Behavior::Panic => panic!("boom"),
            Behavior::UntilCancelled => {
                while !ctx.is_cancelled() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                "stopped".into()
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
        std::fs::write(dir.path().join("root.md"), "You are the root test agent.").unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), SESSION_PROJECT).unwrap();
        Self { dir }
    }

    fn store(&self) -> PathBuf {
        self.dir.path().join("store")
    }

    fn skills(&self) -> PathBuf {
        self.dir.path().join("skills")
    }

    fn add_skill(&self, id: &str, name: &str, body: &str) {
        let d = self.skills().join(id);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(
            d.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: desc of {name}\n---\n{body}\n"),
        )
        .unwrap();
    }

    fn config(&self, agent: AgentDefinition) -> OrchestratorConfig {
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
            vec![agent],
            self.store(),
        );
        cfg.runtime.supervisor_sweep = Duration::from_millis(50);
        cfg.runtime.shutdown_grace = Duration::from_secs(5);
        cfg.project_instructions = Some(self.dir.path().join("AGENTS.md"));
        cfg
    }

    fn agent(&self, tools: &[&str], skills: &[&str]) -> AgentDefinition {
        AgentDefinition::new("root", "fake", "model-x")
            .with_mode(AgentMode::Primary)
            .with_prompt_path(self.dir.path().join("root.md"))
            .with_allowed_tools(tools.iter().copied())
            .with_allowed_skills(skills.iter().copied())
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

async fn run_turn(o: &Orchestrator, sid: &str, text: &str) -> AgentResult {
    let turn = o.submit(&alice(), sid, text.into()).await.unwrap();
    wait_terminal(o, sid, &turn.turn_id).await
}

async fn history(o: &Orchestrator, sid: &str) -> Vec<MessageView> {
    o.history(&alice(), sid, PageRequest::first(200))
        .await
        .unwrap()
        .items
}

fn tool_messages(h: &[MessageView]) -> Vec<&MessageView> {
    h.iter().filter(|m| m.role == MessageRole::Tool).collect()
}

fn tool_texts(req: &ModelRequest) -> Vec<String> {
    req.messages
        .iter()
        .filter(|m| m.role == ModelRole::Tool)
        .map(|m| m.text.clone())
        .collect()
}

fn config_reason(e: OrchestratorError) -> ConfigReason {
    match e.reason() {
        Some(r) => r,
        None => panic!("expected config error, got {e:?}"),
    }
}

async fn one_session(
    env: &Env,
    gw: &Arc<ScriptedGateway>,
    tools: Vec<Arc<dyn HostTool>>,
    agent: AgentDefinition,
) -> (Orchestrator, String) {
    let o = build(env.config(agent), gw, tools).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    (o, sid)
}

#[tokio::test]
async fn at07_denied_tool_does_not_run_or_write_and_is_paired() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (read, read_runs) = Probe::new("read_note", ToolEffect::ReadOnly, Behavior::Echo);
    let (write, write_runs) = Probe::new("write_file", ToolEffect::Mutating, Behavior::Echo);
    gw.push_tool_calls("", vec![("call_1", "write_file", json!({"text": "x"}))]);
    gw.push_text("done");
    let (o, sid) = one_session(&env, &gw, vec![read, write], env.agent(&["read_note"], &[])).await;

    let r = run_turn(&o, &sid, "write something").await;
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(
        write_runs.load(Ordering::SeqCst),
        0,
        "denied tool must not run"
    );
    assert_eq!(read_runs.load(Ordering::SeqCst), 0);

    let h = history(&o, &sid).await;
    let tools = tool_messages(&h);
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].tool_call_id.as_deref(), Some("call_1"));
    assert!(
        tools[0].text.contains("TOOL_NOT_ALLOWED"),
        "{}",
        tools[0].text
    );

    // Tool bị từ chối không được quảng bá cho model.
    let reqs = gw.requests();
    let names: Vec<_> = reqs[0].tools.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["read_note"]);
    // Nội dung kết quả lỗi được gửi lại model ở bước kế tiếp.
    assert!(tool_texts(&reqs[1])[0].contains("TOOL_NOT_ALLOWED"));

    let mut c = env.sql().await;
    let status: String =
        sqlx::query("SELECT status FROM tool_invocations WHERE tool_call_id='call_1'")
            .fetch_one(&mut c)
            .await
            .unwrap()
            .get(0);
    assert_eq!(status, "failed");
}

#[tokio::test]
async fn at08_skill_loaded_into_history_and_catalog_lists_only_allowed() {
    let env = Env::new();
    env.add_skill("alpha", "alpha", "ALPHA-BODY steps");
    env.add_skill("beta", "beta", "BETA-SECRET-BODY");
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "skill", json!({"id": "alpha"}))]);
    gw.push_text("used alpha");
    let mut cfg = env.config(env.agent(&["skill"], &["alpha"]));
    cfg.skills_dir = Some(env.skills());
    let o = build(cfg, &gw, vec![]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();

    let r = run_turn(&o, &sid, "use the skill").await;
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(r.output.as_deref(), Some("used alpha"));

    let reqs = gw.requests();
    assert!(reqs[0].system.contains("<id>alpha</id>"));
    assert!(
        !reqs[0].system.contains("beta"),
        "denied skill leaked in catalog"
    );
    assert!(reqs[0].tools.iter().any(|t| t.name == "skill"));
    let loaded = &tool_texts(&reqs[1])[0];
    assert!(loaded.contains("ALPHA-BODY") && loaded.contains("<skill_content"));

    // Skill history thuộc session Root thực thi.
    let h = history(&o, &sid).await;
    assert!(tool_messages(&h)[0].text.contains("ALPHA-BODY"));
}

#[tokio::test]
async fn at09_denied_skill_is_permission_denied_without_leak() {
    let env = Env::new();
    env.add_skill("alpha", "alpha", "ALPHA-BODY");
    env.add_skill("beta", "beta", "BETA-SECRET-BODY");
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "skill", json!({"id": "beta"}))]);
    gw.push_text("ok");
    let mut cfg = env.config(env.agent(&["skill"], &["alpha"]));
    cfg.skills_dir = Some(env.skills());
    let o = build(cfg, &gw, vec![]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();

    let r = run_turn(&o, &sid, "load beta").await;
    assert_eq!(r.status, RunStatus::Completed);
    let h = history(&o, &sid).await;
    let t = &tool_messages(&h)[0].text;
    assert!(t.contains("PERMISSION_DENIED"), "{t}");
    assert!(!t.contains("BETA-SECRET-BODY") && !t.contains("desc of beta"));
    let all = format!("{:?}", gw.requests());
    assert!(!all.contains("BETA-SECRET-BODY"));
}

#[cfg(unix)]
#[tokio::test]
async fn at15_symlink_and_traversal_are_denied() {
    use std::os::unix::fs::symlink;

    // 1. SKILL.md là symlink trỏ ra ngoài root.
    let env = Env::new();
    let secret = env.dir.path().join("outside.md");
    std::fs::write(
        &secret,
        "---\nname: leak\ndescription: leak\n---\nOUTSIDE-SECRET",
    )
    .unwrap();
    std::fs::create_dir_all(env.skills().join("evil")).unwrap();
    symlink(&secret, env.skills().join("evil/SKILL.md")).unwrap();
    let gw = ScriptedGateway::new();
    let mut cfg = env.config(env.agent(&["skill"], &[]));
    cfg.skills_dir = Some(env.skills());
    let err = build(cfg, &gw, vec![])
        .await
        .err()
        .expect("symlinked SKILL.md rejected");
    assert_eq!(config_reason(err), ConfigReason::InvalidSkill);

    // 2. Cả thư mục skill là symlink.
    let env = Env::new();
    let outside = env.dir.path().join("outside_dir");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(
        outside.join("SKILL.md"),
        "---\nname: leak\ndescription: leak\n---\nx",
    )
    .unwrap();
    std::fs::create_dir_all(env.skills()).unwrap();
    symlink(&outside, env.skills().join("linked")).unwrap();
    let mut cfg = env.config(env.agent(&["skill"], &[]));
    cfg.skills_dir = Some(env.skills());
    let err = build(cfg, &gw, vec![])
        .await
        .err()
        .expect("symlinked dir rejected");
    assert_eq!(config_reason(err), ConfigReason::InvalidSkill);

    // 3. File liên quan là symlink: không được liệt kê hay lộ nội dung; id traversal bị từ chối.
    let env = Env::new();
    env.add_skill("alpha", "alpha", "ALPHA-BODY");
    let secret = env.dir.path().join("secret.txt");
    std::fs::write(&secret, "TOP-SECRET").unwrap();
    symlink(&secret, env.skills().join("alpha/linked.txt")).unwrap();
    std::fs::write(env.skills().join("alpha/real.txt"), "ok").unwrap();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls("", vec![("c1", "skill", json!({"id": "../outside"}))]);
    gw.push_tool_calls("", vec![("c2", "skill", json!({"id": "alpha"}))]);
    gw.push_text("done");
    let mut cfg = env.config(env.agent(&["skill"], &["alpha"]));
    cfg.skills_dir = Some(env.skills());
    let o = build(cfg, &gw, vec![]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    let h = history(&o, &sid).await;
    let tools = tool_messages(&h);
    assert!(tools[0].text.contains("INVALID_SKILL"), "{}", tools[0].text);
    assert!(tools[1].text.contains("real.txt"));
    assert!(!tools[1].text.contains("linked.txt") && !tools[1].text.contains("TOP-SECRET"));
}

#[tokio::test]
async fn at28_result_committed_and_progress_not_persisted() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (t, runs) = Probe::new("echo_tool", ToolEffect::ReadOnly, Behavior::Echo);
    gw.push_tool_calls("", vec![("c1", "echo_tool", json!({"text": "hi"}))]);
    gw.push_text("final");
    let (o, sid) = one_session(&env, &gw, vec![t], env.agent(&["echo_tool"], &[])).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let h = history(&o, &sid).await;
    assert_eq!(tool_messages(&h)[0].text, "echo_tool:hi");

    let mut c = env.sql().await;
    let row = sqlx::query("SELECT status, result_json, error_code FROM tool_invocations")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>(0), "completed");
    let result: Value = serde_json::from_str(&row.get::<String, _>(1)).unwrap();
    assert_eq!(result["content"], "echo_tool:hi");
    assert!(
        !row.get::<String, _>(1).contains("working"),
        "progress must not be persisted"
    );
}

#[tokio::test]
async fn at33_settle_failure_never_replays_tool() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (t, runs) = Probe::new("mutate", ToolEffect::Mutating, Behavior::Echo);
    gw.push_tool_calls("", vec![("c1", "mutate", json!({"text": "a"}))]);
    gw.push_text("never reached");
    let (o, sid) = one_session(&env, &gw, vec![t], env.agent(&["mutate"], &[])).await;

    // Fault: ghi tool message bị abort -> settle không commit được.
    let mut c = env.sql().await;
    sqlx::query(
        "CREATE TRIGGER block_tool_msg BEFORE INSERT ON messages WHEN NEW.role='tool' \
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
    assert!(r.final_message_id.is_none());

    // Chờ thêm vài chu kỳ sweep: tool không bị chạy lại, LLM không được gọi lần hai.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1, "mutating tool replayed");
    assert_eq!(gw.call_count(), 1);
    assert!(tool_messages(&history(&o, &sid).await).is_empty());
}

#[tokio::test]
async fn at35_gateway_error_persists_no_invocation_and_runs_no_tool() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (t, runs) = Probe::new("echo_tool", ToolEffect::ReadOnly, Behavior::Echo);
    gw.push_error(orchestrator_agent_rs::test_support::GatewayError::Provider);
    let (o, sid) = one_session(&env, &gw, vec![t], env.agent(&["echo_tool"], &[])).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Failed);
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    let mut c = env.sql().await;
    let n: i64 = sqlx::query("SELECT COUNT(*) FROM tool_invocations")
        .fetch_one(&mut c)
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
}

#[tokio::test]
async fn at36_all_calls_committed_before_sequential_dispatch() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (t, runs) = Probe::new("echo_tool", ToolEffect::ReadOnly, Behavior::Echo);
    let log = t.log.clone();
    gw.push_tool_calls(
        "",
        vec![
            ("a", "echo_tool", json!({"text": "1"})),
            ("b", "echo_tool", json!({"text": "2"})),
        ],
    );
    gw.push_text("both done");
    let (o, sid) = one_session(&env, &gw, vec![t], env.agent(&["echo_tool"], &[])).await;

    // Trigger: nếu tool đầu chạy mà invocation thứ hai chưa 'requested' thì chặn bằng log.
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    assert_eq!(
        *log.lock().unwrap(),
        vec!["start:1", "end:1", "start:2", "end:2"],
        "execution must be sequential"
    );

    // LLM bước kế tiếp chỉ được gọi khi cả hai result đã settle.
    let reqs = gw.requests();
    assert_eq!(reqs.len(), 2);
    assert_eq!(tool_texts(&reqs[1]), vec!["echo_tool:1", "echo_tool:2"]);
    let order: Vec<_> = reqs[1]
        .messages
        .iter()
        .filter(|m| m.role == ModelRole::Tool)
        .map(|m| m.tool_call_id.clone().unwrap())
        .collect();
    assert_eq!(order, vec!["a", "b"]);

    // Mọi invocation + assistant call đã commit cùng một assistant message; steps/sequence liền mạch.
    let h = history(&o, &sid).await;
    assert_eq!(h[1].tool_calls.len(), 2);
    let seqs: Vec<u64> = h.iter().map(|m| m.sequence).collect();
    assert_eq!(seqs, (1..=h.len() as u64).collect::<Vec<_>>());
    assert_eq!(
        tool_messages(&h).len(),
        2,
        "exactly one tool message per call"
    );
    let mut c = env.sql().await;
    let msgs: i64 = sqlx::query("SELECT COUNT(*) FROM messages WHERE role='tool'")
        .fetch_one(&mut c)
        .await
        .unwrap()
        .get(0);
    assert_eq!(msgs, 2);
}

#[tokio::test]
async fn at39_session_policy_narrows_catalog_tools_and_loading() {
    let env = Env::new();
    env.add_skill("alpha", "alpha", "ALPHA-BODY");
    env.add_skill("beta", "beta", "BETA-BODY");
    let gw = ScriptedGateway::new();
    let (t1, _) = Probe::new("tool_a", ToolEffect::ReadOnly, Behavior::Echo);
    let (t2, _) = Probe::new("tool_b", ToolEffect::ReadOnly, Behavior::Echo);
    let mut cfg = env.config(env.agent(&["skill", "tool_a", "tool_b"], &["alpha", "beta"]));
    cfg.skills_dir = Some(env.skills());
    let o = build(cfg, &gw, vec![t1, t2]).await.unwrap();

    let narrow = o
        .create_session_with_policy(
            &alice(),
            "root",
            SessionPolicy::default()
                .with_allowed_tools(["skill", "tool_a"])
                .with_allowed_skills(["alpha"]),
        )
        .await
        .unwrap();
    let wide = o.create_session(&alice(), "root").await.unwrap();
    // Policy không thể mở rộng: skill/tool không có ở agent bị bỏ qua.
    let widened = o
        .create_session_with_policy(
            &alice(),
            "root",
            SessionPolicy::default()
                .with_allowed_tools(["tool_a", "nonexistent"])
                .with_allowed_skills(["alpha", "ghost"]),
        )
        .await
        .unwrap();

    gw.push_text("n");
    run_turn(&o, &narrow, "hi").await;
    gw.push_text("w");
    run_turn(&o, &wide, "hi").await;
    gw.push_text("x");
    run_turn(&o, &widened, "hi").await;
    let reqs = gw.requests();
    let names = |r: &ModelRequest| {
        let mut n: Vec<_> = r.tools.iter().map(|t| t.name.clone()).collect();
        n.sort();
        n
    };
    assert_eq!(names(&reqs[0]), vec!["skill", "tool_a"]);
    assert!(reqs[0].system.contains("<id>alpha</id>") && !reqs[0].system.contains("<id>beta</id>"));
    assert_eq!(names(&reqs[1]), vec!["skill", "tool_a", "tool_b"]);
    assert!(reqs[1].system.contains("<id>beta</id>"));
    assert_eq!(names(&reqs[2]), vec!["tool_a"]);
    assert!(!reqs[2].system.contains("ghost"));

    // Forced load của skill ngoài policy bị từ chối ở session hẹp, vẫn chạy ở session rộng.
    gw.push_tool_calls("", vec![("c1", "skill", json!({"id": "beta"}))]);
    gw.push_text("after");
    run_turn(&o, &narrow, "load beta").await;
    gw.push_tool_calls("", vec![("c2", "skill", json!({"id": "beta"}))]);
    gw.push_text("after");
    run_turn(&o, &wide, "load beta").await;
    let hn = history(&o, &narrow).await;
    let hw = history(&o, &wide).await;
    assert!(tool_messages(&hn)[0].text.contains("PERMISSION_DENIED"));
    assert!(tool_messages(&hw)[0].text.contains("BETA-BODY"));
    // Lịch sử tool thuộc đúng session thực thi.
    assert!(!hn.iter().any(|m| m.text.contains("BETA-BODY")));
}

#[tokio::test]
async fn at43_provider_call_ids_are_not_global_keys() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (t, runs) = Probe::new("echo_tool", ToolEffect::ReadOnly, Behavior::Echo);
    gw.push_tool_calls("", vec![("call_0", "echo_tool", json!({"text": "one"}))]);
    gw.push_tool_calls("", vec![("call_0", "echo_tool", json!({"text": "two"}))]);
    gw.push_text("done");
    let (o, sid) = one_session(&env, &gw, vec![t], env.agent(&["echo_tool"], &[])).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    let h = history(&o, &sid).await;
    let tools = tool_messages(&h);
    assert_eq!(tools.len(), 2);
    assert_eq!(tools[0].text, "echo_tool:one");
    assert_eq!(tools[1].text, "echo_tool:two");
    let mut c = env.sql().await;
    let rows = sqlx::query("SELECT id, assistant_message_id FROM tool_invocations ORDER BY rowid")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0].get::<String, _>(0), rows[1].get::<String, _>(0));
    assert_ne!(rows[0].get::<String, _>(1), rows[1].get::<String, _>(1));

    // Cùng call id hai lần trong *một* response: protocol error, không persist gì.
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (t, runs) = Probe::new("echo_tool", ToolEffect::ReadOnly, Behavior::Echo);
    gw.push_tool_calls(
        "",
        vec![
            ("dup", "echo_tool", json!({"text": "1"})),
            ("dup", "echo_tool", json!({"text": "2"})),
        ],
    );
    let (o, sid) = one_session(&env, &gw, vec![t], env.agent(&["echo_tool"], &[])).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Failed);
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("PROVIDER_PROTOCOL_ERROR")
    );
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    let mut c = env.sql().await;
    let n: i64 = sqlx::query("SELECT COUNT(*) FROM tool_invocations")
        .fetch_one(&mut c)
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
    assert_eq!(
        history(&o, &sid).await.len(),
        1,
        "only the user input persisted"
    );
}

#[tokio::test]
async fn invalid_args_become_technical_result_with_correct_pairing() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (t, runs) = Probe::new("echo_tool", ToolEffect::Mutating, Behavior::Echo);
    gw.push_tool_calls(
        "",
        vec![
            ("bad", "echo_tool", json!({"text": 5})),
            ("good", "echo_tool", json!({"text": "ok"})),
        ],
    );
    gw.push_text("done");
    let (o, sid) = one_session(&env, &gw, vec![t], env.agent(&["echo_tool"], &[])).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "invalid call must not dispatch"
    );
    let h = history(&o, &sid).await;
    let tools = tool_messages(&h);
    assert_eq!(tools[0].tool_call_id.as_deref(), Some("bad"));
    assert!(tools[0].text.contains("INVALID_ARGUMENTS"));
    assert_eq!(tools[1].tool_call_id.as_deref(), Some("good"));
    assert_eq!(tools[1].text, "echo_tool:ok");
}

#[tokio::test]
async fn output_cap_is_explicit_and_hard_cap_fails_closed() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (big, _) = Probe::new("big_tool", ToolEffect::ReadOnly, Behavior::Big(10_000));
    let (huge, _) = Probe::new("huge_tool", ToolEffect::ReadOnly, Behavior::Big(50_000));
    gw.push_tool_calls(
        "",
        vec![
            ("c1", "big_tool", json!({"text": "a"})),
            ("c2", "huge_tool", json!({"text": "b"})),
        ],
    );
    gw.push_text("done");
    let mut cfg = env.config(env.agent(&["big_tool", "huge_tool"], &[]));
    cfg.runtime.tool_output_cap_bytes = 1_000;
    cfg.runtime.tool_result_hard_cap_bytes = 20_000;
    let o = build(cfg, &gw, vec![big, huge]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    let h = history(&o, &sid).await;
    let tools = tool_messages(&h);
    assert!(tools[0].text.len() < 2_000, "model text must be capped");
    assert!(
        tools[0].text.contains("truncated"),
        "cap must be explicit: {}",
        tools[0].text
    );
    assert!(tools[1].text.contains("TOOL_OUTPUT_TOO_LARGE"));
    // Full bounded content nằm ở result_json (≤ hard cap).
    let mut c = env.sql().await;
    let stored: String =
        sqlx::query("SELECT result_json FROM tool_invocations WHERE tool_call_id='c1'")
            .fetch_one(&mut c)
            .await
            .unwrap()
            .get(0);
    assert!(stored.len() >= 10_000);
}

#[tokio::test]
async fn mandatory_base_and_project_instructions_on_every_request() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (t, _) = Probe::new("echo_tool", ToolEffect::ReadOnly, Behavior::Echo);
    gw.push_tool_calls("", vec![("c1", "echo_tool", json!({"text": "x"}))]);
    gw.push_text("done");
    let (o, sid) = one_session(&env, &gw, vec![t], env.agent(&["echo_tool"], &[])).await;
    run_turn(&o, &sid, "go").await;
    let reqs = gw.requests();
    assert_eq!(reqs.len(), 2);
    for r in &reqs {
        assert!(
            r.system.starts_with(BASE.trim_end()),
            "base must lead every request"
        );
        assert!(r.system.contains("You are the root test agent."));
        assert!(r.system.contains(SESSION_PROJECT));
        assert!(
            r.system.find("root test agent").unwrap() > BASE.trim_end().len() - 1,
            "agent prompt after base"
        );
    }
}

#[tokio::test]
async fn hallucinated_subagent_is_unavailable_in_m2() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls(
        "",
        vec![("c1", "subagent", json!({"agent": "x", "task": "y"}))],
    );
    gw.push_text("done");
    let (o, sid) = one_session(&env, &gw, vec![], env.agent(&["subagent"], &[])).await;
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    assert!(gw.requests()[0].tools.iter().all(|t| t.name != "subagent"));
    let h = history(&o, &sid).await;
    assert!(tool_messages(&h)[0].text.contains("TOOL_UNAVAILABLE"));
}

#[tokio::test]
async fn tool_timeout_and_panic_are_classified_by_effect() {
    let env = Env::new();
    // ReadOnly timeout -> technical result, run continues.
    let gw = ScriptedGateway::new();
    let (slow, _) = Probe::new(
        "slow_ro",
        ToolEffect::ReadOnly,
        Behavior::Sleep(Duration::from_secs(5)),
    );
    let (pan, _) = Probe::new("panic_ro", ToolEffect::ReadOnly, Behavior::Panic);
    gw.push_tool_calls(
        "",
        vec![
            ("c1", "slow_ro", json!({"text": "a"})),
            ("c2", "panic_ro", json!({"text": "b"})),
        ],
    );
    gw.push_text("done");
    let mut cfg = env.config(env.agent(&["slow_ro", "panic_ro"], &[]));
    cfg.runtime.tool_timeout = Duration::from_millis(150);
    let o = build(cfg, &gw, vec![slow, pan]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Completed);
    let h = history(&o, &sid).await;
    let tools = tool_messages(&h);
    assert!(tools[0].text.contains("TOOL_TIMEOUT"));
    assert!(tools[1].text.contains("TOOL_PANICKED"));
    o.shutdown().await.unwrap();
    drop(o);

    // Mutating timeout -> outcome unknown, run Interrupted, không replay.
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (slow, runs) = Probe::new(
        "slow_mut",
        ToolEffect::Mutating,
        Behavior::Sleep(Duration::from_secs(5)),
    );
    gw.push_tool_calls("", vec![("c1", "slow_mut", json!({"text": "a"}))]);
    gw.push_text("never");
    let mut cfg = env.config(env.agent(&["slow_mut"], &[]));
    cfg.runtime.tool_timeout = Duration::from_millis(150);
    let o = build(cfg, &gw, vec![slow]).await.unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let r = run_turn(&o, &sid, "go").await;
    assert_eq!(r.status, RunStatus::Interrupted);
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("TOOL_OUTCOME_UNKNOWN")
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(gw.call_count(), 1);
}

#[tokio::test]
async fn shutdown_waits_for_cooperative_tool_and_commits_its_result() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (t, runs) = Probe::new("coop", ToolEffect::ReadOnly, Behavior::UntilCancelled);
    gw.push_tool_calls("", vec![("c1", "coop", json!({"text": "a"}))]);
    gw.push_text("never");
    let (o, sid) = one_session(&env, &gw, vec![t], env.agent(&["coop"], &[])).await;
    let turn = o.submit(&alice(), &sid, "go".into()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while runs.load(Ordering::SeqCst) == 0 {
        assert!(tokio::time::Instant::now() < deadline, "tool never started");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    o.shutdown().await.unwrap();
    drop(o);

    let (t2, _) = Probe::new("coop", ToolEffect::ReadOnly, Behavior::UntilCancelled);
    let o2 = build(
        env.config(env.agent(&["coop"], &[])),
        &ScriptedGateway::new(),
        vec![t2],
    )
    .await
    .unwrap();
    // M5: tool ReadOnly đã settle trước khi shutdown nên attempt bị ngắt được resume ở lần
    // khởi động sau; result đã commit giữ nguyên và tool không bị chạy lại.
    let r = wait_terminal(&o2, &sid, &turn.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.attempt, 1);
    let h = history(&o2, &sid).await;
    assert_eq!(
        tool_messages(&h)[0].text,
        "stopped",
        "result of the cooperative tool committed"
    );
    assert_eq!(
        tool_messages(&h).len(),
        1,
        "the tool result is not duplicated"
    );
    assert_eq!(gw.call_count(), 1, "no LLM step after cancel");
}

#[tokio::test]
async fn startup_validation_rejects_bad_skill_configuration() {
    let env = Env::new();
    env.add_skill("alpha", "alpha", "body");
    let gw = ScriptedGateway::new();

    // Agent tham chiếu skill không tồn tại.
    let mut cfg = env.config(env.agent(&["skill"], &["missing"]));
    cfg.skills_dir = Some(env.skills());
    let err = build(cfg, &gw, vec![]).await.err().unwrap();
    assert_eq!(config_reason(err), ConfigReason::UnknownSkill);

    // Hai skill trùng frontmatter name.
    env.add_skill("alpha2", "alpha", "body");
    let mut cfg = env.config(env.agent(&["skill"], &["alpha"]));
    cfg.skills_dir = Some(env.skills());
    let err = build(cfg, &gw, vec![]).await.err().unwrap();
    assert_eq!(config_reason(err), ConfigReason::DuplicateSkill);

    // Project instructions không đọc được.
    let env = Env::new();
    let mut cfg = env.config(env.agent(&[], &[]));
    cfg.project_instructions = Some(Path::new("/nonexistent/AGENTS.md").to_path_buf());
    let err = build(cfg, &gw, vec![]).await.err().unwrap();
    assert_eq!(config_reason(err), ConfigReason::ProjectInstructionsInvalid);
    let _ = ErrorCode::NotFound;
}
