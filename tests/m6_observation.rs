//! M6 gate (fake LLM + SQLite thật): AT-25–34/37/45 và ST-01–16 qua public `Orchestrator`
//! (`subscribe`/`snapshot`). Mode: deterministic fake gateway (`test-support`) + fault hook
//! chỉ điều khiển **publish** (`FeedFault`). KHÔNG phải HTTP/SSE wire evidence: harness client
//! transport-neutral ở dưới cài đặt đúng hợp đồng lifecycle của docs/16 §16.8 (subscribe trước
//! snapshot, refresh tới khi đạt revision, resync khi lag, refresh định kỳ khi còn active).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestrator_agent_rs::test_support::{
    FeedFault, GatewayError, ModelResponse, ModelToolCall, ScriptedGateway, StreamEvent,
};
use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, CallerContext, ConfigReason, ErrorCode, ExecutionEvent,
    FamilySnapshot, FeedItem, HostTool, InvocationStatus, LlmBackend, LlmConfig, LlmProviderConfig,
    MessageRole, Orchestrator, OrchestratorConfig, PageRequest, ProviderAuth, RunStatus,
    SecretString, Subscription, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};

const ROOT_MARK: &str = "You are the root test agent.";
const REVIEWER_MARK: &str = "You are the reviewer test agent.";
const API_KEY: &str = "sk-test-not-real";

// ───────────────────────────── tools ─────────────────────────────

#[derive(Clone)]
enum Behavior {
    Echo,
    /// `n` lần progress, mỗi lần cách nhau `gap`.
    Progress {
        n: usize,
        gap: Duration,
    },
    /// `n` progress liên tiếp không chờ (kiểm tra throttle).
    Burst(usize),
    /// Progress chứa secret.
    LeakProgress,
    Sleep(Duration),
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

    async fn execute(&self, ctx: ToolContext, _args: Value) -> Result<ToolOutcome, ToolError> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        match &self.behavior {
            Behavior::Echo => {}
            Behavior::Progress { n, gap } => {
                for i in 0..*n {
                    ctx.progress(&format!("step {i}"));
                    tokio::time::sleep(*gap).await;
                }
            }
            Behavior::Burst(n) => {
                for i in 0..*n {
                    ctx.progress(&format!("tick {i}"));
                }
            }
            Behavior::LeakProgress => ctx.progress(&format!("auth header {API_KEY} sent")),
            Behavior::Sleep(d) => tokio::time::sleep(*d).await,
        }
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!("{}:done", self.name),
            metadata: Value::Null,
        })
    }
}

// ───────────────────────────── env ─────────────────────────────

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

    fn root(&self, tools: &[&str]) -> AgentDefinition {
        AgentDefinition::new("root", "fake", "model-x")
            .with_mode(AgentMode::Primary)
            .with_prompt_path(self.dir.path().join("root.md"))
            .with_allowed_tools(tools.iter().copied())
    }

    fn reviewer(&self) -> AgentDefinition {
        AgentDefinition::new("reviewer", "fake", "model-x")
            .with_mode(AgentMode::Subagent)
            .with_description("Reviews code and reports findings")
            .with_prompt_path(self.dir.path().join("reviewer.md"))
    }

    fn config(&self, agents: Vec<AgentDefinition>) -> OrchestratorConfig {
        let mut cfg = OrchestratorConfig::new(
            LlmConfig {
                providers: vec![LlmProviderConfig {
                    id: "fake".into(),
                    backend: LlmBackend::OpenAi,
                    endpoint: None,
                    auth: ProviderAuth::ApiKey(SecretString::new(API_KEY)),
                    request_timeout: Duration::from_secs(30),
                }],
            },
            agents,
            self.store(),
        );
        cfg.runtime.supervisor_sweep = Duration::from_millis(50);
        cfg.runtime.shutdown_grace = Duration::from_secs(5);
        cfg.runtime.progress_min_interval = Duration::from_millis(10);
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

async fn build(
    cfg: OrchestratorConfig,
    gw: &Arc<ScriptedGateway>,
    tools: Vec<Arc<dyn HostTool>>,
    fault: Option<Arc<FeedFault>>,
) -> Arc<Orchestrator> {
    let mut b = Orchestrator::builder(cfg).with_gateway(gw.clone());
    if let Some(f) = fault {
        b = b.with_feed_fault(f);
    }
    for t in tools {
        b = b.register_tool(t);
    }
    Arc::new(b.build().await.unwrap())
}

/// Root agent đơn giản có tool `probe`.
async fn single(
    env: &Env,
    gw: &Arc<ScriptedGateway>,
    tool: Option<Arc<dyn HostTool>>,
) -> (Arc<Orchestrator>, String) {
    let names: &[&str] = if tool.is_some() { &["probe"] } else { &[] };
    let o = build(
        env.config(vec![env.root(names)]),
        gw,
        tool.into_iter().collect(),
        None,
    )
    .await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    (o, sid)
}

async fn family(
    env: &Env,
    gw: &Arc<ScriptedGateway>,
    fault: Option<Arc<FeedFault>>,
) -> (Arc<Orchestrator>, String) {
    let o = build(
        env.config(vec![env.root(&["subagent"]), env.reviewer()]),
        gw,
        vec![],
        fault,
    )
    .await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    (o, sid)
}

async fn wait_terminal(o: &Orchestrator, session: &str, turn: &str) -> RunStatus {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let r = o.result(&alice(), session, turn).await.unwrap();
        if r.status != RunStatus::Running {
            return r.status;
        }
        assert!(tokio::time::Instant::now() < deadline, "run stuck: {r:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn run_turn(o: &Orchestrator, sid: &str, text: &str) -> RunStatus {
    let turn = o.submit(&alice(), sid, text.into()).await.unwrap();
    wait_terminal(o, sid, &turn.turn_id).await
}

fn call(id: &str, name: &str, args: Value) -> ModelResponse {
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

fn probe_args() -> Value {
    json!({ "text": "x" })
}

// ───────────────────────────── collector ─────────────────────────────

/// Đọc subscription ở task riêng và ghi lại mọi item theo thứ tự nhận.
struct Collector {
    items: Arc<Mutex<Vec<FeedItem>>>,
}

fn collect(mut sub: Subscription) -> Collector {
    let items = Arc::new(Mutex::new(Vec::new()));
    let sink = items.clone();
    tokio::spawn(async move {
        while let Some(item) = sub.recv().await {
            sink.lock().unwrap().push(item);
        }
    });
    Collector { items }
}

impl Collector {
    fn events(&self) -> Vec<ExecutionEvent> {
        self.items
            .lock()
            .unwrap()
            .iter()
            .filter_map(|i| match i {
                FeedItem::Event(e) => Some(e.clone()),
                FeedItem::ResyncRequired => None,
            })
            .collect()
    }

    fn types(&self) -> Vec<String> {
        self.events().into_iter().map(|e| e.event_type).collect()
    }

    fn of(&self, ty: &str) -> Vec<ExecutionEvent> {
        self.events()
            .into_iter()
            .filter(|e| e.event_type == ty)
            .collect()
    }

    async fn wait_for(&self, what: &str, pred: impl Fn(&[ExecutionEvent]) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if pred(&self.events()) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timeout waiting for {what}; got {:?}",
                self.types()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

fn position(events: &[ExecutionEvent], pred: impl Fn(&ExecutionEvent) -> bool) -> usize {
    events
        .iter()
        .position(pred)
        .unwrap_or_else(|| panic!("event not found in {:?}", types_of(events)))
}

fn types_of(events: &[ExecutionEvent]) -> Vec<&str> {
    events.iter().map(|e| e.event_type.as_str()).collect()
}

fn deltas_of(events: &[ExecutionEvent], session: &str) -> String {
    events
        .iter()
        .filter(|e| e.event_type == "text.delta" && e.session_id == session)
        .map(|e| e.payload["delta"].as_str().unwrap().to_owned())
        .collect()
}

// ───────────────────────────── client harness ─────────────────────────────

/// Trạng thái client reference (docs/16 §16.8). Chỉ dựng từ snapshot canonical; event committed
/// chỉ làm invalidate, ephemeral chỉ là preview.
#[derive(Default)]
struct View {
    snapshot: Option<FamilySnapshot>,
    previews: HashMap<String, String>,
    subscriptions: usize,
    resyncs: usize,
    refreshes: usize,
    /// Snapshot có revision của một session thấp hơn snapshot trước đó.
    regressions: usize,
    /// Event committed tới với revision thấp hơn revision cao nhất đã thấy của session đó.
    inversions: usize,
    terminal_events: usize,
}

struct Observer {
    view: Arc<Mutex<View>>,
}

fn is_terminal_event(e: &ExecutionEvent) -> bool {
    matches!(
        e.event_type.as_str(),
        "run.completed" | "run.failed" | "run.cancelled" | "run.interrupted" | "tool.completed"
    )
}

fn spawn_observer(o: Arc<Orchestrator>, root: String, refresh: Duration) -> Observer {
    let view = Arc::new(Mutex::new(View::default()));
    let shared = view.clone();
    tokio::spawn(async move {
        let who = alice();
        let apply = |snap: FamilySnapshot| {
            let mut v = shared.lock().unwrap();
            if let Some(old) = &v.snapshot {
                let regressed = old.sessions().any(|s| {
                    snap.session(&s.session_id)
                        .is_some_and(|n| n.state_revision < s.state_revision)
                });
                if regressed {
                    v.regressions += 1;
                }
            }
            // Canonical thắng preview: bỏ preview của session đã hết active.
            v.previews.retain(|session, _| {
                snap.session(session).is_some_and(
                    |s| matches!(&s.latest_run, Some(r) if r.status == RunStatus::Running),
                )
            });
            v.refreshes += 1;
            v.snapshot = Some(snap);
        };
        'connect: loop {
            // Subscribe TRƯỚC snapshot.
            let Ok(mut sub) = o.subscribe(&who, &root).await else {
                return;
            };
            {
                let mut v = shared.lock().unwrap();
                v.subscriptions += 1;
                v.previews.clear();
            }
            let mut seen: HashMap<String, i64> = HashMap::new();
            match o.snapshot(&who, &root).await {
                Ok(s) => apply(s),
                Err(_) => return,
            }
            loop {
                let active = shared
                    .lock()
                    .unwrap()
                    .snapshot
                    .as_ref()
                    .is_some_and(FamilySnapshot::has_active_work);
                let tick = async {
                    if active {
                        tokio::time::sleep(refresh).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                };
                tokio::select! {
                    item = sub.recv() => match item {
                        Some(FeedItem::Event(e)) => {
                            if is_terminal_event(&e) {
                                shared.lock().unwrap().terminal_events += 1;
                            }
                            if let Some(rev) = e.state_revision {
                                let max = seen.entry(e.session_id.clone()).or_insert(0);
                                if rev < *max {
                                    shared.lock().unwrap().inversions += 1;
                                }
                                *max = (*max).max(rev);
                                // Refresh tới khi snapshot không cũ hơn mọi invalidation đã thấy.
                                loop {
                                    let current = shared.lock().unwrap().snapshot.clone();
                                    if current.as_ref().is_some_and(|s| {
                                        seen.iter().all(|(sid, r)| s.covers(sid, *r))
                                    }) {
                                        break;
                                    }
                                    match o.snapshot(&who, &root).await {
                                        Ok(s) => apply(s),
                                        Err(_) => return,
                                    }
                                }
                            } else if e.event_type == "text.delta" {
                                let mut v = shared.lock().unwrap();
                                let active = v.snapshot.as_ref().is_some_and(|s| {
                                    s.session(&e.session_id).is_some_and(|x| {
                                        matches!(&x.latest_run, Some(r) if r.status == RunStatus::Running)
                                    })
                                });
                                if active {
                                    v.previews
                                        .entry(e.session_id.clone())
                                        .or_default()
                                        .push_str(e.payload["delta"].as_str().unwrap_or_default());
                                }
                            }
                        }
                        Some(FeedItem::ResyncRequired) | None => {
                            shared.lock().unwrap().resyncs += 1;
                            continue 'connect;
                        }
                    },
                    () = tick => match o.snapshot(&who, &root).await {
                        Ok(s) => apply(s),
                        Err(_) => return,
                    },
                }
            }
        }
    });
    Observer { view }
}

impl Observer {
    async fn eventually(&self, what: &str, within: Duration, pred: impl Fn(&View) -> bool) {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if pred(&self.view.lock().unwrap()) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timeout waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn root_completed(v: &View) -> bool {
    v.snapshot
        .as_ref()
        .is_some_and(|s| matches!(&s.root.latest_run, Some(r) if r.status == RunStatus::Completed))
}

// ───────────────────────────── AT-26 / ST-01 / ST-12 ─────────────────────────────

#[tokio::test]
async fn at26_st01_text_deltas_arrive_in_order_and_only_the_final_message_is_stored() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let frags: Vec<String> = (0..20).map(|i| format!("p{i} ")).collect();
    let refs: Vec<&str> = frags.iter().map(String::as_str).collect();
    gw.push_stream_text(&refs);
    let (o, sid) = single(&env, &gw, None).await;
    let col = collect(o.subscribe(&alice(), &sid).await.unwrap());

    assert_eq!(run_turn(&o, &sid, "go").await, RunStatus::Completed);
    col.wait_for("run.completed", |e| {
        e.iter().any(|x| x.event_type == "run.completed")
    })
    .await;

    let events = col.events();
    let deltas: Vec<&ExecutionEvent> = events
        .iter()
        .filter(|e| e.event_type == "text.delta")
        .collect();
    // Có API key được cấu hình nên feed giữ lại phần đuôi để che secret bị cắt: số delta có
    // thể ít hơn số fragment, nhưng text ghép lại và thứ tự không đổi.
    assert!(
        deltas.len() >= 2,
        "text phải được stream, không dồn một lần"
    );
    let step_ids: std::collections::HashSet<_> = deltas
        .iter()
        .map(|e| e.payload["step_id"].as_str().unwrap())
        .collect();
    assert_eq!(step_ids.len(), 1, "một step");
    for (i, e) in deltas.iter().enumerate() {
        assert_eq!(e.payload["delta_index"], json!(i), "index đơn điệu");
        assert_eq!(e.state_revision, None, "delta là ephemeral");
    }
    assert_eq!(deltas_of(&events, &sid), frags.concat());
    let started = position(&events, |e| e.event_type == "step.started");
    let ended = position(&events, |e| e.event_type == "step.ended");
    let first = position(&events, |e| e.event_type == "text.delta");
    assert!(started < first && first < ended);
    assert_eq!(events[ended].payload["completed"], json!(true));

    // SQLite chỉ giữ message đã commit: 1 user + 1 assistant (không message theo delta).
    let h = o
        .history(&alice(), &sid, PageRequest::first(100))
        .await
        .unwrap()
        .items;
    assert_eq!(h.len(), 2);
    assert_eq!(h[1].role, MessageRole::Assistant);
    assert_eq!(h[1].text, frags.concat());
    let mut c = env.sql().await;
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(n, 2);
}

#[tokio::test]
async fn st12_st13_stream_error_before_finished_runs_no_tool_and_marks_the_preview_incomplete() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_stream_failure(
        vec![
            StreamEvent::TextDelta("partial ".into()),
            StreamEvent::ToolCallReady {
                id: "c1".into(),
                name: "probe".into(),
            },
        ],
        GatewayError::Transport,
    );
    let (probe, runs) = Probe::new("probe", ToolEffect::Mutating, Behavior::Echo);
    let (o, sid) = single(&env, &gw, Some(probe)).await;
    let col = collect(o.subscribe(&alice(), &sid).await.unwrap());

    assert_eq!(run_turn(&o, &sid, "go").await, RunStatus::Failed);
    col.wait_for("run.failed", |e| {
        e.iter().any(|x| x.event_type == "run.failed")
    })
    .await;

    assert_eq!(runs.load(Ordering::SeqCst), 0, "ST-13: tool không được gọi");
    let events = col.events();
    assert!(
        events.iter().all(|e| e.event_type != "tool.started"),
        "không invocation nào được persist: {:?}",
        types_of(&events)
    );
    assert_eq!(deltas_of(&events, &sid), "partial ");
    let ended = events
        .iter()
        .find(|e| e.event_type == "step.ended")
        .unwrap();
    assert_eq!(ended.payload["completed"], json!(false));
    assert!(ended.payload["error_code"].is_string());
    // Delta không phải final text bền vững.
    let h = o
        .history(&alice(), &sid, PageRequest::first(100))
        .await
        .unwrap()
        .items;
    assert!(h.iter().all(|m| m.role != MessageRole::Assistant));
    let snap = o.snapshot(&alice(), &sid).await.unwrap();
    assert!(snap.root.invocations.is_empty());
}

// ───────────────────────────── AT-28 / ST-03 / ST-14 ─────────────────────────────

#[tokio::test]
async fn at28_st03_progress_is_ephemeral_and_terminal_events_follow_the_commit() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_response(call("c1", "probe", probe_args()));
    gw.push_text("done");
    let (probe, _) = Probe::new(
        "probe",
        ToolEffect::ReadOnly,
        Behavior::Progress {
            n: 3,
            gap: Duration::from_millis(40),
        },
    );
    let (o, sid) = single(&env, &gw, Some(probe)).await;
    let mut sub = o.subscribe(&alice(), &sid).await.unwrap();
    let turn = o.submit(&alice(), &sid, "go".into()).await.unwrap();

    let mut seen: Vec<ExecutionEvent> = Vec::new();
    loop {
        let Some(FeedItem::Event(e)) = tokio::time::timeout(Duration::from_secs(15), sub.recv())
            .await
            .expect("timeout")
        else {
            panic!("feed ended early");
        };
        // Mọi committed event phải thấy được trong snapshot ngay khi nhận (publish sau commit).
        if let Some(rev) = e.state_revision {
            let snap = o.snapshot(&alice(), &sid).await.unwrap();
            assert!(
                snap.covers(&e.session_id, rev),
                "{} rev {rev} chưa commit",
                e.event_type
            );
            if e.event_type == "tool.completed" {
                let inv = &snap.root.invocations[0];
                assert_eq!(inv.status, InvocationStatus::Completed);
            }
            if e.event_type == "run.completed" {
                let r = snap.root.latest_run.as_ref().unwrap();
                assert_eq!(r.status, RunStatus::Completed);
            }
        }
        let done = e.event_type == "run.completed";
        seen.push(e);
        if done {
            break;
        }
    }
    wait_terminal(&o, &sid, &turn.turn_id).await;

    let started = position(&seen, |e| e.event_type == "tool.started");
    let first_progress = position(&seen, |e| e.event_type == "tool.progress");
    let completed = position(&seen, |e| e.event_type == "tool.completed");
    assert!(started < first_progress && first_progress < completed);
    let progress: Vec<_> = seen
        .iter()
        .filter(|e| e.event_type == "tool.progress")
        .collect();
    assert_eq!(progress.len(), 3);
    assert!(progress.iter().all(|e| e.state_revision.is_none()));
    assert_eq!(progress[2].payload["message"], json!("step 2"));
    assert_eq!(progress[0].payload["tool_call_id"], json!("c1"));
    assert_eq!(seen[started].payload["tool_name"], json!("probe"));
    assert_eq!(seen[started].root_session_id, sid);

    // Progress có thể mất: snapshot (canonical) không cần nó để dựng lại state.
    let snap = o.snapshot(&alice(), &sid).await.unwrap();
    assert_eq!(snap.root.invocations[0].status, InvocationStatus::Completed);
}

#[tokio::test]
async fn progress_bursts_are_throttled_and_never_block_the_tool() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_response(call("c1", "probe", probe_args()));
    gw.push_text("done");
    let (probe, _) = Probe::new("probe", ToolEffect::ReadOnly, Behavior::Burst(500));
    let mut cfg = env.config(vec![env.root(&["probe"])]);
    cfg.runtime.progress_min_interval = Duration::from_secs(60);
    let o = build(cfg, &gw, vec![probe], None).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let col = collect(o.subscribe(&alice(), &sid).await.unwrap());

    assert_eq!(run_turn(&o, &sid, "go").await, RunStatus::Completed);
    col.wait_for("run.completed", |e| {
        e.iter().any(|x| x.event_type == "run.completed")
    })
    .await;
    assert_eq!(col.of("tool.progress").len(), 1, "burst bị throttle");
}

#[tokio::test]
async fn st14_two_tool_calls_persist_first_then_dispatch_sequentially() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_tool_calls(
        "",
        vec![("c1", "probe", probe_args()), ("c2", "probe", probe_args())],
    );
    gw.push_text("done");
    let (probe, runs) = Probe::new("probe", ToolEffect::ReadOnly, Behavior::Echo);
    let (o, sid) = single(&env, &gw, Some(probe)).await;
    let col = collect(o.subscribe(&alice(), &sid).await.unwrap());

    assert_eq!(run_turn(&o, &sid, "go").await, RunStatus::Completed);
    col.wait_for("run.completed", |e| {
        e.iter().any(|x| x.event_type == "run.completed")
    })
    .await;
    assert_eq!(runs.load(Ordering::SeqCst), 2);

    let events = col.events();
    let call_of = |e: &ExecutionEvent| e.payload["tool_call_id"].as_str().map(str::to_owned);
    let s1 = position(&events, |e| {
        e.event_type == "tool.started" && call_of(e).as_deref() == Some("c1")
    });
    let s2 = position(&events, |e| {
        e.event_type == "tool.started" && call_of(e).as_deref() == Some("c2")
    });
    let d1 = position(&events, |e| {
        e.event_type == "tool.completed" && call_of(e).as_deref() == Some("c1")
    });
    let d2 = position(&events, |e| {
        e.event_type == "tool.completed" && call_of(e).as_deref() == Some("c2")
    });
    assert!(
        s1 < s2 && s2 < d1 && d1 < d2,
        "persist cả hai trước, rồi tuần tự"
    );
    let second_step = events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.event_type == "step.started")
        .nth(1)
        .unwrap()
        .0;
    assert!(second_step > d2, "step kế tiếp chỉ sau mọi outcome");
}

// ───────────────────────────── AT-25 ─────────────────────────────

#[tokio::test]
async fn at25_dropping_the_subscription_does_not_cancel_the_run() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_delayed(
        Duration::from_millis(300),
        ModelResponse {
            text: "finished anyway".into(),
            finish_reason: "end_turn".into(),
            tool_calls: vec![],
            usage: None,
        },
    );
    let (o, sid) = single(&env, &gw, None).await;
    let mut sub = o.subscribe(&alice(), &sid).await.unwrap();
    let turn = o.submit(&alice(), &sid, "go".into()).await.unwrap();
    // 202 đã trả (submit xong) trong khi run còn chạy.
    assert_eq!(
        o.result(&alice(), &sid, &turn.turn_id)
            .await
            .unwrap()
            .status,
        RunStatus::Running
    );
    let first = sub.recv().await;
    assert!(matches!(first, Some(FeedItem::Event(_))));
    drop(sub); // client disconnect

    assert_eq!(
        wait_terminal(&o, &sid, &turn.turn_id).await,
        RunStatus::Completed
    );
    let r = o.result(&alice(), &sid, &turn.turn_id).await.unwrap();
    assert_eq!(r.output.as_deref(), Some("finished anyway"));
}

// ───────────────────────────── AT-29 / ST-04 / AT-31 ─────────────────────────────

#[tokio::test]
async fn at29_st04_child_events_reach_the_family_before_the_parent_tool_completes() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_response(call(
        "c1",
        "subagent",
        json!({ "agent": "reviewer", "description": "d", "prompt": "review" }),
    ));
    gw.push_delayed(
        Duration::from_millis(300),
        ModelResponse {
            text: "child verdict".into(),
            finish_reason: "end_turn".into(),
            tool_calls: vec![],
            usage: None,
        },
    );
    gw.push_text("root done");
    let (o, sid) = family(&env, &gw, None).await;
    let col = collect(o.subscribe(&alice(), &sid).await.unwrap());
    let turn = o.submit(&alice(), &sid, "go".into()).await.unwrap();

    // Child đang chạy: event của child đã lên family trong khi parent tool chưa completed.
    col.wait_for("child run.started", |e| {
        e.iter()
            .any(|x| x.event_type == "run.started" && x.session_id != sid)
    })
    .await;
    assert!(
        col.of("tool.completed").is_empty(),
        "parent tool chưa được completed khi child còn chạy"
    );
    assert_eq!(
        o.result(&alice(), &sid, &turn.turn_id)
            .await
            .unwrap()
            .status,
        RunStatus::Running
    );
    assert_eq!(
        wait_terminal(&o, &sid, &turn.turn_id).await,
        RunStatus::Completed
    );
    col.wait_for("root run.completed", |e| {
        e.iter()
            .any(|x| x.event_type == "run.completed" && x.session_id == sid)
    })
    .await;

    let events = col.events();
    assert!(events.iter().all(|e| e.root_session_id == sid));
    let child = events
        .iter()
        .find(|e| e.event_type == "run.started" && e.session_id != sid)
        .unwrap()
        .session_id
        .clone();
    let parent_started = position(&events, |e| {
        e.event_type == "tool.started" && e.session_id == sid
    });
    let changed = position(&events, |e| {
        e.event_type == "session.changed" && e.session_id == sid
    });
    assert_eq!(events[changed].payload["reason"], json!("child_created"));
    assert_eq!(events[changed].payload["child_session_id"], json!(child));
    assert!(events[changed].state_revision.is_some());
    let child_start = position(&events, |e| {
        e.event_type == "run.started" && e.session_id == child
    });
    let child_done = position(&events, |e| {
        e.event_type == "run.completed" && e.session_id == child
    });
    let parent_done = position(&events, |e| {
        e.event_type == "tool.completed" && e.session_id == sid
    });
    assert!(parent_started < changed && changed < child_start);
    assert!(child_start < child_done && child_done < parent_done);

    // Snapshot family: child được khám phá, có parent link và trạng thái terminal.
    let snap = o.snapshot(&alice(), &sid).await.unwrap();
    assert_eq!(snap.children.len(), 1);
    assert_eq!(snap.children[0].session_id, child);
    assert_eq!(
        snap.children[0].parent_session_id.as_deref(),
        Some(sid.as_str())
    );
    assert_eq!(
        snap.children[0].latest_run.as_ref().unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(
        snap.root.invocations[0].child_session_id.as_deref(),
        Some(child.as_str())
    );
    assert!(!snap.has_active_work());
}

// ───────────────────────────── AT-30 / ST-06 ─────────────────────────────

#[tokio::test]
async fn at30_st06_a_lagged_reader_resyncs_alone_and_the_runtime_is_not_blocked() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let mut cfg = env.config(vec![env.root(&[])]);
    cfg.runtime.event_capacity = 32;
    let o = build(cfg, &gw, vec![], None).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();

    let mut slow = o.subscribe(&alice(), &sid).await.unwrap();
    let fast = collect(o.subscribe(&alice(), &sid).await.unwrap());
    for turn in 0..6 {
        let frag = "x".repeat(30);
        gw.push_stream_text(&[frag.as_str(); 10]);
        assert_eq!(
            run_turn(&o, &sid, &format!("turn {turn}")).await,
            RunStatus::Completed,
            "runtime không bị reader chậm chặn"
        );
        tokio::time::sleep(Duration::from_millis(30)).await; // fast reader drain
    }
    fast.wait_for("6 completions", |e| {
        e.iter().filter(|x| x.event_type == "run.completed").count() == 6
    })
    .await;
    assert!(
        !fast
            .items
            .lock()
            .unwrap()
            .iter()
            .any(|i| matches!(i, FeedItem::ResyncRequired)),
        "reader nhanh không bị ảnh hưởng"
    );

    // Reader chậm: bị resync rồi đóng riêng.
    assert_eq!(slow.recv().await, Some(FeedItem::ResyncRequired));
    assert_eq!(slow.recv().await, None);
    // Subscribe lại → vẫn nhận event bình thường.
    let mut again = o.subscribe(&alice(), &sid).await.unwrap();
    gw.push_text("after resync");
    let turn = o.submit(&alice(), &sid, "again".into()).await.unwrap();
    assert!(matches!(again.recv().await, Some(FeedItem::Event(_))));
    assert_eq!(
        wait_terminal(&o, &sid, &turn.turn_id).await,
        RunStatus::Completed
    );
}

// ───────────────────────────── AT-32 / ST-09 / ST-11 ─────────────────────────────

#[tokio::test]
async fn at32_st09_other_owners_and_child_ids_are_denied_and_families_are_isolated() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_response(call(
        "c1",
        "subagent",
        json!({ "agent": "reviewer", "description": "d", "prompt": "review" }),
    ));
    gw.push_text("child");
    gw.push_text("root");
    let (o, sid) = family(&env, &gw, None).await;
    let bob = CallerContext::new("bob", "ws1");
    let other_ws = CallerContext::new("alice", "ws2");

    for stranger in [&bob, &other_ws] {
        let e = o.subscribe(stranger, &sid).await.unwrap_err();
        assert_eq!(e.code(), ErrorCode::PermissionDenied);
        let e = o.snapshot(stranger, &sid).await.unwrap_err();
        assert_eq!(e.code(), ErrorCode::PermissionDenied);
    }
    assert_eq!(
        o.subscribe(&alice(), "nope").await.unwrap_err().code(),
        ErrorCode::NotFound
    );

    // Family khác của cùng owner không rò event sang subscriber này.
    let other = o.create_session(&alice(), "root").await.unwrap();
    let mut spy = o.subscribe(&alice(), &other).await.unwrap();
    assert_eq!(run_turn(&o, &sid, "go").await, RunStatus::Completed);

    // Child do chính family này tạo: nằm trong snapshot, nhưng không thể subscribe/snapshot
    // trực tiếp theo child id và người lạ không thấy gì dù biết child id.
    let snap = o.snapshot(&alice(), &sid).await.unwrap();
    let child = snap.children[0].session_id.clone();
    assert_eq!(
        o.subscribe(&alice(), &child).await.unwrap_err().code(),
        ErrorCode::InvalidInput
    );
    assert_eq!(
        o.snapshot(&alice(), &child).await.unwrap_err().code(),
        ErrorCode::InvalidInput
    );
    assert_eq!(
        o.subscribe(&bob, &child).await.unwrap_err().code(),
        ErrorCode::PermissionDenied
    );
    assert_eq!(
        o.snapshot(&bob, &child).await.unwrap_err().code(),
        ErrorCode::PermissionDenied
    );
    let leaked = tokio::time::timeout(Duration::from_millis(150), spy.recv()).await;
    assert!(leaked.is_err(), "family khác không nhận event: {leaked:?}");
}

#[tokio::test]
async fn st11_configured_secrets_never_reach_events_even_when_split_across_deltas() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_response(call("c1", "probe", probe_args()));
    // Secret bị cắt qua nhiều fragment trong text của model.
    gw.push_stream_text(&["the key is sk-te", "st-not", "-real; bye"]);
    let (probe, _) = Probe::new("probe", ToolEffect::ReadOnly, Behavior::LeakProgress);
    let (o, sid) = single(&env, &gw, Some(probe)).await;
    let col = collect(o.subscribe(&alice(), &sid).await.unwrap());

    assert_eq!(run_turn(&o, &sid, "go").await, RunStatus::Completed);
    col.wait_for("run.completed", |e| {
        e.iter().any(|x| x.event_type == "run.completed")
    })
    .await;

    let events = col.events();
    let wire: String = events.iter().map(|e| e.to_json().to_string()).collect();
    assert!(!wire.contains(API_KEY), "secret lọt vào event: {wire}");
    assert!(!wire.contains("sk-te"), "mảnh secret lọt vào event");
    assert_eq!(deltas_of(&events, &sid), "the key is [redacted]; bye");
    let progress = events
        .iter()
        .find(|e| e.event_type == "tool.progress")
        .unwrap();
    assert_eq!(
        progress.payload["message"],
        json!("auth header [redacted] sent")
    );
    // Snapshot không mang arguments/kết quả/progress của tool.
    let snap = o.snapshot(&alice(), &sid).await.unwrap();
    assert!(!format!("{:?}", snap.root.invocations).contains("auth header"));
}

// ───────────────────────────── AT-33 / ST-07 ─────────────────────────────

#[tokio::test]
async fn at33_st07_failed_tool_result_commit_publishes_no_confirmed_completion() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_response(call("c1", "probe", probe_args()));
    gw.push_text("never reached");
    let (probe, runs) = Probe::new("probe", ToolEffect::Mutating, Behavior::Echo);
    let (o, sid) = single(&env, &gw, Some(probe)).await;
    let mut c = env.sql().await;
    sqlx::query(
        "CREATE TRIGGER block_tool_msg BEFORE INSERT ON messages WHEN NEW.role = 'tool' \
         BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(&mut c)
    .await
    .unwrap();
    let col = collect(o.subscribe(&alice(), &sid).await.unwrap());

    assert_eq!(run_turn(&o, &sid, "go").await, RunStatus::Interrupted);
    col.wait_for("run.interrupted", |e| {
        e.iter().any(|x| x.event_type == "run.interrupted")
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await; // vài chu kỳ sweep

    let types = col.types();
    assert!(
        !types
            .iter()
            .any(|t| t == "tool.completed" || t == "run.completed"),
        "không có completion giả: {types:?}"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1, "không replay");
    let interrupted = col.of("tool.interrupted");
    assert_eq!(interrupted.len(), 1);
    assert_eq!(
        interrupted[0].payload["error_code"],
        json!("TOOL_OUTCOME_UNKNOWN")
    );
    let snap = o.snapshot(&alice(), &sid).await.unwrap();
    assert_eq!(
        snap.root.invocations[0].status,
        InvocationStatus::Interrupted
    );
}

#[tokio::test]
async fn at33_failed_final_commit_publishes_run_failed_not_run_completed() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_text("final text");
    let (o, sid) = single(&env, &gw, None).await;
    let mut c = env.sql().await;
    sqlx::query(
        "CREATE TRIGGER block_complete BEFORE UPDATE ON agent_runs WHEN NEW.status = 'completed' \
         BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(&mut c)
    .await
    .unwrap();
    let col = collect(o.subscribe(&alice(), &sid).await.unwrap());

    assert_eq!(run_turn(&o, &sid, "go").await, RunStatus::Failed);
    col.wait_for("run.failed", |e| {
        e.iter().any(|x| x.event_type == "run.failed")
    })
    .await;
    let types = col.types();
    assert!(!types.iter().any(|t| t == "run.completed"), "{types:?}");
    assert!(
        col.of("message.committed").is_empty(),
        "final message không commit nên không được báo committed"
    );
}

// ───────────────────────────── AT-34 / ST-08 ─────────────────────────────

#[tokio::test]
async fn at34_st08_unknown_mutating_outcome_is_visible_as_interrupted_and_never_replayed() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_response(call("c1", "probe", probe_args()));
    gw.push_text("never");
    let (probe, runs) = Probe::new(
        "probe",
        ToolEffect::Mutating,
        Behavior::Sleep(Duration::from_secs(5)),
    );
    let mut cfg = env.config(vec![env.root(&["probe"])]);
    cfg.runtime.tool_timeout = Duration::from_millis(100);
    let o = build(cfg.clone(), &gw, vec![probe.clone()], None).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let col = collect(o.subscribe(&alice(), &sid).await.unwrap());

    assert_eq!(run_turn(&o, &sid, "go").await, RunStatus::Interrupted);
    col.wait_for("run.interrupted", |e| {
        e.iter().any(|x| x.event_type == "run.interrupted")
    })
    .await;
    let types = col.types();
    assert!(
        !types
            .iter()
            .any(|t| t == "tool.failed" || t == "tool.completed"),
        "{types:?}"
    );
    let tool_pos = types.iter().position(|t| t == "tool.interrupted").unwrap();
    let run_pos = types.iter().position(|t| t == "run.interrupted").unwrap();
    assert!(tool_pos < run_pos);

    // Snapshot hiển thị unresolved, kể cả sau restart (recovery bị chặn, không replay).
    o.shutdown().await.unwrap();
    drop(o);
    let gw2 = ScriptedGateway::new();
    let o2 = build(cfg, &gw2, vec![probe], None).await;
    let snap = o2.snapshot(&alice(), &sid).await.unwrap();
    let run = snap.root.latest_run.as_ref().unwrap();
    assert_eq!(run.status, RunStatus::Interrupted);
    assert_eq!(
        snap.root.invocations[0].status,
        InvocationStatus::Interrupted
    );
    assert!(!snap.has_active_work());
    assert_eq!(runs.load(Ordering::SeqCst), 1, "không replay sau restart");
}

// ───────────────────────────── snapshot ─────────────────────────────

#[tokio::test]
async fn snapshot_is_canonical_bounded_and_tracks_revisions() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let mut cfg = env.config(vec![env.root(&[])]);
    cfg.runtime.page_size = 3;
    let o = build(cfg, &gw, vec![], None).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();

    let empty = o.snapshot(&alice(), &sid).await.unwrap();
    assert!(empty.root.latest_run.is_none() && empty.children.is_empty());
    assert!(!empty.has_active_work());

    let mut last = empty.root.state_revision;
    for i in 0..3 {
        assert_eq!(
            run_turn(&o, &sid, &format!("m{i}")).await,
            RunStatus::Completed
        );
        let snap = o.snapshot(&alice(), &sid).await.unwrap();
        assert!(
            snap.root.state_revision > last,
            "revision tăng theo transaction"
        );
        last = snap.root.state_revision;
    }
    let snap = o.snapshot(&alice(), &sid).await.unwrap();
    // 6 message canonical nhưng snapshot chỉ mang 3 gần nhất, đúng thứ tự.
    assert_eq!(snap.root.recent_messages.len(), 3);
    let seq: Vec<u64> = snap
        .root
        .recent_messages
        .iter()
        .map(|m| m.sequence)
        .collect();
    assert!(seq.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(snap.root.recent_messages[2].text, "echo: m2");
    // Lịch sử đầy đủ vẫn đọc bằng phân trang.
    let all = o
        .history(&alice(), &sid, PageRequest::first(100))
        .await
        .unwrap();
    assert!(all.items.len() == 3 || all.next_cursor.is_some());
}

#[tokio::test]
async fn snapshot_reports_active_work_while_running() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.hold();
    let (o, sid) = single(&env, &gw, None).await;
    let turn = o.submit(&alice(), &sid, "go".into()).await.unwrap();
    let snap = o.snapshot(&alice(), &sid).await.unwrap();
    assert!(snap.has_active_work());
    assert_eq!(
        snap.root.latest_run.as_ref().unwrap().status,
        RunStatus::Running
    );
    gw.release();
    wait_terminal(&o, &sid, &turn.turn_id).await;
    assert!(!o.snapshot(&alice(), &sid).await.unwrap().has_active_work());
}

// ───────────────────── AT-31 / ST-10 / AT-37 / ST-15 / AT-45 / ST-16 ─────────────────────

fn child_family_script(gw: &Arc<ScriptedGateway>) {
    gw.push_response(call(
        "c1",
        "subagent",
        json!({ "agent": "reviewer", "description": "d", "prompt": "review" }),
    ));
    gw.push_delayed(
        Duration::from_millis(100),
        ModelResponse {
            text: "child verdict".into(),
            finish_reason: "end_turn".into(),
            tool_calls: vec![],
            usage: None,
        },
    );
    gw.push_text("root done");
}

#[tokio::test]
async fn at31_st05_st10_client_converges_across_child_creation_and_reconnect() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    child_family_script(&gw);
    let (o, sid) = family(&env, &gw, None).await;
    let obs = spawn_observer(o.clone(), sid.clone(), Duration::from_secs(30));
    obs.eventually("first snapshot", Duration::from_secs(5), |v| {
        v.snapshot.is_some()
    })
    .await;

    let turn = o.submit(&alice(), &sid, "go".into()).await.unwrap();
    obs.eventually("child discovered", Duration::from_secs(10), |v| {
        v.snapshot.as_ref().is_some_and(|s| s.children.len() == 1)
    })
    .await;
    // Không refresh định kỳ (30s): hội tụ hoàn toàn nhờ invalidation từ committed events.
    obs.eventually("converged", Duration::from_secs(10), |v| {
        root_completed(v)
            && v.snapshot.as_ref().is_some_and(|s| {
                matches!(&s.children[0].latest_run, Some(r) if r.status == RunStatus::Completed)
            })
    })
    .await;
    wait_terminal(&o, &sid, &turn.turn_id).await;
    let v = obs.view.lock().unwrap();
    assert_eq!(v.regressions, 0, "state không regress");
    assert_eq!(v.subscriptions, 1);
    assert!(v.previews.is_empty(), "canonical terminal thay preview");
}

#[tokio::test]
async fn st05_a_reconnecting_client_recovers_terminal_state_from_snapshot() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_delayed(
        Duration::from_millis(300),
        ModelResponse {
            text: "done while away".into(),
            finish_reason: "end_turn".into(),
            tool_calls: vec![],
            usage: None,
        },
    );
    let (o, sid) = single(&env, &gw, None).await;
    let turn = o.submit(&alice(), &sid, "go".into()).await.unwrap();
    // Client 1 mở rồi mất kết nối giữa chừng.
    let sub = o.subscribe(&alice(), &sid).await.unwrap();
    drop(sub);
    assert_eq!(
        wait_terminal(&o, &sid, &turn.turn_id).await,
        RunStatus::Completed
    );
    // Client 2 kết nối sau khi run đã kết thúc: subscribe rồi snapshot thấy terminal state.
    let obs = spawn_observer(o.clone(), sid.clone(), Duration::from_secs(30));
    obs.eventually(
        "terminal from snapshot",
        Duration::from_secs(5),
        root_completed,
    )
    .await;
}

#[tokio::test]
async fn at37_st15_out_of_order_publication_never_loses_or_regresses_canonical_state() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    child_family_script(&gw);
    let fault = FeedFault::new();
    fault.reverse_pairs(true);
    let (o, sid) = family(&env, &gw, Some(fault.clone())).await;
    // Refresh ngắn: event lẻ cuối bị giữ lại bởi fault, snapshot định kỳ bù phần đó.
    let obs = spawn_observer(o.clone(), sid.clone(), Duration::from_millis(150));
    obs.eventually("first snapshot", Duration::from_secs(5), |v| {
        v.snapshot.is_some()
    })
    .await;

    o.submit(&alice(), &sid, "go".into()).await.unwrap();
    obs.eventually("converged", Duration::from_secs(15), |v| {
        root_completed(v) && v.snapshot.as_ref().is_some_and(|s| {
            s.children.len() == 1
                && matches!(&s.children[0].latest_run, Some(r) if r.status == RunStatus::Completed)
        })
    })
    .await;
    let v = obs.view.lock().unwrap();
    assert!(v.inversions > 0, "fault phải thật sự đảo thứ tự publish");
    assert_eq!(v.regressions, 0, "snapshot không bao giờ lùi revision");
    assert_eq!(v.subscriptions, 1, "không cần reconnect");
}

#[tokio::test]
async fn at45_st16_periodic_snapshot_converges_when_terminal_publication_is_lost() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_response(call("c1", "probe", probe_args()));
    gw.push_text("done");
    let (probe, _) = Probe::new("probe", ToolEffect::ReadOnly, Behavior::Echo);
    let fault = FeedFault::new();
    fault.suppress_terminal(true);
    let o = build(
        env.config(vec![env.root(&["probe"])]),
        &gw,
        vec![probe],
        Some(fault),
    )
    .await;
    let sid = o.create_session(&alice(), "root").await.unwrap();

    // Đối chứng: không refresh định kỳ → view kẹt ở trạng thái active dù SSE còn sống.
    let stuck = spawn_observer(o.clone(), sid.clone(), Duration::from_secs(3600));
    stuck
        .eventually("first snapshot", Duration::from_secs(5), |v| {
            v.snapshot.is_some()
        })
        .await;
    // Có refresh định kỳ ngắn → hội tụ mà không reconnect.
    let periodic = spawn_observer(o.clone(), sid.clone(), Duration::from_millis(100));
    periodic
        .eventually("first snapshot", Duration::from_secs(5), |v| {
            v.snapshot.is_some()
        })
        .await;

    let turn = o.submit(&alice(), &sid, "go".into()).await.unwrap();
    assert_eq!(
        wait_terminal(&o, &sid, &turn.turn_id).await,
        RunStatus::Completed
    );

    periodic
        .eventually(
            "periodic convergence",
            Duration::from_secs(5),
            root_completed,
        )
        .await;
    {
        let v = periodic.view.lock().unwrap();
        assert_eq!(v.terminal_events, 0, "terminal publication thật sự bị mất");
        assert_eq!(v.subscriptions, 1, "connection vẫn sống, không reconnect");
        assert_eq!(v.resyncs, 0);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !root_completed(&stuck.view.lock().unwrap()),
        "không có refresh định kỳ thì invalidation đã mất không tự hội tụ"
    );
}

// ───────────────────────────── lifecycle / config ─────────────────────────────

#[tokio::test]
async fn shutdown_ends_subscriptions_and_rejects_new_observation() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let (o, sid) = single(&env, &gw, None).await;
    let mut sub = o.subscribe(&alice(), &sid).await.unwrap();
    o.shutdown().await.unwrap();
    assert_eq!(sub.recv().await, None);
    assert_eq!(
        o.subscribe(&alice(), &sid).await.unwrap_err().code(),
        ErrorCode::ShuttingDown
    );
    assert_eq!(
        o.snapshot(&alice(), &sid).await.unwrap_err().code(),
        ErrorCode::ShuttingDown
    );
}

#[tokio::test]
async fn archiving_and_cancelling_publish_committed_invalidations() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.hold();
    let (o, sid) = single(&env, &gw, None).await;
    let col = collect(o.subscribe(&alice(), &sid).await.unwrap());
    let turn = o.submit(&alice(), &sid, "go".into()).await.unwrap();
    col.wait_for("run.started", |e| {
        e.iter().any(|x| x.event_type == "run.started")
    })
    .await;
    o.cancel(&alice(), &sid).await.unwrap();
    assert_eq!(
        wait_terminal(&o, &sid, &turn.turn_id).await,
        RunStatus::Cancelled
    );
    col.wait_for("run.cancelled", |e| {
        e.iter().any(|x| x.event_type == "run.cancelled")
    })
    .await;
    o.archive_session(&alice(), &sid).await.unwrap();
    col.wait_for("archived", |e| {
        e.iter()
            .any(|x| x.event_type == "session.changed" && x.payload["reason"] == json!("archived"))
    })
    .await;
    let events = col.events();
    let revs: Vec<i64> = events.iter().filter_map(|e| e.state_revision).collect();
    assert!(revs.windows(2).all(|w| w[0] <= w[1]), "{revs:?}");
    assert!(o.snapshot(&alice(), &sid).await.unwrap().root.archived);
}

#[tokio::test]
async fn invalid_observation_limits_are_rejected_at_build() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    for edit in [
        (|c: &mut OrchestratorConfig| c.runtime.event_capacity = 1) as fn(&mut OrchestratorConfig),
        |c| c.runtime.event_capacity = 1_000_000,
        |c| c.runtime.event_delta_max_bytes = 8,
        |c| c.runtime.snapshot_refresh = Duration::ZERO,
    ] {
        let mut cfg = env.config(vec![env.root(&[])]);
        edit(&mut cfg);
        let err = Orchestrator::builder(cfg)
            .with_gateway(gw.clone())
            .build()
            .await
            .err()
            .expect("config must be rejected");
        assert_eq!(err.reason(), Some(ConfigReason::InvalidRuntimeConfig));
    }
    let o = build(env.config(vec![env.root(&[])]), &gw, vec![], None).await;
    assert_eq!(o.snapshot_refresh_interval(), Duration::from_secs(30));
}

#[tokio::test]
async fn large_text_fragments_are_split_without_losing_order_or_text() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let big = "é".repeat(500); // 1000 byte
    gw.push_stream_text(&[big.as_str(), "tail"]);
    let mut cfg = env.config(vec![env.root(&[])]);
    cfg.runtime.event_delta_max_bytes = 64;
    let o = build(cfg, &gw, vec![], None).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let col = collect(o.subscribe(&alice(), &sid).await.unwrap());
    assert_eq!(run_turn(&o, &sid, "go").await, RunStatus::Completed);
    col.wait_for("run.completed", |e| {
        e.iter().any(|x| x.event_type == "run.completed")
    })
    .await;
    let events = col.events();
    let deltas: Vec<&ExecutionEvent> = events
        .iter()
        .filter(|e| e.event_type == "text.delta")
        .collect();
    assert!(deltas.len() > 2);
    assert!(deltas
        .iter()
        .all(|e| e.payload["delta"].as_str().unwrap().len() <= 64));
    for (i, e) in deltas.iter().enumerate() {
        assert_eq!(e.payload["delta_index"], json!(i));
    }
    assert_eq!(deltas_of(&events, &sid), format!("{big}tail"));
}

#[tokio::test]
async fn at27_st02_tool_runs_only_after_the_response_finished_and_calls_are_persisted() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    // Gateway báo call đã ghép đủ (arguments từng mảnh đã được assemble) trước `Finished`.
    gw.push_response_streamed(
        vec![StreamEvent::ToolCallReady {
            id: "c1".into(),
            name: "probe".into(),
        }],
        call("c1", "probe", probe_args()),
    );
    gw.push_text("done");
    let (probe, runs) = Probe::new("probe", ToolEffect::Mutating, Behavior::Echo);
    let (o, sid) = single(&env, &gw, Some(probe)).await;
    let col = collect(o.subscribe(&alice(), &sid).await.unwrap());
    assert_eq!(run_turn(&o, &sid, "go").await, RunStatus::Completed);
    col.wait_for("run.completed", |e| {
        e.iter().any(|x| x.event_type == "run.completed")
    })
    .await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let events = col.events();
    let ended = position(&events, |e| e.event_type == "step.ended");
    let committed = position(&events, |e| e.event_type == "message.committed");
    let started = position(&events, |e| e.event_type == "tool.started");
    let done = position(&events, |e| e.event_type == "tool.completed");
    // Finished → persist assistant+invocation → mới dispatch tool.
    assert!(ended < committed && committed <= started && started < done);
}
