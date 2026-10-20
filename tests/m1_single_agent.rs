//! M1 gate (fake LLM + SQLite thật): AT-01/18/42/46/50 mức cơ bản, crash-window scheduling
//! trong live process, concurrent submit chỉ một claim, history cursor bind scope,
//! unauthorized reads denied, close/reopen đọc được dữ liệu.
//!
//! Mode: deterministic fake gateway (`test-support`) — KHÔNG phải evidence provider live.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::test_support::{GatewayError, ModelRole, ScriptedGateway};
use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, CallerContext, ErrorCode, LlmBackend, LlmConfig,
    LlmProviderConfig, MessageRole, Orchestrator, OrchestratorConfig, PageRequest, ProviderAuth,
    RunStatus, SecretString,
};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};

const BASE: &str = include_str!("../src/base_instructions.md");
const AGENT_PROMPT: &str = "You are the root test agent.";

struct Env {
    dir: tempfile::TempDir,
}

impl Env {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("root.md"), AGENT_PROMPT).unwrap();
        std::fs::write(dir.path().join("sub.md"), "sub").unwrap();
        Self { dir }
    }

    fn store(&self) -> PathBuf {
        self.dir.path().join("store")
    }

    fn config(&self) -> OrchestratorConfig {
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
            vec![
                AgentDefinition::new("root", "fake", "model-x")
                    .with_mode(AgentMode::Primary)
                    .with_prompt_path(self.dir.path().join("root.md")),
                AgentDefinition::new("sub", "fake", "model-x")
                    .with_mode(AgentMode::Subagent)
                    .with_prompt_path(self.dir.path().join("sub.md")),
            ],
            self.store(),
        );
        cfg.runtime.supervisor_sweep = Duration::from_millis(50);
        cfg.runtime.shutdown_grace = Duration::from_secs(5);
        cfg.runtime.page_size = 3;
        cfg
    }

    async fn build(&self, gw: &Arc<ScriptedGateway>) -> Orchestrator {
        Orchestrator::builder(self.config())
            .with_gateway(gw.clone())
            .build()
            .await
            .unwrap()
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

async fn wait_terminal(o: &Orchestrator, c: &CallerContext, s: &str, t: &str) -> AgentResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let r = o.result(c, s, t).await.unwrap();
        if r.status != RunStatus::Running {
            return r;
        }
        assert!(tokio::time::Instant::now() < deadline, "run stuck: {r:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_until(mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "condition not met");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn history_texts(o: &Orchestrator, c: &CallerContext, s: &str) -> Vec<(MessageRole, String)> {
    let mut out = Vec::new();
    let mut cursor = None;
    loop {
        let page = o
            .history(
                c,
                s,
                PageRequest {
                    cursor: cursor.clone(),
                    limit: 100,
                },
            )
            .await
            .unwrap();
        out.extend(page.items.into_iter().map(|m| (m.role, m.text)));
        match page.next_cursor {
            Some(n) => cursor = Some(n),
            None => return out,
        }
    }
}

#[tokio::test]
async fn at01_text_chat_persists_and_survives_close_reopen() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_text("Xin chào!");
    let o = env.build(&gw).await;

    let sid = o.create_session(&alice(), "root").await.unwrap();
    let turn = o.submit(&alice(), &sid, "hello".into()).await.unwrap();
    assert_eq!(turn.session_id, sid);
    let r = wait_terminal(&o, &alice(), &sid, &turn.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(r.output.as_deref(), Some("Xin chào!"));
    assert_eq!(r.attempt, 0);
    assert!(r.final_message_id.is_some() && r.error.is_none());

    let hist = history_texts(&o, &alice(), &sid).await;
    assert_eq!(
        hist,
        vec![
            (MessageRole::User, "hello".to_string()),
            (MessageRole::Assistant, "Xin chào!".to_string())
        ]
    );
    // Final pointer trỏ đúng assistant message của chính run.
    let page = o
        .history(&alice(), &sid, PageRequest::first(10))
        .await
        .unwrap();
    assert_eq!(page.items[1].id, r.final_message_id.clone().unwrap());
    assert_eq!(page.items[1].run_id.as_deref(), Some(r.run_id.as_str()));
    assert_eq!(page.items[0].run_id, None);

    o.shutdown().await.unwrap();
    drop(o);

    // Close/reopen: dữ liệu đọc lại được từ SQLite thật; không có run nào bị chạy lại.
    let gw2 = ScriptedGateway::new();
    let o2 = env.build(&gw2).await;
    let r2 = o2.result(&alice(), &sid, &turn.turn_id).await.unwrap();
    assert_eq!(r2, r);
    assert_eq!(history_texts(&o2, &alice(), &sid).await, hist);
    assert_eq!(gw2.call_count(), 0);
    o2.shutdown().await.unwrap();
}

#[tokio::test]
async fn multi_turn_history_reaches_the_model_and_results_are_per_turn() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_text("first");
    gw.push_text("second");
    let o = env.build(&gw).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();

    let t1 = o.submit(&alice(), &sid, "one".into()).await.unwrap();
    let r1 = wait_terminal(&o, &alice(), &sid, &t1.turn_id).await;
    let t2 = o.submit(&alice(), &sid, "two".into()).await.unwrap();
    let r2 = wait_terminal(&o, &alice(), &sid, &t2.turn_id).await;

    assert_ne!(t1.turn_id, t2.turn_id);
    // Lineage: mỗi turn trỏ đúng final của mình, không phải assistant cuối của session.
    assert_eq!(r1.output.as_deref(), Some("first"));
    assert_eq!(r2.output.as_deref(), Some("second"));
    let reqs = gw.requests();
    let seen: Vec<(ModelRole, &str)> = reqs[1]
        .messages
        .iter()
        .map(|m| (m.role, m.text.as_str()))
        .collect();
    assert_eq!(
        seen,
        vec![
            (ModelRole::User, "one"),
            (ModelRole::Assistant, "first"),
            (ModelRole::User, "two")
        ]
    );
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn at18_archived_session_rejects_submit_but_history_stays_readable() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let o = env.build(&gw).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let t = o.submit(&alice(), &sid, "hi".into()).await.unwrap();
    wait_terminal(&o, &alice(), &sid, &t.turn_id).await;

    o.archive_session(&alice(), &sid).await.unwrap();
    o.archive_session(&alice(), &sid).await.unwrap(); // idempotent
    let err = o.submit(&alice(), &sid, "again".into()).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::SessionArchived);
    assert_eq!(err.code().as_str(), "SESSION_ARCHIVED");

    assert_eq!(history_texts(&o, &alice(), &sid).await.len(), 2);
    assert_eq!(
        gw.call_count(),
        1,
        "rejected submit must not reach the model"
    );
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn at42_latest_attempt_owns_the_result_and_final_pointer() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_text("attempt-0 final");
    let o = env.build(&gw).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let t = o.submit(&alice(), &sid, "go".into()).await.unwrap();
    let first = wait_terminal(&o, &alice(), &sid, &t.turn_id).await;
    assert_eq!(first.output.as_deref(), Some("attempt-0 final"));

    // Mô phỏng attempt mới của cùng turn (recovery thật đến ở M5): giữ gateway để attempt
    // 1 đang chạy trong khi attempt 0 đã có assistant final.
    gw.hold();
    gw.push_text("attempt-1 final");
    let mut conn = env.sql().await;
    sqlx::query(
        "INSERT INTO agent_runs(id, session_id, agent_id, turn_id, resume_attempt, provider_id, \
         model_id, status) VALUES('run-attempt-1', ?1, 'root', ?2, 1, 'fake', 'model-x', 'running')",
    )
    .bind(&sid)
    .bind(&t.turn_id)
    .execute(&mut conn)
    .await
    .unwrap();
    wait_until(|| gw.call_count() == 2).await;

    let running = o.result(&alice(), &sid, &t.turn_id).await.unwrap();
    assert_eq!(running.run_id, "run-attempt-1");
    assert_eq!(running.attempt, 1);
    assert_eq!(running.status, RunStatus::Running);
    assert_eq!(running.output, None, "prior attempt's final must not leak");
    assert_eq!(running.final_message_id, None);

    gw.release();
    let last = wait_terminal(&o, &alice(), &sid, &t.turn_id).await;
    assert_eq!(last.run_id, "run-attempt-1");
    assert_eq!(last.output.as_deref(), Some("attempt-1 final"));
    assert_ne!(last.final_message_id, first.final_message_id);
    let owner_run: String = sqlx::query_scalar("SELECT run_id FROM messages WHERE id = ?1")
        .bind(last.final_message_id.as_deref().unwrap())
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(owner_run, "run-attempt-1");
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn at46_every_model_request_carries_base_then_agent_prompt() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let o = env.build(&gw).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    for text in ["a", "b", "c"] {
        let t = o.submit(&alice(), &sid, text.into()).await.unwrap();
        wait_terminal(&o, &alice(), &sid, &t.turn_id).await;
    }
    let reqs = gw.requests();
    assert_eq!(reqs.len(), 3);
    for r in &reqs {
        assert!(
            r.system.starts_with(BASE.trim_end()),
            "base must come first"
        );
        assert!(r.system.ends_with(AGENT_PROMPT));
        assert_eq!(
            (r.provider_id.as_str(), r.model_id.as_str()),
            ("fake", "model-x")
        );
    }
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn at50_second_instance_fails_until_owner_releases_safely() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let first = env.build(&gw).await;

    let err = Orchestrator::builder(env.config())
        .with_gateway(gw.clone())
        .build()
        .await
        .err()
        .expect("second build must fail while owner is held");
    assert_eq!(err.code(), ErrorCode::StoreAlreadyOwned);
    assert_eq!(err.code().as_str(), "STORE_ALREADY_OWNED");

    // Giữ run đang chạy: shutdown phải join worker rồi mới nhả lock.
    let sid = first.create_session(&alice(), "root").await.unwrap();
    gw.hold();
    let t = first.submit(&alice(), &sid, "slow".into()).await.unwrap();
    wait_until(|| gw.call_count() == 1).await;
    first.shutdown().await.unwrap();
    first.shutdown().await.unwrap(); // idempotent
    assert_eq!(
        first
            .submit(&alice(), &sid, "x".into())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::ShuttingDown
    );

    // M5: run bị shutdown ngắt (`interrupted`/SHUTTING_DOWN) được phục hồi an toàn ở lần khởi
    // động sau bằng attempt mới giữ `turn_id` — không kẹt session, không chạy lại tool nào.
    gw.release();
    let second = env.build(&gw).await;
    let r = wait_terminal(&second, &alice(), &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.attempt, 1, "the same turn resumes as a new attempt");
    let t2 = second.submit(&alice(), &sid, "next".into()).await.unwrap();
    assert_eq!(
        wait_terminal(&second, &alice(), &sid, &t2.turn_id)
            .await
            .status,
        RunStatus::Completed
    );
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn running_runs_left_by_a_dead_process_are_resumed_not_stuck() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let o = env.build(&gw).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let fingerprint = o.provider_fingerprint("fake").unwrap().to_owned();
    o.shutdown().await.unwrap();
    drop(o);

    // Giả lập process chết khi run còn `running` (không có shutdown).
    let mut conn = env.sql().await;
    sqlx::query(
        "INSERT INTO messages(id, session_id, sequence, role, content_json) \
         VALUES('m-stale', ?1, 1, 'user', '{\"text\":\"hello\"}')",
    )
    .bind(&sid)
    .execute(&mut conn)
    .await
    .unwrap();
    sqlx::query("UPDATE sessions SET next_sequence = 2 WHERE id = ?1")
        .bind(&sid)
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO agent_runs(id, session_id, agent_id, turn_id, provider_id, model_id, \
         provider_config_fingerprint, status) \
         VALUES('stale', ?1, 'root', 'turn-stale', 'fake', 'model-x', ?2, 'running')",
    )
    .bind(&sid)
    .bind(&fingerprint)
    .execute(&mut conn)
    .await
    .unwrap();

    // Không có tool dang dở và routing identity không đổi → attempt mới giữ `turn_id` (M5).
    let o2 = env.build(&gw).await;
    let r = wait_terminal(&o2, &alice(), &sid, "turn-stale").await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.attempt, 1);
    assert_eq!(r.output.as_deref(), Some("echo: hello"));
    assert_eq!(
        gw.call_count(),
        1,
        "one model request for the resumed attempt"
    );
    // Session không còn bị chặn bởi claim cũ.
    let t = o2.submit(&alice(), &sid, "ok".into()).await.unwrap();
    wait_terminal(&o2, &alice(), &sid, &t.turn_id).await;
    o2.shutdown().await.unwrap();
}

#[tokio::test]
async fn crash_window_claim_without_spawn_is_rescheduled_in_live_process() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_text("rescued");
    let drop_triggers = Arc::new(AtomicBool::new(true));
    // Sweep đủ dài để assertion "chưa có task" không đua với supervisor khi máy bận.
    let mut cfg = env.config();
    cfg.runtime.supervisor_sweep = Duration::from_secs(1);
    let o = Orchestrator::builder(cfg)
        .with_gateway(gw.clone())
        .with_dropped_admission_triggers(drop_triggers.clone())
        .build()
        .await
        .unwrap();
    let sid = o.create_session(&alice(), "root").await.unwrap();

    // Claim đã commit (Accepted) nhưng trigger sau admission bị bỏ: chưa có task.
    let t = o.submit(&alice(), &sid, "hello".into()).await.unwrap();
    let pending = o.result(&alice(), &sid, &t.turn_id).await.unwrap();
    assert_eq!(pending.status, RunStatus::Running);
    assert_eq!(gw.call_count(), 0);

    // Sweep định kỳ của supervisor phải nhặt claim hụt, chạy đúng một lần.
    let r = wait_terminal(&o, &alice(), &sid, &t.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(r.output.as_deref(), Some("rescued"));
    assert_eq!(gw.call_count(), 1);
    assert!(drop_triggers.load(Ordering::Acquire));
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn repeated_reconcile_never_spawns_a_duplicate_task() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.hold();
    let o = env.build(&gw).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let t = o.submit(&alice(), &sid, "slow".into()).await.unwrap();
    wait_until(|| gw.call_count() == 1).await;

    // Sweep 50ms chạy nhiều lần trong lúc run còn đang giữ.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(gw.call_count(), 1, "owned run must not be spawned twice");
    gw.release();
    wait_terminal(&o, &alice(), &sid, &t.turn_id).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(gw.call_count(), 1, "terminal run must not be rescheduled");
    o.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_submit_yields_exactly_one_claim() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.hold();
    let o = Arc::new(env.build(&gw).await);
    let sid = o.create_session(&alice(), "root").await.unwrap();

    let mut tasks = Vec::new();
    for i in 0..16 {
        let o = o.clone();
        let sid = sid.clone();
        tasks.push(tokio::spawn(async move {
            o.submit(&alice(), &sid, format!("msg-{i}")).await
        }));
    }
    let mut accepted = Vec::new();
    let mut busy = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(turn) => accepted.push(turn),
            Err(e) => {
                assert_eq!(e.code(), ErrorCode::SessionBusy, "{e}");
                busy += 1;
            }
        }
    }
    assert_eq!(accepted.len(), 1);
    assert_eq!(busy, 15);

    let mut conn = env.sql().await;
    let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_runs WHERE session_id = ?1")
        .bind(&sid)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let users: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND role = 'user'")
            .bind(&sid)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        (runs, users),
        (1, 1),
        "losers must leave no input or claim behind"
    );

    wait_until(|| gw.call_count() == 1).await;
    gw.release();
    wait_terminal(&o, &alice(), &sid, &accepted[0].turn_id).await;
    assert_eq!(gw.call_count(), 1);
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn history_cursor_is_bound_to_its_session_and_pages_are_stable() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let o = env.build(&gw).await;
    let a = o.create_session(&alice(), "root").await.unwrap();
    let b = o.create_session(&alice(), "root").await.unwrap();
    for i in 0..3 {
        let t = o.submit(&alice(), &a, format!("a{i}")).await.unwrap();
        wait_terminal(&o, &alice(), &a, &t.turn_id).await;
    }
    let t = o.submit(&alice(), &b, "b0".into()).await.unwrap();
    wait_terminal(&o, &alice(), &b, &t.turn_id).await;

    // page_size=3 → limit lớn bị clamp; 6 message → 2 trang.
    let p1 = o
        .history(&alice(), &a, PageRequest::first(1000))
        .await
        .unwrap();
    assert_eq!(p1.items.len(), 3);
    let cursor = p1.next_cursor.clone().expect("more pages");
    let p2 = o
        .history(&alice(), &a, PageRequest::after(&cursor, 1000))
        .await
        .unwrap();
    assert_eq!(p2.items.len(), 3);
    assert!(p2.next_cursor.is_none());
    let seqs: Vec<u64> = p1
        .items
        .iter()
        .chain(&p2.items)
        .map(|m| m.sequence)
        .collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(
        p1.items
            .iter()
            .chain(&p2.items)
            .filter(|m| m.role == MessageRole::User)
            .count(),
        3
    );

    // Cursor của session A không dùng được cho session B (cùng caller hợp lệ).
    let e = o
        .history(&alice(), &b, PageRequest::after(&cursor, 10))
        .await
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidInput);
    // Cursor giả/hỏng bị từ chối.
    for junk in ["", "zz", "00", "6831|x"] {
        let e = o
            .history(&alice(), &a, PageRequest::after(junk, 10))
            .await
            .unwrap_err();
        assert_eq!(e.code(), ErrorCode::InvalidInput, "{junk:?}");
    }
    // Cursor không phải authorization token: người khác có cursor vẫn bị từ chối theo scope.
    let mallory = CallerContext::new("mallory", "ws1");
    let e = o
        .history(&mallory, &a, PageRequest::after(&cursor, 10))
        .await
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::PermissionDenied);
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn unauthorized_access_is_denied_on_every_operation() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let o = env.build(&gw).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();
    let t = o
        .submit(&alice(), &sid, "secret plan".into())
        .await
        .unwrap();
    wait_terminal(&o, &alice(), &sid, &t.turn_id).await;
    let calls = gw.call_count();

    for intruder in [
        CallerContext::new("bob", "ws1"),
        CallerContext::new("alice", "other-workspace"),
    ] {
        let denied = ErrorCode::PermissionDenied;
        assert_eq!(
            o.history(&intruder, &sid, PageRequest::first(10))
                .await
                .unwrap_err()
                .code(),
            denied
        );
        assert_eq!(
            o.result(&intruder, &sid, &t.turn_id)
                .await
                .unwrap_err()
                .code(),
            denied
        );
        assert_eq!(
            o.submit(&intruder, &sid, "hijack".into())
                .await
                .unwrap_err()
                .code(),
            denied
        );
        assert_eq!(
            o.archive_session(&intruder, &sid).await.unwrap_err().code(),
            denied
        );
    }
    assert_eq!(
        gw.call_count(),
        calls,
        "denied submit must not reach the model"
    );
    assert_eq!(
        history_texts(&o, &alice(), &sid).await.len(),
        2,
        "no hijack input persisted"
    );

    // Không tồn tại / agent không hợp lệ.
    assert_eq!(
        o.history(&alice(), "no-such", PageRequest::first(1))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    assert_eq!(
        o.result(&alice(), &sid, "no-such-turn")
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    assert_eq!(
        o.create_session(&alice(), "ghost")
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    // Subagent-only không thể là root; host không tự gán parent.
    assert_eq!(
        o.create_session(&alice(), "sub").await.unwrap_err().code(),
        ErrorCode::PermissionDenied
    );
    assert_eq!(
        o.submit(&alice(), &sid, "   ".into())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::InvalidInput
    );
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn provider_failure_ends_run_with_sanitized_code_and_frees_the_session() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    gw.push_error(GatewayError::Auth);
    gw.push_error(GatewayError::Timeout);
    let o = env.build(&gw).await;
    let sid = o.create_session(&alice(), "root").await.unwrap();

    let t1 = o.submit(&alice(), &sid, "x".into()).await.unwrap();
    let r1 = wait_terminal(&o, &alice(), &sid, &t1.turn_id).await;
    assert_eq!(r1.status, RunStatus::Failed);
    assert_eq!(r1.output, None);
    let e = r1.error.unwrap();
    assert_eq!(e.code, "PROVIDER_AUTH_FAILED");
    assert!(!e.message.contains("sk-"));

    let t2 = o.submit(&alice(), &sid, "y".into()).await.unwrap();
    let r2 = wait_terminal(&o, &alice(), &sid, &t2.turn_id).await;
    assert_eq!(r2.status, RunStatus::TimedOut);
    assert_eq!(r2.error.unwrap().code, "PROVIDER_TIMEOUT");

    // Lỗi không tạo assistant message; history chỉ có input của user.
    let hist = history_texts(&o, &alice(), &sid).await;
    assert!(
        hist.iter().all(|(r, _)| *r == MessageRole::User),
        "{hist:?}"
    );
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn build_creates_store_with_owner_lock_and_clean_install_schema() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let o = env.build(&gw).await;
    assert!(Path::new(&env.store().join("orchestrator.db")).exists());
    assert!(Path::new(&env.store().join("owner.lock")).exists());
    let mut conn = env.sql().await;
    let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(mode, "wal");
    let idx: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_one_running_run_per_session'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(idx, 1);
    o.shutdown().await.unwrap();
}
