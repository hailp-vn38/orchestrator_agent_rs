//! Tests cho `SessionStore`.

use super::transcript::*;
use super::*;
use crate::error::ErrorCode;
use crate::types::{CallerContext, PageRequest, RunStatus, SessionPolicy};

fn caller() -> CallerContext {
    CallerContext::new("u1", "w1")
}

fn route(_: &str) -> Option<RunRoute> {
    Some(RunRoute {
        provider_id: "p".into(),
        model_id: "m".into(),
        fingerprint: "v1:x".into(),
    })
}

async fn store() -> (tempfile::TempDir, SessionStore) {
    let dir = tempfile::tempdir().unwrap();
    let s = SessionStore::open(&dir.path().join(DB_FILE)).await.unwrap();
    (dir, s)
}

#[test]
fn cursor_roundtrip_and_scope_binding() {
    let c = encode_cursor("sess-a", 7);
    assert_eq!(decode_cursor(&c, "sess-a").unwrap(), 7);
    assert_eq!(
        decode_cursor(&c, "sess-b").unwrap_err().code(),
        ErrorCode::InvalidInput
    );
    for junk in ["", "zz", "abc", "6831", "é"] {
        assert!(decode_cursor(junk, "sess-a").is_err(), "{junk}");
    }
}

#[tokio::test]
async fn migration_runs_from_empty_db_and_is_rerunnable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(DB_FILE);
    let s = SessionStore::open(&path).await.unwrap();
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '\\_%' ESCAPE '\\' \
         AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(&s.pool)
    .await
    .unwrap();
    assert_eq!(
        tables,
        [
            "agent_runs",
            "artifacts",
            "messages",
            "sessions",
            "tool_invocations"
        ]
    );
    s.close().await;
    SessionStore::open(&path).await.unwrap();
}

#[tokio::test]
async fn final_message_pointer_and_terminal_state_commit_atomically() {
    let (_d, s) = store().await;
    let sid = s.create_root_session(&caller(), "root").await.unwrap();
    let turn = s.admit_turn(&caller(), &sid, "hi", route).await.unwrap();
    let run_id = s.running_run_ids().await.unwrap().remove(0);

    // Ép bước cập nhật run thất bại sau khi message đã insert trong cùng transaction.
    sqlx::query(
        "CREATE TRIGGER fail_complete BEFORE UPDATE ON agent_runs \
         WHEN NEW.status = 'completed' BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(&s.pool)
    .await
    .unwrap();
    assert!(s.complete_run(&run_id, "answer").await.is_err());
    let assistants: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE role = 'assistant'")
            .fetch_one(&s.pool)
            .await
            .unwrap();
    assert_eq!(assistants, 0, "assistant message must roll back");
    let r = s.result(&caller(), &sid, &turn.turn_id).await.unwrap();
    assert_eq!(r.status, RunStatus::Running);
    assert_eq!(r.final_message_id, None);

    sqlx::query("DROP TRIGGER fail_complete")
        .execute(&s.pool)
        .await
        .unwrap();
    let mid = s.complete_run(&run_id, "answer").await.unwrap().unwrap();
    let r = s.result(&caller(), &sid, &turn.turn_id).await.unwrap();
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(r.final_message_id.as_deref(), Some(mid.as_str()));
    assert_eq!(r.output.as_deref(), Some("answer"));
    // Terminal run không bị ghi đè lần hai.
    assert_eq!(s.complete_run(&run_id, "again").await.unwrap(), None);
}

#[tokio::test]
async fn second_claim_on_same_session_is_busy_and_leaves_no_trace() {
    let (_d, s) = store().await;
    let sid = s.create_root_session(&caller(), "root").await.unwrap();
    s.admit_turn(&caller(), &sid, "one", route).await.unwrap();
    let e = s
        .admit_turn(&caller(), &sid, "two", route)
        .await
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::SessionBusy);
    let users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE role = 'user'")
        .fetch_one(&s.pool)
        .await
        .unwrap();
    assert_eq!(users, 1);
}

/// Parent root session + run `running` + invocation `subagent` đã `running`.
async fn parent_invocation(s: &SessionStore, c: &CallerContext) -> (String, String, String) {
    let sid = s.create_root_session(c, "root").await.unwrap();
    s.admit_turn(c, &sid, "go", route).await.unwrap();
    let run_id = run_of(s, &sid).await;
    let inv = next_subagent_call(s, &run_id, "c1").await;
    (sid, run_id, inv)
}

async fn run_of(s: &SessionStore, session_id: &str) -> String {
    sqlx::query_scalar(
        "SELECT id FROM agent_runs WHERE session_id = ?1 ORDER BY rowid DESC LIMIT 1",
    )
    .bind(session_id)
    .fetch_one(&s.pool)
    .await
    .unwrap()
}

async fn next_subagent_call(s: &SessionStore, run_id: &str, call_id: &str) -> String {
    let calls = [StoredToolCall {
        id: call_id.into(),
        name: "subagent".into(),
        arguments: serde_json::json!({}),
    }];
    let inv = s
        .commit_tool_calls(run_id, "", &calls)
        .await
        .unwrap()
        .unwrap()
        .remove(0)
        .id;
    assert!(s.begin_invocation(run_id, &inv).await.unwrap());
    inv
}

async fn count(s: &SessionStore, sql: &str, bind: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(bind)
        .fetch_one(&s.pool)
        .await
        .unwrap()
}

fn new_child(agent: &str) -> ChildTarget<'_> {
    ChildTarget::New { agent_id: agent }
}

#[tokio::test]
async fn child_admission_links_atomically_and_is_idempotent_per_invocation() {
    let (_d, s) = store().await;
    let (parent, _run, inv) = parent_invocation(&s, &caller()).await;

    let a = s
        .admit_child_turn(&inv, new_child("rev"), "review this", route)
        .await
        .unwrap();
    assert!(a.created);
    let parent_of: Option<String> =
        sqlx::query_scalar("SELECT parent_session_id FROM sessions WHERE id = ?1")
            .bind(&a.session_id)
            .fetch_one(&s.pool)
            .await
            .unwrap();
    assert_eq!(parent_of.as_deref(), Some(parent.as_str()));
    let (cs, ct): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT child_session_id, child_turn_id FROM tool_invocations WHERE id = ?1",
    )
    .bind(&inv)
    .fetch_one(&s.pool)
    .await
    .unwrap();
    assert_eq!(cs.as_deref(), Some(a.session_id.as_str()));
    assert_eq!(ct.as_deref(), Some(a.turn_id.as_str()));
    let child_run = run_of(&s, &a.session_id).await;
    assert_eq!(
        s.load_run(&child_run).await.unwrap().unwrap().status,
        RunStatus::Running
    );

    // Replay (parent crash sau admission): đúng turn cũ, không append/claim thêm.
    for _ in 0..2 {
        let again = s
            .admit_child_turn(&inv, new_child("rev"), "review this", route)
            .await
            .unwrap();
        assert_eq!(again.session_id, a.session_id);
        assert_eq!(again.turn_id, a.turn_id);
        assert!(!again.created);
    }
    let q = "SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND role = 'user'";
    assert_eq!(count(&s, q, &a.session_id).await, 1);
    let q = "SELECT COUNT(*) FROM agent_runs WHERE session_id = ?1";
    assert_eq!(count(&s, q, &a.session_id).await, 1);

    // Child terminal trước khi parent ghi result: replay vẫn trả đúng turn, wait đúng final.
    s.complete_run(&child_run, "child final").await.unwrap();
    let after = s
        .admit_child_turn(&inv, new_child("rev"), "review this", route)
        .await
        .unwrap();
    assert_eq!(after.turn_id, a.turn_id);
    assert!(!after.created);
    let r = s
        .load_result(&after.session_id, &after.turn_id)
        .await
        .unwrap();
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(r.output.as_deref(), Some("child final"));
    assert_eq!(count(&s, q, &a.session_id).await, 1);
}

#[tokio::test]
async fn identical_followup_prompt_in_a_new_invocation_is_a_new_logical_turn() {
    let (_d, s) = store().await;
    let (_parent, run, inv1) = parent_invocation(&s, &caller()).await;
    let a = s
        .admit_child_turn(&inv1, new_child("rev"), "same prompt", route)
        .await
        .unwrap();
    let child_run = run_of(&s, &a.session_id).await;
    s.complete_run(&child_run, "first").await.unwrap();
    let settled = crate::executor::Settled::failure("X", "done");
    assert!(s.settle_invocation(&run, &inv1, &settled).await.unwrap());

    let inv2 = next_subagent_call(&s, &run, "c2").await;
    let resume = ChildTarget::Resume {
        session_id: &a.session_id,
        agent_id: "rev",
    };
    let b = s
        .admit_child_turn(&inv2, resume, "same prompt", route)
        .await
        .unwrap();
    assert!(b.created);
    assert_eq!(b.session_id, a.session_id);
    assert_ne!(
        b.turn_id, a.turn_id,
        "identical prompt is not the same turn"
    );
    let q = "SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND role = 'user'";
    assert_eq!(count(&s, q, &a.session_id).await, 2);
    let q = "SELECT COUNT(*) FROM agent_runs WHERE session_id = ?1";
    assert_eq!(count(&s, q, &a.session_id).await, 2);
    // Mỗi (child_session, child_turn) chỉ thuộc một invocation.
    let linked: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT child_turn_id) FROM tool_invocations \
         WHERE child_session_id = ?1",
    )
    .bind(&a.session_id)
    .fetch_one(&s.pool)
    .await
    .unwrap();
    assert_eq!(linked, 2);
}

#[tokio::test]
async fn busy_child_rejects_second_run_without_side_effects() {
    let (_d, s) = store().await;
    let (_p, run, inv1) = parent_invocation(&s, &caller()).await;
    let a = s
        .admit_child_turn(&inv1, new_child("rev"), "one", route)
        .await
        .unwrap();
    let inv2 = next_subagent_call(&s, &run, "c2").await;
    let resume = ChildTarget::Resume {
        session_id: &a.session_id,
        agent_id: "rev",
    };
    let e = s
        .admit_child_turn(&inv2, resume, "two", route)
        .await
        .unwrap_err();
    assert!(matches!(e, ChildAdmitError::Busy), "{e:?}");
    let q = "SELECT COUNT(*) FROM messages WHERE session_id = ?1";
    assert_eq!(count(&s, q, &a.session_id).await, 1, "no appended input");
    let seq: i64 = sqlx::query_scalar("SELECT next_sequence FROM sessions WHERE id = ?1")
        .bind(&a.session_id)
        .fetch_one(&s.pool)
        .await
        .unwrap();
    assert_eq!(seq, 2, "sequence not consumed by the rejected attempt");
    let linked: Option<String> =
        sqlx::query_scalar("SELECT child_turn_id FROM tool_invocations WHERE id = ?1")
            .bind(&inv2)
            .fetch_one(&s.pool)
            .await
            .unwrap();
    assert_eq!(linked, None);
}

#[tokio::test]
async fn resume_requires_same_parent_scope_agent_and_open_session() {
    let (_d, s) = store().await;
    let (pa, ra, inv_a) = parent_invocation(&s, &caller()).await;
    let a = s
        .admit_child_turn(&inv_a, new_child("rev"), "one", route)
        .await
        .unwrap();
    s.complete_run(&run_of(&s, &a.session_id).await, "ok")
        .await
        .unwrap();

    // Root khác (cùng owner) không resume được child của Root A.
    let (_pb, _rb, inv_b) = parent_invocation(&s, &caller()).await;
    let target = |agent| ChildTarget::Resume {
        session_id: &a.session_id,
        agent_id: agent,
    };
    let e = s
        .admit_child_turn(&inv_b, target("rev"), "x", route)
        .await
        .unwrap_err();
    assert!(matches!(e, ChildAdmitError::NotChild), "{e:?}");
    // Session không tồn tại không phân biệt được với session của parent khác.
    let ghost = ChildTarget::Resume {
        session_id: "nope",
        agent_id: "rev",
    };
    let e = s
        .admit_child_turn(&inv_b, ghost, "x", route)
        .await
        .unwrap_err();
    assert!(matches!(e, ChildAdmitError::NotChild), "{e:?}");
    // Root làm session_id trỏ vào chính một root session khác.
    let other_root = ChildTarget::Resume {
        session_id: &pa,
        agent_id: "root",
    };
    let e = s
        .admit_child_turn(&inv_b, other_root, "x", route)
        .await
        .unwrap_err();
    assert!(matches!(e, ChildAdmitError::NotChild), "{e:?}");

    // Đúng parent nhưng sai agent / session đã archive.
    let inv_a2 = next_subagent_call(&s, &ra, "c2").await;
    let e = s
        .admit_child_turn(&inv_a2, target("other"), "x", route)
        .await
        .unwrap_err();
    assert!(matches!(e, ChildAdmitError::AgentMismatch), "{e:?}");
    sqlx::query("UPDATE sessions SET status = 'archived' WHERE id = ?1")
        .bind(&a.session_id)
        .execute(&s.pool)
        .await
        .unwrap();
    let e = s
        .admit_child_turn(&inv_a2, target("rev"), "x", route)
        .await
        .unwrap_err();
    assert!(matches!(e, ChildAdmitError::Archived), "{e:?}");

    // Owner khác với child (dữ liệu bất thường) cũng bị từ chối.
    let other_owner = CallerContext::new("mallory", "w1");
    let (_pm, _rm, inv_m) = parent_invocation(&s, &other_owner).await;
    let e = s
        .admit_child_turn(&inv_m, target("rev"), "x", route)
        .await
        .unwrap_err();
    assert!(matches!(e, ChildAdmitError::NotChild), "{e:?}");
}

#[tokio::test]
async fn child_cannot_delegate_further() {
    let (_d, s) = store().await;
    let (_p, _r, inv) = parent_invocation(&s, &caller()).await;
    let a = s
        .admit_child_turn(&inv, new_child("rev"), "one", route)
        .await
        .unwrap();
    let child_run = run_of(&s, &a.session_id).await;
    let child_inv = next_subagent_call(&s, &child_run, "cc1").await;
    let e = s
        .admit_child_turn(&child_inv, new_child("rev"), "deeper", route)
        .await
        .unwrap_err();
    assert!(matches!(e, ChildAdmitError::DepthLimit), "{e:?}");
    let sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sessions")
        .fetch_one(&s.pool)
        .await
        .unwrap();
    assert_eq!(sessions, 2);
}

#[tokio::test]
async fn failed_admission_rolls_back_session_input_run_and_link() {
    let (_d, s) = store().await;
    let (p, _r, inv) = parent_invocation(&s, &caller()).await;
    // Fault: claim run của child session thất bại sau khi session + input đã ghi.
    sqlx::query(
        "CREATE TRIGGER fail_child_run BEFORE INSERT ON agent_runs \
         WHEN NEW.session_id IN (SELECT id FROM sessions WHERE parent_session_id IS NOT NULL) \
         BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(&s.pool)
    .await
    .unwrap();
    let before: i64 = sqlx::query_scalar("SELECT state_revision FROM sessions WHERE id = ?1")
        .bind(&p)
        .fetch_one(&s.pool)
        .await
        .unwrap();
    let e = s
        .admit_child_turn(&inv, new_child("rev"), "x", route)
        .await
        .unwrap_err();
    assert!(matches!(e, ChildAdmitError::Store), "{e:?}");
    let child_sessions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sessions WHERE parent_session_id IS NOT NULL")
            .fetch_one(&s.pool)
            .await
            .unwrap();
    assert_eq!(child_sessions, 0, "child session must roll back");
    let child_msgs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE role = 'user'")
        .fetch_one(&s.pool)
        .await
        .unwrap();
    assert_eq!(child_msgs, 1, "only the root input remains");
    let link: Option<String> =
        sqlx::query_scalar("SELECT child_session_id FROM tool_invocations WHERE id = ?1")
            .bind(&inv)
            .fetch_one(&s.pool)
            .await
            .unwrap();
    assert_eq!(link, None);
    let after: i64 = sqlx::query_scalar("SELECT state_revision FROM sessions WHERE id = ?1")
        .bind(&p)
        .fetch_one(&s.pool)
        .await
        .unwrap();
    assert_eq!(before, after, "no revision bump on rollback");

    // Gỡ fault: cùng invocation admit lại bình thường (không còn dấu vết dở dang).
    sqlx::query("DROP TRIGGER fail_child_run")
        .execute(&s.pool)
        .await
        .unwrap();
    let ok = s
        .admit_child_turn(&inv, new_child("rev"), "x", route)
        .await
        .unwrap();
    assert!(ok.created);
}

#[tokio::test]
async fn child_gets_only_the_hosts_child_ceiling_not_the_roots_own_policy() {
    let (_d, s) = store().await;
    let policy = SessionPolicy::default()
        .with_allowed_tools(["subagent"])
        .with_child_allowed_tools(["read_note"])
        .with_child_allowed_skills::<_, String>([]);
    let sid = s
        .create_root_session_with(&caller(), "root", &policy)
        .await
        .unwrap();
    s.admit_turn(&caller(), &sid, "go", route).await.unwrap();
    let run = run_of(&s, &sid).await;
    let inv = next_subagent_call(&s, &run, "c1").await;
    let a = s
        .admit_child_turn(&inv, new_child("rev"), "x", route)
        .await
        .unwrap();
    let rec = s
        .load_run(&run_of(&s, &a.session_id).await)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rec.policy.allowed_tools,
        Some(vec!["read_note".to_string()])
    );
    assert_eq!(rec.policy.allowed_skills, Some(Vec::new()));
    assert_eq!(rec.policy.child_allowed_tools, None);
    assert_eq!(rec.parent_session_id.as_deref(), Some(sid.as_str()));

    // Không đặt trần: child dùng đủ quyền của AgentDefinition (không policy).
    let (_p2, _r2, inv2) = parent_invocation(&s, &caller()).await;
    let b = s
        .admit_child_turn(&inv2, new_child("rev"), "x", route)
        .await
        .unwrap();
    let rec = s
        .load_run(&run_of(&s, &b.session_id).await)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rec.policy, SessionPolicy::default());
}

#[tokio::test]
async fn child_session_rejects_direct_admission_but_stays_readable() {
    let (_d, s) = store().await;
    let (_p, _r, inv) = parent_invocation(&s, &caller()).await;
    let a = s
        .admit_child_turn(&inv, new_child("rev"), "x", route)
        .await
        .unwrap();
    s.complete_run(&run_of(&s, &a.session_id).await, "ok")
        .await
        .unwrap();
    let e = s
        .admit_turn(&caller(), &a.session_id, "direct", route)
        .await
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::PermissionDenied);
    let q = "SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND role = 'user'";
    assert_eq!(count(&s, q, &a.session_id).await, 1);
    let h = s
        .history_page(&caller(), &a.session_id, PageRequest::first(10), 50)
        .await
        .unwrap();
    assert_eq!(h.items.len(), 2);
}

#[tokio::test]
async fn concurrent_resumes_of_one_child_admit_exactly_one() {
    let (_d, s) = store().await;
    let (_p, run, inv1) = parent_invocation(&s, &caller()).await;
    let a = s
        .admit_child_turn(&inv1, new_child("rev"), "first", route)
        .await
        .unwrap();
    s.complete_run(&run_of(&s, &a.session_id).await, "ok")
        .await
        .unwrap();
    let settled = crate::executor::Settled::failure("X", "done");
    assert!(s.settle_invocation(&run, &inv1, &settled).await.unwrap());

    // Hai invocation cùng parent run, cùng cố resume child (đã rảnh) đồng thời.
    let calls: Vec<StoredToolCall> = ["c2", "c3"]
        .iter()
        .map(|id| StoredToolCall {
            id: (*id).into(),
            name: "subagent".into(),
            arguments: serde_json::json!({}),
        })
        .collect();
    let pending = s
        .commit_tool_calls(&run, "", &calls)
        .await
        .unwrap()
        .unwrap();
    let target = ChildTarget::Resume {
        session_id: &a.session_id,
        agent_id: "rev",
    };
    let (x, y) = tokio::join!(
        s.admit_child_turn(&pending[0].id, target, "x", route),
        s.admit_child_turn(&pending[1].id, target, "y", route),
    );
    let outcomes = [x, y];
    let ok = outcomes.iter().filter(|r| r.is_ok()).count();
    let busy = outcomes
        .iter()
        .filter(|r| matches!(r, Err(ChildAdmitError::Busy)))
        .count();
    assert_eq!((ok, busy), (1, 1), "{outcomes:?}");
    let q = "SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND role = 'user'";
    assert_eq!(
        count(&s, q, &a.session_id).await,
        2,
        "first + exactly one more"
    );
    let q = "SELECT COUNT(*) FROM agent_runs WHERE session_id = ?1 AND status = 'running'";
    assert_eq!(count(&s, q, &a.session_id).await, 1);
}

/// Mọi SQL phải nằm trong `src/store/` (ADR-007/ADR-016): module khác chỉ gọi `SessionStore`.
#[test]
fn sql_is_confined_to_store_module() {
    fn walk(dir: &std::path::Path, offenders: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n == "store") {
                    continue;
                }
                walk(&path, offenders);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path).unwrap();
                if text.contains("sqlx::query") || text.contains("sqlx::Row") {
                    offenders.push(path.display().to_string());
                }
            }
        }
    }
    let mut offenders = Vec::new();
    walk(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut offenders,
    );
    assert!(offenders.is_empty(), "SQL ngoài src/store: {offenders:?}");
}
