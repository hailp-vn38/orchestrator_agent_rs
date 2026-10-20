//! `SessionStore`: sở hữu toàn bộ query và transaction invariants của execution store
//! (docs/09). Không await LLM/tool trong transaction.
//!
//! Mọi write-transaction bắt đầu bằng một câu lệnh ghi (không đọc trước) để lấy write lock
//! ngay và tránh `SQLITE_BUSY_SNAPSHOT` khi nâng cấp từ read snapshot trong WAL.

use std::path::Path;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use crate::error::{ErrorCode, OrchestratorError};
use crate::types::{
    AcceptedTurn, AgentResult, CallerContext, MessageRole, MessageView, Page, PageRequest,
    ResultError, RunStatus,
};

/// Tên file DB trong `store_dir`.
pub(crate) const DB_FILE: &str = "orchestrator.db";
/// SQLite busy timeout cho writer contention (profile G0: transaction ngắn, pool nhỏ).
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const POOL_MAX_CONNECTIONS: u32 = 5;
/// Trần độ dài `error_message` được persist.
const MAX_ERROR_MESSAGE: usize = 500;

/// Routing đã resolve lúc admission; persist vào `agent_runs` của attempt.
#[derive(Debug, Clone)]
pub(crate) struct RunRoute {
    pub provider_id: String,
    pub model_id: String,
    pub fingerprint: String,
}

#[derive(Debug, Clone)]
pub(crate) struct RunRecord {
    pub session_id: String,
    pub agent_id: String,
    pub provider_id: String,
    pub model_id: String,
    pub status: RunStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConversationRole {
    User,
    Assistant,
}

#[derive(Clone)]
pub(crate) struct SessionStore {
    pool: SqlitePool,
}

fn db(context: &'static str) -> impl Fn(sqlx::Error) -> OrchestratorError {
    move |e| OrchestratorError::storage(context, &e)
}

pub(crate) fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn text_json(text: &str) -> String {
    serde_json::json!({ "text": text }).to_string()
}

fn text_of(content_json: &str) -> String {
    serde_json::from_str::<serde_json::Value>(content_json)
        .ok()
        .and_then(|v| v.get("text").and_then(|t| t.as_str()).map(str::to_owned))
        .unwrap_or_default()
}

fn sanitize_message(msg: &str) -> String {
    msg.chars().take(MAX_ERROR_MESSAGE).collect()
}

impl SessionStore {
    /// Mở DB (WAL, FK, busy timeout) và chạy migration từ DB trống/đã có.
    pub(crate) async fn open(path: &Path) -> Result<Self, OrchestratorError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            .busy_timeout(BUSY_TIMEOUT);
        let pool = SqlitePoolOptions::new()
            .max_connections(POOL_MAX_CONNECTIONS)
            .connect_with(options)
            .await
            .map_err(db("open pool"))?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| OrchestratorError::storage("migrate", &e))?;
        Ok(Self { pool })
    }

    pub(crate) async fn close(&self) {
        self.pool.close().await;
    }

    pub(crate) async fn create_root_session(
        &self,
        caller: &CallerContext,
        agent_id: &str,
    ) -> Result<String, OrchestratorError> {
        let id = new_id();
        sqlx::query(
            "INSERT INTO sessions(id, agent_id, owner_id, workspace_id) VALUES(?1, ?2, ?3, ?4)",
        )
        .bind(&id)
        .bind(agent_id)
        .bind(&caller.owner_id)
        .bind(&caller.workspace_id)
        .execute(&self.pool)
        .await
        .map_err(db("create session"))?;
        Ok(id)
    }

    /// Idempotent. Run đang chạy không bị hủy; chỉ chặn admission mới.
    pub(crate) async fn archive_session(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<(), OrchestratorError> {
        let changed = sqlx::query(
            "UPDATE sessions SET status = 'archived', state_revision = state_revision + 1, \
             updated_at = unixepoch() \
             WHERE id = ?1 AND owner_id = ?2 AND workspace_id = ?3 AND status = 'open'",
        )
        .bind(session_id)
        .bind(&caller.owner_id)
        .bind(&caller.workspace_id)
        .execute(&self.pool)
        .await
        .map_err(db("archive session"))?
        .rows_affected();
        if changed == 1 {
            return Ok(());
        }
        match self.explain_rejection(caller, session_id).await {
            e if e.code() == ErrorCode::SessionArchived => Ok(()),
            e => Err(e),
        }
    }

    /// Một transaction ngắn: cấp sequence, claim run `running` (partial unique index) và
    /// persist user message. Chỉ trả `AcceptedTurn` sau commit; không gọi LLM ở đây.
    pub(crate) async fn admit_turn(
        &self,
        caller: &CallerContext,
        session_id: &str,
        text: &str,
        resolve: impl Fn(&str) -> Option<RunRoute>,
    ) -> Result<AcceptedTurn, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin admission"))?;
        let claimed = sqlx::query(
            "UPDATE sessions SET next_sequence = next_sequence + 1, \
             state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = ?1 AND owner_id = ?2 AND workspace_id = ?3 AND status = 'open' \
             RETURNING next_sequence - 1 AS seq, agent_id",
        )
        .bind(session_id)
        .bind(&caller.owner_id)
        .bind(&caller.workspace_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db("claim sequence"))?;
        let Some(row) = claimed else {
            let _ = tx.rollback().await;
            return Err(self.explain_rejection(caller, session_id).await);
        };
        let seq: i64 = row.try_get("seq").map_err(db("read sequence"))?;
        let agent_id: String = row.try_get("agent_id").map_err(db("read agent"))?;
        let Some(route) = resolve(&agent_id) else {
            let _ = tx.rollback().await;
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "the session's agent is no longer configured",
            ));
        };

        let run_id = new_id();
        let turn_id = new_id();
        let inserted = sqlx::query(
            "INSERT INTO agent_runs(id, session_id, agent_id, turn_id, resume_attempt, \
             provider_id, model_id, provider_config_fingerprint, status) \
             VALUES(?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, 'running')",
        )
        .bind(&run_id)
        .bind(session_id)
        .bind(&agent_id)
        .bind(&turn_id)
        .bind(&route.provider_id)
        .bind(&route.model_id)
        .bind(&route.fingerprint)
        .execute(&mut *tx)
        .await;
        match inserted {
            Ok(_) => {}
            Err(sqlx::Error::Database(d)) if d.is_unique_violation() => {
                let _ = tx.rollback().await;
                return Err(OrchestratorError::new(
                    ErrorCode::SessionBusy,
                    "the session already has an active run",
                ));
            }
            Err(e) => return Err(db("claim run")(e)),
        }

        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json) \
             VALUES(?1, ?2, ?3, 'user', ?4)",
        )
        .bind(new_id())
        .bind(session_id)
        .bind(seq)
        .bind(text_json(text))
        .execute(&mut *tx)
        .await
        .map_err(db("insert user message"))?;
        tx.commit().await.map_err(db("commit admission"))?;
        Ok(AcceptedTurn {
            session_id: session_id.to_owned(),
            turn_id,
        })
    }

    /// Lý do admission/ghi bị từ chối, phân loại theo scope trước rồi tới trạng thái.
    async fn explain_rejection(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> OrchestratorError {
        match self.authorize(caller, session_id).await {
            Err(e) => e,
            Ok(s) if s.archived => {
                OrchestratorError::new(ErrorCode::SessionArchived, "the session is archived")
            }
            Ok(_) => OrchestratorError::new(ErrorCode::Internal, "turn admission was rejected"),
        }
    }

    /// Kiểm tra owner/workspace; dùng cho mọi read công khai.
    async fn authorize(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<SessionScope, OrchestratorError> {
        let row = sqlx::query(
            "SELECT owner_id, workspace_id, status, agent_id FROM sessions WHERE id = ?1",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("load session"))?;
        let Some(row) = row else {
            return Err(OrchestratorError::new(
                ErrorCode::NotFound,
                "session not found",
            ));
        };
        let owner: String = row.try_get("owner_id").map_err(db("read owner"))?;
        let workspace: String = row.try_get("workspace_id").map_err(db("read workspace"))?;
        if owner != caller.owner_id || workspace != caller.workspace_id {
            return Err(OrchestratorError::new(
                ErrorCode::PermissionDenied,
                "the session is outside the caller's scope",
            ));
        }
        let status: String = row.try_get("status").map_err(db("read status"))?;
        Ok(SessionScope {
            archived: status == "archived",
        })
    }

    pub(crate) async fn running_run_ids(&self) -> Result<Vec<String>, OrchestratorError> {
        sqlx::query_scalar(
            "SELECT id FROM agent_runs WHERE status = 'running' ORDER BY started_at, id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db("list running runs"))
    }

    pub(crate) async fn load_run(
        &self,
        run_id: &str,
    ) -> Result<Option<RunRecord>, OrchestratorError> {
        let row = sqlx::query(
            "SELECT session_id, agent_id, provider_id, model_id, status FROM agent_runs WHERE id = ?1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("load run"))?;
        let Some(row) = row else { return Ok(None) };
        let status: String = row.try_get("status").map_err(db("read run status"))?;
        Ok(Some(RunRecord {
            session_id: row.try_get("session_id").map_err(db("read run"))?,
            agent_id: row.try_get("agent_id").map_err(db("read run"))?,
            provider_id: row.try_get("provider_id").map_err(db("read run"))?,
            model_id: row.try_get("model_id").map_err(db("read run"))?,
            status: RunStatus::parse(&status)
                .ok_or_else(|| OrchestratorError::new(ErrorCode::Internal, "unknown run status"))?,
        }))
    }

    /// Transcript user/assistant của session theo thứ tự (M1: toàn bộ; compaction ở M4).
    pub(crate) async fn conversation(
        &self,
        session_id: &str,
    ) -> Result<Vec<(ConversationRole, String)>, OrchestratorError> {
        let rows = sqlx::query(
            "SELECT role, content_json FROM messages \
             WHERE session_id = ?1 AND role IN ('user', 'assistant') ORDER BY sequence",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db("load conversation"))?;
        rows.iter()
            .map(|r| {
                let role: String = r.try_get("role").map_err(db("read role"))?;
                let json: String = r.try_get("content_json").map_err(db("read content"))?;
                let role = if role == "user" {
                    ConversationRole::User
                } else {
                    ConversationRole::Assistant
                };
                Ok((role, text_of(&json)))
            })
            .collect()
    }

    /// Final assistant message, `final_message_id` và terminal `completed` cùng transaction.
    /// `Ok(None)`: run không còn `running` (đã terminal) — không ghi đè.
    pub(crate) async fn complete_run(
        &self,
        run_id: &str,
        text: &str,
    ) -> Result<Option<String>, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin completion"))?;
        let claimed = sqlx::query(
            "UPDATE sessions SET next_sequence = next_sequence + 1, \
             state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = (SELECT session_id FROM agent_runs WHERE id = ?1 AND status = 'running') \
             RETURNING id, next_sequence - 1 AS seq",
        )
        .bind(run_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db("claim final sequence"))?;
        let Some(row) = claimed else {
            let _ = tx.rollback().await;
            return Ok(None);
        };
        let session_id: String = row.try_get("id").map_err(db("read session"))?;
        let seq: i64 = row.try_get("seq").map_err(db("read sequence"))?;
        let message_id = new_id();
        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json, run_id) \
             VALUES(?1, ?2, ?3, 'assistant', ?4, ?5)",
        )
        .bind(&message_id)
        .bind(&session_id)
        .bind(seq)
        .bind(text_json(text))
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("insert final message"))?;
        let changed = sqlx::query(
            "UPDATE agent_runs SET status = 'completed', final_message_id = ?1, \
             steps = steps + 1, finished_at = unixepoch() \
             WHERE id = ?2 AND status = 'running'",
        )
        .bind(&message_id)
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("finish run"))?
        .rows_affected();
        if changed != 1 {
            let _ = tx.rollback().await;
            return Ok(None);
        }
        tx.commit().await.map_err(db("commit completion"))?;
        Ok(Some(message_id))
    }

    /// Terminal không-completed (failed/timed_out/interrupted/cancelled). `Ok(false)`:
    /// run không còn `running`.
    pub(crate) async fn finish_run_without_final(
        &self,
        run_id: &str,
        status: RunStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<bool, OrchestratorError> {
        debug_assert!(status.is_terminal() && status != RunStatus::Completed);
        let mut tx = self.pool.begin().await.map_err(db("begin terminal"))?;
        let bumped = sqlx::query(
            "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = (SELECT session_id FROM agent_runs WHERE id = ?1 AND status = 'running')",
        )
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("bump revision"))?
        .rows_affected();
        if bumped == 0 {
            let _ = tx.rollback().await;
            return Ok(false);
        }
        sqlx::query(
            "UPDATE agent_runs SET status = ?1, error_code = ?2, error_message = ?3, \
             finished_at = unixepoch() WHERE id = ?4 AND status = 'running'",
        )
        .bind(status.as_str())
        .bind(error_code)
        .bind(sanitize_message(error_message))
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("finish run"))?;
        tx.commit().await.map_err(db("commit terminal"))?;
        Ok(true)
    }

    /// Lúc build (đã giữ owner lock): run còn `running` từ process trước không có owner
    /// nào nữa. M1 đánh dấu `interrupted` để session không kẹt; auto-resume/reconcile
    /// thuộc M5, không replay.
    pub(crate) async fn interrupt_stale_runs(&self) -> Result<u64, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin stale scan"))?;
        sqlx::query(
            "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id IN (SELECT session_id FROM agent_runs WHERE status = 'running')",
        )
        .execute(&mut *tx)
        .await
        .map_err(db("bump stale sessions"))?;
        let n = sqlx::query(
            "UPDATE agent_runs SET status = 'interrupted', error_code = 'INTERRUPTED', \
             error_message = 'the process stopped before the run finished', \
             finished_at = unixepoch() WHERE status = 'running'",
        )
        .execute(&mut *tx)
        .await
        .map_err(db("interrupt stale runs"))?
        .rows_affected();
        tx.commit().await.map_err(db("commit stale scan"))?;
        Ok(n)
    }

    pub(crate) async fn history_page(
        &self,
        caller: &CallerContext,
        session_id: &str,
        page: PageRequest,
        max_page: u32,
    ) -> Result<Page<MessageView>, OrchestratorError> {
        self.authorize(caller, session_id).await?;
        let after = match &page.cursor {
            None => 0,
            Some(c) => decode_cursor(c, session_id)?,
        };
        let limit = page.limit.clamp(1, max_page);
        let rows = sqlx::query(
            "SELECT id, sequence, role, content_json, run_id FROM messages \
             WHERE session_id = ?1 AND sequence > ?2 AND role IN ('user', 'assistant', 'tool') \
             ORDER BY sequence LIMIT ?3",
        )
        .bind(session_id)
        .bind(after as i64)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await
        .map_err(db("load history"))?;

        let mut items = Vec::with_capacity(rows.len().min(limit as usize));
        for row in &rows {
            let role: String = row.try_get("role").map_err(db("read role"))?;
            let json: String = row.try_get("content_json").map_err(db("read content"))?;
            let sequence: i64 = row.try_get("sequence").map_err(db("read sequence"))?;
            items.push(MessageView {
                id: row.try_get("id").map_err(db("read id"))?,
                sequence: sequence as u64,
                role: MessageRole::parse(&role).ok_or_else(|| {
                    OrchestratorError::new(ErrorCode::Internal, "unknown message role")
                })?,
                text: text_of(&json),
                run_id: row.try_get("run_id").map_err(db("read run id"))?,
            });
        }
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items.last().map(|m| encode_cursor(session_id, m.sequence))
        } else {
            None
        };
        Ok(Page { items, next_cursor })
    }

    /// Projection của attempt mới nhất; `output` chỉ từ `final_message_id` của chính attempt đó.
    pub(crate) async fn result(
        &self,
        caller: &CallerContext,
        session_id: &str,
        turn_id: &str,
    ) -> Result<AgentResult, OrchestratorError> {
        self.authorize(caller, session_id).await?;
        let row = sqlx::query(
            "SELECT id, resume_attempt, status, final_message_id, error_code, error_message \
             FROM agent_runs WHERE session_id = ?1 AND turn_id = ?2 \
             ORDER BY resume_attempt DESC LIMIT 1",
        )
        .bind(session_id)
        .bind(turn_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("load result"))?;
        let Some(row) = row else {
            return Err(OrchestratorError::new(
                ErrorCode::NotFound,
                "turn not found",
            ));
        };
        let run_id: String = row.try_get("id").map_err(db("read run id"))?;
        let attempt: i64 = row.try_get("resume_attempt").map_err(db("read attempt"))?;
        let status: String = row.try_get("status").map_err(db("read status"))?;
        let final_message_id: Option<String> =
            row.try_get("final_message_id").map_err(db("read final"))?;
        let error_code: Option<String> = row.try_get("error_code").map_err(db("read error"))?;
        let error_message: Option<String> =
            row.try_get("error_message").map_err(db("read error"))?;

        let output = match &final_message_id {
            None => None,
            Some(id) => {
                let json: Option<String> = sqlx::query_scalar(
                    "SELECT content_json FROM messages \
                     WHERE id = ?1 AND run_id = ?2 AND session_id = ?3",
                )
                .bind(id)
                .bind(&run_id)
                .bind(session_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db("load final message"))?;
                json.map(|j| text_of(&j))
            }
        };
        Ok(AgentResult {
            session_id: session_id.to_owned(),
            turn_id: turn_id.to_owned(),
            run_id,
            attempt: attempt as u32,
            status: RunStatus::parse(&status)
                .ok_or_else(|| OrchestratorError::new(ErrorCode::Internal, "unknown run status"))?,
            output,
            final_message_id,
            error: error_code.map(|code| ResultError {
                code,
                message: error_message.unwrap_or_default(),
            }),
        })
    }
}

struct SessionScope {
    archived: bool,
}

const CURSOR_VERSION: &str = "h1";

/// Cursor opaque gắn với đúng session + thứ tự `sequence`. Mỗi trang vẫn xác thực lại
/// scope; cursor không phải authorization token.
fn encode_cursor(session_id: &str, last_sequence: u64) -> String {
    let raw = format!("{CURSOR_VERSION}|{session_id}|{last_sequence}");
    raw.bytes().map(|b| format!("{b:02x}")).collect()
}

fn decode_cursor(cursor: &str, session_id: &str) -> Result<u64, OrchestratorError> {
    let bad = || OrchestratorError::new(ErrorCode::InvalidInput, "invalid cursor for this query");
    if !cursor.len().is_multiple_of(2) || !cursor.is_ascii() {
        return Err(bad());
    }
    let bytes: Result<Vec<u8>, _> = (0..cursor.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&cursor[i..i + 2], 16))
        .collect();
    let raw = String::from_utf8(bytes.map_err(|_| bad())?).map_err(|_| bad())?;
    let mut parts = raw.splitn(3, '|');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(v), Some(s), Some(seq)) if v == CURSOR_VERSION && s == session_id => {
            seq.parse::<u64>().map_err(|_| bad())
        }
        _ => Err(bad()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
