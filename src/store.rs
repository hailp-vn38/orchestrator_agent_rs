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
use crate::executor::Settled;
use crate::types::{
    AcceptedTurn, AgentResult, CallerContext, MessageRole, MessageToolCall, MessageView, Page,
    PageRequest, ResultError, RunStatus, SessionPolicy,
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
    pub owner_id: String,
    pub workspace_id: String,
    pub policy: SessionPolicy,
}

/// Tool call đã persist trong assistant message (`id` = provider call ID).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

/// Transcript dùng để dựng model context (user/assistant/tool theo thứ tự `sequence`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TranscriptEntry {
    User(String),
    Assistant {
        text: String,
        calls: Vec<StoredToolCall>,
    },
    Tool {
        call_id: String,
        tool_name: String,
        text: String,
    },
}

/// Invocation đã persist nhưng chưa settle. `started=false`: `requested` (chưa từng chạy);
/// `started=true`: `running` (outcome chưa biết nếu process chết giữa chừng).
#[derive(Debug, Clone)]
pub(crate) struct PendingInvocation {
    pub id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub arguments: serde_json::Value,
    pub started: bool,
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

fn parse_policy(permissions_json: Option<&str>) -> SessionPolicy {
    let list = |v: &serde_json::Value, key: &str| {
        v.get(key).and_then(|a| a.as_array()).map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        })
    };
    permissions_json
        .and_then(|j| serde_json::from_str::<serde_json::Value>(j).ok())
        .map(|v| SessionPolicy {
            allowed_tools: list(&v, "allowed_tools"),
            allowed_skills: list(&v, "allowed_skills"),
        })
        .unwrap_or_default()
}

/// Tool calls trong `content_json` của assistant message (rỗng nếu là câu trả lời cuối).
fn calls_of(content_json: &str) -> Vec<StoredToolCall> {
    serde_json::from_str::<serde_json::Value>(content_json)
        .ok()
        .and_then(|v| v.get("tool_calls").and_then(|c| c.as_array()).cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|c| {
            Some(StoredToolCall {
                id: c.get("id")?.as_str()?.to_owned(),
                name: c.get("name")?.as_str()?.to_owned(),
                arguments: c.get("arguments").cloned().unwrap_or(serde_json::json!({})),
            })
        })
        .collect()
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

    #[cfg(test)]
    pub(crate) async fn create_root_session(
        &self,
        caller: &CallerContext,
        agent_id: &str,
    ) -> Result<String, OrchestratorError> {
        self.create_root_session_with(caller, agent_id, &SessionPolicy::default())
            .await
    }

    pub(crate) async fn create_root_session_with(
        &self,
        caller: &CallerContext,
        agent_id: &str,
        policy: &SessionPolicy,
    ) -> Result<String, OrchestratorError> {
        let id = new_id();
        let permissions = (policy != &SessionPolicy::default()).then(|| {
            serde_json::json!({
                "allowed_tools": policy.allowed_tools,
                "allowed_skills": policy.allowed_skills,
            })
            .to_string()
        });
        sqlx::query(
            "INSERT INTO sessions(id, agent_id, owner_id, workspace_id, permissions_json) \
             VALUES(?1, ?2, ?3, ?4, ?5)",
        )
        .bind(&id)
        .bind(agent_id)
        .bind(&caller.owner_id)
        .bind(&caller.workspace_id)
        .bind(permissions)
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
            "SELECT r.session_id, r.agent_id, r.provider_id, r.model_id, r.status, \
             s.owner_id, s.workspace_id, s.permissions_json \
             FROM agent_runs r JOIN sessions s ON s.id = r.session_id WHERE r.id = ?1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("load run"))?;
        let Some(row) = row else { return Ok(None) };
        let status: String = row.try_get("status").map_err(db("read run status"))?;
        let permissions: Option<String> =
            row.try_get("permissions_json").map_err(db("read policy"))?;
        Ok(Some(RunRecord {
            session_id: row.try_get("session_id").map_err(db("read run"))?,
            agent_id: row.try_get("agent_id").map_err(db("read run"))?,
            provider_id: row.try_get("provider_id").map_err(db("read run"))?,
            model_id: row.try_get("model_id").map_err(db("read run"))?,
            status: RunStatus::parse(&status)
                .ok_or_else(|| OrchestratorError::new(ErrorCode::Internal, "unknown run status"))?,
            owner_id: row.try_get("owner_id").map_err(db("read run"))?,
            workspace_id: row.try_get("workspace_id").map_err(db("read run"))?,
            policy: parse_policy(permissions.as_deref()),
        }))
    }

    /// Transcript user/assistant/tool của session theo thứ tự (toàn bộ; compaction ở M4).
    pub(crate) async fn conversation(
        &self,
        session_id: &str,
    ) -> Result<Vec<TranscriptEntry>, OrchestratorError> {
        let rows = sqlx::query(
            "SELECT m.role, m.content_json, m.tool_call_id, ti.tool_name \
             FROM messages m LEFT JOIN tool_invocations ti ON ti.id = m.tool_invocation_id \
             WHERE m.session_id = ?1 AND m.role IN ('user', 'assistant', 'tool') \
             ORDER BY m.sequence",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db("load conversation"))?;
        rows.iter()
            .map(|r| {
                let role: String = r.try_get("role").map_err(db("read role"))?;
                let json: String = r.try_get("content_json").map_err(db("read content"))?;
                Ok(match role.as_str() {
                    "user" => TranscriptEntry::User(text_of(&json)),
                    "tool" => {
                        let call_id: Option<String> =
                            r.try_get("tool_call_id").map_err(db("read call id"))?;
                        let tool_name: Option<String> =
                            r.try_get("tool_name").map_err(db("read tool name"))?;
                        TranscriptEntry::Tool {
                            call_id: call_id.unwrap_or_default(),
                            tool_name: tool_name.unwrap_or_default(),
                            text: text_of(&json),
                        }
                    }
                    _ => TranscriptEntry::Assistant {
                        text: text_of(&json),
                        calls: calls_of(&json),
                    },
                })
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

    /// Một transaction: assistant message (text + tất cả tool calls) và **mọi** invocation
    /// `requested`. Chỉ sau commit này mới được dispatch bất kỳ tool nào. `Ok(None)`: run
    /// không còn `running`. Trả invocation theo đúng thứ tự provider.
    pub(crate) async fn commit_tool_calls(
        &self,
        run_id: &str,
        text: &str,
        calls: &[StoredToolCall],
    ) -> Result<Option<Vec<PendingInvocation>>, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin tool calls"))?;
        let Some((session_id, seq)) = claim_sequence(&mut tx, run_id).await? else {
            let _ = tx.rollback().await;
            return Ok(None);
        };
        let message_id = new_id();
        let content = serde_json::json!({
            "text": text,
            "tool_calls": calls
                .iter()
                .map(|c| serde_json::json!({
                    "id": c.id, "name": c.name, "arguments": c.arguments,
                }))
                .collect::<Vec<_>>(),
        })
        .to_string();
        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json, run_id) \
             VALUES(?1, ?2, ?3, 'assistant', ?4, ?5)",
        )
        .bind(&message_id)
        .bind(&session_id)
        .bind(seq)
        .bind(content)
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("insert assistant tool-call message"))?;
        let mut pending = Vec::with_capacity(calls.len());
        for call in calls {
            let id = new_id();
            sqlx::query(
                "INSERT INTO tool_invocations(id, session_id, run_id, tool_call_id, \
                 assistant_message_id, tool_name, arguments_json, status) \
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, 'requested')",
            )
            .bind(&id)
            .bind(&session_id)
            .bind(run_id)
            .bind(&call.id)
            .bind(&message_id)
            .bind(&call.name)
            .bind(call.arguments.to_string())
            .execute(&mut *tx)
            .await
            .map_err(db("insert tool invocation"))?;
            pending.push(PendingInvocation {
                id,
                tool_call_id: call.id.clone(),
                tool_name: call.name.clone(),
                arguments: call.arguments.clone(),
                started: false,
            });
        }
        let changed = sqlx::query(
            "UPDATE agent_runs SET steps = steps + 1 WHERE id = ?1 AND status = 'running'",
        )
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("count step"))?
        .rows_affected();
        if changed != 1 {
            let _ = tx.rollback().await;
            return Ok(None);
        }
        tx.commit().await.map_err(db("commit tool calls"))?;
        Ok(Some(pending))
    }

    /// `requested` → `running`, commit **trước** khi gọi tool: nếu process chết sau điểm này
    /// thì outcome là unknown và không bao giờ được replay mù. `Ok(false)`: không còn hợp lệ.
    pub(crate) async fn begin_invocation(
        &self,
        run_id: &str,
        invocation_id: &str,
    ) -> Result<bool, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin invocation"))?;
        let started = sqlx::query(
            "UPDATE tool_invocations SET status = 'running', started_at = unixepoch() \
             WHERE id = ?1 AND run_id = ?2 AND status = 'requested' \
             AND EXISTS (SELECT 1 FROM agent_runs WHERE id = ?2 AND status = 'running') \
             RETURNING session_id",
        )
        .bind(invocation_id)
        .bind(run_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db("start invocation"))?;
        let Some(row) = started else {
            let _ = tx.rollback().await;
            return Ok(false);
        };
        let session_id: String = row.try_get("session_id").map_err(db("read session"))?;
        sqlx::query(
            "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = ?1",
        )
        .bind(&session_id)
        .execute(&mut *tx)
        .await
        .map_err(db("bump revision"))?;
        tx.commit().await.map_err(db("commit invocation start"))?;
        Ok(true)
    }

    /// Ghi tool-result message **và** đưa invocation tới `completed`/`failed` trong cùng một
    /// transaction (docs/05 §5.4). Nếu commit lỗi thì không có gì được xác nhận.
    /// `Ok(false)`: invocation/run không còn hợp lệ (không ghi đè).
    pub(crate) async fn settle_invocation(
        &self,
        run_id: &str,
        invocation_id: &str,
        settled: &Settled,
    ) -> Result<bool, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin settle"))?;
        let status = if settled.failed {
            "failed"
        } else {
            "completed"
        };
        let updated = sqlx::query(
            "UPDATE tool_invocations SET status = ?3, result_json = ?4, error_code = ?5, \
             finished_at = unixepoch() \
             WHERE id = ?1 AND run_id = ?2 AND status IN ('requested', 'running') \
             AND EXISTS (SELECT 1 FROM agent_runs WHERE id = ?2 AND status = 'running') \
             RETURNING session_id, tool_call_id",
        )
        .bind(invocation_id)
        .bind(run_id)
        .bind(status)
        .bind(&settled.result_json)
        .bind(&settled.error_code)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db("settle invocation"))?;
        let Some(row) = updated else {
            let _ = tx.rollback().await;
            return Ok(false);
        };
        let session_id: String = row.try_get("session_id").map_err(db("read session"))?;
        let call_id: String = row.try_get("tool_call_id").map_err(db("read call id"))?;
        let seq: i64 = sqlx::query_scalar(
            "UPDATE sessions SET next_sequence = next_sequence + 1, \
             state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = ?1 RETURNING next_sequence - 1",
        )
        .bind(&session_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db("claim result sequence"))?;
        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json, \
             tool_call_id, tool_invocation_id, run_id) \
             VALUES(?1, ?2, ?3, 'tool', ?4, ?5, ?6, ?7)",
        )
        .bind(new_id())
        .bind(&session_id)
        .bind(seq)
        .bind(
            serde_json::json!({ "text": settled.model_text, "is_error": settled.failed })
                .to_string(),
        )
        .bind(&call_id)
        .bind(invocation_id)
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("insert tool result"))?;
        tx.commit().await.map_err(db("commit tool result"))?;
        Ok(true)
    }

    /// Invocation chưa settle của run, theo đúng thứ tự provider.
    pub(crate) async fn pending_invocations(
        &self,
        run_id: &str,
    ) -> Result<Vec<PendingInvocation>, OrchestratorError> {
        let rows = sqlx::query(
            "SELECT id, tool_call_id, tool_name, arguments_json, status FROM tool_invocations \
             WHERE run_id = ?1 AND status IN ('requested', 'running') ORDER BY rowid",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db("load pending invocations"))?;
        rows.iter()
            .map(|r| {
                let args: String = r.try_get("arguments_json").map_err(db("read arguments"))?;
                let status: String = r.try_get("status").map_err(db("read status"))?;
                Ok(PendingInvocation {
                    id: r.try_get("id").map_err(db("read id"))?,
                    tool_call_id: r.try_get("tool_call_id").map_err(db("read call id"))?,
                    tool_name: r.try_get("tool_name").map_err(db("read tool name"))?,
                    arguments: serde_json::from_str(&args).unwrap_or(serde_json::json!({})),
                    started: status == "running",
                })
            })
            .collect()
    }

    /// Terminal không-completed (failed/timed_out/interrupted/cancelled). `Ok(false)`:
    /// run không còn `running`. Invocation chưa settle của run được đưa về terminal cùng
    /// transaction (không để `requested`/`running` treo) mà **không** giả outcome.
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
        sqlx::query(
            "UPDATE tool_invocations SET status = ?1, error_code = ?2, finished_at = unixepoch() \
             WHERE run_id = ?3 AND status IN ('requested', 'running')",
        )
        .bind(if status == RunStatus::Cancelled {
            "cancelled"
        } else {
            "interrupted"
        })
        .bind(error_code)
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("interrupt invocations"))?;
        tx.commit().await.map_err(db("commit terminal"))?;
        Ok(true)
    }

    /// Lúc build (đã giữ owner lock): run còn `running` từ process trước không có owner
    /// nào nữa. Đánh dấu `interrupted` (cả invocation dang dở) để session không kẹt;
    /// auto-resume/reconcile thuộc M5, **không replay** tool.
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
        sqlx::query(
            "UPDATE tool_invocations SET status = 'interrupted', error_code = 'INTERRUPTED', \
             finished_at = unixepoch() WHERE status IN ('requested', 'running')",
        )
        .execute(&mut *tx)
        .await
        .map_err(db("interrupt stale invocations"))?;
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
            "SELECT id, sequence, role, content_json, run_id, tool_call_id FROM messages \
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
                tool_call_id: row.try_get("tool_call_id").map_err(db("read call id"))?,
                tool_calls: calls_of(&json)
                    .into_iter()
                    .map(|c| MessageToolCall {
                        call_id: c.id,
                        name: c.name,
                    })
                    .collect(),
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

/// Cấp sequence kế tiếp cho session của một run còn `running`. `None`: run đã terminal.
async fn claim_sequence(
    tx: &mut sqlx::SqliteConnection,
    run_id: &str,
) -> Result<Option<(String, i64)>, OrchestratorError> {
    let row = sqlx::query(
        "UPDATE sessions SET next_sequence = next_sequence + 1, \
         state_revision = state_revision + 1, updated_at = unixepoch() \
         WHERE id = (SELECT session_id FROM agent_runs WHERE id = ?1 AND status = 'running') \
         RETURNING id, next_sequence - 1 AS seq",
    )
    .bind(run_id)
    .fetch_optional(tx)
    .await
    .map_err(db("claim sequence"))?;
    let Some(row) = row else { return Ok(None) };
    Ok(Some((
        row.try_get("id").map_err(db("read session"))?,
        row.try_get("seq").map_err(db("read sequence"))?,
    )))
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
