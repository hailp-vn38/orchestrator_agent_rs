//! `SessionStore`: sở hữu toàn bộ query và transaction invariants của execution store
//! (docs/09). Không await LLM/tool trong transaction.
//!
//! Mọi write-transaction bắt đầu bằng một câu lệnh ghi (không đọc trước) để lấy write lock
//! ngay và tránh `SQLITE_BUSY_SNAPSHOT` khi nâng cấp từ read snapshot trong WAL.

use std::path::Path;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

mod observe;
mod recovery;
pub(crate) use observe::EventInfo;
pub(crate) use recovery::{RecoveryPolicy, Resolution, CODE_NO_EFFECT, CODE_SHUTTING_DOWN};

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
/// Timeout chờ connection từ pool. Với feature `test-support` clock tokio có thể bị pause
/// (AT-11): clock ảo nhảy tới timer gần nhất mỗi khi runtime chờ thread của SQLite, nên
/// timer này phải luôn là timer xa nhất để không gây lỗi giả.
#[cfg(not(feature = "test-support"))]
const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(feature = "test-support")]
const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(24 * 3600);
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
    pub run_id: String,
    pub session_id: String,
    pub agent_id: String,
    pub provider_id: String,
    pub model_id: String,
    pub status: RunStatus,
    pub owner_id: String,
    pub workspace_id: String,
    pub policy: SessionPolicy,
    /// `Some` khi run thuộc child session (depth 1): child không được ủy quyền tiếp.
    pub parent_session_id: Option<String>,
}

impl RunRecord {
    /// Root của family (depth 1): parent nếu là child, ngược lại chính session.
    pub(crate) fn root_session_id(&self) -> &str {
        self.parent_session_id
            .as_deref()
            .unwrap_or(&self.session_id)
    }
}

/// Đích của một child turn: tạo child mới hoặc tiếp tục child cũ của chính parent.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ChildTarget<'a> {
    New {
        agent_id: &'a str,
    },
    Resume {
        session_id: &'a str,
        agent_id: &'a str,
    },
}

/// Child turn đã admit (hoặc đã admit trước đó cho cùng parent invocation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChildAdmission {
    pub session_id: String,
    pub turn_id: String,
    /// `false`: replay idempotent của một admission đã commit; không tạo thêm gì.
    pub created: bool,
}

/// Lý do admission child bị từ chối. Mọi nhánh từ chối đều rollback toàn bộ transaction.
#[derive(Debug)]
pub(crate) enum ChildAdmitError {
    /// Invocation/run của parent không còn hợp lệ (đã terminal hoặc không phải `subagent`).
    ParentUnavailable,
    /// Không phải child của parent này (không tồn tại, parent khác, owner/workspace khác).
    NotChild,
    Archived,
    AgentMismatch,
    DepthLimit,
    Busy,
    /// Child session có turn mới nhất đang chờ operator reconcile (M5).
    Blocked,
    RouteUnavailable,
    /// Lỗi lưu trữ; chi tiết đã ghi tracing ở `OrchestratorError::storage`.
    Store,
}

impl From<OrchestratorError> for ChildAdmitError {
    fn from(_: OrchestratorError) -> Self {
        Self::Store
    }
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

/// Checkpoint compaction mới nhất của session (docs/09 §9.8). Transcript gốc không bị xóa;
/// model context = summary + các entry có `sequence >= keep_from_sequence`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    pub summary: String,
    pub keep_from_sequence: u64,
    /// Yêu cầu của user cho turn đang chạy, chỉ có khi message gốc nằm trong phần đã nén.
    pub objective: Option<String>,
}

/// Dữ liệu để ghi một checkpoint mới.
#[derive(Debug, Clone)]
pub(crate) struct NewCheckpoint {
    pub summary: String,
    pub keep_from_sequence: u64,
    pub objective: Option<String>,
    /// `provider/model` đã sinh summary (không chứa secret).
    pub model_ref: String,
}

/// Context của một session đã áp checkpoint mới nhất: entry kèm `sequence`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Context {
    pub checkpoint: Option<Checkpoint>,
    pub entries: Vec<(u64, TranscriptEntry)>,
}

/// Version của encoding checkpoint trong `messages.content_json`.
const CHECKPOINT_VERSION: u32 = 1;

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

/// JSON của policy để lưu vào `sessions.permissions_json`; `None` khi không thu hẹp gì.
fn policy_json(policy: &SessionPolicy) -> Option<String> {
    (policy != &SessionPolicy::default()).then(|| {
        serde_json::json!({
            "allowed_tools": policy.allowed_tools,
            "allowed_skills": policy.allowed_skills,
            "child_allowed_tools": policy.child_allowed_tools,
            "child_allowed_skills": policy.child_allowed_skills,
        })
        .to_string()
    })
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
            child_allowed_tools: list(&v, "child_allowed_tools"),
            child_allowed_skills: list(&v, "child_allowed_skills"),
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
            .acquire_timeout(POOL_ACQUIRE_TIMEOUT)
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
        let permissions = policy_json(policy);
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
             AND parent_session_id IS NULL \
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
        // Turn mới nhất đang chờ reconcile outcome không xác định: không được đi tiếp tới
        // model request kế tiếp (docs/18 §18.4.2 `RECOVERY_BLOCKED`).
        if recovery::session_awaits_reconciliation(&mut tx, session_id).await? {
            let _ = tx.rollback().await;
            return Err(OrchestratorError::new(
                ErrorCode::RecoveryBlocked,
                "the session has an interrupted tool call whose outcome must be resolved first",
            ));
        }
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

    /// Một transaction ngắn (docs/06 §6.3): verify ownership/ancestry, append child input,
    /// claim child run và persist `child_session_id/child_turn_id` lên parent invocation.
    /// `tool_invocations.id` là idempotency key: gọi lại cho cùng invocation trả đúng turn đã
    /// admit (kể cả khi child đã terminal), không append/claim thêm. Invocation mới luôn tạo
    /// logical turn mới dù prompt giống hệt. Không await LLM trong transaction.
    pub(crate) async fn admit_child_turn(
        &self,
        parent_invocation_id: &str,
        target: ChildTarget<'_>,
        prompt: &str,
        resolve: impl Fn(&str) -> Option<RunRoute>,
    ) -> Result<ChildAdmission, ChildAdmitError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(db("begin child admission"))?;
        match admit_child_inner(&mut tx, parent_invocation_id, target, prompt, resolve).await {
            Ok(admission) => {
                tx.commit().await.map_err(db("commit child admission"))?;
                Ok(admission)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }

    /// Lý do admission/ghi bị từ chối, phân loại theo scope trước rồi tới trạng thái.
    async fn explain_rejection(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> OrchestratorError {
        match self.authorize(caller, session_id).await {
            Err(e) => e,
            // Child chỉ nhận input qua `subagent` của parent (có link + idempotency key).
            Ok(s) if s.is_child => OrchestratorError::new(
                ErrorCode::PermissionDenied,
                "a child session only accepts input through its parent",
            ),
            Ok(s) if s.archived => {
                OrchestratorError::new(ErrorCode::SessionArchived, "the session is archived")
            }
            Ok(_) => OrchestratorError::new(ErrorCode::Internal, "turn admission was rejected"),
        }
    }

    /// Kiểm tra session tồn tại và nằm trong scope của caller (không đọc/ghi gì thêm).
    pub(crate) async fn check_scope(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<(), OrchestratorError> {
        self.authorize(caller, session_id).await.map(|_| ())
    }

    /// Như [`Self::check_scope`] nhưng session phải là Root của một family (observation).
    pub(crate) async fn check_root_scope(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<(), OrchestratorError> {
        if self.authorize(caller, session_id).await?.is_child {
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "observation is addressed by the root session of a family",
            ));
        }
        Ok(())
    }

    /// Kiểm tra owner/workspace; dùng cho mọi read công khai.
    async fn authorize(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<SessionScope, OrchestratorError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(db("acquire connection"))?;
        authorize_on(&mut conn, caller, session_id).await
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
             s.owner_id, s.workspace_id, s.permissions_json, s.parent_session_id \
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
            run_id: run_id.to_owned(),
            session_id: row.try_get("session_id").map_err(db("read run"))?,
            agent_id: row.try_get("agent_id").map_err(db("read run"))?,
            provider_id: row.try_get("provider_id").map_err(db("read run"))?,
            model_id: row.try_get("model_id").map_err(db("read run"))?,
            status: RunStatus::parse(&status)
                .ok_or_else(|| OrchestratorError::new(ErrorCode::Internal, "unknown run status"))?,
            owner_id: row.try_get("owner_id").map_err(db("read run"))?,
            workspace_id: row.try_get("workspace_id").map_err(db("read run"))?,
            policy: parse_policy(permissions.as_deref()),
            parent_session_id: row
                .try_get("parent_session_id")
                .map_err(db("read run parent"))?,
        }))
    }

    /// Context của session: checkpoint compaction mới nhất (nếu có) và các entry
    /// user/assistant/tool từ `keep_from_sequence` trở đi theo thứ tự `sequence`.
    pub(crate) async fn conversation(
        &self,
        session_id: &str,
    ) -> Result<Context, OrchestratorError> {
        let checkpoint = self.latest_checkpoint(session_id).await?;
        let from = checkpoint.as_ref().map_or(0, |c| c.keep_from_sequence);
        let rows = sqlx::query(
            "SELECT m.sequence, m.role, m.content_json, m.tool_call_id, ti.tool_name \
             FROM messages m LEFT JOIN tool_invocations ti ON ti.id = m.tool_invocation_id \
             WHERE m.session_id = ?1 AND m.sequence >= ?2 \
             AND m.role IN ('user', 'assistant', 'tool') \
             ORDER BY m.sequence",
        )
        .bind(session_id)
        .bind(from as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db("load conversation"))?;
        let entries = rows
            .iter()
            .map(|r| {
                let role: String = r.try_get("role").map_err(db("read role"))?;
                let seq: i64 = r.try_get("sequence").map_err(db("read sequence"))?;
                let json: String = r.try_get("content_json").map_err(db("read content"))?;
                let entry = match role.as_str() {
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
                };
                Ok((seq as u64, entry))
            })
            .collect::<Result<Vec<_>, OrchestratorError>>()?;
        Ok(Context {
            checkpoint,
            entries,
        })
    }

    async fn latest_checkpoint(
        &self,
        session_id: &str,
    ) -> Result<Option<Checkpoint>, OrchestratorError> {
        let json: Option<String> = sqlx::query_scalar(
            "SELECT content_json FROM messages WHERE session_id = ?1 AND role = 'compaction' \
             ORDER BY sequence DESC LIMIT 1",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("load checkpoint"))?;
        Ok(json.and_then(|j| {
            let parsed = serde_json::from_str::<serde_json::Value>(&j).ok();
            if parsed.is_none() {
                tracing::warn!("latest compaction checkpoint is unreadable; ignoring it");
            }
            let v = parsed?;
            Some(Checkpoint {
                summary: v.get("summary")?.as_str()?.to_owned(),
                keep_from_sequence: v.get("keep_from_sequence")?.as_u64()?,
                objective: v
                    .get("objective")
                    .and_then(|o| o.as_str())
                    .map(str::to_owned),
            })
        }))
    }

    /// Ghi checkpoint compaction của session mà run đang chạy (một transaction, một sequence).
    /// Original messages không bị đụng tới. `Ok(false)`: run không còn `running`, hoặc
    /// frontier không tiến so với checkpoint trước / vượt quá message cuối.
    pub(crate) async fn append_compaction(
        &self,
        run_id: &str,
        checkpoint: &NewCheckpoint,
    ) -> Result<bool, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin compaction"))?;
        let Some((session_id, seq)) = claim_sequence(&mut tx, run_id).await? else {
            let _ = tx.rollback().await;
            return Ok(false);
        };
        let previous: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(CAST(json_extract(content_json, '$.keep_from_sequence') AS INTEGER)) \
             FROM messages WHERE session_id = ?1 AND role = 'compaction'",
        )
        .bind(&session_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db("read previous frontier"))?;
        let frontier = checkpoint.keep_from_sequence as i64;
        if frontier <= previous.unwrap_or(0) || frontier >= seq {
            let _ = tx.rollback().await;
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json, run_id) \
             VALUES(?1, ?2, ?3, 'compaction', ?4, ?5)",
        )
        .bind(new_id())
        .bind(&session_id)
        .bind(seq)
        .bind(
            serde_json::json!({
                "text": checkpoint.summary,
                "summary": checkpoint.summary,
                "keep_from_sequence": checkpoint.keep_from_sequence,
                "objective": checkpoint.objective,
                "version": CHECKPOINT_VERSION,
                "model_ref": checkpoint.model_ref,
            })
            .to_string(),
        )
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("insert compaction"))?;
        tx.commit().await.map_err(db("commit compaction"))?;
        Ok(true)
    }

    /// Số step đã commit của run (assistant message có tool call hoặc final).
    pub(crate) async fn run_steps(&self, run_id: &str) -> Result<u32, OrchestratorError> {
        let steps: Option<i64> = sqlx::query_scalar("SELECT steps FROM agent_runs WHERE id = ?1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db("read run steps"))?;
        Ok(steps.unwrap_or(0).max(0) as u32)
    }

    /// Cộng usage provider báo (chỉ số đo được, không ước lượng) vào run đang chạy.
    pub(crate) async fn add_usage(
        &self,
        run_id: &str,
        input_tokens: u64,
        output_tokens: u64,
    ) -> Result<(), OrchestratorError> {
        sqlx::query(
            "UPDATE agent_runs SET input_tokens = input_tokens + ?2, \
             output_tokens = output_tokens + ?3 WHERE id = ?1 AND status = 'running'",
        )
        .bind(run_id)
        .bind(input_tokens.min(i64::MAX as u64) as i64)
        .bind(output_tokens.min(i64::MAX as u64) as i64)
        .execute(&self.pool)
        .await
        .map_err(db("add usage"))?;
        Ok(())
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
    ///
    /// `interrupted` là trạng thái có thể phục hồi (M5): invocation `running` (outcome chưa
    /// biết) thành `interrupted`, còn invocation `requested` (chưa từng chạy) được **giữ**
    /// `requested` để attempt phục hồi kế tiếp chạy chúng, không mất.
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
        // `requested` chỉ được giữ cho run `interrupted` (có thể resume); mọi terminal khác
        // đưa chúng về terminal vì run đó sẽ không chạy lại.
        sqlx::query(
            "UPDATE tool_invocations SET status = ?1, error_code = ?2, finished_at = unixepoch() \
             WHERE run_id = ?3 AND (status = 'running' OR (status = 'requested' AND ?4))",
        )
        .bind(if status == RunStatus::Cancelled {
            "cancelled"
        } else {
            "interrupted"
        })
        .bind(error_code)
        .bind(run_id)
        .bind(status != RunStatus::Interrupted)
        .execute(&mut *tx)
        .await
        .map_err(db("interrupt invocations"))?;
        tx.commit().await.map_err(db("commit terminal"))?;
        Ok(true)
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
            items.push(message_from_row(row)?);
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
        self.load_result(session_id, turn_id).await
    }

    /// Như [`Self::result`] nhưng không kiểm scope: chỉ dùng nội bộ cho parent invocation đã
    /// được admit (child do runtime tạo, đã kiểm owner/workspace/ancestry lúc admission).
    pub(crate) async fn load_result(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<AgentResult, OrchestratorError> {
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

/// Thân của [`SessionStore::admit_child_turn`]; caller commit/rollback. Câu lệnh đầu tiên là
/// một ghi no-op để lấy write lock ngay (tránh `SQLITE_BUSY_SNAPSHOT`) và đọc link hiện có.
async fn admit_child_inner(
    conn: &mut sqlx::SqliteConnection,
    invocation_id: &str,
    target: ChildTarget<'_>,
    prompt: &str,
    resolve: impl Fn(&str) -> Option<RunRoute>,
) -> Result<ChildAdmission, ChildAdmitError> {
    let link = sqlx::query(
        "UPDATE tool_invocations SET status = status WHERE id = ?1 \
         RETURNING session_id, run_id, tool_name, child_session_id, child_turn_id",
    )
    .bind(invocation_id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db("lock parent invocation"))?;
    let Some(link) = link else {
        return Err(ChildAdmitError::ParentUnavailable);
    };
    let tool_name: String = link.try_get("tool_name").map_err(db("read tool name"))?;
    let parent_session: String = link.try_get("session_id").map_err(db("read session"))?;
    let run_id: String = link.try_get("run_id").map_err(db("read run"))?;
    let linked_session: Option<String> = link
        .try_get("child_session_id")
        .map_err(db("read child session"))?;
    let linked_turn: Option<String> = link
        .try_get("child_turn_id")
        .map_err(db("read child turn"))?;
    if tool_name != "subagent" {
        return Err(ChildAdmitError::ParentUnavailable);
    }
    if let (Some(session_id), Some(turn_id)) = (linked_session, linked_turn) {
        return Ok(ChildAdmission {
            session_id,
            turn_id,
            created: false,
        });
    }

    let running: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM agent_runs WHERE id = ?1 AND status = 'running'")
            .bind(&run_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db("check parent run"))?;
    if running.is_none() {
        return Err(ChildAdmitError::ParentUnavailable);
    }
    let parent = sqlx::query(
        "SELECT parent_session_id, owner_id, workspace_id, permissions_json \
         FROM sessions WHERE id = ?1",
    )
    .bind(&parent_session)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db("load parent session"))?
    .ok_or(ChildAdmitError::ParentUnavailable)?;
    let grandparent: Option<String> = parent
        .try_get("parent_session_id")
        .map_err(db("read parent link"))?;
    if grandparent.is_some() {
        return Err(ChildAdmitError::DepthLimit);
    }
    let owner: String = parent.try_get("owner_id").map_err(db("read owner"))?;
    let workspace: String = parent
        .try_get("workspace_id")
        .map_err(db("read workspace"))?;
    let parent_permissions: Option<String> = parent
        .try_get("permissions_json")
        .map_err(db("read policy"))?;
    // Quyền của child = AgentDefinition(child) ∩ trần do host đặt cho child ở root session;
    // policy dành cho chính Root không áp xuống child.
    let parent_policy = parse_policy(parent_permissions.as_deref());
    let permissions = policy_json(&SessionPolicy {
        allowed_tools: parent_policy.child_allowed_tools,
        allowed_skills: parent_policy.child_allowed_skills,
        ..SessionPolicy::default()
    });

    let (child_session, agent_id) = match target {
        ChildTarget::New { agent_id } => {
            let id = new_id();
            // Child chỉ mang trần quyền host đặt cho child (không kế thừa policy của Root).
            sqlx::query(
                "INSERT INTO sessions(id, parent_session_id, agent_id, owner_id, workspace_id, \
                 permissions_json) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .bind(&id)
            .bind(&parent_session)
            .bind(agent_id)
            .bind(&owner)
            .bind(&workspace)
            .bind(&permissions)
            .execute(&mut *conn)
            .await
            .map_err(db("create child session"))?;
            (id, agent_id.to_owned())
        }
        ChildTarget::Resume {
            session_id,
            agent_id,
        } => {
            let row = sqlx::query(
                "SELECT agent_id, parent_session_id, owner_id, workspace_id, status \
                 FROM sessions WHERE id = ?1",
            )
            .bind(session_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db("load child session"))?;
            // Không phân biệt "không tồn tại" với "của parent/owner khác": không lộ sự tồn tại.
            let Some(row) = row else {
                return Err(ChildAdmitError::NotChild);
            };
            let row_parent: Option<String> = row
                .try_get("parent_session_id")
                .map_err(db("read child parent"))?;
            let row_owner: String = row.try_get("owner_id").map_err(db("read owner"))?;
            let row_workspace: String =
                row.try_get("workspace_id").map_err(db("read workspace"))?;
            if row_parent.as_deref() != Some(parent_session.as_str())
                || row_owner != owner
                || row_workspace != workspace
            {
                return Err(ChildAdmitError::NotChild);
            }
            let status: String = row.try_get("status").map_err(db("read status"))?;
            if status == "archived" {
                return Err(ChildAdmitError::Archived);
            }
            let child_agent: String = row.try_get("agent_id").map_err(db("read agent"))?;
            if child_agent != agent_id {
                return Err(ChildAdmitError::AgentMismatch);
            }
            if recovery::session_awaits_reconciliation(&mut *conn, session_id).await? {
                return Err(ChildAdmitError::Blocked);
            }
            (session_id.to_owned(), child_agent)
        }
    };

    let route = resolve(&agent_id).ok_or(ChildAdmitError::RouteUnavailable)?;
    let seq: i64 = sqlx::query_scalar(
        "UPDATE sessions SET next_sequence = next_sequence + 1, \
         state_revision = state_revision + 1, updated_at = unixepoch() \
         WHERE id = ?1 RETURNING next_sequence - 1",
    )
    .bind(&child_session)
    .fetch_one(&mut *conn)
    .await
    .map_err(db("claim child sequence"))?;
    let turn_id = new_id();
    let inserted = sqlx::query(
        "INSERT INTO agent_runs(id, session_id, agent_id, turn_id, resume_attempt, \
         provider_id, model_id, provider_config_fingerprint, status) \
         VALUES(?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, 'running')",
    )
    .bind(new_id())
    .bind(&child_session)
    .bind(&agent_id)
    .bind(&turn_id)
    .bind(&route.provider_id)
    .bind(&route.model_id)
    .bind(&route.fingerprint)
    .execute(&mut *conn)
    .await;
    match inserted {
        Ok(_) => {}
        Err(sqlx::Error::Database(d)) if d.is_unique_violation() => {
            return Err(ChildAdmitError::Busy);
        }
        Err(e) => return Err(db("claim child run")(e).into()),
    }
    sqlx::query(
        "INSERT INTO messages(id, session_id, sequence, role, content_json) \
         VALUES(?1, ?2, ?3, 'user', ?4)",
    )
    .bind(new_id())
    .bind(&child_session)
    .bind(seq)
    .bind(text_json(prompt))
    .execute(&mut *conn)
    .await
    .map_err(db("insert child input"))?;
    sqlx::query(
        "UPDATE tool_invocations SET child_session_id = ?2, child_turn_id = ?3 WHERE id = ?1",
    )
    .bind(invocation_id)
    .bind(&child_session)
    .bind(&turn_id)
    .execute(&mut *conn)
    .await
    .map_err(db("link child turn"))?;
    // Link mới là state parent-visible: bump revision của parent đúng một lần.
    sqlx::query(
        "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
         WHERE id = ?1",
    )
    .bind(&parent_session)
    .execute(&mut *conn)
    .await
    .map_err(db("bump parent revision"))?;
    Ok(ChildAdmission {
        session_id: child_session,
        turn_id,
        created: true,
    })
}

/// Cột cần có: `id, sequence, role, content_json, run_id, tool_call_id`.
fn message_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<MessageView, OrchestratorError> {
    let role: String = row.try_get("role").map_err(db("read role"))?;
    let json: String = row.try_get("content_json").map_err(db("read content"))?;
    let sequence: i64 = row.try_get("sequence").map_err(db("read sequence"))?;
    Ok(MessageView {
        id: row.try_get("id").map_err(db("read id"))?,
        sequence: sequence as u64,
        role: MessageRole::parse(&role)
            .ok_or_else(|| OrchestratorError::new(ErrorCode::Internal, "unknown message role"))?,
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
    })
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

/// Kiểm tra owner/workspace trên một connection cho trước (để dùng trong read transaction).
async fn authorize_on(
    conn: &mut sqlx::SqliteConnection,
    caller: &CallerContext,
    session_id: &str,
) -> Result<SessionScope, OrchestratorError> {
    let row = sqlx::query(
        "SELECT owner_id, workspace_id, status, agent_id, parent_session_id \
         FROM sessions WHERE id = ?1",
    )
    .bind(session_id)
    .fetch_optional(&mut *conn)
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
    let parent: Option<String> = row
        .try_get("parent_session_id")
        .map_err(db("read parent"))?;
    Ok(SessionScope {
        archived: status == "archived",
        is_child: parent.is_some(),
    })
}

struct SessionScope {
    archived: bool,
    is_child: bool,
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
        let child_msgs: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE role = 'user'")
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
}
