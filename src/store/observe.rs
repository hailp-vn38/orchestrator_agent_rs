//! Đọc phục vụ observation (docs/16 §16.7): thông tin để gắn revision/root vào event sau commit
//! và `family_snapshot` trong **một** read transaction nhất quán. Không ghi gì.

use sqlx::Row;

use super::{db, message_from_row, SessionStore};
use crate::error::{ErrorCode, OrchestratorError};
use crate::types::{
    CallerContext, FamilySnapshot, InvocationSnapshot, InvocationStatus, ResultError, RunSnapshot,
    RunStatus, SessionSnapshot,
};

/// Trần số child trong một snapshot (bounded; vượt thì `children_truncated`).
const MAX_SNAPSHOT_CHILDREN: i64 = 500;
/// Số session đọc: Root + child + 1 để phát hiện bị cắt.
const SNAPSHOT_SESSION_FETCH: i64 = MAX_SNAPSHOT_CHILDREN + 2;

/// Danh tính + revision hiện tại của session nguồn event, đọc sau commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EventInfo {
    pub session_id: String,
    /// Root của family (chính nó nếu là Root; MVP giới hạn depth 1).
    pub root_session_id: String,
    pub revision: i64,
}

/// Invocation bị đóng (`interrupted`/`cancelled`) cùng transaction terminal của run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClosedInvocation {
    pub id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub status: InvocationStatus,
    pub error_code: Option<String>,
}

fn event_info(row: &sqlx::sqlite::SqliteRow) -> Result<EventInfo, OrchestratorError> {
    Ok(EventInfo {
        session_id: row.try_get("id").map_err(db("read event session"))?,
        root_session_id: row.try_get("root").map_err(db("read event root"))?,
        revision: row.try_get("rev").map_err(db("read event revision"))?,
    })
}

impl SessionStore {
    pub(crate) async fn event_info_for_session(
        &self,
        session_id: &str,
    ) -> Result<Option<EventInfo>, OrchestratorError> {
        let row = sqlx::query(
            "SELECT id, COALESCE(parent_session_id, id) AS root, state_revision AS rev \
             FROM sessions WHERE id = ?1",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("load event info"))?;
        row.as_ref().map(event_info).transpose()
    }

    pub(crate) async fn event_info_for_run(
        &self,
        run_id: &str,
    ) -> Result<Option<EventInfo>, OrchestratorError> {
        let row = sqlx::query(
            "SELECT s.id AS id, COALESCE(s.parent_session_id, s.id) AS root, \
             s.state_revision AS rev \
             FROM agent_runs r JOIN sessions s ON s.id = r.session_id WHERE r.id = ?1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("load run event info"))?;
        row.as_ref().map(event_info).transpose()
    }

    /// Invocation của `run_id` đã bị đóng `interrupted`/`cancelled` (theo thứ tự tạo). Invocation
    /// được rehome sang attempt phục hồi nên không lẫn với attempt cũ.
    pub(crate) async fn closed_invocations(
        &self,
        run_id: &str,
    ) -> Result<Vec<ClosedInvocation>, OrchestratorError> {
        let rows = sqlx::query(
            "SELECT id, tool_call_id, tool_name, status, error_code FROM tool_invocations \
             WHERE run_id = ?1 AND status IN ('interrupted', 'cancelled') ORDER BY rowid",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db("load closed invocations"))?;
        rows.iter()
            .map(|r| {
                Ok(ClosedInvocation {
                    id: r.try_get("id").map_err(db("read invocation"))?,
                    tool_call_id: r.try_get("tool_call_id").map_err(db("read invocation"))?,
                    tool_name: r.try_get("tool_name").map_err(db("read invocation"))?,
                    status: {
                        let status: String = r.try_get("status").map_err(db("read invocation"))?;
                        InvocationStatus::parse(&status).ok_or_else(|| {
                            OrchestratorError::new(ErrorCode::Internal, "unknown invocation status")
                        })?
                    },
                    error_code: r.try_get("error_code").map_err(db("read invocation"))?,
                })
            })
            .collect()
    }

    /// Snapshot Root + Child trong một read transaction. Authorization (owner/workspace) mỗi
    /// lần gọi, cho cả Root lẫn Child; Child được tìm theo ancestry trong SQLite ở thời điểm
    /// đọc nên child mới tạo vẫn được thấy. `limit` bounded số message/invocation mỗi session.
    pub(crate) async fn family_snapshot(
        &self,
        caller: &CallerContext,
        root_id: &str,
        limit: u32,
    ) -> Result<FamilySnapshot, OrchestratorError> {
        // Authorization và đọc cùng một transaction: không có cửa sổ giữa kiểm quyền và dữ liệu.
        let mut tx = self.pool.begin().await.map_err(db("begin snapshot"))?;
        let scope = super::authorize_on(&mut tx, caller, root_id).await?;
        if scope.is_child {
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "observation is addressed by the root session of a family",
            ));
        }
        let rows = sqlx::query(
            "SELECT id, parent_session_id, agent_id, status, state_revision FROM sessions \
             WHERE (id = ?1 OR parent_session_id = ?1) AND owner_id = ?2 AND workspace_id = ?3 \
             ORDER BY (id = ?1) DESC, rowid LIMIT ?4",
        )
        .bind(root_id)
        .bind(&caller.owner_id)
        .bind(&caller.workspace_id)
        .bind(SNAPSHOT_SESSION_FETCH)
        .fetch_all(&mut *tx)
        .await
        .map_err(db("load family"))?;

        let mut sessions = Vec::with_capacity(rows.len());
        for row in &rows {
            let session_id: String = row.try_get("id").map_err(db("read session"))?;
            let status: String = row.try_get("status").map_err(db("read session"))?;
            let latest_run = latest_run(&mut tx, &session_id).await?;
            let recent_messages = recent_messages(&mut tx, &session_id, limit).await?;
            let invocations = recent_invocations(&mut tx, &session_id, limit).await?;
            sessions.push(SessionSnapshot {
                parent_session_id: row
                    .try_get("parent_session_id")
                    .map_err(db("read session"))?,
                agent_id: row.try_get("agent_id").map_err(db("read session"))?,
                archived: status == "archived",
                state_revision: row.try_get("state_revision").map_err(db("read session"))?,
                session_id,
                latest_run,
                recent_messages,
                invocations,
            });
        }
        tx.commit().await.map_err(db("end snapshot"))?;

        if sessions.is_empty() {
            return Err(OrchestratorError::new(
                ErrorCode::NotFound,
                "session not found",
            ));
        }
        let root = sessions.remove(0);
        let children_truncated = sessions.len() as i64 > MAX_SNAPSHOT_CHILDREN;
        sessions.truncate(MAX_SNAPSHOT_CHILDREN as usize);
        Ok(FamilySnapshot {
            root,
            children: sessions,
            children_truncated,
        })
    }
}

async fn latest_run(
    tx: &mut sqlx::SqliteConnection,
    session_id: &str,
) -> Result<Option<RunSnapshot>, OrchestratorError> {
    let row = sqlx::query(
        "SELECT id, turn_id, resume_attempt, status, steps, input_tokens, output_tokens, \
         error_code, error_message FROM agent_runs WHERE session_id = ?1 \
         ORDER BY rowid DESC LIMIT 1",
    )
    .bind(session_id)
    .fetch_optional(tx)
    .await
    .map_err(db("load latest run"))?;
    let Some(row) = row else { return Ok(None) };
    let status: String = row.try_get("status").map_err(db("read run"))?;
    let code: Option<String> = row.try_get("error_code").map_err(db("read run"))?;
    let message: Option<String> = row.try_get("error_message").map_err(db("read run"))?;
    let attempt: i64 = row.try_get("resume_attempt").map_err(db("read run"))?;
    let steps: i64 = row.try_get("steps").map_err(db("read run"))?;
    let input: i64 = row.try_get("input_tokens").map_err(db("read run"))?;
    let output: i64 = row.try_get("output_tokens").map_err(db("read run"))?;
    Ok(Some(RunSnapshot {
        run_id: row.try_get("id").map_err(db("read run"))?,
        turn_id: row.try_get("turn_id").map_err(db("read run"))?,
        attempt: u32::try_from(attempt).unwrap_or(u32::MAX),
        status: RunStatus::parse(&status)
            .ok_or_else(|| OrchestratorError::new(ErrorCode::Internal, "unknown run status"))?,
        steps: u32::try_from(steps).unwrap_or(u32::MAX),
        input_tokens: u64::try_from(input).unwrap_or_default(),
        output_tokens: u64::try_from(output).unwrap_or_default(),
        error: code.map(|code| ResultError {
            code,
            message: message.unwrap_or_default(),
        }),
    }))
}

async fn recent_messages(
    tx: &mut sqlx::SqliteConnection,
    session_id: &str,
    limit: u32,
) -> Result<Vec<crate::types::MessageView>, OrchestratorError> {
    let rows = sqlx::query(
        "SELECT id, sequence, role, content_json, run_id, tool_call_id FROM messages \
         WHERE session_id = ?1 AND role IN ('user', 'assistant', 'tool') \
         ORDER BY sequence DESC LIMIT ?2",
    )
    .bind(session_id)
    .bind(i64::from(limit))
    .fetch_all(tx)
    .await
    .map_err(db("load recent messages"))?;
    let mut messages = rows
        .iter()
        .map(message_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    messages.reverse();
    Ok(messages)
}

async fn recent_invocations(
    tx: &mut sqlx::SqliteConnection,
    session_id: &str,
    limit: u32,
) -> Result<Vec<InvocationSnapshot>, OrchestratorError> {
    let rows = sqlx::query(
        "SELECT id, run_id, tool_call_id, tool_name, status, child_session_id, error_code \
         FROM tool_invocations WHERE session_id = ?1 ORDER BY rowid DESC LIMIT ?2",
    )
    .bind(session_id)
    .bind(i64::from(limit))
    .fetch_all(tx)
    .await
    .map_err(db("load recent invocations"))?;
    let mut items = rows
        .iter()
        .map(|r| {
            let status: String = r.try_get("status").map_err(db("read invocation"))?;
            Ok(InvocationSnapshot {
                invocation_id: r.try_get("id").map_err(db("read invocation"))?,
                run_id: r.try_get("run_id").map_err(db("read invocation"))?,
                tool_call_id: r.try_get("tool_call_id").map_err(db("read invocation"))?,
                tool_name: r.try_get("tool_name").map_err(db("read invocation"))?,
                status: InvocationStatus::parse(&status).ok_or_else(|| {
                    OrchestratorError::new(ErrorCode::Internal, "unknown invocation status")
                })?,
                child_session_id: r
                    .try_get("child_session_id")
                    .map_err(db("read invocation"))?,
                error_code: r.try_get("error_code").map_err(db("read invocation"))?,
            })
        })
        .collect::<Result<Vec<_>, OrchestratorError>>()?;
    items.reverse();
    Ok(items)
}
