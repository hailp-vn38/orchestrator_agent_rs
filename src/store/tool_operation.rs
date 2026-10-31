//! Quản lý bản ghi durable `tool_operations` gắn với từng `tool_invocations.id`.

use sqlx::Row;

use super::codec::{db, new_id};
use super::SessionStore;
use crate::artifact::{CODE_ARTIFACT_PERMISSION_DENIED, CODE_ARTIFACT_UNAVAILABLE};
use crate::error::OrchestratorError;
use crate::tool::{
    OperationUpdate, ToolError, ToolOperation, ToolOperationStatus, ToolOperationSummary, ToolScope,
};

fn decode_operation_row(row: &sqlx::sqlite::SqliteRow) -> Result<ToolOperation, sqlx::Error> {
    let status_raw: String = row.try_get("status")?;
    let status = ToolOperationStatus::parse(&status_raw).unwrap_or(ToolOperationStatus::Failed);
    let batch_index: i64 = row.try_get("batch_index")?;
    let repairs_used: i64 = row.try_get("repairs_used")?;
    let user_turns_before: Option<i64> = row.try_get("user_turns_before")?;
    Ok(ToolOperation {
        id: row.try_get("id")?,
        invocation_id: row.try_get("invocation_id")?,
        provider_ref: row.try_get("provider_ref")?,
        account_key: row.try_get("account_key")?,
        remote_session_id: row.try_get("remote_session_id")?,
        continuation_key: row.try_get("continuation_key")?,
        batch_index: u32::try_from(batch_index.max(0)).unwrap_or(0),
        repairs_used: u32::try_from(repairs_used.max(0)).unwrap_or(0),
        batch_paths_json: row.try_get("batch_paths_json")?,
        user_turns_before: user_turns_before.and_then(|n| u32::try_from(n.max(0)).ok()),
        request_json: row.try_get("request_json")?,
        status,
        artifact_ids_json: row.try_get("artifact_ids_json")?,
        result_json: row.try_get("result_json")?,
        remote_deleted_at: row.try_get("remote_deleted_at")?,
        last_activity_at: row.try_get("last_activity_at")?,
        deadline_at: row.try_get("deadline_at")?,
        created_at: row.try_get("created_at")?,
    })
}

impl SessionStore {
    async fn verify_invocation_scope(&self, scope: &ToolScope) -> Result<(), ToolError> {
        let ok: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM tool_invocations ti \
             JOIN sessions s ON s.id = ti.session_id \
             WHERE ti.id = ?1 AND ti.session_id = ?2 \
               AND s.owner_id = ?3 AND s.workspace_id = ?4",
        )
        .bind(&scope.invocation_id)
        .bind(&scope.session_id)
        .bind(&scope.owner_id)
        .bind(&scope.workspace_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not verify tool invocation scope".into(),
        })?;
        if ok.is_none() {
            return Err(ToolError {
                code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
                message: "invocation is outside the authorized scope".into(),
            });
        }
        Ok(())
    }

    pub(crate) async fn current_operation(
        &self,
        scope: &ToolScope,
    ) -> Result<Option<ToolOperation>, ToolError> {
        self.verify_invocation_scope(scope).await?;
        let row = sqlx::query(
            "SELECT id, invocation_id, provider_ref, account_key, remote_session_id, \
             continuation_key, batch_index, repairs_used, batch_paths_json, user_turns_before, \
             request_json, status, artifact_ids_json, result_json, remote_deleted_at, \
             last_activity_at, deadline_at, created_at \
             FROM tool_operations WHERE invocation_id = ?1",
        )
        .bind(&scope.invocation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not read tool operation".into(),
        })?;
        match row {
            Some(ref r) => decode_operation_row(r).map(Some).map_err(|_| ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not decode tool operation".into(),
            }),
            None => Ok(None),
        }
    }

    pub(crate) async fn begin_operation(
        &self,
        scope: &ToolScope,
        deadline_at: i64,
        provider_ref: &str,
    ) -> Result<ToolOperation, ToolError> {
        self.verify_invocation_scope(scope).await?;
        let now = self.now_secs();
        let op_id = new_id();
        let cont_key =
            (!scope.root_session_id.trim().is_empty()).then(|| scope.root_session_id.clone());
        sqlx::query(
            "INSERT INTO tool_operations(\
                id, invocation_id, provider_ref, continuation_key, status, \
                last_activity_at, deadline_at, created_at\
             ) VALUES(?1, ?2, ?3, ?4, 'submitting', ?5, ?6, ?5) \
             ON CONFLICT(invocation_id) DO NOTHING",
        )
        .bind(&op_id)
        .bind(&scope.invocation_id)
        .bind(provider_ref)
        .bind(&cont_key)
        .bind(now)
        .bind(deadline_at)
        .execute(&self.pool)
        .await
        .map_err(|_| ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not begin tool operation".into(),
        })?;
        self.current_operation(scope)
            .await?
            .ok_or_else(|| ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "tool operation missing after insert".into(),
            })
    }

    pub(crate) async fn advance_operation(
        &self,
        scope: &ToolScope,
        update: &OperationUpdate,
    ) -> Result<(), ToolError> {
        self.verify_invocation_scope(scope).await?;
        let now = self.now_secs();
        let status_str = update.status.map(ToolOperationStatus::as_str);
        let batch_index = update.batch_index.map(i64::from);
        let repairs_used = update.repairs_used.map(i64::from);
        let user_turns_before = update.user_turns_before.map(i64::from);
        let changed = sqlx::query(
            "UPDATE tool_operations SET \
                status = COALESCE(?2, status), \
                provider_ref = COALESCE(?3, provider_ref), \
                account_key = COALESCE(?4, account_key), \
                remote_session_id = COALESCE(?5, remote_session_id), \
                continuation_key = COALESCE(?6, continuation_key), \
                batch_index = COALESCE(?7, batch_index), \
                repairs_used = COALESCE(?8, repairs_used), \
                batch_paths_json = COALESCE(?9, batch_paths_json), \
                user_turns_before = COALESCE(?10, user_turns_before), \
                request_json = COALESCE(?11, request_json), \
                artifact_ids_json = COALESCE(?12, artifact_ids_json), \
                result_json = COALESCE(?13, result_json), \
                remote_deleted_at = COALESCE(?14, remote_deleted_at), \
                last_activity_at = ?15 \
             WHERE invocation_id = ?1",
        )
        .bind(&scope.invocation_id)
        .bind(status_str)
        .bind(update.provider_ref.as_deref())
        .bind(update.account_key.as_deref())
        .bind(update.remote_session_id.as_deref())
        .bind(update.continuation_key.as_deref())
        .bind(batch_index)
        .bind(repairs_used)
        .bind(update.batch_paths_json.as_deref())
        .bind(user_turns_before)
        .bind(update.request_json.as_deref())
        .bind(update.artifact_ids_json.as_deref())
        .bind(update.result_json.as_deref())
        .bind(update.remote_deleted_at)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|_| ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not update tool operation".into(),
        })?
        .rows_affected();
        if changed == 0 {
            return Err(ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "tool operation does not exist for invocation".into(),
            });
        }
        Ok(())
    }

    pub(crate) async fn list_tool_operations(
        &self,
    ) -> Result<Vec<ToolOperationSummary>, OrchestratorError> {
        let rows = sqlx::query(&format!(
            "SELECT op.id, op.invocation_id, op.provider_ref, op.account_key, \
                    op.remote_session_id, op.continuation_key, op.batch_index, op.repairs_used, \
                    op.batch_paths_json, op.user_turns_before, op.request_json, op.status, \
                    op.artifact_ids_json, op.result_json, op.remote_deleted_at, \
                    op.last_activity_at, op.deadline_at, op.created_at, \
                    ti.status AS invocation_status, \
                    COALESCE(s.parent_session_id, s.id) AS root_session_id, \
                    EXISTS ( \
                        SELECT 1 FROM agent_runs r \
                        JOIN sessions fs ON fs.id = r.session_id \
                        WHERE (fs.id = COALESCE(s.parent_session_id, s.id) \
                               OR fs.parent_session_id = COALESCE(s.parent_session_id, s.id)) \
                          AND (r.status IN ('running', 'waiting_retry', 'waiting_user') OR ({recoverable})) \
                    ) AS root_has_active_work \
             FROM tool_operations op \
             JOIN tool_invocations ti ON ti.id = op.invocation_id \
             JOIN sessions s ON s.id = ti.session_id \
             ORDER BY op.rowid ASC",
            recoverable = super::recovery::candidate_filter(),
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(db("list tool operations"))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let operation = decode_operation_row(&row).map_err(db("decode tool operation"))?;
            let invocation_status: String = row
                .try_get("invocation_status")
                .map_err(db("read invocation_status"))?;
            let root_session_id: String = row
                .try_get("root_session_id")
                .map_err(db("read root_session_id"))?;
            let active_flag: i64 = row
                .try_get("root_has_active_work")
                .map_err(db("read root_has_active_work"))?;
            out.push(ToolOperationSummary {
                operation,
                invocation_status,
                root_session_id,
                root_has_active_work: active_flag != 0,
            });
        }
        Ok(out)
    }

    pub(crate) async fn mark_remote_session_deleted(
        &self,
        remote_session_id: &str,
    ) -> Result<u64, OrchestratorError> {
        let now = self.now_secs();
        let n = sqlx::query(
            "UPDATE tool_operations SET remote_deleted_at = ?2, last_activity_at = ?2 \
             WHERE remote_session_id = ?1 AND remote_deleted_at IS NULL",
        )
        .bind(remote_session_id)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(db("mark remote session deleted"))?
        .rows_affected();
        Ok(n)
    }

    pub(crate) async fn sweep_stale_tool_operations(&self) -> Result<u64, OrchestratorError> {
        let now = self.now_secs();
        // 1. Open operations past deadline -> expired
        let expired = sqlx::query(
            "UPDATE tool_operations SET status = 'expired', last_activity_at = ?1 \
             WHERE status IN ('submitting', 'running', 'downloading') AND deadline_at <= ?1",
        )
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(db("expire overdue tool operations"))?
        .rows_affected();

        // 2. Open operations whose invocation is already terminal (`completed` / `failed` / `cancelled`) -> failed
        let failed = sqlx::query(
            "UPDATE tool_operations SET status = 'failed', last_activity_at = ?1 \
             WHERE status IN ('submitting', 'running', 'downloading') \
               AND invocation_id IN ( \
                   SELECT id FROM tool_invocations \
                   WHERE status IN ('completed', 'failed', 'cancelled') \
               )",
        )
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(db("fail orphaned tool operations"))?
        .rows_affected();

        Ok(expired + failed)
    }
}
