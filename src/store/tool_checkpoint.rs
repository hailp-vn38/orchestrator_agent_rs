//! Checkpoint pointer của tool invocation (CAS trên `tool_invocations`).

use super::SessionStore;
use sqlx::Row;

impl SessionStore {
    /// Đọc `checkpoint_artifact_id` đã commit của đúng `scope.invocation_id` và xác minh
    /// tính toàn vẹn của artifact đó. Không bao giờ tự kế thừa pointer từ invocation trước.
    pub(crate) async fn current_checkpoint(
        &self,
        scope: &crate::tool::ToolScope,
        artifacts: &crate::artifact::ArtifactStore,
    ) -> Result<Option<String>, crate::tool::ToolError> {
        use crate::artifact::{CODE_ARTIFACT_PERMISSION_DENIED, CODE_ARTIFACT_UNAVAILABLE};
        let row = sqlx::query(
            "SELECT ti.checkpoint_artifact_id \
             FROM tool_invocations ti \
             JOIN sessions s ON s.id = ti.session_id \
             WHERE ti.id = ?1 AND ti.session_id = ?2 AND ti.run_id = ?3 \
               AND s.owner_id = ?4 AND s.workspace_id = ?5",
        )
        .bind(&scope.invocation_id)
        .bind(&scope.session_id)
        .bind(&scope.run_id)
        .bind(&scope.owner_id)
        .bind(&scope.workspace_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| crate::tool::ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not read invocation checkpoint pointer".into(),
        })?
        .ok_or_else(|| crate::tool::ToolError {
            code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
            message: "invocation is outside the authorized scope".into(),
        })?;

        let current: Option<String> =
            row.try_get("checkpoint_artifact_id")
                .map_err(|_| crate::tool::ToolError {
                    code: CODE_ARTIFACT_UNAVAILABLE.into(),
                    message: "could not decode checkpoint pointer".into(),
                })?;
        if let Some(ref id) = current {
            artifacts
                .verify_in_family(
                    &scope.owner_id,
                    &scope.workspace_id,
                    &scope.root_session_id,
                    id,
                )
                .await?;
        }
        Ok(current)
    }

    /// Compare-and-swap (CAS) cập nhật `tool_invocations.checkpoint_artifact_id` cho đúng
    /// `scope.invocation_id` sau khi đã xác minh `manifest_id` tồn tại, thuộc scope và nguyên vẹn.
    pub(crate) async fn commit_checkpoint(
        &self,
        scope: &crate::tool::ToolScope,
        artifacts: &crate::artifact::ArtifactStore,
        manifest_id: &str,
        expected_previous_id: Option<&str>,
    ) -> Result<(), crate::tool::ToolError> {
        use crate::artifact::{
            CODE_ARTIFACT_PERMISSION_DENIED, CODE_ARTIFACT_UNAVAILABLE, CODE_CHECKPOINT_CONFLICT,
        };
        if artifacts.fault().should_fail_checkpoint_cas() {
            return Err(crate::tool::ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "injected fault before checkpoint pointer CAS commit".into(),
            });
        }
        artifacts
            .verify_for_checkpoint_commit(scope, manifest_id)
            .await?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| crate::tool::ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not begin checkpoint CAS transaction".into(),
            })?;

        // Ghi no-op đầu tiên để lấy write lock ngay và đọc pointer hiện tại.
        let row = sqlx::query(
            "UPDATE tool_invocations SET status = status \
             WHERE id = ?1 AND session_id = ?2 AND run_id = ?3 AND status = 'running' \
               AND EXISTS ( \
                 SELECT 1 FROM agent_runs r \
                 JOIN sessions s ON s.id = r.session_id \
                 WHERE r.id = ?3 AND r.session_id = ?2 AND r.status = 'running' \
                   AND s.owner_id = ?4 AND s.workspace_id = ?5 \
               ) \
             RETURNING checkpoint_artifact_id",
        )
        .bind(&scope.invocation_id)
        .bind(&scope.session_id)
        .bind(&scope.run_id)
        .bind(&scope.owner_id)
        .bind(&scope.workspace_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| crate::tool::ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not lock invocation for checkpoint CAS".into(),
        })?;
        let Some(row) = row else {
            let _ = tx.rollback().await;
            return Err(crate::tool::ToolError {
                code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
                message: "invocation is not active in the authorized scope".into(),
            });
        };
        let current: Option<String> = row.try_get("checkpoint_artifact_id").unwrap_or(None);
        if current.as_deref() != expected_previous_id {
            let _ = tx.rollback().await;
            return Err(crate::tool::ToolError {
                code: CODE_CHECKPOINT_CONFLICT.into(),
                message: "checkpoint pointer changed concurrently or expected_previous_id is stale"
                    .into(),
            });
        }
        sqlx::query("UPDATE tool_invocations SET checkpoint_artifact_id = ?2 WHERE id = ?1")
            .bind(&scope.invocation_id)
            .bind(manifest_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| crate::tool::ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not update checkpoint pointer".into(),
            })?;
        sqlx::query(
            "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = ?1",
        )
        .bind(&scope.session_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| crate::tool::ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not bump session revision for checkpoint".into(),
        })?;
        tx.commit().await.map_err(|_| crate::tool::ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not commit checkpoint pointer".into(),
        })?;
        Ok(())
    }
}
