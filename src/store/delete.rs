//! Xoá vật lý một session family (Root + Child) trong một transaction.

use sqlx::Row;

use super::codec::db;
use super::session::authorize_on;
use super::SessionStore;
use crate::error::{ErrorCode, OrchestratorError};
use crate::types::CallerContext;

impl SessionStore {
    /// Xoá Root và mọi Child của nó cùng messages, runs, invocations và artifact rows. Từ chối
    /// (`SESSION_BUSY`, không đổi gì) nếu bất kỳ session nào trong family còn run `running`.
    /// Trả `storage_key` của artifact đã xoá để caller dọn file sau commit.
    ///
    /// FK giữa messages/runs/invocations/sessions có vòng tham chiếu nên transaction dùng
    /// `defer_foreign_keys`: toàn bộ ràng buộc được kiểm lúc commit.
    pub(crate) async fn delete_family(
        &self,
        caller: &CallerContext,
        root_id: &str,
    ) -> Result<Vec<String>, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin delete"))?;
        // Câu lệnh ghi đầu tiên lấy write lock ngay (xem docs của module `store`); no-op.
        sqlx::query("UPDATE sessions SET id = id WHERE id = ?1")
            .bind(root_id)
            .execute(&mut *tx)
            .await
            .map_err(db("lock delete"))?;
        let scope = authorize_on(&mut tx, caller, root_id).await?;
        if scope.is_child {
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "deletion is addressed by the root session of a family",
            ));
        }
        const FAMILY: &str = "(SELECT id FROM sessions WHERE id = ?1 OR parent_session_id = ?1)";
        let running: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM agent_runs WHERE status = 'running' AND session_id IN {FAMILY}"
        ))
        .bind(root_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db("check active runs"))?;
        if running > 0 {
            return Err(OrchestratorError::new(
                ErrorCode::SessionBusy,
                "the session family has an active run; cancel it before deleting",
            ));
        }
        sqlx::query("PRAGMA defer_foreign_keys = ON")
            .execute(&mut *tx)
            .await
            .map_err(db("defer foreign keys"))?;
        let keys = sqlx::query(&format!(
            "DELETE FROM artifacts WHERE session_id IN {FAMILY} RETURNING storage_key"
        ))
        .bind(root_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(db("delete artifacts"))?
        .iter()
        .map(|r| r.try_get("storage_key").map_err(db("read storage key")))
        .collect::<Result<Vec<String>, _>>()?;
        for table in ["tool_invocations", "messages", "agent_runs"] {
            sqlx::query(&format!("DELETE FROM {table} WHERE session_id IN {FAMILY}"))
                .bind(root_id)
                .execute(&mut *tx)
                .await
                .map_err(db("delete family rows"))?;
        }
        sqlx::query("DELETE FROM sessions WHERE id = ?1 OR parent_session_id = ?1")
            .bind(root_id)
            .execute(&mut *tx)
            .await
            .map_err(db("delete sessions"))?;
        tx.commit().await.map_err(db("commit delete"))?;
        Ok(keys)
    }
}
