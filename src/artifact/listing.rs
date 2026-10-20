//! Phân trang artifact theo turn.

use sqlx::Row;

use super::cursor::{decode_artifact_cursor, encode_artifact_cursor};
use super::row::ref_from_row;
use super::store::ArtifactStore;
use crate::error::{ErrorCode, OrchestratorError};
use crate::types::{ArtifactRef, CallerContext, Page, PageRequest};

impl ArtifactStore {
    /// Phân trang danh sách `ArtifactRef` của một `(session_id, turn_id)` (bao gồm cả các artifact
    /// của child turn được ủy quyền trực tiếp bởi turn đó nếu `session_id` là Root).
    pub(crate) async fn list_for_turn(
        &self,
        caller: &CallerContext,
        session_id: &str,
        turn_id: &str,
        page: PageRequest,
    ) -> Result<Page<ArtifactRef>, OrchestratorError> {
        self.list_for_turn_with_max(caller, session_id, turn_id, page, self.limits.page_size)
            .await
    }

    pub(crate) async fn preview_for_turn(
        &self,
        owner_id: &str,
        workspace_id: &str,
        session_id: &str,
        turn_id: &str,
    ) -> Result<Page<ArtifactRef>, OrchestratorError> {
        self.query_turn_page(
            owner_id,
            workspace_id,
            session_id,
            turn_id,
            None,
            self.limits.preview_limit,
        )
        .await
    }

    pub(super) async fn list_for_turn_with_max(
        &self,
        caller: &CallerContext,
        session_id: &str,
        turn_id: &str,
        page: PageRequest,
        max_page: u32,
    ) -> Result<Page<ArtifactRef>, OrchestratorError> {
        // Kiểm tra quyền của caller trên session và sự tồn tại của turn.
        let srow = sqlx::query("SELECT owner_id, workspace_id FROM sessions WHERE id = ?1")
            .bind(session_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| OrchestratorError::storage("load session scope", &e))?
            .ok_or_else(|| OrchestratorError::new(ErrorCode::NotFound, "session not found"))?;
        let owner: String = srow
            .try_get("owner_id")
            .map_err(|e| OrchestratorError::storage("read owner", &e))?;
        let workspace: String = srow
            .try_get("workspace_id")
            .map_err(|e| OrchestratorError::storage("read workspace", &e))?;
        if owner != caller.owner_id || workspace != caller.workspace_id {
            return Err(OrchestratorError::new(
                ErrorCode::PermissionDenied,
                "the session is outside the caller's scope",
            ));
        }
        let turn_exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM agent_runs WHERE session_id = ?1 AND turn_id = ?2 LIMIT 1",
        )
        .bind(session_id)
        .bind(turn_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OrchestratorError::storage("verify turn exists", &e))?;
        if turn_exists.is_none() {
            return Err(OrchestratorError::new(
                ErrorCode::NotFound,
                "turn not found",
            ));
        }

        let limit = page.limit.clamp(1, max_page);
        self.query_turn_page(
            &caller.owner_id,
            &caller.workspace_id,
            session_id,
            turn_id,
            page.cursor.as_deref(),
            limit,
        )
        .await
    }

    pub(super) async fn query_turn_page(
        &self,
        owner_id: &str,
        workspace_id: &str,
        session_id: &str,
        turn_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<Page<ArtifactRef>, OrchestratorError> {
        let (after_created_at, after_id) = match cursor {
            None => (i64::MIN, String::new()),
            Some(c) => decode_artifact_cursor(c, session_id, turn_id)?,
        };
        let rows = sqlx::query(
            "SELECT a.created_at, a.id, a.role, a.name, a.mime_type, a.size_bytes, a.sha256 \
             FROM artifacts a \
             WHERE a.owner_id = ?1 AND a.workspace_id = ?2 \
               AND ( \
                 (a.session_id = ?3 AND a.turn_id = ?4) \
                 OR EXISTS ( \
                   SELECT 1 FROM tool_invocations ti \
                   JOIN agent_runs r ON r.id = ti.run_id \
                   WHERE r.session_id = ?3 AND r.turn_id = ?4 \
                     AND ti.child_session_id = a.session_id \
                     AND ti.child_turn_id = a.turn_id \
                 ) \
               ) \
               AND (a.created_at > ?5 OR (a.created_at = ?5 AND a.id > ?6)) \
             ORDER BY a.created_at ASC, a.id ASC \
             LIMIT ?7",
        )
        .bind(owner_id)
        .bind(workspace_id)
        .bind(session_id)
        .bind(turn_id)
        .bind(after_created_at)
        .bind(&after_id)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OrchestratorError::storage("list turn artifacts", &e))?;

        let mut items = Vec::with_capacity(rows.len().min(limit as usize));
        let mut last_pos: Option<(i64, String)> = None;
        for row in rows.iter().take(limit as usize) {
            let created_at: i64 = row
                .try_get("created_at")
                .map_err(|e| OrchestratorError::storage("read created_at", &e))?;
            let item = ref_from_row(row)?;
            last_pos = Some((created_at, item.id.clone()));
            items.push(item);
        }
        let next_cursor = if rows.len() > limit as usize {
            last_pos.map(|(created_at, id)| {
                encode_artifact_cursor(session_id, turn_id, created_at, &id)
            })
        } else {
            None
        };
        Ok(Page { items, next_cursor })
    }
}
