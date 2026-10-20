//! Phân trang artifact theo turn.

use super::cursor::{decode_artifact_cursor, encode_artifact_cursor};
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
        let (owner, workspace) = self.db.artifact_session_scope(session_id).await?;
        if owner != caller.owner_id || workspace != caller.workspace_id {
            return Err(OrchestratorError::new(
                ErrorCode::PermissionDenied,
                "the session is outside the caller's scope",
            ));
        }
        let turn_exists = self.db.artifact_turn_exists(session_id, turn_id).await?;
        if !turn_exists {
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
        let rows = self
            .db
            .artifact_turn_page_rows(
                owner_id,
                workspace_id,
                session_id,
                turn_id,
                (after_created_at, &after_id),
                i64::from(limit) + 1,
            )
            .await?;

        let mut items = Vec::with_capacity(rows.len().min(limit as usize));
        let mut last_pos: Option<(i64, String)> = None;
        for (created_at, item) in rows.iter().take(limit as usize) {
            last_pos = Some((*created_at, item.id.clone()));
            items.push(item.clone());
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
