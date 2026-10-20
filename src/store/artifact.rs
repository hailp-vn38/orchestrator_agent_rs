//! SQL của bảng `artifacts` và các truy vấn artifact chạm vào `sessions`/`agent_runs`/
//! `tool_invocations`. `ArtifactStore` (filesystem) chỉ gọi qua các hàm ở đây, không tự
//! viết SQL (ADR-007, ADR-016).

use super::SessionStore;
use crate::error::{ErrorCode, OrchestratorError};
use crate::types::{ArtifactRef, ArtifactRole};
use sqlx::Row;

/// Metadata đầy đủ của một artifact đã commit, kèm root session để kiểm tra session family.
pub(crate) struct StoredArtifact {
    pub(crate) artifact: ArtifactRef,
    pub(crate) owner_id: String,
    pub(crate) workspace_id: String,
    pub(crate) root_session_id: String,
    pub(crate) producer_invocation_id: Option<String>,
    pub(crate) storage_key: String,
}

/// Scope đã xác thực của một lần xuất bản artifact (mirror `ArtifactWriteScope`).
pub(crate) struct PublishScope<'a> {
    pub(crate) owner_id: &'a str,
    pub(crate) workspace_id: &'a str,
    pub(crate) session_id: &'a str,
    pub(crate) turn_id: &'a str,
    pub(crate) run_id: &'a str,
    pub(crate) producer_invocation_id: Option<&'a str>,
}

/// Lý do `artifact_publish` thất bại; `ArtifactStore` ánh xạ sang mã `ToolError` tương ứng.
pub(crate) enum PublishError {
    /// Lỗi DB ở bước được mô tả (message tĩnh).
    Db(&'static str),
    SessionOutOfScope,
    InvocationInactive,
    TurnNotInSession,
    QuotaExceeded,
}

impl SessionStore {
    /// Tổng `size_bytes` artifact của `(owner, workspace)`.
    pub(crate) async fn artifact_workspace_used_bytes(
        &self,
        owner_id: &str,
        workspace_id: &str,
    ) -> Result<u64, sqlx::Error> {
        let used: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(size_bytes), 0) FROM artifacts \
             WHERE owner_id = ?1 AND workspace_id = ?2",
        )
        .bind(owner_id)
        .bind(workspace_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(used.max(0) as u64)
    }

    /// Có row `artifacts` nào trỏ tới `storage_key` này không (dùng khi dọn file mồ côi).
    pub(crate) async fn artifact_storage_key_known(
        &self,
        storage_key: &str,
    ) -> Result<bool, OrchestratorError> {
        let known: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM artifacts WHERE storage_key = ?1")
                .bind(storage_key)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| OrchestratorError::storage("check committed key", &e))?;
        Ok(known.is_some())
    }

    /// Transaction xuất bản metadata: bump `state_revision` (lấy write lock), xác minh
    /// invocation/turn, kiểm tra quota rồi `INSERT INTO artifacts`.
    pub(crate) async fn artifact_publish(
        &self,
        scope: &PublishScope<'_>,
        reference: &ArtifactRef,
        storage_key: &str,
        workspace_quota_bytes: u64,
    ) -> Result<(), PublishError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| PublishError::Db("could not begin artifact metadata transaction"))?;

        // Ghi đầu tiên để lấy write lock ngay và tăng revision của session sở hữu.
        let bumped = sqlx::query(
            "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = ?1 AND owner_id = ?2 AND workspace_id = ?3",
        )
        .bind(scope.session_id)
        .bind(scope.owner_id)
        .bind(scope.workspace_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| PublishError::Db("could not lock session for artifact publication"))?
        .rows_affected();
        if bumped != 1 {
            let _ = tx.rollback().await;
            return Err(PublishError::SessionOutOfScope);
        }

        if let Some(inv_id) = scope.producer_invocation_id {
            let valid_inv: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM tool_invocations ti \
                 JOIN agent_runs r ON r.id = ti.run_id \
                 WHERE ti.id = ?1 AND ti.session_id = ?2 AND ti.run_id = ?3 \
                   AND ti.status = 'running' AND r.turn_id = ?4 AND r.status = 'running'",
            )
            .bind(inv_id)
            .bind(scope.session_id)
            .bind(scope.run_id)
            .bind(scope.turn_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| PublishError::Db("could not verify producer invocation state"))?;
            if valid_inv.is_none() {
                let _ = tx.rollback().await;
                return Err(PublishError::InvocationInactive);
            }
        } else {
            let valid_turn: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM agent_runs WHERE session_id = ?1 AND turn_id = ?2 LIMIT 1",
            )
            .bind(scope.session_id)
            .bind(scope.turn_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| PublishError::Db("could not verify turn ownership"))?;
            if valid_turn.is_none() {
                let _ = tx.rollback().await;
                return Err(PublishError::TurnNotInSession);
            }
        }

        let used: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(size_bytes), 0) FROM artifacts \
             WHERE owner_id = ?1 AND workspace_id = ?2",
        )
        .bind(scope.owner_id)
        .bind(scope.workspace_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| PublishError::Db("could not verify workspace quota"))?;
        if (used.max(0) as u64).saturating_add(reference.size_bytes) > workspace_quota_bytes {
            let _ = tx.rollback().await;
            return Err(PublishError::QuotaExceeded);
        }

        sqlx::query(
            "INSERT INTO artifacts(id, owner_id, workspace_id, session_id, turn_id, \
             producer_invocation_id, role, name, mime_type, size_bytes, sha256, storage_key) \
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        )
        .bind(&reference.id)
        .bind(scope.owner_id)
        .bind(scope.workspace_id)
        .bind(scope.session_id)
        .bind(scope.turn_id)
        .bind(scope.producer_invocation_id)
        .bind(reference.role.as_str())
        .bind(&reference.name)
        .bind(&reference.mime_type)
        .bind(reference.size_bytes as i64)
        .bind(&reference.sha256)
        .bind(storage_key)
        .execute(&mut *tx)
        .await
        .map_err(|_| PublishError::Db("could not insert artifact metadata"))?;

        tx.commit()
            .await
            .map_err(|_| PublishError::Db("could not commit artifact metadata"))?;
        Ok(())
    }

    /// Đọc metadata artifact (kèm root session) theo id.
    pub(crate) async fn artifact_load(
        &self,
        artifact_id: &str,
    ) -> Result<Option<StoredArtifact>, OrchestratorError> {
        let row = sqlx::query(
            "SELECT a.id, a.owner_id, a.workspace_id, a.session_id, a.turn_id, \
             a.producer_invocation_id, a.role, a.name, a.mime_type, a.size_bytes, \
             a.sha256, a.storage_key, COALESCE(s.parent_session_id, s.id) AS root_session_id \
             FROM artifacts a \
             JOIN sessions s ON s.id = a.session_id \
             WHERE a.id = ?1",
        )
        .bind(artifact_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OrchestratorError::storage("load artifact metadata", &e))?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(stored_from_row(&row)?))
    }

    /// `arguments_json` và `checkpoint_artifact_id` của một invocation đang thuộc
    /// `(session, run)`; `None` nếu invocation không thuộc đó.
    pub(crate) async fn invocation_input_scope(
        &self,
        invocation_id: &str,
        session_id: &str,
        run_id: &str,
    ) -> Result<Option<(String, Option<String>)>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT arguments_json, checkpoint_artifact_id FROM tool_invocations \
             WHERE id = ?1 AND session_id = ?2 AND run_id = ?3",
        )
        .bind(invocation_id)
        .bind(session_id)
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            let args: String = r.try_get("arguments_json").unwrap_or_default();
            let checkpoint: Option<String> = r.try_get("checkpoint_artifact_id").unwrap_or(None);
            (args, checkpoint)
        }))
    }

    /// `(owner_id, workspace_id)` của session; `NotFound` nếu không tồn tại.
    pub(crate) async fn artifact_session_scope(
        &self,
        session_id: &str,
    ) -> Result<(String, String), OrchestratorError> {
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
        Ok((owner, workspace))
    }

    /// Turn có thuộc session (có `agent_runs` tương ứng) không.
    pub(crate) async fn artifact_turn_exists(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<bool, OrchestratorError> {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM agent_runs WHERE session_id = ?1 AND turn_id = ?2 LIMIT 1",
        )
        .bind(session_id)
        .bind(turn_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OrchestratorError::storage("verify turn exists", &e))?;
        Ok(exists.is_some())
    }

    /// Một trang artifact của turn (gồm artifact của child turn do turn đó ủy quyền), tối đa
    /// `fetch_limit` row, kèm `created_at` để dựng cursor.
    pub(crate) async fn artifact_turn_page_rows(
        &self,
        owner_id: &str,
        workspace_id: &str,
        session_id: &str,
        turn_id: &str,
        after: (i64, &str),
        fetch_limit: i64,
    ) -> Result<Vec<(i64, ArtifactRef)>, OrchestratorError> {
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
        .bind(after.0)
        .bind(after.1)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OrchestratorError::storage("list turn artifacts", &e))?;

        rows.iter()
            .map(|row| {
                let created_at: i64 = row
                    .try_get("created_at")
                    .map_err(|e| OrchestratorError::storage("read created_at", &e))?;
                Ok((created_at, ref_from_row(row)?))
            })
            .collect()
    }
}

fn ref_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<ArtifactRef, OrchestratorError> {
    let role_str: String = row
        .try_get("role")
        .map_err(|e| OrchestratorError::storage("read artifact role", &e))?;
    let role = ArtifactRole::parse(&role_str).ok_or_else(|| {
        OrchestratorError::new(ErrorCode::Internal, "unknown artifact role in store")
    })?;
    let size: i64 = row
        .try_get("size_bytes")
        .map_err(|e| OrchestratorError::storage("read artifact size", &e))?;
    Ok(ArtifactRef {
        id: row
            .try_get("id")
            .map_err(|e| OrchestratorError::storage("read artifact id", &e))?,
        name: row
            .try_get("name")
            .map_err(|e| OrchestratorError::storage("read artifact name", &e))?,
        mime_type: row
            .try_get("mime_type")
            .map_err(|e| OrchestratorError::storage("read artifact mime", &e))?,
        size_bytes: size.max(0) as u64,
        sha256: row
            .try_get("sha256")
            .map_err(|e| OrchestratorError::storage("read artifact sha256", &e))?,
        role,
    })
}

fn stored_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<StoredArtifact, OrchestratorError> {
    Ok(StoredArtifact {
        artifact: ref_from_row(row)?,
        owner_id: row
            .try_get("owner_id")
            .map_err(|e| OrchestratorError::storage("read artifact owner", &e))?,
        workspace_id: row
            .try_get("workspace_id")
            .map_err(|e| OrchestratorError::storage("read artifact workspace", &e))?,
        root_session_id: row
            .try_get("root_session_id")
            .map_err(|e| OrchestratorError::storage("read artifact root session", &e))?,
        producer_invocation_id: row
            .try_get("producer_invocation_id")
            .map_err(|e| OrchestratorError::storage("read producer invocation", &e))?,
        storage_key: row
            .try_get("storage_key")
            .map_err(|e| OrchestratorError::storage("read storage key", &e))?,
    })
}
