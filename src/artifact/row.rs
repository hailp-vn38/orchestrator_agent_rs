//! Ánh xạ row SQLite `artifacts` sang kiểu domain.

use sqlx::Row;

use crate::error::{ErrorCode, OrchestratorError};
use crate::types::{ArtifactRef, ArtifactRole};

pub(super) struct StoredArtifact {
    pub(super) artifact: ArtifactRef,
    pub(super) owner_id: String,
    pub(super) workspace_id: String,
    pub(super) root_session_id: String,
    pub(super) producer_invocation_id: Option<String>,
    pub(super) storage_key: String,
}

pub(super) fn ref_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<ArtifactRef, OrchestratorError> {
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

pub(super) fn stored_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<StoredArtifact, OrchestratorError> {
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
