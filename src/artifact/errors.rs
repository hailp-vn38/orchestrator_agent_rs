//! Mã lỗi ổn định và bộ dựng lỗi dùng chung cho toàn bộ `artifact`.

use std::io;

use crate::error::{ErrorCode, OrchestratorError};
use crate::tool::ToolError;

/// Mã lỗi ổn định cho `ToolError` khi thao tác với artifact/checkpoint.
pub(crate) const CODE_ARTIFACT_TOO_LARGE: &str = "ARTIFACT_TOO_LARGE";
pub(crate) const CODE_WORKSPACE_QUOTA_EXCEEDED: &str = "WORKSPACE_QUOTA_EXCEEDED";
pub(crate) const CODE_DISK_RESERVE_EXCEEDED: &str = "DISK_RESERVE_EXCEEDED";
pub(crate) const CODE_ARTIFACT_INTEGRITY: &str = "ARTIFACT_INTEGRITY_ERROR";
pub(crate) const CODE_ARTIFACT_NOT_FOUND: &str = "ARTIFACT_NOT_FOUND";
pub(crate) const CODE_ARTIFACT_PERMISSION_DENIED: &str = "PERMISSION_DENIED";
pub(crate) const CODE_ARTIFACT_INVALID_INPUT: &str = "INVALID_INPUT";
pub(crate) const CODE_CHECKPOINT_CONFLICT: &str = "CHECKPOINT_CONFLICT";
pub(crate) const CODE_ARTIFACT_UNAVAILABLE: &str = "ARTIFACT_STORE_UNAVAILABLE";

pub(super) fn integrity_orchestrator_error(detail: &'static str) -> OrchestratorError {
    tracing::warn!(detail, "artifact integrity check failed");
    OrchestratorError::new(
        ErrorCode::Unavailable,
        format!("{CODE_ARTIFACT_INTEGRITY}: {detail}"),
    )
}

pub(super) fn map_io_error(context: &'static str, err: &io::Error) -> ToolError {
    tracing::warn!(context, error = %err, "artifact storage I/O error");
    ToolError {
        code: CODE_ARTIFACT_UNAVAILABLE.into(),
        message: "artifact storage operation failed".into(),
    }
}
