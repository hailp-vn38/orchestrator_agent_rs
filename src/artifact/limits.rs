//! Giới hạn vận hành của `ArtifactStore`.

use std::time::Duration;

use crate::config::RuntimeConfig;

/// Giới hạn vận hành của `ArtifactStore` lấy từ [`RuntimeConfig`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct ArtifactLimits {
    pub max_bytes: u64,
    pub workspace_quota_bytes: u64,
    pub disk_reserve_bytes: u64,
    pub download_max_bytes: u64,
    pub preview_limit: u32,
    pub page_size: u32,
    pub staging_grace: Duration,
}

impl ArtifactLimits {
    pub(crate) fn from_runtime(rt: &RuntimeConfig) -> Self {
        Self {
            max_bytes: rt.artifact_max_bytes,
            workspace_quota_bytes: rt.owner_workspace_quota_bytes,
            disk_reserve_bytes: rt.disk_reserve_bytes,
            download_max_bytes: rt.download_max_bytes,
            preview_limit: rt.artifact_preview_limit,
            page_size: rt.page_size,
            staging_grace: rt.artifact_staging_grace,
        }
    }
}
