//! Struct `ArtifactStore`, khởi tạo thư mục và dọn file mồ côi.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use sqlx::SqlitePool;
use tokio::fs;

use super::fault::ArtifactFault;
use super::fsutil::is_expired;
use super::limits::ArtifactLimits;
use super::{ARTIFACTS_DIR, COMMITTED_DIR, STAGING_DIR};
use crate::error::OrchestratorError;

#[derive(Clone)]
pub(crate) struct ArtifactStore {
    pub(super) pool: SqlitePool,
    pub(super) staging_dir: PathBuf,
    pub(super) committed_dir: PathBuf,
    pub(super) limits: ArtifactLimits,
    pub(super) fault: Arc<ArtifactFault>,
}

impl fmt::Debug for ArtifactStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArtifactStore")
            .field("limits", &self.limits)
            .finish()
    }
}

impl ArtifactStore {
    /// Khởi tạo thư mục `artifacts/staging` và `artifacts/committed` bên dưới `store_dir`.
    pub(crate) async fn open(
        pool: SqlitePool,
        store_dir: &Path,
        limits: ArtifactLimits,
        fault: Option<Arc<ArtifactFault>>,
    ) -> Result<Self, OrchestratorError> {
        let root_dir = store_dir.join(ARTIFACTS_DIR);
        let staging_dir = root_dir.join(STAGING_DIR);
        let committed_dir = root_dir.join(COMMITTED_DIR);
        fs::create_dir_all(&staging_dir)
            .await
            .map_err(|e| OrchestratorError::storage("create artifact staging dir", &e))?;
        fs::create_dir_all(&committed_dir)
            .await
            .map_err(|e| OrchestratorError::storage("create artifact committed dir", &e))?;
        Ok(Self {
            pool,
            staging_dir,
            committed_dir,
            limits,
            fault: fault.unwrap_or_default(),
        })
    }

    pub(crate) fn fault(&self) -> &Arc<ArtifactFault> {
        &self.fault
    }

    #[cfg(test)]
    pub(crate) fn committed_dir(&self) -> &Path {
        &self.committed_dir
    }

    #[cfg(test)]
    pub(crate) fn staging_dir(&self) -> &Path {
        &self.staging_dir
    }

    /// Dọn các file mồ côi đã quá hạn `staging_grace` của store.
    pub(crate) async fn sweep_configured_orphans(&self) -> Result<u64, OrchestratorError> {
        self.sweep_orphans(self.limits.staging_grace).await
    }

    /// Dọn các file mồ côi đã quá hạn `grace`:
    /// 1) Mọi file trong `staging/` có `modified` cũ hơn `grace`.
    /// 2) Mọi file trong `committed/` có `modified` cũ hơn `grace` **nhưng không có row** trong
    ///    bảng `artifacts` (xảy ra khi crash sau `rename` trước khi commit SQLite).
    ///
    /// Các artifact đã có mặt trong bảng `artifacts` không bao giờ bị xóa (MVP không có GC/TTL).
    pub(crate) async fn sweep_orphans(&self, grace: Duration) -> Result<u64, OrchestratorError> {
        let now = SystemTime::now();
        let mut removed = 0u64;

        let mut staging = fs::read_dir(&self.staging_dir)
            .await
            .map_err(|e| OrchestratorError::storage("read staging dir", &e))?;
        while let Some(entry) = staging
            .next_entry()
            .await
            .map_err(|e| OrchestratorError::storage("iter staging dir", &e))?
        {
            if is_expired(&entry, now, grace).await && fs::remove_file(entry.path()).await.is_ok() {
                removed += 1;
            }
        }

        let mut committed = fs::read_dir(&self.committed_dir)
            .await
            .map_err(|e| OrchestratorError::storage("read committed dir", &e))?;
        while let Some(entry) = committed
            .next_entry()
            .await
            .map_err(|e| OrchestratorError::storage("iter committed dir", &e))?
        {
            if !is_expired(&entry, now, grace).await {
                continue;
            }
            let Some(key) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let known: Option<i64> =
                sqlx::query_scalar("SELECT 1 FROM artifacts WHERE storage_key = ?1")
                    .bind(&key)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| OrchestratorError::storage("check committed key", &e))?;
            if known.is_none() && fs::remove_file(entry.path()).await.is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}
