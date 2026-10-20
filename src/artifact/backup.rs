//! Sao lưu file artifact đã commit.

use std::path::Path;

use tokio::fs;

use super::fsutil::{copy_and_verify_artifact, sync_directory};
use super::store::ArtifactStore;
use super::{ARTIFACTS_DIR, COMMITTED_DIR, STAGING_DIR};
use crate::error::OrchestratorError;

impl ArtifactStore {
    /// Sao lưu và kiểm tra toàn vẹn các file artifact đã commit sang thư mục backup `dest_root`.
    pub(crate) async fn backup_committed_to(
        &self,
        dest_root: &Path,
        keys: &[(String, u64, String)],
    ) -> Result<(), OrchestratorError> {
        let dest_artifacts = dest_root.join(ARTIFACTS_DIR);
        let dest_staging = dest_artifacts.join(STAGING_DIR);
        let dest_committed = dest_artifacts.join(COMMITTED_DIR);
        fs::create_dir_all(&dest_staging)
            .await
            .map_err(|e| OrchestratorError::storage("create backup staging dir", &e))?;
        fs::create_dir_all(&dest_committed)
            .await
            .map_err(|e| OrchestratorError::storage("create backup committed dir", &e))?;

        for (storage_key, expected_size, expected_sha256) in keys {
            let src_file = self.committed_dir.join(storage_key);
            let dst_file = dest_committed.join(storage_key);
            copy_and_verify_artifact(&src_file, &dst_file, *expected_size, expected_sha256).await?;
        }
        sync_directory(&dest_committed);
        sync_directory(dest_root);
        Ok(())
    }
}
