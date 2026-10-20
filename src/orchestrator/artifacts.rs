//! Artifact ở mức host: liệt kê, mở và sao lưu.

use std::path::Path;

use crate::artifact::ArtifactDownload;
use crate::error::{ErrorCode, OrchestratorError};
use crate::store::DB_FILE;
use crate::types::{ArtifactRef, CallerContext, Page, PageRequest};

use super::Orchestrator;

impl Orchestrator {
    /// Phân trang danh sách [`ArtifactRef`] đã commit của `(session_id, turn_id)` theo thứ tự
    /// ổn định `(created_at, id)` và cursor opaque gắn với đúng `(session_id, turn_id)`.
    pub async fn list_artifacts(
        &self,
        caller: &CallerContext,
        session_id: &str,
        turn_id: &str,
        page: PageRequest,
    ) -> Result<Page<ArtifactRef>, OrchestratorError> {
        self.ensure_open()?;
        self.artifacts
            .list_for_turn(caller, session_id, turn_id, page)
            .await
    }

    /// Mở một artifact đã commit để đọc/stream với giới hạn `download_max_bytes` và kiểm tra
    /// toàn vẹn SHA-256 trên dòng dữ liệu đọc ra. Không bao giờ lộ filesystem path nội bộ.
    pub async fn open_artifact(
        &self,
        caller: &CallerContext,
        artifact_id: &str,
    ) -> Result<ArtifactDownload, OrchestratorError> {
        self.ensure_open()?;
        self.artifacts.open_for_host(caller, artifact_id).await
    }

    /// Tạo bản sao lưu nhất quán (consistent recovery point) của cả SQLite execution store
    /// (qua `VACUUM INTO`, an toàn khi WAL đang mở) và toàn bộ các file committed artifacts
    /// có mặt trong snapshot đó sang `dest_dir` (docs/17 §17.4, docs/18 §18.7).
    pub async fn backup_to(&self, dest_dir: impl AsRef<Path>) -> Result<(), OrchestratorError> {
        self.ensure_open()?;
        let dest = dest_dir.as_ref();
        if dest.as_os_str().is_empty() || dest == self.store_dir {
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "backup destination must be a distinct non-empty directory",
            ));
        }
        let dest_db = dest.join(DB_FILE);
        if tokio::fs::try_exists(&dest_db).await.unwrap_or(false) {
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "backup destination already contains an orchestrator database",
            ));
        }
        tokio::fs::create_dir_all(dest)
            .await
            .map_err(|e| OrchestratorError::storage("create backup dir", &e))?;
        let keys = self.store.backup_sqlite_to(&dest_db).await?;
        self.artifacts.backup_committed_to(dest, &keys).await
    }
}
