//! `ArtifactStore` (docs/17, docs/18 §18.7): module sâu nội bộ quản lý toàn bộ vòng đời
//! artifact trên local filesystem + bảng metadata `artifacts` trong SQLite.
//!
//! Bất biến cốt lõi:
//! - Không bao giờ ghép tên file từ user/tool/model vào đường dẫn vật lý: cả file staging lẫn
//!   file committed đều dùng khóa opaque do server sinh.
//! - Luồng xuất bản bất biến: ghi vào `staging/<tmp>.part` kèm kiểm tra kích thước + băm
//!   SHA-256 tăng dần → `sync_all()` → `rename` nguyên tử sang `committed/<storage_key>` trên
//!   cùng filesystem → `sync_all()` thư mục committed → ghi row `artifacts` và tăng
//!   `sessions.state_revision` trong một write transaction SQLite.
//! - Dữ liệu đang staging hoặc đã rename nhưng chưa commit metadata vào SQLite không bao giờ
//!   được đọc qua bất kỳ API nào; `sweep_orphans` dọn các file mồ côi đã quá `staging_grace`
//!   mà không bao giờ xóa các artifact đã có metadata trong SQLite (MVP không có TTL/GC).
//! - Khi đọc (`open_for_host` / `open_for_tool`), kiểm tra quyền sở hữu/ancestry/explicit input
//!   scope, kiểm tra kích thước file khi mở và xác minh SHA-256 trên luồng byte đọc ra; thiếu
//!   hoặc hỏng byte luôn trả lỗi toàn vẹn, không bao giờ báo thành công giả.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, SystemTime};

use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};

use crate::config::RuntimeConfig;
use crate::error::{ErrorCode, OrchestratorError};
use crate::store::new_id;
use crate::tool::{ToolError, ToolScope};
use crate::types::{ArtifactRef, ArtifactRole, CallerContext, Page, PageRequest};

pub(crate) const ARTIFACTS_DIR: &str = "artifacts";
pub(crate) const STAGING_DIR: &str = "staging";
pub(crate) const COMMITTED_DIR: &str = "committed";

const STREAM_CHUNK_BYTES: usize = 64 * 1024;
const MAX_NAME_CHARS: usize = 255;
const MAX_MIME_CHARS: usize = 128;
const CURSOR_VERSION: &str = "a1";
static ARTIFACT_ID_SEQ: AtomicU64 = AtomicU64::new(0);

fn new_ordered_artifact_id() -> String {
    let ts_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let seq = ARTIFACT_ID_SEQ.fetch_add(1, Ordering::Relaxed) & 0xffff;
    format!("art-{ts_ms:012x}-{seq:04x}-{}", new_id())
}

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

/// Fault hook nội bộ cho crash/failpoint tests quanh staging, rename, metadata publish,
/// checkpoint CAS và disk reserve (AT-47). Không phải API production.
#[derive(Debug)]
pub struct ArtifactFault {
    fail_during_staging: AtomicBool,
    fail_after_rename_before_metadata: AtomicBool,
    fail_checkpoint_cas: AtomicBool,
    /// `u64::MAX` = không giới hạn giả lập.
    available_disk_bytes: AtomicU64,
}

impl Default for ArtifactFault {
    fn default() -> Self {
        Self {
            fail_during_staging: AtomicBool::new(false),
            fail_after_rename_before_metadata: AtomicBool::new(false),
            fail_checkpoint_cas: AtomicBool::new(false),
            available_disk_bytes: AtomicU64::new(u64::MAX),
        }
    }
}

impl ArtifactFault {
    #[cfg(any(test, feature = "test-support"))]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Khi `true`, việc ghi file staging sẽ dừng giữa chừng và để lại file `.part` trong `staging/`.
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_fail_during_staging(&self, enabled: bool) {
        self.fail_during_staging.store(enabled, Ordering::SeqCst);
    }

    /// Khi `true`, sau khi file đã `rename` sang `committed/` thì trả lỗi trước khi ghi SQLite.
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_fail_after_rename_before_metadata(&self, enabled: bool) {
        self.fail_after_rename_before_metadata
            .store(enabled, Ordering::SeqCst);
    }

    /// Khi `true`, `ToolContext::checkpoint().commit(...)` bị chặn trước khi cập nhật pointer.
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_fail_checkpoint_cas(&self, enabled: bool) {
        self.fail_checkpoint_cas.store(enabled, Ordering::SeqCst);
    }

    /// Giả lập dung lượng đĩa trống còn lại (`None` = bỏ giả lập).
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_available_disk_bytes(&self, bytes: Option<u64>) {
        self.available_disk_bytes
            .store(bytes.unwrap_or(u64::MAX), Ordering::SeqCst);
    }

    pub(crate) fn should_fail_staging(&self) -> bool {
        self.fail_during_staging.load(Ordering::SeqCst)
    }

    pub(crate) fn should_fail_after_rename(&self) -> bool {
        self.fail_after_rename_before_metadata
            .load(Ordering::SeqCst)
    }

    pub(crate) fn should_fail_checkpoint_cas(&self) -> bool {
        self.fail_checkpoint_cas.load(Ordering::SeqCst)
    }

    pub(crate) fn simulated_free_bytes(&self) -> Option<u64> {
        let v = self.available_disk_bytes.load(Ordering::SeqCst);
        (v != u64::MAX).then_some(v)
    }
}

enum InputBody {
    Bytes(Vec<u8>),
    Reader(Pin<Box<dyn AsyncRead + Send>>),
}

/// Dữ liệu đầu vào để xuất bản một artifact bất biến qua [`crate::ToolArtifacts::put`].
/// Hỗ trợ truyền từ bộ nhớ hoặc stream qua [`AsyncRead`] mà không ép giữ toàn bộ file trong RAM.
pub struct ArtifactInput {
    pub name: String,
    pub mime_type: String,
    pub role: ArtifactRole,
    body: InputBody,
}

impl fmt::Debug for ArtifactInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArtifactInput")
            .field("name", &self.name)
            .field("mime_type", &self.mime_type)
            .field("role", &self.role)
            .finish()
    }
}

impl ArtifactInput {
    pub fn from_bytes(
        name: impl Into<String>,
        mime_type: impl Into<String>,
        role: ArtifactRole,
        bytes: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            name: name.into(),
            mime_type: mime_type.into(),
            role,
            body: InputBody::Bytes(bytes.into()),
        }
    }

    pub fn primary_bytes(
        name: impl Into<String>,
        mime_type: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
    ) -> Self {
        Self::from_bytes(name, mime_type, ArtifactRole::Primary, bytes)
    }

    pub fn intermediate_bytes(
        name: impl Into<String>,
        mime_type: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
    ) -> Self {
        Self::from_bytes(name, mime_type, ArtifactRole::Intermediate, bytes)
    }

    pub fn from_reader<R>(
        name: impl Into<String>,
        mime_type: impl Into<String>,
        role: ArtifactRole,
        reader: R,
    ) -> Self
    where
        R: AsyncRead + Send + 'static,
    {
        Self {
            name: name.into(),
            mime_type: mime_type.into(),
            role,
            body: InputBody::Reader(Box::pin(reader)),
        }
    }
}

/// Reader async có giới hạn byte và kiểm tra toàn vẹn SHA-256 trên dòng dữ liệu đọc ra.
/// Không bao giờ làm lộ đường dẫn filesystem nội bộ.
pub struct ArtifactReader {
    artifact: ArtifactRef,
    file: File,
    max_bytes: u64,
    read_so_far: u64,
    hasher: Sha256,
    verified: bool,
}

impl fmt::Debug for ArtifactReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArtifactReader")
            .field("artifact", &self.artifact)
            .field("max_bytes", &self.max_bytes)
            .field("read_so_far", &self.read_so_far)
            .field("verified", &self.verified)
            .finish()
    }
}

impl ArtifactReader {
    pub fn metadata(&self) -> &ArtifactRef {
        &self.artifact
    }

    /// Đọc một mảnh tiếp theo (tối đa `max_chunk` byte, kẹp vào `1..=64KiB`).
    /// Trả `Ok(None)` khi đã tới EOF **và** kích thước + SHA-256 khớp với metadata đã commit.
    pub async fn read_chunk(
        &mut self,
        max_chunk: usize,
    ) -> Result<Option<Vec<u8>>, OrchestratorError> {
        let cap = max_chunk.clamp(1, STREAM_CHUNK_BYTES);
        let mut buf = vec![0u8; cap];
        let n = self
            .file
            .read(&mut buf)
            .await
            .map_err(|e| OrchestratorError::storage("read artifact chunk", &e))?;
        if n == 0 {
            self.verify_complete()?;
            return Ok(None);
        }
        buf.truncate(n);
        self.observe_bytes(&buf)?;
        Ok(Some(buf))
    }

    /// Đọc toàn bộ artifact vào bộ nhớ (trong phạm vi `max_bytes` đã cấp cho reader) và
    /// xác minh toàn vẹn kích thước + SHA-256 trước khi trả về.
    pub async fn read_all(mut self) -> Result<Vec<u8>, OrchestratorError> {
        let cap = usize::try_from(self.artifact.size_bytes.min(self.max_bytes)).unwrap_or(0);
        let mut out = Vec::with_capacity(cap);
        while let Some(chunk) = self.read_chunk(STREAM_CHUNK_BYTES).await? {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    /// Đọc toàn bộ artifact dưới dạng chuỗi UTF-8 sau khi đã xác minh SHA-256.
    pub async fn read_to_string(self) -> Result<String, OrchestratorError> {
        let bytes = self.read_all().await?;
        String::from_utf8(bytes).map_err(|_| {
            OrchestratorError::new(
                ErrorCode::InvalidInput,
                "artifact content is not valid UTF-8",
            )
        })
    }

    fn observe_bytes(&mut self, bytes: &[u8]) -> Result<(), OrchestratorError> {
        self.read_so_far = self.read_so_far.saturating_add(bytes.len() as u64);
        if self.read_so_far > self.max_bytes {
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "artifact exceeds the configured read byte limit",
            ));
        }
        if self.read_so_far > self.artifact.size_bytes {
            return Err(integrity_orchestrator_error(
                "artifact file size grew beyond committed metadata",
            ));
        }
        self.hasher.update(bytes);
        if self.read_so_far == self.artifact.size_bytes {
            self.verify_complete()?;
        }
        Ok(())
    }

    fn verify_complete(&mut self) -> Result<(), OrchestratorError> {
        if self.verified {
            return Ok(());
        }
        if self.read_so_far != self.artifact.size_bytes {
            return Err(integrity_orchestrator_error(
                "artifact file is truncated relative to committed size_bytes",
            ));
        }
        let digest = hex_digest(self.hasher.clone().finalize());
        if digest != self.artifact.sha256 {
            return Err(integrity_orchestrator_error(
                "artifact sha256 checksum does not match committed metadata",
            ));
        }
        self.verified = true;
        Ok(())
    }
}

impl AsyncRead for ArtifactReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.file).poll_read(cx, buf);
        match poll {
            Poll::Ready(Ok(())) => {
                let after = buf.filled().len();
                let added = &buf.filled()[before..after];
                if added.is_empty() {
                    if let Err(e) = self.verify_complete() {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            e.message().to_owned(),
                        )));
                    }
                } else if let Err(e) = self.observe_bytes(added) {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        e.message().to_owned(),
                    )));
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

/// Kết quả của [`crate::Orchestrator::open_artifact`]: metadata đã xác minh kèm [`ArtifactReader`]
/// giới hạn theo `RuntimeConfig::download_max_bytes`.
#[derive(Debug)]
pub struct ArtifactDownload {
    pub artifact: ArtifactRef,
    reader: ArtifactReader,
}

impl ArtifactDownload {
    pub fn metadata(&self) -> &ArtifactRef {
        &self.artifact
    }

    pub fn into_reader(self) -> ArtifactReader {
        self.reader
    }

    pub async fn read_chunk(
        &mut self,
        max_chunk: usize,
    ) -> Result<Option<Vec<u8>>, OrchestratorError> {
        self.reader.read_chunk(max_chunk).await
    }

    pub async fn read_all(self) -> Result<Vec<u8>, OrchestratorError> {
        self.reader.read_all().await
    }
}

/// Scope đã được runtime xác thực khi một tool ghi artifact mới.
#[derive(Debug, Clone)]
pub(crate) struct ArtifactWriteScope {
    pub owner_id: String,
    pub workspace_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub run_id: String,
    pub producer_invocation_id: Option<String>,
}

impl ArtifactWriteScope {
    pub(crate) fn from_tool_scope(scope: &ToolScope) -> Self {
        Self {
            owner_id: scope.owner_id.clone(),
            workspace_id: scope.workspace_id.clone(),
            session_id: scope.session_id.clone(),
            turn_id: scope.turn_id.clone(),
            run_id: scope.run_id.clone(),
            producer_invocation_id: Some(scope.invocation_id.clone()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ArtifactStore {
    pool: SqlitePool,
    staging_dir: PathBuf,
    committed_dir: PathBuf,
    limits: ArtifactLimits,
    fault: Arc<ArtifactFault>,
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

    /// Xuất bản bất biến một artifact mới: staging → hash/size → fsync → atomic rename →
    /// sync dir → SQLite metadata + bump `state_revision`.
    pub(crate) async fn put(
        &self,
        scope: &ArtifactWriteScope,
        input: ArtifactInput,
    ) -> Result<ArtifactRef, ToolError> {
        let name = validate_name(&input.name)?;
        let mime_type = validate_mime(&input.mime_type)?;
        self.check_disk_reserve(0)?;

        // Kiểm tra nhanh quota trước khi ghi file lớn ra đĩa; kiểm tra chính thức lặp lại
        // bên trong write transaction SQLite để chống race condition.
        let current_used = self
            .workspace_used_bytes(&scope.owner_id, &scope.workspace_id)
            .await?;
        if current_used >= self.limits.workspace_quota_bytes {
            return Err(ToolError {
                code: CODE_WORKSPACE_QUOTA_EXCEEDED.into(),
                message: "workspace artifact storage quota exceeded".into(),
            });
        }

        let staging_name = format!("{}.part", new_id());
        let staging_path = self.staging_dir.join(&staging_name);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging_path)
            .await
            .map_err(|e| map_io_error("create staging file", &e))?;

        let written = self
            .stream_to_staging(&mut file, input.body, current_used)
            .await;
        let (size_bytes, sha256) = match written {
            Ok(pair) => pair,
            Err(err) => {
                drop(file);
                // Nếu lỗi do fault injection `fail_during_staging`, giữ lại file `.part` trong
                // `staging/` đúng như cửa sổ crash để test kiểm chứng file mồ côi.
                if err.code != "STAGING_FAULT" {
                    let _ = fs::remove_file(&staging_path).await;
                }
                return Err(err);
            }
        };

        if let Err(e) = file.sync_all().await {
            drop(file);
            let _ = fs::remove_file(&staging_path).await;
            return Err(map_io_error("fsync staging file", &e));
        }
        drop(file);

        let storage_key = new_id();
        let committed_path = self.committed_dir.join(&storage_key);
        if let Err(e) = fs::rename(&staging_path, &committed_path).await {
            let _ = fs::remove_file(&staging_path).await;
            return Err(map_io_error("atomic rename artifact", &e));
        }
        sync_directory(&self.committed_dir);

        if self.fault.should_fail_after_rename() {
            // Cửa sổ crash sau khi đã rename sang `committed/` nhưng trước khi ghi SQLite:
            // file mồ côi nằm lại trong `committed/`, không có row trong `artifacts`.
            return Err(ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "injected fault after atomic rename before metadata commit".into(),
            });
        }

        let artifact_id = new_ordered_artifact_id();
        let reference = ArtifactRef {
            id: artifact_id.clone(),
            name,
            mime_type,
            size_bytes,
            sha256,
            role: input.role,
        };

        if let Err(err) = self.publish_metadata(scope, &reference, &storage_key).await {
            let _ = fs::remove_file(&committed_path).await;
            return Err(err);
        }

        Ok(reference)
    }

    async fn stream_to_staging(
        &self,
        file: &mut File,
        body: InputBody,
        current_used: u64,
    ) -> Result<(u64, String), ToolError> {
        let mut hasher = Sha256::new();
        let mut size_bytes = 0u64;

        match body {
            InputBody::Bytes(bytes) => {
                for chunk in bytes.chunks(STREAM_CHUNK_BYTES) {
                    self.write_chunk(file, chunk, &mut size_bytes, &mut hasher, current_used)
                        .await?;
                }
            }
            InputBody::Reader(mut reader) => {
                let mut buf = vec![0u8; STREAM_CHUNK_BYTES];
                loop {
                    let n = reader
                        .read(&mut buf)
                        .await
                        .map_err(|e| map_io_error("read artifact input stream", &e))?;
                    if n == 0 {
                        break;
                    }
                    self.write_chunk(file, &buf[..n], &mut size_bytes, &mut hasher, current_used)
                        .await?;
                }
            }
        }

        Ok((size_bytes, hex_digest(hasher.finalize())))
    }

    async fn write_chunk(
        &self,
        file: &mut File,
        chunk: &[u8],
        size_bytes: &mut u64,
        hasher: &mut Sha256,
        current_used: u64,
    ) -> Result<(), ToolError> {
        *size_bytes = size_bytes.saturating_add(chunk.len() as u64);
        if *size_bytes > self.limits.max_bytes {
            return Err(ToolError {
                code: CODE_ARTIFACT_TOO_LARGE.into(),
                message: format!(
                    "artifact exceeds the {} byte maximum limit",
                    self.limits.max_bytes
                ),
            });
        }
        if current_used.saturating_add(*size_bytes) > self.limits.workspace_quota_bytes {
            return Err(ToolError {
                code: CODE_WORKSPACE_QUOTA_EXCEEDED.into(),
                message: "workspace artifact storage quota exceeded".into(),
            });
        }
        self.check_disk_reserve(*size_bytes)?;
        file.write_all(chunk)
            .await
            .map_err(|e| map_io_error("write staging chunk", &e))?;
        if self.fault.should_fail_staging() {
            let _ = file.flush().await;
            return Err(ToolError {
                code: "STAGING_FAULT".into(),
                message: "injected fault while staging artifact bytes".into(),
            });
        }
        hasher.update(chunk);
        Ok(())
    }

    fn check_disk_reserve(&self, additional_bytes: u64) -> Result<(), ToolError> {
        if let Some(free) = self.fault.simulated_free_bytes() {
            if free.saturating_sub(additional_bytes) < self.limits.disk_reserve_bytes {
                return Err(ToolError {
                    code: CODE_DISK_RESERVE_EXCEEDED.into(),
                    message: "available disk space is below the configured disk_reserve_bytes"
                        .into(),
                });
            }
        }
        Ok(())
    }

    async fn workspace_used_bytes(
        &self,
        owner_id: &str,
        workspace_id: &str,
    ) -> Result<u64, ToolError> {
        let used: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(size_bytes), 0) FROM artifacts \
             WHERE owner_id = ?1 AND workspace_id = ?2",
        )
        .bind(owner_id)
        .bind(workspace_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not query workspace artifact quota".into(),
        })?;
        Ok(used.max(0) as u64)
    }

    async fn publish_metadata(
        &self,
        scope: &ArtifactWriteScope,
        reference: &ArtifactRef,
        storage_key: &str,
    ) -> Result<(), ToolError> {
        let mut tx = self.pool.begin().await.map_err(|_| ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not begin artifact metadata transaction".into(),
        })?;

        // Ghi đầu tiên để lấy write lock ngay và tăng revision của session sở hữu.
        let bumped = sqlx::query(
            "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = ?1 AND owner_id = ?2 AND workspace_id = ?3",
        )
        .bind(&scope.session_id)
        .bind(&scope.owner_id)
        .bind(&scope.workspace_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not lock session for artifact publication".into(),
        })?
        .rows_affected();
        if bumped != 1 {
            let _ = tx.rollback().await;
            return Err(ToolError {
                code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
                message: "session is outside the tool's authorized scope".into(),
            });
        }

        if let Some(inv_id) = &scope.producer_invocation_id {
            let valid_inv: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM tool_invocations ti \
                 JOIN agent_runs r ON r.id = ti.run_id \
                 WHERE ti.id = ?1 AND ti.session_id = ?2 AND ti.run_id = ?3 \
                   AND ti.status = 'running' AND r.turn_id = ?4 AND r.status = 'running'",
            )
            .bind(inv_id)
            .bind(&scope.session_id)
            .bind(&scope.run_id)
            .bind(&scope.turn_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not verify producer invocation state".into(),
            })?;
            if valid_inv.is_none() {
                let _ = tx.rollback().await;
                return Err(ToolError {
                    code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
                    message: "producer invocation is no longer active for this turn".into(),
                });
            }
        } else {
            let valid_turn: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM agent_runs WHERE session_id = ?1 AND turn_id = ?2 LIMIT 1",
            )
            .bind(&scope.session_id)
            .bind(&scope.turn_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not verify turn ownership".into(),
            })?;
            if valid_turn.is_none() {
                let _ = tx.rollback().await;
                return Err(ToolError {
                    code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
                    message: "turn does not belong to the session".into(),
                });
            }
        }

        let used: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(size_bytes), 0) FROM artifacts \
             WHERE owner_id = ?1 AND workspace_id = ?2",
        )
        .bind(&scope.owner_id)
        .bind(&scope.workspace_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not verify workspace quota".into(),
        })?;
        if (used.max(0) as u64).saturating_add(reference.size_bytes)
            > self.limits.workspace_quota_bytes
        {
            let _ = tx.rollback().await;
            return Err(ToolError {
                code: CODE_WORKSPACE_QUOTA_EXCEEDED.into(),
                message: "workspace artifact storage quota exceeded".into(),
            });
        }

        sqlx::query(
            "INSERT INTO artifacts(id, owner_id, workspace_id, session_id, turn_id, \
             producer_invocation_id, role, name, mime_type, size_bytes, sha256, storage_key) \
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        )
        .bind(&reference.id)
        .bind(&scope.owner_id)
        .bind(&scope.workspace_id)
        .bind(&scope.session_id)
        .bind(&scope.turn_id)
        .bind(&scope.producer_invocation_id)
        .bind(reference.role.as_str())
        .bind(&reference.name)
        .bind(&reference.mime_type)
        .bind(reference.size_bytes as i64)
        .bind(&reference.sha256)
        .bind(storage_key)
        .execute(&mut *tx)
        .await
        .map_err(|_| ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not insert artifact metadata".into(),
        })?;

        tx.commit().await.map_err(|_| ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not commit artifact metadata".into(),
        })?;
        Ok(())
    }

    /// Mở artifact cho host (`Orchestrator::open_artifact`): kiểm tra owner/workspace, kiểm tra
    /// `download_max_bytes` và kiểm tra file vật lý tồn tại + đúng kích thước.
    pub(crate) async fn open_for_host(
        &self,
        caller: &CallerContext,
        artifact_id: &str,
    ) -> Result<ArtifactDownload, OrchestratorError> {
        let stored = self
            .load_stored(artifact_id)
            .await?
            .ok_or_else(|| OrchestratorError::new(ErrorCode::NotFound, "artifact not found"))?;
        if stored.owner_id != caller.owner_id || stored.workspace_id != caller.workspace_id {
            return Err(OrchestratorError::new(
                ErrorCode::PermissionDenied,
                "the artifact is outside the caller's scope",
            ));
        }
        if stored.artifact.size_bytes > self.limits.download_max_bytes {
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "artifact exceeds download_max_bytes",
            ));
        }
        let reader = self
            .open_verified_reader(
                stored.artifact.clone(),
                &stored.storage_key,
                self.limits.download_max_bytes,
            )
            .await?;
        Ok(ArtifactDownload {
            artifact: stored.artifact,
            reader,
        })
    }

    /// Mở artifact cho `HostTool` (`ToolContext::artifacts().open`):
    /// Enforce:
    /// 1) Cùng `owner_id` và `workspace_id`.
    /// 2) Session tạo artifact thuộc cùng session family (`scope.root_session_id`).
    ///    Cùng workspace nhưng khác session family bị từ chối!
    /// 3) Explicit invocation input / checkpoint ancestry:
    ///    `artifact_id` phải do chính invocation hiện tại tạo ra, hoặc được truyền tường minh
    ///    trong `arguments_json` của invocation hiện tại, hoặc là `checkpoint_artifact_id`
    ///    của invocation hiện tại (hoặc được tham chiếu bên trong manifest của `checkpoint_artifact_id`).
    pub(crate) async fn open_for_tool(
        &self,
        scope: &ToolScope,
        artifact_id: &str,
    ) -> Result<ArtifactReader, ToolError> {
        let stored = self
            .load_stored(artifact_id)
            .await
            .map_err(|_| ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not read artifact metadata".into(),
            })?
            .ok_or_else(|| ToolError {
                code: CODE_ARTIFACT_NOT_FOUND.into(),
                message: "artifact not found".into(),
            })?;

        self.authorize_tool_access(scope, &stored).await?;

        self.open_verified_reader(stored.artifact, &stored.storage_key, self.limits.max_bytes)
            .await
            .map_err(|e| match e.code() {
                ErrorCode::InvalidInput => ToolError {
                    code: CODE_ARTIFACT_TOO_LARGE.into(),
                    message: e.message().to_owned(),
                },
                _ => ToolError {
                    code: CODE_ARTIFACT_INTEGRITY.into(),
                    message: e.message().to_owned(),
                },
            })
    }

    /// Xác minh một artifact tồn tại, thuộc đúng scope của invocation và có bytes nguyên vẹn
    /// trên đĩa trước khi cho phép CAS vào `tool_invocations.checkpoint_artifact_id`.
    pub(crate) async fn verify_for_checkpoint_commit(
        &self,
        scope: &ToolScope,
        manifest_id: &str,
    ) -> Result<(), ToolError> {
        let mut reader = self.open_for_tool(scope, manifest_id).await?;
        while reader
            .read_chunk(STREAM_CHUNK_BYTES)
            .await
            .map_err(|e| ToolError {
                code: CODE_ARTIFACT_INTEGRITY.into(),
                message: e.message().to_owned(),
            })?
            .is_some()
        {}
        Ok(())
    }

    /// Xác minh hoàn chỉnh một artifact trong cùng session family (dùng cho `checkpoint().current()`
    /// và `resolve_interrupted_invocation`).
    pub(crate) async fn verify_in_family(
        &self,
        owner_id: &str,
        workspace_id: &str,
        root_session_id: &str,
        artifact_id: &str,
    ) -> Result<ArtifactRef, ToolError> {
        let stored = self
            .load_stored(artifact_id)
            .await
            .map_err(|_| ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not read artifact metadata".into(),
            })?
            .ok_or_else(|| ToolError {
                code: CODE_ARTIFACT_NOT_FOUND.into(),
                message: "artifact not found".into(),
            })?;
        if stored.owner_id != owner_id
            || stored.workspace_id != workspace_id
            || stored.root_session_id != root_session_id
        {
            return Err(ToolError {
                code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
                message: "artifact is outside the session family scope".into(),
            });
        }
        let mut reader = self
            .open_verified_reader(
                stored.artifact.clone(),
                &stored.storage_key,
                self.limits.download_max_bytes,
            )
            .await
            .map_err(|e| ToolError {
                code: CODE_ARTIFACT_INTEGRITY.into(),
                message: e.message().to_owned(),
            })?;
        while reader
            .read_chunk(STREAM_CHUNK_BYTES)
            .await
            .map_err(|e| ToolError {
                code: CODE_ARTIFACT_INTEGRITY.into(),
                message: e.message().to_owned(),
            })?
            .is_some()
        {}
        Ok(stored.artifact)
    }

    async fn authorize_tool_access(
        &self,
        scope: &ToolScope,
        stored: &StoredArtifact,
    ) -> Result<(), ToolError> {
        let deny = |msg: &str| ToolError {
            code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
            message: msg.to_owned(),
        };
        if stored.owner_id != scope.owner_id || stored.workspace_id != scope.workspace_id {
            return Err(deny("artifact belongs to a different owner or workspace"));
        }
        // Cùng workspace không tự cấp quyền đọc: phải thuộc cùng session family.
        if stored.root_session_id != scope.root_session_id {
            return Err(deny("artifact belongs to a different session family"));
        }
        // Chính invocation hiện tại tạo ra artifact này.
        if stored.producer_invocation_id.as_deref() == Some(scope.invocation_id.as_str()) {
            return Ok(());
        }

        let inv_row = sqlx::query(
            "SELECT arguments_json, checkpoint_artifact_id FROM tool_invocations \
             WHERE id = ?1 AND session_id = ?2 AND run_id = ?3",
        )
        .bind(&scope.invocation_id)
        .bind(&scope.session_id)
        .bind(&scope.run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ToolError {
            code: CODE_ARTIFACT_UNAVAILABLE.into(),
            message: "could not verify invocation input scope".into(),
        })?
        .ok_or_else(|| deny("invocation is not active in this session"))?;

        let args_json: String = inv_row.try_get("arguments_json").unwrap_or_default();
        let checkpoint_id: Option<String> =
            inv_row.try_get("checkpoint_artifact_id").unwrap_or(None);

        let target_id = stored.artifact.id.as_str();
        if json_mentions_id(&args_json, target_id) {
            return Ok(());
        }
        if checkpoint_id.as_deref() == Some(target_id) {
            return Ok(());
        }
        // Nếu invocation đã có committed checkpoint manifest, cho phép đọc các artifact IDs
        // được ghi bên trong manifest đó (ví dụ các completed item refs khi resume).
        if let Some(cp_id) = checkpoint_id {
            if let Ok(Some(cp_stored)) = self.load_stored(&cp_id).await {
                if cp_stored.owner_id == scope.owner_id
                    && cp_stored.workspace_id == scope.workspace_id
                    && cp_stored.root_session_id == scope.root_session_id
                {
                    if let Ok(reader) = self
                        .open_verified_reader(
                            cp_stored.artifact,
                            &cp_stored.storage_key,
                            self.limits.max_bytes,
                        )
                        .await
                    {
                        if let Ok(text) = reader.read_to_string().await {
                            if json_mentions_id(&text, target_id) {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }

        Err(deny(
            "artifact is not in the invocation's explicit input or checkpoint scope",
        ))
    }

    async fn open_verified_reader(
        &self,
        artifact: ArtifactRef,
        storage_key: &str,
        max_bytes: u64,
    ) -> Result<ArtifactReader, OrchestratorError> {
        if !is_safe_storage_key(storage_key) {
            return Err(integrity_orchestrator_error("invalid artifact storage key"));
        }
        if artifact.size_bytes > max_bytes {
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "artifact exceeds the configured read byte limit",
            ));
        }
        let path = self.committed_dir.join(storage_key);
        let file = File::open(&path)
            .await
            .map_err(|_| integrity_orchestrator_error("committed artifact file is missing"))?;
        let meta = file
            .metadata()
            .await
            .map_err(|_| integrity_orchestrator_error("could not stat committed artifact file"))?;
        if !meta.is_file() || meta.len() != artifact.size_bytes {
            return Err(integrity_orchestrator_error(
                "committed artifact file size does not match metadata",
            ));
        }
        Ok(ArtifactReader {
            artifact,
            file,
            max_bytes,
            read_so_far: 0,
            hasher: Sha256::new(),
            verified: false,
        })
    }

    async fn load_stored(
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

    async fn list_for_turn_with_max(
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

    async fn query_turn_page(
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

    /// Đọc một đoạn văn bản UTF-8 có giới hạn cho built-in tool `artifact_read` của model
    /// trong phạm vi cùng `(owner_id, workspace_id, root_session_id)` (docs/17 §17.3, §17.5).
    /// Luôn xác minh toàn vẹn SHA-256 của file committed trước khi trích xuất đoạn con.
    pub(crate) async fn read_model_excerpt(
        &self,
        scope: &ToolScope,
        artifact_id: &str,
        offset_bytes: u64,
        max_bytes: usize,
    ) -> Result<(ArtifactRef, String, bool), ToolError> {
        let stored = self
            .load_stored(artifact_id)
            .await
            .map_err(|_| ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not read artifact metadata".into(),
            })?
            .ok_or_else(|| ToolError {
                code: CODE_ARTIFACT_NOT_FOUND.into(),
                message: "artifact not found".into(),
            })?;
        if stored.owner_id != scope.owner_id
            || stored.workspace_id != scope.workspace_id
            || stored.root_session_id != scope.root_session_id
        {
            return Err(ToolError {
                code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
                message: "artifact is outside the session family scope".into(),
            });
        }
        let reader = self
            .open_verified_reader(
                stored.artifact.clone(),
                &stored.storage_key,
                self.limits.max_bytes,
            )
            .await
            .map_err(|e| ToolError {
                code: CODE_ARTIFACT_INTEGRITY.into(),
                message: e.message().to_owned(),
            })?;
        let text = reader.read_to_string().await.map_err(|e| match e.code() {
            ErrorCode::InvalidInput => ToolError {
                code: CODE_ARTIFACT_INVALID_INPUT.into(),
                message: e.message().to_owned(),
            },
            _ => ToolError {
                code: CODE_ARTIFACT_INTEGRITY.into(),
                message: e.message().to_owned(),
            },
        })?;
        let total = text.len();
        let mut start = usize::try_from(offset_bytes).unwrap_or(total).min(total);
        while start > 0 && !text.is_char_boundary(start) {
            start -= 1;
        }
        let mut end = start.saturating_add(max_bytes).min(total);
        while end > start && !text.is_char_boundary(end) {
            end -= 1;
        }
        let has_more = end < total;
        Ok((stored.artifact, text[start..end].to_owned(), has_more))
    }

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

struct StoredArtifact {
    artifact: ArtifactRef,
    owner_id: String,
    workspace_id: String,
    root_session_id: String,
    producer_invocation_id: Option<String>,
    storage_key: String,
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

fn validate_name(raw: &str) -> Result<String, ToolError> {
    let trimmed = raw.trim();
    let bad = |msg: &str| ToolError {
        code: CODE_ARTIFACT_INVALID_INPUT.into(),
        message: msg.to_owned(),
    };
    if trimmed.is_empty() || trimmed.chars().count() > MAX_NAME_CHARS {
        return Err(bad("artifact name must be 1..=255 characters"));
    }
    if trimmed == "."
        || trimmed == ".."
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.chars().any(char::is_control)
    {
        return Err(bad(
            "artifact name must be a plain display filename without path separators or control characters",
        ));
    }
    Ok(trimmed.to_owned())
}

fn validate_mime(raw: &str) -> Result<String, ToolError> {
    let trimmed = raw.trim();
    let bad = |msg: &str| ToolError {
        code: CODE_ARTIFACT_INVALID_INPUT.into(),
        message: msg.to_owned(),
    };
    if trimmed.is_empty()
        || trimmed.len() > MAX_MIME_CHARS
        || !trimmed.contains('/')
        || trimmed.starts_with('/')
        || trimmed.ends_with('/')
        || trimmed
            .chars()
            .any(|c| !c.is_ascii_graphic() && c != ' ' && c != ';')
    {
        return Err(bad(
            "mime_type must be a valid media type (e.g. text/markdown)",
        ));
    }
    Ok(trimmed.to_owned())
}

fn is_safe_storage_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 128
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let slice = bytes.as_ref();
    let mut out = String::with_capacity(slice.len() * 2);
    for b in slice {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn integrity_orchestrator_error(detail: &'static str) -> OrchestratorError {
    tracing::warn!(detail, "artifact integrity check failed");
    OrchestratorError::new(
        ErrorCode::Unavailable,
        format!("{CODE_ARTIFACT_INTEGRITY}: {detail}"),
    )
}

fn map_io_error(context: &'static str, err: &io::Error) -> ToolError {
    tracing::warn!(context, error = %err, "artifact storage I/O error");
    ToolError {
        code: CODE_ARTIFACT_UNAVAILABLE.into(),
        message: "artifact storage operation failed".into(),
    }
}

fn sync_directory(dir: &Path) {
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
}

async fn is_expired(entry: &fs::DirEntry, now: SystemTime, grace: Duration) -> bool {
    let Ok(meta) = entry.metadata().await else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    if grace.is_zero() {
        return true;
    }
    let Ok(modified) = meta.modified() else {
        return false;
    };
    now.duration_since(modified).unwrap_or_default() >= grace
}

/// Kiểm tra một chuỗi JSON có chứa đúng string literal `target_id` ở bất kỳ trường nào hay không.
pub(crate) fn json_mentions_id(raw_json: &str, target_id: &str) -> bool {
    if target_id.is_empty() {
        return false;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw_json) else {
        return false;
    };
    value_mentions_id(&value, target_id)
}

fn value_mentions_id(v: &serde_json::Value, target_id: &str) -> bool {
    match v {
        serde_json::Value::String(s) => s == target_id,
        serde_json::Value::Array(items) => items.iter().any(|x| value_mentions_id(x, target_id)),
        serde_json::Value::Object(map) => map.values().any(|x| value_mentions_id(x, target_id)),
        _ => false,
    }
}

async fn copy_and_verify_artifact(
    src: &Path,
    dst: &Path,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<(), OrchestratorError> {
    let mut reader = File::open(src).await.map_err(|_| {
        integrity_orchestrator_error("committed artifact file is missing during backup")
    })?;
    let mut writer = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dst)
        .await
        .map_err(|e| OrchestratorError::storage("create backup artifact file", &e))?;
    let mut hasher = Sha256::new();
    let mut copied = 0u64;
    let mut buf = vec![0u8; STREAM_CHUNK_BYTES];
    loop {
        let n = reader
            .read(&mut buf)
            .await
            .map_err(|e| OrchestratorError::storage("read source artifact for backup", &e))?;
        if n == 0 {
            break;
        }
        copied = copied.saturating_add(n as u64);
        if copied > expected_size {
            let _ = fs::remove_file(dst).await;
            return Err(integrity_orchestrator_error(
                "committed artifact size exceeds metadata during backup",
            ));
        }
        hasher.update(&buf[..n]);
        writer
            .write_all(&buf[..n])
            .await
            .map_err(|e| OrchestratorError::storage("write backup artifact file", &e))?;
    }
    if copied != expected_size {
        let _ = fs::remove_file(dst).await;
        return Err(integrity_orchestrator_error(
            "committed artifact file is truncated during backup",
        ));
    }
    if hex_digest(hasher.finalize()) != expected_sha256 {
        let _ = fs::remove_file(dst).await;
        return Err(integrity_orchestrator_error(
            "committed artifact sha256 mismatch during backup",
        ));
    }
    writer
        .sync_all()
        .await
        .map_err(|e| OrchestratorError::storage("fsync backup artifact file", &e))?;
    Ok(())
}

fn encode_artifact_cursor(
    session_id: &str,
    turn_id: &str,
    created_at: i64,
    last_id: &str,
) -> String {
    let raw = format!("{CURSOR_VERSION}|{session_id}|{turn_id}|{created_at}|{last_id}");
    raw.bytes().map(|b| format!("{b:02x}")).collect()
}

fn decode_artifact_cursor(
    cursor: &str,
    session_id: &str,
    turn_id: &str,
) -> Result<(i64, String), OrchestratorError> {
    let bad = || OrchestratorError::new(ErrorCode::InvalidInput, "invalid cursor for this query");
    if cursor.is_empty() || !cursor.len().is_multiple_of(2) || !cursor.is_ascii() {
        return Err(bad());
    }
    let bytes: Result<Vec<u8>, _> = (0..cursor.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&cursor[i..i + 2], 16))
        .collect();
    let raw = String::from_utf8(bytes.map_err(|_| bad())?).map_err(|_| bad())?;
    let mut parts = raw.splitn(5, '|');
    match (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        (Some(v), Some(s), Some(t), Some(ts), Some(id))
            if v == CURSOR_VERSION && s == session_id && t == turn_id && !id.is_empty() =>
        {
            let parsed_ts = ts.parse::<i64>().map_err(|_| bad())?;
            Ok((parsed_ts, id.to_owned()))
        }
        _ => Err(bad()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{RunRoute, SessionStore, StoredToolCall, DB_FILE};

    fn caller() -> CallerContext {
        CallerContext::new("owner-1", "ws-1")
    }

    fn route(_: &str) -> Option<RunRoute> {
        Some(RunRoute {
            provider_id: "p".into(),
            model_id: "m".into(),
            fingerprint: "v1:fp".into(),
        })
    }

    async fn setup() -> (
        tempfile::TempDir,
        SessionStore,
        ArtifactStore,
        ArtifactWriteScope,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::open(&dir.path().join(DB_FILE)).await.unwrap();
        let limits = ArtifactLimits::from_runtime(&RuntimeConfig::default());
        let artifacts = ArtifactStore::open(store.pool().clone(), dir.path(), limits, None)
            .await
            .unwrap();
        let sid = store.create_root_session(&caller(), "root").await.unwrap();
        let turn = store
            .admit_turn(&caller(), &sid, "hello", route)
            .await
            .unwrap();
        let run_id = store.running_run_ids().await.unwrap().remove(0);
        let calls = vec![StoredToolCall {
            id: "call-1".into(),
            name: "writer".into(),
            arguments: serde_json::json!({}),
        }];
        let inv = store
            .commit_tool_calls(&run_id, "", &calls)
            .await
            .unwrap()
            .unwrap()
            .remove(0);
        assert!(store.begin_invocation(&run_id, &inv.id).await.unwrap());
        let scope = ArtifactWriteScope {
            owner_id: "owner-1".into(),
            workspace_id: "ws-1".into(),
            session_id: sid,
            turn_id: turn.turn_id,
            run_id,
            producer_invocation_id: Some(inv.id),
        };
        (dir, store, artifacts, scope)
    }

    #[tokio::test]
    async fn put_open_and_list_roundtrip_with_integrity_verification() {
        let (_dir, _store, artifacts, scope) = setup().await;
        let payload = b"# Heading\n\nArtifact body content.\n";
        let r = artifacts
            .put(
                &scope,
                ArtifactInput::primary_bytes("report.md", "text/markdown", payload.to_vec()),
            )
            .await
            .unwrap();
        assert_eq!(r.name, "report.md");
        assert_eq!(r.mime_type, "text/markdown");
        assert_eq!(r.size_bytes, payload.len() as u64);
        assert_eq!(r.role, ArtifactRole::Primary);

        let page = artifacts
            .list_for_turn(
                &caller(),
                &scope.session_id,
                &scope.turn_id,
                PageRequest::first(10),
            )
            .await
            .unwrap();
        assert_eq!(page.items, vec![r.clone()]);
        assert_eq!(page.next_cursor, None);

        let dl = artifacts.open_for_host(&caller(), &r.id).await.unwrap();
        assert_eq!(dl.metadata(), &r);
        let read_back = dl.read_all().await.unwrap();
        assert_eq!(read_back, payload);

        // Sửa 1 byte trên đĩa -> phát hiện lỗi SHA-256 ngay khi đọc, không trả về thành công giả.
        let mut entries = std::fs::read_dir(artifacts.committed_dir()).unwrap();
        let file_path = entries.next().unwrap().unwrap().path();
        std::fs::write(&file_path, b"# Heading\n\nArtifact body CORRUPT.\n").unwrap();
        let dl_corrupt = artifacts.open_for_host(&caller(), &r.id).await.unwrap();
        let err = dl_corrupt.read_all().await.unwrap_err();
        assert_eq!(err.code(), ErrorCode::Unavailable);
        assert!(err.message().contains(CODE_ARTIFACT_INTEGRITY));
    }

    #[tokio::test]
    async fn fault_injection_and_orphan_sweep_preserve_committed_artifacts() {
        let (_dir, _store, artifacts, scope) = setup().await;
        let committed_ref = artifacts
            .put(
                &scope,
                ArtifactInput::primary_bytes("keep.txt", "text/plain", b"committed".to_vec()),
            )
            .await
            .unwrap();

        // 1) Lỗi giữa chừng khi staging -> để lại file `.part` trong `staging/`.
        artifacts.fault().set_fail_during_staging(true);
        let err = artifacts
            .put(
                &scope,
                ArtifactInput::primary_bytes("stage-fail.txt", "text/plain", b"partial".to_vec()),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, "STAGING_FAULT");
        artifacts.fault().set_fail_during_staging(false);
        assert_eq!(
            std::fs::read_dir(artifacts.staging_dir()).unwrap().count(),
            1
        );

        // 2) Lỗi sau khi rename sang `committed/` nhưng trước khi ghi SQLite metadata.
        artifacts
            .fault()
            .set_fail_after_rename_before_metadata(true);
        let err2 = artifacts
            .put(
                &scope,
                ArtifactInput::primary_bytes("orphan.txt", "text/plain", b"orphan".to_vec()),
            )
            .await
            .unwrap_err();
        assert_eq!(err2.code, CODE_ARTIFACT_UNAVAILABLE);
        artifacts
            .fault()
            .set_fail_after_rename_before_metadata(false);
        // Có 2 file trong `committed/` nhưng chỉ có 1 row trong SQLite.
        assert_eq!(
            std::fs::read_dir(artifacts.committed_dir())
                .unwrap()
                .count(),
            2
        );
        let page = artifacts
            .list_for_turn(
                &caller(),
                &scope.session_id,
                &scope.turn_id,
                PageRequest::first(10),
            )
            .await
            .unwrap();
        assert_eq!(page.items, vec![committed_ref.clone()]);

        // 3) Sweep với grace lớn -> chưa xóa file mới tạo.
        assert_eq!(
            artifacts
                .sweep_orphans(Duration::from_secs(3600))
                .await
                .unwrap(),
            0
        );

        // 4) Sweep với grace = 0 -> xóa đúng 1 file staging + 1 file committed mồ côi;
        //    artifact đã commit trong SQLite được giữ nguyên vẹn!
        assert_eq!(artifacts.sweep_orphans(Duration::ZERO).await.unwrap(), 2);
        assert_eq!(
            std::fs::read_dir(artifacts.staging_dir()).unwrap().count(),
            0
        );
        assert_eq!(
            std::fs::read_dir(artifacts.committed_dir())
                .unwrap()
                .count(),
            1
        );
        let bytes = artifacts
            .open_for_host(&caller(), &committed_ref.id)
            .await
            .unwrap()
            .read_all()
            .await
            .unwrap();
        assert_eq!(bytes, b"committed");
    }
}
