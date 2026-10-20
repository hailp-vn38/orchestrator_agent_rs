//! Luồng xuất bản bất biến: staging → fsync → rename nguyên tử → metadata SQLite.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use sha2::{Digest, Sha256};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::errors::*;
use super::fsutil::{hex_digest, sync_directory};
use super::input::{ArtifactInput, InputBody};
use super::scope::ArtifactWriteScope;
use super::store::ArtifactStore;
use super::validate::{validate_mime, validate_name};
use super::STREAM_CHUNK_BYTES;
use crate::store::{new_id, PublishError, PublishScope};
use crate::tool::ToolError;
use crate::types::ArtifactRef;

static ARTIFACT_ID_SEQ: AtomicU64 = AtomicU64::new(0);

pub(super) fn new_ordered_artifact_id() -> String {
    let ts_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let seq = ARTIFACT_ID_SEQ.fetch_add(1, Ordering::Relaxed) & 0xffff;
    format!("art-{ts_ms:012x}-{seq:04x}-{}", new_id())
}

impl ArtifactStore {
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

    pub(super) async fn stream_to_staging(
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

    pub(super) async fn write_chunk(
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

    pub(super) fn check_disk_reserve(&self, additional_bytes: u64) -> Result<(), ToolError> {
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

    pub(super) async fn workspace_used_bytes(
        &self,
        owner_id: &str,
        workspace_id: &str,
    ) -> Result<u64, ToolError> {
        let used = self
            .db
            .artifact_workspace_used_bytes(owner_id, workspace_id)
            .await
            .map_err(|_| ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not query workspace artifact quota".into(),
            })?;
        Ok(used)
    }

    pub(super) async fn publish_metadata(
        &self,
        scope: &ArtifactWriteScope,
        reference: &ArtifactRef,
        storage_key: &str,
    ) -> Result<(), ToolError> {
        let publish_scope = PublishScope {
            owner_id: &scope.owner_id,
            workspace_id: &scope.workspace_id,
            session_id: &scope.session_id,
            turn_id: &scope.turn_id,
            run_id: &scope.run_id,
            producer_invocation_id: scope.producer_invocation_id.as_deref(),
        };
        self.db
            .artifact_publish(
                &publish_scope,
                reference,
                storage_key,
                self.limits.workspace_quota_bytes,
            )
            .await
            .map_err(|e| match e {
                PublishError::Db(message) => ToolError {
                    code: CODE_ARTIFACT_UNAVAILABLE.into(),
                    message: message.into(),
                },
                PublishError::SessionOutOfScope => ToolError {
                    code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
                    message: "session is outside the tool's authorized scope".into(),
                },
                PublishError::InvocationInactive => ToolError {
                    code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
                    message: "producer invocation is no longer active for this turn".into(),
                },
                PublishError::TurnNotInSession => ToolError {
                    code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
                    message: "turn does not belong to the session".into(),
                },
                PublishError::QuotaExceeded => ToolError {
                    code: CODE_WORKSPACE_QUOTA_EXCEEDED.into(),
                    message: "workspace artifact storage quota exceeded".into(),
                },
            })
    }
}
