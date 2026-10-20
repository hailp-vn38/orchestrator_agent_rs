//! Reader/download có giới hạn byte và kiểm tra toàn vẹn SHA-256.

use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

use super::errors::integrity_orchestrator_error;
use super::fsutil::hex_digest;
use super::STREAM_CHUNK_BYTES;
use crate::error::{ErrorCode, OrchestratorError};
use crate::types::ArtifactRef;

/// Reader async có giới hạn byte và kiểm tra toàn vẹn SHA-256 trên dòng dữ liệu đọc ra.
/// Không bao giờ làm lộ đường dẫn filesystem nội bộ.
pub struct ArtifactReader {
    pub(super) artifact: ArtifactRef,
    pub(super) file: File,
    pub(super) max_bytes: u64,
    pub(super) read_so_far: u64,
    pub(super) hasher: Sha256,
    pub(super) verified: bool,
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
    pub(super) reader: ArtifactReader,
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
