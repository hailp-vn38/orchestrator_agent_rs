//! Tiện ích filesystem/băm dùng chung.

use std::path::Path;
use std::time::{Duration, SystemTime};

use sha2::{Digest, Sha256};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::errors::integrity_orchestrator_error;
use super::STREAM_CHUNK_BYTES;
use crate::error::OrchestratorError;

pub(super) fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let slice = bytes.as_ref();
    let mut out = String::with_capacity(slice.len() * 2);
    for b in slice {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

pub(super) fn sync_directory(dir: &Path) {
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
}

pub(super) async fn is_expired(entry: &fs::DirEntry, now: SystemTime, grace: Duration) -> bool {
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

pub(super) async fn copy_and_verify_artifact(
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
