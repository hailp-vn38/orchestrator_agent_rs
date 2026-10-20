//! Exclusive process owner lock cho execution store (EMB-07, docs/09 §9.10).
//!
//! OS advisory lock giữ suốt vòng đời engine; bảo vệ startup recovery và owner handoff.
//! Khác với unique partial index (one active run/session) — không thay thế nhau.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;

use crate::error::{ErrorCode, OrchestratorError};

/// Giữ lock đến khi bị drop. Chỉ được drop sau khi mọi local write-capable task đã join.
#[derive(Debug)]
pub(crate) struct OwnerLock {
    _file: File,
}

impl OwnerLock {
    /// Non-blocking. Store đang có owner khác → `STORE_ALREADY_OWNED`.
    pub(crate) fn acquire(path: &Path) -> Result<Self, OrchestratorError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
            .map_err(|e| OrchestratorError::storage("open owner lock", &e))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(OrchestratorError::new(
                ErrorCode::StoreAlreadyOwned,
                "execution store is already owned by another orchestrator instance",
            )),
            Err(TryLockError::Error(e)) => Err(OrchestratorError::storage("lock owner file", &e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusive_and_released_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("owner.lock");
        let a = OwnerLock::acquire(&p).unwrap();
        let err = OwnerLock::acquire(&p).unwrap_err();
        assert_eq!(err.code(), ErrorCode::StoreAlreadyOwned);
        drop(a);
        assert!(OwnerLock::acquire(&p).is_ok());
    }
}
