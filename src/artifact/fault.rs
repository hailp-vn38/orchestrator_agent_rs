//! Fault hook cho crash/failpoint tests.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(any(test, feature = "test-support"))]
use std::sync::Arc;

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
