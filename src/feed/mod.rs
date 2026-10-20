//! `ExecutionFeed` (docs/16): fan-out **best-effort** của sự kiện quan sát, transport-neutral.
//!
//! - SQLite là source of truth; feed chỉ là gợi ý live. Event committed (`state_revision`
//!   `Some`) chỉ được publish **sau** commit và chỉ *invalidate* canonical view — payload không
//!   phải patch. Event ephemeral (`text.delta`, `tool.progress`, `step.*`, `compaction.started`)
//!   có thể mất và không bao giờ ghi đè terminal state từ snapshot.
//! - Một `tokio::sync::broadcast` bounded cho cả process: publisher không bao giờ chờ subscriber;
//!   subscriber chậm nhận `Lagged` → [`FeedItem::ResyncRequired`] rồi bị đóng riêng.
//! - Không có event log/replay: không `Last-Event-ID`, revision không phải resume cursor.
//! - Payload không chứa arguments/kết quả tool, secret hay reasoning ẩn: gateway chỉ chuyển text
//!   đã hiển thị, và giá trị API key đã cấu hình bị che trong mọi text ephemeral.

use std::sync::Mutex;

mod broadcast;
mod emitter;
pub(crate) mod event;
#[cfg(feature = "test-support")]
mod fault;
mod progress;
mod redaction;
mod subscription;
#[cfg(test)]
mod tests;
mod types;

pub(crate) use broadcast::ExecutionFeed;
pub(crate) use emitter::Emitter;
#[cfg(feature = "test-support")]
pub use fault::FeedFault;
pub(crate) use progress::ProgressSink;
pub(crate) use redaction::{chunks, DeltaFilter};
pub use subscription::Subscription;
pub(crate) use types::tool_payload;
pub use types::{ExecutionEvent, FeedItem};

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
