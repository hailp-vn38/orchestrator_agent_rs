//! `SessionStore`: sở hữu toàn bộ query và transaction invariants của execution store
//! (docs/09). Không await LLM/tool trong transaction.
//!
//! Mọi write-transaction bắt đầu bằng một câu lệnh ghi (không đọc trước) để lấy write lock
//! ngay và tránh `SQLITE_BUSY_SNAPSHOT` khi nâng cấp từ read snapshot trong WAL.

use sqlx::SqlitePool;
use std::future::Future as _;

mod admission;
mod artifact;
mod codec;
mod delete;
mod invocation;
mod observe;
mod query;
mod recovery;
mod run;
mod session;
mod tool_checkpoint;
mod tool_operation;
mod transcript;
mod types;

#[cfg(test)]
mod tests;

pub(crate) use artifact::{PublishError, PublishScope, StoredArtifact};
pub(crate) use codec::new_id;
pub(crate) use observe::EventInfo;
pub(crate) use recovery::{
    RecoveryPolicy, Resolution, CODE_EXHAUSTED, CODE_NO_EFFECT, CODE_SHUTTING_DOWN,
    CODE_STORE_UNAVAILABLE,
};
pub(crate) use types::{
    Checkpoint, ChildAdmission, ChildAdmitError, ChildTarget, Context, NewCheckpoint,
    PendingInvocation, RunRecord, RunRoute, StoredToolCall, TranscriptEntry,
};

/// Tên file DB trong `store_dir`.
pub(crate) const DB_FILE: &str = "orchestrator.db";

#[derive(Clone)]
pub(crate) struct SessionStore {
    pool: SqlitePool,
    sys_base_secs: i64,
    tok_base: tokio::time::Instant,
}

/// Giữ một timer ngắn (1ms) trên Tokio timer wheel trong lúc chờ worker thread của `sqlx`.
/// Khi chạy giờ thật, SQLite xong trong ~50µs trước khi timer kịp nổ; khi test bật
/// `tokio::time::pause()`, timer 1ms ngăn time driver nhảy vọt tới các timer dài
/// (`POOL_ACQUIRE_TIMEOUT`, `tool_timeout`) trong lúc chờ I/O thread.
pub(crate) async fn settle_sql<F: std::future::Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let mut tick = std::pin::pin!(tokio::time::sleep(std::time::Duration::from_millis(1)));
    let mut tick_armed = false;
    std::future::poll_fn(|cx| {
        if tick_armed && tick.as_mut().poll(cx).is_ready() {
            std::thread::sleep(std::time::Duration::from_micros(200));
            tick_armed = false;
        }
        if let std::task::Poll::Ready(out) = fut.as_mut().poll(cx) {
            return std::task::Poll::Ready(out);
        }
        if !tick_armed {
            tick.as_mut()
                .reset(tokio::time::Instant::now() + std::time::Duration::from_millis(1));
            let _ = tick.as_mut().poll(cx);
            tick_armed = true;
        }
        std::task::Poll::Pending
    })
    .await
}
