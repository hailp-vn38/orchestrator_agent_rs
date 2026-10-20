//! `SessionStore`: sở hữu toàn bộ query và transaction invariants của execution store
//! (docs/09). Không await LLM/tool trong transaction.
//!
//! Mọi write-transaction bắt đầu bằng một câu lệnh ghi (không đọc trước) để lấy write lock
//! ngay và tránh `SQLITE_BUSY_SNAPSHOT` khi nâng cấp từ read snapshot trong WAL.

use sqlx::SqlitePool;

mod admission;
mod codec;
mod invocation;
mod observe;
mod recovery;
mod run;
mod session;
mod tool_checkpoint;
mod transcript;
mod types;

#[cfg(test)]
mod tests;

pub(crate) use codec::new_id;
pub(crate) use observe::EventInfo;
pub(crate) use recovery::{RecoveryPolicy, Resolution, CODE_NO_EFFECT, CODE_SHUTTING_DOWN};
pub(crate) use types::{
    Checkpoint, ChildAdmission, ChildAdmitError, ChildTarget, Context, NewCheckpoint,
    PendingInvocation, RunRecord, RunRoute, StoredToolCall, TranscriptEntry,
};

/// Tên file DB trong `store_dir`.
pub(crate) const DB_FILE: &str = "orchestrator.db";

#[derive(Clone)]
pub(crate) struct SessionStore {
    pool: SqlitePool,
}
