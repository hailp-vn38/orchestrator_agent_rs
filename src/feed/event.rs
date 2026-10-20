//! Tên event dùng chung cho publisher, fault hook và test.
pub(crate) const RUN_STARTED: &str = "run.started";
pub(crate) const RUN_COMPLETED: &str = "run.completed";
pub(crate) const RUN_FAILED: &str = "run.failed";
pub(crate) const RUN_CANCELLED: &str = "run.cancelled";
pub(crate) const RUN_INTERRUPTED: &str = "run.interrupted";
pub(crate) const STEP_STARTED: &str = "step.started";
pub(crate) const STEP_ENDED: &str = "step.ended";
pub(crate) const TEXT_DELTA: &str = "text.delta";
pub(crate) const MESSAGE_COMMITTED: &str = "message.committed";
pub(crate) const TOOL_STARTED: &str = "tool.started";
pub(crate) const TOOL_PROGRESS: &str = "tool.progress";
pub(crate) const TOOL_COMPLETED: &str = "tool.completed";
pub(crate) const TOOL_FAILED: &str = "tool.failed";
pub(crate) const TOOL_INTERRUPTED: &str = "tool.interrupted";
pub(crate) const SESSION_CHANGED: &str = "session.changed";
pub(crate) const COMPACTION_STARTED: &str = "compaction.started";
pub(crate) const COMPACTION_COMPLETED: &str = "compaction.completed";

/// Event kết thúc một run/tool: publication bị mất thì chỉ snapshot mới bù được.
#[cfg(feature = "test-support")]
pub(crate) const TERMINAL: &[&str] = &[
    RUN_COMPLETED,
    RUN_FAILED,
    RUN_CANCELLED,
    RUN_INTERRUPTED,
    TOOL_COMPLETED,
    TOOL_FAILED,
    TOOL_INTERRUPTED,
];

/// Event terminal của run theo trạng thái (timed_out gộp vào failed).
pub(crate) fn run_terminal(status: crate::types::RunStatus) -> &'static str {
    use crate::types::RunStatus;
    match status {
        RunStatus::Completed => RUN_COMPLETED,
        RunStatus::Cancelled => RUN_CANCELLED,
        RunStatus::Interrupted => RUN_INTERRUPTED,
        _ => RUN_FAILED,
    }
}
