//! `TaskManager` (docs/06): ủy quyền foreground Root → Child qua tool `subagent`.
//!
//! Admission (verify ownership + append child input + claim child run + link vào parent
//! invocation) là **một** transaction của [`crate::store::SessionStore::admit_child_turn`], idempotent theo
//! `tool_invocations.id`. Child không chạy trong module này: run của child do supervisor spawn
//! như mọi claim `running` khác (không untracked task, không giữ `Arc<AgentRuntime>` nên không
//! có vòng tham chiếu). Parent chỉ **đợi** `child_turn_id` đã persist cho tới khi attempt mới
//! nhất terminal; không có deadline tổng cho child.
//!
//! `TaskManager` không ghi transcript: kết quả trả về dưới dạng outcome để `AgentRuntime` ghi
//! tool-result của parent. Text của child là untrusted context; runtime không suy luận gì từ nó
//! (Root tự đánh giá), `completed` chỉ là execution status.

use crate::tool::{ToolError, ToolOutcome};
use crate::types::RunStatus;

mod input;
mod manager;
mod outcome;
#[cfg(test)]
mod tests;

pub(crate) use input::{input_schema, SubagentInput};
pub(crate) use manager::TaskManager;

/// Tên built-in tool ủy quyền cho subagent.
pub(crate) const SUBAGENT_TOOL: &str = "subagent";

/// Kết quả lỗi có cấu trúc khi child turn kết thúc ở trạng thái không-completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChildFailure {
    pub session_id: String,
    pub status: RunStatus,
    pub stop_reason: String,
    pub retryable: bool,
    pub partial_result: Option<String>,
}

/// Kết quả của một lần ủy quyền.
pub(crate) enum Delegation {
    /// Outcome để runtime ghi thành tool-result (thành công hoặc ToolError kỹ thuật trước khi chạy).
    Done(Result<ToolOutcome, ToolError>),
    /// Child turn kết thúc ở trạng thái không-completed: trả kết quả có cấu trúc.
    Failed(ChildFailure),
    /// Parent bị hủy hợp tác (shutdown) khi đang admit/đợi: không có result để ghi.
    Cancelled,
}
