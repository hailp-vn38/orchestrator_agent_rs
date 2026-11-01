//! Chuyển lỗi admission và kết quả terminal của child thành tool outcome.
use super::{ChildFailure, Delegation};
use crate::store::ChildAdmitError;
use crate::tool::{ToolError, ToolOutcome, ToolScope};
use crate::types::{AgentResult, RunStatus};
use serde_json::json;

/// Thay cho text khi child hoàn thành mà không có final text (docs/06 §6.5).
pub(super) const EMPTY_TEXT_PLACEHOLDER: &str = "Subagent completed without a text response.";

pub(super) fn admit_failure(error: ChildAdmitError) -> Delegation {
    let (code, message) = match error {
        ChildAdmitError::NotChild => (
            "SESSION_NOT_CHILD",
            "session_id is not a subagent session of this conversation",
        ),
        ChildAdmitError::Busy => (
            "SESSION_BUSY",
            "that subagent session already has an active run; try again later",
        ),
        ChildAdmitError::Archived => ("SESSION_ARCHIVED", "that subagent session is archived"),
        ChildAdmitError::Blocked => (
            "RECOVERY_BLOCKED",
            "that subagent session has an interrupted tool call that an operator must resolve first",
        ),
        ChildAdmitError::AgentMismatch => (
            "AGENT_MISMATCH",
            "session_id belongs to a different agent; use the agent that created it",
        ),
        ChildAdmitError::DepthLimit => ("DEPTH_LIMIT", "subagents cannot delegate further"),
        ChildAdmitError::RouteUnavailable => (
            "MODEL_UNAVAILABLE",
            "no provider/model is available for the requested agent",
        ),
        ChildAdmitError::ParentUnavailable => {
            return Delegation::Done(Err(ToolError {
                code: "CHILD_FAILED".into(),
                message: "the delegating run is no longer active".into(),
            }))
        }
        ChildAdmitError::Store => ("CHILD_FAILED", "could not start the subagent"),
    };
    Delegation::Done(Err(ToolError {
        code: code.into(),
        message: message.into(),
    }))
}

fn child_failure_is_retryable(status: RunStatus, stop_reason: &str) -> bool {
    status != RunStatus::Cancelled
        && !matches!(
            stop_reason,
            "TOOL_OUTCOME_UNKNOWN"
                | "RECOVERY_BLOCKED"
                | "BUDGET_EXHAUSTED"
                | "CONTEXT_LIMIT"
                | "MODEL_NOT_CONFIGURED"
                | "PROVIDER_INVALID_REQUEST"
                | "PROVIDER_AUTH_FAILED"
                | "PROVIDER_CONFIG_CHANGED"
                | "CANCELLED"
                | "CHILD_CANCELLED"
        )
}

/// Chuyển attempt terminal của child thành outcome của parent invocation.
pub(super) fn outcome_of(scope: &ToolScope, result: AgentResult) -> Delegation {
    let sid = &result.session_id;
    match result.status {
        RunStatus::Completed => {
            // Thiếu/rỗng final text (kể cả final pointer không đọc được) => placeholder.
            let text = result.output.as_deref().filter(|t| !t.trim().is_empty());
            let empty_text = text.is_none();
            let output = text.unwrap_or(EMPTY_TEXT_PLACEHOLDER);
            // Completed kèm mã nghĩa là con dừng có kiểm soát (step cap, ngân sách): phần việc còn
            // lại phải hiện rõ cho cha thay vì trông như đã xong.
            let stop_reason = result.error.as_ref().map(|e| e.code.as_str());
            let stop_attrs = stop_reason
                .map(|code| format!(" incomplete=\"true\" stop_reason=\"{code}\""))
                .unwrap_or_default();
            Delegation::Done(Ok(ToolOutcome {
                call_id: scope.tool_call_id.clone(),
                content: format!(
                    "<subagent sessionID=\"{sid}\" state=\"completed\"{stop_attrs}>\n{output}\n</subagent>"
                ),
                metadata: json!({
                    "session_id": sid,
                    "child_turn_id": result.turn_id,
                    "status": "completed",
                    "empty_text": empty_text,
                    "stop_reason": stop_reason,
                }),
            }))
        }
        RunStatus::TimedOut
        | RunStatus::Cancelled
        | RunStatus::Failed
        | RunStatus::Interrupted
        | RunStatus::Running
        | RunStatus::WaitingRetry
        | RunStatus::WaitingUser => {
            let stop_reason = result
                .error
                .as_ref()
                .map(|e| e.code.trim())
                .filter(|c| !c.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| match result.status {
                    RunStatus::Cancelled => "CANCELLED".into(),
                    RunStatus::TimedOut => "PROVIDER_TIMEOUT".into(),
                    _ => "CHILD_FAILED".into(),
                });
            let retryable = child_failure_is_retryable(result.status, &stop_reason);
            Delegation::Failed(ChildFailure {
                session_id: result.session_id,
                status: result.status,
                stop_reason,
                retryable,
                partial_result: result.partial_output,
            })
        }
    }
}

#[cfg(test)]
mod retryable_tests {
    use super::*;

    #[test]
    fn permanent_failures_are_not_retryable() {
        for code in [
            "CONTEXT_LIMIT",
            "PROVIDER_INVALID_REQUEST",
            "TOOL_OUTCOME_UNKNOWN",
        ] {
            assert!(
                !child_failure_is_retryable(RunStatus::Failed, code),
                "{code}"
            );
        }
    }

    #[test]
    fn exhausted_retries_stay_retryable() {
        assert!(child_failure_is_retryable(
            RunStatus::Failed,
            "RECOVERY_ATTEMPTS_EXHAUSTED"
        ));
    }
}
