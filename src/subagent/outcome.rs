//! Chuyển lỗi admission và kết quả terminal của child thành tool outcome.
use super::Delegation;
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

/// Chuyển attempt terminal của child thành outcome của parent invocation.
pub(super) fn outcome_of(scope: &ToolScope, result: AgentResult) -> Result<ToolOutcome, ToolError> {
    let sid = &result.session_id;
    let error = |code: &str, why: &str| {
        let child_code = result.error.as_ref().map_or("", |e| e.code.as_str());
        Err(ToolError {
            code: code.into(),
            message: format!(
                "subagent session {sid} {why}{}; its history is kept and it can be resumed \
                 with session_id",
                if child_code.is_empty() {
                    String::new()
                } else {
                    format!(" ({child_code})")
                }
            ),
        })
    };
    match result.status {
        RunStatus::Completed => {
            // Thiếu/rỗng final text (kể cả final pointer không đọc được) => placeholder.
            let text = result.output.as_deref().filter(|t| !t.trim().is_empty());
            let empty_text = text.is_none();
            let output = text.unwrap_or(EMPTY_TEXT_PLACEHOLDER);
            Ok(ToolOutcome {
                call_id: scope.tool_call_id.clone(),
                content: format!(
                    "<subagent sessionID=\"{sid}\" state=\"completed\">\n{output}\n</subagent>"
                ),
                metadata: json!({
                    "session_id": sid,
                    "child_turn_id": result.turn_id,
                    "status": "completed",
                    "empty_text": empty_text,
                }),
            })
        }
        RunStatus::TimedOut => error("PROVIDER_TIMEOUT", "timed out waiting for the provider"),
        RunStatus::Cancelled => error("CHILD_CANCELLED", "was cancelled"),
        RunStatus::Failed | RunStatus::Interrupted | RunStatus::Running => {
            let model_problem = matches!(
                result.error.as_ref().map(|e| e.code.as_str()),
                Some("MODEL_NOT_CONFIGURED" | "PROVIDER_AUTH_FAILED")
            );
            if model_problem {
                error("MODEL_UNAVAILABLE", "could not use its model")
            } else {
                error("CHILD_FAILED", "did not complete")
            }
        }
    }
}
