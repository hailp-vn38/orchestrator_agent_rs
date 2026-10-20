//! `TaskManager` (docs/06): ủy quyền foreground Root → Child qua tool `subagent`.
//!
//! Admission (verify ownership + append child input + claim child run + link vào parent
//! invocation) là **một** transaction của [`SessionStore::admit_child_turn`], idempotent theo
//! `tool_invocations.id`. Child không chạy trong module này: run của child do supervisor spawn
//! như mọi claim `running` khác (không untracked task, không giữ `Arc<AgentRuntime>` nên không
//! có vòng tham chiếu). Parent chỉ **đợi** `child_turn_id` đã persist cho tới khi attempt mới
//! nhất terminal; không có deadline tổng cho child.
//!
//! `TaskManager` không ghi transcript: kết quả trả về dưới dạng outcome để `AgentRuntime` ghi
//! tool-result của parent. Text của child là untrusted context; runtime không suy luận gì từ nó
//! (Root tự đánh giá), `completed` chỉ là execution status.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::agent::{AgentDefinition, AgentMode};
use crate::feed::Emitter;
use crate::gateway::ModelTool;
use crate::store::{ChildAdmission, ChildAdmitError, ChildTarget, RunRoute, SessionStore};
use crate::supervisor::AdmissionSignal;
use crate::tool::{ToolError, ToolOutcome, ToolScope};
use crate::types::{AgentResult, RunStatus};

/// Tên built-in tool ủy quyền cho subagent.
pub(crate) const SUBAGENT_TOOL: &str = "subagent";

/// Thay cho text khi child hoàn thành mà không có final text (docs/06 §6.5).
const EMPTY_TEXT_PLACEHOLDER: &str = "Subagent completed without a text response.";

/// Poll `child_turn_id` đã persist: bắt đầu nhanh rồi giãn tới trần (đọc SQLite rẻ, ngắn).
const POLL_START: Duration = Duration::from_millis(10);
const POLL_MAX: Duration = Duration::from_millis(250);
/// Số lần đọc store lỗi liên tiếp trước khi bỏ cuộc đợi (child vẫn chạy và vẫn đọc được sau).
const MAX_POLL_ERRORS: u32 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubagentInput {
    pub agent: String,
    /// Nhãn ngắn cho UI/log; không đưa vào child transcript.
    pub description: String,
    pub prompt: String,
    /// `None` => child mới; `Some` => tiếp tục child cũ của chính parent.
    pub session_id: Option<String>,
}

impl SubagentInput {
    /// Gọi sau khi `args` đã qua [`input_schema`] (envelope kỹ thuật).
    pub(crate) fn from_args(args: &Value) -> Self {
        let text = |k: &str| args[k].as_str().unwrap_or_default().to_owned();
        Self {
            agent: text("agent"),
            description: text("description"),
            prompt: text("prompt"),
            session_id: args["session_id"]
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned),
        }
    }
}

/// Kết quả của một lần ủy quyền.
pub(crate) enum Delegation {
    /// Outcome để runtime ghi thành tool-result (thành công hoặc ToolError kỹ thuật).
    Done(Result<ToolOutcome, ToolError>),
    /// Parent bị hủy hợp tác (shutdown) khi đang admit/đợi: không có result để ghi.
    Cancelled,
}

pub(crate) struct TaskManager {
    store: SessionStore,
    /// Chỉ agent có thể làm child (mode `Subagent|All`), theo id.
    delegable: BTreeMap<String, AgentDefinition>,
    modes: HashMap<String, AgentMode>,
    routes: HashMap<String, RunRoute>,
    signal: AdmissionSignal,
    emitter: Emitter,
}

impl TaskManager {
    pub(crate) fn new(
        store: SessionStore,
        agents: &HashMap<String, AgentDefinition>,
        routes: HashMap<String, RunRoute>,
        signal: AdmissionSignal,
        emitter: Emitter,
    ) -> Self {
        let delegable = agents
            .values()
            .filter(|a| a.mode != AgentMode::Primary)
            .map(|a| (a.id.clone(), a.clone()))
            .collect();
        let modes = agents.values().map(|a| (a.id.clone(), a.mode)).collect();
        Self {
            store,
            delegable,
            modes,
            routes,
            signal,
            emitter,
        }
    }

    /// Không có agent nào làm child được thì không quảng bá và không chạy `subagent`.
    pub(crate) fn has_candidates(&self) -> bool {
        !self.delegable.is_empty()
    }

    /// Định nghĩa tool gửi cho model (chỉ gọi khi [`Self::has_candidates`]).
    pub(crate) fn tool_definition(&self) -> ModelTool {
        let mut description = String::from(
            "Delegate a self-contained task to a specialist subagent and wait for its final \
             text. The reply carries a session_id; pass it back as session_id with the same \
             agent to continue that subagent's conversation. Evaluate the reply yourself: the \
             subagent being completed does not mean the task is done correctly. Available \
             agents:",
        );
        for a in self.delegable.values() {
            description.push_str(&format!("\n- {}: {}", a.id, a.description.trim()));
        }
        ModelTool {
            name: SUBAGENT_TOOL.to_owned(),
            description,
            input_schema: input_schema(),
        }
    }

    /// Admit child turn rồi đợi final của đúng turn đó. Mọi lỗi policy trả ToolError, không
    /// tạo child (docs/06 §6.6). Gọi lại với cùng `scope.invocation_id` không submit trùng.
    pub(crate) async fn delegate(
        &self,
        scope: &ToolScope,
        input: SubagentInput,
        cancel: &CancellationToken,
    ) -> Delegation {
        if cancel.is_cancelled() {
            return Delegation::Cancelled;
        }
        let fail = |code: &str, message: String| {
            Delegation::Done(Err(ToolError {
                code: code.to_owned(),
                message,
            }))
        };
        let Some(mode) = self.modes.get(&input.agent) else {
            return fail(
                "AGENT_NOT_FOUND",
                "the requested agent does not exist".into(),
            );
        };
        if *mode == AgentMode::Primary {
            return fail(
                "MODE_DENIED",
                "the requested agent cannot be used as a subagent".into(),
            );
        }
        if input.prompt.trim().is_empty() {
            return fail("INVALID_ARGUMENTS", "prompt must not be empty".into());
        }

        let target = match &input.session_id {
            None => ChildTarget::New {
                agent_id: &input.agent,
            },
            Some(session_id) => ChildTarget::Resume {
                session_id,
                agent_id: &input.agent,
            },
        };
        let admitted = self
            .store
            .admit_child_turn(&scope.invocation_id, target, &input.prompt, |agent_id| {
                self.routes.get(agent_id).cloned()
            })
            .await;
        let admission = match admitted {
            Ok(a) => a,
            Err(e) => return admit_failure(e),
        };
        tracing::debug!(
            description = %input.description,
            child_session_id = %admission.session_id,
            child_turn_id = %admission.turn_id,
            created = admission.created,
            "child turn admitted"
        );
        if admission.created {
            // Child run đã commit; supervisor (trigger hoặc sweep) sẽ spawn đúng một worker.
            self.signal.admitted();
            // Link child đã commit (revision parent tăng): UI refresh family để thấy child mới.
            self.emitter
                .committed_session(
                    &scope.session_id,
                    Some(&scope.run_id),
                    "session.changed",
                    serde_json::json!({
                        "reason": "child_created",
                        "child_session_id": admission.session_id,
                        "tool_call_id": scope.tool_call_id,
                    }),
                )
                .await;
        }
        self.await_child(scope, &admission, cancel).await
    }

    async fn await_child(
        &self,
        scope: &ToolScope,
        admission: &ChildAdmission,
        cancel: &CancellationToken,
    ) -> Delegation {
        let mut delay = POLL_START;
        let mut errors = 0;
        let result = loop {
            if cancel.is_cancelled() {
                return Delegation::Cancelled;
            }
            match self
                .store
                .load_result(&admission.session_id, &admission.turn_id)
                .await
            {
                Ok(r) if r.status.is_terminal() => break r,
                Ok(_) => errors = 0,
                Err(_) => {
                    errors += 1;
                    if errors >= MAX_POLL_ERRORS {
                        return Delegation::Done(Err(ToolError {
                            code: "CHILD_FAILED".into(),
                            message: format!(
                                "could not read the state of subagent session {}; it may still \
                                 be running and can be resumed with session_id",
                                admission.session_id
                            ),
                        }));
                    }
                }
            }
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Delegation::Cancelled,
                () = tokio::time::sleep(delay) => {}
            }
            delay = (delay * 2).min(POLL_MAX);
        };
        if cancel.is_cancelled() && result.status != RunStatus::Completed {
            // Child dừng vì shutdown chung: parent cũng bị interrupted, không ghi result giả.
            return Delegation::Cancelled;
        }
        Delegation::Done(outcome_of(scope, result))
    }
}

fn admit_failure(error: ChildAdmitError) -> Delegation {
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
fn outcome_of(scope: &ToolScope, result: AgentResult) -> Result<ToolOutcome, ToolError> {
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

/// Envelope kỹ thuật của `subagent`: không có `background`, không business JSON.
pub(crate) fn input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "agent": { "type": "string" },
            "description": { "type": "string" },
            "prompt": { "type": "string" },
            "session_id": { "type": ["string", "null"] }
        },
        "required": ["agent", "description", "prompt"],
        "additionalProperties": false
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_parses_optional_session_id() {
        let a = SubagentInput::from_args(&json!({
            "agent": "reviewer", "description": "d", "prompt": "p"
        }));
        assert_eq!(a.session_id, None);
        let b = SubagentInput::from_args(&json!({
            "agent": "reviewer", "description": "d", "prompt": "p", "session_id": " s1 "
        }));
        assert_eq!(b.session_id.as_deref(), Some("s1"));
        for blank in [json!(null), json!(""), json!("  ")] {
            let c = SubagentInput::from_args(&json!({
                "agent": "r", "description": "d", "prompt": "p", "session_id": blank
            }));
            assert_eq!(c.session_id, None);
        }
    }

    fn scope() -> ToolScope {
        ToolScope {
            owner_id: "o".into(),
            workspace_id: "w".into(),
            session_id: "parent".into(),
            root_session_id: "parent".into(),
            run_id: "run".into(),
            agent_id: "root".into(),
            invocation_id: "inv".into(),
            tool_call_id: "call".into(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
        }
    }

    fn result(status: RunStatus, output: Option<&str>, code: Option<&str>) -> AgentResult {
        AgentResult {
            session_id: "child".into(),
            turn_id: "turn".into(),
            run_id: "r".into(),
            attempt: 0,
            status,
            output: output.map(str::to_owned),
            final_message_id: output.map(|_| "m".into()),
            error: code.map(|c| crate::types::ResultError {
                code: c.into(),
                message: "m".into(),
            }),
        }
    }

    #[test]
    fn missing_or_blank_final_text_becomes_a_placeholder_not_a_panic() {
        for output in [None, Some(""), Some("  \n ")] {
            let out = outcome_of(&scope(), result(RunStatus::Completed, output, None)).unwrap();
            assert!(
                out.content.contains(EMPTY_TEXT_PLACEHOLDER),
                "{}",
                out.content
            );
            assert_eq!(out.metadata["empty_text"], json!(true));
        }
        let out = outcome_of(&scope(), result(RunStatus::Completed, Some("real"), None)).unwrap();
        assert_eq!(
            out.content,
            "<subagent sessionID=\"child\" state=\"completed\">\nreal\n</subagent>"
        );
        assert_eq!(out.metadata["empty_text"], json!(false));
    }

    #[test]
    fn non_completed_children_map_to_stable_tool_error_codes() {
        let code = |status, child_code| {
            outcome_of(&scope(), result(status, None, child_code))
                .unwrap_err()
                .code
        };
        assert_eq!(
            code(RunStatus::TimedOut, Some("PROVIDER_TIMEOUT")),
            "PROVIDER_TIMEOUT"
        );
        assert_eq!(code(RunStatus::Cancelled, None), "CHILD_CANCELLED");
        assert_eq!(
            code(RunStatus::Failed, Some("PROVIDER_UNAVAILABLE")),
            "CHILD_FAILED"
        );
        assert_eq!(
            code(RunStatus::Interrupted, Some("TOOL_OUTCOME_UNKNOWN")),
            "CHILD_FAILED"
        );
        assert_eq!(
            code(RunStatus::Failed, Some("PROVIDER_AUTH_FAILED")),
            "MODEL_UNAVAILABLE"
        );
        assert_eq!(
            code(RunStatus::Failed, Some("MODEL_NOT_CONFIGURED")),
            "MODEL_UNAVAILABLE"
        );
        let e = outcome_of(&scope(), result(RunStatus::Failed, None, Some("X"))).unwrap_err();
        assert!(
            e.message.contains("child") && e.message.contains("session_id"),
            "{}",
            e.message
        );
    }
}
