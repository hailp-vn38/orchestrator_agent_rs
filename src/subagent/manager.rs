//! Admit và đợi child turn, không ghi transcript.
use super::outcome::{admit_failure, outcome_of};
use super::{input_schema, Delegation, SubagentInput, SUBAGENT_TOOL};
use crate::agent::{AgentDefinition, AgentMode};
use crate::feed::{event, Emitter};
use crate::gateway::ModelTool;
use crate::store::{ChildAdmission, ChildTarget, RunRoute, SessionStore};
use crate::supervisor::AdmissionSignal;
use crate::tool::{ToolError, ToolScope};
use crate::types::RunStatus;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Poll `child_turn_id` đã persist: bắt đầu nhanh rồi giãn tới trần (đọc SQLite rẻ, ngắn).
const POLL_START: Duration = Duration::from_millis(10);
const POLL_MAX: Duration = Duration::from_millis(250);
/// Số lần đọc store lỗi liên tiếp trước khi bỏ cuộc đợi (child vẫn chạy và vẫn đọc được sau).
const MAX_POLL_ERRORS: u32 = 5;

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
                    event::SESSION_CHANGED,
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
