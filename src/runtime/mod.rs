//! `AgentRuntime`: runner duy nhất của một run (EMB-10) và là **transcript writer duy nhất**.
//!
//! M2: vòng lặp model ⇄ tool. Thứ tự bắt buộc (docs/08 §8.1, docs/18 §18.6.1):
//! 1. gateway trả toàn response `Done` (không có call nào từ response lỗi/cụt);
//! 2. commit assistant message + **mọi** invocation `requested` trong một transaction;
//! 3. dispatch tuần tự theo thứ tự provider; mỗi invocation `running` được commit trước side
//!    effect và tool-result + terminal state commit chung một transaction;
//! 4. chỉ khi mọi result đã settle mới mở LLM step kế tiếp.
//!
//! M4: `steps` cap tùy chọn (tới cap: request text-only, không tạo run mới), auto compaction
//! trên settled exchange prefix (tối đa một lần compact/retry khi provider báo overflow).
//!
//! Executor chỉ trả outcome; không module nào khác ghi transcript. Outcome không xác định
//! (process chết giữa chừng, commit result lỗi, timeout/panic của tool Mutating) **không bao
//! giờ** bị replay: run chuyển `interrupted`; phục hồi/reconcile nằm ở `store::recovery`
//! (M5) và chỉ tạo attempt mới khi an toàn.
//!
//! M5: hủy hợp tác có hai nguồn — shutdown (`interrupted`/`SHUTTING_DOWN`, phục hồi được) và
//! `cancel` của host (`cancelled`/`CANCELLED`, terminal).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde_json::json;

use tokio_util::sync::CancellationToken;
use tracing::{info, Instrument};

use crate::agent::AgentDefinition;
use crate::executor::{Effective, ToolExecutor, CODE_OUTCOME_UNKNOWN};
use crate::feed::{event, tool_payload, Emitter};
use crate::gateway::{GatewayError, LlmGateway};
use crate::prompt::PromptBuilder;
use crate::store::{SessionStore, StoredToolCall};
use crate::types::RunStatus;

mod compact;
mod model;
mod settle;
mod terminal;

/// Hướng dẫn gửi cho model khi chạm `steps` cap (không persist; đây là execution stop,
/// không phải bằng chứng hoàn tất nghiệp vụ).
const STEP_LIMIT_GUIDANCE: &str = "The step limit for this task has been reached. Tools are \
no longer available. Reply with text only: summarize the work completed so far and list the \
remaining work, without calling any tool.";
/// Text lưu khi model chạm cap mà không trả text nào.
const STEP_LIMIT_FALLBACK: &str = "The step limit was reached before the task was finished; \
no progress summary was produced.";

/// Chính sách context (ước lượng) lấy từ `RuntimeConfig`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ContextPolicy {
    pub limit_tokens: usize,
    pub threshold_percent: u8,
    pub keep_recent: usize,
}

impl ContextPolicy {
    /// Ngưỡng compaction; `agent_limit` (nếu agent khai báo) thay cho limit toàn cục.
    fn threshold(&self, agent_limit: Option<usize>) -> usize {
        agent_limit
            .unwrap_or(self.limit_tokens)
            .saturating_mul(usize::from(self.threshold_percent))
            / 100
    }
}

pub(crate) struct AgentRuntime {
    store: SessionStore,
    gateway: Arc<dyn LlmGateway>,
    prompt: PromptBuilder,
    executor: ToolExecutor,
    /// Snapshot definition lúc build (không hot reload).
    agents: HashMap<String, AgentDefinition>,
    context: ContextPolicy,
    /// Token shutdown của engine: phân biệt hủy do shutdown với `cancel` của host.
    shutdown: CancellationToken,
    /// Publish observation event **sau** commit (docs/16); không bao giờ chặn run.
    emitter: Emitter,
}

/// Kết quả của một lần `execute`: supervisor dựa vào đây để quyết định thử lại.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum Settlement {
    /// Run đã ở trạng thái terminal bền vững (hoặc vốn đã terminal ở nơi khác).
    Terminal,
    /// Không commit được trạng thái kế tiếp; run vẫn `running` và cần thử lại.
    Unsettled,
}

/// Kết luận của một bước điều phối: tiếp tục vòng lặp hay kết thúc `execute`.
enum Flow {
    Continue,
    /// Giá trị trả về của `execute` (xem doc của nó).
    Stop(Settlement),
}

impl AgentRuntime {
    // Composition root duy nhất: các collaborator được nối một lần lúc build.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: SessionStore,
        gateway: Arc<dyn LlmGateway>,
        prompt: PromptBuilder,
        executor: ToolExecutor,
        agents: HashMap<String, AgentDefinition>,
        context: ContextPolicy,
        shutdown: CancellationToken,
        emitter: Emitter,
    ) -> Self {
        Self {
            store,
            gateway,
            prompt,
            executor,
            agents,
            context,
            shutdown,
            emitter,
        }
    }

    /// Chạy một run đã được claim. [`Settlement::Terminal`] khi run đã ở trạng thái terminal
    /// bền vững (hoặc vốn đã terminal); [`Settlement::Unsettled`] khi không commit được
    /// (supervisor thử lại có backoff).
    pub(crate) async fn execute(&self, run_id: &str, cancel: &CancellationToken) -> Settlement {
        let span = tracing::info_span!(
            "agent_run",
            run_id = %run_id,
            session_id = tracing::field::Empty
        );
        self.execute_inner(run_id, cancel).instrument(span).await
    }

    async fn execute_inner(&self, run_id: &str, cancel: &CancellationToken) -> Settlement {
        let run = match self.store.load_run(run_id).await {
            Ok(Some(r)) if r.status == RunStatus::Running => r,
            Ok(_) => return Settlement::Terminal,
            Err(_) => return Settlement::Unsettled,
        };
        tracing::Span::current().record("session_id", tracing::field::display(&run.session_id));
        self.emitter
            .committed_run(
                run_id,
                event::RUN_STARTED,
                json!({ "agent_id": run.agent_id }),
            )
            .await;
        let Some(agent) = self.agents.get(&run.agent_id) else {
            return self.fail(run_id, GatewayError::NotConfigured).await;
        };
        let eff =
            Effective::resolve(agent, &run.policy).for_session(run.parent_session_id.is_some());

        // Run được sweep lại sau khi worker trước kết thúc mà chưa terminal. Invocation đã
        // `running` mà chưa có result nghĩa là outcome không xác định → chặn, không replay.
        let mut pending = match self.store.pending_invocations(run_id).await {
            Ok(p) => p,
            Err(_) => return Settlement::Unsettled,
        };
        if pending.iter().any(|p| p.started) {
            return self
                .finish(
                    run_id,
                    RunStatus::Interrupted,
                    CODE_OUTCOME_UNKNOWN,
                    "a tool call started but its outcome was not recorded; it was not replayed",
                )
                .await;
        }

        loop {
            if let Flow::Stop(done) = self
                .settle_pending(run_id, &run, &eff, pending, cancel)
                .await
            {
                return done;
            }
            if cancel.is_cancelled() {
                return self.fail(run_id, GatewayError::Cancelled).await;
            }

            // Tới cap: request text-only, response là kết quả của run (không tạo run mới).
            let capped = match (agent.steps, self.store.run_steps(run_id).await) {
                (Some(limit), Ok(done)) => done >= limit,
                (_, Err(_)) => return Settlement::Unsettled,
                _ => false,
            };
            let (response, step_id) = match self.model_step(&run, &eff, capped, cancel).await {
                Ok(r) => r,
                Err(done) => return done,
            };

            if capped || response.tool_calls.is_empty() {
                // Tool call (nếu có) bị bỏ khi đã hết steps: chưa persist, không execute.
                let text = if capped && response.text.trim().is_empty() {
                    STEP_LIMIT_FALLBACK
                } else {
                    response.text.as_str()
                };
                return match self.store.complete_run(run_id, text).await {
                    Ok(message_id) => {
                        info!(capped, "run completed");
                        if let Some(message_id) = message_id {
                            self.emitter
                                .committed_run(
                                    run_id,
                                    event::MESSAGE_COMMITTED,
                                    json!({
                                        "message_id": message_id,
                                        "role": "assistant",
                                        "step_id": step_id,
                                        "final": true,
                                    }),
                                )
                                .await;
                            self.emitter
                                .run_terminal(run_id, RunStatus::Completed, None)
                                .await;
                        }
                        Settlement::Terminal
                    }
                    Err(_) => {
                        // Không để terminal dở dang: thử ghi failed; nếu cũng lỗi thì sweep lại.
                        self.finish(
                            run_id,
                            RunStatus::Failed,
                            "INTERNAL",
                            "the final message could not be committed",
                        )
                        .await
                    }
                };
            }

            // Provider call ID chỉ cần duy nhất trong một response; trùng/rỗng nghĩa là không
            // ghép được result một cách đáng tin → từ chối cả response, chưa persist gì.
            let mut seen = HashSet::new();
            if response
                .tool_calls
                .iter()
                .any(|c| c.id.trim().is_empty() || !seen.insert(c.id.as_str()))
            {
                return self.fail(run_id, GatewayError::Protocol).await;
            }
            let calls: Vec<StoredToolCall> = response
                .tool_calls
                .iter()
                .map(|c| StoredToolCall {
                    id: c.id.clone(),
                    name: c.name.clone(),
                    arguments: c.arguments.clone(),
                })
                .collect();
            pending = match self
                .store
                .commit_tool_calls(run_id, &response.text, &calls)
                .await
            {
                Ok(Some(p)) => p,
                Ok(None) => return Settlement::Terminal, // run đã terminal ở nơi khác
                // Chưa có invocation nào → thử lại (có backoff) sẽ gọi lại LLM, an toàn.
                Err(_) => return Settlement::Unsettled,
            };
            self.emitter
                .committed_run(
                    run_id,
                    event::MESSAGE_COMMITTED,
                    json!({
                        "role": "assistant",
                        "step_id": step_id,
                        "final": false,
                        "tool_call_count": pending.len(),
                    }),
                )
                .await;
            for inv in &pending {
                self.emitter
                    .committed_run(
                        run_id,
                        event::TOOL_STARTED,
                        tool_payload(&inv.id, &inv.tool_call_id, &inv.tool_name),
                    )
                    .await;
            }
        }
    }
}
