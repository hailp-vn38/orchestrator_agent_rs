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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::json;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn, Instrument};

use crate::agent::AgentDefinition;
use crate::compaction::{estimate_tokens, plan, SUMMARY_SYSTEM};
use crate::executor::{Dispatch, Effective, Prepared, ToolExecutor, CODE_OUTCOME_UNKNOWN};
use crate::feed::{chunks, Emitter};
use crate::gateway::{
    GatewayError, LlmGateway, ModelMessage, ModelRequest, ModelResponse, ModelRole, StreamEvent,
};
use crate::prompt::{context_messages, PromptBuilder};
use crate::store::{
    new_id, NewCheckpoint, PendingInvocation, RunRecord, SessionStore, StoredToolCall,
    CODE_SHUTTING_DOWN,
};
use crate::tool::ToolScope;
use crate::types::RunStatus;

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
    fn threshold(&self) -> usize {
        self.limit_tokens
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

/// Kết luận của một bước điều phối: tiếp tục vòng lặp hay kết thúc `execute`.
enum Flow {
    Continue,
    /// Giá trị trả về của `execute` (xem doc của nó).
    Stop(bool),
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

    /// Chạy một run đã được claim. Trả `true` khi run đã ở trạng thái terminal bền vững
    /// (hoặc vốn đã terminal); `false` khi không commit được (supervisor sẽ sweep lại).
    pub(crate) async fn execute(&self, run_id: &str, cancel: &CancellationToken) -> bool {
        let span = tracing::info_span!(
            "agent_run",
            run_id = %run_id,
            session_id = tracing::field::Empty
        );
        self.execute_inner(run_id, cancel).instrument(span).await
    }

    async fn execute_inner(&self, run_id: &str, cancel: &CancellationToken) -> bool {
        let run = match self.store.load_run(run_id).await {
            Ok(Some(r)) if r.status == RunStatus::Running => r,
            Ok(_) => return true,
            Err(_) => return false,
        };
        tracing::Span::current().record("session_id", tracing::field::display(&run.session_id));
        self.emitter
            .committed_run(run_id, "run.started", json!({ "agent_id": run.agent_id }))
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
            Err(_) => return false,
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
                (_, Err(_)) => return false,
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
                                    "message.committed",
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
                        true
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
                Ok(None) => return true, // run đã terminal ở nơi khác
                Err(_) => return false,  // chưa có invocation nào → sweep gọi lại LLM, an toàn
            };
            self.emitter
                .committed_run(
                    run_id,
                    "message.committed",
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
                        "tool.started",
                        json!({
                            "tool_call_id": inv.tool_call_id,
                            "invocation_id": inv.id,
                            "tool_name": inv.tool_name,
                        }),
                    )
                    .await;
            }
        }
    }

    async fn build_request(&self, run: &RunRecord, eff: &Effective) -> Option<ModelRequest> {
        let history = self.store.conversation(&run.session_id).await.ok()?;
        let catalog = self.executor.skills().available(&eff.skills);
        let system = self.prompt.system(&run.agent_id, &catalog)?;
        Some(ModelRequest {
            provider_id: run.provider_id.clone(),
            model_id: run.model_id.clone(),
            system,
            messages: context_messages(history),
            tools: self.executor.definitions(eff),
        })
    }

    /// Một logical LLM step: compact chủ động khi gần chạm limit, gọi model, và nếu provider
    /// báo overflow thì compact + retry **tối đa một lần**. `Err(done)`: run đã được kết thúc
    /// (hoặc không commit được) với giá trị trả về của `execute`.
    async fn model_step(
        &self,
        run: &RunRecord,
        eff: &Effective,
        capped: bool,
        cancel: &CancellationToken,
    ) -> Result<(ModelResponse, String), bool> {
        let run_id = run.run_id.as_str();
        let mut request = self.step_request(run, eff, capped).await.ok_or(false)?;
        if estimate_tokens(&request) >= self.context.threshold() {
            // Lỗi compact chủ động không chặn run: request vẫn có thể vừa; overflow thật sẽ
            // đi qua nhánh retry bên dưới.
            if let Ok(true) = self.compact(run, cancel).await {
                request = self.step_request(run, eff, capped).await.ok_or(false)?;
            }
        }
        let mut retried = false;
        loop {
            let step_id = new_id();
            match self.stream_step(run, &request, cancel, &step_id).await {
                Ok(response) => {
                    self.record_usage(run_id, &response).await;
                    return Ok((response, step_id));
                }
                Err(GatewayError::ContextOverflow) if !retried => {
                    retried = true;
                    match self.compact(run, cancel).await {
                        Ok(true) => {
                            request = self.step_request(run, eff, capped).await.ok_or(false)?;
                        }
                        Ok(false) => {
                            return Err(self.fail(run_id, GatewayError::ContextOverflow).await)
                        }
                        Err(e) => return Err(self.fail(run_id, e).await),
                    }
                }
                Err(e) => return Err(self.fail(run_id, e).await),
            }
        }
    }

    /// Một lần gọi model kèm preview ephemeral: `step.started`, `text.delta` theo thứ tự (secret đã
    /// cấu hình bị che kể cả khi bị cắt qua nhiều fragment), rồi `step.ended`. Preview **không**
    /// phải text đã xác nhận: step lỗi giữa chừng kết thúc với `completed=false`. `ToolCallReady`
    /// không phát event và không cho phép execute tool.
    async fn stream_step(
        &self,
        run: &RunRecord,
        request: &ModelRequest,
        cancel: &CancellationToken,
        step_id: &str,
    ) -> Result<ModelResponse, GatewayError> {
        let feed = self.emitter.feed();
        let root = run.parent_session_id.as_deref().unwrap_or(&run.session_id);
        let (session, run_id) = (run.session_id.as_str(), run.run_id.as_str());
        feed.ephemeral(
            root,
            session,
            Some(run_id),
            "step.started",
            json!({ "step_id": step_id }),
        );
        let filter = Mutex::new(feed.delta_filter());
        let next_index = AtomicU64::new(0);
        let emit_text = |text: &str| {
            for part in chunks(text, feed.delta_max_bytes()) {
                feed.ephemeral(
                    root,
                    session,
                    Some(run_id),
                    "text.delta",
                    json!({
                        "step_id": step_id,
                        "delta_index": next_index.fetch_add(1, Ordering::Relaxed),
                        "delta": part,
                    }),
                );
            }
        };
        let sink = |event: StreamEvent| {
            if let StreamEvent::TextDelta(fragment) = event {
                let safe = filter
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(&fragment);
                emit_text(&safe);
            }
        };
        let result = self.gateway.complete(request, cancel, &sink).await;
        let tail = filter.lock().unwrap_or_else(|p| p.into_inner()).flush();
        emit_text(&tail);
        let ended = match &result {
            Ok(response) => json!({
                "step_id": step_id,
                "completed": true,
                "usage": response.usage.map(|u| json!({
                    "input_tokens": u.input_tokens,
                    "output_tokens": u.output_tokens,
                })),
            }),
            Err(e) => json!({ "step_id": step_id, "completed": false, "error_code": e.code() }),
        };
        feed.ephemeral(root, session, Some(run_id), "step.ended", ended);
        result
    }

    /// Request của step; khi `capped` bỏ mọi tool và thêm hướng dẫn text-only.
    async fn step_request(
        &self,
        run: &RunRecord,
        eff: &Effective,
        capped: bool,
    ) -> Option<ModelRequest> {
        let mut request = self.build_request(run, eff).await?;
        if capped {
            request.tools.clear();
            request
                .messages
                .push(ModelMessage::text(ModelRole::User, STEP_LIMIT_GUIDANCE));
        }
        Some(request)
    }

    /// Ghi usage đo được; thiếu usage là bình thường (không lỗi, không bịa số).
    async fn record_usage(&self, run_id: &str, response: &ModelResponse) {
        if let Some(u) = response.usage {
            let (i, o) = (
                u.input_tokens.unwrap_or_default(),
                u.output_tokens.unwrap_or_default(),
            );
            if (i > 0 || o > 0) && self.store.add_usage(run_id, i, o).await.is_err() {
                warn!("could not record provider usage");
            }
        }
    }

    /// Nén settled exchange prefix của session và persist checkpoint. `Ok(true)`: đã có
    /// checkpoint mới; `Ok(false)`: không còn gì để nén (hoặc run không còn running).
    async fn compact(
        &self,
        run: &RunRecord,
        cancel: &CancellationToken,
    ) -> Result<bool, GatewayError> {
        let context = self
            .store
            .conversation(&run.session_id)
            .await
            .map_err(|_| GatewayError::Protocol)?;
        let Some(plan) = plan(&context, self.context.keep_recent) else {
            return Ok(false);
        };
        self.emitter.feed().ephemeral(
            run.parent_session_id.as_deref().unwrap_or(&run.session_id),
            &run.session_id,
            Some(&run.run_id),
            "compaction.started",
            json!({}),
        );
        let request = ModelRequest {
            provider_id: run.provider_id.clone(),
            model_id: run.model_id.clone(),
            system: SUMMARY_SYSTEM.to_owned(),
            messages: vec![ModelMessage::text(ModelRole::User, plan.input)],
            tools: Vec::new(),
        };
        let response = self
            .gateway
            .complete(&request, cancel, &|_: StreamEvent| {})
            .await?;
        self.record_usage(&run.run_id, &response).await;
        let summary = response.text.trim();
        if summary.is_empty() {
            return Err(GatewayError::Protocol);
        }
        let checkpoint = NewCheckpoint {
            summary: summary.to_owned(),
            keep_from_sequence: plan.keep_from_sequence,
            objective: plan.objective,
            model_ref: format!("{}/{}", run.provider_id, run.model_id),
        };
        let written = self
            .store
            .append_compaction(&run.run_id, &checkpoint)
            .await
            .map_err(|_| GatewayError::Protocol)?;
        if written {
            self.emitter
                .committed_run(&run.run_id, "compaction.completed", json!({}))
                .await;
        }
        Ok(written)
    }

    /// Dispatch tuần tự các invocation `requested`; settle đủ trước khi trả `Continue`.
    async fn settle_pending(
        &self,
        run_id: &str,
        run: &RunRecord,
        eff: &Effective,
        pending: Vec<PendingInvocation>,
        cancel: &CancellationToken,
    ) -> Flow {
        for inv in pending {
            if cancel.is_cancelled() {
                // Chưa chạy invocation này → còn `requested`, sẽ được đưa về interrupted.
                return Flow::Stop(self.fail(run_id, GatewayError::Cancelled).await);
            }
            let span = tracing::info_span!(
                "tool_call",
                invocation_id = %inv.id,
                tool_call_id = %inv.tool_call_id,
                tool = %inv.tool_name
            );
            if let Some(done) = self
                .settle_one(run_id, run, eff, &inv, cancel)
                .instrument(span)
                .await
            {
                return Flow::Stop(done);
            }
        }
        Flow::Continue
    }

    /// `Some(v)`: kết thúc `execute` với giá trị `v`; `None`: invocation đã settle.
    async fn settle_one(
        &self,
        run_id: &str,
        run: &RunRecord,
        eff: &Effective,
        inv: &PendingInvocation,
        cancel: &CancellationToken,
    ) -> Option<bool> {
        let settled = match self
            .executor
            .prepare(&inv.tool_name, inv.arguments.clone(), eff)
        {
            // Bị chặn trước dispatch: không có side effect; chỉ ghi lỗi đúng pairing.
            Prepared::Rejected(s) => s,
            Prepared::Run(runnable) => {
                match self.store.begin_invocation(run_id, &inv.id).await {
                    Ok(true) => {}
                    Ok(false) => return Some(true),
                    Err(_) => return Some(false),
                }
                let scope = self.scope(run, eff, inv, run_id);
                match self.executor.run(runnable, scope, eff, cancel).await {
                    Dispatch::Settled(s) => s,
                    Dispatch::Cancelled => {
                        return Some(self.fail(run_id, GatewayError::Cancelled).await)
                    }
                    Dispatch::Unknown(code) => {
                        return Some(
                            self.finish(
                                run_id,
                                RunStatus::Interrupted,
                                code,
                                "the tool outcome is unknown; it was not replayed",
                            )
                            .await,
                        )
                    }
                }
            }
        };
        match self
            .store
            .settle_invocation(run_id, &inv.id, &settled)
            .await
        {
            Ok(true) => {
                // Chỉ sau khi tool-result + terminal state đã commit.
                self.emitter
                    .committed_run(
                        run_id,
                        if settled.failed {
                            "tool.failed"
                        } else {
                            "tool.completed"
                        },
                        json!({
                            "tool_call_id": inv.tool_call_id,
                            "invocation_id": inv.id,
                            "tool_name": inv.tool_name,
                            "error_code": settled.error_code,
                        }),
                    )
                    .await;
                None
            }
            Ok(false) => Some(true),
            Err(_) => {
                // Tool có thể đã chạy xong nhưng không xác nhận được: invocation vẫn `running`,
                // lần sweep sau sẽ chặn thay vì replay.
                warn!("could not commit the tool result; the outcome stays unconfirmed");
                Some(false)
            }
        }
    }

    fn scope(
        &self,
        run: &RunRecord,
        eff: &Effective,
        inv: &PendingInvocation,
        run_id: &str,
    ) -> ToolScope {
        ToolScope {
            owner_id: run.owner_id.clone(),
            workspace_id: run.workspace_id.clone(),
            session_id: run.session_id.clone(),
            root_session_id: run
                .parent_session_id
                .clone()
                .unwrap_or_else(|| run.session_id.clone()),
            run_id: run_id.to_owned(),
            agent_id: run.agent_id.clone(),
            invocation_id: inv.id.clone(),
            tool_call_id: inv.tool_call_id.clone(),
            allowed_tools: eff.tools.iter().cloned().collect(),
            allowed_skills: eff.skills.iter().cloned().collect(),
        }
    }

    async fn fail(&self, run_id: &str, error: GatewayError) -> bool {
        let (status, code, message) = match error {
            GatewayError::Cancelled if self.shutdown.is_cancelled() => (
                RunStatus::Interrupted,
                CODE_SHUTTING_DOWN,
                "the run was interrupted by shutdown",
            ),
            GatewayError::Cancelled => (RunStatus::Cancelled, "CANCELLED", "the run was cancelled"),
            GatewayError::Timeout => (RunStatus::TimedOut, error.code(), error.message()),
            _ => (RunStatus::Failed, error.code(), error.message()),
        };
        self.finish(run_id, status, code, message).await
    }

    async fn finish(&self, run_id: &str, status: RunStatus, code: &str, message: &str) -> bool {
        match self
            .store
            .finish_run_without_final(run_id, status, code, message)
            .await
        {
            Ok(committed) => {
                info!(
                    status = status.as_str(),
                    code, "run finished without final message"
                );
                if committed {
                    self.emitter.run_terminal(run_id, status, Some(code)).await;
                }
                true
            }
            Err(_) => {
                warn!("could not persist terminal state; supervisor will sweep");
                false
            }
        }
    }
}
