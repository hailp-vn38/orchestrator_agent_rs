//! Một LLM step: dựng request, gọi gateway (stream/non-stream), ghi usage.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde_json::json;

use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::compaction::estimate_tokens;
use crate::executor::Effective;
use crate::feed::{chunks, event};
use crate::gateway::{
    GatewayError, ModelMessage, ModelRequest, ModelResponse, ModelRole, StreamEvent,
};
use crate::prompt::context_messages;
use crate::store::{new_id, RunRecord};

use super::{AgentRuntime, Settlement, STEP_LIMIT_GUIDANCE};

impl AgentRuntime {
    pub(super) async fn build_request(
        &self,
        run: &RunRecord,
        eff: &Effective,
    ) -> Option<ModelRequest> {
        let history = self.store.conversation(&run.session_id).await.ok()?;
        let catalog = self.executor.skills().available(&eff.skills);
        let system = self.prompt.system(&run.agent_id, &catalog)?;
        Some(ModelRequest {
            provider_id: run.provider_id.clone(),
            model_id: run.model_id.clone(),
            system,
            messages: context_messages(history),
            tools: self.executor.definitions(eff),
            params: self
                .agents
                .get(&run.agent_id)
                .map(|a| a.llm)
                .unwrap_or_default(),
        })
    }

    /// Một logical LLM step: compact chủ động khi gần chạm limit, gọi model, và nếu provider
    /// báo overflow thì compact + retry **tối đa một lần**. `Err(done)`: run đã được kết thúc
    /// (hoặc không commit được) với giá trị trả về của `execute`.
    pub(super) async fn model_step(
        &self,
        run: &RunRecord,
        eff: &Effective,
        capped: bool,
        cancel: &CancellationToken,
    ) -> Result<(ModelResponse, String), Settlement> {
        let run_id = run.run_id.as_str();
        let mut request = self
            .step_request(run, eff, capped)
            .await
            .ok_or(Settlement::Unsettled)?;
        let agent_limit = self
            .agents
            .get(&run.agent_id)
            .and_then(|a| a.context_limit_tokens);
        if estimate_tokens(&request) >= self.context.threshold(agent_limit) {
            // Lỗi compact chủ động không chặn run: request vẫn có thể vừa; overflow thật sẽ
            // đi qua nhánh retry bên dưới.
            if let Ok(true) = self.compact(run, cancel).await {
                request = self
                    .step_request(run, eff, capped)
                    .await
                    .ok_or(Settlement::Unsettled)?;
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
                            request = self
                                .step_request(run, eff, capped)
                                .await
                                .ok_or(Settlement::Unsettled)?;
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
    pub(super) async fn stream_step(
        &self,
        run: &RunRecord,
        request: &ModelRequest,
        cancel: &CancellationToken,
        step_id: &str,
    ) -> Result<ModelResponse, GatewayError> {
        let feed = self.emitter.feed();
        let root = run.root_session_id();
        let (session, run_id) = (run.session_id.as_str(), run.run_id.as_str());
        feed.ephemeral(
            root,
            session,
            Some(run_id),
            event::STEP_STARTED,
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
                    event::TEXT_DELTA,
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
        feed.ephemeral(root, session, Some(run_id), event::STEP_ENDED, ended);
        result
    }

    /// Request của step; khi `capped` bỏ mọi tool và thêm hướng dẫn text-only.
    pub(super) async fn step_request(
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
    pub(super) async fn record_usage(&self, run_id: &str, response: &ModelResponse) {
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
}
