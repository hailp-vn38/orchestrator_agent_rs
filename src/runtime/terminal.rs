//! Kết thúc run: ghi lỗi gateway hoặc trạng thái terminal.

use tracing::{info, warn};

use crate::gateway::GatewayError;
use crate::store::{CODE_SHUTTING_DOWN, CODE_STORE_UNAVAILABLE};
use crate::types::RunStatus;

use super::{AgentRuntime, Settlement};

impl AgentRuntime {
    pub(super) async fn fail(&self, run_id: &str, error: GatewayError) -> Settlement {
        self.fail_with_detail(run_id, error, None).await
    }

    pub(super) async fn fail_with_detail(
        &self,
        run_id: &str,
        error: GatewayError,
        extra_detail: Option<&str>,
    ) -> Settlement {
        let (status, code, base_message) = match error {
            GatewayError::Cancelled if self.shutdown.is_cancelled() => {
                return self
                    .finish(
                        run_id,
                        RunStatus::Interrupted,
                        CODE_SHUTTING_DOWN,
                        "the run was interrupted by shutdown",
                    )
                    .await;
            }
            GatewayError::Cancelled => {
                return self
                    .finish(
                        run_id,
                        RunStatus::Cancelled,
                        "CANCELLED",
                        "the run was cancelled",
                    )
                    .await;
            }
            GatewayError::Timeout => (RunStatus::TimedOut, error.code(), error.message()),
            _ => (RunStatus::Failed, error.code(), error.message()),
        };
        let run_meta = self.store.load_run(run_id).await.ok().flatten();
        let step_num = self
            .store
            .run_steps(run_id)
            .await
            .ok()
            .map(|s| s.saturating_add(1));
        let gw_detail = run_meta
            .as_ref()
            .and_then(|r| self.gateway.take_last_error_detail(&r.provider_id));
        let detail = gw_detail
            .as_deref()
            .or(extra_detail)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let ctx_header = match (&run_meta, step_num) {
            (Some(r), Some(step)) => format!(
                "{base_message} [agent='{}', provider='{}', model='{}', step={step}]",
                r.agent_id, r.provider_id, r.model_id
            ),
            (Some(r), None) => format!(
                "{base_message} [agent='{}', provider='{}', model='{}']",
                r.agent_id, r.provider_id, r.model_id
            ),
            _ => base_message.to_owned(),
        };
        let message = match detail {
            Some(d) => format!("{ctx_header}: {d}"),
            None => ctx_header,
        };
        self.finish(run_id, status, code, &message).await
    }

    /// Supervisor bỏ cuộc sau nhiều lần `Unsettled` liên tiếp: chuyển run sang `interrupted`
    /// (`STORE_UNAVAILABLE`, phục hồi được lúc khởi động) để không gọi lại model vô hạn.
    /// Invocation `requested` được giữ nguyên; `running` thành `interrupted` như mọi lần ngắt.
    pub(crate) async fn give_up(&self, run_id: &str) -> Settlement {
        self.finish(
            run_id,
            RunStatus::Interrupted,
            CODE_STORE_UNAVAILABLE,
            "the run could not record its progress after repeated attempts; it will be \
             recovered on the next start",
        )
        .await
    }

    pub(super) async fn finish(
        &self,
        run_id: &str,
        status: RunStatus,
        code: &str,
        message: &str,
    ) -> Settlement {
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
                Settlement::Terminal
            }
            Err(_) => {
                warn!("could not persist terminal state; supervisor will sweep");
                Settlement::Unsettled
            }
        }
    }
}
