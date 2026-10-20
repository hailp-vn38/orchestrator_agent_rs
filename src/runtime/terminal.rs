//! Kết thúc run: ghi lỗi gateway hoặc trạng thái terminal.

use tracing::{info, warn};

use crate::gateway::GatewayError;
use crate::store::{CODE_SHUTTING_DOWN, CODE_STORE_UNAVAILABLE};
use crate::types::RunStatus;

use super::{AgentRuntime, Settlement};

impl AgentRuntime {
    pub(super) async fn fail(&self, run_id: &str, error: GatewayError) -> Settlement {
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
