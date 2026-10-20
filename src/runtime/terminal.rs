//! Kết thúc run: ghi lỗi gateway hoặc trạng thái terminal.

use tracing::{info, warn};

use crate::gateway::GatewayError;
use crate::store::CODE_SHUTTING_DOWN;
use crate::types::RunStatus;

use super::AgentRuntime;

impl AgentRuntime {
    pub(super) async fn fail(&self, run_id: &str, error: GatewayError) -> bool {
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

    pub(super) async fn finish(
        &self,
        run_id: &str,
        status: RunStatus,
        code: &str,
        message: &str,
    ) -> bool {
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
