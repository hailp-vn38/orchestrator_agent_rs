//! Kết thúc run: ghi lỗi gateway hoặc trạng thái terminal.

use tracing::{info, warn};

use crate::gateway::{FailureClass, GatewayError};
use crate::store::{CODE_EXHAUSTED, CODE_SHUTTING_DOWN, CODE_STORE_UNAVAILABLE};
use crate::types::RunStatus;

use super::{AgentRuntime, Settlement};

const DURABLE_BACKOFF_SECS: [u64; 4] = [60, 300, 900, 1800];

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
        match error {
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
            _ => {}
        }
        let code = error.code();
        let base_message = error.message();
        let class = error.classify();
        let run_meta = crate::store::settle_sql(self.store.load_run(run_id))
            .await
            .ok()
            .flatten();
        let step_num = match &run_meta {
            Some(r) => crate::store::settle_sql(self.store.turn_steps(&r.session_id, &r.turn_id))
                .await
                .ok()
                .map(|s| s.saturating_add(1)),
            None => crate::store::settle_sql(self.store.run_steps(run_id))
                .await
                .ok()
                .map(|s| s.saturating_add(1)),
        };
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

        match class {
            FailureClass::Retryable { .. } | FailureClass::RetryOnceThenSplit => {
                let Some(ref r) = run_meta else {
                    return Settlement::Unsettled;
                };
                if r.resume_attempt.saturating_add(1) > self.context.max_resume_attempts {
                    return self
                        .finish(run_id, RunStatus::Failed, CODE_EXHAUSTED, &message)
                        .await;
                }
                let retry_idx = crate::store::settle_sql(
                    self.store.turn_retry_attempts(&r.session_id, &r.turn_id),
                )
                .await
                .unwrap_or(r.resume_attempt) as usize;
                let base_secs = DURABLE_BACKOFF_SECS[retry_idx.min(DURABLE_BACKOFF_SECS.len() - 1)];
                let wait_secs = match class {
                    FailureClass::Retryable {
                        retry_after: Some(d),
                    } => d.as_secs().max(base_secs),
                    _ => base_secs,
                };
                let next_at = self
                    .store
                    .now_secs()
                    .saturating_add(wait_secs.min(i64::MAX as u64) as i64);
                self.suspend(
                    run_id,
                    RunStatus::WaitingRetry,
                    code,
                    &message,
                    code,
                    Some(next_at),
                )
                .await
            }
            FailureClass::NeedsUser => {
                self.suspend(run_id, RunStatus::WaitingUser, code, &message, code, None)
                    .await
            }
            FailureClass::Permanent => self.finish(run_id, RunStatus::Failed, code, &message).await,
            FailureClass::Cancelled => {
                self.finish(run_id, RunStatus::Cancelled, "CANCELLED", &message)
                    .await
            }
        }
    }

    pub(super) async fn suspend(
        &self,
        run_id: &str,
        status: RunStatus,
        code: &str,
        message: &str,
        retry_reason: &str,
        next_attempt_at: Option<i64>,
    ) -> Settlement {
        match crate::store::settle_sql(self.store.suspend_run(
            run_id,
            status,
            code,
            message,
            retry_reason,
            next_attempt_at,
        ))
        .await
        {
            Ok(committed) => {
                info!(
                    status = status.as_str(),
                    code, next_attempt_at, "run suspended to waiting state"
                );
                if committed {
                    self.emitter
                        .run_waiting(run_id, status, code, retry_reason, next_attempt_at)
                        .await;
                }
                Settlement::Terminal
            }
            Err(_) => {
                warn!("could not persist waiting state; supervisor will sweep");
                Settlement::Unsettled
            }
        }
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

    /// Supervisor hủy cứng một run khi quá `deadline + reserve` của ngân sách.
    pub(crate) async fn hard_stop_budget(&self, run_id: &str) -> Settlement {
        self.finish(
            run_id,
            RunStatus::Failed,
            crate::types::CODE_BUDGET_EXHAUSTED,
            "the run exceeded its execution budget and reserve",
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
        match crate::store::settle_sql(
            self.store
                .finish_run_without_final(run_id, status, code, message),
        )
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
