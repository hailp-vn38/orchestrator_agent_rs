//! `AgentRuntime`: runner duy nhất của một run (EMB-10). M1: single-agent text turn.
//!
//! Runtime là writer ở mức workflow; mọi ghi history đi qua `SessionStore` và final
//! message + terminal state commit trong cùng một transaction.

use std::collections::HashMap;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn, Instrument};

use crate::gateway::{GatewayError, LlmGateway, ModelMessage, ModelRequest, ModelRole};
use crate::store::{ConversationRole, SessionStore};
use crate::types::RunStatus;

/// Ghép system prompt: mandatory base luôn đứng đầu, prompt của agent không thay thế được.
pub(crate) fn build_system(base: &str, agent_prompt: &str) -> String {
    format!("{}\n\n{}", base.trim_end(), agent_prompt.trim())
}

pub(crate) struct AgentRuntime {
    store: SessionStore,
    gateway: Arc<dyn LlmGateway>,
    /// agent_id → system prompt đã ghép (snapshot lúc build, không hot reload).
    systems: HashMap<String, String>,
}

impl AgentRuntime {
    pub(crate) fn new(
        store: SessionStore,
        gateway: Arc<dyn LlmGateway>,
        systems: HashMap<String, String>,
    ) -> Self {
        Self {
            store,
            gateway,
            systems,
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
        let Some(system) = self.systems.get(&run.agent_id) else {
            return self.fail(run_id, GatewayError::NotConfigured).await;
        };
        let history = match self.store.conversation(&run.session_id).await {
            Ok(h) => h,
            Err(_) => return false,
        };
        let request = ModelRequest {
            provider_id: run.provider_id.clone(),
            model_id: run.model_id.clone(),
            system: system.clone(),
            messages: history
                .into_iter()
                .map(|(role, text)| ModelMessage {
                    role: match role {
                        ConversationRole::User => ModelRole::User,
                        ConversationRole::Assistant => ModelRole::Assistant,
                    },
                    text,
                })
                .collect(),
        };

        match self.gateway.complete(&request, cancel).await {
            Ok(response) => match self.store.complete_run(run_id, &response.text).await {
                Ok(_) => {
                    info!("run completed");
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
            },
            Err(e) => self.fail(run_id, e).await,
        }
    }

    async fn fail(&self, run_id: &str, error: GatewayError) -> bool {
        let (status, code, message) = match error {
            GatewayError::Cancelled => (
                RunStatus::Interrupted,
                "SHUTTING_DOWN",
                "the run was interrupted by shutdown",
            ),
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
            Ok(_) => {
                info!(
                    status = status.as_str(),
                    code, "run finished without final message"
                );
                true
            }
            Err(_) => {
                warn!("could not persist terminal state; supervisor will sweep");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_always_precedes_agent_prompt() {
        let s = build_system("BASE\n", "  agent prompt \n");
        assert!(s.starts_with("BASE"));
        assert!(s.ends_with("agent prompt"));
    }
}
