//! Seam test nội bộ (feature `test-support`): fake `LlmGateway` có kịch bản, và fault hook
//! cho crash window. Không phải API production: host không dựng gateway/store ở đây.

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub use crate::gateway::{
    GatewayError, LlmGateway, ModelMessage, ModelRequest, ModelResponse, ModelRole,
};

/// Fault hook: khi `true`, trigger ngay sau admission bị bỏ (claim đã commit nhưng chưa
/// spawn). Chỉ sweep định kỳ của supervisor mới cứu được claim.
pub type DropAdmissionTriggers = Arc<AtomicBool>;

struct State {
    script: VecDeque<Result<String, GatewayError>>,
    requests: Vec<ModelRequest>,
}

/// Fake gateway: trả theo kịch bản, mặc định echo `echo: <tin nhắn user cuối>`.
/// Ghi lại mọi request để assert (system prompt, history, số lần gọi).
pub struct ScriptedGateway {
    state: Mutex<State>,
    gate: watch::Sender<bool>,
}

impl ScriptedGateway {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                script: VecDeque::new(),
                requests: Vec::new(),
            }),
            gate: watch::channel(true).0,
        })
    }

    pub fn push_text(&self, text: impl Into<String>) {
        self.state().script.push_back(Ok(text.into()));
    }

    pub fn push_error(&self, error: GatewayError) {
        self.state().script.push_back(Err(error));
    }

    /// Các lần gọi sau đó chặn (sau khi đã được ghi nhận) cho tới `release`.
    pub fn hold(&self) {
        self.gate.send_replace(false);
    }

    pub fn release(&self) {
        self.gate.send_replace(true);
    }

    pub fn requests(&self) -> Vec<ModelRequest> {
        self.state().requests.clone()
    }

    /// Số lần gateway đã được gọi (kể cả đang bị `hold`).
    pub fn call_count(&self) -> usize {
        self.state().requests.len()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[async_trait]
impl LlmGateway for ScriptedGateway {
    async fn complete(
        &self,
        request: &ModelRequest,
        cancel: &CancellationToken,
    ) -> Result<ModelResponse, GatewayError> {
        self.state().requests.push(request.clone());
        let mut open = self.gate.subscribe();
        while !*open.borrow() {
            tokio::select! {
                () = cancel.cancelled() => return Err(GatewayError::Cancelled),
                changed = open.changed() => if changed.is_err() { break },
            }
        }
        if cancel.is_cancelled() {
            return Err(GatewayError::Cancelled);
        }
        let next = self.state().script.pop_front();
        let text = match next {
            Some(Err(e)) => return Err(e),
            Some(Ok(t)) => t,
            None => {
                let last_user = request
                    .messages
                    .iter()
                    .rev()
                    .find(|m| m.role == ModelRole::User)
                    .map(|m| m.text.as_str())
                    .unwrap_or_default();
                format!("echo: {last_user}")
            }
        };
        Ok(ModelResponse {
            text,
            finish_reason: "end_turn".into(),
        })
    }
}
