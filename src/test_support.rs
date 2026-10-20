//! Seam test nội bộ (feature `test-support`): fake `LlmGateway` có kịch bản, và fault hook
//! cho crash window. Không phải API production: host không dựng gateway/store ở đây.

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use std::time::Duration;

pub use crate::artifact::ArtifactFault;
pub use crate::feed::FeedFault;
pub use crate::gateway::{
    ClientGateway, GatewayError, LlmGateway, ModelMessage, ModelRequest, ModelResponse, ModelRole,
    ModelTool, ModelToolCall, ModelUsage, StreamEvent, StreamSink,
};

/// Fault hook: khi `true`, trigger ngay sau admission bị bỏ (claim đã commit nhưng chưa
/// spawn). Chỉ sweep định kỳ của supervisor mới cứu được claim.
pub type DropAdmissionTriggers = Arc<AtomicBool>;

type Responder = Box<dyn Fn(&ModelRequest) -> ModelResponse + Send + Sync>;

enum Step {
    Fixed(Result<ModelResponse, GatewayError>),
    /// Dựng response từ request thật (vd. lấy `session_id` child mà runtime vừa sinh).
    Dynamic(Responder),
    /// Phát preview rồi mới trả kết quả (mô phỏng stream; lỗi giữa stream = `Err`).
    Streamed(Vec<StreamEvent>, Result<ModelResponse, GatewayError>),
    /// Chờ `Duration` (theo clock của tokio, điều khiển được bằng `time::pause`).
    Delayed(Duration, Result<ModelResponse, GatewayError>),
}

struct State {
    script: VecDeque<Step>,
    /// Kết quả cho request *summary* của compaction; không tiêu thụ `script`.
    summaries: VecDeque<Result<String, GatewayError>>,
    requests: Vec<ModelRequest>,
    emitted: Vec<StreamEvent>,
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
                summaries: VecDeque::new(),
                requests: Vec::new(),
                emitted: Vec::new(),
            }),
            gate: watch::channel(true).0,
        })
    }

    pub fn push_text(&self, text: impl Into<String>) {
        self.push_response(ModelResponse {
            text: text.into(),
            finish_reason: "end_turn".into(),
            tool_calls: Vec::new(),
            usage: None,
        });
    }

    /// Như [`Self::push_text`] kèm usage do provider báo.
    pub fn push_text_with_usage(&self, text: impl Into<String>, input: u64, output: u64) {
        self.push_response(ModelResponse {
            text: text.into(),
            finish_reason: "end_turn".into(),
            tool_calls: Vec::new(),
            usage: Some(ModelUsage {
                input_tokens: Some(input),
                output_tokens: Some(output),
            }),
        });
    }

    /// Response chứa tool call: `(provider call id, tên tool, arguments)`.
    pub fn push_tool_calls(
        &self,
        text: impl Into<String>,
        calls: Vec<(&str, &str, serde_json::Value)>,
    ) {
        self.push_response(ModelResponse {
            text: text.into(),
            finish_reason: "tool_use".into(),
            tool_calls: calls
                .into_iter()
                .map(|(id, name, arguments)| ModelToolCall {
                    id: id.into(),
                    name: name.into(),
                    arguments,
                })
                .collect(),
            usage: None,
        });
    }

    /// Stream `fragments` (text delta) rồi kết thúc `Finished` với text ghép lại.
    pub fn push_stream_text(&self, fragments: &[&str]) {
        let events = fragments
            .iter()
            .map(|f| StreamEvent::TextDelta((*f).to_owned()))
            .collect();
        self.state().script.push_back(Step::Streamed(
            events,
            Ok(ModelResponse {
                text: fragments.concat(),
                finish_reason: "end_turn".into(),
                tool_calls: Vec::new(),
                usage: None,
            }),
        ));
    }

    /// Phát `events` (preview) rồi mới xác nhận `response` đã `Finished`.
    pub fn push_response_streamed(&self, events: Vec<StreamEvent>, response: ModelResponse) {
        self.state()
            .script
            .push_back(Step::Streamed(events, Ok(response)));
    }

    /// Phát `events` rồi lỗi trước `Finished`: không có response nào được xác nhận.
    pub fn push_stream_failure(&self, events: Vec<StreamEvent>, error: GatewayError) {
        self.state()
            .script
            .push_back(Step::Streamed(events, Err(error)));
    }

    /// Trả `response` sau `delay` (clock tokio).
    pub fn push_delayed(&self, delay: Duration, response: ModelResponse) {
        self.state()
            .script
            .push_back(Step::Delayed(delay, Ok(response)));
    }

    /// Kết quả của lần summary compaction kế tiếp (mặc định `SUMMARY: condensed`).
    pub fn push_summary(&self, result: Result<&str, GatewayError>) {
        self.state().summaries.push_back(result.map(str::to_owned));
    }

    /// Request này là bước summary của compaction (không phải step của agent).
    pub fn is_summary_request(request: &ModelRequest) -> bool {
        request.system == crate::compaction::SUMMARY_SYSTEM
    }

    /// Các request của agent (không tính summary).
    pub fn agent_requests(&self) -> Vec<ModelRequest> {
        self.requests()
            .into_iter()
            .filter(|r| !Self::is_summary_request(r))
            .collect()
    }

    /// Các request summary của compaction.
    pub fn summary_requests(&self) -> Vec<ModelRequest> {
        self.requests()
            .into_iter()
            .filter(Self::is_summary_request)
            .collect()
    }

    /// Preview mà gateway đã phát ra (theo thứ tự), kể cả của stream bị lỗi.
    pub fn emitted_events(&self) -> Vec<StreamEvent> {
        self.state().emitted.clone()
    }

    pub fn push_response(&self, response: ModelResponse) {
        self.state().script.push_back(Step::Fixed(Ok(response)));
    }

    pub fn push_error(&self, error: GatewayError) {
        self.state().script.push_back(Step::Fixed(Err(error)));
    }

    /// Response được tính từ request tại thời điểm gateway được gọi.
    pub fn push_with(
        &self,
        responder: impl Fn(&ModelRequest) -> ModelResponse + Send + Sync + 'static,
    ) {
        self.state()
            .script
            .push_back(Step::Dynamic(Box::new(responder)));
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
        sink: StreamSink<'_>,
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
        if Self::is_summary_request(request) {
            let next = self.state().summaries.pop_front();
            return next
                .unwrap_or_else(|| Ok("SUMMARY: condensed".to_owned()))
                .map(|text| ModelResponse {
                    text,
                    finish_reason: "end_turn".into(),
                    tool_calls: Vec::new(),
                    usage: None,
                });
        }
        let next = self.state().script.pop_front();
        match next {
            Some(Step::Fixed(result)) => result,
            Some(Step::Dynamic(responder)) => Ok(responder(request)),
            Some(Step::Streamed(events, result)) => {
                for event in events {
                    self.state().emitted.push(event.clone());
                    sink(event);
                }
                result
            }
            Some(Step::Delayed(delay, result)) => {
                tokio::select! {
                    () = cancel.cancelled() => Err(GatewayError::Cancelled),
                    () = tokio::time::sleep(delay) => result,
                }
            }
            None => {
                let last_user = request
                    .messages
                    .iter()
                    .rev()
                    .find(|m| m.role == ModelRole::User)
                    .map(|m| m.text.as_str())
                    .unwrap_or_default();
                Ok(ModelResponse {
                    text: format!("echo: {last_user}"),
                    finish_reason: "end_turn".into(),
                    tool_calls: Vec::new(),
                    usage: None,
                })
            }
        }
    }
}
