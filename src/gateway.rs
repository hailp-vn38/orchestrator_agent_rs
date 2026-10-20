//! `LlmGateway`: internal seam giữa runtime và provider protocol (docs/08, docs/18 §18.5).
//!
//! Production dùng [`ClientGateway`] trên crate `llm` (pin `=1.3.8`, evidence G0). Type của
//! `llm` không bao giờ ra public API. M2: tool definitions/calls/results. M4: stream được lắp
//! ráp thành một response duy nhất; text delta và `ToolCallReady` chỉ là preview qua
//! [`StreamSink`] (không persist, không kích hoạt execution).
//!
//! Item ở đây là `pub` trong module private; chỉ re-export qua `test_support` khi bật
//! feature `test-support`, để production host không thể inject/lắp ráp gateway.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use llm::builder::{LLMBackend as LlmCrateBackend, LLMBuilder};
use llm::chat::{ChatMessage, FunctionTool, StreamChunk, Tool};
use llm::error::LLMError;
use llm::{FunctionCall, LLMProvider, ToolCall};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::config::{LlmBackend, LlmProviderConfig, ProviderAuth};

/// Trần output token mỗi request khi config provider chưa có tùy chọn riêng (M1).
const MAX_OUTPUT_TOKENS: u32 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelRole {
    User,
    Assistant,
    /// Kết quả của một tool call trước đó (`tool_call_id` + `tool_name` được điền).
    Tool,
}

/// Tool call do model đề xuất; `id` là provider call ID (chỉ duy nhất trong một response).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelMessage {
    pub role: ModelRole,
    pub text: String,
    /// Chỉ dùng với `Assistant`: các tool call của message đó.
    pub tool_calls: Vec<ModelToolCall>,
    /// Chỉ dùng với `Tool`: provider call ID mà result này ghép với.
    pub tool_call_id: Option<String>,
    /// Chỉ dùng với `Tool`: tên tool đã được gọi.
    pub tool_name: Option<String>,
}

impl ModelMessage {
    pub fn text(role: ModelRole, text: impl Into<String>) -> Self {
        Self {
            role,
            text: text.into(),
            tool_calls: Vec::new(),
            tool_call_id: None,
            tool_name: None,
        }
    }

    pub fn assistant_with_calls(text: impl Into<String>, calls: Vec<ModelToolCall>) -> Self {
        Self {
            tool_calls: calls,
            ..Self::text(ModelRole::Assistant, text)
        }
    }

    pub fn tool_result(
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            tool_call_id: Some(call_id.into()),
            tool_name: Some(tool_name.into()),
            ..Self::text(ModelRole::Tool, text)
        }
    }
}

/// Tool được cung cấp cho model ở request này (đã lọc theo effective permissions).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelTool {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// Request đã resolve theo agent: `system` = mandatory base + prompt của agent (+ project
/// instructions + skill catalog được phép).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRequest {
    pub provider_id: String,
    pub model_id: String,
    pub system: String,
    pub messages: Vec<ModelMessage>,
    pub tools: Vec<ModelTool>,
}

/// Usage do provider báo; `None` ở cấp response nghĩa là provider không trả số liệu
/// (runtime không bịa số và không coi đó là lỗi).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModelUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelResponse {
    pub text: String,
    pub finish_reason: String,
    /// Chỉ có khi toàn response đã `Done` và mọi arguments là JSON hợp lệ.
    pub tool_calls: Vec<ModelToolCall>,
    pub usage: Option<ModelUsage>,
}

/// Preview ephemeral trong lúc stream. Không phải trạng thái bền vững: lỗi/hủy giữa chừng
/// khiến mọi preview bị bỏ, và `ToolCallReady` **không** cho phép execute tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    TextDelta(String),
    /// Một call đã đủ arguments hợp lệ nhưng response chưa `Finished` (chưa được xác nhận).
    ToolCallReady {
        id: String,
        name: String,
    },
}

/// Nơi nhận preview; gọi đồng bộ theo đúng thứ tự provider phát.
pub type StreamSink<'a> = &'a (dyn Fn(StreamEvent) + Send + Sync);

/// Lỗi gateway đã sanitize: văn bản thô của provider bị bỏ (có thể echo secret).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayError {
    NotConfigured,
    Auth,
    InvalidRequest,
    Provider,
    Transport,
    Timeout,
    Cancelled,
    Protocol,
    /// Provider từ chối vì prompt vượt context window trước khi sinh output.
    ContextOverflow,
}

impl GatewayError {
    /// Mã ổn định ghi vào `agent_runs.error_code` (docs/18 §18.5.4).
    pub fn code(self) -> &'static str {
        match self {
            Self::NotConfigured => "MODEL_NOT_CONFIGURED",
            Self::Auth => "PROVIDER_AUTH_FAILED",
            Self::InvalidRequest => "PROVIDER_INVALID_REQUEST",
            Self::Provider | Self::Transport => "PROVIDER_UNAVAILABLE",
            Self::Timeout => "PROVIDER_TIMEOUT",
            Self::Cancelled => "CANCELLED",
            Self::Protocol => "PROVIDER_PROTOCOL_ERROR",
            Self::ContextOverflow => "CONTEXT_LIMIT",
        }
    }

    /// Thông điệp cố định, không chứa dữ liệu từ provider.
    pub fn message(self) -> &'static str {
        match self {
            Self::NotConfigured => "no provider/model is configured for this agent",
            Self::Auth => "the provider rejected the configured credentials",
            Self::InvalidRequest => "the provider rejected the request",
            Self::Provider => "the provider reported an error",
            Self::Transport => "could not reach the provider",
            Self::Timeout => "the provider request timed out",
            Self::Cancelled => "the request was cancelled",
            Self::Protocol => "the provider returned an unexpected response",
            Self::ContextOverflow => "the conversation exceeds the model context limit",
        }
    }
}

#[async_trait]
pub trait LlmGateway: Send + Sync {
    /// Một lượt model hoàn chỉnh. Chỉ trả `Ok` khi toàn response đã `Done`; mọi
    /// lỗi/hủy/timeout giữa chừng loại bỏ phần text và call đã nhận. `sink` chỉ nhận preview.
    async fn complete(
        &self,
        request: &ModelRequest,
        cancel: &CancellationToken,
        sink: StreamSink<'_>,
    ) -> Result<ModelResponse, GatewayError>;
}

type RawStream = Pin<Box<dyn Stream<Item = Result<StreamChunk, LLMError>> + Send>>;

/// Gateway thật: dựng client `llm` lười theo `(provider, model, system)` và tái sử dụng.
pub struct ClientGateway {
    providers: HashMap<String, LlmProviderConfig>,
    clients: Mutex<HashMap<String, Arc<dyn LLMProvider>>>,
}

impl ClientGateway {
    pub fn new(providers: &[LlmProviderConfig]) -> Self {
        Self {
            providers: providers
                .iter()
                .map(|p| (p.id.trim().to_string(), p.clone()))
                .collect(),
            clients: Mutex::new(HashMap::new()),
        }
    }

    fn client(
        &self,
        cfg: &LlmProviderConfig,
        request: &ModelRequest,
    ) -> Result<Arc<dyn LLMProvider>, GatewayError> {
        let key = {
            let mut h = Sha256::new();
            h.update(request.system.as_bytes());
            let digest: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
            format!("{}\n{}\n{digest}", cfg.id.trim(), request.model_id)
        };
        let mut clients = self.clients.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(c) = clients.get(&key) {
            return Ok(Arc::clone(c));
        }
        let client: Arc<dyn LLMProvider> = Arc::from(build_client(cfg, request)?);
        clients.insert(key, Arc::clone(&client));
        Ok(client)
    }
}

fn build_client(
    cfg: &LlmProviderConfig,
    request: &ModelRequest,
) -> Result<Box<dyn LLMProvider>, GatewayError> {
    let backend = match cfg.backend {
        LlmBackend::OpenAi | LlmBackend::OpenAiCompatible => LlmCrateBackend::OpenAI,
        LlmBackend::Anthropic => LlmCrateBackend::Anthropic,
        LlmBackend::Gemini => LlmCrateBackend::Google,
        LlmBackend::Ollama => LlmCrateBackend::Ollama,
    };
    let mut builder = LLMBuilder::new()
        .backend(backend)
        .model(request.model_id.clone())
        .max_tokens(MAX_OUTPUT_TOKENS)
        .timeout_seconds(cfg.request_timeout.as_secs().max(1))
        .system(request.system.clone());
    if let ProviderAuth::ApiKey(key) = &cfg.auth {
        builder = builder.api_key(key.expose());
    }
    if let Some(endpoint) = cfg.normalized_endpoint() {
        builder = builder.base_url(endpoint);
    }
    builder.build().map_err(|e| map_error(&e))
}

#[async_trait]
impl LlmGateway for ClientGateway {
    async fn complete(
        &self,
        request: &ModelRequest,
        cancel: &CancellationToken,
        sink: StreamSink<'_>,
    ) -> Result<ModelResponse, GatewayError> {
        let cfg = self
            .providers
            .get(request.provider_id.trim())
            .ok_or(GatewayError::NotConfigured)?;
        if request.model_id.trim().is_empty() {
            return Err(GatewayError::NotConfigured);
        }
        let client = self.client(cfg, request)?;
        let messages = to_chat_messages(&request.messages);
        let tools: Vec<Tool> = request
            .tools
            .iter()
            .map(|t| Tool {
                tool_type: "function".to_string(),
                function: FunctionTool {
                    name: t.name.clone(),
                    description: t.description.clone(),
                    parameters: t.input_schema.clone(),
                },
                cache_control: None,
            })
            .collect();

        // Deadline bao trùm cả bước tạo stream lẫn đọc stream: đây là timeout của *một*
        // request LLM, không phải của cả run.
        let work = async {
            let raw: RawStream = client
                .chat_stream_with_tools(&messages, (!tools.is_empty()).then_some(&tools[..]))
                .await
                .map_err(|e| map_error(&e))?;
            collect(raw, sink).await
        };
        tokio::select! {
            () = cancel.cancelled() => Err(GatewayError::Cancelled),
            r = tokio::time::timeout(cfg.request_timeout, work) => {
                r.unwrap_or(Err(GatewayError::Timeout))
            }
        }
    }
}

/// Chuyển transcript nội bộ sang message của `llm`. Các tool result liên tiếp (kết quả của
/// cùng một assistant response) gộp thành **một** message để mọi provider chấp nhận.
/// Nội dung result đi trong `function.arguments` (quy ước của crate `llm`).
fn to_chat_messages(messages: &[ModelMessage]) -> Vec<ChatMessage> {
    let mut out = Vec::with_capacity(messages.len());
    let mut i = 0;
    while i < messages.len() {
        let m = &messages[i];
        match m.role {
            ModelRole::User => out.push(ChatMessage::user().content(m.text.clone()).build()),
            ModelRole::Assistant if m.tool_calls.is_empty() => {
                out.push(ChatMessage::assistant().content(m.text.clone()).build());
            }
            ModelRole::Assistant => {
                let calls = m
                    .tool_calls
                    .iter()
                    .map(|c| ToolCall {
                        id: c.id.clone(),
                        call_type: "function".to_string(),
                        function: FunctionCall {
                            name: c.name.clone(),
                            arguments: c.arguments.to_string(),
                        },
                    })
                    .collect();
                out.push(
                    ChatMessage::assistant()
                        .content(m.text.clone())
                        .tool_use(calls)
                        .build(),
                );
            }
            ModelRole::Tool => {
                let mut results = Vec::new();
                while i < messages.len() && messages[i].role == ModelRole::Tool {
                    let r = &messages[i];
                    results.push(ToolCall {
                        id: r.tool_call_id.clone().unwrap_or_default(),
                        call_type: "function".to_string(),
                        function: FunctionCall {
                            name: r.tool_name.clone().unwrap_or_default(),
                            arguments: r.text.clone(),
                        },
                    });
                    i += 1;
                }
                out.push(ChatMessage::user().tool_result(results).build());
                continue;
            }
        }
        i += 1;
    }
    out
}

/// Gom stream thành một response đã `Done`. Stream cụt (không `Done`) là lỗi protocol.
/// Tool call chỉ được trả khi **toàn** response kết thúc thành công: lỗi/hủy/timeout sau
/// `ToolUseComplete` vẫn loại bỏ mọi call đã nhận (không có gì để dispatch).
async fn collect(mut raw: RawStream, sink: StreamSink<'_>) -> Result<ModelResponse, GatewayError> {
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    loop {
        match raw.next().await {
            None => return Err(GatewayError::Protocol),
            Some(Err(e)) => return Err(map_error(&e)),
            Some(Ok(chunk)) => match chunk {
                StreamChunk::Text(t) => {
                    if !t.is_empty() {
                        sink(StreamEvent::TextDelta(t.clone()));
                    }
                    text.push_str(&t);
                }
                StreamChunk::ToolUseStart { .. } | StreamChunk::ToolUseInputDelta { .. } => {}
                StreamChunk::ToolUseComplete { tool_call, .. } => {
                    let raw_args = tool_call.function.arguments.trim();
                    let arguments = if raw_args.is_empty() {
                        serde_json::json!({})
                    } else {
                        serde_json::from_str(raw_args).map_err(|_| GatewayError::Protocol)?
                    };
                    sink(StreamEvent::ToolCallReady {
                        id: tool_call.id.clone(),
                        name: tool_call.function.name.clone(),
                    });
                    tool_calls.push(ModelToolCall {
                        id: tool_call.id,
                        name: tool_call.function.name,
                        arguments,
                    });
                }
                StreamChunk::Done { stop_reason } => {
                    return Ok(ModelResponse {
                        text,
                        finish_reason: stop_reason,
                        tool_calls,
                        // `llm` 1.3.8 không phát usage trong stream: thiếu usage không phải lỗi.
                        usage: None,
                    });
                }
            },
        }
    }
}

/// Mapping đã sanitize (từ spike G0): phân loại theo biến thể/HTTP status, bỏ raw body.
fn map_error(e: &LLMError) -> GatewayError {
    match e {
        LLMError::AuthError(_) => GatewayError::Auth,
        LLMError::InvalidRequest(m) if looks_like_overflow(m) => GatewayError::ContextOverflow,
        LLMError::InvalidRequest(_) | LLMError::ToolConfigError(_) => GatewayError::InvalidRequest,
        LLMError::ProviderError(_) => GatewayError::Provider,
        LLMError::HttpError(_) | LLMError::RetryExceeded { .. } => GatewayError::Transport,
        LLMError::ResponseFormatError {
            message,
            raw_response,
        } => match http_status(message) {
            Some(400) | Some(413) | Some(422)
                if looks_like_overflow(message) || looks_like_overflow(raw_response) =>
            {
                GatewayError::ContextOverflow
            }
            Some(401) | Some(403) => GatewayError::Auth,
            Some(400) | Some(404) | Some(413) | Some(422) => GatewayError::InvalidRequest,
            Some(429) | Some(500..=599) => GatewayError::Provider,
            _ => GatewayError::Protocol,
        },
        LLMError::JsonError(_) | LLMError::Generic(_) => GatewayError::Protocol,
    }
}

/// Nhận diện lỗi overflow từ mô tả của provider. Chỉ dùng để phân loại; văn bản không bao
/// giờ được echo ra ngoài.
fn looks_like_overflow(text: &str) -> bool {
    let t = text.to_ascii_lowercase();
    [
        "context_length_exceeded",
        "context length",
        "context window",
        "maximum context",
        "too many tokens",
        "prompt is too long",
        "reduce the length",
        "request too large",
    ]
    .iter()
    .any(|needle| t.contains(needle))
}

fn http_status(msg: &str) -> Option<u16> {
    let i = msg.find("status: ")? + 8;
    msg[i..].chars().take(3).collect::<String>().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn collect_q(raw: RawStream) -> Result<ModelResponse, GatewayError> {
        collect(raw, &|_| {}).await
    }

    /// Chạy `collect` và trả kèm preview theo đúng thứ tự nhận được.
    async fn collect_events(
        raw: RawStream,
    ) -> (Result<ModelResponse, GatewayError>, Vec<StreamEvent>) {
        let seen = Mutex::new(Vec::new());
        let sink = |e: StreamEvent| seen.lock().unwrap().push(e);
        let r = collect(raw, &sink).await;
        let events = seen.into_inner().unwrap();
        (r, events)
    }

    fn stream(items: Vec<Result<StreamChunk, LLMError>>) -> RawStream {
        Box::pin(futures::stream::iter(items))
    }

    #[tokio::test]
    async fn collects_text_until_done() {
        let r = collect_q(stream(vec![
            Ok(StreamChunk::Text("he".into())),
            Ok(StreamChunk::Text("llo".into())),
            Ok(StreamChunk::Done {
                stop_reason: "end_turn".into(),
            }),
        ]))
        .await
        .unwrap();
        assert_eq!(r.text, "hello");
        assert_eq!(r.finish_reason, "end_turn");
    }

    #[tokio::test]
    async fn truncated_or_failed_stream_discards_text() {
        let cut = collect_q(stream(vec![Ok(StreamChunk::Text("x".into()))])).await;
        assert_eq!(cut, Err(GatewayError::Protocol));
        let failed = collect_q(stream(vec![
            Ok(StreamChunk::Text("x".into())),
            Err(LLMError::HttpError("boom sk-SECRET".into())),
        ]))
        .await;
        assert_eq!(failed, Err(GatewayError::Transport));
    }

    fn tool_call(id: &str, args: &str) -> ToolCall {
        ToolCall {
            id: id.into(),
            call_type: "function".into(),
            function: FunctionCall {
                name: "t".into(),
                arguments: args.into(),
            },
        }
    }

    #[tokio::test]
    async fn tool_calls_are_returned_only_after_done() {
        let r = collect_q(stream(vec![
            Ok(StreamChunk::ToolUseStart {
                index: 0,
                id: "c1".into(),
                name: "t".into(),
            }),
            Ok(StreamChunk::ToolUseInputDelta {
                index: 0,
                partial_json: "{\"a\":".into(),
            }),
            Ok(StreamChunk::ToolUseInputDelta {
                index: 0,
                partial_json: "1}".into(),
            }),
            Ok(StreamChunk::ToolUseComplete {
                index: 0,
                tool_call: tool_call("c1", "{\"a\":1}"),
            }),
            Ok(StreamChunk::ToolUseComplete {
                index: 1,
                tool_call: tool_call("c2", ""),
            }),
            Ok(StreamChunk::Done {
                stop_reason: "tool_use".into(),
            }),
        ]))
        .await
        .unwrap();
        assert_eq!(r.tool_calls.len(), 2);
        assert_eq!(r.tool_calls[0].id, "c1");
        assert_eq!(r.tool_calls[0].arguments, serde_json::json!({"a": 1}));
        assert_eq!(r.tool_calls[1].arguments, serde_json::json!({}));
    }

    /// AT-35 (mức gateway): lỗi sau `ToolUseComplete` mà chưa `Done` → không trả call nào.
    #[tokio::test]
    async fn error_after_complete_tool_call_yields_no_calls() {
        let failed = collect_q(stream(vec![
            Ok(StreamChunk::ToolUseComplete {
                index: 0,
                tool_call: tool_call("c1", "{}"),
            }),
            Err(LLMError::HttpError("boom".into())),
        ]))
        .await;
        assert_eq!(failed, Err(GatewayError::Transport));
        let cut = collect_q(stream(vec![Ok(StreamChunk::ToolUseComplete {
            index: 0,
            tool_call: tool_call("c1", "{}"),
        })]))
        .await;
        assert_eq!(cut, Err(GatewayError::Protocol));
    }

    #[tokio::test]
    async fn malformed_tool_arguments_are_a_protocol_error() {
        let r = collect_q(stream(vec![
            Ok(StreamChunk::ToolUseComplete {
                index: 0,
                tool_call: tool_call("c1", "{bad"),
            }),
            Ok(StreamChunk::Done {
                stop_reason: "tool_use".into(),
            }),
        ]))
        .await;
        assert_eq!(r, Err(GatewayError::Protocol));
    }

    #[test]
    fn consecutive_tool_results_are_grouped_into_one_message() {
        let call = |id: &str| ModelToolCall {
            id: id.into(),
            name: "t".into(),
            arguments: serde_json::json!({}),
        };
        let msgs = vec![
            ModelMessage::text(ModelRole::User, "go"),
            ModelMessage::assistant_with_calls("", vec![call("a"), call("b")]),
            ModelMessage::tool_result("a", "t", "ra"),
            ModelMessage::tool_result("b", "t", "rb"),
            ModelMessage::text(ModelRole::Assistant, "done"),
        ];
        let out = to_chat_messages(&msgs);
        assert_eq!(out.len(), 4);
        match &out[2].message_type {
            llm::chat::MessageType::ToolResult(rs) => {
                let ids: Vec<&str> = rs.iter().map(|r| r.id.as_str()).collect();
                assert_eq!(ids, ["a", "b"]);
                assert_eq!(rs[1].function.arguments, "rb");
            }
            other => panic!("expected tool result, got {other:?}"),
        }
    }

    #[test]
    fn error_mapping_never_exposes_raw_text() {
        assert_eq!(
            map_error(&LLMError::AuthError("sk-1".into())),
            GatewayError::Auth
        );
        let r = |m: &str| LLMError::ResponseFormatError {
            message: m.into(),
            raw_response: "sk-LEAK".into(),
        };
        assert_eq!(
            map_error(&r("API returned error status: 401 Unauthorized")),
            GatewayError::Auth
        );
        assert_eq!(
            map_error(&r("API returned error status: 429 Too Many")),
            GatewayError::Provider
        );
        for e in [
            GatewayError::Auth,
            GatewayError::Provider,
            GatewayError::Transport,
            GatewayError::Protocol,
        ] {
            assert!(!e.message().contains("sk-"));
        }
    }

    /// AT-26 (mức gateway): 20 fragment được phát đúng thứ tự và gom đúng nội dung.
    #[tokio::test]
    async fn twenty_text_fragments_stream_in_order_and_assemble() {
        let mut items: Vec<Result<StreamChunk, LLMError>> = (0..20)
            .map(|i| Ok(StreamChunk::Text(format!("[{i}]"))))
            .collect();
        items.push(Ok(StreamChunk::Done {
            stop_reason: "end_turn".into(),
        }));
        let (r, events) = collect_events(stream(items)).await;
        let expected: Vec<String> = (0..20).map(|i| format!("[{i}]")).collect();
        let deltas: Vec<String> = events
            .iter()
            .map(|e| match e {
                StreamEvent::TextDelta(t) => t.clone(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(deltas, expected);
        assert_eq!(r.unwrap().text, expected.concat());
    }

    /// AT-35: `ToolCallReady` chỉ là preview; lỗi trước `Done` không trả call nào.
    #[tokio::test]
    async fn tool_call_ready_then_error_is_a_preview_only() {
        let (r, events) = collect_events(stream(vec![
            Ok(StreamChunk::ToolUseComplete {
                index: 0,
                tool_call: tool_call("c1", "{}"),
            }),
            Err(LLMError::HttpError("boom".into())),
        ]))
        .await;
        assert_eq!(r, Err(GatewayError::Transport));
        assert_eq!(
            events,
            vec![StreamEvent::ToolCallReady {
                id: "c1".into(),
                name: "t".into()
            }]
        );
    }

    #[test]
    fn context_overflow_is_classified_without_leaking_text() {
        let overflow = LLMError::ResponseFormatError {
            message: "API returned error status: 400 Bad Request".into(),
            raw_response: "{\"error\":{\"code\":\"context_length_exceeded\",\"key\":\"sk-LEAK\"}}"
                .into(),
        };
        assert_eq!(map_error(&overflow), GatewayError::ContextOverflow);
        assert_eq!(
            map_error(&LLMError::InvalidRequest("prompt is too long".into())),
            GatewayError::ContextOverflow
        );
        let plain = LLMError::ResponseFormatError {
            message: "API returned error status: 400 Bad Request".into(),
            raw_response: "bad".into(),
        };
        assert_eq!(map_error(&plain), GatewayError::InvalidRequest);
        assert_eq!(GatewayError::ContextOverflow.code(), "CONTEXT_LIMIT");
        assert!(!GatewayError::ContextOverflow.message().contains("sk-"));
    }
}
