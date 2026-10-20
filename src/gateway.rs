//! `LlmGateway`: internal seam giữa runtime và provider protocol (docs/08, docs/18 §18.5).
//!
//! Production dùng [`ClientGateway`] trên crate `llm` (pin `=1.3.8`, evidence G0). Type của
//! `llm` không bao giờ ra public API. M1 chỉ cần text-only; tool/stream ordering đến ở M2/M4.
//!
//! Item ở đây là `pub` trong module private; chỉ re-export qua `test_support` khi bật
//! feature `test-support`, để production host không thể inject/lắp ráp gateway.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use llm::builder::{LLMBackend as LlmCrateBackend, LLMBuilder};
use llm::chat::{ChatMessage, StreamChunk};
use llm::error::LLMError;
use llm::LLMProvider;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::config::{LlmBackend, LlmProviderConfig, ProviderAuth};

/// Trần output token mỗi request khi config provider chưa có tùy chọn riêng (M1).
const MAX_OUTPUT_TOKENS: u32 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelRole {
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelMessage {
    pub role: ModelRole,
    pub text: String,
}

/// Request đã resolve theo agent: `system` = mandatory base + prompt của agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRequest {
    pub provider_id: String,
    pub model_id: String,
    pub system: String,
    pub messages: Vec<ModelMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelResponse {
    pub text: String,
    pub finish_reason: String,
}

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
        }
    }
}

#[async_trait]
pub trait LlmGateway: Send + Sync {
    /// Một lượt model hoàn chỉnh. Chỉ trả `Ok` khi toàn response đã `Done`; mọi
    /// lỗi/hủy/timeout giữa chừng loại bỏ phần text đã nhận.
    async fn complete(
        &self,
        request: &ModelRequest,
        cancel: &CancellationToken,
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
    ) -> Result<ModelResponse, GatewayError> {
        let cfg = self
            .providers
            .get(request.provider_id.trim())
            .ok_or(GatewayError::NotConfigured)?;
        if request.model_id.trim().is_empty() {
            return Err(GatewayError::NotConfigured);
        }
        let client = self.client(cfg, request)?;
        let messages: Vec<ChatMessage> = request
            .messages
            .iter()
            .map(|m| match m.role {
                ModelRole::User => ChatMessage::user().content(m.text.clone()).build(),
                ModelRole::Assistant => ChatMessage::assistant().content(m.text.clone()).build(),
            })
            .collect();

        // Deadline bao trùm cả bước tạo stream lẫn đọc stream: đây là timeout của *một*
        // request LLM, không phải của cả run.
        let work = async {
            let raw: RawStream = client
                .chat_stream_with_tools(&messages, None)
                .await
                .map_err(|e| map_error(&e))?;
            collect(raw).await
        };
        tokio::select! {
            () = cancel.cancelled() => Err(GatewayError::Cancelled),
            r = tokio::time::timeout(cfg.request_timeout, work) => {
                r.unwrap_or(Err(GatewayError::Timeout))
            }
        }
    }
}

/// Gom stream thành một response đã `Done`. Stream cụt (không `Done`) là lỗi protocol;
/// tool-use khi không cung cấp tool cũng là lỗi protocol ở M1.
async fn collect(mut raw: RawStream) -> Result<ModelResponse, GatewayError> {
    let mut text = String::new();
    loop {
        match raw.next().await {
            None => return Err(GatewayError::Protocol),
            Some(Err(e)) => return Err(map_error(&e)),
            Some(Ok(chunk)) => match chunk {
                StreamChunk::Text(t) => text.push_str(&t),
                StreamChunk::ToolUseStart { .. }
                | StreamChunk::ToolUseInputDelta { .. }
                | StreamChunk::ToolUseComplete { .. } => return Err(GatewayError::Protocol),
                StreamChunk::Done { stop_reason } => {
                    return Ok(ModelResponse {
                        text,
                        finish_reason: stop_reason,
                    })
                }
            },
        }
    }
}

/// Mapping đã sanitize (từ spike G0): phân loại theo biến thể/HTTP status, bỏ raw body.
fn map_error(e: &LLMError) -> GatewayError {
    match e {
        LLMError::AuthError(_) => GatewayError::Auth,
        LLMError::InvalidRequest(_) | LLMError::ToolConfigError(_) => GatewayError::InvalidRequest,
        LLMError::ProviderError(_) => GatewayError::Provider,
        LLMError::HttpError(_) | LLMError::RetryExceeded { .. } => GatewayError::Transport,
        LLMError::ResponseFormatError { message, .. } => match http_status(message) {
            Some(401) | Some(403) => GatewayError::Auth,
            Some(400) | Some(404) | Some(413) | Some(422) => GatewayError::InvalidRequest,
            Some(429) | Some(500..=599) => GatewayError::Provider,
            _ => GatewayError::Protocol,
        },
        LLMError::JsonError(_) | LLMError::Generic(_) => GatewayError::Protocol,
    }
}

fn http_status(msg: &str) -> Option<u16> {
    let i = msg.find("status: ")? + 8;
    msg[i..].chars().take(3).collect::<String>().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(items: Vec<Result<StreamChunk, LLMError>>) -> RawStream {
        Box::pin(futures::stream::iter(items))
    }

    #[tokio::test]
    async fn collects_text_until_done() {
        let r = collect(stream(vec![
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
        let cut = collect(stream(vec![Ok(StreamChunk::Text("x".into()))])).await;
        assert_eq!(cut, Err(GatewayError::Protocol));
        let failed = collect(stream(vec![
            Ok(StreamChunk::Text("x".into())),
            Err(LLMError::HttpError("boom sk-SECRET".into())),
        ]))
        .await;
        assert_eq!(failed, Err(GatewayError::Transport));
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
}
