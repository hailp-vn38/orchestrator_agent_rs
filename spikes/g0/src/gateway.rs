//! Gateway normalization spike over `llm` 1.3.8.
//!
//! Concrete stream type used by the crate (verified by compile):
//! `Pin<Box<dyn Stream<Item = Result<llm::chat::StreamChunk, LLMError>> + Send>>`.

use std::pin::Pin;
use std::time::Duration;

use futures::{Stream, StreamExt};
use llm::builder::{LLMBackend, LLMBuilder};
use llm::chat::{ChatMessage, StreamChunk, Tool};
use llm::error::LLMError;
use llm::{LLMProvider, ToolCall};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    OpenAi,
    Anthropic,
}

/// Typed, secret-free config. The API key is passed separately at build time.
#[derive(Debug, Clone)]
pub struct ProviderConfig {
    pub provider_id: String,
    pub backend: Backend,
    pub model_id: String,
    pub base_url: Option<String>,
    pub request_timeout: Duration,
    pub max_tokens: u32,
}

impl ProviderConfig {
    /// Non-secret routing fingerprint: backend + endpoint host/path + model. Never the key.
    pub fn fingerprint(&self) -> String {
        let mut h = Sha256::new();
        h.update(format!(
            "{:?}|{}|{}",
            self.backend,
            self.base_url.as_deref().unwrap_or("default"),
            self.model_id
        ));
        hex8(&h.finalize())
    }

    /// Endpoint identity safe to log: scheme://host only.
    pub fn endpoint_identity(&self) -> String {
        match &self.base_url {
            None => "default".into(),
            Some(u) => {
                let mut it = u.splitn(4, '/');
                let (s, _, host) = (it.next().unwrap_or(""), it.next(), it.next().unwrap_or(""));
                format!("{s}//{host}")
            }
        }
    }
}

fn hex8(b: &[u8]) -> String {
    b.iter().take(8).map(|x| format!("{x:02x}")).collect()
}

pub fn build_client(
    cfg: &ProviderConfig,
    api_key: &str,
    system: Option<&str>,
) -> Result<Box<dyn LLMProvider>, LLMError> {
    let backend = match cfg.backend {
        Backend::OpenAi => LLMBackend::OpenAI,
        Backend::Anthropic => LLMBackend::Anthropic,
    };
    let mut b = LLMBuilder::new()
        .backend(backend)
        .api_key(api_key)
        .model(cfg.model_id.clone())
        .max_tokens(cfg.max_tokens)
        .timeout_seconds(cfg.request_timeout.as_secs().max(1));
    if let Some(u) = &cfg.base_url {
        b = b.base_url(u.clone());
    }
    if let Some(sys) = system {
        b = b.system(sys);
    }
    b.build()
}

/// Gateway-level event (design 08.3). Usage is `None`: `StreamChunk::Done` carries no usage.
#[derive(Debug, Clone, PartialEq)]
pub enum ModelEvent {
    TextDelta { ordinal: u64, text: String },
    ToolCallReady(ToolCall),
    Finished { finish_reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayError {
    Auth,
    InvalidRequest,
    Provider,
    Transport,
    Timeout,
    Cancelled,
    Protocol,
}

impl GatewayError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Auth => "llm_auth",
            Self::InvalidRequest => "llm_invalid_request",
            Self::Provider => "llm_provider",
            Self::Transport => "llm_transport",
            Self::Timeout => "llm_timeout",
            Self::Cancelled => "llm_cancelled",
            Self::Protocol => "llm_protocol",
        }
    }
}

/// Sanitized mapping: raw provider text is dropped (may contain echoed secrets).
pub fn map_error(e: &LLMError) -> GatewayError {
    match e {
        LLMError::AuthError(_) => GatewayError::Auth,
        LLMError::InvalidRequest(_) | LLMError::ToolConfigError(_) => GatewayError::InvalidRequest,
        LLMError::ProviderError(_) => GatewayError::Provider,
        LLMError::HttpError(_) | LLMError::RetryExceeded { .. } => GatewayError::Transport,
        // llm 1.3.8 reports non-2xx Anthropic responses as ResponseFormatError with the status
        // embedded in the message; classify by status, never forward the raw body.
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

pub type RawStream = Pin<Box<dyn Stream<Item = Result<StreamChunk, LLMError>> + Send>>;

/// A fully collected, successfully finished provider response.
#[derive(Debug, Default, PartialEq)]
pub struct CollectedResponse {
    pub text: String,
    pub deltas: usize,
    pub tool_calls: Vec<ToolCall>,
    pub fragments_seen: usize,
    pub finish_reason: String,
    pub events: Vec<ModelEvent>,
}

/// Normalizes and drains a raw stream. Tool calls are only returned when the whole
/// response ended with `Done` (design 08.1/08.3). Any error/cancel/timeout discards
/// unconfirmed calls: the `Err` carries no calls, so nothing can be dispatched.
pub async fn collect(
    mut raw: RawStream,
    cancel: &CancellationToken,
    timeout: Duration,
) -> Result<CollectedResponse, GatewayError> {
    let mut out = CollectedResponse::default();
    let mut ordinal = 0u64;
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    loop {
        let next = tokio::select! {
            _ = cancel.cancelled() => return Err(GatewayError::Cancelled),
            _ = &mut deadline => return Err(GatewayError::Timeout),
            n = raw.next() => n,
        };
        match next {
            None => return Err(GatewayError::Protocol), // ended without Done
            Some(Err(e)) => return Err(map_error(&e)),
            Some(Ok(chunk)) => match chunk {
                StreamChunk::Text(t) => {
                    out.text.push_str(&t);
                    out.deltas += 1;
                    out.events.push(ModelEvent::TextDelta { ordinal, text: t });
                    ordinal += 1;
                }
                StreamChunk::ToolUseStart { .. } => {}
                StreamChunk::ToolUseInputDelta { .. } => out.fragments_seen += 1,
                StreamChunk::ToolUseComplete { tool_call, .. } => {
                    if serde_json::from_str::<serde_json::Value>(&tool_call.function.arguments)
                        .is_err()
                    {
                        return Err(GatewayError::Protocol);
                    }
                    out.events
                        .push(ModelEvent::ToolCallReady(tool_call.clone()));
                    out.tool_calls.push(tool_call);
                }
                StreamChunk::Done { stop_reason } => {
                    out.finish_reason = stop_reason.clone();
                    out.events.push(ModelEvent::Finished {
                        finish_reason: stop_reason,
                    });
                    return Ok(out);
                }
            },
        }
    }
}

pub async fn stream_with_tools(
    client: &dyn LLMProvider,
    messages: &[ChatMessage],
    tools: Option<&[Tool]>,
) -> Result<RawStream, GatewayError> {
    client
        .chat_stream_with_tools(messages, tools)
        .await
        .map_err(|e| map_error(&e))
}

/// Stream fault harness (labelled injection, NOT live-service evidence).
pub fn fake_stream(items: Vec<Result<StreamChunk, LLMError>>) -> RawStream {
    Box::pin(futures::stream::iter(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use llm::FunctionCall;

    fn call(id: &str, args: &str) -> ToolCall {
        ToolCall {
            id: id.into(),
            call_type: "function".into(),
            function: FunctionCall {
                name: "t".into(),
                arguments: args.into(),
            },
        }
    }
    const T: Duration = Duration::from_secs(2);

    #[tokio::test]
    async fn happy_path_orders_events_and_finishes() {
        let s = fake_stream(vec![
            Ok(StreamChunk::Text("he".into())),
            Ok(StreamChunk::Text("llo".into())),
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
                tool_call: call("c1", "{\"a\":1}"),
            }),
            Ok(StreamChunk::Done {
                stop_reason: "tool_use".into(),
            }),
        ]);
        let r = collect(s, &CancellationToken::new(), T).await.unwrap();
        assert_eq!(r.text, "hello");
        assert_eq!(r.fragments_seen, 2);
        assert_eq!(r.tool_calls[0].id, "c1");
        assert!(matches!(r.events.last(), Some(ModelEvent::Finished { .. })));
    }

    #[tokio::test]
    async fn error_after_tool_call_ready_yields_no_calls() {
        let s = fake_stream(vec![
            Ok(StreamChunk::ToolUseComplete {
                index: 0,
                tool_call: call("c1", "{}"),
            }),
            Err(LLMError::HttpError("boom sk-SECRET".into())),
        ]);
        let r = collect(s, &CancellationToken::new(), T).await;
        assert_eq!(r, Err(GatewayError::Transport)); // no calls reachable, error is secret-free
    }

    #[tokio::test]
    async fn truncated_stream_without_done_is_error() {
        let s = fake_stream(vec![Ok(StreamChunk::Text("x".into()))]);
        assert_eq!(
            collect(s, &CancellationToken::new(), T).await,
            Err(GatewayError::Protocol)
        );
    }

    #[tokio::test]
    async fn invalid_tool_arguments_rejected() {
        let s = fake_stream(vec![
            Ok(StreamChunk::ToolUseComplete {
                index: 0,
                tool_call: call("c1", "{bad"),
            }),
            Ok(StreamChunk::Done {
                stop_reason: "tool_use".into(),
            }),
        ]);
        assert_eq!(
            collect(s, &CancellationToken::new(), T).await,
            Err(GatewayError::Protocol)
        );
    }

    #[tokio::test]
    async fn cancel_and_timeout() {
        let tok = CancellationToken::new();
        tok.cancel();
        let pending: RawStream = Box::pin(futures::stream::pending());
        assert_eq!(
            collect(pending, &tok, T).await,
            Err(GatewayError::Cancelled)
        );
        let pending: RawStream = Box::pin(futures::stream::pending());
        assert_eq!(
            collect(
                pending,
                &CancellationToken::new(),
                Duration::from_millis(50)
            )
            .await,
            Err(GatewayError::Timeout)
        );
    }

    #[test]
    fn error_mapping_and_fingerprint_hide_secrets() {
        assert_eq!(
            map_error(&LLMError::AuthError("sk-1".into())),
            GatewayError::Auth
        );
        let r = |m: &str| LLMError::ResponseFormatError {
            message: m.into(),
            raw_response: "sk-LEAK".into(),
        };
        assert_eq!(
            map_error(&r("Anthropic API returned error status: 401 Unauthorized")),
            GatewayError::Auth
        );
        assert_eq!(
            map_error(&r("Anthropic API returned error status: 429 Too Many")),
            GatewayError::Provider
        );
        assert_eq!(
            map_error(&r("Anthropic API returned error status: 400 Bad")),
            GatewayError::InvalidRequest
        );
        let c = ProviderConfig {
            provider_id: "p".into(),
            backend: Backend::OpenAi,
            model_id: "m".into(),
            base_url: Some("https://host.example/v1/".into()),
            request_timeout: T,
            max_tokens: 16,
        };
        assert_eq!(c.endpoint_identity(), "https://host.example");
        assert_eq!(c.fingerprint().len(), 16);
    }
}
