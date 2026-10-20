use llm::error::LLMError;

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

/// Mapping đã sanitize (từ spike G0): phân loại theo biến thể/HTTP status, bỏ raw body.
pub(crate) fn map_error(e: &LLMError) -> GatewayError {
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
