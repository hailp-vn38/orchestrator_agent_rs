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
    Stalled,
    RequestLimitExceeded,
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
            Self::Stalled => "PROVIDER_STALLED",
            Self::RequestLimitExceeded => "REQUEST_LIMIT_EXCEEDED",
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
            Self::Stalled => "the provider stream stalled without activity",
            Self::RequestLimitExceeded => "the provider request exceeded the maximum total duration",
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

/// Tạo mô tả chẩn đoán an toàn (không chứa raw secret hay endpoint URL) từ `LLMError`.
pub(crate) fn error_detail(e: &LLMError) -> Option<String> {
    match e {
        LLMError::AuthError(_) => Some("upstream rejected authentication credentials".to_owned()),
        LLMError::InvalidRequest(m) if looks_like_overflow(m) => {
            Some("upstream reported prompt/context length exceeded".to_owned())
        }
        LLMError::InvalidRequest(_) => {
            Some("upstream rejected request payload or parameters".to_owned())
        }
        LLMError::ToolConfigError(_) => Some("invalid tool schema configuration".to_owned()),
        LLMError::ProviderError(_) => Some("upstream provider error response".to_owned()),
        LLMError::HttpError(_) => Some("HTTP transport connection or stream read error".to_owned()),
        LLMError::RetryExceeded { attempts, .. } => {
            Some(format!("HTTP retries exhausted after {attempts} attempts"))
        }
        LLMError::ResponseFormatError {
            message,
            raw_response,
        } => match http_status(message) {
            Some(status)
                if looks_like_overflow(message) || looks_like_overflow(raw_response) =>
            {
                Some(format!(
                    "upstream returned HTTP {status} (context length exceeded)"
                ))
            }
            Some(status) => Some(format!("upstream returned HTTP {status}")),
            None => Some("upstream response format mismatch".to_owned()),
        },
        LLMError::JsonError(_) => Some("malformed JSON in upstream response".to_owned()),
        LLMError::Generic(_) => Some("upstream stream/protocol error".to_owned()),
    }
}

/// Làm sạch chuỗi chẩn đoán lỗi: loại bỏ URL (`http://`, `https://`), địa chỉ IP/localhost
/// và các token nhạy cảm (`sk-...`, `Bearer ...`, JWT `eyJ...`).
pub fn sanitize_diagnostic_detail(raw: &str) -> String {
    let mut out = Vec::new();
    let mut redact_next = false;
    for token in raw.split_whitespace() {
        if redact_next {
            out.push("[REDACTED]".to_owned());
            redact_next = false;
            continue;
        }
        let clean_tok = token.trim_matches(|c: char| {
            matches!(
                c,
                '\'' | '"' | '(' | ')' | '[' | ']' | '{' | '}' | ',' | ';'
            )
        });
        let lower = clean_tok.to_ascii_lowercase();
        if lower == "bearer" {
            out.push(token.to_owned());
            redact_next = true;
            continue;
        }
        let is_sensitive = lower.contains("://")
            || lower.contains("127.0.0.1")
            || lower.contains("localhost")
            || lower.contains("0.0.0.0")
            || lower.contains("[::1]")
            || lower.starts_with("sk-")
            || lower.starts_with("pk-")
            || lower.starts_with("rk-")
            || lower.starts_with("ghp_")
            || lower.starts_with("gho_")
            || lower.starts_with("ghu_")
            || lower.starts_with("xoxb-")
            || lower.starts_with("xoxp-")
            || clean_tok.starts_with("eyJ")
            || clean_tok.starts_with("AIza");
        if is_sensitive {
            out.push("[REDACTED]".to_owned());
        } else {
            out.push(token.to_owned());
        }
    }
    let joined = out.join(" ");
    if joined.chars().count() > 300 {
        let truncated: String = joined.chars().take(300).collect();
        format!("{truncated}...")
    } else {
        joined
    }
}
