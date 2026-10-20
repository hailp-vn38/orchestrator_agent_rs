//! Typed public config (docs/18 §18.5.2). Host truyền config đã resolve secret;
//! library validate cục bộ, không gọi remote khi build.

use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::agent::AgentDefinition;
use crate::error::{ConfigReason, OrchestratorError};
use crate::secret::SecretString;

#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    pub llm: LlmConfig,
    pub agents: Vec<AgentDefinition>,
}

#[derive(Debug, Clone, Default)]
pub struct LlmConfig {
    pub providers: Vec<LlmProviderConfig>,
}

#[derive(Debug, Clone)]
pub struct LlmProviderConfig {
    /// Alias cấu hình cục bộ, duy nhất; không phải tên backend.
    pub id: String,
    pub backend: LlmBackend,
    /// Bắt buộc với `Ollama` và `OpenAiCompatible`; tùy chọn với backend chuẩn.
    pub endpoint: Option<String>,
    pub auth: ProviderAuth,
    /// Timeout cho *một* request LLM, không phải deadline cả session.
    pub request_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LlmBackend {
    OpenAi,
    Anthropic,
    Gemini,
    Ollama,
    OpenAiCompatible,
}

impl LlmBackend {
    fn kind(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
            Self::Gemini => "gemini",
            Self::Ollama => "ollama",
            Self::OpenAiCompatible => "openai_compatible",
        }
    }

    fn endpoint_required(self) -> bool {
        matches!(self, Self::Ollama | Self::OpenAiCompatible)
    }

    /// Backend cloud chuẩn bắt buộc có API key; local/compatible có thể không auth.
    fn api_key_required(self) -> bool {
        matches!(self, Self::OpenAi | Self::Anthropic | Self::Gemini)
    }
}

#[derive(Debug, Clone)]
pub enum ProviderAuth {
    None,
    ApiKey(SecretString),
}

/// Version của canonical encoding; đổi encoding phải tăng số này.
const FINGERPRINT_VERSION: &str = "v1";

impl LlmProviderConfig {
    pub(crate) fn validate(&self) -> Result<(), OrchestratorError> {
        let id = self.id.trim();
        if id.is_empty() {
            return Err(OrchestratorError::config(
                ConfigReason::EmptyProviderId,
                "provider id must not be empty",
            ));
        }
        if self.request_timeout.is_zero() {
            return Err(OrchestratorError::config(
                ConfigReason::InvalidTimeout,
                format!("provider '{id}': request_timeout must be > 0"),
            ));
        }
        match (&self.endpoint, self.backend.endpoint_required()) {
            (None, true) => {
                return Err(OrchestratorError::config(
                    ConfigReason::EndpointRequired,
                    format!("provider '{id}': endpoint is required for this backend"),
                ))
            }
            (Some(ep), _) => {
                normalize_endpoint(ep).ok_or_else(|| {
                    // Không echo endpoint: có thể chứa thông tin nhạy cảm.
                    OrchestratorError::config(
                        ConfigReason::InvalidEndpoint,
                        format!(
                            "provider '{id}': endpoint is not a valid credential-free http(s) URL"
                        ),
                    )
                })?;
            }
            (None, false) => {}
        }
        let key_ok = match &self.auth {
            ProviderAuth::ApiKey(k) => !k.is_empty(),
            ProviderAuth::None => false,
        };
        if self.backend.api_key_required() && !key_ok {
            return Err(OrchestratorError::config(
                ConfigReason::MissingApiKey,
                format!("provider '{id}': an API key is required for this backend"),
            ));
        }
        if let ProviderAuth::ApiKey(k) = &self.auth {
            if k.is_empty() {
                return Err(OrchestratorError::config(
                    ConfigReason::MissingApiKey,
                    format!("provider '{id}': API key must not be empty"),
                ));
            }
        }
        Ok(())
    }

    /// SHA-256 có version của các trường non-secret làm đổi routing (EMB-16):
    /// backend kind + endpoint chuẩn hóa. Không gồm auth hay timeout.
    pub fn routing_fingerprint(&self) -> String {
        let endpoint = self
            .endpoint
            .as_deref()
            .and_then(normalize_endpoint)
            .unwrap_or_else(|| "-".to_string());
        let canonical = format!(
            "orchestrator-provider-fingerprint/{FINGERPRINT_VERSION}\nbackend={}\nendpoint={endpoint}\n",
            self.backend.kind()
        );
        let digest = Sha256::digest(canonical.as_bytes());
        let mut out = String::with_capacity(FINGERPRINT_VERSION.len() + 1 + 64);
        out.push_str(FINGERPRINT_VERSION);
        out.push(':');
        for b in digest {
            out.push_str(&format!("{b:02x}"));
        }
        out
    }
}

impl LlmConfig {
    pub(crate) fn validate(&self) -> Result<(), OrchestratorError> {
        if self.providers.is_empty() {
            return Err(OrchestratorError::config(
                ConfigReason::NoProviders,
                "at least one LLM provider is required",
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for p in &self.providers {
            p.validate()?;
            if !seen.insert(p.id.trim().to_string()) {
                return Err(OrchestratorError::config(
                    ConfigReason::DuplicateProviderId,
                    format!("duplicate provider id '{}'", p.id.trim()),
                ));
            }
        }
        Ok(())
    }
}

/// Chuẩn hóa `http(s)://host[:port][/path]`; trả `None` nếu sai cú pháp hoặc có
/// userinfo/query/fragment/khoảng trắng (có thể mang credentials).
fn normalize_endpoint(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() || raw.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return None;
    }
    let (scheme, rest) = raw.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return None;
    }
    if rest.contains(['?', '#']) {
        return None;
    }
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    if authority.is_empty() || authority.contains('@') {
        return None;
    }
    let host = authority.rsplit_once(':').map_or(authority, |(h, port)| {
        if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) {
            h
        } else {
            authority
        }
    });
    if host.is_empty() || host.contains(':') && !host.starts_with('[') {
        return None;
    }
    let path = path.trim_end_matches('/');
    Some(format!(
        "{scheme}://{}{path}",
        authority.to_ascii_lowercase()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_normalization() {
        assert_eq!(
            normalize_endpoint("HTTP://Host:8080/v1/").as_deref(),
            Some("http://host:8080/v1")
        );
        assert_eq!(
            normalize_endpoint("https://api.example.com").as_deref(),
            Some("https://api.example.com")
        );
        for bad in [
            "",
            "ftp://x",
            "localhost:1234",
            "http://user:pw@host",
            "http://host/v1?key=abc",
            "http://host#frag",
            "http://",
            "http://ho st",
            "http://host:12x",
        ] {
            assert_eq!(normalize_endpoint(bad), None, "{bad}");
        }
    }
}
