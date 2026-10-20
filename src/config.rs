//! Typed public config (docs/18 §18.5.2). Host truyền config đã resolve secret;
//! library validate cục bộ, không gọi remote khi build.

use std::path::PathBuf;
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::agent::AgentDefinition;
use crate::error::{ConfigReason, OrchestratorError};
use crate::secret::SecretString;

#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    pub llm: LlmConfig,
    pub agents: Vec<AgentDefinition>,
    /// Thư mục execution store do library sở hữu (SQLite + owner lock). Mặc định nên là
    /// DB riêng, không dùng database nghiệp vụ của host (EMB-06).
    pub store_dir: PathBuf,
    pub runtime: RuntimeConfig,
    /// Thư mục `skills/` của project (`skills/<id>/SKILL.md`). `None` = không có skill.
    /// Scan một lần lúc build; thay đổi cần restart (không hot reload).
    pub skills_dir: Option<PathBuf>,
    /// File project instructions (vd. `AGENTS.md`), snapshot lúc build. Là untrusted
    /// context: không thay thế base hay quyền thực thi.
    pub project_instructions: Option<PathBuf>,
}

impl OrchestratorConfig {
    pub fn new(
        llm: LlmConfig,
        agents: Vec<AgentDefinition>,
        store_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            llm,
            agents,
            store_dir: store_dir.into(),
            runtime: RuntimeConfig::default(),
            skills_dir: None,
            project_instructions: None,
        }
    }

    pub fn with_skills_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.skills_dir = Some(dir.into());
        self
    }

    pub fn with_project_instructions(mut self, path: impl Into<PathBuf>) -> Self {
        self.project_instructions = Some(path.into());
        self
    }

    pub(crate) fn validate_local(&self) -> Result<(), OrchestratorError> {
        if self.store_dir.as_os_str().is_empty() {
            return Err(OrchestratorError::config(
                ConfigReason::InvalidStoreDir,
                "store_dir must not be empty",
            ));
        }
        self.runtime.validate()
    }
}

/// Giới hạn vận hành của managed execution (đơn vị trong tên field).
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Chu kỳ sweep của supervisor: reconcile durable claim chưa có owner trong process.
    pub supervisor_sweep: Duration,
    /// Trần số item mỗi trang history; `limit` của host được clamp vào `1..=page_size`.
    pub page_size: u32,
    /// Thời gian chờ các local task quiesce/join **sau khi** đã yêu cầu cancel hợp tác trong
    /// `shutdown()`. Hết hạn mà còn task có thể ghi: `shutdown` trả lỗi và giữ owner lock.
    pub shutdown_grace: Duration,
    /// Thời gian drain trước khi cancel: admission mới đã bị từ chối, run đang chạy được tự
    /// kết thúc. `0` = cancel hợp tác ngay (mặc định).
    pub shutdown_drain: Duration,
    /// Số lần recovery tối đa mỗi turn (`resume_attempt` lớn nhất được tạo tự động). Vượt thì
    /// attempt cuối giữ `interrupted` với `RECOVERY_ATTEMPTS_EXHAUSTED` (cần người xử lý).
    pub max_resume_attempts: u32,
    /// Timeout của *một* thao tác tool (không phải của cả run). Quá hạn: tool ReadOnly trả
    /// lỗi kỹ thuật `TOOL_TIMEOUT`; tool Mutating có outcome unknown → run `interrupted`.
    pub tool_timeout: Duration,
    /// Trần byte model-visible của một tool result; vượt thì cắt kèm marker rõ ràng.
    pub tool_output_cap_bytes: usize,
    /// Trần byte được persist trong `result_json`; vượt thì outcome bị từ chối bằng
    /// `TOOL_OUTPUT_TOO_LARGE` (không persist phần dư).
    pub tool_result_hard_cap_bytes: usize,
    /// Trần byte của một `SKILL.md`; vượt thì `skill(id)` trả lỗi rõ, không cắt âm thầm.
    pub skill_max_bytes: usize,
    /// Context window (token, **ước lượng** ~4 byte/token) dùng để quyết định compaction.
    pub context_limit_tokens: usize,
    /// Phần trăm `context_limit_tokens` mà request ước lượng chạm tới thì auto compaction.
    pub compaction_threshold_percent: u8,
    /// Số exchange settled gần nhất luôn giữ nguyên văn (không đưa vào summary).
    pub compaction_keep_recent: usize,
    /// Sức chứa của broadcast `ExecutionFeed` (số event). Subscriber chậm hơn mức này nhận
    /// `ResyncRequired`; publisher không bao giờ bị chặn.
    pub event_capacity: usize,
    /// Trần byte của một `text.delta`; fragment lớn hơn được cắt thành nhiều delta liên tiếp
    /// (không mất text, giữ thứ tự).
    pub event_delta_max_bytes: usize,
    /// Khoảng tối thiểu giữa hai `tool.progress` của cùng một invocation; update tới sớm hơn bị
    /// bỏ (ephemeral, best-effort).
    pub progress_min_interval: Duration,
    /// Chu kỳ host refresh snapshot khi family còn active, kể cả khi stream còn sống (bắt
    /// terminal commit bị mất publication). Engine không tự refresh: đây là giá trị host đọc
    /// qua `Orchestrator::snapshot_refresh_interval`.
    pub snapshot_refresh: Duration,
}

impl Default for RuntimeConfig {
    /// Giá trị theo profile G0 (estimated, chưa phải số đo).
    fn default() -> Self {
        Self {
            supervisor_sweep: Duration::from_secs(30),
            page_size: 50,
            shutdown_grace: Duration::from_secs(30),
            shutdown_drain: Duration::ZERO,
            max_resume_attempts: 10,
            tool_timeout: Duration::from_secs(300),
            tool_output_cap_bytes: 64 * 1024,
            tool_result_hard_cap_bytes: 8 * 1024 * 1024,
            skill_max_bytes: 64 * 1024,
            context_limit_tokens: 100_000,
            compaction_threshold_percent: 80,
            compaction_keep_recent: 2,
            event_capacity: 4096,
            event_delta_max_bytes: 4 * 1024,
            progress_min_interval: Duration::from_millis(200),
            snapshot_refresh: Duration::from_secs(30),
        }
    }
}

impl RuntimeConfig {
    pub(crate) fn validate(&self) -> Result<(), OrchestratorError> {
        let bad = |m: &str| {
            Err(OrchestratorError::config(
                ConfigReason::InvalidRuntimeConfig,
                m,
            ))
        };
        if self.supervisor_sweep.is_zero() {
            return bad("supervisor_sweep must be > 0");
        }
        if self.shutdown_grace.is_zero() {
            return bad("shutdown_grace must be > 0");
        }
        if !(1..=100).contains(&self.max_resume_attempts) {
            return bad("max_resume_attempts must be in 1..=100");
        }
        if self.tool_timeout.is_zero() {
            return bad("tool_timeout must be > 0");
        }
        if self.tool_output_cap_bytes == 0
            || self.tool_output_cap_bytes > self.tool_result_hard_cap_bytes
        {
            return bad("tool_output_cap_bytes must be in 1..=tool_result_hard_cap_bytes");
        }
        if self.skill_max_bytes == 0 {
            return bad("skill_max_bytes must be > 0");
        }
        if self.context_limit_tokens == 0 {
            return bad("context_limit_tokens must be > 0");
        }
        if !(1..=99).contains(&self.compaction_threshold_percent) {
            return bad("compaction_threshold_percent must be in 1..=99");
        }
        if self.compaction_keep_recent == 0 {
            return bad("compaction_keep_recent must be >= 1");
        }
        if !(1..=500).contains(&self.page_size) {
            return bad("page_size must be in 1..=500");
        }
        if !(2..=65_536).contains(&self.event_capacity) {
            return bad("event_capacity must be in 2..=65536");
        }
        if !(64..=64 * 1024).contains(&self.event_delta_max_bytes) {
            return bad("event_delta_max_bytes must be in 64..=65536");
        }
        if self.snapshot_refresh.is_zero() {
            return bad("snapshot_refresh must be > 0");
        }
        Ok(())
    }
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
    pub(crate) fn kind(self) -> &'static str {
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

    /// Endpoint đã chuẩn hóa (credential-free http(s) URL), `None` nếu không đặt.
    pub(crate) fn normalized_endpoint(&self) -> Option<String> {
        self.endpoint.as_deref().and_then(normalize_endpoint)
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
