//! Stable, sanitized public error contract (docs/18 §18.4.2).

use std::fmt;

/// Stable error codes. Chuỗi trả về bởi [`ErrorCode::as_str`] là hợp đồng công khai.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCode {
    InvalidInput,
    PermissionDenied,
    NotFound,
    SessionBusy,
    SessionArchived,
    RecoveryBlocked,
    ProviderConfigChanged,
    StoreAlreadyOwned,
    ShuttingDown,
    Unavailable,
    Internal,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "INVALID_INPUT",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::NotFound => "NOT_FOUND",
            Self::SessionBusy => "SESSION_BUSY",
            Self::SessionArchived => "SESSION_ARCHIVED",
            Self::RecoveryBlocked => "RECOVERY_BLOCKED",
            Self::ProviderConfigChanged => "PROVIDER_CONFIG_CHANGED",
            Self::StoreAlreadyOwned => "STORE_ALREADY_OWNED",
            Self::ShuttingDown => "SHUTTING_DOWN",
            Self::Unavailable => "UNAVAILABLE",
            Self::Internal => "INTERNAL",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Lý do cấu hình bị từ chối. Mỗi biến thể có mã ổn định qua [`ConfigReason::as_str`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ConfigReason {
    NoProviders,
    NoAgents,
    EmptyProviderId,
    DuplicateProviderId,
    InvalidEndpoint,
    EndpointRequired,
    InvalidTimeout,
    MissingApiKey,
    EmptyAgentId,
    DuplicateAgentId,
    UnknownProvider,
    EmptyModelId,
    InvalidSteps,
    PromptPathInvalid,
    UnknownTool,
    InvalidToolName,
    ReservedToolName,
    DuplicateTool,
    InvalidToolDescriptor,
    InvalidToolSchema,
    BaseInstructionsInvalid,
    InvalidStoreDir,
    InvalidRuntimeConfig,
    InvalidSkillsDir,
    InvalidSkill,
    DuplicateSkill,
    UnknownSkill,
    ProjectInstructionsInvalid,
}

impl ConfigReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoProviders => "NO_PROVIDERS",
            Self::NoAgents => "NO_AGENTS",
            Self::EmptyProviderId => "EMPTY_PROVIDER_ID",
            Self::DuplicateProviderId => "DUPLICATE_PROVIDER_ID",
            Self::InvalidEndpoint => "INVALID_ENDPOINT",
            Self::EndpointRequired => "ENDPOINT_REQUIRED",
            Self::InvalidTimeout => "INVALID_TIMEOUT",
            Self::MissingApiKey => "MISSING_API_KEY",
            Self::EmptyAgentId => "EMPTY_AGENT_ID",
            Self::DuplicateAgentId => "DUPLICATE_AGENT_ID",
            Self::UnknownProvider => "UNKNOWN_PROVIDER",
            Self::EmptyModelId => "EMPTY_MODEL_ID",
            Self::InvalidSteps => "INVALID_STEPS",
            Self::PromptPathInvalid => "PROMPT_PATH_INVALID",
            Self::UnknownTool => "UNKNOWN_TOOL",
            Self::InvalidToolName => "INVALID_TOOL_NAME",
            Self::ReservedToolName => "RESERVED_TOOL_NAME",
            Self::DuplicateTool => "DUPLICATE_TOOL",
            Self::InvalidToolDescriptor => "INVALID_TOOL_DESCRIPTOR",
            Self::InvalidToolSchema => "INVALID_TOOL_SCHEMA",
            Self::BaseInstructionsInvalid => "BASE_INSTRUCTIONS_INVALID",
            Self::InvalidStoreDir => "INVALID_STORE_DIR",
            Self::InvalidRuntimeConfig => "INVALID_RUNTIME_CONFIG",
            Self::InvalidSkillsDir => "INVALID_SKILLS_DIR",
            Self::InvalidSkill => "INVALID_SKILL",
            Self::DuplicateSkill => "DUPLICATE_SKILL",
            Self::UnknownSkill => "UNKNOWN_SKILL",
            Self::ProjectInstructionsInvalid => "PROJECT_INSTRUCTIONS_INVALID",
        }
    }
}

/// Lỗi công khai của library. `message` luôn đã được sanitize: không bao giờ chứa
/// secret, endpoint thô hay nội dung phản hồi provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorError {
    code: ErrorCode,
    reason: Option<ConfigReason>,
    message: String,
}

impl OrchestratorError {
    /// Lỗi runtime không thuộc validate cấu hình. `message` phải đã sanitize.
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            reason: None,
            message: message.into(),
        }
    }

    /// Lỗi lưu trữ: chi tiết chỉ ghi tracing, không trả về host.
    pub(crate) fn storage(context: &'static str, err: &dyn std::fmt::Display) -> Self {
        tracing::warn!(context, error = %err, "execution store error");
        Self::new(ErrorCode::Unavailable, "execution store operation failed")
    }

    pub(crate) fn config(reason: ConfigReason, message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::InvalidInput,
            reason: Some(reason),
            message: message.into(),
        }
    }

    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// Mã chi tiết khi lỗi đến từ validate cấu hình lúc build.
    pub fn reason(&self) -> Option<ConfigReason> {
        self.reason
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for OrchestratorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.reason {
            Some(r) => write!(f, "{}/{}: {}", self.code, r.as_str(), self.message),
            None => write!(f, "{}: {}", self.code, self.message),
        }
    }
}

impl std::error::Error for OrchestratorError {}
