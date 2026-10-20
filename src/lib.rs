//! `orchestrator_agent_rs` — embeddable multi-agent orchestrator (v5.0 design).
//!
//! M0 (library shell): typed config/errors, [`Orchestrator::builder`], [`HostTool`]
//! registration contract, embedded mandatory base và registry validation.
//! M1 (single agent): SQLite execution store + owner lock, durable admission (`submit`),
//! managed supervisor, single-agent text run, `result`/`history`.
//! Tool loop, skill, child, compaction, recovery đầy đủ đến ở M2+.

mod agent;
mod config;
mod error;
// Item `pub` trong module private; chỉ lộ ra qua `test_support` khi bật feature.
mod gateway;
mod orchestrator;
mod owner_lock;
mod runtime;
mod secret;
mod store;
mod supervisor;
#[cfg(feature = "test-support")]
pub mod test_support;
mod tool;
mod types;

pub use agent::{AgentDefinition, AgentMode, AgentRegistry};
pub use config::{
    LlmBackend, LlmConfig, LlmProviderConfig, OrchestratorConfig, ProviderAuth, RuntimeConfig,
};
pub use error::{ConfigReason, ErrorCode, OrchestratorError};
pub use orchestrator::{Orchestrator, OrchestratorBuilder};
pub use secret::SecretString;
pub use tool::{
    HostTool, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome, ToolScope,
};
pub use types::{
    AcceptedTurn, AgentResult, CallerContext, MessageRole, MessageView, Page, PageRequest,
    ResultError, RunStatus,
};
