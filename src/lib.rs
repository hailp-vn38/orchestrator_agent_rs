//! `orchestrator_agent_rs` — embeddable multi-agent orchestrator (v5.0 design).
//!
//! M0 (library shell): typed config/errors, [`Orchestrator::builder`], [`HostTool`]
//! registration contract, embedded mandatory base và registry validation.
//! Runtime, persistence và LLM gateway đến ở M1+.

mod agent;
mod config;
mod error;
mod orchestrator;
mod secret;
mod tool;

pub use agent::{AgentDefinition, AgentMode, AgentRegistry};
pub use config::{LlmBackend, LlmConfig, LlmProviderConfig, OrchestratorConfig, ProviderAuth};
pub use error::{ConfigReason, ErrorCode, OrchestratorError};
pub use orchestrator::{Orchestrator, OrchestratorBuilder};
pub use secret::SecretString;
pub use tool::{
    HostTool, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome, ToolScope,
};
