//! `orchestrator_agent_rs` — embeddable multi-agent orchestrator (v5.0 design).
//!
//! M0 (library shell): typed config/errors, [`Orchestrator::builder`], [`HostTool`]
//! registration contract, embedded mandatory base và registry validation.
//! M1 (single agent): SQLite execution store + owner lock, durable admission (`submit`),
//! managed supervisor, single-agent text run, `result`/`history`.
//! M2 (tools & skills): ToolExecutor, SkillManager, PromptBuilder, tool loop tuần tự.
//! M3 (parent–child): tool `subagent` foreground, child session riêng, admission atomic.
//! M4 (continuous): steps cap tùy chọn, auto compaction trong active turn, stream assembly.
//! M5 (recovery & lifecycle): startup recovery bounded, operator reconciliation, cancel
//! (parent → active child), shutdown drain/quiesce/join.
//! M6 (observation): `subscribe` (bounded broadcast, best-effort) + `snapshot` (family, một read
//! transaction); committed event chỉ publish sau commit và chỉ invalidate canonical view.
//! M7 (artifacts): local `ArtifactStore`, scoped `ToolContext` capabilities (`artifacts().put/open`,
//! `checkpoint().current/commit`), `list_artifacts`, `open_artifact`, `backup_to` và `AgentResult`
//! artifact preview/cursor.

mod agent;
mod artifact;
mod compaction;
mod config;
mod error;
mod executor;
mod feed;
// Item `pub` trong module private; chỉ lộ ra qua `test_support` khi bật feature.
mod gateway;
mod orchestrator;
mod owner_lock;
mod prompt;
mod runtime;
mod secret;
mod skill;
mod store;
mod subagent;
mod supervisor;
#[cfg(feature = "test-support")]
pub mod test_support;
mod tool;
mod types;

pub use agent::{AgentDefinition, AgentMode, AgentRegistry};
pub use artifact::{ArtifactDownload, ArtifactInput, ArtifactReader};
pub use config::{
    LlmBackend, LlmConfig, LlmProviderConfig, OrchestratorConfig, ProviderAuth, RuntimeConfig,
};
pub use error::{ConfigReason, ErrorCode, OrchestratorError};
pub use feed::{ExecutionEvent, FeedItem, Subscription};
pub use orchestrator::{Orchestrator, OrchestratorBuilder};
pub use secret::SecretString;
pub use tool::{
    HostTool, ToolArtifacts, ToolCheckpoint, ToolContext, ToolDescriptor, ToolEffect, ToolError,
    ToolOutcome, ToolScope,
};
pub use types::{
    AcceptedTurn, AgentResult, ArtifactRef, ArtifactRole, CallerContext, FamilySnapshot,
    HistoryHit, HistorySearch, InvocationSnapshot, InvocationStatus, MessageRole, MessageToolCall,
    MessageView, OperatorContext, Page, PageRequest, ResultError, RunSnapshot, RunStatus,
    RunSummary, SessionFilter, SessionPolicy, SessionSearch, SessionSnapshot, SessionSummary,
    TurnSummary, VerifiedToolResolution,
};
