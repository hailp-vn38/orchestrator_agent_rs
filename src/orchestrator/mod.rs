//! Orchestrator handle + builder: single-agent text, tool/skill loop, parent–child trên SQLite store.
//! Chỉ những gì đã hoạt động mới được expose. M5 thêm startup recovery, `cancel`,
//! `resolve_interrupted_invocation` và shutdown nhiều pha. M6 thêm observation:
//! `subscribe` (feed bounded, best-effort) và `snapshot` (canonical, một read transaction).
//! M7 thêm `ArtifactStore`, `list_artifacts`, `open_artifact`, `backup_to`, và `AgentResult`
//! artifact preview + cursor.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::agent::{AgentDefinition, AgentRegistry};
use crate::artifact::ArtifactStore;
use crate::config::{OrchestratorConfig, RuntimeConfig};
use crate::executor::ToolLimits;
use crate::feed::{Emitter, ExecutionFeed};
use crate::gateway::LlmGateway;
use crate::store::{RunRoute, SessionStore};
use crate::supervisor::Supervisor;
use crate::tool::HostTool;

mod artifacts;
mod builder;
mod lifecycle;
mod observe;
mod query;
mod session;

#[cfg(test)]
mod tests;

pub struct Orchestrator {
    base_version: u32,
    registry: AgentRegistry,
    tools: BTreeMap<String, Arc<dyn HostTool>>,
    fingerprints: BTreeMap<String, String>,
    store_dir: PathBuf,
    store: SessionStore,
    artifacts: ArtifactStore,
    supervisor: Supervisor,
    runtime: RuntimeConfig,
    /// Tool replay-safe (xem `replay_safe_names`) cho policy recovery sau operator resolve.
    replay_safe: BTreeSet<String>,
    limits: ToolLimits,
    feed: ExecutionFeed,
    emitter: Emitter,
}

pub struct OrchestratorBuilder {
    config: OrchestratorConfig,
    tools: Vec<Arc<dyn HostTool>>,
    gateway: Option<Arc<dyn LlmGateway>>,
    drop_admission_triggers: Arc<AtomicBool>,
    #[cfg(feature = "test-support")]
    feed_fault: Option<Arc<crate::feed::FeedFault>>,
    #[cfg(feature = "test-support")]
    artifact_fault: Option<Arc<crate::artifact::ArtifactFault>>,
}

/// Routing đã resolve của một agent theo provider/model của **chính nó** (Root và Child có
/// thể dùng provider khác nhau).
fn route_of(def: &AgentDefinition, fingerprints: &BTreeMap<String, String>) -> Option<RunRoute> {
    let provider_id = def.provider_id.trim().to_string();
    let fingerprint = fingerprints.get(&provider_id)?.clone();
    Some(RunRoute {
        model_id: def.model_id.trim().to_string(),
        provider_id,
        fingerprint,
    })
}
