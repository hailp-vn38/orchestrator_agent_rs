//! Orchestrator handle + builder: single-agent text, tool/skill loop, parent–child trên SQLite store.
//! Chỉ những gì đã hoạt động mới được expose. M5 thêm startup recovery, `cancel`,
//! `resolve_interrupted_invocation` và shutdown nhiều pha. M6 thêm observation:
//! `subscribe` (feed bounded, best-effort) và `snapshot` (canonical, một read transaction).
//! M7 thêm `ArtifactStore`, `list_artifacts`, `open_artifact`, `backup_to`, và `AgentResult`
//! artifact preview + cursor.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use crate::agent::AgentDefinition;
use crate::artifact::ArtifactStore;
use crate::config::{OrchestratorConfig, RuntimeConfig};
use crate::executor::ToolLimits;
use crate::feed::{Emitter, ExecutionFeed};
use crate::gateway::{ClientGateway, LlmGateway};
use crate::prompt::PromptBuilder;
use crate::skill::SkillManager;
use crate::store::{RunRoute, SessionStore};
use crate::subagent::TaskManager;
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

pub(crate) struct LiveAgentCatalog {
    pub agents: HashMap<&'static str, &'static AgentDefinition>,
    pub fingerprints: BTreeMap<&'static str, &'static str>,
}

impl LiveAgentCatalog {
    pub(crate) fn new(
        agents: HashMap<String, AgentDefinition>,
        fingerprints: BTreeMap<String, String>,
        prev: Option<&Self>,
    ) -> Self {
        let mut next_agents = HashMap::with_capacity(agents.len());
        for (id, def) in agents {
            match prev.and_then(|p| p.agents.get_key_value(id.as_str())) {
                Some((&prev_id, &prev_def)) if *prev_def == def => {
                    next_agents.insert(prev_id, prev_def);
                }
                _ => {
                    let leaked_id: &'static str = Box::leak(id.into_boxed_str());
                    let leaked_def: &'static AgentDefinition = Box::leak(Box::new(def));
                    next_agents.insert(leaked_id, leaked_def);
                }
            }
        }
        let mut next_fps = BTreeMap::new();
        for (id, fp) in fingerprints {
            match prev.and_then(|p| p.fingerprints.get_key_value(id.as_str())) {
                Some((&prev_id, &prev_fp)) if prev_fp == fp => {
                    next_fps.insert(prev_id, prev_fp);
                }
                _ => {
                    let leaked_id: &'static str = Box::leak(id.into_boxed_str());
                    let leaked_fp: &'static str = Box::leak(fp.into_boxed_str());
                    next_fps.insert(leaked_id, leaked_fp);
                }
            }
        }
        Self {
            agents: next_agents,
            fingerprints: next_fps,
        }
    }

    pub(crate) fn get_agent(&self, id: &str) -> Option<&'static AgentDefinition> {
        self.agents.get(id).copied()
    }
}

pub struct Orchestrator {
    base_version: u32,
    catalog: Arc<RwLock<LiveAgentCatalog>>,
    prompt: PromptBuilder,
    tasks: TaskManager,
    skills: Arc<RwLock<Arc<SkillManager>>>,
    skills_dir: Option<PathBuf>,
    has_provider_runtime: bool,
    client_gateway: Option<Arc<ClientGateway>>,
    tools: BTreeMap<String, Arc<dyn HostTool>>,
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
    provider_runtime: Option<Arc<dyn crate::gateway::ProviderRuntime>>,
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

fn route_of_live(
    def: &AgentDefinition,
    fingerprints: &BTreeMap<&'static str, &'static str>,
) -> Option<RunRoute> {
    let provider_id = def.provider_id.trim();
    let fingerprint = (*fingerprints.get(provider_id)?).to_string();
    Some(RunRoute {
        model_id: def.model_id.trim().to_string(),
        provider_id: provider_id.to_string(),
        fingerprint,
    })
}
