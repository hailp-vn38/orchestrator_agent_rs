//! `OrchestratorBuilder`: nối config, registry, store, gateway, tool và recovery lúc build.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::agent::{AgentDefinition, AgentRegistry};
use crate::artifact::{ArtifactLimits, ArtifactStore};
use crate::config::{OrchestratorConfig, ProviderAuth};
use crate::error::{ConfigReason, OrchestratorError};
use crate::executor::{replay_safe_names, ToolExecutor, ToolLimits};
use crate::feed::{Emitter, ExecutionFeed};
use crate::gateway::{ClientGateway, LlmGateway};
use crate::owner_lock::OwnerLock;
use crate::prompt::PromptBuilder;
use crate::runtime::{AgentRuntime, ContextPolicy};
use crate::skill::SkillManager;
use crate::store::{RecoveryPolicy, RunRoute, SessionStore, DB_FILE};
use crate::subagent::TaskManager;
use crate::supervisor::{AdmissionSignal, Supervisor};
use crate::tool::{validate_descriptor, HostTool, ToolServices};

use super::{route_of, Orchestrator, OrchestratorBuilder};

const PROJECT_INSTRUCTIONS_MAX_BYTES: usize = 64 * 1024;

/// Mandatory base, embed lúc compile; host không cung cấp file.
pub(super) const BASE_INSTRUCTIONS: &str = include_str!("../base_instructions.md");
const BASE_VERSION_PREFIX: &str = "<!-- base-instructions-version: ";
const OWNER_LOCK_FILE: &str = "owner.lock";

impl Orchestrator {
    pub fn builder(config: OrchestratorConfig) -> OrchestratorBuilder {
        OrchestratorBuilder {
            config,
            tools: Vec::new(),
            gateway: None,
            drop_admission_triggers: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "test-support")]
            feed_fault: None,
            #[cfg(feature = "test-support")]
            artifact_fault: None,
        }
    }
}

impl OrchestratorBuilder {
    pub fn register_tool(mut self, tool: Arc<dyn HostTool>) -> Self {
        self.tools.push(tool);
        self
    }

    /// Test support nội bộ: thay gateway thật bằng fake (không phải API production).
    #[cfg(feature = "test-support")]
    pub fn with_gateway(mut self, gateway: Arc<dyn LlmGateway>) -> Self {
        self.gateway = Some(gateway);
        self
    }

    /// Test support nội bộ: fault hook bỏ trigger sau admission.
    #[cfg(feature = "test-support")]
    pub fn with_dropped_admission_triggers(mut self, flag: Arc<AtomicBool>) -> Self {
        self.drop_admission_triggers = flag;
        self
    }

    /// Test support nội bộ: fault hook làm sai lệch việc publish sau commit.
    #[cfg(feature = "test-support")]
    pub fn with_feed_fault(mut self, fault: Arc<crate::feed::FeedFault>) -> Self {
        self.feed_fault = Some(fault);
        self
    }

    /// Test support nội bộ: fault hook cho staging/rename/checkpoint CAS của ArtifactStore.
    #[cfg(feature = "test-support")]
    pub fn with_artifact_fault(mut self, fault: Arc<crate::artifact::ArtifactFault>) -> Self {
        self.artifact_fault = Some(fault);
        self
    }

    /// Validate cấu hình cục bộ, giữ owner lock của store, mở SQLite (migrate) và khởi
    /// động supervisor. Không tạo Tokio runtime, không gọi remote provider; chạy trong
    /// runtime hiện có của host.
    pub async fn build(self) -> Result<Orchestrator, OrchestratorError> {
        let base_version = parse_base_version(BASE_INSTRUCTIONS)?;

        self.config.validate_local()?;
        self.config.llm.validate()?;

        let mut tools: BTreeMap<String, Arc<dyn HostTool>> = BTreeMap::new();
        for tool in self.tools {
            let desc = tool.descriptor();
            validate_descriptor(&desc)?;
            if tool.timeout().is_some_and(|t| t.is_zero()) {
                return Err(OrchestratorError::config(
                    ConfigReason::InvalidTimeout,
                    format!("tool '{}': timeout must be > 0 when set", desc.name),
                ));
            }
            if tools.contains_key(&desc.name) {
                return Err(OrchestratorError::config(
                    ConfigReason::DuplicateTool,
                    format!("duplicate tool '{}'", desc.name),
                ));
            }
            tools.insert(desc.name, tool);
        }

        let names: HashSet<String> = tools.keys().cloned().collect();
        let registry = AgentRegistry::build(self.config.agents, &self.config.llm, &names).await?;

        let skills = match &self.config.skills_dir {
            Some(dir) => SkillManager::discover(dir, self.config.runtime.skill_max_bytes).await?,
            None => SkillManager::empty(self.config.runtime.skill_max_bytes),
        };
        if self.config.skills_dir.is_some() {
            for def in registry.list() {
                if let Some(missing) = def.allowed_skills.iter().find(|s| !skills.contains(s)) {
                    return Err(OrchestratorError::config(
                        ConfigReason::UnknownSkill,
                        format!("agent '{}': unknown skill '{}'", def.id, missing),
                    ));
                }
            }
        }

        let project = match &self.config.project_instructions {
            Some(path) => {
                let bad = || {
                    OrchestratorError::config(
                        ConfigReason::ProjectInstructionsInvalid,
                        "project instructions must be a readable UTF-8 file within 64KiB",
                    )
                };
                let text = tokio::fs::read_to_string(path).await.map_err(|_| bad())?;
                if text.len() > PROJECT_INSTRUCTIONS_MAX_BYTES {
                    return Err(bad());
                }
                Some(text)
            }
            None => None,
        };

        // Snapshot prompt lúc build (registry bất biến, không hot reload).
        let mut prompts: Vec<(String, String)> = Vec::new();
        for def in registry.list() {
            let prompt = tokio::fs::read_to_string(&def.prompt_path)
                .await
                .map_err(|_| {
                    OrchestratorError::config(
                        ConfigReason::PromptPathInvalid,
                        format!(
                            "agent '{}': prompt_path must be a readable UTF-8 file",
                            def.id
                        ),
                    )
                })?;
            prompts.push((def.id.clone(), prompt));
        }
        let prompt_builder = PromptBuilder::new(
            BASE_INSTRUCTIONS,
            project.as_deref(),
            prompts.iter().map(|(id, p)| (id.as_str(), p.as_str())),
        );
        let agents: HashMap<String, AgentDefinition> =
            registry.list().map(|d| (d.id.clone(), d.clone())).collect();
        let fingerprints: BTreeMap<String, String> = self
            .config
            .llm
            .providers
            .iter()
            .map(|p| (p.id.trim().to_string(), p.routing_fingerprint()))
            .collect();

        // Owner guard từ lúc mở execution store (không đợi M5). Lock nhả khi supervisor
        // và mọi task local đã dừng.
        let store_dir = &self.config.store_dir;
        tokio::fs::create_dir_all(store_dir)
            .await
            .map_err(|e| OrchestratorError::storage("create store dir", &e))?;
        let owner_lock = OwnerLock::acquire(&store_dir.join(OWNER_LOCK_FILE))?;
        let store = SessionStore::open(&store_dir.join(DB_FILE)).await?;
        let artifact_limits = ArtifactLimits::from_runtime(&self.config.runtime);
        #[cfg(feature = "test-support")]
        let artifact_fault = self.artifact_fault;
        #[cfg(not(feature = "test-support"))]
        let artifact_fault = None;
        let artifacts =
            ArtifactStore::open(store.clone(), store_dir, artifact_limits, artifact_fault).await?;
        let swept = artifacts.sweep_configured_orphans().await?;
        if swept > 0 {
            tracing::info!(
                swept,
                "swept orphan staging/unpublished artifact files under owner lock"
            );
        }
        // Dưới owner lock không còn process nào khác: mọi run `running` còn lại là của process
        // đã chết. Đánh dấu interrupted trước, rồi mới quyết định resume/block (không replay).
        let interrupted = store.mark_stale_runs_interrupted().await?;
        if interrupted > 0 {
            tracing::warn!(
                interrupted,
                "runs left running by a previous process were marked interrupted"
            );
        }

        let signal = AdmissionSignal::new(self.drop_admission_triggers);
        let routes: HashMap<String, RunRoute> = registry
            .list()
            .filter_map(|d| route_of(d, &fingerprints).map(|r| (d.id.clone(), r)))
            .collect();
        let replay_safe = replay_safe_names(&tools);
        let limits = ToolLimits {
            timeout: self.config.runtime.tool_timeout,
            model_cap_bytes: self.config.runtime.tool_output_cap_bytes,
            hard_cap_bytes: self.config.runtime.tool_result_hard_cap_bytes,
            progress_interval: self.config.runtime.progress_min_interval,
        };

        // API key đã cấu hình bị che trong mọi text ephemeral của feed (không bao giờ phát đi).
        let secrets: Vec<String> = self
            .config
            .llm
            .providers
            .iter()
            .filter_map(|p| match &p.auth {
                ProviderAuth::ApiKey(key) => Some(key.expose().to_owned()),
                ProviderAuth::None => None,
            })
            .collect();
        #[allow(unused_mut)]
        let mut feed = ExecutionFeed::new(
            self.config.runtime.event_capacity,
            self.config.runtime.event_delta_max_bytes,
            secrets,
        );
        #[cfg(feature = "test-support")]
        if let Some(fault) = self.feed_fault {
            feed = feed.with_fault(fault);
        }
        let emitter = Emitter::new(feed.clone(), store.clone());

        // Startup recovery bounded (docs/15 §15.7): attempt mới giữ turn_id khi an toàn; outcome
        // không xác định hoặc routing identity đổi thì chặn. Chạy trước khi supervisor spawn.
        let report = store
            .recover_all(&RecoveryPolicy {
                max_resume_attempt: self.config.runtime.max_resume_attempts,
                route: &|agent_id| routes.get(agent_id).cloned(),
                replay_safe: &|tool| replay_safe.contains(tool),
            })
            .await?;
        if report != Default::default() {
            tracing::info!(
                resumed = report.resumed,
                blocked = report.blocked,
                exhausted = report.exhausted,
                "startup recovery finished"
            );
        }

        let tasks = TaskManager::new(
            store.clone(),
            &agents,
            routes,
            signal.clone(),
            emitter.clone(),
        );
        let executor = ToolExecutor::new(
            tools.clone(),
            Arc::new(skills),
            tasks,
            limits,
            feed.clone(),
            ToolServices {
                artifacts: artifacts.clone(),
                store: store.clone(),
                emitter: emitter.clone(),
            },
        );

        let gateway: Arc<dyn LlmGateway> = match self.gateway {
            Some(g) => g,
            None => Arc::new(ClientGateway::new(&self.config.llm.providers)),
        };
        let shutdown = CancellationToken::new();
        let runtime = AgentRuntime::new(
            store.clone(),
            gateway,
            prompt_builder,
            executor,
            agents,
            ContextPolicy {
                limit_tokens: self.config.runtime.context_limit_tokens,
                threshold_percent: self.config.runtime.compaction_threshold_percent,
                keep_recent: self.config.runtime.compaction_keep_recent,
            },
            shutdown.clone(),
            emitter.clone(),
        );
        let supervisor = Supervisor::start(
            store.clone(),
            runtime,
            self.config.runtime.supervisor_sweep,
            self.config.runtime.max_concurrent_root_runs,
            owner_lock,
            signal,
            shutdown,
        );

        Ok(Orchestrator {
            base_version,
            registry,
            tools,
            fingerprints,
            store_dir: self.config.store_dir,
            store,
            artifacts,
            supervisor,
            runtime: self.config.runtime,
            replay_safe,
            limits,
            feed,
            emitter,
        })
    }
}

pub(super) fn parse_base_version(base: &str) -> Result<u32, OrchestratorError> {
    let bad = || {
        OrchestratorError::config(
            ConfigReason::BaseInstructionsInvalid,
            "embedded base instructions are missing a valid version header",
        )
    };
    let first = base.lines().next().ok_or_else(bad)?;
    let version = first
        .strip_prefix(BASE_VERSION_PREFIX)
        .and_then(|r| r.strip_suffix(" -->"))
        .and_then(|v| v.trim().parse::<u32>().ok())
        .ok_or_else(bad)?;
    if base.lines().skip(1).all(|l| l.trim().is_empty()) {
        return Err(bad());
    }
    Ok(version)
}
