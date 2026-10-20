//! Orchestrator handle + builder. M1: single-agent text execution trên SQLite store.
//! Chỉ những gì đã hoạt động mới được expose; tool loop/skill/child đến ở M2+.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::agent::{AgentDefinition, AgentMode, AgentRegistry};
use crate::config::{OrchestratorConfig, RuntimeConfig};
use crate::error::{ConfigReason, ErrorCode, OrchestratorError};
use crate::gateway::{ClientGateway, LlmGateway};
use crate::owner_lock::OwnerLock;
use crate::runtime::{build_system, AgentRuntime};
use crate::store::{RunRoute, SessionStore, DB_FILE};
use crate::supervisor::Supervisor;
use crate::tool::{validate_descriptor, HostTool, ToolDescriptor};
use crate::types::{AcceptedTurn, AgentResult, CallerContext, MessageView, Page, PageRequest};

/// Mandatory base, embed lúc compile; host không cung cấp file.
const BASE_INSTRUCTIONS: &str = include_str!("base_instructions.md");
const BASE_VERSION_PREFIX: &str = "<!-- base-instructions-version: ";
const OWNER_LOCK_FILE: &str = "owner.lock";

pub struct Orchestrator {
    base_version: u32,
    registry: AgentRegistry,
    tools: BTreeMap<String, Arc<dyn HostTool>>,
    fingerprints: BTreeMap<String, String>,
    store: SessionStore,
    supervisor: Supervisor,
    runtime: RuntimeConfig,
}

pub struct OrchestratorBuilder {
    config: OrchestratorConfig,
    tools: Vec<Arc<dyn HostTool>>,
    gateway: Option<Arc<dyn LlmGateway>>,
    drop_admission_triggers: Arc<AtomicBool>,
}

impl Orchestrator {
    pub fn builder(config: OrchestratorConfig) -> OrchestratorBuilder {
        OrchestratorBuilder {
            config,
            tools: Vec::new(),
            gateway: None,
            drop_admission_triggers: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn agent(&self, id: &str) -> Option<&AgentDefinition> {
        self.registry.get(id)
    }

    /// Danh sách agent id đã sắp xếp (ổn định cho test/log).
    pub fn agent_ids(&self) -> Vec<&str> {
        let mut ids: Vec<&str> = self.registry.list().map(|a| a.id.as_str()).collect();
        ids.sort_unstable();
        ids
    }

    pub fn tool_descriptors(&self) -> Vec<ToolDescriptor> {
        self.tools.values().map(|t| t.descriptor()).collect()
    }

    /// Non-secret routing fingerprint của provider alias (EMB-16).
    pub fn provider_fingerprint(&self, provider_id: &str) -> Option<&str> {
        self.fingerprints.get(provider_id).map(String::as_str)
    }

    pub fn base_instructions_version(&self) -> u32 {
        self.base_version
    }

    /// Tạo **root** session thuộc scope của caller. Agent phải tồn tại và không phải
    /// `Subagent`-only; child session do runtime tạo, host không gán parent tùy ý.
    pub async fn create_session(
        &self,
        caller: &CallerContext,
        agent_id: &str,
    ) -> Result<String, OrchestratorError> {
        self.ensure_open()?;
        let agent = self
            .registry
            .get(agent_id)
            .ok_or_else(|| OrchestratorError::new(ErrorCode::NotFound, "agent not found"))?;
        if agent.mode == AgentMode::Subagent {
            return Err(OrchestratorError::new(
                ErrorCode::PermissionDenied,
                "agent cannot be used as a root session",
            ));
        }
        self.store.create_root_session(caller, &agent.id).await
    }

    /// Trả sau khi input + run claim đã commit; không đợi LLM. `SESSION_BUSY` nếu session
    /// đã có run active; `SESSION_ARCHIVED` nếu session đã archive.
    pub async fn submit(
        &self,
        caller: &CallerContext,
        session_id: &str,
        text: String,
    ) -> Result<AcceptedTurn, OrchestratorError> {
        self.ensure_open()?;
        if text.trim().is_empty() {
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "text must not be empty",
            ));
        }
        let turn = self
            .store
            .admit_turn(caller, session_id, &text, |agent_id| self.route(agent_id))
            .await?;
        self.supervisor.notify_admitted();
        Ok(turn)
    }

    /// Projection của attempt mới nhất cho `(session_id, turn_id)`; đang chạy thì `output=None`.
    pub async fn result(
        &self,
        caller: &CallerContext,
        session_id: &str,
        turn_id: &str,
    ) -> Result<AgentResult, OrchestratorError> {
        self.ensure_open()?;
        self.store.result(caller, session_id, turn_id).await
    }

    /// Lịch sử canonical của **một** session, phân trang theo cursor opaque gắn với session.
    pub async fn history(
        &self,
        caller: &CallerContext,
        session_id: &str,
        page: PageRequest,
    ) -> Result<Page<MessageView>, OrchestratorError> {
        self.ensure_open()?;
        self.store
            .history_page(caller, session_id, page, self.runtime.page_size)
            .await
    }

    /// Chặn admission mới; history vẫn đọc được. Run đang chạy không bị hủy. Idempotent.
    pub async fn archive_session(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<(), OrchestratorError> {
        self.ensure_open()?;
        self.store.archive_session(caller, session_id).await
    }

    /// Dừng admission, hủy hợp tác run đang chạy (→ `interrupted`), join trong
    /// `shutdown_grace`, đóng store rồi nhả owner lock. Quá grace trả lỗi và giữ lock.
    /// Cancel/lifecycle đầy đủ (kể cả non-cooperative tool) hoàn thiện ở M5.
    pub async fn shutdown(&self) -> Result<(), OrchestratorError> {
        self.supervisor.shutdown(self.runtime.shutdown_grace).await
    }

    fn ensure_open(&self) -> Result<(), OrchestratorError> {
        if self.supervisor.is_shutting_down() {
            return Err(OrchestratorError::new(
                ErrorCode::ShuttingDown,
                "the orchestrator is shutting down",
            ));
        }
        Ok(())
    }

    fn route(&self, agent_id: &str) -> Option<RunRoute> {
        let def = self.registry.get(agent_id)?;
        let provider_id = def.provider_id.trim().to_string();
        let fingerprint = self.fingerprints.get(&provider_id)?.clone();
        Some(RunRoute {
            model_id: def.model_id.trim().to_string(),
            provider_id,
            fingerprint,
        })
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

        // Snapshot prompt lúc build (registry bất biến, không hot reload).
        let mut systems = HashMap::new();
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
            systems.insert(def.id.clone(), build_system(BASE_INSTRUCTIONS, &prompt));
        }

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
        let interrupted = store.interrupt_stale_runs().await?;
        if interrupted > 0 {
            tracing::warn!(
                interrupted,
                "runs left running by a previous process were marked interrupted"
            );
        }

        let gateway: Arc<dyn LlmGateway> = match self.gateway {
            Some(g) => g,
            None => Arc::new(ClientGateway::new(&self.config.llm.providers)),
        };
        let runtime = AgentRuntime::new(store.clone(), gateway, systems);
        let supervisor = Supervisor::start(
            store.clone(),
            runtime,
            self.config.runtime.supervisor_sweep,
            owner_lock,
            self.drop_admission_triggers,
        );

        Ok(Orchestrator {
            base_version,
            registry,
            tools,
            fingerprints,
            store,
            supervisor,
            runtime: self.config.runtime,
        })
    }
}

fn parse_base_version(base: &str) -> Result<u32, OrchestratorError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_base_is_valid_and_versioned() {
        assert_eq!(parse_base_version(BASE_INSTRUCTIONS), Ok(1));
    }

    #[test]
    fn base_version_rejects_malformed() {
        assert!(parse_base_version("").is_err());
        assert!(parse_base_version("no header\ntext").is_err());
        assert!(parse_base_version("<!-- base-instructions-version: x -->\ntext").is_err());
        assert!(parse_base_version("<!-- base-instructions-version: 1 -->\n\n  \n").is_err());
    }
}
