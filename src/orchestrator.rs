//! Orchestrator handle + builder: single-agent text, tool/skill loop, parent–child trên SQLite store.
//! Chỉ những gì đã hoạt động mới được expose. M5 thêm startup recovery, `cancel`,
//! `resolve_interrupted_invocation` và shutdown nhiều pha. M6 thêm observation:
//! `subscribe` (feed bounded, best-effort) và `snapshot` (canonical, một read transaction).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::agent::{AgentDefinition, AgentMode, AgentRegistry};
use crate::config::{OrchestratorConfig, ProviderAuth, RuntimeConfig};
use crate::error::{ConfigReason, ErrorCode, OrchestratorError};
use crate::executor::{replay_safe_names, Settled, ToolExecutor, ToolLimits};
use crate::feed::{event, Emitter, ExecutionFeed, Subscription};
use crate::gateway::{ClientGateway, LlmGateway};
use crate::owner_lock::OwnerLock;
use crate::prompt::PromptBuilder;
use crate::runtime::{AgentRuntime, ContextPolicy};
use crate::skill::SkillManager;
use crate::store::{RecoveryPolicy, Resolution, RunRoute, SessionStore, CODE_NO_EFFECT, DB_FILE};
use crate::subagent::TaskManager;
use crate::supervisor::{AdmissionSignal, Supervisor};
use crate::tool::{validate_descriptor, HostTool, ToolDescriptor};
use crate::types::{
    AcceptedTurn, AgentResult, CallerContext, FamilySnapshot, MessageView, OperatorContext, Page,
    PageRequest, RunStatus, SessionPolicy, VerifiedToolResolution,
};

const PROJECT_INSTRUCTIONS_MAX_BYTES: usize = 64 * 1024;

/// Mandatory base, embed lúc compile; host không cung cấp file.
const BASE_INSTRUCTIONS: &str = include_str!("base_instructions.md");
const BASE_VERSION_PREFIX: &str = "<!-- base-instructions-version: ";
const OWNER_LOCK_FILE: &str = "owner.lock";
/// Trần độ dài evidence/operator id trong audit (ký tự).
const EVIDENCE_MAX_CHARS: usize = 2000;
const OPERATOR_ID_MAX_CHARS: usize = 128;

pub struct Orchestrator {
    base_version: u32,
    registry: AgentRegistry,
    tools: BTreeMap<String, Arc<dyn HostTool>>,
    fingerprints: BTreeMap<String, String>,
    store: SessionStore,
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
}

impl Orchestrator {
    pub fn builder(config: OrchestratorConfig) -> OrchestratorBuilder {
        OrchestratorBuilder {
            config,
            tools: Vec::new(),
            gateway: None,
            drop_admission_triggers: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "test-support")]
            feed_fault: None,
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
        self.create_session_with_policy(caller, agent_id, SessionPolicy::default())
            .await
    }

    /// Như [`Self::create_session`] nhưng thu hẹp quyền tool/skill của session. Policy chỉ
    /// giao với quyền của agent, không bao giờ mở rộng được.
    pub async fn create_session_with_policy(
        &self,
        caller: &CallerContext,
        agent_id: &str,
        policy: SessionPolicy,
    ) -> Result<String, OrchestratorError> {
        let _gate = self.supervisor.admission().await?;
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
        self.store
            .create_root_session_with(caller, &agent.id, &policy)
            .await
    }

    /// Trả sau khi input + run claim đã commit; không đợi LLM. `SESSION_BUSY` nếu session
    /// đã có run active; `SESSION_ARCHIVED` nếu session đã archive.
    pub async fn submit(
        &self,
        caller: &CallerContext,
        session_id: &str,
        text: String,
    ) -> Result<AcceptedTurn, OrchestratorError> {
        // Giữ cổng admission tới khi commit xong: shutdown không đóng giữa chừng và submit sau
        // shutdown bị từ chối trước commit.
        let _gate = self.supervisor.admission().await?;
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

    /// Mở stream sự kiện của một session family (docs/16). Chỉ nhận **Root**; owner/workspace
    /// được kiểm tra tại đây, và event của family khác không bao giờ được giao (child mới tạo
    /// vẫn nằm trong family vì `root_session_id` do engine resolve từ ancestry).
    ///
    /// Event chỉ là gợi ý live: committed event (`state_revision` `Some`) chỉ *invalidate* view,
    /// không dùng payload làm patch. Hợp đồng cho host adapter/client: (1) subscribe **trước**
    /// khi gọi [`Self::snapshot`]; (2) refresh snapshot (debounce) tới khi mọi session đạt ≥
    /// revision đã thấy; (3) khi nhận `ResyncRequired`/stream kết thúc: bỏ preview chưa xác
    /// nhận, subscribe lại rồi snapshot lại; (4) khi `FamilySnapshot::has_active_work`, refresh
    /// theo [`Self::snapshot_refresh_interval`] **kể cả khi stream còn sống**.
    pub async fn subscribe(
        &self,
        caller: &CallerContext,
        root_session_id: &str,
    ) -> Result<Subscription, OrchestratorError> {
        self.ensure_open()?;
        self.store.check_root_scope(caller, root_session_id).await?;
        Ok(self.feed.subscribe(root_session_id))
    }

    /// Snapshot canonical của Root và Child đã xác nhận trong SQLite, đọc trong một read
    /// transaction (revision từng session nhất quán với nội dung). Authorization mỗi lần gọi.
    pub async fn snapshot(
        &self,
        caller: &CallerContext,
        root_session_id: &str,
    ) -> Result<FamilySnapshot, OrchestratorError> {
        self.ensure_open()?;
        self.store
            .family_snapshot(caller, root_session_id, self.runtime.page_size)
            .await
    }

    /// Chu kỳ host nên refresh snapshot khi family còn active (`RuntimeConfig::snapshot_refresh`).
    pub fn snapshot_refresh_interval(&self) -> std::time::Duration {
        self.runtime.snapshot_refresh
    }

    /// Chặn admission mới; history vẫn đọc được. Run đang chạy không bị hủy. Idempotent.
    pub async fn archive_session(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<(), OrchestratorError> {
        self.ensure_open()?;
        self.store.archive_session(caller, session_id).await?;
        self.emitter
            .committed_session(
                session_id,
                None,
                event::SESSION_CHANGED,
                serde_json::json!({ "reason": "archived" }),
            )
            .await;
        Ok(())
    }

    /// Hủy hợp tác run đang active của session (và child đang
    /// được nó chờ), trả về khi yêu cầu hủy đã được chấp nhận. Run kết thúc `cancelled`
    /// (terminal, không tự resume); tool Mutating đang chạy tự dừng theo cờ hợp tác và effect
    /// ngoài hệ thống không được đảo ngược. Idempotent: không có run active thì `Ok`.
    pub async fn cancel(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<(), OrchestratorError> {
        self.ensure_open()?;
        self.store.check_scope(caller, session_id).await?;
        // Parent trước child: parent dừng trước nên không ghi kết quả "child bị hủy" giả.
        for run_id in self.store.active_run_family(session_id).await? {
            if !self.supervisor.cancel_run(&run_id) {
                // Claim chưa có worker: ghi terminal trực tiếp; nếu worker vừa spawn thì hủy nốt.
                self.store
                    .finish_run_without_final(
                        &run_id,
                        RunStatus::Cancelled,
                        "CANCELLED",
                        "the run was cancelled",
                    )
                    .await?;
                self.supervisor.cancel_run(&run_id);
                self.emitter
                    .run_terminal(&run_id, RunStatus::Cancelled, Some("CANCELLED"))
                    .await;
            }
        }
        Ok(())
    }

    /// Operator-only: reconcile invocation `interrupted` (outcome không xác định) bằng kết luận
    /// đã xác minh. Chỉ nhận **internal** `invocation_id`. Tool-result, terminal update và audit
    /// commit trong một transaction **trước** khi supervisor resume; không bao giờ replay tool.
    /// `ConfirmedNoEffect` thành technical failure. Resume bị chặn tiếp (vd. routing identity đổi,
    /// còn invocation khác chưa resolve) thì resolution vẫn được ghi và trạng thái hiện trong
    /// `result`.
    pub async fn resolve_interrupted_invocation(
        &self,
        operator: &OperatorContext,
        session_id: &str,
        invocation_id: &str,
        decision: VerifiedToolResolution,
    ) -> Result<(), OrchestratorError> {
        // Resolution có thể tạo attempt mới: giữ cổng admission để shutdown không đóng giữa
        // chừng và không có claim mồ côi sau khi supervisor đã dừng.
        let _gate = self.supervisor.admission().await?;
        if !operator.can_resolve_interrupted {
            return Err(OrchestratorError::new(
                ErrorCode::PermissionDenied,
                "the operator is not allowed to resolve interrupted invocations",
            ));
        }
        let invalid = |m: &str| OrchestratorError::new(ErrorCode::InvalidInput, m.to_owned());
        let clean = |s: &str, max: usize| {
            let t = s.trim();
            !t.is_empty() && t.chars().count() <= max && !t.chars().any(char::is_control)
        };
        if !clean(&operator.operator_id, OPERATOR_ID_MAX_CHARS) {
            return Err(invalid("operator_id must be 1..=128 printable characters"));
        }
        let (outcome_call_id, settled, evidence) = match decision {
            VerifiedToolResolution::ConfirmedCompleted { outcome, evidence } => {
                let call_id = outcome.call_id.clone();
                let settled = self.limits.settle(Ok(outcome));
                if settled.failed {
                    return Err(invalid(
                        "the confirmed outcome exceeds the persistence limits",
                    ));
                }
                (Some(call_id), settled, evidence)
            }
            VerifiedToolResolution::ConfirmedNoEffect { evidence } => (
                None,
                Settled::failure(
                    CODE_NO_EFFECT,
                    "the operator confirmed the tool had no effect; it was not replayed",
                ),
                evidence,
            ),
        };
        if !clean(&evidence, EVIDENCE_MAX_CHARS) {
            return Err(invalid("evidence must be 1..=2000 printable characters"));
        }
        self.store
            .resolve_invocation(
                (&operator.owner_id, &operator.workspace_id),
                session_id,
                invocation_id,
                &Resolution {
                    operator_id: operator.operator_id.trim().to_owned(),
                    evidence: evidence.trim().to_owned(),
                    outcome_call_id,
                    settled,
                },
            )
            .await?;
        tracing::info!(
            session_id,
            invocation_id,
            operator_id = %operator.operator_id.trim(),
            "interrupted invocation resolved by an operator"
        );
        self.emitter
            .committed_session(
                session_id,
                None,
                event::SESSION_CHANGED,
                serde_json::json!({
                    "reason": "invocation_resolved",
                    "invocation_id": invocation_id,
                }),
            )
            .await;
        let route = |agent_id: &str| route_of(self.registry.get(agent_id)?, &self.fingerprints);
        let replay_safe = |tool: &str| self.replay_safe.contains(tool);
        let policy = RecoveryPolicy {
            max_resume_attempt: self.runtime.max_resume_attempts,
            route: &route,
            replay_safe: &replay_safe,
        };
        match self
            .store
            .recover_after_resolution(session_id, &policy)
            .await
        {
            Ok(report) if report.resumed > 0 => self.supervisor.notify_admitted(),
            Ok(_) => {}
            // Resolution đã commit; lần startup sau sẽ tự phục hồi.
            Err(_) => tracing::warn!(session_id, "resume after resolution could not be recorded"),
        }
        Ok(())
    }

    /// Dừng admission, drain có hạn, hủy hợp tác, quiesce/join rồi mới đóng store và nhả owner
    /// lock. Quá `shutdown_grace` mà còn task có thể ghi: trả lỗi **incomplete** và giữ lock.
    /// Run bị ngắt là `interrupted` (phục hồi ở lần khởi động sau); không giả completed/cancelled.
    pub async fn shutdown(&self) -> Result<(), OrchestratorError> {
        self.supervisor
            .shutdown(self.runtime.shutdown_drain, self.runtime.shutdown_grace)
            .await?;
        // Mọi worker đã dừng và đã publish xong: kết thúc các subscription (event đã xếp hàng
        // vẫn được giao trước).
        self.feed.close();
        Ok(())
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
        route_of(self.registry.get(agent_id)?, &self.fingerprints)
    }
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
        let executor =
            ToolExecutor::new(tools.clone(), Arc::new(skills), tasks, limits, feed.clone());

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
            owner_lock,
            signal,
            shutdown,
        );

        Ok(Orchestrator {
            base_version,
            registry,
            tools,
            fingerprints,
            store,
            supervisor,
            runtime: self.config.runtime,
            replay_safe,
            limits,
            feed,
            emitter,
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
