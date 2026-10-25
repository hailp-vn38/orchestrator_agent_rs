//! Truy vấn định nghĩa agent, vòng đời session, submit/result/history và cancel.

use std::collections::{BTreeMap, HashSet};

use crate::agent::{AgentDefinition, AgentMode};
use crate::config::{LlmConfig, LlmProviderConfig};
use crate::error::{ConfigReason, ErrorCode, OrchestratorError};
use crate::feed::event;
use crate::tool::ToolDescriptor;
use crate::types::{
    AcceptedTurn, AgentResult, CallerContext, MessageView, Page, PageRequest, RunStatus,
    SessionPolicy,
};

use super::builder::{validate_and_load_agents, ValidatedAgents};
use super::{LiveAgentCatalog, Orchestrator};

impl Orchestrator {
    pub fn agent(&self, id: &str) -> Option<&AgentDefinition> {
        let guard = self.catalog.read().unwrap_or_else(|p| p.into_inner());
        guard.get_agent(id)
    }

    /// Danh sách agent id đã sắp xếp (ổn định cho test/log).
    pub fn agent_ids(&self) -> Vec<&str> {
        let guard = self.catalog.read().unwrap_or_else(|p| p.into_inner());
        let mut ids: Vec<&str> = guard.agents.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Snapshot các `AgentDefinition` đang active (đã cập nhật qua `reconfigure`).
    pub fn agent_definitions(&self) -> Vec<AgentDefinition> {
        let guard = self.catalog.read().unwrap_or_else(|p| p.into_inner());
        let mut defs: Vec<AgentDefinition> = guard.agents.values().map(|d| (*d).clone()).collect();
        defs.sort_by(|a, b| a.id.cmp(&b.id));
        defs
    }

    /// Cập nhật live danh sách LLM providers và `AgentDefinition` cho mọi turn/run mới.
    /// Các run đang chạy không bị ngắt giữa chừng.
    pub async fn reconfigure(
        &self,
        providers: Vec<LlmProviderConfig>,
        agents: Vec<AgentDefinition>,
    ) -> Result<(), OrchestratorError> {
        self.ensure_open()?;
        if self.has_provider_runtime {
            return Err(OrchestratorError::config(
                ConfigReason::ProviderRuntimeConflict,
                "cannot reconfigure llm.providers when a provider runtime is used",
            ));
        }
        let llm_cfg = LlmConfig {
            providers: providers.clone(),
        };
        llm_cfg.validate()?;
        let new_fingerprints: BTreeMap<String, String> = providers
            .iter()
            .map(|p| (p.id.trim().to_string(), p.routing_fingerprint()))
            .collect();
        let registered_tools: HashSet<String> = self.tools.keys().cloned().collect();
        let ValidatedAgents {
            agents: new_agents,
            routes: new_routes,
            prompts,
        } = validate_and_load_agents(
            agents,
            &new_fingerprints,
            &registered_tools,
            &self.skills,
            self.has_skills_dir,
        )
        .await?;

        let mut catalog_guard = self.catalog.write().unwrap_or_else(|p| p.into_inner());
        if let Some(gw) = &self.client_gateway {
            gw.replace_providers(&providers);
        }
        self.prompt
            .replace(prompts.iter().map(|(id, p)| (id.as_str(), p.as_str())));
        self.tasks.replace(&new_agents, new_routes);
        *catalog_guard =
            LiveAgentCatalog::new(new_agents, new_fingerprints, Some(&catalog_guard));
        Ok(())
    }

    pub fn tool_descriptors(&self) -> Vec<ToolDescriptor> {
        self.tools.values().map(|t| t.descriptor()).collect()
    }

    /// Non-secret routing fingerprint của provider alias (EMB-16).
    pub fn provider_fingerprint(&self, provider_id: &str) -> Option<&str> {
        let guard = self.catalog.read().unwrap_or_else(|p| p.into_inner());
        guard.fingerprints.get(provider_id).copied()
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
            .catalog
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get_agent(agent_id)
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
    /// Kèm trang đầu giới hạn `artifact_preview` (`<= artifact_preview_limit`) và
    /// `artifacts_next_cursor` (docs/17 §17.6, docs/18 §18.7).
    pub async fn result(
        &self,
        caller: &CallerContext,
        session_id: &str,
        turn_id: &str,
    ) -> Result<AgentResult, OrchestratorError> {
        self.ensure_open()?;
        let mut res = self.store.result(caller, session_id, turn_id).await?;
        let preview = self
            .artifacts
            .preview_for_turn(&caller.owner_id, &caller.workspace_id, session_id, turn_id)
            .await?;
        res.artifact_preview = preview.items;
        res.artifacts_next_cursor = preview.next_cursor;
        Ok(res)
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
}
