//! Truy vấn định nghĩa agent, vòng đời session, submit/result/history và cancel.

use crate::agent::{AgentDefinition, AgentMode};
use crate::error::{ErrorCode, OrchestratorError};
use crate::feed::event;
use crate::tool::ToolDescriptor;
use crate::types::{
    AcceptedTurn, AgentResult, CallerContext, MessageView, Page, PageRequest, RunStatus,
    SessionPolicy,
};

use super::Orchestrator;

impl Orchestrator {
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
