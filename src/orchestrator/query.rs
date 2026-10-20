//! Read API: liệt kê/tìm session, run, turn và lịch sử xuyên session. Chỉ đọc; mọi lời gọi
//! kiểm owner/workspace của `CallerContext`.

use crate::error::OrchestratorError;
use crate::types::{
    CallerContext, HistoryHit, HistorySearch, Page, PageRequest, RunSummary, SessionFilter,
    SessionSearch, SessionSummary, TurnSummary,
};

use super::Orchestrator;

impl Orchestrator {
    /// Root session của caller (không gồm Child), mới tạo trước, lọc theo metadata.
    pub async fn list_sessions(
        &self,
        caller: &CallerContext,
        filter: &SessionFilter,
        page: PageRequest,
    ) -> Result<Page<SessionSummary>, OrchestratorError> {
        self.ensure_open()?;
        self.store
            .list_root_sessions(caller, filter, None, page, self.runtime.page_size)
            .await
    }

    /// Như [`Self::list_sessions`] nhưng chỉ giữ family có `agent_id` hoặc nội dung
    /// user/assistant (Root lẫn Child) khớp từ khóa. `InvalidInput` nếu từ khóa rỗng/quá dài.
    pub async fn search_sessions(
        &self,
        caller: &CallerContext,
        search: &SessionSearch,
        page: PageRequest,
    ) -> Result<Page<SessionSummary>, OrchestratorError> {
        self.ensure_open()?;
        self.store
            .list_root_sessions(
                caller,
                &search.filter,
                Some(&search.text),
                page,
                self.runtime.page_size,
            )
            .await
    }

    /// Metadata của một session (Root hoặc Child) theo id.
    pub async fn get_session(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<SessionSummary, OrchestratorError> {
        self.ensure_open()?;
        self.store.get_session(caller, session_id).await
    }

    /// Mọi attempt của session theo thứ tự tạo, kể cả retry sau recovery. `turn_id` thu hẹp
    /// về các attempt của một turn.
    pub async fn list_runs(
        &self,
        caller: &CallerContext,
        session_id: &str,
        turn_id: Option<&str>,
        page: PageRequest,
    ) -> Result<Page<RunSummary>, OrchestratorError> {
        self.ensure_open()?;
        self.store
            .list_runs(caller, session_id, turn_id, page, self.runtime.page_size)
            .await
    }

    /// Một run (attempt) theo `run_id`.
    pub async fn get_run(
        &self,
        caller: &CallerContext,
        run_id: &str,
    ) -> Result<RunSummary, OrchestratorError> {
        self.ensure_open()?;
        self.store.get_run(caller, run_id).await
    }

    /// Các turn của session theo thứ tự tạo: input của user, số attempt, attempt mới nhất và
    /// output đã commit.
    pub async fn list_turns(
        &self,
        caller: &CallerContext,
        session_id: &str,
        page: PageRequest,
    ) -> Result<Page<TurnSummary>, OrchestratorError> {
        self.ensure_open()?;
        self.store
            .list_turns(caller, session_id, page, self.runtime.page_size)
            .await
    }

    /// Tìm message (user/assistant/tool) chứa từ khóa trong mọi session thuộc scope của
    /// caller, kết quả mới nhất trước. Khớp không phân biệt hoa/thường với ASCII.
    pub async fn search_history(
        &self,
        caller: &CallerContext,
        search: &HistorySearch,
        page: PageRequest,
    ) -> Result<Page<HistoryHit>, OrchestratorError> {
        self.ensure_open()?;
        self.store
            .search_history(caller, search, page, self.runtime.page_size)
            .await
    }

    /// Xoá vĩnh viễn **Root** session cùng mọi Child, messages, runs, invocations và artifact
    /// (cả file). Không hoàn tác được. `SESSION_BUSY` nếu family còn run active: host phải
    /// [`Self::cancel`] và đợi run kết thúc trước. Subscriber của family không nhận event xoá;
    /// `snapshot` sau đó trả `NOT_FOUND`. File artifact xoá sau commit; file còn sót (lỗi I/O)
    /// do orphan sweep dọn.
    pub async fn delete_session(
        &self,
        caller: &CallerContext,
        root_session_id: &str,
    ) -> Result<(), OrchestratorError> {
        let _gate = self.supervisor.admission().await?;
        let keys = self.store.delete_family(caller, root_session_id).await?;
        self.artifacts.remove_committed(&keys).await;
        Ok(())
    }
}
