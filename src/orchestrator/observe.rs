//! Observation: subscribe feed và snapshot.

use crate::error::OrchestratorError;
use crate::feed::Subscription;
use crate::types::{CallerContext, FamilySnapshot};

use super::Orchestrator;

impl Orchestrator {
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
}
