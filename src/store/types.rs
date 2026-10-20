//! Kiểu dữ liệu của execution store.

use crate::error::OrchestratorError;
use crate::types::{RunStatus, SessionPolicy};

/// Routing đã resolve lúc admission; persist vào `agent_runs` của attempt.
#[derive(Debug, Clone)]
pub(crate) struct RunRoute {
    pub provider_id: String,
    pub model_id: String,
    pub fingerprint: String,
}

#[derive(Debug, Clone)]
pub(crate) struct RunRecord {
    pub run_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub agent_id: String,
    pub provider_id: String,
    pub model_id: String,
    pub status: RunStatus,
    pub owner_id: String,
    pub workspace_id: String,
    pub policy: SessionPolicy,
    /// `Some` khi run thuộc child session (depth 1): child không được ủy quyền tiếp.
    pub parent_session_id: Option<String>,
}

impl RunRecord {
    /// Root của family (depth 1): parent nếu là child, ngược lại chính session.
    pub(crate) fn root_session_id(&self) -> &str {
        self.parent_session_id
            .as_deref()
            .unwrap_or(&self.session_id)
    }
}

/// Đích của một child turn: tạo child mới hoặc tiếp tục child cũ của chính parent.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ChildTarget<'a> {
    New {
        agent_id: &'a str,
    },
    Resume {
        session_id: &'a str,
        agent_id: &'a str,
    },
}

/// Child turn đã admit (hoặc đã admit trước đó cho cùng parent invocation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChildAdmission {
    pub session_id: String,
    pub turn_id: String,
    /// `false`: replay idempotent của một admission đã commit; không tạo thêm gì.
    pub created: bool,
}

/// Lý do admission child bị từ chối. Mọi nhánh từ chối đều rollback toàn bộ transaction.
#[derive(Debug)]
pub(crate) enum ChildAdmitError {
    /// Invocation/run của parent không còn hợp lệ (đã terminal hoặc không phải `subagent`).
    ParentUnavailable,
    /// Không phải child của parent này (không tồn tại, parent khác, owner/workspace khác).
    NotChild,
    Archived,
    AgentMismatch,
    DepthLimit,
    Busy,
    /// Child session có turn mới nhất đang chờ operator reconcile (M5).
    Blocked,
    RouteUnavailable,
    /// Lỗi lưu trữ; chi tiết đã ghi tracing ở `OrchestratorError::storage`.
    Store,
}

impl From<OrchestratorError> for ChildAdmitError {
    fn from(_: OrchestratorError) -> Self {
        Self::Store
    }
}

/// Tool call đã persist trong assistant message (`id` = provider call ID).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

/// Transcript dùng để dựng model context (user/assistant/tool theo thứ tự `sequence`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TranscriptEntry {
    User(String),
    Assistant {
        text: String,
        calls: Vec<StoredToolCall>,
    },
    Tool {
        call_id: String,
        tool_name: String,
        text: String,
    },
}

/// Checkpoint compaction mới nhất của session (docs/09 §9.8). Transcript gốc không bị xóa;
/// model context = summary + các entry có `sequence >= keep_from_sequence`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    pub summary: String,
    pub keep_from_sequence: u64,
    /// Yêu cầu của user cho turn đang chạy, chỉ có khi message gốc nằm trong phần đã nén.
    pub objective: Option<String>,
}

/// Dữ liệu để ghi một checkpoint mới.
#[derive(Debug, Clone)]
pub(crate) struct NewCheckpoint {
    pub summary: String,
    pub keep_from_sequence: u64,
    pub objective: Option<String>,
    /// `provider/model` đã sinh summary (không chứa secret).
    pub model_ref: String,
}

/// Context của một session đã áp checkpoint mới nhất: entry kèm `sequence`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Context {
    pub checkpoint: Option<Checkpoint>,
    pub entries: Vec<(u64, TranscriptEntry)>,
}

/// Invocation đã persist nhưng chưa settle. `started=false`: `requested` (chưa từng chạy);
/// `started=true`: `running` (outcome chưa biết nếu process chết giữa chừng).
#[derive(Debug, Clone)]
pub(crate) struct PendingInvocation {
    pub id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub arguments: serde_json::Value,
    pub started: bool,
}
