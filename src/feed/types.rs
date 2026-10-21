//! Envelope sự kiện và phần tử của subscription.
use serde_json::{json, Value};

/// Payload chung của event tool (kèm `arguments` JSON mà LLM truyền cho tool).
pub(crate) fn tool_payload(
    invocation_id: &str,
    tool_call_id: &str,
    tool_name: &str,
    arguments: &Value,
) -> Value {
    json!({
        "tool_call_id": tool_call_id,
        "invocation_id": invocation_id,
        "tool_name": tool_name,
        "arguments": arguments,
    })
}

/// Envelope tối thiểu (docs/16 §16.4). `payload` có kiểu theo `event_type`.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ExecutionEvent {
    pub event_type: String,
    /// Root của session family, do engine resolve từ ancestry (không tin input của client).
    pub root_session_id: String,
    /// Nguồn của event: Root hoặc Child.
    pub session_id: String,
    pub run_id: Option<String>,
    /// `Some` chỉ với committed invalidation (sau DB transaction); `None` với ephemeral.
    /// Scoped theo session, **không** phải cursor resume.
    pub state_revision: Option<i64>,
    pub timestamp_ms: i64,
    pub payload: Value,
}

impl ExecutionEvent {
    pub fn is_committed(&self) -> bool {
        self.state_revision.is_some()
    }

    /// Dạng JSON của envelope để host adapter (SSE/…) tự serialize.
    pub fn to_json(&self) -> Value {
        json!({
            "event_type": self.event_type,
            "root_session_id": self.root_session_id,
            "session_id": self.session_id,
            "run_id": self.run_id,
            "state_revision": self.state_revision,
            "timestamp_ms": self.timestamp_ms,
            "payload": self.payload,
        })
    }
}

/// Một phần tử của [`super::Subscription`].
#[derive(Debug, Clone, PartialEq)]
pub enum FeedItem {
    Event(ExecutionEvent),
    /// Subscriber đã mất event (broadcast `Lagged`). Subscription này kết thúc ngay sau đó:
    /// bỏ preview chưa xác nhận, mở subscription mới **trước** rồi lấy snapshot mới.
    ResyncRequired,
}
