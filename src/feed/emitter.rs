//! Resolve revision từ SQLite rồi phát committed invalidation.
use super::{event, now_ms, tool_payload, ExecutionEvent, ExecutionFeed};
use crate::store::SessionStore;
use crate::types::{InvocationStatus, RunStatus};
use serde_json::{json, Value};

/// Publisher dùng chung của runtime/orchestrator/task manager: resolve root + revision từ SQLite
/// **sau** commit rồi mới publish committed event.
#[derive(Clone)]
pub(crate) struct Emitter {
    feed: ExecutionFeed,
    store: SessionStore,
}

impl Emitter {
    pub(crate) fn new(feed: ExecutionFeed, store: SessionStore) -> Self {
        Self { feed, store }
    }

    pub(crate) fn feed(&self) -> &ExecutionFeed {
        &self.feed
    }

    /// Committed invalidation cho `session_id`. Revision đọc sau commit nên luôn ≥ revision của
    /// transaction vừa commit; không đọc được thì bỏ qua (snapshot định kỳ sẽ hội tụ).
    pub(crate) async fn committed_session(
        &self,
        session_id: &str,
        run_id: Option<&str>,
        event_type: &str,
        payload: Value,
    ) {
        if let Ok(Some(info)) = self.store.event_info_for_session(session_id).await {
            self.publish(info, run_id, event_type, payload);
        }
    }

    pub(crate) async fn committed_run(&self, run_id: &str, event_type: &str, payload: Value) {
        if let Ok(Some(info)) = self.store.event_info_for_run(run_id).await {
            self.publish(info, Some(run_id), event_type, payload);
        }
    }

    fn publish(
        &self,
        info: crate::store::EventInfo,
        run_id: Option<&str>,
        event_type: &str,
        payload: Value,
    ) {
        self.feed.publish(ExecutionEvent {
            event_type: event_type.to_owned(),
            root_session_id: info.root_session_id,
            session_id: info.session_id,
            run_id: run_id.map(str::to_owned),
            state_revision: Some(info.revision),
            timestamp_ms: now_ms(),
            payload,
        });
    }

    /// Sau khi run đã commit terminal: event cho invocation bị đóng cùng transaction (nếu có),
    /// rồi event terminal của run. Gọi **chỉ** khi commit thành công.
    pub(crate) async fn run_terminal(
        &self,
        run_id: &str,
        status: RunStatus,
        error_code: Option<&str>,
    ) {
        if let Ok(closed) = self.store.closed_invocations(run_id).await {
            for inv in closed {
                let mut payload =
                    tool_payload(&inv.id, &inv.tool_call_id, &inv.tool_name, &inv.arguments);
                let event_type = if inv.status == InvocationStatus::Cancelled {
                    payload["status"] = json!("cancelled");
                    event::TOOL_FAILED
                } else {
                    payload["error_code"] = json!(inv.error_code);
                    event::TOOL_INTERRUPTED
                };
                self.committed_run(run_id, event_type, payload).await;
            }
        }
        let (steps, input_tokens, output_tokens) = self
            .store
            .run_usage_summary(run_id)
            .await
            .ok()
            .flatten()
            .unwrap_or((0, 0, 0));
        let event_type = event::run_terminal(status);
        self.committed_run(
            run_id,
            event_type,
            json!({
                "status": status.as_str(),
                "error_code": error_code,
                "steps": steps,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": input_tokens.saturating_add(output_tokens),
            }),
        )
        .await;
    }
}
