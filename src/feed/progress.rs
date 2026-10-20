//! Progress của một invocation, có throttle.
use super::{event, lock, ExecutionFeed};
use serde_json::json;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Sender progress gắn với một invocation; nằm trong `ToolContext` (không public type riêng).
#[derive(Clone)]
pub(crate) struct ProgressSink {
    feed: ExecutionFeed,
    root_session_id: String,
    session_id: String,
    run_id: String,
    tool_call_id: String,
    invocation_id: String,
    min_interval: Duration,
    last: Arc<Mutex<Option<Instant>>>,
}

impl fmt::Debug for ProgressSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProgressSink")
    }
}

impl ProgressSink {
    pub(crate) fn new(
        feed: &ExecutionFeed,
        scope: &crate::tool::ToolScope,
        min_interval: Duration,
    ) -> Self {
        Self {
            feed: feed.clone(),
            root_session_id: scope.root_session_id.clone(),
            session_id: scope.session_id.clone(),
            run_id: scope.run_id.clone(),
            tool_call_id: scope.tool_call_id.clone(),
            invocation_id: scope.invocation_id.clone(),
            min_interval,
            last: Arc::new(Mutex::new(None)),
        }
    }

    /// Throttle theo `min_interval`: update tới quá sớm bị bỏ (ephemeral, best-effort).
    pub(crate) fn send(&self, message: &str, percent: Option<u8>) {
        {
            let mut last = lock(&self.last);
            let now = Instant::now();
            if last.is_some_and(|t| now.duration_since(t) < self.min_interval) {
                return;
            }
            *last = Some(now);
        }
        let mut payload = json!({
            "tool_call_id": self.tool_call_id,
            "invocation_id": self.invocation_id,
            "message": self.feed.redact(message),
        });
        if let Some(percent) = percent {
            payload["percent"] = json!(percent);
        }
        self.feed.ephemeral(
            &self.root_session_id,
            &self.session_id,
            Some(&self.run_id),
            event::TOOL_PROGRESS,
            payload,
        );
    }
}
