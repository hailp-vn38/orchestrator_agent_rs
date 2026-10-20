//! Fault hook cho test việc hội tụ qua canonical snapshot.
use super::{event, lock, ExecutionEvent};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Fault hook (feature `test-support`): làm sai lệch **việc publish** sau khi DB đã commit, để
/// kiểm chứng hội tụ qua snapshot. Chỉ điều khiển fault; assertion vẫn nhìn hành vi bên ngoài.
#[cfg(feature = "test-support")]
#[derive(Default)]
pub struct FeedFault {
    suppress_terminal: AtomicBool,
    reverse_pairs: AtomicBool,
    held: Mutex<Option<ExecutionEvent>>,
}

#[cfg(feature = "test-support")]
impl FeedFault {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// `true`: không publish event terminal (`run.*`, `tool.completed/failed/interrupted`, final
    /// `message.committed`) dù DB đã commit (AT-45/ST-16). Connection/subscription vẫn sống.
    pub fn suppress_terminal(&self, on: bool) {
        self.suppress_terminal.store(on, Ordering::Release);
    }

    /// `true`: các committed event được publish đảo thứ tự theo từng cặp (revision cao trước
    /// revision thấp, AT-37/ST-15). Event lẻ cuối cùng bị giữ lại tới event committed kế tiếp.
    pub fn reverse_pairs(&self, on: bool) {
        self.reverse_pairs.store(on, Ordering::Release);
    }

    pub(super) fn intercept(&self, event: ExecutionEvent) -> Vec<ExecutionEvent> {
        if !event.is_committed() {
            return vec![event];
        }
        let final_message =
            event.event_type == event::MESSAGE_COMMITTED && event.payload["final"] == json!(true);
        if self.suppress_terminal.load(Ordering::Acquire)
            && (final_message || event::TERMINAL.contains(&event.event_type.as_str()))
        {
            return Vec::new();
        }
        let mut held = lock(&self.held);
        if self.reverse_pairs.load(Ordering::Acquire) {
            return match held.take() {
                Some(previous) => vec![event, previous],
                None => {
                    *held = Some(event);
                    Vec::new()
                }
            };
        }
        match held.take() {
            Some(previous) => vec![previous, event],
            None => vec![event],
        }
    }
}
