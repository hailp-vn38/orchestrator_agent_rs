//! Subscription chỉ nhận event của một session family.
use super::{ExecutionEvent, FeedItem};
use std::fmt;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Stream sự kiện của đúng **một** session family. Tạo bằng `Orchestrator::subscribe` sau
/// khi đã kiểm tra owner/workspace. Chậm hay bị drop không chặn runtime hay subscriber khác.
pub struct Subscription {
    rx: broadcast::Receiver<ExecutionEvent>,
    root_session_id: String,
    closed: CancellationToken,
    finished: bool,
}

impl Subscription {
    pub fn root_session_id(&self) -> &str {
        &self.root_session_id
    }

    /// Event kế tiếp của family. `None`: feed đã đóng (shutdown) hoặc subscription đã kết thúc
    /// sau [`FeedItem::ResyncRequired`]. Event của family khác không bao giờ được trả về.
    pub async fn recv(&mut self) -> Option<FeedItem> {
        if self.finished {
            return None;
        }
        loop {
            tokio::select! {
                biased;
                received = self.rx.recv() => match received {
                    Ok(event) if event.root_session_id == self.root_session_id => {
                        return Some(FeedItem::Event(event));
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        self.finished = true;
                        return Some(FeedItem::ResyncRequired);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        self.finished = true;
                        return None;
                    }
                },
                () = self.closed.cancelled() => {
                    self.finished = true;
                    return None;
                }
            }
        }
    }
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("root_session_id", &self.root_session_id)
            .finish_non_exhaustive()
    }
}

impl Subscription {
    pub(super) fn new(
        rx: broadcast::Receiver<ExecutionEvent>,
        root_session_id: &str,
        closed: CancellationToken,
    ) -> Self {
        Self {
            rx,
            root_session_id: root_session_id.to_owned(),
            closed,
            finished: false,
        }
    }
}
