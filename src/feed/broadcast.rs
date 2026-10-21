//! Fan-out best-effort qua bounded Tokio broadcast.
use super::redaction::{redact_all, redact_value, MIN_SECRET_LEN};
#[cfg(feature = "test-support")]
use super::FeedFault;
use super::{now_ms, DeltaFilter, ExecutionEvent, Subscription};
use serde_json::Value;
use std::fmt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

struct FeedInner {
    tx: broadcast::Sender<ExecutionEvent>,
    closed: CancellationToken,
    secrets: Vec<String>,
    delta_max_bytes: usize,
    #[cfg(feature = "test-support")]
    fault: Option<Arc<FeedFault>>,
}

/// Concrete module duy nhất cho fan-out (không trait/broker framework).
#[derive(Clone)]
pub(crate) struct ExecutionFeed {
    inner: Arc<FeedInner>,
}

impl fmt::Debug for ExecutionFeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExecutionFeed")
    }
}

impl ExecutionFeed {
    /// `secrets`: giá trị API key đã cấu hình, che trong mọi text ephemeral.
    pub(crate) fn new(capacity: usize, delta_max_bytes: usize, secrets: Vec<String>) -> Self {
        let secrets = secrets
            .into_iter()
            .filter(|s| s.len() >= MIN_SECRET_LEN)
            .collect();
        Self {
            inner: Arc::new(FeedInner {
                tx: broadcast::channel(capacity).0,
                closed: CancellationToken::new(),
                secrets,
                delta_max_bytes,
                #[cfg(feature = "test-support")]
                fault: None,
            }),
        }
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn with_fault(self, fault: Arc<FeedFault>) -> Self {
        let inner = Arc::try_unwrap(self.inner)
            .unwrap_or_else(|_| unreachable!("the feed is configured before it is shared"));
        Self {
            inner: Arc::new(FeedInner {
                fault: Some(fault),
                ..inner
            }),
        }
    }

    pub(crate) fn subscribe(&self, root_session_id: &str) -> Subscription {
        Subscription::new(
            self.inner.tx.subscribe(),
            root_session_id,
            self.inner.closed.clone(),
        )
    }

    /// Kết thúc mọi subscription (shutdown). Event đã xếp hàng vẫn được trả hết trước.
    pub(crate) fn close(&self) {
        self.inner.closed.cancel();
    }

    pub(crate) fn delta_max_bytes(&self) -> usize {
        self.inner.delta_max_bytes
    }

    pub(crate) fn redact(&self, text: &str) -> String {
        redact_all(&self.inner.secrets, text)
    }

    pub(crate) fn delta_filter(&self) -> DeltaFilter {
        DeltaFilter::new(self.inner.secrets.clone())
    }

    /// Không bao giờ chờ subscriber; không có receiver thì event bị bỏ (không queue bền vững).
    pub(crate) fn publish(&self, mut event: ExecutionEvent) {
        redact_value(&self.inner.secrets, &mut event.payload);
        #[cfg(feature = "test-support")]
        if let Some(fault) = &self.inner.fault {
            for e in fault.intercept(event) {
                let _ = self.inner.tx.send(e);
            }
            return;
        }
        let _ = self.inner.tx.send(event);
    }

    pub(crate) fn ephemeral(
        &self,
        root_session_id: &str,
        session_id: &str,
        run_id: Option<&str>,
        event_type: &str,
        payload: Value,
    ) {
        self.publish(ExecutionEvent {
            event_type: event_type.to_owned(),
            root_session_id: root_session_id.to_owned(),
            session_id: session_id.to_owned(),
            run_id: run_id.map(str::to_owned),
            state_revision: None,
            timestamp_ms: now_ms(),
            payload,
        });
    }
}
