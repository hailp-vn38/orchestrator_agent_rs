//! `ExecutionFeed` (docs/16): fan-out **best-effort** của sự kiện quan sát, transport-neutral.
//!
//! - SQLite là source of truth; feed chỉ là gợi ý live. Event committed (`state_revision`
//!   `Some`) chỉ được publish **sau** commit và chỉ *invalidate* canonical view — payload không
//!   phải patch. Event ephemeral (`text.delta`, `tool.progress`, `step.*`, `compaction.started`)
//!   có thể mất và không bao giờ ghi đè terminal state từ snapshot.
//! - Một `tokio::sync::broadcast` bounded cho cả process: publisher không bao giờ chờ subscriber;
//!   subscriber chậm nhận `Lagged` → [`FeedItem::ResyncRequired`] rồi bị đóng riêng.
//! - Không có event log/replay: không `Last-Event-ID`, revision không phải resume cursor.
//! - Payload không chứa arguments/kết quả tool, secret hay reasoning ẩn: gateway chỉ chuyển text
//!   đã hiển thị, và giá trị API key đã cấu hình bị che trong mọi text ephemeral.

use std::fmt;
#[cfg(feature = "test-support")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::store::SessionStore;
use crate::types::{InvocationStatus, RunStatus};

/// Thay cho giá trị secret bị che.
const REDACTED: &str = "[redacted]";
/// Secret ngắn hơn ngưỡng này không bị che: che chuỗi 1–2 ký tự sẽ phá nội dung hợp lệ.
const MIN_SECRET_LEN: usize = 8;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
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

/// Một phần tử của [`Subscription`].
#[derive(Debug, Clone, PartialEq)]
pub enum FeedItem {
    Event(ExecutionEvent),
    /// Subscriber đã mất event (broadcast `Lagged`). Subscription này kết thúc ngay sau đó:
    /// bỏ preview chưa xác nhận, mở subscription mới **trước** rồi lấy snapshot mới.
    ResyncRequired,
}

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

    fn intercept(&self, event: ExecutionEvent) -> Vec<ExecutionEvent> {
        const TERMINAL: &[&str] = &[
            "run.completed",
            "run.failed",
            "run.cancelled",
            "run.interrupted",
            "tool.completed",
            "tool.failed",
            "tool.interrupted",
        ];
        if !event.is_committed() {
            return vec![event];
        }
        let final_message =
            event.event_type == "message.committed" && event.payload["final"] == json!(true);
        if self.suppress_terminal.load(Ordering::Acquire)
            && (final_message || TERMINAL.contains(&event.event_type.as_str()))
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

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
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
        Subscription {
            rx: self.inner.tx.subscribe(),
            root_session_id: root_session_id.to_owned(),
            closed: self.inner.closed.clone(),
            finished: false,
        }
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
    pub(crate) fn publish(&self, event: ExecutionEvent) {
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

fn redact_all(secrets: &[String], text: &str) -> String {
    let mut out = text.to_owned();
    for secret in secrets {
        if out.contains(secret.as_str()) {
            out = out.replace(secret.as_str(), REDACTED);
        }
    }
    out
}

/// Che secret trong một stream text kể cả khi secret bị cắt qua nhiều fragment: giữ lại phần
/// đuôi (độ dài secret dài nhất − 1 byte) cho tới khi fragment kế tiếp tới hoặc `flush`.
pub(crate) struct DeltaFilter {
    secrets: Vec<String>,
    hold: usize,
    pending: String,
}

impl DeltaFilter {
    fn new(secrets: Vec<String>) -> Self {
        let hold = secrets.iter().map(String::len).max().map_or(0, |n| n - 1);
        Self {
            secrets,
            hold,
            pending: String::new(),
        }
    }

    /// Phần text đã an toàn để phát ngay (có thể rỗng khi đang giữ đuôi).
    pub(crate) fn push(&mut self, fragment: &str) -> String {
        if self.secrets.is_empty() {
            return fragment.to_owned();
        }
        self.pending.push_str(fragment);
        let redacted = redact_all(&self.secrets, &self.pending);
        if redacted.len() <= self.hold {
            self.pending = redacted;
            return String::new();
        }
        let mut cut = redacted.len() - self.hold;
        while !redacted.is_char_boundary(cut) {
            cut -= 1;
        }
        self.pending = redacted[cut..].to_owned();
        redacted[..cut].to_owned()
    }

    /// Phát nốt phần đang giữ (cuối stream hoặc khi stream lỗi).
    pub(crate) fn flush(&mut self) -> String {
        redact_all(&self.secrets, &std::mem::take(&mut self.pending))
    }
}

/// Cắt `text` thành các phần ≤ `max` byte tại ranh giới ký tự, giữ nguyên thứ tự.
pub(crate) fn chunks(text: &str, max: usize) -> Vec<&str> {
    let max = max.max(4);
    let mut out = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let mut end = rest.len().min(max);
        while !rest.is_char_boundary(end) {
            end -= 1;
        }
        let (head, tail) = rest.split_at(end);
        out.push(head);
        rest = tail;
    }
    out
}

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
    pub(crate) fn send(&self, message: &str) {
        {
            let mut last = lock(&self.last);
            let now = Instant::now();
            if last.is_some_and(|t| now.duration_since(t) < self.min_interval) {
                return;
            }
            *last = Some(now);
        }
        self.feed.ephemeral(
            &self.root_session_id,
            &self.session_id,
            Some(&self.run_id),
            "tool.progress",
            json!({
                "tool_call_id": self.tool_call_id,
                "invocation_id": self.invocation_id,
                "message": self.feed.redact(message),
            }),
        );
    }
}

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
                let (event_type, payload) = if inv.status == InvocationStatus::Cancelled {
                    (
                        "tool.failed",
                        json!({
                            "tool_call_id": inv.tool_call_id,
                            "invocation_id": inv.id,
                            "tool_name": inv.tool_name,
                            "status": "cancelled",
                        }),
                    )
                } else {
                    (
                        "tool.interrupted",
                        json!({
                            "tool_call_id": inv.tool_call_id,
                            "invocation_id": inv.id,
                            "tool_name": inv.tool_name,
                            "error_code": inv.error_code,
                        }),
                    )
                };
                self.committed_run(run_id, event_type, payload).await;
            }
        }
        let event_type = match status {
            RunStatus::Completed => "run.completed",
            RunStatus::Cancelled => "run.cancelled",
            RunStatus::Interrupted => "run.interrupted",
            _ => "run.failed",
        };
        self.committed_run(
            run_id,
            event_type,
            json!({ "status": status.as_str(), "error_code": error_code }),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_respect_char_boundaries_and_order() {
        let parts = chunks("aé😀bcd", 4);
        assert_eq!(parts.concat(), "aé😀bcd");
        assert!(parts.iter().all(|p| p.len() <= 4));
    }

    #[test]
    fn delta_filter_redacts_secret_split_across_fragments() {
        let secret = "sk-secret-value-123";
        let mut f = DeltaFilter::new(vec![secret.to_owned()]);
        let mut out = String::new();
        for frag in ["key is sk-sec", "ret-val", "ue-123 ok ", "bye"] {
            out.push_str(&f.push(frag));
        }
        out.push_str(&f.flush());
        assert_eq!(out, "key is [redacted] ok bye");
        assert!(!out.contains("secret"));
    }

    #[test]
    fn delta_filter_is_transparent_without_secrets() {
        let mut f = DeltaFilter::new(Vec::new());
        assert_eq!(f.push("hello"), "hello");
        assert_eq!(f.flush(), "");
    }

    #[test]
    fn short_secrets_are_not_redacted() {
        let feed = ExecutionFeed::new(8, 64, vec!["abc".into()]);
        assert_eq!(feed.redact("abc"), "abc");
    }

    #[tokio::test]
    async fn lagged_subscriber_resyncs_and_ends_without_blocking_publisher() {
        let feed = ExecutionFeed::new(2, 64, Vec::new());
        let mut slow = feed.subscribe("root");
        for i in 0..10 {
            feed.ephemeral("root", "root", None, "text.delta", json!({ "i": i }));
        }
        assert_eq!(slow.recv().await, Some(FeedItem::ResyncRequired));
        assert_eq!(slow.recv().await, None);
    }

    #[tokio::test]
    async fn subscription_only_sees_its_own_family() {
        let feed = ExecutionFeed::new(8, 64, Vec::new());
        let mut a = feed.subscribe("a");
        feed.ephemeral("b", "b", None, "text.delta", json!({}));
        feed.ephemeral("a", "a", None, "text.delta", json!({}));
        match a.recv().await {
            Some(FeedItem::Event(e)) => assert_eq!(e.root_session_id, "a"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn closing_the_feed_drains_queued_events_then_ends() {
        let feed = ExecutionFeed::new(8, 64, Vec::new());
        let mut a = feed.subscribe("a");
        feed.ephemeral("a", "a", None, "text.delta", json!({}));
        feed.close();
        assert!(matches!(a.recv().await, Some(FeedItem::Event(_))));
        assert_eq!(a.recv().await, None);
    }
}
