//! Managed supervisor (docs/18 §18.5 "Failure ordering của submit", EMB-17).
//!
//! Giữ owned task handles theo `run_id` (idempotent, không duplicate), reconcile các
//! durable claim chưa có owner trong process: sau admission, khi worker kết thúc, và qua
//! sweep định kỳ. Claim hụt sau commit/trước spawn nên không bị kẹt `running` vô hạn.
//!
//! M5: mỗi worker có `CancellationToken` riêng (con của token shutdown) để `cancel` một run mà
//! không ảnh hưởng run khác; shutdown là giao thức nhiều pha (docs/18 §18.9): đóng admission →
//! drain có hạn → cancel hợp tác → quiesce/join → đóng store → nhả owner lock.
//!
//! Worker trả `Unsettled` (không commit được, run vẫn `running`) thì run đó chỉ được spawn lại
//! sau backoff mũ; quá [`MAX_UNSETTLED_ATTEMPTS`] lần liên tiếp thì runtime chuyển run sang
//! `interrupted/STORE_UNAVAILABLE` để không gọi lại model vô hạn khi store ghi lỗi kéo dài.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::error::{ErrorCode, OrchestratorError};
use crate::owner_lock::OwnerLock;
use crate::runtime::{AgentRuntime, Settlement};
use crate::store::SessionStore;

/// Số lần `Unsettled` liên tiếp của một run trước khi bỏ cuộc (mỗi lần có thể là một lần gọi
/// model). Run bỏ cuộc vẫn phục hồi được ở lần khởi động sau.
const MAX_UNSETTLED_ATTEMPTS: u32 = 5;
/// Trần backoff tính theo bội số của chu kỳ sweep.
const MAX_BACKOFF_FACTOR: u32 = 16;

/// Kênh đánh thức supervisor sau khi một claim vừa commit (root submit hoặc child admission).
/// Clone được chia sẻ giữa `Orchestrator` (submit) và `TaskManager` (child admission).
#[derive(Clone)]
pub(crate) struct AdmissionSignal {
    wake: Arc<Notify>,
    /// Fault hook: bỏ qua trigger sau admission (mô phỏng crash window commit→spawn).
    drop_triggers: Arc<AtomicBool>,
}

impl AdmissionSignal {
    pub(crate) fn new(drop_triggers: Arc<AtomicBool>) -> Self {
        Self {
            wake: Arc::new(Notify::new()),
            drop_triggers,
        }
    }

    /// Gọi sau khi một claim vừa commit. `Notify` giữ permit nên trigger không bị mất nếu
    /// loop đang bận; claim bị bỏ trigger vẫn được sweep định kỳ cứu.
    pub(crate) fn admitted(&self) {
        if self.drop_triggers.load(Ordering::Acquire) {
            debug!("admission trigger suppressed by fault hook");
            return;
        }
        self.wake.notify_one();
    }
}

/// Worker đang sở hữu một run: handle để join và token để hủy riêng run đó.
struct Owned {
    handle: JoinHandle<()>,
    cancel: CancellationToken,
    /// Root run (không có parent): tính vào `max_root_runs`.
    root: bool,
}

/// Run vừa kết thúc `Unsettled`: số lần liên tiếp và thời điểm sớm nhất được spawn lại.
struct Retry {
    failures: u32,
    not_before: Instant,
}

struct Shared {
    store: SessionStore,
    runtime: AgentRuntime,
    owned: Mutex<HashMap<String, Owned>>,
    /// Backoff của run `Unsettled` (in-memory; restart dùng recovery bounded thay thế).
    retries: Mutex<HashMap<String, Retry>>,
    signal: AdmissionSignal,
    /// Token shutdown: hủy hợp tác mọi run và dừng loop.
    cancel: CancellationToken,
    /// Báo khi một worker rời `owned` (phục vụ chờ idle trong shutdown).
    idle: Notify,
    sweep: Duration,
    /// `RuntimeConfig::max_concurrent_root_runs`; child không bị tính.
    max_root_runs: Option<usize>,
    /// Chỉ nhả sau khi mọi local write-capable task đã join (hoặc khi Shared bị drop).
    owner_lock: Mutex<Option<OwnerLock>>,
}

pub(crate) struct Supervisor {
    shared: Arc<Shared>,
    loop_task: Mutex<Option<JoinHandle<()>>>,
    /// Admission mới (root `submit`/`create_session`) đã bị đóng bởi shutdown.
    closed: AtomicBool,
    /// Read guard giữ suốt một admission đang chạy; shutdown lấy write để chờ chúng xong, nên
    /// `submit` sau shutdown bị từ chối **trước** commit và không có claim nào lọt sau đó.
    gate: RwLock<()>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl Supervisor {
    /// Phải gọi trong Tokio runtime của host; không tạo runtime.
    pub(crate) fn start(
        store: SessionStore,
        runtime: AgentRuntime,
        sweep: Duration,
        max_root_runs: Option<usize>,
        owner_lock: OwnerLock,
        signal: AdmissionSignal,
        shutdown: CancellationToken,
    ) -> Self {
        let shared = Arc::new(Shared {
            store,
            runtime,
            owned: Mutex::new(HashMap::new()),
            retries: Mutex::new(HashMap::new()),
            signal,
            cancel: shutdown,
            idle: Notify::new(),
            sweep,
            max_root_runs,
            owner_lock: Mutex::new(Some(owner_lock)),
        });
        let handle = tokio::spawn(Arc::clone(&shared).run_loop());
        Self {
            shared,
            loop_task: Mutex::new(Some(handle)),
            closed: AtomicBool::new(false),
            gate: RwLock::new(()),
        }
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.closed.load(Ordering::Acquire) || self.shared.cancel.is_cancelled()
    }

    /// Cổng admission: giữ guard suốt thao tác ghi để shutdown không đóng giữa chừng.
    /// `SHUTTING_DOWN` nếu đã đóng — trước khi có bất kỳ commit nào.
    pub(crate) async fn admission(
        &self,
    ) -> Result<tokio::sync::RwLockReadGuard<'_, ()>, OrchestratorError> {
        let guard = self.gate.read().await;
        if self.is_shutting_down() {
            return Err(OrchestratorError::new(
                ErrorCode::ShuttingDown,
                "the orchestrator is shutting down",
            ));
        }
        Ok(guard)
    }

    /// Hủy hợp tác đúng run `run_id` nếu worker đang thuộc process này. `false`: chưa có
    /// worker (claim chưa spawn) — caller tự ghi terminal state vào store.
    pub(crate) fn cancel_run(&self, run_id: &str) -> bool {
        match lock(&self.shared.owned).get(run_id) {
            Some(owned) => {
                owned.cancel.cancel();
                true
            }
            None => false,
        }
    }

    /// Đánh thức supervisor sau khi một claim vừa commit.
    pub(crate) fn notify_admitted(&self) {
        self.shared.signal.admitted();
    }

    /// Giao thức shutdown (docs/18 §18.9). Idempotent và gọi lại được sau khi trả lỗi.
    ///
    /// Đóng admission (chờ admission đang chạy xong); drain có hạn `drain` (run đang chạy tự
    /// kết thúc, claim mới của child vẫn được spawn); cancel hợp tác mọi run; quiesce/join
    /// trong `grace`; đóng store rồi mới nhả owner lock. Quá `grace` mà còn worker có thể ghi:
    /// trả lỗi và **giữ** owner lock.
    pub(crate) async fn shutdown(
        &self,
        drain: Duration,
        grace: Duration,
    ) -> Result<(), OrchestratorError> {
        self.closed.store(true, Ordering::Release);
        drop(self.gate.write().await);

        if !drain.is_zero() && !self.shared.cancel.is_cancelled() {
            // Claim đã commit mà chưa spawn phải có owner trước khi chờ idle.
            self.shared.reconcile().await;
            let _ = tokio::time::timeout(drain, self.shared.wait_idle()).await;
        }
        self.shared.cancel.cancel();
        // Admission lọt vào ngay trước khi đóng: cho nó một worker để bị cancel đúng cách
        // (interrupted/SHUTTING_DOWN) thay vì để claim `running` không owner.
        self.shared.spawn_unowned(true).await;

        let loop_handle = lock(&self.loop_task).take();
        let shared = Arc::clone(&self.shared);
        let quiesce = async move {
            if let Some(h) = loop_handle {
                let _ = h.await;
            }
            shared.wait_idle().await;
        };
        if tokio::time::timeout(grace, quiesce).await.is_err() {
            return Err(OrchestratorError::new(
                ErrorCode::Unavailable,
                "shutdown is incomplete: a local task is still running; the store stays owned",
            ));
        }
        self.shared.store.close().await;
        drop(lock(&self.shared.owner_lock).take());
        Ok(())
    }
}

impl Drop for Supervisor {
    /// Drop không hứa graceful shutdown; chỉ yêu cầu dừng. Owner lock nằm trong `Shared`
    /// nên chỉ được nhả khi task cuối cùng thoát.
    fn drop(&mut self) {
        self.shared.cancel.cancel();
    }
}

impl Shared {
    /// Ghi nhận kết quả của worker **trước** khi nó rời `owned`, để lần reconcile kế tiếp
    /// thấy backoff. Quá [`MAX_UNSETTLED_ATTEMPTS`] lần liên tiếp thì bỏ cuộc (terminal
    /// `interrupted`); nếu cả việc đó cũng không ghi được thì tiếp tục backoff ở mức trần.
    async fn record_settlement(&self, run_id: &str, settlement: Settlement) {
        if settlement == Settlement::Terminal {
            lock(&self.retries).remove(run_id);
            return;
        }
        let failures = lock(&self.retries)
            .get(run_id)
            .map_or(0, |r| r.failures)
            .saturating_add(1);
        let settlement = if failures >= MAX_UNSETTLED_ATTEMPTS {
            warn!(
                run_id,
                failures, "run keeps failing to record progress; giving up"
            );
            self.runtime.give_up(run_id).await
        } else {
            debug!(run_id, failures, "run unsettled; retrying after backoff");
            settlement
        };
        let mut retries = lock(&self.retries);
        if settlement == Settlement::Terminal {
            retries.remove(run_id);
        } else {
            let factor = 1u32
                .checked_shl(failures.saturating_sub(1))
                .unwrap_or(u32::MAX)
                .min(MAX_BACKOFF_FACTOR);
            retries.insert(
                run_id.to_owned(),
                Retry {
                    failures,
                    not_before: Instant::now() + self.sweep.saturating_mul(factor),
                },
            );
        }
    }

    async fn run_loop(self: Arc<Self>) {
        self.reconcile().await;
        let mut tick = tokio::time::interval_at(Instant::now() + self.sweep, self.sweep);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => break,
                () = self.signal.wake.notified() => {}
                _ = tick.tick() => {}
            }
            self.reconcile().await;
        }
    }

    /// Chờ tới khi không còn worker nào (kể cả worker đang ghi). Không bao giờ bỏ worker.
    async fn wait_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if lock(&self.owned).is_empty() {
                return;
            }
            // Poll dự phòng: worker panic có thể không kịp báo.
            tokio::select! {
                () = &mut notified => {}
                () = tokio::time::sleep(Duration::from_millis(50)) => {
                    lock(&self.owned).retain(|_, o| !o.handle.is_finished());
                }
            }
        }
    }

    /// Spawn đúng một worker cho mỗi durable claim `running` chưa có owner.
    async fn reconcile(self: &Arc<Self>) {
        if self.cancel.is_cancelled() {
            return;
        }
        self.spawn_unowned(false).await;
    }

    /// Như `reconcile` nhưng cũng chạy khi đã shutdown (để claim còn lại được cancel đúng).
    /// `shutting_down`: bỏ qua backoff và giới hạn concurrency, vì shutdown cần mọi claim có
    /// owner để kết thúc đúng cách (`interrupted/SHUTTING_DOWN`).
    async fn spawn_unowned(self: &Arc<Self>, shutting_down: bool) {
        let claims = match self.store.running_claims().await {
            Ok(claims) => claims,
            Err(_) => {
                warn!("reconcile could not list running runs; will retry on next sweep");
                return;
            }
        };
        let now = Instant::now();
        let mut owned = lock(&self.owned);
        let mut retries = lock(&self.retries);
        // Run không còn `running` thì không cần giữ backoff.
        retries.retain(|id, _| claims.iter().any(|(c, _)| c == id));
        // Handle đã kết thúc mà chưa tự xóa (vd. worker panic) không còn là owner hợp lệ.
        owned.retain(|_, o| !o.handle.is_finished());
        let mut active_roots = owned.values().filter(|o| o.root).count();
        for (id, is_child) in claims {
            if owned.contains_key(&id) {
                continue;
            }
            if !shutting_down {
                if retries.get(&id).is_some_and(|r| r.not_before > now) {
                    continue;
                }
                // Claim vượt giới hạn vẫn durable và chờ slot theo thứ tự admission.
                if !is_child && self.max_root_runs.is_some_and(|max| active_roots >= max) {
                    continue;
                }
            }
            if !is_child {
                active_roots += 1;
            }
            let me = Arc::clone(self);
            let run_id = id.clone();
            let token = self.cancel.child_token();
            let worker_token = token.clone();
            // Spawn khi đang giữ lock: worker xóa entry của chính nó cũng cần lock này nên
            // luôn thấy entry đã được insert.
            let handle = tokio::spawn(async move {
                let settlement = me.runtime.execute(&run_id, &worker_token).await;
                me.record_settlement(&run_id, settlement).await;
                lock(&me.owned).remove(&run_id);
                me.idle.notify_waiters();
                // Slot vừa trống: claim khác có thể đang chờ owner. Run vừa `Unsettled` không
                // bị spawn lại ngay vì đang trong backoff.
                me.signal.wake.notify_one();
            });
            owned.insert(
                id,
                Owned {
                    handle,
                    cancel: token,
                    root: !is_child,
                },
            );
        }
    }
}
