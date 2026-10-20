//! Managed supervisor (docs/18 §18.5 "Failure ordering của submit", EMB-17).
//!
//! Giữ owned task handles theo `run_id` (idempotent, không duplicate), reconcile các
//! durable claim chưa có owner trong process: sau admission, khi worker kết thúc, và qua
//! sweep định kỳ. Claim hụt sau commit/trước spawn nên không bị kẹt `running` vô hạn.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::error::{ErrorCode, OrchestratorError};
use crate::owner_lock::OwnerLock;
use crate::runtime::AgentRuntime;
use crate::store::SessionStore;

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

struct Shared {
    store: SessionStore,
    runtime: AgentRuntime,
    owned: Mutex<HashMap<String, JoinHandle<()>>>,
    signal: AdmissionSignal,
    cancel: CancellationToken,
    sweep: Duration,
    /// Chỉ nhả sau khi mọi local write-capable task đã join (hoặc khi Shared bị drop).
    owner_lock: Mutex<Option<OwnerLock>>,
}

pub(crate) struct Supervisor {
    shared: Arc<Shared>,
    loop_task: Mutex<Option<JoinHandle<()>>>,
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
        owner_lock: OwnerLock,
        signal: AdmissionSignal,
    ) -> Self {
        let shared = Arc::new(Shared {
            store,
            runtime,
            owned: Mutex::new(HashMap::new()),
            signal,
            cancel: CancellationToken::new(),
            sweep,
            owner_lock: Mutex::new(Some(owner_lock)),
        });
        let handle = tokio::spawn(Arc::clone(&shared).run_loop());
        Self {
            shared,
            loop_task: Mutex::new(Some(handle)),
        }
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shared.cancel.is_cancelled()
    }

    /// Đánh thức supervisor sau khi một claim vừa commit.
    pub(crate) fn notify_admitted(&self) {
        self.shared.signal.admitted();
    }

    /// Dừng admission/loop, hủy hợp tác các run, join trong `grace`, đóng store rồi nhả
    /// owner lock. Quá grace: trả lỗi và **không** nhả lock. Idempotent.
    pub(crate) async fn shutdown(&self, grace: Duration) -> Result<(), OrchestratorError> {
        self.shared.cancel.cancel();
        let loop_handle = lock(&self.loop_task).take();
        let shared = Arc::clone(&self.shared);
        let drain = async move {
            if let Some(h) = loop_handle {
                let _ = h.await;
            }
            loop {
                let handles: Vec<JoinHandle<()>> =
                    lock(&shared.owned).drain().map(|(_, h)| h).collect();
                if handles.is_empty() {
                    break;
                }
                for h in handles {
                    let _ = h.await;
                }
            }
        };
        if tokio::time::timeout(grace, drain).await.is_err() {
            return Err(OrchestratorError::new(
                ErrorCode::Unavailable,
                "shutdown grace expired while tasks were still running; store stays owned",
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

    /// Spawn đúng một worker cho mỗi durable claim `running` chưa có owner.
    async fn reconcile(self: &Arc<Self>) {
        if self.cancel.is_cancelled() {
            return;
        }
        let ids = match self.store.running_run_ids().await {
            Ok(ids) => ids,
            Err(_) => {
                warn!("reconcile could not list running runs; will retry on next sweep");
                return;
            }
        };
        let mut owned = lock(&self.owned);
        // Handle đã kết thúc mà chưa tự xóa (vd. worker panic) không còn là owner hợp lệ.
        owned.retain(|_, h| !h.is_finished());
        for id in ids {
            if owned.contains_key(&id) {
                continue;
            }
            let me = Arc::clone(self);
            let run_id = id.clone();
            // Spawn khi đang giữ lock: worker xóa entry của chính nó cũng cần lock này nên
            // luôn thấy entry đã được insert.
            let handle = tokio::spawn(async move {
                let settled = me.runtime.execute(&run_id, &me.cancel).await;
                lock(&me.owned).remove(&run_id);
                if settled {
                    // Có thể còn claim khác đang chờ owner.
                    me.signal.wake.notify_one();
                }
            });
            owned.insert(id, handle);
        }
    }
}
