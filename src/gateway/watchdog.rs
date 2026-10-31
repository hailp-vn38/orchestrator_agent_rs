use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use crate::config::RequestLimits;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Expiry {
    Stalled,
    LimitExceeded,
}

pub(crate) struct Watchdog {
    limits: RequestLimits,
    started: Instant,
    /// Mili giây từ `started` tới tín hiệu gần nhất (+1 để phân biệt mốc 0ms); 0 nghĩa là chưa có tín hiệu.
    last_signal_ms: AtomicU64,
    /// Đánh thức đúng một lần khi nhận tín hiệu đầu tiên (phòng trường hợp `first_byte > idle`).
    first_signal: Notify,
}

impl Watchdog {
    pub(crate) fn new(limits: RequestLimits) -> Self {
        Self {
            limits,
            started: Instant::now(),
            last_signal_ms: AtomicU64::new(0),
            first_signal: Notify::new(),
        }
    }

    pub(crate) fn touch(&self) {
        let elapsed_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX - 1);
        let prev = self
            .last_signal_ms
            .swap(elapsed_ms.saturating_add(1), Ordering::AcqRel);
        if prev == 0 {
            self.first_signal.notify_one();
        }
    }

    /// Chờ tới khi vượt giới hạn. Tính lại mốc sau mỗi lần thức.
    pub(crate) async fn expired(&self) -> Expiry {
        let total_deadline = self.started + self.limits.max_total;
        loop {
            let notified = self.first_signal.notified();
            let raw = self.last_signal_ms.load(Ordering::Acquire);
            let idle_deadline = if raw == 0 {
                self.started + self.limits.first_byte
            } else {
                self.started + Duration::from_millis(raw - 1) + self.limits.idle
            };
            let next_deadline = total_deadline.min(idle_deadline);
            let now = Instant::now();
            if now >= next_deadline {
                let raw_now = self.last_signal_ms.load(Ordering::Acquire);
                if raw_now != raw && now < total_deadline {
                    continue;
                }
                return if idle_deadline <= total_deadline {
                    Expiry::Stalled
                } else {
                    Expiry::LimitExceeded
                };
            }
            if raw == 0 {
                tokio::select! {
                    () = tokio::time::sleep_until(next_deadline) => {}
                    () = notified => continue,
                }
            } else {
                tokio::time::sleep_until(next_deadline).await;
            }
        }
    }
}
