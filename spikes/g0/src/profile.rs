//! Deployment profile: concrete values + units + over-cap behaviour. All values here are
//! DECISIONS (estimated), not measurements, except where `store` probes measure them.

use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct Profile {
    pub persist_result_hard_cap_bytes: u64, // over cap: reject outcome as tool_error, store truncated marker
    pub model_visible_cap_bytes: u64, // over cap: truncate with marker; full result stays in store
    pub event_payload_cap_bytes: u64, // over cap: replace payload by invalidation-only event
    pub progress_throttle: Duration,  // coalesce progress events
    pub broadcast_capacity: usize,    // lagging subscriber gets Lagged -> must refresh snapshot
    pub page_size: u32,               // history/list page; larger request clamped
    pub download_max_bytes: u64,      // bounded download; over cap aborts with stable error
    pub artifact_max_bytes: u64,      // over cap: reject write
    pub owner_workspace_quota_bytes: u64, // over quota: reject new artifact
    pub disk_reserve_bytes: u64,      // free space below reserve: reject new artifact
    pub llm_request_timeout: Duration, // per-request; maps to llm_timeout
    pub tool_timeout: Duration,       // per-tool operation; maps to tool_timeout
    pub max_recovery_attempts: u32,   // over: block turn
    pub supervisor_sweep: Duration,
    pub snapshot_refresh: Duration,
    pub shutdown_grace: Duration, // after grace with writer alive: shutdown returns error
    pub usage_policy: UsagePolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsagePolicy {
    /// Record provider usage when present, otherwise mark estimated; no hard cost quota in MVP.
    RecordMeasuredElseEstimated,
}

const MIB: u64 = 1024 * 1024;

impl Default for Profile {
    fn default() -> Self {
        Self {
            persist_result_hard_cap_bytes: 8 * MIB,
            model_visible_cap_bytes: 64 * 1024,
            event_payload_cap_bytes: 16 * 1024,
            progress_throttle: Duration::from_millis(250),
            broadcast_capacity: 1024,
            page_size: 50,
            download_max_bytes: 256 * MIB,
            artifact_max_bytes: 128 * MIB,
            owner_workspace_quota_bytes: 4 * 1024 * MIB,
            disk_reserve_bytes: 1024 * MIB,
            llm_request_timeout: Duration::from_secs(120),
            tool_timeout: Duration::from_secs(300),
            max_recovery_attempts: 10,
            supervisor_sweep: Duration::from_secs(30),
            snapshot_refresh: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(30),
            usage_policy: UsagePolicy::RecordMeasuredElseEstimated,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ProfileError(pub &'static str);

impl Profile {
    pub fn validate(&self) -> Result<(), ProfileError> {
        let e = |m| Err(ProfileError(m));
        if self.model_visible_cap_bytes == 0
            || self.model_visible_cap_bytes > self.persist_result_hard_cap_bytes
        {
            return e("model_visible_cap must be in 1..=persist_result_hard_cap");
        }
        if self.event_payload_cap_bytes == 0
            || self.event_payload_cap_bytes > self.model_visible_cap_bytes
        {
            return e("event_payload_cap must be in 1..=model_visible_cap");
        }
        if self.page_size == 0 || self.page_size > 500 {
            return e("page_size must be 1..=500");
        }
        if self.broadcast_capacity == 0 {
            return e("broadcast_capacity must be > 0");
        }
        if self.artifact_max_bytes == 0
            || self.artifact_max_bytes > self.owner_workspace_quota_bytes
        {
            return e("artifact_max must be in 1..=owner_workspace_quota");
        }
        if self.download_max_bytes < self.artifact_max_bytes {
            return e("download_max must be >= artifact_max");
        }
        if self.llm_request_timeout.is_zero() || self.tool_timeout.is_zero() {
            return e("timeouts must be > 0");
        }
        if self.max_recovery_attempts == 0 {
            return e("max_recovery_attempts must be > 0");
        }
        if self.supervisor_sweep.is_zero() || self.snapshot_refresh.is_zero() {
            return e("sweep/refresh must be > 0");
        }
        if self.shutdown_grace.is_zero() {
            return e("shutdown_grace must be > 0");
        }
        Ok(())
    }

    pub fn clamp_page(&self, requested: u32) -> u32 {
        requested.clamp(1, self.page_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_valid() {
        assert_eq!(Profile::default().validate(), Ok(()));
    }

    #[test]
    fn invalid_boundaries_rejected() {
        let d = Profile::default();
        let bad = [
            Profile {
                model_visible_cap_bytes: 0,
                ..d.clone()
            },
            Profile {
                model_visible_cap_bytes: d.persist_result_hard_cap_bytes + 1,
                ..d.clone()
            },
            Profile {
                event_payload_cap_bytes: d.model_visible_cap_bytes + 1,
                ..d.clone()
            },
            Profile {
                page_size: 0,
                ..d.clone()
            },
            Profile {
                page_size: 501,
                ..d.clone()
            },
            Profile {
                broadcast_capacity: 0,
                ..d.clone()
            },
            Profile {
                artifact_max_bytes: d.owner_workspace_quota_bytes + 1,
                ..d.clone()
            },
            Profile {
                download_max_bytes: d.artifact_max_bytes - 1,
                ..d.clone()
            },
            Profile {
                llm_request_timeout: Duration::ZERO,
                ..d.clone()
            },
            Profile {
                max_recovery_attempts: 0,
                ..d.clone()
            },
            Profile {
                shutdown_grace: Duration::ZERO,
                ..d.clone()
            },
        ];
        for p in bad {
            assert!(p.validate().is_err(), "{p:?}");
        }
    }

    #[test]
    fn valid_edges_accepted_and_page_clamped() {
        let d = Profile::default();
        let edge = Profile {
            page_size: 500,
            model_visible_cap_bytes: 1,
            event_payload_cap_bytes: 1,
            ..d.clone()
        };
        assert_eq!(edge.validate(), Ok(()));
        assert_eq!(d.clamp_page(0), 1);
        assert_eq!(d.clamp_page(10_000), d.page_size);
    }
}
