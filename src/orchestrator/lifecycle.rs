//! Vòng đời engine: operator resolve invocation, shutdown, kiểm tra trạng thái.

use crate::error::{ErrorCode, OrchestratorError};
use crate::executor::Settled;
use crate::feed::event;
use crate::store::{RecoveryPolicy, Resolution, RunRoute, CODE_NO_EFFECT};
use crate::types::{OperatorContext, VerifiedToolResolution};

use super::{route_of, Orchestrator};

/// Trần độ dài evidence/operator id trong audit (ký tự).
const EVIDENCE_MAX_CHARS: usize = 2000;
const OPERATOR_ID_MAX_CHARS: usize = 128;

impl Orchestrator {
    /// Operator-only: reconcile invocation `interrupted` (outcome không xác định) bằng kết luận
    /// đã xác minh. Chỉ nhận **internal** `invocation_id`. Tool-result, terminal update và audit
    /// commit trong một transaction **trước** khi supervisor resume; không bao giờ replay tool.
    /// `ConfirmedNoEffect` thành technical failure. Resume bị chặn tiếp (vd. routing identity đổi,
    /// còn invocation khác chưa resolve) thì resolution vẫn được ghi và trạng thái hiện trong
    /// `result`.
    pub async fn resolve_interrupted_invocation(
        &self,
        operator: &OperatorContext,
        session_id: &str,
        invocation_id: &str,
        decision: VerifiedToolResolution,
    ) -> Result<(), OrchestratorError> {
        // Resolution có thể tạo attempt mới: giữ cổng admission để shutdown không đóng giữa
        // chừng và không có claim mồ côi sau khi supervisor đã dừng.
        let _gate = self.supervisor.admission().await?;
        if !operator.can_resolve_interrupted {
            return Err(OrchestratorError::new(
                ErrorCode::PermissionDenied,
                "the operator is not allowed to resolve interrupted invocations",
            ));
        }
        let invalid = |m: &str| OrchestratorError::new(ErrorCode::InvalidInput, m.to_owned());
        let clean = |s: &str, max: usize| {
            let t = s.trim();
            !t.is_empty() && t.chars().count() <= max && !t.chars().any(char::is_control)
        };
        if !clean(&operator.operator_id, OPERATOR_ID_MAX_CHARS) {
            return Err(invalid("operator_id must be 1..=128 printable characters"));
        }
        let (outcome_call_id, settled, evidence) = match decision {
            VerifiedToolResolution::ConfirmedCompleted { outcome, evidence } => {
                if let Some(ids) = outcome.metadata.get("artifact_ids") {
                    let Some(list) = ids.as_array() else {
                        return Err(invalid("metadata.artifact_ids must be an array of strings"));
                    };
                    let root_sid = self
                        .store
                        .root_session_of(&operator.owner_id, &operator.workspace_id, session_id)
                        .await?;
                    for entry in list {
                        let Some(aid) = entry.as_str().map(str::trim).filter(|s| !s.is_empty())
                        else {
                            return Err(invalid(
                                "metadata.artifact_ids entries must be non-empty strings",
                            ));
                        };
                        self.artifacts
                            .verify_in_family(
                                &operator.owner_id,
                                &operator.workspace_id,
                                &root_sid,
                                aid,
                            )
                            .await
                            .map_err(|_| {
                                invalid(
                                    "confirmed outcome references an uncommitted, unauthorized, or corrupt artifact",
                                )
                            })?;
                    }
                }
                let call_id = outcome.call_id.clone();
                let settled = self.limits.settle(Ok(outcome));
                if settled.failed {
                    return Err(invalid(
                        "the confirmed outcome exceeds the persistence limits",
                    ));
                }
                (Some(call_id), settled, evidence)
            }
            VerifiedToolResolution::ConfirmedNoEffect { evidence } => (
                None,
                Settled::failure(
                    CODE_NO_EFFECT,
                    "the operator confirmed the tool had no effect; it was not replayed",
                ),
                evidence,
            ),
        };
        if !clean(&evidence, EVIDENCE_MAX_CHARS) {
            return Err(invalid("evidence must be 1..=2000 printable characters"));
        }
        self.store
            .resolve_invocation(
                (&operator.owner_id, &operator.workspace_id),
                session_id,
                invocation_id,
                &Resolution {
                    operator_id: operator.operator_id.trim().to_owned(),
                    evidence: evidence.trim().to_owned(),
                    outcome_call_id,
                    settled,
                },
            )
            .await?;
        tracing::info!(
            session_id,
            invocation_id,
            operator_id = %operator.operator_id.trim(),
            "interrupted invocation resolved by an operator"
        );
        self.emitter
            .committed_session(
                session_id,
                None,
                event::SESSION_CHANGED,
                serde_json::json!({
                    "reason": "invocation_resolved",
                    "invocation_id": invocation_id,
                }),
            )
            .await;
        let route = |agent_id: &str| route_of(self.registry.get(agent_id)?, &self.fingerprints);
        let replay_safe = |tool: &str| self.replay_safe.contains(tool);
        let policy = RecoveryPolicy {
            max_resume_attempt: self.runtime.max_resume_attempts,
            route: &route,
            replay_safe: &replay_safe,
        };
        match self
            .store
            .recover_after_resolution(session_id, &policy)
            .await
        {
            Ok(report) if report.resumed > 0 => self.supervisor.notify_admitted(),
            Ok(_) => {}
            // Resolution đã commit; lần startup sau sẽ tự phục hồi.
            Err(_) => tracing::warn!(session_id, "resume after resolution could not be recorded"),
        }
        Ok(())
    }

    /// Dừng admission, drain có hạn, hủy hợp tác, quiesce/join rồi mới đóng store và nhả owner
    /// lock. Quá `shutdown_grace` mà còn task có thể ghi: trả lỗi **incomplete** và giữ lock.
    /// Run bị ngắt là `interrupted` (phục hồi ở lần khởi động sau); không giả completed/cancelled.
    pub async fn shutdown(&self) -> Result<(), OrchestratorError> {
        self.supervisor
            .shutdown(self.runtime.shutdown_drain, self.runtime.shutdown_grace)
            .await?;
        // Mọi worker đã dừng và đã publish xong: kết thúc các subscription (event đã xếp hàng
        // vẫn được giao trước).
        self.feed.close();
        Ok(())
    }

    pub(super) fn ensure_open(&self) -> Result<(), OrchestratorError> {
        if self.supervisor.is_shutting_down() {
            return Err(OrchestratorError::new(
                ErrorCode::ShuttingDown,
                "the orchestrator is shutting down",
            ));
        }
        Ok(())
    }

    pub(super) fn route(&self, agent_id: &str) -> Option<RunRoute> {
        route_of(self.registry.get(agent_id)?, &self.fingerprints)
    }
}
