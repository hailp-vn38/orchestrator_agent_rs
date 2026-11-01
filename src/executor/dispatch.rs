//! Chạy invocation, timeout/cancel và xác minh artifact của HostTool.
use super::{
    Dispatch, Effective, Runnable, Settled, ToolExecutor, CODE_OUTCOME_UNKNOWN, CODE_PANICKED,
    CODE_TIMEOUT,
};
use crate::feed::ProgressSink;
use crate::skill::{render, SkillManager};
use crate::subagent::Delegation;
use crate::tool::{
    CancelSignal, HostTool, ToolContext, ToolEffect, ToolError, ToolOutcome, ToolScope,
};
use futures::FutureExt;
use serde_json::{json, Value};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const USER_CANCEL_GRACE: Duration = Duration::from_secs(30);
const SHUTDOWN_CANCEL_GRACE_MAX: Duration = Duration::from_secs(20);

impl ToolExecutor {
    /// Chạy một invocation đã `prepare`. Cancel là hợp tác: tool nhận cờ `is_cancelled` và tự
    /// dừng; executor không bỏ tool đang chạy (shutdown chờ trong grace). Timeout là per-op.
    pub(crate) async fn run(
        &self,
        runnable: Runnable,
        scope: ToolScope,
        eff: &Effective,
        skills: &SkillManager,
        cancel: &CancellationToken,
    ) -> Dispatch {
        match runnable {
            Runnable::Skill { id } => Dispatch::Settled(match skills.load(&eff.skills, &id) {
                Ok(loaded) => {
                    let content = render(&loaded);
                    // Skill không bị cắt theo tool cap: `skill_max_bytes` đã chặn lỗi rõ ràng.
                    Settled {
                        failed: false,
                        result_json: json!({
                            "content": content,
                            "metadata": { "skill_id": loaded.id },
                        })
                        .to_string(),
                        model_text: content,
                        error_code: None,
                    }
                }
                Err(e) => Settled::failure(e.code(), &e.message()),
            }),
            Runnable::ArtifactRead {
                artifact_id,
                offset_bytes,
                max_bytes,
            } => {
                let cap = max_bytes
                    .and_then(|n| usize::try_from(n).ok())
                    .map_or(self.limits.model_cap_bytes, |n| {
                        n.clamp(1, self.limits.model_cap_bytes)
                    });
                let result = match self
                    .services
                    .artifacts
                    .read_model_excerpt(&scope, &artifact_id, offset_bytes, cap)
                    .await
                {
                    Ok((art, excerpt, has_more)) => Ok(ToolOutcome {
                        call_id: scope.tool_call_id,
                        content: excerpt,
                        metadata: json!({
                            "artifact_id": art.id,
                            "name": art.name,
                            "mime_type": art.mime_type,
                            "size_bytes": art.size_bytes,
                            "offset_bytes": offset_bytes,
                            "truncated": has_more,
                        }),
                    }),
                    Err(e) => Err(e),
                };
                Dispatch::Settled(self.settle(result))
            }
            Runnable::Host { tool, effect, args } => {
                self.run_host(tool, effect, args, scope, cancel).await
            }
            // Không áp `tool_timeout` lên cả child (không deadline tổng); timeout theo từng
            // request/tool diễn ra bên trong run của child.
            Runnable::Subagent { input } => {
                match self
                    .tasks
                    .delegate(&scope, input, eff.subagents.as_ref(), cancel)
                    .await
                {
                    Delegation::Done(result) => Dispatch::Settled(self.settle(result)),
                    Delegation::Failed(failure) => {
                        let cap = self.limits.model_cap_bytes;
                        let partial_result = failure
                            .partial_result
                            .as_deref()
                            .map(|text| super::truncate_utf8(text, cap));
                        let wrap = |body: &str| {
                            format!(
                                "<subagent sessionID=\"{}\" state=\"failed\" retryable=\"{}\" stop_reason=\"{}\">\n{}\n</subagent>",
                                failure.session_id, failure.retryable, failure.stop_reason, body
                            )
                        };
                        // Cắt phần thân, không cắt cả chuỗi, để model luôn thấy thẻ đóng.
                        let body_cap = cap.saturating_sub(wrap("").len());
                        let model_text = wrap(&super::truncate_utf8(
                            partial_result.as_deref().unwrap_or(""),
                            body_cap,
                        ));
                        let result_json = json!({
                            "session_id": failure.session_id,
                            "status": failure.status.as_str(),
                            "stop_reason": failure.stop_reason,
                            "retryable": failure.retryable,
                            "partial_result": partial_result,
                        })
                        .to_string();
                        Dispatch::Settled(Settled {
                            failed: true,
                            model_text,
                            result_json,
                            error_code: Some(failure.stop_reason),
                        })
                    }
                    Delegation::Cancelled => Dispatch::Cancelled,
                }
            }
        }
    }

    async fn run_host(
        &self,
        tool: Arc<dyn HostTool>,
        effect: ToolEffect,
        args: Value,
        scope: ToolScope,
        cancel: &CancellationToken,
    ) -> Dispatch {
        let cancelled = Arc::new(AtomicBool::new(cancel.is_cancelled()));
        let cancel_signal = CancelSignal::new(cancel.clone(), self.shutdown.clone());
        let progress = ProgressSink::new(&self.feed, &scope, self.limits.progress_interval);
        let context = ToolContext::new(
            scope.clone(),
            Arc::clone(&cancelled),
            cancel_signal,
            Some(progress),
            Some(self.services.clone()),
        );
        let guarded = AssertUnwindSafe(tool.execute(context, args)).catch_unwind();
        let shutdown_token = self.shutdown.clone();
        let shutdown_grace = self.limits.shutdown_grace;
        let watcher = async {
            cancel.cancelled().await;
            cancelled.store(true, Ordering::Release);
            let grace = if shutdown_token.is_cancelled() {
                shutdown_grace
                    .saturating_sub(Duration::from_secs(5))
                    .min(SHUTDOWN_CANCEL_GRACE_MAX)
            } else {
                USER_CANCEL_GRACE
            };
            tokio::time::sleep(grace).await;
        };
        let limit = tool.timeout().unwrap_or(self.limits.timeout);
        enum HostOutcome {
            Finished(Result<Result<ToolOutcome, ToolError>, Box<dyn std::any::Any + Send>>),
            CancelGraceElapsed,
        }
        let timed = tokio::time::timeout(limit, async {
            tokio::select! {
                r = guarded => HostOutcome::Finished(r),
                () = watcher => HostOutcome::CancelGraceElapsed,
            }
        });
        match timed.await {
            Ok(HostOutcome::Finished(Ok(Ok(outcome)))) => {
                if let Err(err) = self.verify_outcome_artifacts(&scope, &outcome).await {
                    return Dispatch::Settled(self.settle(Err(err)));
                }
                Dispatch::Settled(self.settle(Ok(outcome)))
            }
            Ok(HostOutcome::Finished(Ok(Err(err)))) => {
                if effect == ToolEffect::Resumable
                    && self.shutdown.is_cancelled()
                    && err.code == "CANCELLED"
                {
                    return Dispatch::Cancelled;
                }
                if effect == ToolEffect::Resumable && err.code == CODE_OUTCOME_UNKNOWN {
                    return Dispatch::Unknown(CODE_OUTCOME_UNKNOWN);
                }
                if effect == ToolEffect::Resumable && err.code == crate::tool::CODE_TOOL_RETRY_LATER
                {
                    return Dispatch::RetryLater(err.message);
                }
                Dispatch::Settled(self.settle(Err(err)))
            }
            Ok(HostOutcome::CancelGraceElapsed) => Dispatch::Cancelled,
            Err(_) => {
                self.abnormal(
                    effect,
                    &scope,
                    CODE_TIMEOUT,
                    "the tool exceeded its time limit",
                )
                .await
            }
            Ok(HostOutcome::Finished(Err(_panic))) => {
                self.abnormal(effect, &scope, CODE_PANICKED, "the tool panicked")
                    .await
            }
        }
    }

    async fn verify_outcome_artifacts(
        &self,
        scope: &ToolScope,
        outcome: &ToolOutcome,
    ) -> Result<(), ToolError> {
        let Some(ids) = outcome.metadata.get("artifact_ids") else {
            return Ok(());
        };
        let Some(list) = ids.as_array() else {
            return Err(ToolError {
                code: crate::artifact::CODE_ARTIFACT_INVALID_INPUT.into(),
                message: "metadata.artifact_ids must be an array of strings".into(),
            });
        };
        for entry in list {
            let Some(aid) = entry.as_str().map(str::trim).filter(|s| !s.is_empty()) else {
                return Err(ToolError {
                    code: crate::artifact::CODE_ARTIFACT_INVALID_INPUT.into(),
                    message: "metadata.artifact_ids entries must be non-empty strings".into(),
                });
            };
            self.services
                .artifacts
                .verify_in_family(
                    &scope.owner_id,
                    &scope.workspace_id,
                    &scope.root_session_id,
                    aid,
                )
                .await?;
        }
        Ok(())
    }

    /// Preserve an open resumable operation on timeout/panic, so a new invocation
    /// cannot accidentally duplicate the remote side effect.
    async fn abnormal(
        &self,
        effect: ToolEffect,
        scope: &ToolScope,
        code: &str,
        message: &str,
    ) -> Dispatch {
        if effect == ToolEffect::Resumable {
            let open = self
                .services
                .store
                .current_operation(scope)
                .await
                .map_or(true, |op| op.is_some_and(|op| op.status.is_open()));
            if open {
                return Dispatch::Unknown(CODE_OUTCOME_UNKNOWN);
            }
        }
        match effect {
            ToolEffect::ReadOnly | ToolEffect::Resumable => {
                Dispatch::Settled(Settled::failure(code, message))
            }
            ToolEffect::Mutating => Dispatch::Unknown(CODE_OUTCOME_UNKNOWN),
        }
    }

    fn settle(&self, result: Result<ToolOutcome, ToolError>) -> Settled {
        self.limits.settle(result)
    }
}
