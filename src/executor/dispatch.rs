//! Chạy invocation, timeout/cancel và xác minh artifact của HostTool.
use super::{
    Dispatch, Effective, Runnable, Settled, ToolExecutor, CODE_OUTCOME_UNKNOWN, CODE_PANICKED,
    CODE_TIMEOUT,
};
use crate::feed::ProgressSink;
use crate::skill::render;
use crate::subagent::Delegation;
use crate::tool::{HostTool, ToolContext, ToolEffect, ToolError, ToolOutcome, ToolScope};
use futures::FutureExt;
use serde_json::{json, Value};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

impl ToolExecutor {
    /// Chạy một invocation đã `prepare`. Cancel là hợp tác: tool nhận cờ `is_cancelled` và tự
    /// dừng; executor không bỏ tool đang chạy (shutdown chờ trong grace). Timeout là per-op.
    pub(crate) async fn run(
        &self,
        runnable: Runnable,
        scope: ToolScope,
        eff: &Effective,
        cancel: &CancellationToken,
    ) -> Dispatch {
        match runnable {
            Runnable::Skill { id } => Dispatch::Settled(match self.skills.load(&eff.skills, &id) {
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
                match self.tasks.delegate(&scope, input, cancel).await {
                    Delegation::Done(result) => Dispatch::Settled(self.settle(result)),
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
        let progress = ProgressSink::new(&self.feed, &scope, self.limits.progress_interval);
        let context = ToolContext::new(
            scope.clone(),
            Arc::clone(&cancelled),
            Some(progress),
            Some(self.services.clone()),
        );
        let guarded = AssertUnwindSafe(tool.execute(context, args)).catch_unwind();
        let watcher = async {
            cancel.cancelled().await;
            cancelled.store(true, Ordering::Release);
            std::future::pending::<()>().await;
        };
        let limit = tool.timeout().unwrap_or(self.limits.timeout);
        let timed = tokio::time::timeout(limit, async {
            tokio::select! {
                r = guarded => r,
                () = watcher => unreachable!("the cancel watcher never completes"),
            }
        });
        match timed.await {
            Ok(Ok(Ok(outcome))) => {
                if let Err(err) = self.verify_outcome_artifacts(&scope, &outcome).await {
                    return Dispatch::Settled(self.settle(Err(err)));
                }
                Dispatch::Settled(self.settle(Ok(outcome)))
            }
            Ok(Ok(Err(err))) => Dispatch::Settled(self.settle(Err(err))),
            Err(_) => self.abnormal(effect, CODE_TIMEOUT, "the tool exceeded its time limit"),
            Ok(Err(_panic)) => self.abnormal(effect, CODE_PANICKED, "the tool panicked"),
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

    /// Timeout/panic: tool ReadOnly có thể báo lỗi kỹ thuật; tool Mutating có outcome unknown.
    fn abnormal(&self, effect: ToolEffect, code: &str, message: &str) -> Dispatch {
        match effect {
            ToolEffect::ReadOnly => Dispatch::Settled(Settled::failure(code, message)),
            ToolEffect::Mutating => Dispatch::Unknown(CODE_OUTCOME_UNKNOWN),
        }
    }

    fn settle(&self, result: Result<ToolOutcome, ToolError>) -> Settled {
        self.limits.settle(result)
    }
}
