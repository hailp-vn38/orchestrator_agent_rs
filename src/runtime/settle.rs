//! Dispatch và settle tool invocation theo đúng thứ tự provider.

use serde_json::json;

use tokio_util::sync::CancellationToken;
use tracing::{warn, Instrument};

use crate::executor::{Dispatch, Effective, Prepared};
use crate::feed::{event, tool_payload};
use crate::gateway::GatewayError;
use crate::store::{PendingInvocation, RunRecord};
use crate::tool::ToolScope;
use crate::types::RunStatus;

use super::{AgentRuntime, Flow};

impl AgentRuntime {
    /// Dispatch tuần tự các invocation `requested`; settle đủ trước khi trả `Continue`.
    pub(super) async fn settle_pending(
        &self,
        run_id: &str,
        run: &RunRecord,
        eff: &Effective,
        pending: Vec<PendingInvocation>,
        cancel: &CancellationToken,
    ) -> Flow {
        for inv in pending {
            if cancel.is_cancelled() {
                // Chưa chạy invocation này → còn `requested`, sẽ được đưa về interrupted.
                return Flow::Stop(self.fail(run_id, GatewayError::Cancelled).await);
            }
            let span = tracing::info_span!(
                "tool_call",
                invocation_id = %inv.id,
                tool_call_id = %inv.tool_call_id,
                tool = %inv.tool_name
            );
            if let Some(done) = self
                .settle_one(run_id, run, eff, &inv, cancel)
                .instrument(span)
                .await
            {
                return Flow::Stop(done);
            }
        }
        Flow::Continue
    }

    /// `Some(v)`: kết thúc `execute` với giá trị `v`; `None`: invocation đã settle.
    pub(super) async fn settle_one(
        &self,
        run_id: &str,
        run: &RunRecord,
        eff: &Effective,
        inv: &PendingInvocation,
        cancel: &CancellationToken,
    ) -> Option<bool> {
        let settled = match self
            .executor
            .prepare(&inv.tool_name, inv.arguments.clone(), eff)
        {
            // Bị chặn trước dispatch: không có side effect; chỉ ghi lỗi đúng pairing.
            Prepared::Rejected(s) => s,
            Prepared::Run(runnable) => {
                match self.store.begin_invocation(run_id, &inv.id).await {
                    Ok(true) => {}
                    Ok(false) => return Some(true),
                    Err(_) => return Some(false),
                }
                let scope = self.scope(run, eff, inv, run_id);
                match self.executor.run(runnable, scope, eff, cancel).await {
                    Dispatch::Settled(s) => s,
                    Dispatch::Cancelled => {
                        return Some(self.fail(run_id, GatewayError::Cancelled).await)
                    }
                    Dispatch::Unknown(code) => {
                        return Some(
                            self.finish(
                                run_id,
                                RunStatus::Interrupted,
                                code,
                                "the tool outcome is unknown; it was not replayed",
                            )
                            .await,
                        )
                    }
                }
            }
        };
        match self
            .store
            .settle_invocation(run_id, &inv.id, &settled)
            .await
        {
            Ok(true) => {
                // Chỉ sau khi tool-result + terminal state đã commit.
                let mut payload = tool_payload(&inv.id, &inv.tool_call_id, &inv.tool_name);
                payload["error_code"] = json!(settled.error_code);
                self.emitter
                    .committed_run(
                        run_id,
                        if settled.failed {
                            event::TOOL_FAILED
                        } else {
                            event::TOOL_COMPLETED
                        },
                        payload,
                    )
                    .await;
                None
            }
            Ok(false) => Some(true),
            Err(_) => {
                // Tool có thể đã chạy xong nhưng không xác nhận được: invocation vẫn `running`,
                // lần sweep sau sẽ chặn thay vì replay.
                warn!("could not commit the tool result; the outcome stays unconfirmed");
                Some(false)
            }
        }
    }

    pub(super) fn scope(
        &self,
        run: &RunRecord,
        eff: &Effective,
        inv: &PendingInvocation,
        run_id: &str,
    ) -> ToolScope {
        ToolScope {
            owner_id: run.owner_id.clone(),
            workspace_id: run.workspace_id.clone(),
            session_id: run.session_id.clone(),
            root_session_id: run.root_session_id().to_owned(),
            turn_id: run.turn_id.clone(),
            run_id: run_id.to_owned(),
            agent_id: run.agent_id.clone(),
            invocation_id: inv.id.clone(),
            tool_call_id: inv.tool_call_id.clone(),
            allowed_tools: eff.tools.iter().cloned().collect(),
            allowed_skills: eff.skills.iter().cloned().collect(),
        }
    }
}
