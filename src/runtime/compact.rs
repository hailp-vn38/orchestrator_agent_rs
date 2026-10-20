//! Compaction: tóm tắt prefix đã settle để giữ context dưới ngưỡng.

use serde_json::json;

use tokio_util::sync::CancellationToken;

use crate::compaction::{plan, SUMMARY_SYSTEM};
use crate::feed::event;
use crate::gateway::{GatewayError, ModelMessage, ModelRequest, ModelRole, StreamEvent};
use crate::store::{NewCheckpoint, RunRecord};

use super::AgentRuntime;

impl AgentRuntime {
    /// Nén settled exchange prefix của session và persist checkpoint. `Ok(true)`: đã có
    /// checkpoint mới; `Ok(false)`: không còn gì để nén (hoặc run không còn running).
    pub(super) async fn compact(
        &self,
        run: &RunRecord,
        cancel: &CancellationToken,
    ) -> Result<bool, GatewayError> {
        let context = self
            .store
            .conversation(&run.session_id)
            .await
            .map_err(|_| GatewayError::Protocol)?;
        let Some(plan) = plan(&context, self.context.keep_recent) else {
            return Ok(false);
        };
        self.emitter.feed().ephemeral(
            run.root_session_id(),
            &run.session_id,
            Some(&run.run_id),
            event::COMPACTION_STARTED,
            json!({}),
        );
        let request = ModelRequest {
            provider_id: run.provider_id.clone(),
            model_id: run.model_id.clone(),
            system: SUMMARY_SYSTEM.to_owned(),
            messages: vec![ModelMessage::text(ModelRole::User, plan.input)],
            tools: Vec::new(),
            params: Default::default(),
        };
        let response = self
            .gateway
            .complete(&request, cancel, &|_: StreamEvent| {})
            .await?;
        self.record_usage(&run.run_id, &response).await;
        let summary = response.text.trim();
        if summary.is_empty() {
            return Err(GatewayError::Protocol);
        }
        let summary_with_refs = match self.store.session_durable_ledger(&run.session_id).await {
            Ok(Some(ledger)) if !summary.contains("<preserved_artifact_refs>") => {
                format!("{summary}\n{ledger}")
            }
            _ => summary.to_owned(),
        };
        let checkpoint = NewCheckpoint {
            summary: summary_with_refs,
            keep_from_sequence: plan.keep_from_sequence,
            objective: plan.objective,
            model_ref: format!("{}/{}", run.provider_id, run.model_id),
        };
        let written = self
            .store
            .append_compaction(&run.run_id, &checkpoint)
            .await
            .map_err(|_| GatewayError::Protocol)?;
        if written {
            self.emitter
                .committed_run(&run.run_id, event::COMPACTION_COMPLETED, json!({}))
                .await;
        }
        Ok(written)
    }
}
