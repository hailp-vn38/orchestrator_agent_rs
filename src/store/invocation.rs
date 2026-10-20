//! Tool invocation: commit tool call, bắt đầu, settle, liệt kê pending.

use super::codec::{db, new_id};
use super::transcript::claim_sequence;
use super::types::{PendingInvocation, StoredToolCall};
use super::SessionStore;
use crate::error::OrchestratorError;
use crate::executor::Settled;
use sqlx::Row;

impl SessionStore {
    /// Một transaction: assistant message (text + tất cả tool calls) và **mọi** invocation
    /// `requested`. Chỉ sau commit này mới được dispatch bất kỳ tool nào. `Ok(None)`: run
    /// không còn `running`. Trả invocation theo đúng thứ tự provider.
    pub(crate) async fn commit_tool_calls(
        &self,
        run_id: &str,
        text: &str,
        calls: &[StoredToolCall],
    ) -> Result<Option<Vec<PendingInvocation>>, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin tool calls"))?;
        let Some((session_id, seq)) = claim_sequence(&mut tx, run_id).await? else {
            let _ = tx.rollback().await;
            return Ok(None);
        };
        let message_id = new_id();
        let content = serde_json::json!({
            "text": text,
            "tool_calls": calls
                .iter()
                .map(|c| serde_json::json!({
                    "id": c.id, "name": c.name, "arguments": c.arguments,
                }))
                .collect::<Vec<_>>(),
        })
        .to_string();
        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json, run_id) \
             VALUES(?1, ?2, ?3, 'assistant', ?4, ?5)",
        )
        .bind(&message_id)
        .bind(&session_id)
        .bind(seq)
        .bind(content)
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("insert assistant tool-call message"))?;
        let mut pending = Vec::with_capacity(calls.len());
        for call in calls {
            let id = new_id();
            sqlx::query(
                "INSERT INTO tool_invocations(id, session_id, run_id, tool_call_id, \
                 assistant_message_id, tool_name, arguments_json, status) \
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, 'requested')",
            )
            .bind(&id)
            .bind(&session_id)
            .bind(run_id)
            .bind(&call.id)
            .bind(&message_id)
            .bind(&call.name)
            .bind(call.arguments.to_string())
            .execute(&mut *tx)
            .await
            .map_err(db("insert tool invocation"))?;
            pending.push(PendingInvocation {
                id,
                tool_call_id: call.id.clone(),
                tool_name: call.name.clone(),
                arguments: call.arguments.clone(),
                started: false,
            });
        }
        let changed = sqlx::query(
            "UPDATE agent_runs SET steps = steps + 1 WHERE id = ?1 AND status = 'running'",
        )
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("count step"))?
        .rows_affected();
        if changed != 1 {
            let _ = tx.rollback().await;
            return Ok(None);
        }
        tx.commit().await.map_err(db("commit tool calls"))?;
        Ok(Some(pending))
    }

    /// `requested` → `running`, commit **trước** khi gọi tool: nếu process chết sau điểm này
    /// thì outcome là unknown và không bao giờ được replay mù. `Ok(false)`: không còn hợp lệ.
    pub(crate) async fn begin_invocation(
        &self,
        run_id: &str,
        invocation_id: &str,
    ) -> Result<bool, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin invocation"))?;
        let started = sqlx::query(
            "UPDATE tool_invocations SET status = 'running', started_at = unixepoch() \
             WHERE id = ?1 AND run_id = ?2 AND status = 'requested' \
             AND EXISTS (SELECT 1 FROM agent_runs WHERE id = ?2 AND status = 'running') \
             RETURNING session_id",
        )
        .bind(invocation_id)
        .bind(run_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db("start invocation"))?;
        let Some(row) = started else {
            let _ = tx.rollback().await;
            return Ok(false);
        };
        let session_id: String = row.try_get("session_id").map_err(db("read session"))?;
        sqlx::query(
            "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = ?1",
        )
        .bind(&session_id)
        .execute(&mut *tx)
        .await
        .map_err(db("bump revision"))?;
        tx.commit().await.map_err(db("commit invocation start"))?;
        Ok(true)
    }

    /// Ghi tool-result message **và** đưa invocation tới `completed`/`failed` trong cùng một
    /// transaction (docs/05 §5.4). Nếu commit lỗi thì không có gì được xác nhận.
    /// `Ok(false)`: invocation/run không còn hợp lệ (không ghi đè).
    pub(crate) async fn settle_invocation(
        &self,
        run_id: &str,
        invocation_id: &str,
        settled: &Settled,
    ) -> Result<bool, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin settle"))?;
        let status = if settled.failed {
            "failed"
        } else {
            "completed"
        };
        let updated = sqlx::query(
            "UPDATE tool_invocations SET status = ?3, result_json = ?4, error_code = ?5, \
             finished_at = unixepoch() \
             WHERE id = ?1 AND run_id = ?2 AND status IN ('requested', 'running') \
             AND EXISTS (SELECT 1 FROM agent_runs WHERE id = ?2 AND status = 'running') \
             RETURNING session_id, tool_call_id",
        )
        .bind(invocation_id)
        .bind(run_id)
        .bind(status)
        .bind(&settled.result_json)
        .bind(&settled.error_code)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db("settle invocation"))?;
        let Some(row) = updated else {
            let _ = tx.rollback().await;
            return Ok(false);
        };
        let session_id: String = row.try_get("session_id").map_err(db("read session"))?;
        let call_id: String = row.try_get("tool_call_id").map_err(db("read call id"))?;
        let seq: i64 = sqlx::query_scalar(
            "UPDATE sessions SET next_sequence = next_sequence + 1, \
             state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = ?1 RETURNING next_sequence - 1",
        )
        .bind(&session_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db("claim result sequence"))?;
        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json, \
             tool_call_id, tool_invocation_id, run_id) \
             VALUES(?1, ?2, ?3, 'tool', ?4, ?5, ?6, ?7)",
        )
        .bind(new_id())
        .bind(&session_id)
        .bind(seq)
        .bind(
            serde_json::json!({ "text": settled.model_text, "is_error": settled.failed })
                .to_string(),
        )
        .bind(&call_id)
        .bind(invocation_id)
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("insert tool result"))?;
        tx.commit().await.map_err(db("commit tool result"))?;
        Ok(true)
    }

    /// Invocation chưa settle của run, theo đúng thứ tự provider.
    pub(crate) async fn pending_invocations(
        &self,
        run_id: &str,
    ) -> Result<Vec<PendingInvocation>, OrchestratorError> {
        let rows = sqlx::query(
            "SELECT id, tool_call_id, tool_name, arguments_json, status FROM tool_invocations \
             WHERE run_id = ?1 AND status IN ('requested', 'running') ORDER BY rowid",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db("load pending invocations"))?;
        rows.iter()
            .map(|r| {
                let args: String = r.try_get("arguments_json").map_err(db("read arguments"))?;
                let status: String = r.try_get("status").map_err(db("read status"))?;
                Ok(PendingInvocation {
                    id: r.try_get("id").map_err(db("read id"))?,
                    tool_call_id: r.try_get("tool_call_id").map_err(db("read call id"))?,
                    tool_name: r.try_get("tool_name").map_err(db("read tool name"))?,
                    arguments: serde_json::from_str(&args).unwrap_or(serde_json::json!({})),
                    started: status == "running",
                })
            })
            .collect()
    }
}
