//! Vòng đời của `agent_runs`: load, usage, steps, kết thúc.

use super::codec::{db, new_id, parse_policy, sanitize_message, text_json, text_of};
use super::types::RunRecord;
use super::SessionStore;
use crate::error::{ErrorCode, OrchestratorError};
use crate::types::{AgentResult, CallerContext, ResultError, RunStatus};
use sqlx::Row;

impl SessionStore {
    #[cfg(test)]
    pub(crate) async fn running_run_ids(&self) -> Result<Vec<String>, OrchestratorError> {
        sqlx::query_scalar(
            "SELECT id FROM agent_runs WHERE status = 'running' ORDER BY started_at, id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db("list running runs"))
    }

    /// Mọi claim `running` theo thứ tự admission, kèm cờ child (session có parent) để
    /// supervisor áp giới hạn concurrency chỉ cho root.
    pub(crate) async fn running_claims(&self) -> Result<Vec<(String, bool)>, OrchestratorError> {
        let rows = sqlx::query(
            "SELECT r.id, s.parent_session_id IS NOT NULL AS is_child FROM agent_runs r \
             JOIN sessions s ON s.id = r.session_id WHERE r.status = 'running' \
             ORDER BY r.started_at, r.id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db("list running runs"))?;
        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get("id").map_err(db("read run id"))?,
                    r.try_get("is_child").map_err(db("read child flag"))?,
                ))
            })
            .collect()
    }

    pub(crate) async fn load_run(
        &self,
        run_id: &str,
    ) -> Result<Option<RunRecord>, OrchestratorError> {
        let row = sqlx::query(
            "SELECT r.session_id, r.turn_id, r.agent_id, r.provider_id, r.model_id, r.status, \
             s.owner_id, s.workspace_id, s.permissions_json, s.parent_session_id \
             FROM agent_runs r JOIN sessions s ON s.id = r.session_id WHERE r.id = ?1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("load run"))?;
        let Some(row) = row else { return Ok(None) };
        let status: String = row.try_get("status").map_err(db("read run status"))?;
        let permissions: Option<String> =
            row.try_get("permissions_json").map_err(db("read policy"))?;
        Ok(Some(RunRecord {
            run_id: run_id.to_owned(),
            session_id: row.try_get("session_id").map_err(db("read run"))?,
            turn_id: row.try_get("turn_id").map_err(db("read run turn"))?,
            agent_id: row.try_get("agent_id").map_err(db("read run"))?,
            provider_id: row.try_get("provider_id").map_err(db("read run"))?,
            model_id: row.try_get("model_id").map_err(db("read run"))?,
            status: RunStatus::parse(&status)
                .ok_or_else(|| OrchestratorError::new(ErrorCode::Internal, "unknown run status"))?,
            owner_id: row.try_get("owner_id").map_err(db("read run"))?,
            workspace_id: row.try_get("workspace_id").map_err(db("read run"))?,
            policy: parse_policy(permissions.as_deref()),
            parent_session_id: row
                .try_get("parent_session_id")
                .map_err(db("read run parent"))?,
        }))
    }

    /// Số step đã commit của run (assistant message có tool call hoặc final).
    pub(crate) async fn run_steps(&self, run_id: &str) -> Result<u32, OrchestratorError> {
        let steps: Option<i64> = sqlx::query_scalar("SELECT steps FROM agent_runs WHERE id = ?1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db("read run steps"))?;
        Ok(steps.unwrap_or(0).max(0) as u32)
    }

    /// Cộng usage provider báo (chỉ số đo được, không ước lượng) vào run đang chạy.
    pub(crate) async fn add_usage(
        &self,
        run_id: &str,
        input_tokens: u64,
        output_tokens: u64,
    ) -> Result<(), OrchestratorError> {
        sqlx::query(
            "UPDATE agent_runs SET input_tokens = input_tokens + ?2, \
             output_tokens = output_tokens + ?3 WHERE id = ?1 AND status = 'running'",
        )
        .bind(run_id)
        .bind(input_tokens.min(i64::MAX as u64) as i64)
        .bind(output_tokens.min(i64::MAX as u64) as i64)
        .execute(&self.pool)
        .await
        .map_err(db("add usage"))?;
        Ok(())
    }

    /// Final assistant message, `final_message_id` và terminal `completed` cùng transaction.
    /// `Ok(None)`: run không còn `running` (đã terminal) — không ghi đè.
    pub(crate) async fn complete_run(
        &self,
        run_id: &str,
        text: &str,
    ) -> Result<Option<String>, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin completion"))?;
        let claimed = sqlx::query(
            "UPDATE sessions SET next_sequence = next_sequence + 1, \
             state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = (SELECT session_id FROM agent_runs WHERE id = ?1 AND status = 'running') \
             RETURNING id, next_sequence - 1 AS seq",
        )
        .bind(run_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db("claim final sequence"))?;
        let Some(row) = claimed else {
            let _ = tx.rollback().await;
            return Ok(None);
        };
        let session_id: String = row.try_get("id").map_err(db("read session"))?;
        let seq: i64 = row.try_get("seq").map_err(db("read sequence"))?;
        let message_id = new_id();
        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json, run_id) \
             VALUES(?1, ?2, ?3, 'assistant', ?4, ?5)",
        )
        .bind(&message_id)
        .bind(&session_id)
        .bind(seq)
        .bind(text_json(text))
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("insert final message"))?;
        let changed = sqlx::query(
            "UPDATE agent_runs SET status = 'completed', final_message_id = ?1, \
             steps = steps + 1, finished_at = unixepoch() \
             WHERE id = ?2 AND status = 'running'",
        )
        .bind(&message_id)
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("finish run"))?
        .rows_affected();
        if changed != 1 {
            let _ = tx.rollback().await;
            return Ok(None);
        }
        tx.commit().await.map_err(db("commit completion"))?;
        Ok(Some(message_id))
    }

    /// Terminal không-completed (failed/timed_out/interrupted/cancelled). `Ok(false)`:
    /// run không còn `running`. Invocation chưa settle của run được đưa về terminal cùng
    /// transaction (không để `requested`/`running` treo) mà **không** giả outcome.
    ///
    /// `interrupted` là trạng thái có thể phục hồi (M5): invocation `running` (outcome chưa
    /// biết) thành `interrupted`, còn invocation `requested` (chưa từng chạy) được **giữ**
    /// `requested` để attempt phục hồi kế tiếp chạy chúng, không mất.
    pub(crate) async fn finish_run_without_final(
        &self,
        run_id: &str,
        status: RunStatus,
        error_code: &str,
        error_message: &str,
    ) -> Result<bool, OrchestratorError> {
        debug_assert!(status.is_terminal() && status != RunStatus::Completed);
        let mut tx = self.pool.begin().await.map_err(db("begin terminal"))?;
        let bumped = sqlx::query(
            "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = (SELECT session_id FROM agent_runs WHERE id = ?1 AND status = 'running')",
        )
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("bump revision"))?
        .rows_affected();
        if bumped == 0 {
            let _ = tx.rollback().await;
            return Ok(false);
        }
        sqlx::query(
            "UPDATE agent_runs SET status = ?1, error_code = ?2, error_message = ?3, \
             finished_at = unixepoch() WHERE id = ?4 AND status = 'running'",
        )
        .bind(status.as_str())
        .bind(error_code)
        .bind(sanitize_message(error_message))
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("finish run"))?;
        // `requested` chỉ được giữ cho run `interrupted` (có thể resume); mọi terminal khác
        // đưa chúng về terminal vì run đó sẽ không chạy lại.
        sqlx::query(
            "UPDATE tool_invocations SET status = ?1, error_code = ?2, finished_at = unixepoch() \
             WHERE run_id = ?3 AND (status = 'running' OR (status = 'requested' AND ?4))",
        )
        .bind(if status == RunStatus::Cancelled {
            "cancelled"
        } else {
            "interrupted"
        })
        .bind(error_code)
        .bind(run_id)
        .bind(status != RunStatus::Interrupted)
        .execute(&mut *tx)
        .await
        .map_err(db("interrupt invocations"))?;
        tx.commit().await.map_err(db("commit terminal"))?;
        Ok(true)
    }

    /// Projection của attempt mới nhất; `output` chỉ từ `final_message_id` của chính attempt đó.
    pub(crate) async fn result(
        &self,
        caller: &CallerContext,
        session_id: &str,
        turn_id: &str,
    ) -> Result<AgentResult, OrchestratorError> {
        self.authorize(caller, session_id).await?;
        self.load_result(session_id, turn_id).await
    }

    /// Như [`Self::result`] nhưng không kiểm scope: chỉ dùng nội bộ cho parent invocation đã
    /// được admit (child do runtime tạo, đã kiểm owner/workspace/ancestry lúc admission).
    pub(crate) async fn load_result(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<AgentResult, OrchestratorError> {
        let row = sqlx::query(
            "SELECT id, resume_attempt, status, final_message_id, error_code, error_message \
             FROM agent_runs WHERE session_id = ?1 AND turn_id = ?2 \
             ORDER BY resume_attempt DESC LIMIT 1",
        )
        .bind(session_id)
        .bind(turn_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("load result"))?;
        let Some(row) = row else {
            return Err(OrchestratorError::new(
                ErrorCode::NotFound,
                "turn not found",
            ));
        };
        let run_id: String = row.try_get("id").map_err(db("read run id"))?;
        let attempt: i64 = row.try_get("resume_attempt").map_err(db("read attempt"))?;
        let status: String = row.try_get("status").map_err(db("read status"))?;
        let final_message_id: Option<String> =
            row.try_get("final_message_id").map_err(db("read final"))?;
        let error_code: Option<String> = row.try_get("error_code").map_err(db("read error"))?;
        let error_message: Option<String> =
            row.try_get("error_message").map_err(db("read error"))?;

        let output = match &final_message_id {
            None => None,
            Some(id) => {
                let json: Option<String> = sqlx::query_scalar(
                    "SELECT content_json FROM messages \
                     WHERE id = ?1 AND run_id = ?2 AND session_id = ?3",
                )
                .bind(id)
                .bind(&run_id)
                .bind(session_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db("load final message"))?;
                json.map(|j| text_of(&j))
            }
        };
        Ok(AgentResult {
            session_id: session_id.to_owned(),
            turn_id: turn_id.to_owned(),
            run_id,
            attempt: attempt as u32,
            status: RunStatus::parse(&status)
                .ok_or_else(|| OrchestratorError::new(ErrorCode::Internal, "unknown run status"))?,
            output,
            final_message_id,
            error: error_code.map(|code| ResultError {
                code,
                message: error_message.unwrap_or_default(),
            }),
            artifact_preview: Vec::new(),
            artifacts_next_cursor: None,
        })
    }
}
