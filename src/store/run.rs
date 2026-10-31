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
            "SELECT r.session_id, r.turn_id, r.resume_attempt, r.agent_id, r.provider_id, \
             r.model_id, r.status, s.owner_id, s.workspace_id, s.permissions_json, \
             s.parent_session_id \
             FROM agent_runs r JOIN sessions s ON s.id = r.session_id WHERE r.id = ?1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("load run"))?;
        let Some(row) = row else { return Ok(None) };
        let status: String = row.try_get("status").map_err(db("read run status"))?;
        let attempt: i64 = row.try_get("resume_attempt").map_err(db("read attempt"))?;
        let permissions: Option<String> =
            row.try_get("permissions_json").map_err(db("read policy"))?;
        Ok(Some(RunRecord {
            run_id: run_id.to_owned(),
            session_id: row.try_get("session_id").map_err(db("read run"))?,
            turn_id: row.try_get("turn_id").map_err(db("read run turn"))?,
            resume_attempt: u32::try_from(attempt).unwrap_or(u32::MAX),
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

    /// Tổng số step đã commit xuyên suốt mọi attempt của cùng `(session_id, turn_id)`.
    pub(crate) async fn turn_steps(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<u32, OrchestratorError> {
        let steps: Option<i64> = sqlx::query_scalar(
            "SELECT COALESCE(SUM(steps), 0) FROM agent_runs WHERE session_id = ?1 AND turn_id = ?2",
        )
        .bind(session_id)
        .bind(turn_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("read turn steps"))?;
        Ok(steps.unwrap_or(0).max(0) as u32)
    }

    /// Số attempt trong cùng `(session_id, turn_id)` đã có `retry_reason` (để crash recovery
    /// không làm dãn lịch backoff `[1m, 5m, 15m, 30m]`).
    pub(crate) async fn turn_retry_attempts(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<u32, OrchestratorError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_runs \
             WHERE session_id = ?1 AND turn_id = ?2 AND retry_reason IS NOT NULL",
        )
        .bind(session_id)
        .bind(turn_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db("count turn retry attempts"))?;
        Ok(u32::try_from(count).unwrap_or(u32::MAX))
    }

    /// Văn bản assistant gần nhất của `(session_id, turn_id)`; nếu chưa có thì lấy tool result
    /// gần nhất (không tính kết quả lỗi).
    pub(crate) async fn latest_turn_text(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<Option<String>, OrchestratorError> {
        let assistant_jsons: Vec<String> = sqlx::query_scalar(
            "SELECT m.content_json FROM messages m \
             JOIN agent_runs r ON r.id = m.run_id \
             WHERE m.session_id = ?1 AND r.turn_id = ?2 AND m.role = 'assistant' \
             ORDER BY m.sequence DESC LIMIT 16",
        )
        .bind(session_id)
        .bind(turn_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db("load turn assistant messages"))?;
        if let Some(found) = assistant_jsons
            .into_iter()
            .map(|j| text_of(&j))
            .find(|t| !t.trim().is_empty())
        {
            return Ok(Some(found));
        }
        let tool_jsons: Vec<String> = sqlx::query_scalar(
            "SELECT m.content_json FROM messages m \
             JOIN agent_runs r ON r.id = m.run_id \
             WHERE m.session_id = ?1 AND r.turn_id = ?2 AND m.role = 'tool' \
             ORDER BY m.sequence DESC LIMIT 16",
        )
        .bind(session_id)
        .bind(turn_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db("load turn tool messages"))?;
        Ok(tool_jsons
            .into_iter()
            .map(|j| text_of(&j))
            .find(|t| !t.trim().is_empty()))
    }

    /// Số lần `llm_attempts` trong cùng `(session_id, turn_id)` ở bước `step` kết thúc với
    /// `outcome = 'REQUEST_LIMIT_EXCEEDED'`.
    pub(crate) async fn turn_step_limit_exceeded_count(
        &self,
        session_id: &str,
        turn_id: &str,
        step: u32,
    ) -> Result<u32, OrchestratorError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM llm_attempts a \
             JOIN agent_runs r ON r.id = a.run_id \
             WHERE r.session_id = ?1 AND r.turn_id = ?2 AND a.step = ?3 \
               AND a.outcome = 'REQUEST_LIMIT_EXCEEDED'",
        )
        .bind(session_id)
        .bind(turn_id)
        .bind(i64::from(step))
        .fetch_one(&self.pool)
        .await
        .map_err(db("count step limit exceeded"))?;
        Ok(u32::try_from(count).unwrap_or(u32::MAX))
    }

    /// Danh sách `run_id` của các root run đang `running` nhưng thực chất đang chờ một subagent
    /// child ở trạng thái `waiting_retry` hoặc `waiting_user` (nhường slot `active_roots`).
    pub(crate) async fn roots_parked_on_waiting_child(
        &self,
    ) -> Result<std::collections::HashSet<String>, OrchestratorError> {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT ti.run_id FROM tool_invocations ti \
             JOIN agent_runs pr ON pr.id = ti.run_id AND pr.status = 'running' \
             JOIN agent_runs cr ON cr.session_id = ti.child_session_id \
                  AND cr.turn_id = ti.child_turn_id \
                  AND cr.status IN ('waiting_retry', 'waiting_user') \
             WHERE ti.status = 'running' AND ti.child_session_id IS NOT NULL",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db("list roots parked on waiting child"))?;
        Ok(ids.into_iter().collect())
    }

    /// Cộng usage provider báo hoặc gateway ước lượng vào run đang chạy.
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

    pub(crate) async fn record_llm_attempt(
        &self,
        id: &str,
        run_id: &str,
        step: u32,
        first_byte_ms: Option<u64>,
        last_activity_ms: Option<u64>,
        chars_received: u64,
        outcome: &str,
    ) -> Result<(), OrchestratorError> {
        sqlx::query(
            "INSERT INTO llm_attempts(id, run_id, step, first_byte_ms, last_activity_ms, \
             chars_received, outcome) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .bind(id)
        .bind(run_id)
        .bind(i64::from(step))
        .bind(first_byte_ms.map(|v| v.min(i64::MAX as u64) as i64))
        .bind(last_activity_ms.map(|v| v.min(i64::MAX as u64) as i64))
        .bind(chars_received.min(i64::MAX as u64) as i64)
        .bind(outcome)
        .execute(&self.pool)
        .await
        .map_err(db("record llm attempt"))?;
        Ok(())
    }

    /// Tổng `(steps, input_tokens, output_tokens)` đã ghi nhận của `run_id`.
    pub(crate) async fn run_usage_summary(
        &self,
        run_id: &str,
    ) -> Result<Option<(u32, u64, u64)>, OrchestratorError> {
        let row =
            sqlx::query("SELECT steps, input_tokens, output_tokens FROM agent_runs WHERE id = ?1")
                .bind(run_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db("read run usage summary"))?;
        let Some(row) = row else { return Ok(None) };
        let steps: i64 = row.try_get("steps").map_err(db("read steps"))?;
        let input: i64 = row.try_get("input_tokens").map_err(db("read input"))?;
        let output: i64 = row.try_get("output_tokens").map_err(db("read output"))?;
        Ok(Some((
            u32::try_from(steps).unwrap_or(u32::MAX),
            u64::try_from(input).unwrap_or_default(),
            u64::try_from(output).unwrap_or_default(),
        )))
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

    /// Tạm dừng run sang `waiting_retry` (kèm `next_attempt_at`) hoặc `waiting_user` (`None`).
    /// Không đóng các invocation `requested` để attempt kế tiếp tiếp tục.
    pub(crate) async fn suspend_run(
        &self,
        run_id: &str,
        status: RunStatus,
        error_code: &str,
        error_message: &str,
        retry_reason: &str,
        next_attempt_at: Option<i64>,
    ) -> Result<bool, OrchestratorError> {
        debug_assert!(status.is_waiting());
        let mut tx = self.pool.begin().await.map_err(db("begin suspend"))?;
        let bumped = sqlx::query(
            "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = (SELECT session_id FROM agent_runs WHERE id = ?1 AND status = 'running')",
        )
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("bump revision on suspend"))?
        .rows_affected();
        if bumped == 0 {
            let _ = tx.rollback().await;
            return Ok(false);
        }
        sqlx::query(
            "UPDATE agent_runs SET status = ?1, error_code = ?2, error_message = ?3, \
             retry_reason = ?4, next_attempt_at = ?5, finished_at = NULL \
             WHERE id = ?6 AND status = 'running'",
        )
        .bind(status.as_str())
        .bind(error_code)
        .bind(sanitize_message(error_message))
        .bind(retry_reason)
        .bind(next_attempt_at)
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("suspend run"))?;
        tx.commit().await.map_err(db("commit suspend"))?;
        Ok(true)
    }

    /// Terminal không-completed (failed/timed_out/interrupted/cancelled). `Ok(false)`:
    /// run không còn active (`running`/`waiting_retry`/`waiting_user`). Invocation chưa settle
    /// của run được đưa về terminal cùng transaction (không để `requested`/`running` treo) mà
    /// **không** giả outcome.
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
             WHERE id = (SELECT session_id FROM agent_runs WHERE id = ?1 \
                 AND status IN ('running', 'waiting_retry', 'waiting_user'))",
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
             next_attempt_at = NULL, finished_at = unixepoch() \
             WHERE id = ?4 AND status IN ('running', 'waiting_retry', 'waiting_user')",
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
            "SELECT id, resume_attempt, status, steps, input_tokens, output_tokens, \
             final_message_id, error_code, error_message, next_attempt_at, retry_reason \
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
        let steps: i64 = row.try_get("steps").map_err(db("read steps"))?;
        let input_tokens: i64 = row.try_get("input_tokens").map_err(db("read input"))?;
        let output_tokens: i64 = row.try_get("output_tokens").map_err(db("read output"))?;
        let final_message_id: Option<String> =
            row.try_get("final_message_id").map_err(db("read final"))?;
        let error_code: Option<String> = row.try_get("error_code").map_err(db("read error"))?;
        let error_message: Option<String> =
            row.try_get("error_message").map_err(db("read error"))?;
        let next_attempt_at: Option<i64> = row
            .try_get("next_attempt_at")
            .map_err(db("read next_attempt_at"))?;
        let retry_reason: Option<String> = row
            .try_get("retry_reason")
            .map_err(db("read retry_reason"))?;

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
        let partial_output = if output.is_some() {
            None
        } else {
            self.latest_turn_text(session_id, turn_id).await?
        };
        Ok(AgentResult {
            session_id: session_id.to_owned(),
            turn_id: turn_id.to_owned(),
            run_id,
            attempt: attempt as u32,
            status: RunStatus::parse(&status)
                .ok_or_else(|| OrchestratorError::new(ErrorCode::Internal, "unknown run status"))?,
            steps: u32::try_from(steps).unwrap_or(u32::MAX),
            input_tokens: u64::try_from(input_tokens).unwrap_or_default(),
            output_tokens: u64::try_from(output_tokens).unwrap_or_default(),
            output,
            partial_output,
            final_message_id,
            error: error_code.map(|code| ResultError {
                code,
                message: error_message.unwrap_or_default(),
            }),
            next_attempt_at,
            retry_reason,
            artifact_preview: Vec::new(),
            artifacts_next_cursor: None,
        })
    }
}
