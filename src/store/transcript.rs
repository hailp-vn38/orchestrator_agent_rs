//! Transcript: đọc context/checkpoint compaction, phân trang lịch sử, cấp sequence.

use super::codec::{calls_of, db, new_id, text_of};
use super::types::{Checkpoint, Context, NewCheckpoint, TranscriptEntry};
use super::SessionStore;
use crate::error::{ErrorCode, OrchestratorError};
use crate::types::{CallerContext, MessageRole, MessageToolCall, MessageView, Page, PageRequest};
use sqlx::Row;

impl SessionStore {
    /// Context của session: checkpoint compaction mới nhất (nếu có) và các entry
    /// user/assistant/tool từ `keep_from_sequence` trở đi theo thứ tự `sequence`.
    pub(crate) async fn conversation(
        &self,
        session_id: &str,
    ) -> Result<Context, OrchestratorError> {
        let checkpoint = self.latest_checkpoint(session_id).await?;
        let from = checkpoint.as_ref().map_or(0, |c| c.keep_from_sequence);
        let rows = sqlx::query(
            "SELECT m.sequence, m.role, m.content_json, m.tool_call_id, ti.tool_name \
             FROM messages m LEFT JOIN tool_invocations ti ON ti.id = m.tool_invocation_id \
             WHERE m.session_id = ?1 AND m.sequence >= ?2 \
             AND m.role IN ('user', 'assistant', 'tool') \
             ORDER BY m.sequence",
        )
        .bind(session_id)
        .bind(from as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db("load conversation"))?;
        let entries = rows
            .iter()
            .map(|r| {
                let role: String = r.try_get("role").map_err(db("read role"))?;
                let seq: i64 = r.try_get("sequence").map_err(db("read sequence"))?;
                let json: String = r.try_get("content_json").map_err(db("read content"))?;
                let entry = match role.as_str() {
                    "user" => TranscriptEntry::User(text_of(&json)),
                    "tool" => {
                        let call_id: Option<String> =
                            r.try_get("tool_call_id").map_err(db("read call id"))?;
                        let tool_name: Option<String> =
                            r.try_get("tool_name").map_err(db("read tool name"))?;
                        TranscriptEntry::Tool {
                            call_id: call_id.unwrap_or_default(),
                            tool_name: tool_name.unwrap_or_default(),
                            text: text_of(&json),
                        }
                    }
                    _ => TranscriptEntry::Assistant {
                        text: text_of(&json),
                        calls: calls_of(&json),
                    },
                };
                Ok((seq as u64, entry))
            })
            .collect::<Result<Vec<_>, OrchestratorError>>()?;
        Ok(Context {
            checkpoint,
            entries,
        })
    }

    pub(super) async fn latest_checkpoint(
        &self,
        session_id: &str,
    ) -> Result<Option<Checkpoint>, OrchestratorError> {
        let json: Option<String> = sqlx::query_scalar(
            "SELECT content_json FROM messages WHERE session_id = ?1 AND role = 'compaction' \
             ORDER BY sequence DESC LIMIT 1",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("load checkpoint"))?;
        Ok(json.and_then(|j| {
            let parsed = serde_json::from_str::<serde_json::Value>(&j).ok();
            if parsed.is_none() {
                tracing::warn!("latest compaction checkpoint is unreadable; ignoring it");
            }
            let v = parsed?;
            Some(Checkpoint {
                summary: v.get("summary")?.as_str()?.to_owned(),
                keep_from_sequence: v.get("keep_from_sequence")?.as_u64()?,
                objective: v
                    .get("objective")
                    .and_then(|o| o.as_str())
                    .map(str::to_owned),
            })
        }))
    }

    /// Dựng danh sách tham chiếu bền vững (committed artifact IDs, manifest checkpoint IDs,
    /// invocation metadata) của session để gắn nguyên văn vào compaction checkpoint, đảm bảo
    /// model không bao giờ mất ID sau khi nén context (docs/17 §17.10).
    pub(crate) async fn session_durable_ledger(
        &self,
        session_id: &str,
    ) -> Result<Option<String>, OrchestratorError> {
        let art_rows = sqlx::query(
            "SELECT id, name, mime_type, role, size_bytes FROM artifacts \
             WHERE session_id = ?1 ORDER BY created_at ASC, id ASC LIMIT 64",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db("list session artifacts for ledger"))?;

        let cp_rows = sqlx::query(
            "SELECT id, tool_name, checkpoint_artifact_id, result_json FROM tool_invocations \
             WHERE session_id = ?1 \
               AND (checkpoint_artifact_id IS NOT NULL OR result_json IS NOT NULL) \
             ORDER BY rowid DESC LIMIT 32",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db("list invocation checkpoints for ledger"))?;

        let mut lines = Vec::new();
        for row in art_rows {
            let id: String = row.try_get("id").unwrap_or_default();
            let name: String = row.try_get("name").unwrap_or_default();
            let mime: String = row.try_get("mime_type").unwrap_or_default();
            let role: String = row.try_get("role").unwrap_or_default();
            let size: i64 = row.try_get("size_bytes").unwrap_or(0);
            lines.push(format!(
                "- artifact id={id} name={name} role={role} mime={mime} bytes={size}"
            ));
        }
        for row in cp_rows {
            let inv_id: String = row.try_get("id").unwrap_or_default();
            let tool: String = row.try_get("tool_name").unwrap_or_default();
            let cp: Option<String> = row.try_get("checkpoint_artifact_id").unwrap_or(None);
            let res_json: Option<String> = row.try_get("result_json").unwrap_or(None);
            if let Some(cp_id) = cp {
                lines.push(format!(
                    "- invocation id={inv_id} tool={tool} checkpoint_manifest_id={cp_id}"
                ));
            }
            if let Some(raw) = res_json {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
                    if let Some(meta) = v.get("metadata").filter(|m| !m.is_null()) {
                        let meta_str = meta.to_string();
                        if meta_str != "{}" && meta_str.len() <= 512 {
                            lines.push(format!(
                                "- invocation id={inv_id} tool={tool} metadata={meta_str}"
                            ));
                        }
                    }
                }
            }
        }
        if lines.is_empty() {
            Ok(None)
        } else {
            Ok(Some(format!(
                "<preserved_artifact_refs>\n{}\n</preserved_artifact_refs>",
                lines.join("\n")
            )))
        }
    }

    /// Ghi checkpoint compaction của session mà run đang chạy (một transaction, một sequence).
    /// Original messages không bị đụng tới. `Ok(false)`: run không còn `running`, hoặc
    /// frontier không tiến so với checkpoint trước / vượt quá message cuối.
    pub(crate) async fn append_compaction(
        &self,
        run_id: &str,
        checkpoint: &NewCheckpoint,
    ) -> Result<bool, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin compaction"))?;
        let Some((session_id, seq)) = claim_sequence(&mut tx, run_id).await? else {
            let _ = tx.rollback().await;
            return Ok(false);
        };
        let previous: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(CAST(json_extract(content_json, '$.keep_from_sequence') AS INTEGER)) \
             FROM messages WHERE session_id = ?1 AND role = 'compaction'",
        )
        .bind(&session_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db("read previous frontier"))?;
        let frontier = checkpoint.keep_from_sequence as i64;
        if frontier <= previous.unwrap_or(0) || frontier >= seq {
            let _ = tx.rollback().await;
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json, run_id) \
             VALUES(?1, ?2, ?3, 'compaction', ?4, ?5)",
        )
        .bind(new_id())
        .bind(&session_id)
        .bind(seq)
        .bind(
            serde_json::json!({
                "text": checkpoint.summary,
                "summary": checkpoint.summary,
                "keep_from_sequence": checkpoint.keep_from_sequence,
                "objective": checkpoint.objective,
                "version": CHECKPOINT_VERSION,
                "model_ref": checkpoint.model_ref,
            })
            .to_string(),
        )
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("insert compaction"))?;
        tx.commit().await.map_err(db("commit compaction"))?;
        Ok(true)
    }

    pub(crate) async fn history_page(
        &self,
        caller: &CallerContext,
        session_id: &str,
        page: PageRequest,
        max_page: u32,
    ) -> Result<Page<MessageView>, OrchestratorError> {
        self.authorize(caller, session_id).await?;
        let after = match &page.cursor {
            None => 0,
            Some(c) => decode_cursor(c, session_id)?,
        };
        let limit = page.limit.clamp(1, max_page);
        let rows = sqlx::query(
            "SELECT id, sequence, role, content_json, run_id, tool_call_id FROM messages \
             WHERE session_id = ?1 AND sequence > ?2 AND role IN ('user', 'assistant', 'tool') \
             ORDER BY sequence LIMIT ?3",
        )
        .bind(session_id)
        .bind(after as i64)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await
        .map_err(db("load history"))?;

        let mut items = Vec::with_capacity(rows.len().min(limit as usize));
        for row in &rows {
            items.push(message_from_row(row)?);
        }
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items.last().map(|m| encode_cursor(session_id, m.sequence))
        } else {
            None
        };
        Ok(Page { items, next_cursor })
    }
}

/// Version của encoding checkpoint trong `messages.content_json`.
const CHECKPOINT_VERSION: u32 = 1;

/// Cột cần có: `id, sequence, role, content_json, run_id, tool_call_id`.
pub(super) fn message_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<MessageView, OrchestratorError> {
    let role: String = row.try_get("role").map_err(db("read role"))?;
    let json: String = row.try_get("content_json").map_err(db("read content"))?;
    let sequence: i64 = row.try_get("sequence").map_err(db("read sequence"))?;
    Ok(MessageView {
        id: row.try_get("id").map_err(db("read id"))?,
        sequence: sequence as u64,
        role: MessageRole::parse(&role)
            .ok_or_else(|| OrchestratorError::new(ErrorCode::Internal, "unknown message role"))?,
        text: text_of(&json),
        run_id: row.try_get("run_id").map_err(db("read run id"))?,
        tool_call_id: row.try_get("tool_call_id").map_err(db("read call id"))?,
        tool_calls: calls_of(&json)
            .into_iter()
            .map(|c| MessageToolCall {
                call_id: c.id,
                name: c.name,
            })
            .collect(),
    })
}

/// Cấp sequence kế tiếp cho session của một run còn `running`. `None`: run đã terminal.
pub(super) async fn claim_sequence(
    tx: &mut sqlx::SqliteConnection,
    run_id: &str,
) -> Result<Option<(String, i64)>, OrchestratorError> {
    let row = sqlx::query(
        "UPDATE sessions SET next_sequence = next_sequence + 1, \
         state_revision = state_revision + 1, updated_at = unixepoch() \
         WHERE id = (SELECT session_id FROM agent_runs WHERE id = ?1 AND status = 'running') \
         RETURNING id, next_sequence - 1 AS seq",
    )
    .bind(run_id)
    .fetch_optional(tx)
    .await
    .map_err(db("claim sequence"))?;
    let Some(row) = row else { return Ok(None) };
    Ok(Some((
        row.try_get("id").map_err(db("read session"))?,
        row.try_get("seq").map_err(db("read sequence"))?,
    )))
}

pub(super) const CURSOR_VERSION: &str = "h1";

/// Cursor opaque gắn với đúng session + thứ tự `sequence`. Mỗi trang vẫn xác thực lại
/// scope; cursor không phải authorization token.
pub(super) fn encode_cursor(session_id: &str, last_sequence: u64) -> String {
    let raw = format!("{CURSOR_VERSION}|{session_id}|{last_sequence}");
    raw.bytes().map(|b| format!("{b:02x}")).collect()
}

pub(super) fn decode_cursor(cursor: &str, session_id: &str) -> Result<u64, OrchestratorError> {
    let bad = || OrchestratorError::new(ErrorCode::InvalidInput, "invalid cursor for this query");
    if !cursor.len().is_multiple_of(2) || !cursor.is_ascii() {
        return Err(bad());
    }
    let bytes: Result<Vec<u8>, _> = (0..cursor.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&cursor[i..i + 2], 16))
        .collect();
    let raw = String::from_utf8(bytes.map_err(|_| bad())?).map_err(|_| bad())?;
    let mut parts = raw.splitn(3, '|');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(v), Some(s), Some(seq)) if v == CURSOR_VERSION && s == session_id => {
            seq.parse::<u64>().map_err(|_| bad())
        }
        _ => Err(bad()),
    }
}
