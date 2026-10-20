//! Read API độc lập với runtime: liệt kê/tìm session, run, turn và lịch sử xuyên session.
//! Chỉ đọc. Mọi truy vấn bị ràng buộc `(owner_id, workspace_id)` của caller trong chính SQL
//! (hoặc qua `authorize` cho địa chỉ theo id); cursor là keyset trên `rowid`, gắn với loại
//! truy vấn và không phải authorization token.

use sqlx::Row;

use super::codec::{db, text_of};
use super::session::authorize_on;
use super::transcript::{decode_cursor, encode_cursor, message_from_row};
use super::SessionStore;
use crate::error::{ErrorCode, OrchestratorError};
use crate::types::{
    CallerContext, HistoryHit, HistorySearch, MessageRole, Page, PageRequest, ResultError,
    RunStatus, RunSummary, SessionFilter, SessionSummary, TurnSummary,
};

/// Độ dài tối đa của từ khóa tìm kiếm (ký tự).
const MAX_QUERY_CHARS: usize = 256;
/// Độ dài tối đa của snippet (ký tự) và số ký tự giữ trước vị trí khớp.
const SNIPPET_CHARS: usize = 160;
const SNIPPET_LEAD: usize = 60;

const SESSION_COLUMNS: &str =
    "s.rowid AS rid, s.id, s.parent_session_id, s.agent_id, s.status, s.state_revision, \
     s.created_at, s.updated_at";

const RUN_COLUMNS: &str = "r.rowid AS rid, r.id, r.session_id, r.turn_id, r.agent_id, \
     r.resume_attempt, r.provider_id, r.model_id, r.status, r.steps, r.input_tokens, \
     r.output_tokens, r.started_at, r.finished_at, r.final_message_id, r.error_code, \
     r.error_message";

fn page_limit(page: &PageRequest, max_page: u32) -> u32 {
    page.limit.clamp(1, max_page)
}

fn cursor_after(page: &PageRequest, scope: &str) -> Result<Option<i64>, OrchestratorError> {
    page.cursor
        .as_deref()
        .map(|c| decode_cursor(c, scope).map(|v| v as i64))
        .transpose()
}

/// Cắt `rows` về `limit` và sinh cursor từ `rid` của phần tử cuối nếu còn trang sau.
fn paginate<T>(mut items: Vec<(i64, T)>, limit: u32, scope: &str) -> Page<T> {
    let next_cursor = if items.len() > limit as usize {
        items.truncate(limit as usize);
        items
            .last()
            .map(|(rid, _)| encode_cursor(scope, *rid as u64))
    } else {
        None
    };
    Page {
        items: items.into_iter().map(|(_, v)| v).collect(),
        next_cursor,
    }
}

/// Từ khóa → mẫu `LIKE` đã escape (`\` là ký tự escape).
fn like_pattern(text: &str) -> Result<String, OrchestratorError> {
    let text = text.trim();
    if text.is_empty() || text.chars().count() > MAX_QUERY_CHARS {
        return Err(OrchestratorError::new(
            ErrorCode::InvalidInput,
            "search text must be 1..=256 characters",
        ));
    }
    let mut out = String::with_capacity(text.len() + 2);
    out.push('%');
    for c in text.chars() {
        if matches!(c, '%' | '_' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('%');
    Ok(out)
}

fn role_str(role: MessageRole) -> &'static str {
    match role {
        MessageRole::User => "user",
        MessageRole::Assistant => "assistant",
        MessageRole::Tool => "tool",
    }
}

fn snippet_of(text: &str, needle: &str) -> String {
    let at = text
        .to_ascii_lowercase()
        .find(&needle.trim().to_ascii_lowercase())
        .map_or(0, |byte| text[..byte].chars().count());
    let start = at.saturating_sub(SNIPPET_LEAD);
    let body: String = text.chars().skip(start).take(SNIPPET_CHARS).collect();
    let mut out = String::new();
    if start > 0 {
        out.push('…');
    }
    out.push_str(&body);
    if text.chars().skip(start).count() > SNIPPET_CHARS {
        out.push('…');
    }
    out
}

fn session_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<SessionSummary, OrchestratorError> {
    let status: String = row.try_get("status").map_err(db("read session"))?;
    Ok(SessionSummary {
        session_id: row.try_get("id").map_err(db("read session"))?,
        parent_session_id: row
            .try_get("parent_session_id")
            .map_err(db("read session"))?,
        agent_id: row.try_get("agent_id").map_err(db("read session"))?,
        archived: status == "archived",
        state_revision: row.try_get("state_revision").map_err(db("read session"))?,
        created_at: row.try_get("created_at").map_err(db("read session"))?,
        updated_at: row.try_get("updated_at").map_err(db("read session"))?,
    })
}

fn run_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<RunSummary, OrchestratorError> {
    let status: String = row.try_get("status").map_err(db("read run"))?;
    let attempt: i64 = row.try_get("resume_attempt").map_err(db("read run"))?;
    let steps: i64 = row.try_get("steps").map_err(db("read run"))?;
    let input: i64 = row.try_get("input_tokens").map_err(db("read run"))?;
    let output: i64 = row.try_get("output_tokens").map_err(db("read run"))?;
    let code: Option<String> = row.try_get("error_code").map_err(db("read run"))?;
    let message: Option<String> = row.try_get("error_message").map_err(db("read run"))?;
    Ok(RunSummary {
        run_id: row.try_get("id").map_err(db("read run"))?,
        session_id: row.try_get("session_id").map_err(db("read run"))?,
        turn_id: row.try_get("turn_id").map_err(db("read run"))?,
        agent_id: row.try_get("agent_id").map_err(db("read run"))?,
        attempt: u32::try_from(attempt).unwrap_or(u32::MAX),
        provider_id: row.try_get("provider_id").map_err(db("read run"))?,
        model_id: row.try_get("model_id").map_err(db("read run"))?,
        status: RunStatus::parse(&status)
            .ok_or_else(|| OrchestratorError::new(ErrorCode::Internal, "unknown run status"))?,
        steps: u32::try_from(steps).unwrap_or(u32::MAX),
        input_tokens: u64::try_from(input).unwrap_or_default(),
        output_tokens: u64::try_from(output).unwrap_or_default(),
        started_at: row.try_get("started_at").map_err(db("read run"))?,
        finished_at: row.try_get("finished_at").map_err(db("read run"))?,
        final_message_id: row.try_get("final_message_id").map_err(db("read run"))?,
        error: code.map(|code| ResultError {
            code,
            message: message.unwrap_or_default(),
        }),
    })
}

impl SessionStore {
    /// Root session của caller, mới tạo trước. `text` khớp `agent_id` hoặc
    /// nội dung user/assistant của bất kỳ session nào trong family.
    pub(crate) async fn list_root_sessions(
        &self,
        caller: &CallerContext,
        filter: &SessionFilter,
        text: Option<&str>,
        page: PageRequest,
        max_page: u32,
    ) -> Result<Page<SessionSummary>, OrchestratorError> {
        let like = text.map(like_pattern).transpose()?;
        let after = cursor_after(&page, "sessions")?;
        let limit = page_limit(&page, max_page);
        let sql = format!(
            "SELECT {SESSION_COLUMNS} FROM sessions s \
             WHERE s.owner_id = ?1 AND s.workspace_id = ?2 AND s.parent_session_id IS NULL \
             AND (?3 IS NULL OR s.agent_id = ?3) \
             AND (?4 IS NULL OR s.status = ?4) \
             AND (?5 IS NULL OR s.rowid < ?5) \
             AND (?6 IS NULL OR s.agent_id LIKE ?6 ESCAPE '\\' OR EXISTS ( \
                  SELECT 1 FROM messages m JOIN sessions c ON c.id = m.session_id \
                  WHERE (c.id = s.id OR c.parent_session_id = s.id) \
                  AND m.role IN ('user', 'assistant') \
                  AND json_extract(m.content_json, '$.text') LIKE ?6 ESCAPE '\\')) \
             ORDER BY s.rowid DESC LIMIT ?7"
        );
        let status = filter.archived.map(|a| if a { "archived" } else { "open" });
        let rows = sqlx::query(&sql)
            .bind(&caller.owner_id)
            .bind(&caller.workspace_id)
            .bind(filter.agent_id.as_deref())
            .bind(status)
            .bind(after)
            .bind(like.as_deref())
            .bind(i64::from(limit) + 1)
            .fetch_all(&self.pool)
            .await
            .map_err(db("list sessions"))?;
        let items = rows
            .iter()
            .map(|r| {
                Ok((
                    r.try_get("rid").map_err(db("read session"))?,
                    session_from_row(r)?,
                ))
            })
            .collect::<Result<Vec<_>, OrchestratorError>>()?;
        Ok(paginate(items, limit, "sessions"))
    }

    pub(crate) async fn get_session(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<SessionSummary, OrchestratorError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(db("acquire connection"))?;
        authorize_on(&mut conn, caller, session_id).await?;
        let sql = format!("SELECT {SESSION_COLUMNS} FROM sessions s WHERE s.id = ?1");
        let row = sqlx::query(&sql)
            .bind(session_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db("load session"))?
            .ok_or_else(|| OrchestratorError::new(ErrorCode::NotFound, "session not found"))?;
        session_from_row(&row)
    }

    /// Mọi attempt của session theo thứ tự tạo; `turn_id` thu hẹp về một turn (gồm retry).
    pub(crate) async fn list_runs(
        &self,
        caller: &CallerContext,
        session_id: &str,
        turn_id: Option<&str>,
        page: PageRequest,
        max_page: u32,
    ) -> Result<Page<RunSummary>, OrchestratorError> {
        self.authorize(caller, session_id).await?;
        let scope = format!("runs:{session_id}:{}", turn_id.unwrap_or(""));
        let after = cursor_after(&page, &scope)?.unwrap_or(0);
        let limit = page_limit(&page, max_page);
        let sql = format!(
            "SELECT {RUN_COLUMNS} FROM agent_runs r \
             WHERE r.session_id = ?1 AND (?2 IS NULL OR r.turn_id = ?2) AND r.rowid > ?3 \
             ORDER BY r.rowid LIMIT ?4"
        );
        let rows = sqlx::query(&sql)
            .bind(session_id)
            .bind(turn_id)
            .bind(after)
            .bind(i64::from(limit) + 1)
            .fetch_all(&self.pool)
            .await
            .map_err(db("list runs"))?;
        let items = rows
            .iter()
            .map(|r| Ok((r.try_get("rid").map_err(db("read run"))?, run_from_row(r)?)))
            .collect::<Result<Vec<_>, OrchestratorError>>()?;
        Ok(paginate(items, limit, &scope))
    }

    pub(crate) async fn get_run(
        &self,
        caller: &CallerContext,
        run_id: &str,
    ) -> Result<RunSummary, OrchestratorError> {
        let sql = format!("SELECT {RUN_COLUMNS} FROM agent_runs r WHERE r.id = ?1");
        let row = sqlx::query(&sql)
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db("load run"))?
            .ok_or_else(|| OrchestratorError::new(ErrorCode::NotFound, "run not found"))?;
        let run = run_from_row(&row)?;
        self.authorize(caller, &run.session_id).await?;
        Ok(run)
    }

    /// Turn của session theo thứ tự tạo, mỗi turn kèm attempt mới nhất và input của user.
    pub(crate) async fn list_turns(
        &self,
        caller: &CallerContext,
        session_id: &str,
        page: PageRequest,
        max_page: u32,
    ) -> Result<Page<TurnSummary>, OrchestratorError> {
        self.authorize(caller, session_id).await?;
        let scope = format!("turns:{session_id}");
        let after = cursor_after(&page, &scope)?.unwrap_or(0);
        let limit = page_limit(&page, max_page);
        let turns = sqlx::query(
            "SELECT turn_id, MIN(rowid) AS first, COUNT(*) AS attempts FROM agent_runs \
             WHERE session_id = ?1 GROUP BY turn_id HAVING MIN(rowid) > ?2 \
             ORDER BY first LIMIT ?3",
        )
        .bind(session_id)
        .bind(after)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await
        .map_err(db("list turns"))?;

        let mut items = Vec::with_capacity(turns.len());
        for turn in &turns {
            let first: i64 = turn.try_get("first").map_err(db("read turn"))?;
            let turn_id: String = turn.try_get("turn_id").map_err(db("read turn"))?;
            let attempts: i64 = turn.try_get("attempts").map_err(db("read turn"))?;
            // Phần tử thừa chỉ để biết còn trang sau; không cần dựng chi tiết.
            if items.len() >= limit as usize {
                items.push((first, None));
                continue;
            }
            let sql = format!(
                "SELECT {RUN_COLUMNS} FROM agent_runs r WHERE r.session_id = ?1 \
                 AND r.turn_id = ?2 ORDER BY r.resume_attempt DESC LIMIT 1"
            );
            let row = sqlx::query(&sql)
                .bind(session_id)
                .bind(&turn_id)
                .fetch_one(&self.pool)
                .await
                .map_err(db("load latest attempt"))?;
            let latest_run = run_from_row(&row)?;
            let input: Option<String> = sqlx::query_scalar(
                "SELECT content_json FROM messages \
                 WHERE session_id = ?1 AND turn_id = ?2 AND role = 'user' LIMIT 1",
            )
            .bind(session_id)
            .bind(&turn_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db("load turn input"))?;
            let output = self.load_result(session_id, &turn_id).await?.output;
            items.push((
                first,
                Some(TurnSummary {
                    session_id: session_id.to_owned(),
                    turn_id,
                    input: input.map(|j| text_of(&j)),
                    attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
                    latest_run,
                    output,
                }),
            ));
        }
        let page = paginate(items, limit, &scope);
        Ok(Page {
            items: page.items.into_iter().flatten().collect(),
            next_cursor: page.next_cursor,
        })
    }

    /// Tìm message theo từ khóa trong mọi session thuộc scope của caller, mới nhất trước.
    pub(crate) async fn search_history(
        &self,
        caller: &CallerContext,
        query: &HistorySearch,
        page: PageRequest,
        max_page: u32,
    ) -> Result<Page<HistoryHit>, OrchestratorError> {
        let like = like_pattern(&query.text)?;
        if let Some(root) = &query.root_session_id {
            self.check_root_scope(caller, root).await?;
        }
        let after = cursor_after(&page, "history")?;
        let limit = page_limit(&page, max_page);
        let rows = sqlx::query(
            "SELECT m.rowid AS rid, m.id, m.sequence, m.role, m.content_json, m.run_id, \
             m.tool_call_id, s.id AS sid, COALESCE(s.parent_session_id, s.id) AS root \
             FROM messages m JOIN sessions s ON s.id = m.session_id \
             WHERE s.owner_id = ?1 AND s.workspace_id = ?2 \
             AND m.role IN ('user', 'assistant', 'tool') \
             AND (?3 IS NULL OR m.role = ?3) \
             AND (?4 IS NULL OR s.id = ?4 OR s.parent_session_id = ?4) \
             AND (?5 IS NULL OR m.rowid < ?5) \
             AND json_extract(m.content_json, '$.text') LIKE ?6 ESCAPE '\\' \
             ORDER BY m.rowid DESC LIMIT ?7",
        )
        .bind(&caller.owner_id)
        .bind(&caller.workspace_id)
        .bind(query.role.map(role_str))
        .bind(query.root_session_id.as_deref())
        .bind(after)
        .bind(&like)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await
        .map_err(db("search history"))?;
        let items = rows
            .iter()
            .map(|r| {
                let message = message_from_row(r)?;
                let snippet = snippet_of(&message.text, &query.text);
                Ok((
                    r.try_get("rid").map_err(db("read hit"))?,
                    HistoryHit {
                        session_id: r.try_get("sid").map_err(db("read hit"))?,
                        root_session_id: r.try_get("root").map_err(db("read hit"))?,
                        message,
                        snippet,
                    },
                ))
            })
            .collect::<Result<Vec<_>, OrchestratorError>>()?;
        Ok(paginate(items, limit, "history"))
    }
}
