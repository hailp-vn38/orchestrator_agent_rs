//! Recovery & lifecycle trên execution store (docs/15 §15.7, docs/09 §9.5/§9.10, ADR-013/017).
//!
//! Mọi hàm chạy dưới **exclusive owner guard** của engine (lock giữ từ lúc build), nên không
//! có process nào khác quét/ghi cùng lúc. Quy tắc bất biến:
//! - Attempt cũ luôn đi tới `interrupted`; attempt mới giữ `turn_id`, `resume_attempt + 1`
//!   (bounded) và chỉ được tạo khi **an toàn**: không có invocation đã/đang chạy mà outcome
//!   chưa biết và không replay-safe, và routing identity (provider/model/fingerprint) không đổi.
//! - Outcome không xác định của tool có thể ghi **không bao giờ** bị replay hay bịa result;
//!   chỉ operator reconciliation (`resolve_invocation`) mới gỡ được.
//! - Attempt mới nhận lại các invocation chưa settle (đổi `run_id`, về `requested`) để chạy
//!   tiếp đúng thứ tự provider; message của attempt cũ giữ nguyên `run_id` (lineage).

use sqlx::Row;

use super::codec::{db, new_id, sanitize_message};
use super::{RunRoute, SessionStore};
use crate::error::{ErrorCode, OrchestratorError};
use crate::executor::{Settled, CODE_OUTCOME_UNKNOWN};

pub(crate) const CODE_INTERRUPTED: &str = "INTERRUPTED";
pub(crate) const CODE_SHUTTING_DOWN: &str = "SHUTTING_DOWN";
pub(crate) const CODE_CONFIG_CHANGED: &str = "PROVIDER_CONFIG_CHANGED";
pub(crate) const CODE_BLOCKED: &str = "RECOVERY_BLOCKED";
pub(crate) const CODE_EXHAUSTED: &str = "RECOVERY_ATTEMPTS_EXHAUSTED";
/// Supervisor bỏ cuộc sau nhiều lần liên tiếp không commit được (store ghi lỗi kéo dài).
/// Phục hồi được ở lần khởi động sau, theo đúng điều kiện an toàn như mọi attempt khác.
pub(crate) const CODE_STORE_UNAVAILABLE: &str = "STORE_UNAVAILABLE";
/// Child turn không được resume vì tool call `subagent` của parent đã có kết quả (parent đã
/// nhận lỗi và đi tiếp): không ai tiêu thụ output nữa. Không chặn admission của child session.
pub(crate) const CODE_DELEGATION_CLOSED: &str = "DELEGATION_CLOSED";
/// Operator xác nhận tool không tạo side effect: invocation thành technical failure.
pub(crate) const CODE_NO_EFFECT: &str = "TOOL_NO_EFFECT_CONFIRMED";

/// `error_code` của một attempt `interrupted` mà startup/operator được phép thử phục hồi.
/// `RECOVERY_ATTEMPTS_EXHAUSTED` cố ý không có ở đây: cần người xử lý, không vòng lặp vô hạn.
const RECOVERABLE_CODES: &str = "'INTERRUPTED','SHUTTING_DOWN','TOOL_OUTCOME_UNKNOWN',\
                                 'PROVIDER_CONFIG_CHANGED','RECOVERY_BLOCKED','STORE_UNAVAILABLE'";

/// Điều kiện của attempt đáng được phục hồi (tham số `r` là alias của `agent_runs`): attempt
/// mới nhất của turn mới nhất trong session, đã `interrupted` vì lý do phục hồi được, và
/// session không còn run `running`.
fn candidate_filter() -> String {
    format!(
        "r.status = 'interrupted' AND r.error_code IN ({RECOVERABLE_CODES}) \
         AND r.resume_attempt = (SELECT MAX(x.resume_attempt) FROM agent_runs x \
              WHERE x.session_id = r.session_id AND x.turn_id = r.turn_id) \
         AND NOT EXISTS (SELECT 1 FROM agent_runs z WHERE z.session_id = r.session_id \
              AND z.turn_id <> r.turn_id AND z.rowid > r.rowid) \
         AND NOT EXISTS (SELECT 1 FROM agent_runs y WHERE y.session_id = r.session_id \
              AND y.status = 'running')"
    )
}

/// Chính sách recovery do engine cung cấp (store không biết registry/tool).
pub(crate) struct RecoveryPolicy<'a> {
    /// Trần `resume_attempt` được phép tạo (attempt đầu tiên là 0).
    pub max_resume_attempt: u32,
    /// Routing hiện hành của agent; `None` nếu agent không còn được cấu hình.
    pub route: &'a (dyn Fn(&str) -> Option<RunRoute> + Sync),
    /// Tool có thể chạy lại an toàn dù outcome lần trước không xác định (skill, subagent đã
    /// idempotent theo invocation ID, tool ReadOnly). Tool không rõ → `false`.
    pub replay_safe: &'a (dyn Fn(&str) -> bool + Sync),
}

/// Kết luận phục hồi của một attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TurnRecovery {
    /// Đã tạo attempt mới (`running`) giữ nguyên `turn_id`.
    Resumed,
    /// Bị chặn: giữ `interrupted` với mã này; session/result phản ánh đúng trạng thái.
    Blocked(&'static str),
    /// Hết `resume_attempt`: cần người xử lý.
    Exhausted,
    /// Không còn điều kiện phục hồi (đã xử lý, session bận, turn đã cũ…).
    NotApplicable,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecoveryReport {
    pub resumed: u32,
    pub blocked: u32,
    pub exhausted: u32,
}

/// Quyết định của operator đã được chuẩn hóa để store ghi trong một transaction.
pub(crate) struct Resolution {
    pub operator_id: String,
    pub evidence: String,
    /// Provider call ID mà operator tuyên bố outcome thuộc về (nếu có) — phải khớp invocation.
    pub outcome_call_id: Option<String>,
    pub settled: Settled,
}

struct Unsettled {
    tool_name: String,
    status: String,
    child_session_id: Option<String>,
    child_turn_id: Option<String>,
}

impl SessionStore {
    /// Lúc build, dưới owner lock: run còn `running` từ process trước không còn owner nào.
    /// Chuyển `interrupted` (`INTERRUPTED`) và invocation `running` (outcome chưa biết) thành
    /// `interrupted`; invocation `requested` giữ nguyên (chưa từng chạy). Chưa tạo attempt mới.
    pub(crate) async fn mark_stale_runs_interrupted(&self) -> Result<u64, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin stale scan"))?;
        sqlx::query(
            "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id IN (SELECT session_id FROM agent_runs WHERE status = 'running')",
        )
        .execute(&mut *tx)
        .await
        .map_err(db("bump stale sessions"))?;
        sqlx::query(
            "UPDATE tool_invocations SET status = 'interrupted', error_code = ?1, \
             finished_at = unixepoch() WHERE status = 'running' \
             AND run_id IN (SELECT id FROM agent_runs WHERE status = 'running')",
        )
        .bind(CODE_INTERRUPTED)
        .execute(&mut *tx)
        .await
        .map_err(db("interrupt stale invocations"))?;
        let n = sqlx::query(
            "UPDATE agent_runs SET status = 'interrupted', error_code = ?1, \
             error_message = 'the process stopped before the run finished', \
             finished_at = unixepoch() WHERE status = 'running'",
        )
        .bind(CODE_INTERRUPTED)
        .execute(&mut *tx)
        .await
        .map_err(db("interrupt stale runs"))?
        .rows_affected();
        tx.commit().await.map_err(db("commit stale scan"))?;
        Ok(n)
    }

    /// Thử phục hồi mọi attempt đáng phục hồi; child session trước parent để parent thấy
    /// trạng thái đã quyết định của child.
    pub(crate) async fn recover_all(
        &self,
        policy: &RecoveryPolicy<'_>,
    ) -> Result<RecoveryReport, OrchestratorError> {
        let ids: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT r.id FROM agent_runs r JOIN sessions s ON s.id = r.session_id \
             WHERE {} ORDER BY (s.parent_session_id IS NULL), r.rowid",
            candidate_filter()
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(db("list recoverable runs"))?;
        let mut report = RecoveryReport::default();
        for id in ids {
            tally(&mut report, self.recover_run(&id, policy).await?);
        }
        Ok(report)
    }

    /// Sau khi operator resolve một invocation của `session_id`: thử phục hồi session đó rồi
    /// parent của nó (parent có thể đang chờ child bị chặn).
    pub(crate) async fn recover_after_resolution(
        &self,
        session_id: &str,
        policy: &RecoveryPolicy<'_>,
    ) -> Result<RecoveryReport, OrchestratorError> {
        let mut report = RecoveryReport::default();
        let mut next = Some(session_id.to_owned());
        while let Some(sid) = next.take() {
            let id: Option<String> = sqlx::query_scalar(&format!(
                "SELECT r.id FROM agent_runs r WHERE r.session_id = ?1 AND {}",
                candidate_filter()
            ))
            .bind(&sid)
            .fetch_optional(&self.pool)
            .await
            .map_err(db("find recoverable run"))?;
            if let Some(id) = id {
                tally(&mut report, self.recover_run(&id, policy).await?);
            }
            next = sqlx::query_scalar("SELECT parent_session_id FROM sessions WHERE id = ?1")
                .bind(&sid)
                .fetch_optional(&self.pool)
                .await
                .map_err(db("read parent session"))?
                .flatten();
        }
        Ok(report)
    }

    /// Một transaction: quyết định `Resumed`/`Blocked`/`Exhausted` cho attempt `run_id`.
    pub(crate) async fn recover_run(
        &self,
        run_id: &str,
        policy: &RecoveryPolicy<'_>,
    ) -> Result<TurnRecovery, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin recovery"))?;
        match recover_run_inner(&mut tx, run_id, policy).await {
            Ok(outcome) => {
                tx.commit().await.map_err(db("commit recovery"))?;
                Ok(outcome)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }

    /// Ghi kết quả đã được operator xác minh cho invocation `interrupted`: tool-result
    /// message, terminal update và audit trong **một** transaction. Không replay tool.
    pub(crate) async fn resolve_invocation(
        &self,
        scope: (&str, &str),
        session_id: &str,
        invocation_id: &str,
        resolution: &Resolution,
    ) -> Result<(), OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin resolution"))?;
        match resolve_inner(&mut tx, scope, session_id, invocation_id, resolution).await {
            Ok(()) => {
                tx.commit().await.map_err(db("commit resolution"))?;
                Ok(())
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }

    /// Run `running` của `session_id` và của child đang được invocation của nó chờ (depth 1).
    /// Thứ tự: chính session trước, child sau (parent phải dừng trước khi child bị hủy).
    pub(crate) async fn active_run_family(
        &self,
        session_id: &str,
    ) -> Result<Vec<String>, OrchestratorError> {
        let mut out: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM agent_runs WHERE session_id = ?1 AND status = 'running'",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db("find active run"))?;
        let children: Vec<String> = sqlx::query_scalar(
            "SELECT r.id FROM tool_invocations ti \
             JOIN agent_runs pr ON pr.id = ti.run_id AND pr.status = 'running' \
             JOIN agent_runs r ON r.session_id = ti.child_session_id \
                  AND r.turn_id = ti.child_turn_id AND r.status = 'running' \
             WHERE ti.session_id = ?1 AND ti.child_session_id IS NOT NULL \
             AND ti.status IN ('requested', 'running')",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db("find active children"))?;
        out.extend(children);
        Ok(out)
    }
}

/// Turn mới nhất của session đang chờ operator reconcile (outcome không xác định hoặc child
/// bị chặn). Dùng để từ chối input mới trước khi có model request tiếp theo.
pub(super) async fn session_awaits_reconciliation(
    tx: &mut sqlx::SqliteConnection,
    session_id: &str,
) -> Result<bool, OrchestratorError> {
    let blocked: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM agent_runs r WHERE r.session_id = ?1 AND r.status = 'interrupted' \
         AND r.error_code IN ('TOOL_OUTCOME_UNKNOWN', 'RECOVERY_BLOCKED') \
         AND r.resume_attempt = (SELECT MAX(x.resume_attempt) FROM agent_runs x \
              WHERE x.session_id = r.session_id AND x.turn_id = r.turn_id) \
         AND NOT EXISTS (SELECT 1 FROM agent_runs z WHERE z.session_id = r.session_id \
              AND z.turn_id <> r.turn_id AND z.rowid > r.rowid)",
    )
    .bind(session_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db("check reconciliation state"))?;
    Ok(blocked.is_some())
}

fn tally(report: &mut RecoveryReport, outcome: TurnRecovery) {
    match outcome {
        TurnRecovery::Resumed => report.resumed += 1,
        TurnRecovery::Blocked(_) => report.blocked += 1,
        TurnRecovery::Exhausted => report.exhausted += 1,
        TurnRecovery::NotApplicable => {}
    }
}

async fn recover_run_inner(
    tx: &mut sqlx::SqliteConnection,
    run_id: &str,
    policy: &RecoveryPolicy<'_>,
) -> Result<TurnRecovery, OrchestratorError> {
    // Ghi no-op đầu tiên để lấy write lock ngay (tránh SQLITE_BUSY_SNAPSHOT).
    let row = sqlx::query(
        "UPDATE agent_runs SET status = status WHERE id = ?1 \
         RETURNING session_id, agent_id, turn_id, resume_attempt, provider_id, model_id, \
         provider_config_fingerprint, error_code",
    )
    .bind(run_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db("lock recovery run"))?;
    let Some(row) = row else {
        return Ok(TurnRecovery::NotApplicable);
    };
    let eligible: Option<i64> = sqlx::query_scalar(&format!(
        "SELECT 1 FROM agent_runs r WHERE r.id = ?1 AND {}",
        candidate_filter()
    ))
    .bind(run_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db("check recovery eligibility"))?;
    if eligible.is_none() {
        return Ok(TurnRecovery::NotApplicable);
    }

    let session_id: String = row.try_get("session_id").map_err(db("read session"))?;
    let agent_id: String = row.try_get("agent_id").map_err(db("read agent"))?;
    let turn_id: String = row.try_get("turn_id").map_err(db("read turn"))?;
    let attempt: i64 = row.try_get("resume_attempt").map_err(db("read attempt"))?;
    let provider_id: String = row.try_get("provider_id").map_err(db("read provider"))?;
    let model_id: String = row.try_get("model_id").map_err(db("read model"))?;
    let fingerprint: Option<String> = row
        .try_get("provider_config_fingerprint")
        .map_err(db("read fingerprint"))?;
    let current_code: Option<String> = row.try_get("error_code").map_err(db("read code"))?;

    let unsettled = load_unsettled(tx, run_id).await?;
    let mut block: Option<(&'static str, &'static str)> = None;
    for inv in &unsettled {
        // `requested` chưa từng chạy; mọi trạng thái khác nghĩa là tool có thể đã tạo effect.
        if inv.status != "requested" && !(policy.replay_safe)(&inv.tool_name) {
            block = Some((
                CODE_OUTCOME_UNKNOWN,
                "a tool call started but its outcome was not recorded; an operator must resolve it",
            ));
            break;
        }
    }
    if block.is_none() {
        for inv in &unsettled {
            if let (Some(child), Some(turn)) = (&inv.child_session_id, &inv.child_turn_id) {
                if child_is_blocked(tx, child, turn).await? {
                    block = Some((
                        CODE_BLOCKED,
                        "the delegated subagent turn is blocked and must be recovered first",
                    ));
                    break;
                }
            }
        }
    }
    let route = (policy.route)(&agent_id);
    if block.is_none() {
        match &route {
            None => {
                block = Some((CODE_BLOCKED, "the run's agent is no longer configured"));
            }
            Some(r)
                if r.provider_id != provider_id
                    || r.model_id != model_id
                    || fingerprint.as_deref() != Some(r.fingerprint.as_str()) =>
            {
                block = Some((
                    CODE_CONFIG_CHANGED,
                    "the provider routing identity changed since the interrupted attempt; \
                     restore the original configuration to resume",
                ));
            }
            Some(_) => {}
        }
    }
    if let Some((code, message)) = block {
        set_blocked(
            tx,
            run_id,
            &session_id,
            current_code.as_deref(),
            code,
            message,
        )
        .await?;
        tracing::warn!(session_id = %session_id, run_id, code, "recovery blocked");
        return Ok(TurnRecovery::Blocked(code));
    }
    if delegation_closed(tx, &session_id, &turn_id).await? {
        set_blocked(
            tx,
            run_id,
            &session_id,
            current_code.as_deref(),
            CODE_DELEGATION_CLOSED,
            "the delegating tool call already has a result; the subagent turn was not resumed",
        )
        .await?;
        tracing::info!(session_id = %session_id, run_id, "child turn not resumed: delegation closed");
        return Ok(TurnRecovery::NotApplicable);
    }
    let route = route.expect("route checked above");

    let next = attempt + 1;
    if next > i64::from(policy.max_resume_attempt) {
        sqlx::query("UPDATE agent_runs SET error_code = ?1, error_message = ?2 WHERE id = ?3")
            .bind(CODE_EXHAUSTED)
            .bind("automatic recovery attempts are exhausted; the turn needs attention")
            .bind(run_id)
            .execute(&mut *tx)
            .await
            .map_err(db("mark attempts exhausted"))?;
        bump_revision(tx, &session_id).await?;
        tracing::warn!(session_id = %session_id, run_id, "recovery attempts exhausted");
        return Ok(TurnRecovery::Exhausted);
    }

    let new_run = new_id();
    let inserted = sqlx::query(
        "INSERT INTO agent_runs(id, session_id, agent_id, turn_id, resume_attempt, \
         provider_id, model_id, provider_config_fingerprint, status) \
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'running')",
    )
    .bind(&new_run)
    .bind(&session_id)
    .bind(&agent_id)
    .bind(&turn_id)
    .bind(next)
    .bind(&route.provider_id)
    .bind(&route.model_id)
    .bind(&route.fingerprint)
    .execute(&mut *tx)
    .await;
    match inserted {
        Ok(_) => {}
        Err(sqlx::Error::Database(d)) if d.is_unique_violation() => {
            return Ok(TurnRecovery::NotApplicable);
        }
        Err(e) => return Err(db("create recovery attempt")(e)),
    }
    // Invocation chưa settle chuyển sang attempt mới và chạy lại theo thứ tự provider.
    sqlx::query(
        "UPDATE tool_invocations SET run_id = ?1, status = 'requested', error_code = NULL, \
         finished_at = NULL WHERE run_id = ?2 AND status IN ('requested', 'running', 'interrupted')",
    )
    .bind(&new_run)
    .bind(run_id)
    .execute(&mut *tx)
    .await
    .map_err(db("move invocations"))?;
    bump_revision(tx, &session_id).await?;
    tracing::info!(
        session_id = %session_id,
        turn_id = %turn_id,
        previous_run_id = run_id,
        run_id = %new_run,
        resume_attempt = next,
        "interrupted turn resumed as a new attempt"
    );
    Ok(TurnRecovery::Resumed)
}

async fn load_unsettled(
    tx: &mut sqlx::SqliteConnection,
    run_id: &str,
) -> Result<Vec<Unsettled>, OrchestratorError> {
    let rows = sqlx::query(
        "SELECT tool_name, status, child_session_id, child_turn_id FROM tool_invocations \
         WHERE run_id = ?1 AND status IN ('requested', 'running', 'interrupted') ORDER BY rowid",
    )
    .bind(run_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(db("load unsettled invocations"))?;
    rows.iter()
        .map(|r| {
            Ok(Unsettled {
                tool_name: r.try_get("tool_name").map_err(db("read tool"))?,
                status: r.try_get("status").map_err(db("read status"))?,
                child_session_id: r.try_get("child_session_id").map_err(db("read child"))?,
                child_turn_id: r.try_get("child_turn_id").map_err(db("read child turn"))?,
            })
        })
        .collect()
}

/// Tool call `subagent` của parent đã giao `child_turn` và đã có kết quả cuối (parent đã đi
/// tiếp). `false` cho root session hoặc khi parent vẫn đang chờ (vd. cùng bị ngắt do crash).
async fn delegation_closed(
    tx: &mut sqlx::SqliteConnection,
    child_session: &str,
    child_turn: &str,
) -> Result<bool, OrchestratorError> {
    let closed: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM tool_invocations WHERE child_session_id = ?1 AND child_turn_id = ?2 \
         AND status IN ('completed', 'failed', 'cancelled')",
    )
    .bind(child_session)
    .bind(child_turn)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db("read delegating invocation"))?;
    Ok(closed.is_some())
}

/// Child turn đang ở trạng thái chờ operator/config (không phải terminal cuối cùng).
async fn child_is_blocked(
    tx: &mut sqlx::SqliteConnection,
    child_session: &str,
    child_turn: &str,
) -> Result<bool, OrchestratorError> {
    let row = sqlx::query(
        "SELECT status, error_code FROM agent_runs WHERE session_id = ?1 AND turn_id = ?2 \
         ORDER BY resume_attempt DESC LIMIT 1",
    )
    .bind(child_session)
    .bind(child_turn)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db("read child attempt"))?;
    let Some(row) = row else { return Ok(false) };
    let status: String = row.try_get("status").map_err(db("read child status"))?;
    let code: Option<String> = row.try_get("error_code").map_err(db("read child code"))?;
    Ok(status == "interrupted"
        && matches!(
            code.as_deref(),
            Some(CODE_OUTCOME_UNKNOWN | CODE_CONFIG_CHANGED | CODE_BLOCKED)
        ))
}

async fn set_blocked(
    tx: &mut sqlx::SqliteConnection,
    run_id: &str,
    session_id: &str,
    current: Option<&str>,
    code: &str,
    message: &str,
) -> Result<(), OrchestratorError> {
    if current == Some(code) {
        return Ok(());
    }
    sqlx::query("UPDATE agent_runs SET error_code = ?1, error_message = ?2 WHERE id = ?3")
        .bind(code)
        .bind(sanitize_message(message))
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db("mark blocked"))?;
    bump_revision(tx, session_id).await
}

async fn bump_revision(
    tx: &mut sqlx::SqliteConnection,
    session_id: &str,
) -> Result<(), OrchestratorError> {
    sqlx::query(
        "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
         WHERE id = ?1",
    )
    .bind(session_id)
    .execute(&mut *tx)
    .await
    .map_err(db("bump revision"))?;
    Ok(())
}

async fn resolve_inner(
    tx: &mut sqlx::SqliteConnection,
    scope: (&str, &str),
    session_id: &str,
    invocation_id: &str,
    resolution: &Resolution,
) -> Result<(), OrchestratorError> {
    let not_found = || OrchestratorError::new(ErrorCode::NotFound, "invocation not found");
    // Ghi no-op để lấy write lock và đọc invocation trong cùng snapshot.
    let row = sqlx::query(
        "UPDATE tool_invocations SET status = status WHERE id = ?1 \
         RETURNING session_id, run_id, tool_call_id, status",
    )
    .bind(invocation_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db("lock invocation"))?
    .ok_or_else(not_found)?;
    let inv_session: String = row.try_get("session_id").map_err(db("read session"))?;
    if inv_session != session_id {
        return Err(not_found());
    }
    let owner = sqlx::query("SELECT owner_id, workspace_id FROM sessions WHERE id = ?1")
        .bind(session_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db("load session scope"))?
        .ok_or_else(not_found)?;
    let (owner_id, workspace_id): (String, String) = (
        owner.try_get("owner_id").map_err(db("read owner"))?,
        owner
            .try_get("workspace_id")
            .map_err(db("read workspace"))?,
    );
    if owner_id != scope.0 || workspace_id != scope.1 {
        return Err(OrchestratorError::new(
            ErrorCode::PermissionDenied,
            "the session is outside the operator's scope",
        ));
    }

    let status: String = row.try_get("status").map_err(db("read status"))?;
    let run_id: String = row.try_get("run_id").map_err(db("read run"))?;
    let call_id: String = row.try_get("tool_call_id").map_err(db("read call id"))?;
    let invalid = |m: &str| OrchestratorError::new(ErrorCode::InvalidInput, m.to_owned());
    if status != "interrupted" {
        return Err(invalid("the invocation is not awaiting resolution"));
    }
    // Chỉ attempt mới nhất của turn mới nhất, đã `interrupted`, mới đang chờ reconcile.
    let awaiting: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM agent_runs r WHERE r.id = ?1 AND r.status = 'interrupted' \
         AND r.resume_attempt = (SELECT MAX(x.resume_attempt) FROM agent_runs x \
              WHERE x.session_id = r.session_id AND x.turn_id = r.turn_id) \
         AND NOT EXISTS (SELECT 1 FROM agent_runs z WHERE z.session_id = r.session_id \
              AND z.turn_id <> r.turn_id AND z.rowid > r.rowid)",
    )
    .bind(&run_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db("check awaiting run"))?;
    if awaiting.is_none() {
        return Err(invalid("the invocation is not awaiting resolution"));
    }
    if let Some(claimed) = &resolution.outcome_call_id {
        if claimed != &call_id {
            return Err(invalid("the outcome does not belong to this invocation"));
        }
    }

    let failed = resolution.settled.failed;
    sqlx::query(
        "UPDATE tool_invocations SET status = ?2, result_json = ?3, error_code = ?4, \
         finished_at = unixepoch(), resolved_by = ?5, resolution_evidence = ?6, \
         resolved_at = unixepoch() WHERE id = ?1",
    )
    .bind(invocation_id)
    .bind(if failed { "failed" } else { "completed" })
    .bind(&resolution.settled.result_json)
    .bind(&resolution.settled.error_code)
    .bind(&resolution.operator_id)
    .bind(&resolution.evidence)
    .execute(&mut *tx)
    .await
    .map_err(db("resolve invocation"))?;
    let seq: i64 = sqlx::query_scalar(
        "UPDATE sessions SET next_sequence = next_sequence + 1, \
         state_revision = state_revision + 1, updated_at = unixepoch() \
         WHERE id = ?1 RETURNING next_sequence - 1",
    )
    .bind(session_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(db("claim resolution sequence"))?;
    sqlx::query(
        "INSERT INTO messages(id, session_id, sequence, role, content_json, \
         tool_call_id, tool_invocation_id, run_id) \
         VALUES(?1, ?2, ?3, 'tool', ?4, ?5, ?6, ?7)",
    )
    .bind(new_id())
    .bind(session_id)
    .bind(seq)
    .bind(
        serde_json::json!({ "text": resolution.settled.model_text, "is_error": failed })
            .to_string(),
    )
    .bind(&call_id)
    .bind(invocation_id)
    .bind(&run_id)
    .execute(&mut *tx)
    .await
    .map_err(db("insert resolution result"))?;
    Ok(())
}
