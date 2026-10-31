//! Admission của turn (Root) và child turn (delegation depth 1).

use super::artifact::PublishError;
use super::codec::{budget_json, db, new_id, parse_budget, parse_policy, policy_json, text_json};
use super::run::{turn_active_secs_on, turn_tokens_on};
use super::types::{ChildAdmission, ChildAdmitError, ChildTarget, RunRoute};
use super::SessionStore;
use crate::error::{ErrorCode, OrchestratorError};
use crate::types::{AcceptedTurn, ArtifactRef, CallerContext, RunBudget, SessionPolicy};
use sqlx::Row;

enum AdmitInTxError {
    Rejected,
    Other(OrchestratorError),
}

impl SessionStore {
    /// Một transaction ngắn: cấp sequence, claim run `running` (partial unique index) và
    /// persist user message. Chỉ trả `AcceptedTurn` sau commit; không gọi LLM ở đây.
    #[cfg(test)]
    pub(crate) async fn admit_turn(
        &self,
        caller: &CallerContext,
        session_id: &str,
        text: &str,
        resolve: impl Fn(&str) -> Option<RunRoute>,
    ) -> Result<AcceptedTurn, OrchestratorError> {
        self.admit_turn_with_artifacts(caller, session_id, text, resolve, &[], 0, None)
            .await
    }

    /// Admit turn và đồng thời INSERT danh sách `staged` artifacts vào đúng `(session_id, turn_id)`
    /// vừa cấp phát trong cùng một write transaction SQLite.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn admit_turn_with_artifacts(
        &self,
        caller: &CallerContext,
        session_id: &str,
        text: &str,
        resolve: impl Fn(&str) -> Option<RunRoute>,
        staged: &[(ArtifactRef, String)],
        workspace_quota_bytes: u64,
        budget: Option<&RunBudget>,
    ) -> Result<AcceptedTurn, OrchestratorError> {
        let now = self.now_secs();
        let mut tx = self.pool.begin().await.map_err(db("begin admission"))?;
        let (turn, _run_id) =
            match Self::admit_turn_in_tx(&mut tx, caller, session_id, text, resolve, now, budget)
                .await
            {
                Ok(pair) => pair,
                Err(AdmitInTxError::Rejected) => {
                    let _ = tx.rollback().await;
                    return Err(self.explain_rejection(caller, session_id).await);
                }
                Err(AdmitInTxError::Other(err)) => {
                    let _ = tx.rollback().await;
                    return Err(err);
                }
            };

        let record_scope = super::artifact::ArtifactRecordScope {
            owner_id: &caller.owner_id,
            workspace_id: &caller.workspace_id,
            session_id,
            turn_id: &turn.turn_id,
            producer_invocation_id: None,
        };
        for (reference, storage_key) in staged {
            if let Err(err) = Self::insert_artifact_in_tx(
                &mut tx,
                &record_scope,
                reference,
                storage_key,
                workspace_quota_bytes,
            )
            .await
            {
                let _ = tx.rollback().await;
                let mapped = match err {
                    PublishError::QuotaExceeded => OrchestratorError::new(
                        ErrorCode::Unavailable,
                        format!(
                            "{}: workspace artifact storage quota exceeded",
                            crate::artifact::CODE_WORKSPACE_QUOTA_EXCEEDED
                        ),
                    ),
                    PublishError::Db(msg) => OrchestratorError::new(
                        ErrorCode::Unavailable,
                        format!("{}: {msg}", crate::artifact::CODE_ARTIFACT_UNAVAILABLE),
                    ),
                    PublishError::SessionOutOfScope
                    | PublishError::InvocationInactive
                    | PublishError::TurnNotInSession => OrchestratorError::new(
                        ErrorCode::PermissionDenied,
                        "artifact publication denied",
                    ),
                };
                return Err(mapped);
            }
        }

        tx.commit().await.map_err(db("commit admission"))?;
        Ok(turn)
    }

    async fn admit_turn_in_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        caller: &CallerContext,
        session_id: &str,
        text: &str,
        resolve: impl Fn(&str) -> Option<RunRoute>,
        now: i64,
        budget: Option<&RunBudget>,
    ) -> Result<(AcceptedTurn, String), AdmitInTxError> {
        let claimed = sqlx::query(
            "UPDATE sessions SET next_sequence = next_sequence + 1, \
             state_revision = state_revision + 1, updated_at = unixepoch() \
             WHERE id = ?1 AND owner_id = ?2 AND workspace_id = ?3 AND status = 'open' \
             AND parent_session_id IS NULL \
             RETURNING next_sequence - 1 AS seq, agent_id",
        )
        .bind(session_id)
        .bind(&caller.owner_id)
        .bind(&caller.workspace_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| AdmitInTxError::Other(db("claim sequence")(e)))?;
        let Some(row) = claimed else {
            return Err(AdmitInTxError::Rejected);
        };
        let seq: i64 = row
            .try_get("seq")
            .map_err(|e| AdmitInTxError::Other(db("read sequence")(e)))?;
        let agent_id: String = row
            .try_get("agent_id")
            .map_err(|e| AdmitInTxError::Other(db("read agent")(e)))?;
        // Turn mới nhất đang chờ reconcile outcome không xác định: không được đi tiếp tới
        // model request kế tiếp (docs/18 §18.4.2 `RECOVERY_BLOCKED`).
        if super::recovery::session_awaits_reconciliation(tx, session_id)
            .await
            .map_err(AdmitInTxError::Other)?
        {
            return Err(AdmitInTxError::Other(OrchestratorError::new(
                ErrorCode::RecoveryBlocked,
                "the session has an interrupted tool call whose outcome must be resolved first",
            )));
        }
        let Some(route) = resolve(&agent_id) else {
            return Err(AdmitInTxError::Other(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "the session's agent is no longer configured",
            )));
        };

        let run_id = new_id();
        let turn_id = new_id();
        let run_started_at = budget.map_or(now, |b| b.started_at);
        let serialized_budget = budget.map(budget_json);
        let inserted = sqlx::query(
            "INSERT INTO agent_runs(id, session_id, agent_id, turn_id, resume_attempt, \
             provider_id, model_id, provider_config_fingerprint, status, started_at, budget_json) \
             VALUES(?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, 'running', ?8, ?9)",
        )
        .bind(&run_id)
        .bind(session_id)
        .bind(&agent_id)
        .bind(&turn_id)
        .bind(&route.provider_id)
        .bind(&route.model_id)
        .bind(&route.fingerprint)
        .bind(run_started_at)
        .bind(serialized_budget)
        .execute(&mut **tx)
        .await;
        match inserted {
            Ok(_) => {}
            Err(sqlx::Error::Database(d)) if d.is_unique_violation() => {
                return Err(AdmitInTxError::Other(OrchestratorError::new(
                    ErrorCode::SessionBusy,
                    "the session already has an active run",
                )));
            }
            Err(e) => return Err(AdmitInTxError::Other(db("claim run")(e))),
        }

        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json, turn_id) \
             VALUES(?1, ?2, ?3, 'user', ?4, ?5)",
        )
        .bind(new_id())
        .bind(session_id)
        .bind(seq)
        .bind(text_json(text))
        .bind(&turn_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| AdmitInTxError::Other(db("insert user message")(e)))?;
        Ok((
            AcceptedTurn {
                session_id: session_id.to_owned(),
                turn_id,
            },
            run_id,
        ))
    }

    /// Một transaction ngắn (docs/06 §6.3): verify ownership/ancestry, append child input,
    /// claim child run và persist `child_session_id/child_turn_id` lên parent invocation.
    /// `tool_invocations.id` là idempotency key: gọi lại cho cùng invocation trả đúng turn đã
    /// admit (kể cả khi child đã terminal), không append/claim thêm. Invocation mới luôn tạo
    /// logical turn mới dù prompt giống hệt. Không await LLM trong transaction.
    pub(crate) async fn admit_child_turn(
        &self,
        parent_invocation_id: &str,
        target: ChildTarget<'_>,
        prompt: &str,
        resolve: impl Fn(&str) -> Option<RunRoute>,
    ) -> Result<ChildAdmission, ChildAdmitError> {
        let now = self.now_secs();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(db("begin child admission"))?;
        match admit_child_inner(&mut tx, parent_invocation_id, target, prompt, resolve, now).await {
            Ok(admission) => {
                tx.commit().await.map_err(db("commit child admission"))?;
                Ok(admission)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }

    /// Lý do admission/ghi bị từ chối, phân loại theo scope trước rồi tới trạng thái.
    pub(super) async fn explain_rejection(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> OrchestratorError {
        match self.authorize(caller, session_id).await {
            Err(e) => e,
            // Child chỉ nhận input qua `subagent` của parent (có link + idempotency key).
            Ok(s) if s.is_child => OrchestratorError::new(
                ErrorCode::PermissionDenied,
                "a child session only accepts input through its parent",
            ),
            Ok(s) if s.archived => {
                OrchestratorError::new(ErrorCode::SessionArchived, "the session is archived")
            }
            Ok(_) => OrchestratorError::new(ErrorCode::Internal, "turn admission was rejected"),
        }
    }
}

/// Thân của [`SessionStore::admit_child_turn`]; caller commit/rollback. Câu lệnh đầu tiên là
/// một ghi no-op để lấy write lock ngay (tránh `SQLITE_BUSY_SNAPSHOT`) và đọc link hiện có.
pub(super) async fn admit_child_inner(
    conn: &mut sqlx::SqliteConnection,
    invocation_id: &str,
    target: ChildTarget<'_>,
    prompt: &str,
    resolve: impl Fn(&str) -> Option<RunRoute>,
    now: i64,
) -> Result<ChildAdmission, ChildAdmitError> {
    let link = sqlx::query(
        "UPDATE tool_invocations SET status = status WHERE id = ?1 \
         RETURNING session_id, run_id, tool_name, child_session_id, child_turn_id",
    )
    .bind(invocation_id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db("lock parent invocation"))?;
    let Some(link) = link else {
        return Err(ChildAdmitError::ParentUnavailable);
    };
    let tool_name: String = link.try_get("tool_name").map_err(db("read tool name"))?;
    let parent_session: String = link.try_get("session_id").map_err(db("read session"))?;
    let run_id: String = link.try_get("run_id").map_err(db("read run"))?;
    let linked_session: Option<String> = link
        .try_get("child_session_id")
        .map_err(db("read child session"))?;
    let linked_turn: Option<String> = link
        .try_get("child_turn_id")
        .map_err(db("read child turn"))?;
    if tool_name != "subagent" {
        return Err(ChildAdmitError::ParentUnavailable);
    }
    if let (Some(session_id), Some(turn_id)) = (linked_session, linked_turn) {
        return Ok(ChildAdmission {
            session_id,
            turn_id,
            created: false,
        });
    }

    let parent_run = sqlx::query(
        "SELECT turn_id, budget_json FROM agent_runs WHERE id = ?1 AND status = 'running'",
    )
    .bind(&run_id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db("check parent run"))?;
    let Some(parent_run) = parent_run else {
        return Err(ChildAdmitError::ParentUnavailable);
    };
    let parent_turn_id: String = parent_run
        .try_get("turn_id")
        .map_err(db("read parent turn"))?;
    let parent_budget_raw: Option<String> = parent_run
        .try_get("budget_json")
        .map_err(db("read parent budget"))?;
    let child_budget_json = if let Some(parent_budget) = parse_budget(parent_budget_raw.as_deref())
    {
        let used_active = turn_active_secs_on(
            &mut *conn,
            &parent_session,
            &parent_turn_id,
            &parent_budget,
            now,
        )
        .await?;
        let used_tokens = turn_tokens_on(&mut *conn, &parent_session, &parent_turn_id).await?;
        let remaining_active = parent_budget.active_limit_secs.saturating_sub(used_active);
        let child_budget = RunBudget {
            started_at: now,
            total_deadline_at: parent_budget
                .total_deadline_at
                .saturating_sub(parent_budget.reserve_secs.min(i64::MAX as u64) as i64),
            active_limit_secs: remaining_active.saturating_sub(parent_budget.reserve_secs),
            reserve_secs: parent_budget.reserve_secs,
            max_tokens: parent_budget
                .max_tokens
                .map(|m| m.saturating_sub(used_tokens)),
            waited_secs: 0,
        };
        Some(budget_json(&child_budget))
    } else {
        None
    };
    let parent = sqlx::query(
        "SELECT parent_session_id, owner_id, workspace_id, permissions_json \
         FROM sessions WHERE id = ?1",
    )
    .bind(&parent_session)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db("load parent session"))?
    .ok_or(ChildAdmitError::ParentUnavailable)?;
    let grandparent: Option<String> = parent
        .try_get("parent_session_id")
        .map_err(db("read parent link"))?;
    if grandparent.is_some() {
        return Err(ChildAdmitError::DepthLimit);
    }
    let owner: String = parent.try_get("owner_id").map_err(db("read owner"))?;
    let workspace: String = parent
        .try_get("workspace_id")
        .map_err(db("read workspace"))?;
    let parent_permissions: Option<String> = parent
        .try_get("permissions_json")
        .map_err(db("read policy"))?;
    // Quyền của child = AgentDefinition(child) ∩ trần do host đặt cho child ở root session;
    // policy dành cho chính Root không áp xuống child.
    let parent_policy = parse_policy(parent_permissions.as_deref());
    let permissions = policy_json(&SessionPolicy {
        allowed_tools: parent_policy.child_allowed_tools,
        allowed_skills: parent_policy.child_allowed_skills,
        ..SessionPolicy::default()
    });

    let (child_session, agent_id) = match target {
        ChildTarget::New { agent_id } => {
            let id = new_id();
            // Child chỉ mang trần quyền host đặt cho child (không kế thừa policy của Root).
            sqlx::query(
                "INSERT INTO sessions(id, parent_session_id, agent_id, owner_id, workspace_id, \
                 permissions_json) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .bind(&id)
            .bind(&parent_session)
            .bind(agent_id)
            .bind(&owner)
            .bind(&workspace)
            .bind(&permissions)
            .execute(&mut *conn)
            .await
            .map_err(db("create child session"))?;
            (id, agent_id.to_owned())
        }
        ChildTarget::Resume {
            session_id,
            agent_id,
        } => {
            let row = sqlx::query(
                "SELECT agent_id, parent_session_id, owner_id, workspace_id, status \
                 FROM sessions WHERE id = ?1",
            )
            .bind(session_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db("load child session"))?;
            // Không phân biệt "không tồn tại" với "của parent/owner khác": không lộ sự tồn tại.
            let Some(row) = row else {
                return Err(ChildAdmitError::NotChild);
            };
            let row_parent: Option<String> = row
                .try_get("parent_session_id")
                .map_err(db("read child parent"))?;
            let row_owner: String = row.try_get("owner_id").map_err(db("read owner"))?;
            let row_workspace: String =
                row.try_get("workspace_id").map_err(db("read workspace"))?;
            if row_parent.as_deref() != Some(parent_session.as_str())
                || row_owner != owner
                || row_workspace != workspace
            {
                return Err(ChildAdmitError::NotChild);
            }
            let status: String = row.try_get("status").map_err(db("read status"))?;
            if status == "archived" {
                return Err(ChildAdmitError::Archived);
            }
            let child_agent: String = row.try_get("agent_id").map_err(db("read agent"))?;
            if child_agent != agent_id {
                return Err(ChildAdmitError::AgentMismatch);
            }
            if super::recovery::session_awaits_reconciliation(&mut *conn, session_id).await? {
                return Err(ChildAdmitError::Blocked);
            }
            (session_id.to_owned(), child_agent)
        }
    };

    let route = resolve(&agent_id).ok_or(ChildAdmitError::RouteUnavailable)?;
    let seq: i64 = sqlx::query_scalar(
        "UPDATE sessions SET next_sequence = next_sequence + 1, \
         state_revision = state_revision + 1, updated_at = unixepoch() \
         WHERE id = ?1 RETURNING next_sequence - 1",
    )
    .bind(&child_session)
    .fetch_one(&mut *conn)
    .await
    .map_err(db("claim child sequence"))?;
    let turn_id = new_id();
    let inserted = sqlx::query(
        "INSERT INTO agent_runs(id, session_id, agent_id, turn_id, resume_attempt, \
         provider_id, model_id, provider_config_fingerprint, status, started_at, budget_json) \
         VALUES(?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, 'running', ?8, ?9)",
    )
    .bind(new_id())
    .bind(&child_session)
    .bind(&agent_id)
    .bind(&turn_id)
    .bind(&route.provider_id)
    .bind(&route.model_id)
    .bind(&route.fingerprint)
    .bind(now)
    .bind(child_budget_json)
    .execute(&mut *conn)
    .await;
    match inserted {
        Ok(_) => {}
        Err(sqlx::Error::Database(d)) if d.is_unique_violation() => {
            return Err(ChildAdmitError::Busy);
        }
        Err(e) => return Err(db("claim child run")(e).into()),
    }
    sqlx::query(
        "INSERT INTO messages(id, session_id, sequence, role, content_json, turn_id) \
         VALUES(?1, ?2, ?3, 'user', ?4, ?5)",
    )
    .bind(new_id())
    .bind(&child_session)
    .bind(seq)
    .bind(text_json(prompt))
    .bind(&turn_id)
    .execute(&mut *conn)
    .await
    .map_err(db("insert child input"))?;
    sqlx::query(
        "UPDATE tool_invocations SET child_session_id = ?2, child_turn_id = ?3 WHERE id = ?1",
    )
    .bind(invocation_id)
    .bind(&child_session)
    .bind(&turn_id)
    .execute(&mut *conn)
    .await
    .map_err(db("link child turn"))?;
    // Link mới là state parent-visible: bump revision của parent đúng một lần.
    sqlx::query(
        "UPDATE sessions SET state_revision = state_revision + 1, updated_at = unixepoch() \
         WHERE id = ?1",
    )
    .bind(&parent_session)
    .execute(&mut *conn)
    .await
    .map_err(db("bump parent revision"))?;
    Ok(ChildAdmission {
        session_id: child_session,
        turn_id,
        created: true,
    })
}
