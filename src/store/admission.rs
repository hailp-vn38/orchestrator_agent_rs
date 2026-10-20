//! Admission của turn (Root) và child turn (delegation depth 1).

use super::codec::{db, new_id, parse_policy, policy_json, text_json};
use super::types::{ChildAdmission, ChildAdmitError, ChildTarget, RunRoute};
use super::SessionStore;
use crate::error::{ErrorCode, OrchestratorError};
use crate::types::{AcceptedTurn, CallerContext, SessionPolicy};
use sqlx::Row;

impl SessionStore {
    /// Một transaction ngắn: cấp sequence, claim run `running` (partial unique index) và
    /// persist user message. Chỉ trả `AcceptedTurn` sau commit; không gọi LLM ở đây.
    pub(crate) async fn admit_turn(
        &self,
        caller: &CallerContext,
        session_id: &str,
        text: &str,
        resolve: impl Fn(&str) -> Option<RunRoute>,
    ) -> Result<AcceptedTurn, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(db("begin admission"))?;
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
        .fetch_optional(&mut *tx)
        .await
        .map_err(db("claim sequence"))?;
        let Some(row) = claimed else {
            let _ = tx.rollback().await;
            return Err(self.explain_rejection(caller, session_id).await);
        };
        let seq: i64 = row.try_get("seq").map_err(db("read sequence"))?;
        let agent_id: String = row.try_get("agent_id").map_err(db("read agent"))?;
        // Turn mới nhất đang chờ reconcile outcome không xác định: không được đi tiếp tới
        // model request kế tiếp (docs/18 §18.4.2 `RECOVERY_BLOCKED`).
        if super::recovery::session_awaits_reconciliation(&mut tx, session_id).await? {
            let _ = tx.rollback().await;
            return Err(OrchestratorError::new(
                ErrorCode::RecoveryBlocked,
                "the session has an interrupted tool call whose outcome must be resolved first",
            ));
        }
        let Some(route) = resolve(&agent_id) else {
            let _ = tx.rollback().await;
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "the session's agent is no longer configured",
            ));
        };

        let run_id = new_id();
        let turn_id = new_id();
        let inserted = sqlx::query(
            "INSERT INTO agent_runs(id, session_id, agent_id, turn_id, resume_attempt, \
             provider_id, model_id, provider_config_fingerprint, status) \
             VALUES(?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, 'running')",
        )
        .bind(&run_id)
        .bind(session_id)
        .bind(&agent_id)
        .bind(&turn_id)
        .bind(&route.provider_id)
        .bind(&route.model_id)
        .bind(&route.fingerprint)
        .execute(&mut *tx)
        .await;
        match inserted {
            Ok(_) => {}
            Err(sqlx::Error::Database(d)) if d.is_unique_violation() => {
                let _ = tx.rollback().await;
                return Err(OrchestratorError::new(
                    ErrorCode::SessionBusy,
                    "the session already has an active run",
                ));
            }
            Err(e) => return Err(db("claim run")(e)),
        }

        sqlx::query(
            "INSERT INTO messages(id, session_id, sequence, role, content_json) \
             VALUES(?1, ?2, ?3, 'user', ?4)",
        )
        .bind(new_id())
        .bind(session_id)
        .bind(seq)
        .bind(text_json(text))
        .execute(&mut *tx)
        .await
        .map_err(db("insert user message"))?;
        tx.commit().await.map_err(db("commit admission"))?;
        Ok(AcceptedTurn {
            session_id: session_id.to_owned(),
            turn_id,
        })
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
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(db("begin child admission"))?;
        match admit_child_inner(&mut tx, parent_invocation_id, target, prompt, resolve).await {
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

    let running: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM agent_runs WHERE id = ?1 AND status = 'running'")
            .bind(&run_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db("check parent run"))?;
    if running.is_none() {
        return Err(ChildAdmitError::ParentUnavailable);
    }
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
         provider_id, model_id, provider_config_fingerprint, status) \
         VALUES(?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, 'running')",
    )
    .bind(new_id())
    .bind(&child_session)
    .bind(&agent_id)
    .bind(&turn_id)
    .bind(&route.provider_id)
    .bind(&route.model_id)
    .bind(&route.fingerprint)
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
        "INSERT INTO messages(id, session_id, sequence, role, content_json) \
         VALUES(?1, ?2, ?3, 'user', ?4)",
    )
    .bind(new_id())
    .bind(&child_session)
    .bind(seq)
    .bind(text_json(prompt))
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
