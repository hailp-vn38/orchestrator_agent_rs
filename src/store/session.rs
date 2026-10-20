//! Vòng đời session: mở DB, tạo/archive session, kiểm tra scope/owner, backup SQLite.

use super::codec::{db, new_id, policy_json};
use super::SessionStore;
use crate::error::{ErrorCode, OrchestratorError};
use crate::types::{CallerContext, SessionPolicy};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::Row;
use std::path::Path;
use std::time::Duration;

/// SQLite busy timeout cho writer contention (profile G0: transaction ngắn, pool nhỏ).
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const POOL_MAX_CONNECTIONS: u32 = 5;
/// Timeout chờ connection từ pool. Với feature `test-support` clock tokio có thể bị pause
/// (AT-11): clock ảo nhảy tới timer gần nhất mỗi khi runtime chờ thread của SQLite, nên
/// timer này phải luôn là timer xa nhất để không gây lỗi giả.
#[cfg(not(feature = "test-support"))]
const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(feature = "test-support")]
const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(24 * 3600);

impl SessionStore {
    /// Mở DB (WAL, FK, busy timeout) và chạy migration từ DB trống/đã có.
    pub(crate) async fn open(path: &Path) -> Result<Self, OrchestratorError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            .busy_timeout(BUSY_TIMEOUT);
        let pool = SqlitePoolOptions::new()
            .max_connections(POOL_MAX_CONNECTIONS)
            .acquire_timeout(POOL_ACQUIRE_TIMEOUT)
            .connect_with(options)
            .await
            .map_err(db("open pool"))?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| OrchestratorError::storage("migrate", &e))?;
        Ok(Self { pool })
    }

    pub(crate) async fn close(&self) {
        self.pool.close().await;
    }

    #[cfg(test)]
    pub(crate) async fn create_root_session(
        &self,
        caller: &CallerContext,
        agent_id: &str,
    ) -> Result<String, OrchestratorError> {
        self.create_root_session_with(caller, agent_id, &SessionPolicy::default())
            .await
    }

    pub(crate) async fn create_root_session_with(
        &self,
        caller: &CallerContext,
        agent_id: &str,
        policy: &SessionPolicy,
    ) -> Result<String, OrchestratorError> {
        let id = new_id();
        let permissions = policy_json(policy);
        sqlx::query(
            "INSERT INTO sessions(id, agent_id, owner_id, workspace_id, permissions_json) \
             VALUES(?1, ?2, ?3, ?4, ?5)",
        )
        .bind(&id)
        .bind(agent_id)
        .bind(&caller.owner_id)
        .bind(&caller.workspace_id)
        .bind(permissions)
        .execute(&self.pool)
        .await
        .map_err(db("create session"))?;
        Ok(id)
    }

    /// Idempotent. Run đang chạy không bị hủy; chỉ chặn admission mới.
    pub(crate) async fn archive_session(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<(), OrchestratorError> {
        let changed = sqlx::query(
            "UPDATE sessions SET status = 'archived', state_revision = state_revision + 1, \
             updated_at = unixepoch() \
             WHERE id = ?1 AND owner_id = ?2 AND workspace_id = ?3 AND status = 'open'",
        )
        .bind(session_id)
        .bind(&caller.owner_id)
        .bind(&caller.workspace_id)
        .execute(&self.pool)
        .await
        .map_err(db("archive session"))?
        .rows_affected();
        if changed == 1 {
            return Ok(());
        }
        match self.explain_rejection(caller, session_id).await {
            e if e.code() == ErrorCode::SessionArchived => Ok(()),
            e => Err(e),
        }
    }

    /// Kiểm tra session tồn tại và nằm trong scope của caller (không đọc/ghi gì thêm).
    pub(crate) async fn check_scope(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<(), OrchestratorError> {
        self.authorize(caller, session_id).await.map(|_| ())
    }

    /// Như [`Self::check_scope`] nhưng session phải là Root của một family (observation).
    pub(crate) async fn check_root_scope(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<(), OrchestratorError> {
        if self.authorize(caller, session_id).await?.is_child {
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "observation is addressed by the root session of a family",
            ));
        }
        Ok(())
    }

    /// Kiểm tra owner/workspace; dùng cho mọi read công khai.
    pub(super) async fn authorize(
        &self,
        caller: &CallerContext,
        session_id: &str,
    ) -> Result<SessionScope, OrchestratorError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(db("acquire connection"))?;
        authorize_on(&mut conn, caller, session_id).await
    }

    /// Resolve `root_session_id` của một session thuộc `(owner_id, workspace_id)`.
    pub(crate) async fn root_session_of(
        &self,
        owner_id: &str,
        workspace_id: &str,
        session_id: &str,
    ) -> Result<String, OrchestratorError> {
        let row = sqlx::query(
            "SELECT owner_id, workspace_id, COALESCE(parent_session_id, id) AS root_session_id \
             FROM sessions WHERE id = ?1",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db("load session root"))?
        .ok_or_else(|| OrchestratorError::new(ErrorCode::NotFound, "session not found"))?;
        let owner: String = row.try_get("owner_id").map_err(db("read owner"))?;
        let workspace: String = row.try_get("workspace_id").map_err(db("read workspace"))?;
        if owner != owner_id || workspace != workspace_id {
            return Err(OrchestratorError::new(
                ErrorCode::PermissionDenied,
                "the session is outside the caller's scope",
            ));
        }
        row.try_get("root_session_id")
            .map_err(db("read root_session_id"))
    }

    /// Tạo bản sao SQLite nhất quán tại `dest_db_path` bằng `VACUUM INTO` (an toàn khi WAL đang
    /// hoạt động) và trả về danh sách `(storage_key, size_bytes, sha256)` của toàn bộ committed
    /// artifacts có mặt trong bản snapshot đó.
    pub(crate) async fn backup_sqlite_to(
        &self,
        dest_db_path: &Path,
    ) -> Result<Vec<(String, u64, String)>, OrchestratorError> {
        let dest_str = dest_db_path.to_str().ok_or_else(|| {
            OrchestratorError::new(ErrorCode::InvalidInput, "backup path must be valid UTF-8")
        })?;
        sqlx::query("VACUUM INTO ?1")
            .bind(dest_str)
            .execute(&self.pool)
            .await
            .map_err(db("vacuum into backup db"))?;

        let opts = SqliteConnectOptions::new()
            .filename(dest_db_path)
            .read_only(true);
        let backup_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .map_err(db("open backup db"))?;
        let rows = sqlx::query("SELECT storage_key, size_bytes, sha256 FROM artifacts ORDER BY id")
            .fetch_all(&backup_pool)
            .await
            .map_err(db("list backup artifacts"))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let key: String = row.try_get("storage_key").map_err(db("read storage_key"))?;
            let size: i64 = row.try_get("size_bytes").map_err(db("read size_bytes"))?;
            let sha256: String = row.try_get("sha256").map_err(db("read sha256"))?;
            out.push((key, size.max(0) as u64, sha256));
        }
        backup_pool.close().await;
        Ok(out)
    }
}

/// Kiểm tra owner/workspace trên một connection cho trước (để dùng trong read transaction).
pub(super) async fn authorize_on(
    conn: &mut sqlx::SqliteConnection,
    caller: &CallerContext,
    session_id: &str,
) -> Result<SessionScope, OrchestratorError> {
    let row = sqlx::query(
        "SELECT owner_id, workspace_id, status, agent_id, parent_session_id \
         FROM sessions WHERE id = ?1",
    )
    .bind(session_id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db("load session"))?;
    let Some(row) = row else {
        return Err(OrchestratorError::new(
            ErrorCode::NotFound,
            "session not found",
        ));
    };
    let owner: String = row.try_get("owner_id").map_err(db("read owner"))?;
    let workspace: String = row.try_get("workspace_id").map_err(db("read workspace"))?;
    if owner != caller.owner_id || workspace != caller.workspace_id {
        return Err(OrchestratorError::new(
            ErrorCode::PermissionDenied,
            "the session is outside the caller's scope",
        ));
    }
    let status: String = row.try_get("status").map_err(db("read status"))?;
    let parent: Option<String> = row
        .try_get("parent_session_id")
        .map_err(db("read parent"))?;
    Ok(SessionScope {
        archived: status == "archived",
        is_child: parent.is_some(),
    })
}

pub(super) struct SessionScope {
    pub(super) archived: bool,
    pub(super) is_child: bool,
}
