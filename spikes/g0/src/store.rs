//! SQLite migration/contention probe + std::fs owner lock.

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;

const SCHEMA: &[&str] = &[
    "CREATE TABLE sessions(id TEXT PRIMARY KEY, status TEXT NOT NULL)",
    "CREATE TABLE messages(id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL REFERENCES sessions(id), role TEXT NOT NULL, body TEXT NOT NULL)",
    "CREATE TABLE agent_runs(id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id), turn_id TEXT NOT NULL, state TEXT NOT NULL)",
    "CREATE TABLE tool_invocations(id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES agent_runs(id), state TEXT NOT NULL)",
    "CREATE TABLE artifacts(id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id), bytes INTEGER NOT NULL)",
    // single-claim guard: at most one active run per session
    "CREATE UNIQUE INDEX one_active_run ON agent_runs(session_id) WHERE state='active'",
];

pub async fn open(path: &Path, busy: Duration) -> Result<SqlitePool, sqlx::Error> {
    let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(busy);
    SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(opts)
        .await
}

pub async fn migrate(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    for s in SCHEMA {
        sqlx::query(s).execute(&mut *tx).await?;
    }
    tx.commit().await
}

#[derive(Debug)]
pub struct ContentionResult {
    pub writers: usize,
    pub claims_won: usize,
    pub elapsed: Duration,
}

/// N concurrent claim attempts on one session: exactly one may win, no SQLITE_BUSY surfaces.
pub async fn claim_contention(
    pool: &SqlitePool,
    writers: usize,
) -> Result<ContentionResult, sqlx::Error> {
    sqlx::query("INSERT INTO sessions(id,status) VALUES('s1','open')")
        .execute(pool)
        .await?;
    let t = Instant::now();
    let mut hs = Vec::new();
    for i in 0..writers {
        let p = pool.clone();
        hs.push(tokio::spawn(async move {
            sqlx::query(
                "INSERT INTO agent_runs(id,session_id,turn_id,state) VALUES(?,?,?,'active')",
            )
            .bind(format!("r{i}"))
            .bind("s1")
            .bind(format!("t{i}"))
            .execute(&p)
            .await
            .map(|_| ())
        }));
    }
    let mut won = 0;
    for h in hs {
        match h.await.expect("join") {
            Ok(()) => won += 1,
            Err(sqlx::Error::Database(d)) if d.is_unique_violation() => {}
            Err(e) => return Err(e),
        }
    }
    Ok(ContentionResult {
        writers,
        claims_won: won,
        elapsed: t.elapsed(),
    })
}

pub fn try_owner_lock(path: &Path) -> std::io::Result<Option<File>> {
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?;
    // std::fs::File::try_lock (stable since Rust 1.89) replaces the fs4 dependency.
    match f.try_lock() {
        Ok(()) => Ok(Some(f)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(e)) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn migrate_fk_wal_and_single_claim() {
        let dir = tempfile::tempdir().unwrap();
        let pool = open(&dir.path().join("g0.db"), Duration::from_secs(5))
            .await
            .unwrap();
        let t = Instant::now();
        migrate(&pool).await.unwrap();
        println!("migration_ms={}", t.elapsed().as_millis());
        let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(mode, "wal");
        // FK enforced
        assert!(sqlx::query(
            "INSERT INTO messages(session_id,role,body) VALUES('nope','user','x')"
        )
        .execute(&pool)
        .await
        .is_err());
        let r = claim_contention(&pool, 32).await.unwrap();
        println!(
            "contention writers={} won={} elapsed_ms={}",
            r.writers,
            r.claims_won,
            r.elapsed.as_millis()
        );
        assert_eq!(r.claims_won, 1);
    }

    #[tokio::test]
    async fn close_reopen_reads_data() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("g0.db");
        let pool = open(&p, Duration::from_secs(5)).await.unwrap();
        migrate(&pool).await.unwrap();
        sqlx::query("INSERT INTO sessions(id,status) VALUES('a','open')")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let pool = open(&p, Duration::from_secs(5)).await.unwrap();
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sessions")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn owner_lock_is_exclusive_and_released_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("owner.lock");
        let a = try_owner_lock(&p).unwrap();
        assert!(a.is_some());
        assert!(try_owner_lock(&p).unwrap().is_none());
        drop(a);
        assert!(try_owner_lock(&p).unwrap().is_some());
    }
}
