CREATE TABLE IF NOT EXISTS llm_attempts (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES agent_runs(id),
    step INTEGER NOT NULL,
    started_at INTEGER NOT NULL DEFAULT (unixepoch()),
    first_byte_ms INTEGER,
    last_activity_ms INTEGER,
    chars_received INTEGER NOT NULL DEFAULT 0,
    outcome TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_attempts_run ON llm_attempts(run_id, step);
