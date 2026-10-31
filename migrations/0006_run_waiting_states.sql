-- migrations/0006_run_waiting_states.sql
-- Chạy với foreign_keys=OFF; xem SessionStore::open.
DROP INDEX IF EXISTS idx_runs_session;
DROP INDEX IF EXISTS idx_runs_turn;
DROP INDEX IF EXISTS idx_one_running_run_per_session;

CREATE TABLE agent_runs_new (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE RESTRICT,
    agent_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    resume_attempt INTEGER NOT NULL DEFAULT 0 CHECK (resume_attempt >= 0),
    provider_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    provider_config_fingerprint TEXT,
    final_message_id TEXT REFERENCES messages(id),
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN (
            'running',
            'completed',
            'failed',
            'cancelled',
            'timed_out',
            'interrupted',
            'waiting_retry',
            'waiting_user'
        )),
    steps INTEGER NOT NULL DEFAULT 0 CHECK (steps >= 0),
    input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    output_tokens INTEGER NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    started_at INTEGER NOT NULL DEFAULT (unixepoch()),
    finished_at INTEGER,
    error_code TEXT,
    error_message TEXT,
    next_attempt_at INTEGER,
    retry_reason TEXT,
    CHECK ((status = 'waiting_retry') = (next_attempt_at IS NOT NULL))
);

INSERT INTO agent_runs_new (
    rowid,
    id,
    session_id,
    agent_id,
    turn_id,
    resume_attempt,
    provider_id,
    model_id,
    provider_config_fingerprint,
    final_message_id,
    status,
    steps,
    input_tokens,
    output_tokens,
    started_at,
    finished_at,
    error_code,
    error_message
)
SELECT
    rowid,
    id,
    session_id,
    agent_id,
    turn_id,
    resume_attempt,
    provider_id,
    model_id,
    provider_config_fingerprint,
    final_message_id,
    status,
    steps,
    input_tokens,
    output_tokens,
    started_at,
    finished_at,
    error_code,
    error_message
FROM agent_runs;

DROP TABLE agent_runs;
ALTER TABLE agent_runs_new RENAME TO agent_runs;

CREATE INDEX idx_runs_session ON agent_runs(session_id, started_at);
CREATE INDEX idx_runs_turn ON agent_runs(session_id, turn_id, resume_attempt);
CREATE UNIQUE INDEX idx_one_active_run_per_session ON agent_runs(session_id)
    WHERE status IN ('running','waiting_retry','waiting_user');
CREATE INDEX idx_runs_next_attempt ON agent_runs(next_attempt_at)
    WHERE status = 'waiting_retry';
