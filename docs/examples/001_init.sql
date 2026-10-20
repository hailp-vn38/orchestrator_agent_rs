-- Orchestrator Agent RS: MVP example migration (not executed on production)
-- Application connection setup: PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    parent_session_id TEXT REFERENCES sessions(id) ON DELETE RESTRICT,
    agent_id TEXT NOT NULL,
    owner_id TEXT NOT NULL DEFAULT 'local',
    workspace_id TEXT NOT NULL DEFAULT 'default',
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','archived')),
    next_sequence INTEGER NOT NULL DEFAULT 1 CHECK (next_sequence >= 1),
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
    CHECK (id <> COALESCE(parent_session_id,''))
);
CREATE INDEX IF NOT EXISTS idx_sessions_parent ON sessions(parent_session_id);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE RESTRICT,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    role TEXT NOT NULL CHECK (role IN ('user','assistant','tool')),
    content_json TEXT NOT NULL CHECK (json_valid(content_json)),
    tool_call_id TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    UNIQUE (session_id, sequence)
);

CREATE TABLE IF NOT EXISTS agent_runs (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE RESTRICT,
    agent_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running','completed','failed','cancelled','timed_out','interrupted')),
    steps INTEGER NOT NULL DEFAULT 0 CHECK (steps >= 0),
    input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    output_tokens INTEGER NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    started_at INTEGER NOT NULL DEFAULT (unixepoch()),
    finished_at INTEGER,
    error_code TEXT,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_session ON agent_runs(session_id, started_at);
-- The only admission guard for concurrent runs in MVP.
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_running_run_per_session
    ON agent_runs(session_id) WHERE status = 'running';

CREATE TABLE IF NOT EXISTS tool_invocations (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE RESTRICT,
    run_id TEXT NOT NULL REFERENCES agent_runs(id) ON DELETE RESTRICT,
    tool_call_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    arguments_json TEXT NOT NULL CHECK (json_valid(arguments_json)),
    child_session_id TEXT REFERENCES sessions(id) ON DELETE RESTRICT,
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    status TEXT NOT NULL DEFAULT 'requested'
        CHECK (status IN ('requested','running','completed','failed','cancelled','interrupted')),
    error_code TEXT,
    started_at INTEGER NOT NULL DEFAULT (unixepoch()),
    finished_at INTEGER,
    UNIQUE (session_id, tool_call_id),
    CHECK (child_session_id IS NULL OR tool_name = 'subagent')
);
CREATE INDEX IF NOT EXISTS idx_invocations_run ON tool_invocations(run_id, started_at);
CREATE INDEX IF NOT EXISTS idx_invocations_child ON tool_invocations(child_session_id);

-- Enforced by SessionStore orchestration, not SQLite FK alone:
-- 1) child.parent_session_id = parent subagent invocation.session_id
-- 2) owner/workspace match; no parent cycles
-- 3) invocation.run_id references a run of the same invocation.session_id
-- 4) message sequence allocated + inserted atomically
-- 5) append result and finish invocation in the same transaction
-- 6) unknown side-effect outcomes MUST NOT be replayed after crash
