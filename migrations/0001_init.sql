-- Orchestrator Agent RS: v5.0 fresh-install migration 0001 (copied from docs/examples/001_init.sql).
-- Connection setup is done by SessionStore::open: foreign_keys=ON, journal_mode=WAL, busy_timeout.

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    parent_session_id TEXT REFERENCES sessions(id) ON DELETE RESTRICT,
    agent_id TEXT NOT NULL,
    owner_id TEXT NOT NULL DEFAULT 'local',
    workspace_id TEXT NOT NULL DEFAULT 'default',
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','archived')),
    next_sequence INTEGER NOT NULL DEFAULT 1 CHECK (next_sequence >= 1),
    state_revision INTEGER NOT NULL DEFAULT 0 CHECK (state_revision >= 0),
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
    CHECK (id <> COALESCE(parent_session_id,''))
);
CREATE INDEX IF NOT EXISTS idx_sessions_parent ON sessions(parent_session_id);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE RESTRICT,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    role TEXT NOT NULL CHECK (role IN ('user','assistant','tool','synthetic','compaction')),
    content_json TEXT NOT NULL CHECK (json_valid(content_json)),
    tool_call_id TEXT, -- provider ID, not session-wide unique
    tool_invocation_id TEXT REFERENCES tool_invocations(id), -- set on tool result messages
    run_id TEXT REFERENCES agent_runs(id), -- optional for input/synthetic; required by runner for assistant/tool
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    UNIQUE (session_id, sequence)
);

CREATE TABLE IF NOT EXISTS agent_runs (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE RESTRICT,
    agent_id TEXT NOT NULL,
    turn_id TEXT NOT NULL, -- stable across restart recovery attempts
    resume_attempt INTEGER NOT NULL DEFAULT 0 CHECK (resume_attempt >= 0),
    provider_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    provider_config_fingerprint TEXT, -- non-secret routing identity per attempt
    final_message_id TEXT REFERENCES messages(id), -- confirmed final from this attempt
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
CREATE INDEX IF NOT EXISTS idx_runs_turn ON agent_runs(session_id, turn_id, resume_attempt);
-- The only admission guard for concurrent runs in MVP.
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_running_run_per_session
    ON agent_runs(session_id) WHERE status = 'running';

CREATE TABLE IF NOT EXISTS tool_invocations (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE RESTRICT,
    run_id TEXT NOT NULL REFERENCES agent_runs(id) ON DELETE RESTRICT,
    tool_call_id TEXT NOT NULL, -- provider call ID scoped by assistant_message_id
    assistant_message_id TEXT NOT NULL REFERENCES messages(id),
    tool_name TEXT NOT NULL,
    arguments_json TEXT NOT NULL CHECK (json_valid(arguments_json)),
    child_session_id TEXT REFERENCES sessions(id) ON DELETE RESTRICT,
    child_turn_id TEXT, -- logical turn in linked child session
    checkpoint_artifact_id TEXT, -- committed/verified immutable manifest pointer
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    status TEXT NOT NULL DEFAULT 'requested'
        CHECK (status IN ('requested','running','completed','failed','cancelled','interrupted')),
    error_code TEXT,
    started_at INTEGER NOT NULL DEFAULT (unixepoch()),
    finished_at INTEGER,
    UNIQUE (assistant_message_id, tool_call_id),
    CHECK (child_session_id IS NULL OR tool_name = 'subagent'),
    CHECK (child_turn_id IS NULL OR (child_session_id IS NOT NULL AND tool_name = 'subagent'))
);
CREATE INDEX IF NOT EXISTS idx_invocations_run ON tool_invocations(run_id, started_at);
CREATE INDEX IF NOT EXISTS idx_invocations_child ON tool_invocations(child_session_id, child_turn_id);
CREATE INDEX IF NOT EXISTS idx_invocations_assistant ON tool_invocations(assistant_message_id);
CREATE INDEX IF NOT EXISTS idx_messages_tool_invocation ON messages(tool_invocation_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_invocation_per_child_turn
    ON tool_invocations(child_session_id, child_turn_id) WHERE child_turn_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE RESTRICT,
    turn_id TEXT NOT NULL,
    producer_invocation_id TEXT REFERENCES tool_invocations(id), -- internal stable invocation key
    role TEXT NOT NULL CHECK(role IN ('primary','intermediate')),
    name TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
    sha256 TEXT NOT NULL,
    storage_key TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS idx_artifacts_turn ON artifacts(workspace_id, turn_id, created_at, id);

-- Enforced by SessionStore orchestration, not SQLite FK alone:
-- 1) child.parent_session_id = parent subagent invocation.session_id
-- 2) owner/workspace match; no parent cycles
-- 3) invocation.run_id references a run of the same invocation.session_id
-- 4) message sequence allocated + inserted atomically
-- 5) append result and finish invocation in the same transaction
-- 6) unknown side-effect outcomes MUST NOT be replayed after crash
-- 15) parent invocation -> child session/turn admission atomic and idempotent
-- 16) assistant_message_id belongs to same run/session; tool ID scoped to message
--     every newly persisted role='tool' result carries tool_invocation_id
--     pointing to the same invocation whose provider ID is copied to tool_call_id
-- 17) final_message_id belongs to same run/session and is committed with terminal status
-- 18) checkpoint pointer CAS only after verified artifact publication
--     artifact producer attribution uses internal producer_invocation_id, not provider tool_call_id
-- 19) process owner guard released only after local write-capable tasks quiesce/join

-- Recovery/compaction invariants enforced in Rust SessionStore:
-- 7) durable input + active run claim before 202 Accepted
-- 8) old running -> interrupted, then new attempt with same turn_id and resume_attempt+1
-- 9) mutating tool outcome unknown => block; never replay without reconciliation
-- 10) messages.role='compaction' JSON has summary/keep_from_sequence/version;
--     original messages remain stored and checkpoint frontier preserves complete tool pairs

-- Streaming/reconnect invariants (no event_log table):
-- 11) Every transaction that changes committed session-visible state bumps
--     sessions.state_revision once per affected session; publish SSE terminal
--     events only AFTER commit using the returned revision.
-- 12) sessions.next_sequence is only for message ordering; not UI state revision.
-- 13) ephemeral model text/tool progress deltas are not stored in SQLite.
-- 14) family_snapshot reads parent/child canonical states in one read transaction.
-- 20) fresh-install schema has forward FK references among messages/runs/invocations;
--     nullable result/final pointers are populated transactionally after row admission;
--     this SQL is not an in-place ALTER migration.
