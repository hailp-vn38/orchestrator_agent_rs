CREATE TABLE IF NOT EXISTS tool_operations (
    id TEXT PRIMARY KEY,
    invocation_id TEXT NOT NULL UNIQUE REFERENCES tool_invocations(id),
    provider_ref TEXT,
    account_key TEXT,
    remote_session_id TEXT,
    continuation_key TEXT,
    batch_index INTEGER NOT NULL DEFAULT 0,
    repairs_used INTEGER NOT NULL DEFAULT 0,
    batch_paths_json TEXT CHECK (batch_paths_json IS NULL OR json_valid(batch_paths_json)),
    user_turns_before INTEGER,
    request_json TEXT CHECK (request_json IS NULL OR json_valid(request_json)),
    status TEXT NOT NULL CHECK (status IN
        ('submitting','running','downloading','committed','cancelled','failed','expired')),
    artifact_ids_json TEXT CHECK (artifact_ids_json IS NULL OR json_valid(artifact_ids_json)),
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    remote_deleted_at INTEGER,
    last_activity_at INTEGER NOT NULL DEFAULT (unixepoch()),
    deadline_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX IF NOT EXISTS idx_tool_operations_open ON tool_operations(status)
    WHERE status IN ('submitting','running','downloading');
