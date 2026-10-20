-- M5: audit của operator reconciliation (`resolve_interrupted_invocation`). Cột nullable trên
-- bảng `tool_invocations` hiện có; không thêm bảng. NULL = invocation không do operator resolve.
-- `resolution_evidence` là bằng chứng operator đã xác minh (bounded, không secret).
ALTER TABLE tool_invocations ADD COLUMN resolved_by TEXT;
ALTER TABLE tool_invocations ADD COLUMN resolution_evidence TEXT;
ALTER TABLE tool_invocations ADD COLUMN resolved_at INTEGER;

