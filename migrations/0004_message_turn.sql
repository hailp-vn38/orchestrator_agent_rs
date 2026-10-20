-- Read API (`list_turns`/`search_history`): gắn user input với logical turn của nó. Cột nullable
-- trên bảng `messages` hiện có; không thêm bảng. NULL = message không phải input của một turn
-- (assistant/tool/compaction) hoặc input được ghi trước migration này (không backfill).
ALTER TABLE messages ADD COLUMN turn_id TEXT;
CREATE INDEX IF NOT EXISTS idx_messages_turn ON messages(session_id, turn_id);

