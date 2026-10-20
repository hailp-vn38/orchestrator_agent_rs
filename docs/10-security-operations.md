# 10 — Security, Permissions & Operations

## 10.1. Trust model

User prompt, repository files, skill SKILL.md, tool results và subagent final text đều có thể chứa prompt injection. **Enforce quyền trong Rust `ToolExecutor`/`TaskManager`**, không dựa vào system prompt.

## 10.2. Permission & ownership

- Agent Definition có allowlists cho tools và skills; session chỉ được thu hẹp, không tự tăng quyền.
- ToolExecutor authorize mỗi call và resource; SkillManager lọc catalog **và** check lại trong `skill(id)`.
- Subagent tool check target mode, depth, workspace/user ownership, parent relationship và active run.
- File tools canonicalize đường dẫn, chống symlink/path traversal; skill resource files không được đọc ngoài policy.
- Child session không tự đọc parent history; workspace chung không có nghĩa tự được quyền sửa mọi file.

## 10.3. Secrets

Credentials từ environment/secret store. Không đưa keys, sensitive request headers, raw provider error bodies hoặc chain-of-thought vào SQLite/log/traces. Sanitize ToolError chuyển về Root.

## 10.4. Minimal tracing (MVP)

Sử dụng `tracing` spans làm parent-child hierarchy: run span có `session_id`, `run_id`, `agent_id`; subagent child span thêm `parent_session_id` và `tool_call_id`; model span thêm `provider_id`, `model_id`; tool span thêm name/status. Không ép mọi event mang tất cả identifiers.

`tracing` có thể ghi nội bộ thông tin `subagent.created/resumed` và `skill.loaded`; **SSE public không phát event riêng trùng lặp**: sử dụng `tool.*`/`session.changed`, `run.*`. Metrics tokens/cost/timing gắn vào event có dữ liệu; không tự tạo usage giả.

## 10.5. Runtime limits

Depth=1 trong MVP. `agent.steps` tùy chọn, mặc định không có step cap. Timeout chỉ áp dụng **từng** LLM request hoặc tool operation, không mặc định áp cho toàn session/child. Giữ permissions, output visibility cap, configurable quota/cost budget, explicit user cancellation và managed Tokio task ownership. **Không semantic auto-retry** hoặc tự replay mutating tool outcome unknown.

## 10.6. Recovery & safety tests

- Trace nối được Root → Subagent → Skill → Tool, kiểm tra secrets không bị leak.
- SSE + snapshot endpoints phải authorize root ownership/child ancestry/workspace; nhận child tạo sau subscription, không leak event của tenant khác. Redact tool args/results/secrets/private reasoning khỏi public events.
- Lagged SSE reader nhận resync signal/connection close; **không** làm nghẽn publisher/AgentRuntime. Event stream disconnect không đồng nghĩa explicit cancel.
- Parent cancel dừng child (nếu provider/tool hỗ trợ cancel) và persist trạng thái interrupted/cancelled thích hợp.
- Restart có thể tự resume safe execution cùng `turn_id` qua new attempt (bounded). Nếu pending mutating-tool outcome unknown thì **block và reconcile** trước khi chạy tiếp; không tự replay.
- SQLite admission bằng unique partial index: hai run đồng thời trên cùng session chỉ một được chấp nhận.

## 10.7. Realtime event privacy

Event payload cho UI độc lập với transcript/LLM payload: có redaction và giới hạn riêng, đặc biệt tool arguments, shell command outputs và provider metadata. Không stream hidden chain-of-thought, API credentials hay thông tin workspace ngoài quyền truy cập. Theo dõi family phải được xác thực với cùng owner/workspace cho mọi child, kể cả child phát sinh sau khi SSE bắt đầu. Xem [16](16-streaming-events.md).
