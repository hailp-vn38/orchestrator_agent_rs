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

Các sự kiện hữu ích: `run.started/finished`, `tool.started/finished`, `subagent.created/resumed`, `skill.loaded`. Metrics tokens/cost/timing gắn vào event có dữ liệu tương ứng; không chế số token chính xác nếu provider không trả.

## 10.5. Runtime limits

Depth=1, max steps, delegation count, timeout và tool-result visibility limits. Foreground child cancellation phải truyền từ parent; không spawn task không có owner. **Không semantic auto-retry**. Transient provider retry có thể thêm sau, nhưng không tự retry mutating tool khi kết quả không chắc chắn.

## 10.6. Recovery & safety tests

- Trace nối được Root → Subagent → Skill → Tool, kiểm tra secrets không bị leak.
- Parent cancel dừng child (nếu provider/tool hỗ trợ cancel) và persist trạng thái interrupted/cancelled thích hợp.
- Restart đánh dấu run cũ `interrupted`, giữ incomplete tool invocation để reconcile, không tự replay side effects.
- SQLite admission bằng unique partial index: hai run đồng thời trên cùng session chỉ một được chấp nhận.
