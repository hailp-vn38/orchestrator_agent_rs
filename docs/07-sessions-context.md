# 07 — Sessions, Runs & Context

## 7.1. Khái niệm

| Identity | Mục đích | Lưu trữ |
|---|---|---|
| `session_id` | Hội thoại bền vững, có `parent_session_id`, có thể resume nhiều lần | `sessions` |
| `run_id` | Một attempt của AgentRuntime, có status/usage riêng | `agent_runs` |
| `tool_call_id` | Pair tool call/result trong một session | `tool_invocations` + `messages` |
| `child_session_id` | Child được gắn với một `subagent` tool invocation | `tool_invocations.child_session_id` |

Không có bảng `tasks` riêng ở MVP. `Session` không đồng nghĩa `Run`; child có thể có nhiều lượt run nối tiếp.

## 7.2. Parent–child hierarchy

```text
ses_root (agent=root, parent=NULL)
  ├── ses_explore (agent=explore, parent=ses_root)
  └── ses_review  (agent=reviewer, parent=ses_root)
```

Child không tự kế thừa toàn bộ parent history/loaded skill content. Root phải đưa các thông tin liên quan vào prompt khi delegate.

## 7.3. Session lifecycle

```mermaid
stateDiagram-v2
    [*] --> Open
    Open --> Archived
    Archived --> [*]
```

**Session status chỉ `open` hoặc `archived`** để tránh lưu trùng running state. **Run status** mới là `running/completed/failed/cancelled/timed_out/interrupted`. Session lỗi run vẫn có thể `open` và resume; archived không nhận run mới. Có thể truy vấn một session đang thực thi qua `agent_runs.status='running'`.

## 7.4. Message model

- `messages` ghi user, assistant, tool result với `content_json`, `sequence`; assistant tool calls thuộc assistant content và có `tool_call_id` khớp tool result.
- `next_sequence` được reserve atomically; `UNIQUE(session_id, sequence)` bảo đảm thứ tự.
- `AgentRuntime` là owner duy nhất ghi transcript; `ToolExecutor` và `TaskManager` không ghi messages thay runtime.
- Trước khi gọi LLM, không để dangling tool call/result pair còn `requested/running/interrupted` mà chưa reconcile.

## 7.5. Single active run / session

`SessionStore.start_run()` thực hiện SQLite `INSERT INTO agent_runs(status='running')` trong transaction ngắn. **Unique partial index** `(session_id) WHERE status='running'` quyết định admission; khi xung đột trả `SESSION_BUSY` thay vì đợi khóa ngầm. Không cần `sessions.active_run_id` hoặc per-session Tokio mutex thứ hai ở MVP.

Khi run hoàn thành, `finish_run()` ghi final message và cập nhật run status trong transaction thích hợp. Khi process restart (MVP single instance), trước khi nhận request mới: reconcile các run cũ `running -> interrupted`, invocation `requested/running -> interrupted`. Không tự replay mutation tool có thể đã tạo side effects.

## 7.6. Context construction

Chỉ **PromptBuilder.build(session_id)** tạo model context (base + agent + project instructions, skills catalog, history, available tools). Không tạo `ContextManager` riêng. Skill content đã load xuất hiện nhờ tool result trong history cùng session.

## 7.7. Context limit MVP / sau MVP

MVP ước lượng context và dự trữ output tokens. Nếu vượt, trả `CONTEXT_LIMIT` rõ ràng và vẫn giữ transcript bền vững. Không tự ý xóa history hoặc chèn summary suy đoán. Giai đoạn sau có thể thiết kế compaction preserving tool-call pairs, user intent và skill reinjection.

## 7.8. Ownership

`parent_session_id` cố định; mọi resume phải kiểm tra đúng parent, owner/workspace và permissions hiện tại. Một shared Rust process không có nghĩa nhiều users/sessions mặc nhiên tin cậy nhau.
