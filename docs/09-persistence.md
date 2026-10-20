# 09 — SQLite Persistence (MVP)

## 9.1. Quyết định

SQLite + SQLx, WAL, `foreign_keys=ON`, `busy_timeout`, một shared Rust process. `SessionStore` sở hữu toàn bộ queries và transaction invariants. Không thêm Redis, queue broker hoặc event store trong MVP.

**Migration mẫu:** [examples/001_init.sql](examples/001_init.sql).

## 9.2. Bốn bảng tối thiểu

| Bảng | Dữ liệu |
|---|---|
| `sessions` | `id`, `parent_session_id`, agent, owner/workspace, `status=open/archived`, `next_sequence`. |
| `messages` | Ordered transcript, `role`, `content_json`, optional tool call ID. |
| `agent_runs` | Attempt, model/provider, `running/completed/failed/cancelled/timed_out/interrupted`, usage. |
| `tool_invocations` | Tool call input, execution state, result/error và optional `child_session_id` cho `subagent`. |

**Không có `tasks` table**: `tool_invocations` với `tool_name='subagent'` chính là một delegation record. Một child session có thể được liên kết tới nhiều invocations ở các lần resume. Không có trùng lặp task status/output.

## 9.3. Constraints và ownership

- `UNIQUE(session_id, sequence)` bảo đảm message ordering.
- `UNIQUE(session_id, tool_call_id)` bảo đảm tool call ID duy nhất trong một session.
- `CREATE UNIQUE INDEX ... ON agent_runs(session_id) WHERE status='running'` là **single active run admission**. Không dùng `sessions.active_run_id`, không dùng cùng lúc một per-session mutex khác ở MVP.
- `child_session_id` là nullable FK đến `sessions(id)`. Khi không null, `tool_name` phải là `subagent`; app kiểm tra `child.parent_session_id = parent invocation.session_id` và owner/workspace.
- `session.status` chỉ `open/archived`. Failed/timed-out là **run state**, không phải session state.

## 9.4. Transactions quan trọng

### Start run

Một transaction ngắn: assert session `open`, insert `agent_runs(status='running')`. Nếu vi phạm partial unique index trả `SESSION_BUSY`. **Không await LLM/tool trong transaction**.

### Append message

Một transaction: lấy và tăng `sessions.next_sequence`, insert message đúng sequence; `AgentRuntime` là writer ở mức workflow. ToolExecutor trả kết quả về, không tự append history.

### Tool call + result

1. AgentRuntime persist assistant message có tool calls và `tool_invocations` trạng thái `requested` **trước** khi execute.
2. Với `subagent` lần đầu: `TaskManager` tạo child session và update `tool_invocations.child_session_id` trong một transaction ngắn. Khi resume: check ownership rồi link child cũ.
3. AgentRuntime persist tool-result message **cùng transaction** với update invocation `completed/failed`. Final assistant message và `finish_run(completed)` được ghi bền vững trước khi báo thành công.
4. Tool call thiếu confirmed result sau crash ở trạng thái `interrupted`: **không tự retry**, không tự tạo fake result để tiếp tục model. Cần reconciliation hoặc quyết định của Root/User.

## 9.5. Startup recovery (single instance)

Trước khi cho phép request: update các `agent_runs.running` còn sót thành `interrupted`, cùng các tool invocations `requested/running` thành `interrupted`. Dữ liệu gốc không bị xóa. Run mới không bị mở khi transcript chứa side-effect tool outcome chưa xác định. Nếu sau này nhiều process cùng chạy, cần lease/heartbeat thay cho startup-only reconciliation.

## 9.6. Tool outputs và retention

MVP lưu final text/tool result vào SQLite (`result_json`) cùng những metadata cần replay; giới hạn phần text đưa vào context ở `PromptBuilder/ToolExecutor`. Không thiết kế artifact store tổng quát, background job table hay TTL cleanup trong MVP. Nếu model-visible content quá dài, báo `truncated` nhưng giữ raw result trong DB (giới hạn bằng hard persistence size cap). Nếu raw vượt hard cap, trả technical ToolError thay vì silent loss. Artifact-by-reference có thể bổ sung sau.

## 9.7. SQLite config

WAL, FK per connection, busy timeout; pool nhỏ (1–5 connections) và transactions ngắn. Backup qua SQLite backup API/VACUUM INTO, không copy một `.db` đang được ghi mà bỏ qua WAL.
