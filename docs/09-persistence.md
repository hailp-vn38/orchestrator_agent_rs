# 09 — SQLite Persistence (MVP)

## 9.1. Quyết định

SQLite + SQLx, WAL, `foreign_keys=ON`, `busy_timeout`, một shared Rust process. `SessionStore` sở hữu toàn bộ queries và transaction invariants. Không thêm Redis, queue broker hoặc event store trong MVP.

**Migration mẫu:** [examples/001_init.sql](examples/001_init.sql).

## 9.2. Năm bảng tối thiểu

| Bảng | Dữ liệu |
|---|---|
| `sessions` | `id`, `parent_session_id`, agent, owner/workspace, `status=open/archived`, `next_sequence`. |
| `messages` | Ordered transcript, gồm `user/assistant/tool/synthetic/compaction` và optional tool call ID. |
| `agent_runs` | Attempt, model/provider, `turn_id`, `resume_attempt`, `running/completed/failed/cancelled/timed_out/interrupted`, usage. |
| `tool_invocations` | Tool state/result, `assistant_message_id`, optional `child_session_id/child_turn_id`, checkpoint pointer. |
| `artifacts` | Committed immutable artifact metadata, private bytes thuộc ArtifactStore. |

**Không có `tasks` table**: `tool_invocations` với `tool_name='subagent'` chính là một delegation record. Một child session có thể được liên kết tới nhiều invocations ở các lần resume. Không có trùng lặp task status/output.

## 9.3. Constraints và ownership

- `UNIQUE(session_id, sequence)` bảo đảm message ordering.
- `UNIQUE(assistant_message_id, tool_call_id)` bảo đảm provider tool-call ID duy nhất trong từng assistant response; internal `tool_invocations.id` là durable PK. `messages.tool_invocation_id` trên tool-result message trỏ chính xác invocation đó (provider ID vẫn giữ nguyên). SessionStore validate assistant message, tool result, run/session ownership trong transaction.
- `CREATE UNIQUE INDEX ... ON agent_runs(session_id) WHERE status='running'` là **single active run admission**. Không dùng `sessions.active_run_id`, không dùng cùng lúc một per-session mutex khác ở MVP.
- `child_session_id` là nullable FK đến `sessions(id)`, `child_turn_id` là logical turn thuộc linked child session và phải được admit atomically cùng child input/run claim. Khi không null, `tool_name` phải là `subagent`; app kiểm tra `child.parent_session_id = parent invocation.session_id` và owner/workspace.
- `session.status` chỉ `open/archived`. Failed/timed-out là **run state**, không phải session state.
- `sessions.state_revision` là monotonic UI revision **theo session**, tăng trong transaction khi committed run/message/invocation/link/compaction state thay đổi. **Không** thêm `events` table hoặc lưu từng token delta.

## 9.4. Transactions quan trọng

### Durable admission và start run

Một transaction ngắn: assert session `open`, atomically persist incoming user/delegation message và `agent_runs(status='running', turn_id, resume_attempt=0)` execution claim. Trả `SESSION_BUSY` nếu partial unique index xung đột. **API có thể trả `202 Accepted` trước khi LLM hoàn thành**, managed Tokio execution chạy ngoài HTTP request. Không await LLM/tool trong transaction.

### Append message

Một transaction: lấy và tăng `sessions.next_sequence`, insert message đúng sequence; `AgentRuntime` là writer ở mức workflow. ToolExecutor trả kết quả về, không tự append history.

### Tool call + result

1. AgentRuntime persist assistant message có tool calls và `tool_invocations` trạng thái `requested` **trước** khi execute.
2. `SessionStore.admit_child_turn(parent_invocation_id, child, prompt)` atomically create/reuse child session, append child input, insert child run claim và link `child_session_id/child_turn_id`. Repeated same invocation returns same child turn; a follow-up parent invocation admits new child turn even when prompt is identical.
3. AgentRuntime persist tool-result message **cùng transaction** với update invocation `completed/failed`; tăng `sessions.state_revision` rồi publish terminal SSE event **sau commit**. Final assistant message, `agent_runs.final_message_id` và `finish_run(completed)` được ghi **trong cùng transaction**, final pointer phải trỏ message của đúng run/session.
4. Tool call thiếu confirmed result sau crash ở trạng thái `interrupted`: **không tự retry**, không tự tạo fake result để tiếp tục model. Cần reconciliation hoặc quyết định của Root/User.

## 9.5. Startup recovery (single instance)

Khi startup (chỉ một owner SQLite), scan stale `agent_runs.running`. Nếu không có unresolved mutating tool outcome, old run → `interrupted`, new run cùng `turn_id` và `resume_attempt+1`, synthetic continuation marker; tiếp tục runner. Giới hạn attempts/turn (ví dụ 10) chống crash-loop. Nếu tool đang `requested/running` với uncertain side effects, invocation → `interrupted`, block execution và yêu cầu reconciliation; không tự replay. Data gốc được giữ. Multi-process cần lease/heartbeat riêng.

## 9.6. Tool outputs, artifacts và retention

MVP giữ bounded tool result/metadata trong SQLite; immutable output lớn nằm trong local `ArtifactStore` với bảng `artifacts`, không silent truncate durable bytes. `SessionStore` sở hữu `tool_invocations.checkpoint_artifact_id` CAS; ArtifactStore verify manifest ref. Chỉ last committed pointer là authoritative, không lấy newest file. Không `tasks` table, TTL/GC hoặc remote storage adapters; backup/restore cả SQLite/WAL và artifact bytes ở consistent recovery point.

## 9.7. SQLite config

WAL, FK per connection, busy timeout; pool nhỏ (1–5 connections) và transactions ngắn. Backup qua SQLite backup API/VACUUM INTO, không copy một `.db` đang được ghi mà bỏ qua WAL.

## 9.8. Compaction checkpoint (không thêm bảng)

`messages.role='compaction'` chứa JSON gồm `summary`, `keep_from_sequence`, `version`, `model_ref`. `PromptBuilder` dùng checkpoint mới nhất + recent **settled** exchanges (kể cả prefix active turn), original messages vẫn còn. `messages.role='synthetic'` chứa internal restart continuation marker, không giả danh user input. `agent_runs.turn_id` và `resume_attempt` hỗ trợ recovery bounded. SQLite migration ví dụ đã cập nhật tương ứng. Xem [15](15-long-running-sessions.md).

## 9.9. Session-family snapshot (SSE reconnect)

`SessionStore.family_snapshot(root_id)` đọc family membership và per-session committed state/revision trong một transaction ngắn; history dài phân trang. `state_revision` không phải `next_sequence` hay SSE cursor. Committed notifications chỉ invalidate snapshot; không rebuild state bằng cách merge event payload. Xem [16](16-streaming-events.md).

## 9.10. Lineage, owner guard và recovery

- `messages.run_id` xác định assistant/tool message thuộc attempt nào; `agent_runs.final_message_id` chỉ được gán terminal và trỏ final assistant của same run/session, không lấy assistant message cuối từ prior attempt.
- Artifact metadata `producer_invocation_id` (nullable) và checkpoint CAS target dùng **internal invocation ID**, không provider call ID; validate corresponding owner/session/turn.
- `tool_invocations.assistant_message_id` chứa namespace provider call IDs, `UNIQUE(assistant_message_id, tool_call_id)`, internal PK khác provider ID. `messages.tool_invocation_id` (nullable FK) được gán bắt buộc cho các tool result mới, liên kết tới đúng invocation dù provider ID được tái sử dụng.
- Parent invocation → child session + logical child turn được persist atomically cùng child admission. Child recovery attempt mới giữ `child_turn_id`.
- Exclusive process owner lock bảo vệ one owner/store (khác unique partial index one run/session). Lock chỉ nhả sau local write-capable tasks quiesce/join; unresolved external mutation vẫn block.
- `agent_runs.provider_config_fingerprint` non-secret so sánh routing identity khi recover.
