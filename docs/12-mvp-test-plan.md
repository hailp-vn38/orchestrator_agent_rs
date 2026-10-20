# 12 — MVP Roadmap & Acceptance Tests

## 12.1. Milestones (tracer bullets)

| Milestone | Scope | Điều kiện hoàn thành |
|---|---|---|
| M0 — Bootstrap | Cargo, Axum, tracing, SQLx migration, config | Start app, migrate DB, health check. |
| M1 — Single Agent | Agent registry, llm gateway, prompt, simple chat | 1 agent nhiều lượt, restart đọc lại history. |
| M2 — Tools | Tool registry, read file, tool loop, permission | LLM gọi tool, đúng call ID, tool denied rõ ràng. |
| M3 — Skill | Discover SKILL.md, catalog, `skill(id)` | Skill chỉ nạp khi gọi và tồn tại trong session history. |
| M4 — Parent–Child | TaskManager, subagent foreground + resume; link `child_session_id` với invocation | Root gọi child provider khác, nhận final text và follow-up same session. |
| M5 — Long-running | Managed continuous execution, no default steps cap, summary compaction | >100 steps và client disconnect không dừng run; context giảm bằng checkpoint. |
| M6 — Recovery & Safety | Bounded restart resume, tool reconciliation, cancellation, tracing | Safe execution tự resume; uncertain mutation không replay; unique index bảo vệ concurrent runs. |
| M7 — Live Events & SSE | LlmGateway.stream, ToolContext.progress, Root/Child event family, snapshot/reconnect | Thấy child/tools live, SSE disconnect không dừng run; reconnect phục hồi canonical state. |

## 12.2. Test pyramid

- **Unit:** Agent selection, permissions, prompt assembly, skill metadata, JSON tool input parsing.
- **Integration:** SQLx real SQLite temp DB, session parent ID, message sequencing, restart recovery, race conditions.
- **Fake LLM:** deterministic scripted tool calls/text/error, test loop and Root continuation without live provider.
- **Provider smoke:** ít nhất hai providers cho text delta, tool call/result assembly, usage, timeout và error mid-stream; test streaming **bắt buộc cho MVP** với fallback nếu tool stream capability thiếu.
- **E2E:** Axum API + SQLite + skill + subagent multi-turn.

## 12.3. Acceptance tests chi tiết

| ID | Scenario | Expected |
|---|---|---|
| AT-01 | User → Root text chat | Response và messages lưu SQLite. |
| AT-02 | Root gọi reviewer | Child session có `parent_session_id` đúng. |
| AT-03 | Reviewer final plain text | Root nhận tool result có `session_id`, `status=completed`, `output` text. |
| AT-04 | Reviewer trả sai định dạng được Root mong đợi | Runtime **không** tự reject vì semantic; Root có thể hỏi lại. |
| AT-05 | Root hỏi tiếp cùng session ID | Child history được tái sử dụng; `tool_invocations` parent mới và `agent_runs` child mới; không có `tasks` table. |
| AT-06 | Root cố resume child của Root khác | `SESSION_NOT_CHILD` hoặc ownership error. |
| AT-07 | Child bị deny `write_file` | Tool bị chặn; không có write. |
| AT-08 | Child gọi `skill(id)` được phép | Skill content trong child history, không tự chui vào Root history. |
| AT-09 | Child gọi skill bị deny | Tool error. |
| AT-10 | Child không có final text | Placeholder, không panic. |
| AT-11 | Child vượt 120 giây do nhiều LLM/tool calls | Không auto timeout toàn child; per-operation timeout độc lập. |
| AT-12 | Process crash giữa tool side effect và confirmed result | Run/invocation được mark `interrupted`, không auto replay, không tiếp tục LLM với dangling call. |
| AT-13 | Root gọi Subagent bằng provider khác | Provider routing theo child AgentDefinition. |
| AT-14 | Hai run song song cùng child session | Chỉ một được admit; không corrupted history. |
| AT-15 | Skill file có path traversal/symlink escaping | Bị chặn. |
| AT-16 | App restart sau hoàn thành child | Truy vấn được history, child link tại `tool_invocations.child_session_id`. |
| AT-17 | Crash để lại running run không có unresolved mutating tool | Mark run interrupted, auto resume cùng turn_id, tăng resume_attempt bounded. |
| AT-18 | Session bị archived | `start_run` từ chối; session history vẫn có thể đọc. |
| AT-19 | Agent không cấu hình `steps`, model gọi >100 steps | Runtime tiếp tục cho tới final/cancel/terminal error. |
| AT-20 | Agent cấu hình `steps=5` | Đến cap không gọi tools nữa, text-only progress + remaining work, không tự tạo run mới. |
| AT-21 | Near context limit | Persist compaction checkpoint, giữ original history, giữ complete tool-call/result pairs. |
| AT-22 | Provider context overflow | Auto compact/retry một lần rồi return recoverable error nếu vẫn quá. |
| AT-23 | Crash tại safe point | Auto resume bounded attempt trong same turn_id. |
| AT-24 | Crash giữa write và confirmed result | Block; không replay mutation khi outcome unknown. |
| AT-25 | HTTP disconnect sau `202 Accepted` | Managed run tiếp tục; client query status/history được. |
| AT-26 | Provider stream 20 text fragments | Live text đúng thứ tự; chỉ full committed assistant message được lưu SQLite. |
| AT-27 | Provider chia tool arguments qua nhiều fragments | Tool không execute cho đến khi toàn provider response `Finished`, tất cả calls hợp lệ và persisted. |
| AT-28 | Tool có progress và final result | Live progress có thể mất; terminal tool event sau SQLite commit và snapshot tái dựng được. |
| AT-29 | Foreground child đang chạy + Root chờ | SSE family nhìn thấy Child events trước khi parent tool result hoàn tất. |
| AT-30 | SSE reader chậm bị broadcast Lagged | Reader resync/đóng riêng; execution + reader khác tiếp tục bình thường. |
| AT-31 | SSE reconnect/snapshot race với child vừa tạo | Committed events làm refresh family snapshot; client hội tụ đúng child list/terminal state. |
| AT-32 | Subscribe child khác owner hoặc stream có secret | Deny/redact, không rò rỉ dữ liệu. |
| AT-33 | DB terminal transaction fail | Không phát confirmed `tool.completed`/`run.completed` giả. |
| AT-34 | Server crash giữa mutation/tool result | Recovery đánh dấu interrupted, không replay, SSE snapshot hiển thị unresolved. |
| AT-35 | Provider stream ToolCallReady rồi lỗi trước Finished | Không execute tool và không persist unconfirmed invocation. |
| AT-36 | Assistant response gồm hai tools | Persist đủ calls rồi dispatch tuần tự; next LLM step chỉ sau cả hai results. |
| AT-37 | SQLite commits rev 15 rồi 16 nhưng SSE publish 16 trước 15 | Snapshot refresh >=16, không mất canonical state hay bị rollback UI. |

## 12.4. Definition of Done

- Migrations từ DB trống chạy thành công.
- CI chạy cargo fmt, clippy, test và integration SQLite.
- Source code không có API keys hardcoded.
- Thực hiện được E2E Root → Subagent → Skill → final text → Root follow-up → final response.
- E2E một session nhiều model steps, auto compaction và safe restart, không step cap mặc định.
- Bổ sung ít nhất một provider thứ hai để chứng minh model routing.
- Failure trả error có code; tracing spans có session/run/tool-call IDs và child linkage khi relevant.
- SSE + session family snapshots là acceptance scope của MVP, không deferred. Replay toàn bộ delta events/WebSocket chỉ sau MVP. Xem [16](16-streaming-events.md).

## 12.5. Tính năng để sau

- Background task inbox, multi-agent parallel edits, nested delegation >1, structured-output specialist tools, semantic validation tùy chọn khi có use case, model fallback policy, distributed workers.
