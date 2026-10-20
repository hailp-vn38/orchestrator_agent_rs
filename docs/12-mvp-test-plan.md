# 12 — MVP Roadmap & Acceptance Tests

## 12.1. Milestones (tracer bullets)

| Milestone | Scope | Điều kiện hoàn thành |
|---|---|---|
| M0 — Bootstrap | Cargo, Axum, tracing, SQLx migration, config | Start app, migrate DB, health check. |
| M1 — Single Agent | Agent registry, llm gateway, prompt, simple chat | 1 agent nhiều lượt, restart đọc lại history. |
| M2 — Tools | Tool registry, read file, tool loop, permission | LLM gọi tool, đúng call ID, tool denied rõ ràng. |
| M3 — Skill | Discover SKILL.md, catalog, `skill(id)` | Skill chỉ nạp khi gọi và tồn tại trong session history. |
| M4 — Parent–Child | TaskManager, subagent foreground + resume; link `child_session_id` với invocation | Root gọi child provider khác, nhận final text và follow-up same session. |
| M5 — Harden | Budgets, timeout, cancellation, tracing, SQLite recovery | Failure không làm mất history hoặc replay side effects; unique index chặn cùng-session concurrent run. |

## 12.2. Test pyramid

- **Unit:** Agent selection, permissions, prompt assembly, skill metadata, JSON tool input parsing.
- **Integration:** SQLx real SQLite temp DB, session parent ID, message sequencing, restart replay, race conditions.
- **Fake LLM:** deterministic scripted tool calls/text/error, test loop and Root continuation without live provider.
- **Provider smoke:** ít nhất hai providers cho text, tool call/result, usage, timeout; chạy trong môi trường test có credentials. Streaming thử nghiệm trong spike, không bắt buộc cho MVP.
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
| AT-11 | Child vượt timeout | Child run ghi `timed_out`; parent nhận ToolError, invocation parent ghi failure, DB nhất quán. |
| AT-12 | Process crash giữa tool side effect và confirmed result | Run/invocation được mark `interrupted`, không auto replay, không tiếp tục LLM với dangling call. |
| AT-13 | Root gọi Subagent bằng provider khác | Provider routing theo child AgentDefinition. |
| AT-14 | Hai run song song cùng child session | Chỉ một được admit; không corrupted history. |
| AT-15 | Skill file có path traversal/symlink escaping | Bị chặn. |
| AT-16 | App restart sau hoàn thành child | Truy vấn được history, child link tại `tool_invocations.child_session_id`. |
| AT-17 | Crash để lại running run / requested tool | Startup reconcile thành `interrupted`, không tự replay side effects hoặc gửi dangling pair sang LLM. |
| AT-18 | Session bị archived | `start_run` từ chối; session history vẫn có thể đọc. |

## 12.4. Definition of Done

- Migrations từ DB trống chạy thành công.
- CI chạy cargo fmt, clippy, test và integration SQLite.
- Source code không có API keys hardcoded.
- Thực hiện được E2E Root → Subagent → Skill → final text → Root follow-up → final response.
- Bổ sung ít nhất một provider thứ hai để chứng minh model routing.
- Failure trả error có code; tracing spans có session/run/tool-call IDs và child linkage khi relevant.

## 12.5. Tính năng để sau

- Background task inbox, multi-agent parallel edits, nested delegation >1, structured-output specialist tools, semantic validation tùy chọn khi có use case, model fallback policy, distributed workers.
