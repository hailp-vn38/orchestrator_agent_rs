# 12 — MVP Roadmap & Acceptance Tests

## 12.1. Milestones (canonical tracer bullets)

| Milestone | Scope | Điều kiện hoàn thành |
|---|---|---|
| M0 — Library shell | Cargo `[lib]`, public Orchestrator/HostTool, typed config, embedded base | Rust host builds/instantiates crate without Axum. |
| M1 — Single Agent | SQLite durable admission, supervisor, host-injected LlmGateway, result/history | Accepted turn không bị mất scheduling trong live process. |
| M2 — Tools & Skills | HostTool scoped context, permissions, tool loop, skill catalog/load | No transcript double writer, denied skill/tool không execute/leak. |
| M3 — Parent–Child | Foreground delegation/resume, idempotent atomic child turn claim | Crash không duplicate child objective; same child session supports follow-up. |
| M4 — Continuous | No default step cap, active-turn compaction, provider full response streaming | >100 steps; no premature stop/tool execution. |
| M5 — Recovery & Lifecycle | Exclusive owner guard, unknown mutation reconciliation, cancel/shutdown | No blind mutation replay, no unsafe owner handoff. |
| M6 — Observation | Transport-neutral `subscribe/snapshot`; optional Axum SSE adapter | Lag/reconnect/missed terminal notification converges to canonical state. |
| M7 — Artifacts | Immutable local store, scoped reads, manifest CAS, bounded `AgentResult` preview | Playlist/document/media refs + fault tests/backup consistency. |

**Đây là roadmap/test catalog canonical cho v5.0**. Tài liệu 17/18 tham chiếu milestone và AT IDs ở đây, không bảo trì kế hoạch song song.

## 12.2. Test pyramid

- **Unit:** Agent selection, permissions, prompt assembly, skill metadata, JSON tool input parsing.
- **Integration:** SQLx real SQLite temp DB, session parent ID, message sequencing, restart recovery, race conditions.
- **Fake LLM:** deterministic scripted tool calls/text/error, test loop and Root continuation without live provider.
- **Provider smoke:** ít nhất hai providers cho text delta, tool call/result assembly, usage, timeout và error mid-stream; test streaming **bắt buộc cho MVP** với fallback nếu tool stream capability thiếu.
- **E2E:** Tokio Rust host embeds library + SQLite + skill + subagent multi-turn; Axum optional smoke adapter.

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
| AT-38 | Parent crash sau child admission hoặc child completed trước parent tool result, kể cả prompt follow-up trùng | Mỗi parent invocation chỉ admit đúng một child turn, wait đúng final. |
| AT-39 | Hai session cùng agent với effective skill permissions khác nhau | Denied skill không xuất hiện trong catalog hoặc load được. |
| AT-40 | HostTool B đọc artifact output của A; ID ngoài scope hoặc bytes corrupt | Scoped bounded stream; deny invalid scope; integrity errors. |
| AT-41 | Shutdown grace hết, non-cooperative local tool còn write-capable | Không báo stopped/success, owner guard không nhả; engine khác chưa acquire. |
| AT-42 | New recovery attempt đang chạy, prior attempt có assistant final | AgentResult.output=None; terminal attempt mới trỏ đúng final_message_id. |
| AT-43 | Provider tái dùng tool_call_id ở hai assistant responses | Hai internal invocations riêng, ghép đúng call/results từng message. |
| AT-44 | Một active turn vượt context threshold nhiều lần | Compact settled exchanges prefix, giữ objective và tool pairs, không cần prior complete turns. |
| AT-45 | Suppress terminal publish sau DB commit nhưng SSE còn connected | Periodic snapshot reconciliation hội tụ terminal canonical state. |
| AT-46 | YAML DTO example + embedded mandatory base | Host converts đúng AgentDefinition fields, mọi model request có base. |
| AT-47 | Crash quanh manifest publish/CAS | Chỉ last committed pointer authoritative; verified refs, safe replay policy. |
| AT-48 | Pinned crate llm + 2 provider smoke | Compile, text/tool streaming & Finished semantics, model capabilities, redacted secrets. |
| AT-49 | Operator resolve unknown mutation outcome | Authorized audited terminal transaction trước resume; no blind replay. |
| AT-50 | Hai library instances dùng cùng execution store | Second build fails while owner held, only succeeds after safe release. |

## 12.4. Definition of Done

- Migrations từ DB trống chạy thành công.
- CI chạy cargo fmt, clippy, test và integration SQLite.
- Source code không có API keys hardcoded.
- Thực hiện được E2E Root → Subagent → Skill → final text → Root follow-up → final response.
- E2E một session nhiều model steps, auto compaction và safe restart, không step cap mặc định.
- Bổ sung ít nhất một provider thứ hai để chứng minh model routing.
- Failure trả error có code; tracing spans có session/run/tool-call IDs và child linkage khi relevant.
- Scoped ArtifactStore reads, manifest checkpoint durability, owner handoff và consistent DB/files backup đều là acceptance scope.
- SSE + session family snapshots là acceptance scope của MVP, không deferred. Replay toàn bộ delta events/WebSocket chỉ sau MVP. Xem [16](16-streaming-events.md).

## 12.5. Tính năng để sau

- Background task inbox, multi-agent parallel edits, nested delegation >1, structured-output specialist tools, semantic validation tùy chọn khi có use case, model fallback policy, distributed workers.
