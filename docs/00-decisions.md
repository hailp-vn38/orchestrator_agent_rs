# 00 — Quyết định và phạm vi

## 0.1. Mục tiêu

Xây **Multi-Agent Orchestrator dạng Rust library có thể nhúng vào server host** hỗ trợ Root Agent chủ động phân việc cho Subagent, mỗi agent có cấu hình model, prompt, tool permissions, skill catalog và session riêng. Tất cả chạy trong một **Shared Runtime** Rust, lưu trạng thái bền vững với SQLite và hỗ trợ nhiều provider thông qua crate `llm`.

## 0.2. Các quyết định đã chốt

| ID | Quyết định |
|---|---|
| D-01 | Modular monolith bằng Rust; Tokio là async runtime. |
| D-02 | Dùng crate `llm` làm thư viện truy cập model; bọc bởi module `LlmGateway` nhỏ. |
| D-03 | SQLx + SQLite (WAL, foreign keys, busy timeout), không yêu cầu Redis/queue broker trong MVP. |
| D-04 | **Root/primary** quyết định phân việc, đánh giá kết quả và tự chọn có gọi tiếp Subagent không. |
| D-05 | **Subagent** chạy trong child session; mọi agent dùng chung Agent Runtime. |
| D-06 | Giao việc bằng tool `subagent(agent, description, prompt, session_id?)`. |
| D-07 | Mặc định Subagent trả **final text** trong tool result cùng child session ID. Không ép JSON nghiệp vụ. |
| D-08 | **Không bắt buộc** ResultValidator ngữ nghĩa, Reviewer Agent, TaskContract hay auto-retry vì thiếu format. |
| D-09 | Kiểm tra kỹ thuật vẫn bắt buộc: permissions, session ownership, tool input schema, trạng thái, giới hạn tài nguyên. |
| D-10 | Root có thể tiếp tục child session cũ bằng `session_id`; không sao chép toàn bộ Root history sang child. |
| D-11 | Prompt gồm base/agent/project instructions, skill catalog và session history. |
| D-12 | Skill discovery theo metadata; toàn bộ `SKILL.md` nạp qua `skill(id)` khi agent cần, trong đúng session. |
| D-13 | Foreground delegation làm trước; background Subagent notification, parallel child và distributed sau MVP. |
| D-14 | `steps` tùy chọn và mặc định unset; đạt cap thì text-only progress report, không báo lỗi hay tự tạo run mới. |
| D-15 | Không đặt deadline cho toàn child/session; chỉ timeout từng LLM/tool operation. |
| D-16 | Auto summary compaction với SQLite checkpoint khi context gần đầy, giữ nguyên full transcript. |
| D-17 | Session restart recovery bounded, an toàn với uncertain tool side effects; không có business `Task` scheduler cho MVP. |
| D-18 | SSE + unified execution events cho Root, Child, Tool, Skill và model text delta; stream độc lập với tuổi thọ run. |
| D-19 | SQLite lưu canonical committed states và per-session `state_revision`; progress/delta ephemeral, không replay SSE qua `Last-Event-ID` trong MVP. |
| D-20 | Client subscribe SSE trước, đọc session-family snapshot; committed events chỉ invalidate snapshot, không merge state patches; `Lagged` → resync. |
| D-21 | Tool chỉ dispatch khi toàn provider response `Finished` và đầy đủ các tool calls đã persist; MVP dispatch tuần tự, next model step chờ mọi tool results. |
| D-22 | `subagent`/`skill` sử dụng generic tool events; `ToolContext::progress` không cần public ProgressReporter; snapshot refresh theo invalidation thay vì event reducer. |
| D-23 | Library-first: host sở hữu Tokio, AuthN, transport; expose một public `Orchestrator`, không tạo `OrchestratorEngine` thứ hai. |
| D-24 | Typed host-injected `LlmConfig` + non-secret provider routing fingerprint mỗi run attempt. |
| D-25 | HostTool scoped artifacts put/open + checkpoint CAS; ArtifactStore local và bảng `artifacts` thuộc MVP, không GC/TTL/remote trait. |
| D-26 | Parent invocation ID là idempotency key; atomic admission persist child input/run và `child_turn_id`. |
| D-27 | `agent_runs.final_message_id` và `messages.run_id` xác định final đúng terminal attempt. |
| D-28 | Internal invocation ID khác provider `tool_call_id`; provider ID scoped theo assistant message. |
| D-29 | Process owner lock chỉ nhả khi local write-capable tasks đã quiesce/join; unknown external mutations cần reconciliation. |
| D-30 | Skill catalog/load cùng effective session permissions; denied skill không xuất hiện trong prompt. |
| D-31 | Compaction được phép lấy settled exchange prefix ngay trong active turn; không cắt call/result pair. |
| D-32 | SSE best-effort, canonical snapshot; periodic revision reconcile khi vẫn connected và active run. |

## 0.3. MVP bao gồm

- Public `Orchestrator` admit input bền vững rồi trả `AcceptedTurn`; Tokio managed execution độc lập với host transport (Axum chỉ là optional adapter).
- Registry, Prompt Builder, Tool Executor, Skill Loader, SQLite history và multi-provider LlmGateway.
- LLM → Tools/Subagents → LLM **liên tục, không step cap mặc định**.
- Foreground `subagent`, child history riêng, Root tự đánh giá final text và follow-up qua session ID.
- Auto summary compaction, durable checkpoint, keeping original messages.
- Startup safe recovery (bounded), cancellation, owner lock/shutdown an toàn, technical errors và tracing.
- Local ArtifactStore immutable, scoped read/download, manifest CAS checkpoint, bounded AgentResult artifact preview.
- **Streaming MVP:** LlmGateway normalized text/tool-call stream; SSE quan sát run/step/tool/progress và child sessions; authenticated session-family snapshot/resync.

## 0.4. Không thuộc MVP

- Semantic validation/auto-repair bắt buộc cho kết quả Subagent.
- Business `tasks` table, task scheduler độc lập, Redis/Kafka và distributed agents.
- Background Subagent notification/inbox, parallel child runs, arbitrarily deep nesting, skill hot reload.
- Provider-native compaction (MVP dùng summary). Không bảo đảm exactly-once cho mutating tools.
- WebSocket; lossless SSE/event-log replay; persist từng text delta; event broker.

## 0.5. Giới hạn và cấu hình

| Policy | Mặc định | Ý nghĩa |
|---|---|---|
| `agent.steps` | **Unset** | Không giới hạn steps mặc định. Nếu set, đến cap thì text-only summary, dừng gọi tool. |
| `max_subagent_depth` | 1 | Root→child, child không gọi grandchild ở MVP. |
| `child_timeout_s` | **Không có** | Không timeout toàn child dù chạy hàng giờ. |
| `llm_request_timeout_s` | Ví dụ 120 | Timeout từng model request, không phải cả run. |
| `tool_timeout_s` | Tool-specific | Timeout từng operation, không mặc định 120s cho shell dài. |
| `compaction.auto` | true | Auto summary khi context gần đầy. |
| `compaction.keep_recent_tokens` | Ví dụ 15.000 | Giữ recent settled exchanges cạnh summary, kể cả active turn. |
| `recovery.max_attempts_per_turn` | Ví dụ 10 | Chống crash-loop. |
| `max_concurrent_children` | 1 ở MVP | Không phải giới hạn tổng số child gọi **tuần tự**. |
| `max_tool_output_bytes/lines` | 50 KiB / 2.000 | Giới hạn model-visible result, không giới hạn full history. |
| `events.broadcast_capacity` | Ví dụ 4.096 | Ring buffer realtime; lagged SSE client resync. |
| `events.heartbeat_s` | Ví dụ 15 | Heartbeat riêng từng SSE connection, không ảnh hưởng runtime. |
| `events.active_snapshot_refresh_s` | Ví dụ 30 | Host resync snapshot định kỳ khi family có active run, kể cả SSE còn kết nối. |
| `shutdown.grace_s` | Deployment-defined | Không release owner guard nếu local write-capable task chưa quiesce. |
| `artifacts.max_bytes/quotas` | Deployment-defined | Hard quota và disk headroom, không GC/TTL trong MVP. |

Các số tham chiếu là cấu hình gợi ý cho **dự án Rust**, không phải hard defaults của OpenCode. Cost/quota cap nếu được cấu hình, permission và explicit cancellation vẫn bắt buộc.

## 0.6. Định nghĩa thành công

Một yêu cầu đi qua Root (provider A), Root gọi `subagent` (provider B), child dùng `skill` và tool đọc file, trả final text, Root nhận tool result, có thể gọi child lần nữa với cùng session ID, rồi trả câu trả lời cuối. Restart process vẫn đọc được parent/child histories đúng thứ tự.

## 0.7. Điều cần xác minh trước implementation

- Feature, tool-call và streaming support thực tế của phiên bản `llm` khoá trong `Cargo.lock`.
- Cách crate `llm` biểu diễn assistant tool call/result theo từng provider; không tự đoán API chưa kiểm chứng.
- Token usage, model capabilities, prompt/system support, cancellation/error classes.
- SQLite concurrency và thời gian migrate trên môi trường triển khai.
