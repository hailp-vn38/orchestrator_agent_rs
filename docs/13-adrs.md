# 13 — Architectural Decision Records

Mỗi quyết định dưới đây có trạng thái **Accepted** theo thảo luận thiết kế. Khi thay đổi, ghi ADR supersede thay vì sửa lịch sử không dấu vết.

## ADR-001 — Shared Runtime + Parent–Child Sessions

**Status:** Accepted

**Decision:** Một Rust process dùng chung AgentRuntime/ToolExecutor/LlmGateway; mỗi conversation có session bền vững với parent ID, mỗi run có identity riêng.

**Why:** Tránh RPC giữa các agent khi chưa có yêu cầu scale, dễ tracing và quản lý context riêng.

**Alternatives rejected:** Process-per-agent, distributed message bus từ MVP.

**Consequences:** Cần session lock, resource budget và crash handling.

## ADR-002 — Root-controlled Subagent Result Assessment

**Status:** Accepted

**Decision:** Subagent trả final text qua tool result. Root quyết định accept/follow-up/check/redirect. Không bắt buộc semantic ResultValidator, TaskContract hay auto-retry do format không đúng.

**Why:** Tác vụ mở có đầu ra biến thiên; validation bắt chặt gây lỗi giả, tăng độ trễ/chi phí.

**Consequences:** `completed` chỉ biểu thị execution success, không là chứng nhận quality. Root phải có instruction đánh giá và budget chống lặp.

## ADR-003 — Child Session Resume by ID

**Status:** Accepted

**Decision:** Gọi `subagent` với `session_id` để append follow-up vào child cũ; child thuộc đúng parent và owner.

**Why:** Tiết kiệm context, hỗ trợ sửa yêu cầu tự nhiên.

**Consequences:** Child session có thể có nhiều task/run; cần khóa single-active-run per session.

## ADR-004 — Skill Progressive Disclosure

**Status:** Accepted

**Decision:** Skill catalog chỉ hiển thị ID/name/description; `SKILL.md` và refs load qua `skill(id)` khi cần, theo permissions; Root/child có skill history riêng.

**Why:** Context budget ổn định, workflow chuyên biệt không nhồi tất cả vào prompt.

**Consequences:** Tool failures, skills version hash và permission checks cần tracing.

## ADR-005 — Prompt Composition

**Status:** Accepted

**Decision:** Compose mandatory base + agent + project instructions + skill catalog + active history per session. Agent prompt không thay thế runtime policy.

**Why:** Giữ quy tắc harness nhất quán, vẫn hỗ trợ per-agent specialization.

**Consequences:** Khác hành vi override `agent.system` trong OpenCode; cần test prompt assembly và model cache.

## ADR-006 — Rust + Tokio + crate llm

**Status:** Accepted (implementation API subject to provider spike)

**Decision:** Dùng crate Rust `llm`, bọc trong LlmGateway không lộ crate-specific types ra business modules.

**Why:** Multi-provider và tool-calling abstractions sẵn có.

**Consequences:** Cần khóa phiên bản và kiểm thử tool parity/streaming giữa ít nhất hai providers.

## ADR-007 — SQLite + SQLx

**Status:** Accepted

**Decision:** SQLite WAL lưu `sessions/messages/agent_runs/tool_invocations`; không có bảng `tasks` riêng; transactional updates ngắn; không queue broker từ MVP.

**Why:** Đủ cho modular monolith chạy single-node.

**Consequences:** Single-writer giới hạn write throughput; unique partial index là admission guard, không `sessions.active_run_id` hay per-session mutex trùng lặp ở MVP.

## ADR-008 — Foreground Delegation First

**Status:** Accepted

**Decision:** MVP chỉ foreground Subagent, input không có `background`, output thành công chỉ `completed`. Background sẽ có synthetic durable inbox và notification logic ở giai đoạn sau.

**Why:** Ưu tiên luồng cơ bản đúng, tránh run races và event duplication sớm.

**Consequences:** Parent đợi child, nhưng không có deadline tổng cho child; request/tool-level timeout và explicit cancel propagation.

## ADR-009 — Deep Modules, One Transcript Writer

**Status:** Accepted

**Decision:** API gọi trực tiếp shared `AgentRuntime`; `PromptBuilder` là owner duy nhất của context construction; `AgentRuntime` sở hữu transcript persistence; `ToolExecutor` chỉ trả outcome, `TaskManager` chỉ điều phối child. Concrete YAML AgentRegistry và local SkillManager không cần trait trong MVP.

**Why:** Tránh shallow pass-through (`OrchestratorEngine`, `ContextManager` riêng) và tránh double writes/races khi nhiều modules cùng cập nhật messages.

**Consequences:** Recursive child runs cần runtime handle được borrow tại call-time (box async recursion point nếu cần). Test qua public interfaces; chỉ tách abstraction khi có adapter thứ hai thực.

## ADR-010 — Single Run Admission and Crash Outcome

**Status:** Accepted

**Decision:** Session status chỉ `open/archived`; run status mang success/failure/interrupted. `agent_runs` có unique partial index cho một active run/session. Không claim exactly-once side effects; interrupted tool chưa có result phải reconcile trước khi tiếp tục model.

**Why:** Một cơ chế concurrency rõ ràng và không gây replay nguy hiểm sau crash.

**Consequences:** Safe session có thể tự resume từ durable claim; uncertain side-effect tool phải block/reconcile. Multi-instance lease và background inbox dành cho sau MVP.

## ADR-011 — Continuous Sessions Without Default Step Limit

**Status:** Accepted (2026-10-20)

**Decision:** Shared AgentRuntime chạy session-driven LLM/tool loop, `agent.steps` là `Option` mặc định unset. Nếu set, tại cap buộc text-only progress/remaining report, không fail hoặc auto create run mới. Không đặt wall-clock deadline cho toàn child, chỉ timeout mỗi operation.

**Why:** Task nhiều giờ cần nhiều LLM/tool/subagent calls; 20/15 steps hoặc child 120s gây dừng giả.

**Consequences:** Cần cost/usage guardrails, cancel propagation, compaction và crash recovery. Final text không chứng minh mục tiêu nghiệp vụ đạt.

## ADR-012 — Durable Summary Compaction

**Status:** Accepted (2026-10-20)

**Decision:** Auto compact context near model limit, ghi `messages.role=compaction` checkpoint rồi dùng summary + recent complete turns. Original transcript giữ nguyên; provider overflow được compact/retry có giới hạn. MVP dùng summary, native provider compaction để sau.

**Why:** Session dài không thể gửi full context vô hạn.

**Consequences:** Summary lossy; phải giữ user goal, evidence, important decisions và có đường load skill lại.

## ADR-013 — Safe Automatic Restart Recovery

**Status:** Accepted (2026-10-20)

**Decision:** Durable `agent_runs.running` là execution claim. Startup chỉ bởi one-owner server, tự resume run bị interrupted khi safe qua new run cùng `turn_id`, tăng `resume_attempt` tối đa theo config. Nếu mutating tool outcome unknown thì block/reconcile, không auto replay. HTTP disconnect không mặc định hủy execution.

**Why:** Task nhiều giờ phải sống qua disconnect/restart, tránh crash loops và side-effect duplication.

**Consequences:** Cần admission, checkpoints, recovery idempotence và cơ chế unblock. Không cần business Task scheduler chỉ để vượt step cap.

## ADR-014 — Streaming Ephemeral Events + Durable Session Snapshots

**Status:** Accepted (2026-10-20); reviewed and committed with v4.1 documentation.

**Decision:** LlmGateway chuẩn hóa provider text/tool-call stream; AgentRuntime phát lifecycle events, ToolExecutor thông báo progress qua ToolContext. Dùng concrete `ExecutionFeed`/Tokio broadcast và Axum SSE để theo dõi cả Root/Child sessions. **SQLite giữ canonical states** và thêm `sessions.state_revision`, không thêm event store hay persist từng token. Committed events phát sau transaction, ephemeral deltas/progress không được replay. Client subscribe trước snapshot, committed notifications invalidate canonical view và trigger debounced snapshot refresh; không merge SSE payloads thành state patches. SSE disconnect không hủy run.

**Why:** Task hàng giờ cần live observability, tool/child visibility và reconnect; thiết kế deep modules giữ ToolExecutor độc lập HTTP, SessionStore là owner của state, tránh duplicated data/event infrastructure.

**Alternatives rejected:** WebSocket ngay từ MVP, Kafka/Redis, ghi mọi delta vào SQLite, `Last-Event-ID` replay chưa có durable log, per-agent event modules hoặc UI trực tiếp query TaskManager internals.

**Consequences:** Realtime stream best-effort, preview có thể mất; cần revision, family auth, bounded broadcast lag policy, terminal publish-after-commit và out-of-order invalidation tests. **Step ordering:** đợi provider stream `Finished` rồi persist mọi tool calls trước khi dispatch tuần tự, không execute từ `ToolCallReady` giữa stream. Nếu cần lossless event timeline, tạo ADR mới và durable event-log riêng. Chi tiết: [16 — Streaming](16-streaming-events.md).

## ADR-015 — Embeddable Library + Host-Owned Transport

**Status:** Accepted (2026-10-20); **supersedes only the public entry point in ADR-009 and Axum coupling in ADR-014**.

**Decision:** Cargo `[lib]` exposes public `Orchestrator` + `HostTool`; host owns Tokio, AuthN and transport (Axum/SSE is an optional adapter). The existing shared `AgentRuntime`, one transcript writer, `ExecutionFeed` and SQLite canonical snapshot invariants remain valid.

**Why:** A Rust server host already has transport and business context. Avoid duplicate orchestration layers.

## ADR-016 — ArtifactStore, Delegation & Message Lineage

**Status:** Accepted (2026-10-20); **extends ADR-007/010/013**, no business tasks table.

**Decision:** MVP has concrete local immutable ArtifactStore with fifth SQLite metadata table; SessionStore owns manifest checkpoint CAS pointer. Parent invocation ID is idempotency key for atomic child-turn admission. `agent_runs.final_message_id` plus `messages.run_id` bind the final to a terminal attempt. Internal invocation key is distinct from provider tool-call ID scoped by assistant message. HostTool reads/writes artifacts with runtime-minted scoped context. No GC/TTL/remote adapters.

**Why:** Durable large media/docs and crash recovery without ambiguous child turns or stale final responses.

## ADR-017 — Owner Handoff and Snapshot Reconciliation

**Status:** Accepted (2026-10-20); **clarifies ADR-013/014**.

**Decision:** Shutdown releases store process-owner lock only after local write-capable tasks quiesce/join; an incomplete shutdown cannot report success. External uncertain mutations still require reconciliation. Best-effort SSE notifications are supplemented by periodic canonical snapshot refresh while active runs exist, even if connection stays open. No durable event-log.

**Why:** Preserve single-owner execution and UI convergence without distributed scheduling or replay infrastructure.
