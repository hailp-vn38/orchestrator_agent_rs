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

**Consequences:** Parent đợi child; hệ thống cần timeout và cancel propagation.

## ADR-009 — Deep Modules, One Transcript Writer

**Status:** Accepted

**Decision:** API gọi trực tiếp shared `AgentRuntime`; `PromptBuilder` là owner duy nhất của context construction; `AgentRuntime` sở hữu transcript persistence; `ToolExecutor` chỉ trả outcome, `TaskManager` chỉ điều phối child. Concrete YAML AgentRegistry và local SkillManager không cần trait trong MVP.

**Why:** Tránh shallow pass-through (`OrchestratorEngine`, `ContextManager` riêng) và tránh double writes/races khi nhiều modules cùng cập nhật messages.

**Consequences:** Recursive child runs cần runtime handle được borrow tại call-time (box async recursion point nếu cần). Test qua public interfaces; chỉ tách abstraction khi có adapter thứ hai thực.

## ADR-010 — Single Run Admission and Crash Outcome

**Status:** Accepted

**Decision:** Session status chỉ `open/archived`; run status mang success/failure/interrupted. `agent_runs` có unique partial index cho một active run/session. Không claim exactly-once side effects; interrupted tool chưa có result phải reconcile trước khi tiếp tục model.

**Why:** Một cơ chế concurrency rõ ràng và không gây replay nguy hiểm sau crash.

**Consequences:** Một số session sẽ cần thao tác phục hồi thủ công; background lease và resumable queued jobs chỉ triển khai ở giai đoạn sau.
