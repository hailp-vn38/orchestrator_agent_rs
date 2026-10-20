# Orchestrator Agent RS — Tài liệu thiết kế

**Trạng thái:** Baseline **v2 — sau Ponytail + Codebase Design review** · **Ngày:** 2026-10-20 · **Đích:** Repository `hailp-vn38/orchestrator_agent_rs`

Bộ tài liệu là **thiết kế dự kiến**, không khẳng định source code đã được triển khai. Mô hình tham khảo: OpenCode `anomalyco/opencode`, nhưng implementation mới sử dụng Rust, Tokio, crate `llm`, SQLx và SQLite.

## Thay đổi chính trong bản v2

- Không còn `OrchestratorEngine` và `ContextManager` riêng; `PromptBuilder` tạo context, `AgentRuntime` persist transcript.
- Chỉ 4 bảng SQLite: `sessions`, `messages`, `agent_runs`, `tool_invocations`; delegation liên kết child bằng `tool_invocations.child_session_id`.
- Một active run/session nhờ unique partial index; `session` chỉ `open/archived`, lỗi nằm ở `run`.
- MVP skill chỉ load từ `skills/` lúc startup, foreground delegation, không hot reload, background, semantic validation hoặc auto-compaction.
- Side-effect tool outcome sau crash có thể unknown: không tự replay.

## Mục lục

| Tài liệu | Phạm vi |
|---|---|
| [00. Quyết định thiết kế](00-decisions.md) | Các quyết định đã chốt, mục tiêu, phạm vi MVP và điểm chưa chốt |
| [01. Kiến trúc tổng thể](01-architecture.md) | Sơ đồ modules, dependencies, phân chia trách nhiệm |
| [02. Agent](02-agents.md) | Root/Subagent, registry, định nghĩa agent, lifecycle |
| [03. Prompt](03-prompts.md) | Prompt Builder, thứ tự instructions, agent/project prompt |
| [04. Skill](04-skills.md) | Discovery, catalog, `skill` tool, SKILL.md, session scope |
| [05. Tool](05-tools.md) | Tool registry, execution, permissions, tool result và error |
| [06. Giao tiếp Root ↔ Subagent](06-delegation.md) | `subagent` tool, kết quả, resume, failure, background sau MVP |
| [07. Session & Context](07-sessions-context.md) | Parent–child sessions, history, compaction, isolation |
| [08. Shared Runtime & LLM](08-runtime-llm.md) | Agent loop, Tokio, multi-provider, budget; streaming sau MVP |
| [09. SQLite Persistence](09-persistence.md) | Schema, transaction, WAL, crash recovery |
| [10. Security & Operations](10-security-operations.md) | Permission enforcement, secret, file access, tracing |
| [11. End-to-end Flows](11-flows.md) | Mermaid sequence diagrams cho các kịch bản chính |
| [12. MVP & Test Plan](12-mvp-test-plan.md) | Increment, acceptance tests, non-goals |
| [13. ADRs](13-adrs.md) | Quyết định kiến trúc và alternatives |
| [14. Glossary](14-glossary.md) | Thuật ngữ chung trong codebase |

## Tài liệu mẫu

- [SQL migration tham khảo](examples/001_init.sql)
- [Agent config mẫu](examples/agents.yaml)
- [Prompt Root mẫu](examples/prompts/root.md)
- [Prompt Explorer mẫu](examples/prompts/explorer.md)
- [Prompt Reviewer mẫu](examples/prompts/reviewer.md)
- [Skill Rust Review mẫu](examples/skills/rust-review/SKILL.md)
- [Skill Task Planning mẫu](examples/skills/task-planning/SKILL.md)

**Dùng các file mẫu:** copy `docs/examples/agents.yaml` thành `config/agents.yaml`, copy `docs/examples/prompts/*` vào `prompts/`, và `docs/examples/skills/*` vào `skills/` khi khởi tạo project. Các model IDs chỉ minh hoạ, cần kiểm tra provider thực tế.

## Nguyên tắc điều hướng

1. **Đọc [00](00-decisions.md) và [01](01-architecture.md)** để hiểu phương án.
2. Khi hiện thực module, đọc riêng file của module đó và các giao ước tại [06](06-delegation.md), [07](07-sessions-context.md).
3. Khi thay đổi hành vi, cập nhật file module, [13 ADR](13-adrs.md) và các test tương ứng.
4. Các giá trị mặc định (timeout, depth, token...) là **đề xuất MVP**, không phải giới hạn của OpenCode hay crate `llm`.

## Nguồn tham khảo

- OpenCode repo: https://github.com/anomalyco/opencode
- OpenCode subagent tool (nhánh `dev`, tại thời điểm nghiên cứu): https://github.com/anomalyco/opencode/blob/dev/packages/core/src/tool/plugin/subagent.ts
- OpenCode skill tool: https://github.com/anomalyco/opencode/blob/dev/packages/core/src/tool/plugin/skill.ts
- OpenCode skill catalog: https://github.com/anomalyco/opencode/blob/dev/packages/core/src/skill/instructions.ts
- OpenCode session context: https://github.com/anomalyco/opencode/blob/dev/packages/core/src/session/context.ts
- Rust `llm`: https://docs.rs/llm/latest/llm/

**Lưu ý:** Thiết kế tham khảo hành vi OpenCode, không sao chép mã nguồn hoặc bảo đảm tương thích binary/API. Trước khi code cần khoá phiên bản crate và chạy spike xác minh tool calling, streaming của các provider thực tế.
