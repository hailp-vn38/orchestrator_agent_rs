# Hiểu dự án Orchestrator Agent RS: baseline v4.1 và đề xuất Results & Artifacts

Ngày đọc: **2026-10-20**. Phạm vi: `docs/README.md`, toàn bộ tài liệu `00`–`17`, migration/config/prompt/skill trong `docs/examples/`. **Baseline v4.1 đã được accepted về thiết kế; tài liệu 17 vẫn là Proposed**, chưa thay đổi scope MVP đã chốt. [Nguồn: baseline](../README.md), [trạng thái đề xuất 17](../17-agent-results-artifacts.md), [compatibility notes](../17-agent-results-artifacts.md#1715-compatibility-notes-and-open-questions). Nguồn sơ cấp của bản tổng hợp này là thiết kế do chính repository sở hữu. Các liên kết OpenCode và docs.rs trong tài liệu gốc chưa được kiểm chứng trực tiếp trong lần đọc này; không suy ra hành vi upstream hiện tại từ chúng. Bộ tài liệu tự xác định là **thiết kế dự kiến**, các interface Rust là minh họa, chưa chứng minh có implementation đã compile hay runtime đã vượt acceptance tests. [Nguồn: trạng thái và giới hạn của baseline](../README.md).

## 1. Dự án muốn tạo ra điều gì?

Orchestrator Agent RS là **Multi-Agent Orchestrator** chạy trong một Rust process: Root nhận mục tiêu của người dùng, tự quyết định dùng tools hay giao một phần việc cho agent chuyên môn, đánh giá kết quả và tiếp tục cho tới khi có final text. Mỗi agent có model/provider, prompt, tool permissions và skill catalog riêng; mỗi hội thoại có session/history bền vững trong SQLite. Root và child dùng cùng runtime, không cần RPC hay message broker giữa agents. Stack đã chọn: Rust, Tokio, Axum, crate `llm`, SQLx và SQLite WAL. [Nguồn: mục tiêu và D-01–D-07](../00-decisions.md#01-mục-tiêu), [topology](../01-architecture.md#11-topology).

Hai yêu cầu định hình nền tảng là **thực thi nhiều giờ** và **quan sát tiến trình trực tiếp**. Một turn có thể dùng hàng trăm bước; HTTP/SSE disconnect không hủy execution. Context được compact và history được lưu bền vững để hỗ trợ tiếp tục an toàn sau restart. SSE giúp UI thấy Root/Child/tools đang làm gì, nhưng SQLite mới là nguồn trạng thái đã commit. Đây đều là mục tiêu MVP, không phải các mở rộng để sau. [Nguồn: MVP](../00-decisions.md#03-mvp-bao-gồm), [long-running core](../15-long-running-sessions.md#151-core-rule), [streaming scope](../16-streaming-events.md#161-decision--non-goals).

`completed` chỉ khẳng định **execution đã kết thúc thành công**, không chứng nhận yêu cầu nghiệp vụ đã đạt. Child thường trả plain text; Root tự đánh giá, hỏi tiếp hoặc kiểm chứng. Runtime không bắt buộc TaskContract, semantic ResultValidator, reviewer thứ hai hay auto-repair vì text không đúng format mong đợi. Ngược lại, kiểm tra quyền, ownership, tool input schema và tài nguyên vẫn bắt buộc. [Nguồn: D-07–D-09](../00-decisions.md#02-các-quyết-định-đã-chốt), [đánh giá child](../06-delegation.md#65-root-đánh-giá-kết-quả).

## 2. Mô hình miền cần giữ tách biệt

| Khái niệm | Ý nghĩa và quan hệ |
|---|---|
| `AgentDefinition` / `agent_id` | Cấu hình vai trò, mode, prompt, model, allowlists; bất biến trong một run, không chứa history. |
| `Session` / `session_id` | Conversation bền vững; chọn agent, gắn owner/workspace; child có `parent_session_id`. |
| `Turn` / `turn_id` | Một input của user hoặc delegation; giữ identity qua các recovery attempts. |
| `Run` / `run_id` | Một execution attempt trong session; có thể chứa nhiều steps. Recovery tạo attempt mới cùng turn. |
| `Step` | Một lượt gọi LLM và các tool calls liên quan; không đồng nghĩa run hay business task. |
| `Invocation` / `tool_call_id` | Một tool call và outcome tương ứng; ID phải giữ nguyên qua adapter và history. |
| `Delegation` / `child_session_id` | Một invocation của tool `subagent`; nhiều invocations có thể tiếp tục cùng child session. |

Nguồn: [Agent roles/definition](../02-agents.md#21-vai-trò), [session identities](../07-sessions-context.md#71-khái-niệm), [Session–Turn–Run–Step](../15-long-running-sessions.md#152-session-turn-run-step), [glossary](../14-glossary.md).

Session chỉ có `open/archived`; lỗi hoặc trạng thái đang chạy thuộc **run** (`running/completed/failed/cancelled/timed_out/interrupted`). Một session chỉ có một running run, được enforce bằng unique partial index SQLite; khi busy trả `SESSION_BUSY`. Không thêm `sessions.active_run_id` hoặc mutex riêng làm cơ chế admission thứ hai. [Nguồn: lifecycle và admission](../07-sessions-context.md#73-session-lifecycle), [constraints](../09-persistence.md#93-constraints-và-ownership).

## 3. Kiến trúc và ownership

| Module | Trách nhiệm chính và ranh giới |
|---|---|
| `AgentRuntime` | `submit/wait/resume/cancel`; managed LLM/tool loop, admission, recovery/cancel; owner duy nhất điều phối ghi transcript qua store. |
| `PromptBuilder` | Dựng context từ instructions, catalog, checkpoint/history và tool definitions; owner duy nhất của context construction. |
| `SessionStore` | SQLite queries/transactions, ordering/admission, revisions, child links, checkpoint và family snapshot. |
| `ToolExecutor` | Decode/schema, authorize, dispatch, normalize/truncate tool outcomes; trả kết quả cho runtime, không tự ghi messages. |
| `TaskManager` | Toàn bộ foreground delegation lifecycle: check target/ownership/depth, create/resume/link child, gọi runtime và chờ final text. |
| `SkillManager` | Startup discovery/snapshot và load skill theo session permissions. |
| `AgentRegistry` | Concrete YAML registry load một lần lúc startup; `get/list`, không hot reload. |
| `LlmGateway` | Adapter crate `llm`; đổi nội bộ ↔ provider, chuẩn hóa text/tool-call stream và lỗi/capabilities. |
| `ExecutionFeed` | Concrete Tokio broadcast cho realtime events; không giữ event log hay quyết định thành công nghiệp vụ. |
| Axum/API/SSE adapter | Submit/cancel/read endpoints, auth, family filtering, heartbeat, lag/resync; không sở hữu execution lifetime. |

Nguồn: [module interfaces](../01-architecture.md#12-modules-và-interfaces), [ownership rules](../01-architecture.md#13-ownership-và-dependency-rules), [streaming modules](../16-streaming-events.md#162-phân-chia-modules-codebase-design).

Khởi đầu bằng **một Cargo package** với các module `agent`, `runtime`, `prompt`, `skill`, `tool`, `subagent`, `session`, `llm`, `events`, `api`. Thiết kế bỏ `OrchestratorEngine`, `ContextManager` và business task scheduler riêng. Registry/SkillManager concrete không cần thêm trait khi chỉ có một implementation. `TaskManager` nhận borrowed runtime tại execution time; nếu async recursion cần box thì box ở nhánh child invocation, tránh vòng sở hữu `Arc`. Không giữ SQLite transaction khi await LLM, tool I/O hay child. [Nguồn: dependency rules và source tree](../01-architecture.md#13-ownership-và-dependency-rules), [ADR-009](../13-adrs.md#adr-009--deep-modules-one-transcript-writer).

## 4. Luồng chuẩn: user → Root → child → Root

1. `POST /sessions/{id}/messages` gọi runtime; transaction ngắn persist input, reserve sequence và claim running run trước khi trả `202 Accepted(session_id, turn_id)`.
2. Managed Tokio runner dựng context, gọi model đã cấu hình. HTTP request có thể kết thúc trong khi runner tiếp tục.
3. Nếu model muốn giao việc, runtime persist assistant tool calls và invocations trước; ToolExecutor authorize rồi chuyển `subagent` cho TaskManager.
4. TaskManager kiểm tra policy, tạo/resume child, link invocation với `child_session_id`, rồi gọi cùng runtime qua `submit` + `wait`.
5. Child có transcript riêng, gọi skill/tools theo quyền của chính child và trả final assistant text. TaskManager tạo envelope `{session_id, status: "completed", output}`.
6. Parent runtime persist tool result và invocation completion; Root xem text để tổng hợp, kiểm chứng hoặc gọi tiếp cùng child ID. Cuối cùng persist final assistant message và run terminal state.

Nguồn: [HTTP-independent execution](../08-runtime-llm.md#82-interface-và-http-independent-execution), [foreground sequence](../06-delegation.md#63-foreground-delegation), [end-to-end flows](../11-flows.md#112-root--child--resume-foreground).

MVP chỉ **foreground**, depth=1, một child đồng thời; không giới hạn mặc định tổng số child calls tuần tự. Không có input `background` hoặc successful output `running`. Child không nhận toàn bộ Root history, loaded skills hay reasoning; Root phải đưa context cần thiết vào delegation prompt. Resume chỉ được phép với đúng parent/owner/workspace, phù hợp agent policy và child không busy. Child hoàn thành không có text thì trả placeholder với `empty_text=true`; lỗi kỹ thuật dùng ToolError, kèm child ID nếu đã tạo. [Nguồn: envelope/resume/errors](../06-delegation.md#62-inputoutput-envelope), [context isolation](../03-prompts.md#36-prompt-của-root-vs-subagent), [limits](../00-decisions.md#05-giới-hạn-và-cấu-hình).

## 5. Prompt, skill, tools và quyền

PromptBuilder compose theo thứ tự ổn định: **mandatory runtime base + agent prompt + project instructions + filtered skill catalog + session history/current objective + tool definitions**. MVP đọc root `AGENTS.md`; active run nên dùng snapshot instructions ổn định. Model-facing types là nội bộ, không để crate `llm` types lan vào business modules. Nội dung prompt không thay thế enforcement trong Rust. [Nguồn: prompt sources/construction](../03-prompts.md#32-các-nguồn-dữ-liệu), [project instructions](../03-prompts.md#35-load-project-instructions).

Skill là **gói instructions**, không phải agent/tool và không tự chạy code. Startup scan `skills/<id>/SKILL.md` một lần, validate metadata/IDs, giữ metadata + content snapshot. Context chỉ chứa catalog `id/name/description`; gọi `skill(id)` mới load toàn instructions vào tool result của đúng session. Resource paths chỉ là tham chiếu: đọc tiếp cần file tool được phép. Không auto-run scripts, không silent truncate instructions quá hard limit, không hot reload/global/remote skill ở MVP. Compaction có thể khiến agent cần gọi lại skill. [Nguồn: discovery/load](../04-skills.md#42-quy-trình-theo-opencode), [skill output/trust](../04-skills.md#45-skill-tool).

ToolExecutor phải kiểm tra input và resource permission mỗi lần gọi. Allowlists của AgentDefinition và session permissions chỉ được thu hẹp; child dùng quyền riêng. File tools cần canonicalize, chặn traversal/symlink escape. Tool output, file repo, skill instructions và child final text là untrusted context. Secrets lấy từ env/secret store; không đưa credentials, raw sensitive provider errors hay private reasoning vào transcript/log/SSE. [Nguồn: technical execution](../05-tools.md#54-execution--transcript-ownership), [trust/permission/secrets](../10-security-operations.md#101-trust-model).

Model-visible tool output có cap tham chiếu **50 KiB / 2.000 lines** và metadata `truncated`. Raw result được lưu `result_json` trong hard persistence cap; vượt hard cap phải báo technical error. Trong **baseline v4.1**, chưa có artifact store hay read-by-offset tổng quát trong MVP; đề xuất 17 mở rộng artifact-by-reference nhưng chưa được accepted (xem mục 12 bên dưới). Các con số trong config là gợi ý cho dự án, không phải defaults đã xác minh của OpenCode. [Nguồn: limits](../00-decisions.md#05-giới-hạn-và-cấu-hình), [large output](../05-tools.md#55-text-output-lớn), [retention](../09-persistence.md#96-tool-outputs-và-retention).

## 6. Persistence và an toàn sau crash

**Baseline v4.1** chỉ có **bốn bảng**: `sessions` cho conversation/ownership/parent/revision, `messages` cho ordered transcript, `agent_runs` cho attempts/turn/model/status/usage và `tool_invocations` cho tool input/outcome/child link. Delegation chính là invocation `tool_name='subagent'`, không lưu trùng status/output vào bảng `tasks`. [Nguồn: schema ownership](../09-persistence.md#92-bốn-bảng-tối-thiểu), [migration mẫu](../examples/001_init.sql).

Các invariant bền vững: sequence được cấp atomically, unique `(session_id, sequence)`; tool-call ID unique theo session; partial index bảo đảm một running run; persist calls trước side effects; tool-result message và invocation terminal update nằm cùng transaction; final text và completed run durable trước khi báo thành công. App còn phải enforce child-parent/owner/workspace, chống parent cycles và bảo đảm invocation trỏ run của cùng session — FK trong migration không tự làm hết các kiểm tra này. [Nguồn: constraints/transactions](../09-persistence.md#93-constraints-và-ownership), [invariants trong SQL](../examples/001_init.sql).

Startup recovery giả định **instance cũ đã dừng**. Scan stale running claims; safe attempt cũ thành `interrupted`, tạo run mới cùng `turn_id`, tăng `resume_attempt`, thêm synthetic continuation marker rồi resume. Attempts bị giới hạn (ví dụ 10) để tránh crash-loop. Không thêm user message giả hay run mới chỉ để né configured steps cap. Multi-instance leases/leader election nằm ngoài thiết kế MVP. [Nguồn: recovery](../15-long-running-sessions.md#157-safe-startup-recovery), [startup storage](../09-persistence.md#95-startup-recovery-single-instance).

Crash sau mutation nhưng trước confirmed result dẫn tới **outcome unknown**: invocation `interrupted`, execution bị block để reconcile, không blind replay và không bịa tool result. Read-only/idempotent tools chỉ được reconcile tự động khi có policy chứng minh an toàn. Không đưa dangling tool-call/result pair cho LLM. Recovery không cam kết exactly-once side effects. [Nguồn: tool crash semantics](../05-tools.md#54-execution--transcript-ownership), [crash flow](../11-flows.md#114-crash-giữa-side-effect-tool-và-result), [recovery policy](../15-long-running-sessions.md#157-safe-startup-recovery).

## 7. Long-running execution và context

`agent.steps` mặc định **None**, không có overall child/session deadline. Timeout từng LLM request/tool operation, explicit cancellation, permissions và quota/cost policy vẫn áp dụng. Nếu set `steps=N>0`, tới cap model phải trả text-only progress/remaining report với `tool_choice=none`; kết thúc turn nhưng session còn open, không tự spawn run mới. Parent cancel propagate xuống child theo khả năng cancellation của provider/tool. [Nguồn: optional steps](../15-long-running-sessions.md#155-optional-step-limit), [stop table](../15-long-running-sessions.md#158-stop-semantics-table), [Tokio guards](../08-runtime-llm.md#85-tokio-và-resource-limits).

Khi gần context limit, tính cả instructions/tool schemas/history/output reserve, summarize older complete turns và persist `messages.role='compaction'` gồm `summary`, `keep_from_sequence`, `version`, `model_ref`. Context mới gồm mandatory system + checkpoint + recent complete turns. **Giữ full original transcript**; frontier không cắt giữa tool call/result. Summary có thể mất chi tiết nên giữ goal/decisions/evidence/artifact refs. Provider context overflow trước output được compact/retry một lần; vẫn không vừa thì trả `CONTEXT_LIMIT` recoverable. [Nguồn: compaction](../15-long-running-sessions.md#156-context-compaction), [checkpoint storage](../09-persistence.md#98-compaction-checkpoint-không-thêm-bảng).

## 8. Điểm đặc trưng của baseline v4.1: streaming có ranh giới commit

### Model stream không trực tiếp kích hoạt side effects

`LlmGateway` chuẩn hóa `TextDelta`, `ToolCallReady`, `Finished`. `ToolCallReady` chỉ có nghĩa call ID/JSON arguments đã assemble và validate, **chưa được execute**. Runtime đợi toàn response `Finished` thành công, thu đủ calls, commit assistant message và mọi requested invocations rồi mới dispatch tuần tự theo thứ tự provider. Next model step chờ mọi outcomes persist xong. Nếu stream lỗi sau `ToolCallReady` nhưng trước `Finished`, không execute và không persist unconfirmed invocation; text deltas chỉ là preview chưa phải final. Provider thiếu partial tool-call stream có thể dùng full-response fallback. [Nguồn: normalized gateway](../08-runtime-llm.md#83-llmgateway--seam-nhỏ), [ordering contract](../16-streaming-events.md#165-provider-streaming-và-tool-call-assembly).

### Events để quan sát; SQLite giữ canonical state

| Loại | Ví dụ | Hợp đồng |
|---|---|---|
| Committed notification | `run.*` terminal/admitted, `message.committed`, `tool.started/completed/failed/interrupted`, `session.changed`, `compaction.completed` | Publish sau commit, có `state_revision`; chỉ invalidate snapshot. |
| Ephemeral | `text.delta`, `tool.progress`, `step.started/ended`, `compaction.started` | Có thể mất, không persist mỗi fragment và không replay khi reconnect. |
| SSE control | `stream.connected`, `resync.required` | Handshake/resync connection, không phải workflow durable. |

Nguồn: [taxonomy](../16-streaming-events.md#163-event-taxonomy-và-durability), [envelope](../16-streaming-events.md#164-event-envelope-và-identity), [connection protocol](../16-streaming-events.md#168-sse-endpoints-disconnect--reconnect).

Envelope có server-resolved `root_session_id`, source `session_id`, optional `run_id/state_revision`, timestamp và payload theo event type. Text delta nhận diện bằng session/message/step/delta index. Không tạo token UUID hay SSE `id:`/`Last-Event-ID` giả khi không có durable event log. `subagent`/`skill` dùng generic `tool.*` với tool name; child có own run/text/tool events, không thêm duplicate public `subagent.*` hoặc `skill.loaded`. Parent tool completion chỉ sau child final và parent tool result đều committed. [Nguồn: event identity](../16-streaming-events.md#164-event-envelope-và-identity), [tool/child lifecycle](../16-streaming-events.md#166-tool--subagent-lifecycle-progress-ownership).

`ToolContext::progress` dùng private sender, best-effort/nonblocking; ToolExecutor không nhận SSE client và không cần public ProgressReporter trait riêng. `ExecutionFeed` là concrete broadcast bounded; lagged reader nhận resync signal nếu gửi được rồi đóng connection, không block publisher/runtime hoặc subscriber khác. Heartbeat riêng connection; disconnect không cancel. [Nguồn: progress ownership](../16-streaming-events.md#166-tool--subagent-lifecycle-progress-ownership), [backpressure](../16-streaming-events.md#169-backpressure--resource-policy).

### Reconnect bằng snapshot + invalidation

1. Mở `GET /sessions/{root_id}/events?include_children=true`, đợi `stream.connected`, buffer notifications/previews.
2. Gọi `GET /sessions/{root_id}/snapshot?include_children=true`, hydrate canonical family state và revision từng session.
3. Committed event có revision mới hơn chỉ lên lịch debounced snapshot refresh; nếu có invalidation mới trong lúc fetch thì tiếp tục refresh cho tới khi hội tụ. Không apply event payload như state patches.
4. Render ephemeral preview chỉ khi entity/step còn active; snapshot terminal luôn thắng. Disconnect/Lagged thì bỏ preview chưa confirmed và bắt đầu lại từ subscribe trước snapshot.

Nguồn: [revision/snapshot](../16-streaming-events.md#167-sqlite-snapshot--state_revision), [reconnect protocol](../16-streaming-events.md#168-sse-endpoints-disconnect--reconnect).

`sessions.state_revision` tăng một lần mỗi session bị thay đổi trong transaction, khác message sequence và không phải SSE replay cursor. Family snapshot đọc state/revisions/membership nhất quán trong một read transaction, history dài được phân trang. Event publish có thể out of order (rev16 trước rev15) mà không rollback canonical UI vì snapshot bao phủ committed state. Auth phải kiểm tra ownership/workspace/ancestry cả SSE và snapshot, kể cả child tạo sau subscription; envelope root ID không thay auth. [Nguồn: revisions](../16-streaming-events.md#167-sqlite-snapshot--state_revision), [security](../16-streaming-events.md#1610-security-authorization--observability).

## 9. Các file mẫu chứng minh ý đồ cấu hình

| File | Điều thể hiện | Giới hạn |
|---|---|---|
| [`agents.yaml`](../examples/agents.yaml) | Root OpenAI, explorer Google, reviewer Anthropic; root có subagent/skill/file tools, children chỉ read/search/skill; no default steps/child deadline; compaction/recovery/events config. | Schema dự án và model IDs minh họa; phải validate thực tế. |
| [`001_init.sql`](../examples/001_init.sql) | Bốn tables, statuses/checks/FKs/unique admission, state_revision, recovery/streaming comments. | Migration tham khảo; nhiều invariants thuộc Rust workflow, chưa chứng minh production migration. |
| [`root.md`](../examples/prompts/root.md) | Root tự đánh giá child, follow-up session cũ, báo uncertainty, không unsafe replay; giữ mục tiêu qua long-running work. | Prompt guidance cần runtime enforcement. |
| [`explorer.md`](../examples/prompts/explorer.md), [`reviewer.md`](../examples/prompts/reviewer.md) | Chuyên môn riêng, file evidence, history isolation, final text tự do. | Không tự tạo quyền đọc/sửa ngoài allowlists. |
| [`task-planning`](../examples/skills/task-planning/SKILL.md), [`rust-review`](../examples/skills/rust-review/SKILL.md) | Workflows cho delegation/assessment và Rust review có evidence; không bắt JSON. | Là instructions mẫu; không phải module thực thi. |

Hướng dẫn bootstrap của repo là copy examples sang `config/`, `prompts/`, `skills/` khi khởi tạo; đây chưa phải cấu hình runtime đã vận hành. [Nguồn: dùng file mẫu](../README.md#tài-liệu-mẫu).

## 10. MVP, gates và trạng thái chứng cứ

Roadmap chạy M0 bootstrap → M1 single agent → M2 tools → M3 skills → M4 foreground child/resume → M5 long-running/compaction → M6 recovery/safety → M7 SSE/family snapshot. Tài liệu có **37 acceptance scenarios AT-01–AT-37** và **15 streaming scenarios ST-01–ST-15**. Đây là kế hoạch/expected behavior, **không phải kết quả test đã pass**. [Nguồn: milestones/AT/DoD](../12-mvp-test-plan.md), [ST suite](../16-streaming-events.md#1611-acceptance-criteria).

Gate kỹ thuật trước khi phụ thuộc sâu vào crate `llm`: khóa phiên bản, spike **OpenAI + một provider khác** cho text/multi-turn, tool call→result→continuation giữ IDs, capabilities, error classes, timeout/cancellation, usage và streaming/error-mid-stream. Exact stream return type/API phải theo spike; không tự đoán crate API từ pseudocode. [Nguồn: pre-implementation verification](../00-decisions.md#07-điều-cần-xác-minh-trước-implementation), [provider spike](../08-runtime-llm.md#86-provider-spike-bắt-buộc).

Definition of Done gồm migrations từ DB trống, fmt/clippy/tests/integration SQLite, không hardcoded secrets, E2E Root→child→skill→Root follow-up→final, provider thứ hai, long-running/compaction/safe restart và SSE/snapshot. Tests nên phân tầng unit, real SQLite integration/races, scripted fake LLM, provider smoke và E2E. Các lỗi cần test đặc biệt: DB terminal commit fail không phát success giả; unknown mutation không replay; provider stream lỗi trước Finished không dispatch; calls trong cùng response dispatch tuần tự; snapshot race/out-of-order revision; cross-owner/secret leak. [Nguồn: test pyramid/DoD](../12-mvp-test-plan.md#122-test-pyramid), [AT details](../12-mvp-test-plan.md#123-acceptance-tests-chi-tiết).

Ngoài MVP **theo baseline v4.1**: background child/inbox, parallel child jobs, depth>1, distributed workers/leases, business task scheduler, queue brokers, semantic validator bắt buộc, general artifact store, skill remote/global/hot reload, provider-native compaction, provider fallback policy, WebSocket và lossless event replay. Mở rộng phải giữ các invariant cốt lõi; thay quyết định kiến trúc cần cập nhật module docs/ADR/tests tương ứng. [Nguồn: non-goals](../00-decisions.md#04-không-thuộc-mvp), [sau MVP](../12-mvp-test-plan.md#125-tính-năng-để-sau), [quy tắc tài liệu](../README.md#nguyên-tắc-điều-hướng), [ADR policy](../13-adrs.md).

## 11. Những chỗ cần làm rõ khi chuyển sang implementation

Đây là các khoảng trống hoặc lệch biểu diễn của **thiết kế**, không phải kết luận có bug runtime:

- **Recovery reconciliation/unblock:** policy đã chốt block mutation outcome unknown và chỉ resume khi safe, nhưng tài liệu chưa đặc tả command/API, evidence model và transaction workflow cụ thể để operator reconcile/unblock parent-child execution. Cần chốt trước M6. [Nguồn: recovery flow](../15-long-running-sessions.md#157-safe-startup-recovery), [tool transactions](../09-persistence.md#94-transactions-quan-trọng).
- **Shape của `ToolContext` chưa đồng nhất:** mục 05 minh họa owned `String` với `agent_id`; mục 16 dùng lifetime/borrowed strings và thêm `call_id`. Các ví dụ cùng giữ progress/private sender và borrowed runtime contract, nhưng cần hợp nhất concrete shape trước code. [Đối chiếu: 05 interface](../05-tools.md#53-interface-nội-bộ), [16 progress interface](../16-streaming-events.md#166-tool--subagent-lifecycle-progress-ownership).
- **YAML → Rust types:** config mẫu dùng map agents và fields `prompt/provider/model/tools/skills`, còn struct AgentDefinition dùng `id/prompt_path/provider_id/model_id/allowed_tools/allowed_skills`. Đây chưa phải lỗi nếu có config DTO/conversion/serde mapping, nhưng mapping và validation chưa được đặc tả. [Đối chiếu: YAML](../examples/agents.yaml), [AgentDefinition](../02-agents.md#22-agent-definition-thiết-kế).
- **Một số tên/nhãn cần đọc theo ngữ cảnh:** header SQL vẫn ghi “v3 continuous-session example” dù đã có `state_revision` và streaming invariants; ADR-001 dùng chữ “session lock” trong consequences, trong khi quyết định cụ thể ADR-007/010 và persistence đã chốt SQLite partial unique index, không thêm mutex. Các điểm này cần đồng bộ wording nếu sửa docs sau; bản tổng hợp không tự sửa quyết định. [Đối chiếu: SQL header](../examples/001_init.sql), [ADR-001](../13-adrs.md#adr-001--shared-runtime--parentchild-sessions), [ADR-007](../13-adrs.md#adr-007--sqlite--sqlx), [admission rule](../07-sessions-context.md#75-single-active-run--session).
- **Giá trị cấu hình và provider support vẫn cần bằng chứng:** token estimates/context threshold, tool persistence hard cap, event payload cap/throttle và timeout từng tool cần quyết định/đo phù hợp model/runtime. Không lấy các ví dụ 120 giây, 15.000 tokens, 4.096 events hay model IDs làm cam kết provider/current production. [Nguồn: suggested policies](../00-decisions.md#05-giới-hạn-và-cấu-hình), [gateway verification](../08-runtime-llm.md#86-provider-spike-bắt-buộc), [event resource policy](../16-streaming-events.md#169-backpressure--resource-policy), [sample config](../examples/agents.yaml).

Các quyết định chủ đạo nhất quán xuyên tài liệu: **Root-controlled assessment; shared runtime với isolated sessions; một transcript writer; một admission guard; continuous execution không cap mặc định; checkpoint giữ original history; không replay mutation unknown; commit trước public terminal event; SSE invalidation + snapshot thay event reducer**. Đây là các ràng buộc cần mang theo khi hiện thực từng milestone, không phải bằng chứng rằng milestone đã hoàn thành. [Nguồn tổng hợp: D-01–D-22](../00-decisions.md#02-các-quyết-định-đã-chốt), [ADR-001–014](../13-adrs.md).


## 12. Đề xuất 17: Agent Results & Artifacts

### Trạng thái và ảnh hưởng tới baseline

Tài liệu 17 đề xuất thêm **ArtifactStore** cho outputs lớn và **AgentResult** để đọc kết quả theo turn; đây là mở rộng scope có chủ đích. Chỉ **khi được accepted**, mới bổ sung bảng `artifacts` thứ năm, cột `tool_invocations.checkpoint_artifact_id` và đồng bộ docs 05/09 cùng decisions ledger. Hiện không được diễn giải đề xuất này thành thay đổi đã chốt của baseline bốn bảng hoặc general artifact store trong MVP. [Nguồn: persistence extension](../17-agent-results-artifacts.md#174-persistence-and-filesystem), [compatibility](../17-agent-results-artifacts.md#1715-compatibility-notes-and-open-questions), [baseline retention](../09-persistence.md#96-tool-outputs-và-retention), [ADR-007](../13-adrs.md#adr-007--sqlite--sqlx).

Foreground `subagent(agent, description, prompt, session_id?)`, `SubagentOutput {session_id, status, output}`, final text tự do và Root tự đánh giá vẫn giữ nguyên. `AgentResult` không thay envelope delegation, không bắt child trả business JSON và không thêm TaskContract/ResultValidator/reviewer/scheduler. Run `completed` vẫn có thể chỉ hoàn thành 18/20 mục nghiệp vụ. [Nguồn: decisions/non-goals](../17-agent-results-artifacts.md#171-decisions-and-non-goals), [compatibility](../17-agent-results-artifacts.md#1715-compatibility-notes-and-open-questions), [baseline envelope](../06-delegation.md#62-inputoutput-envelope).

### Hai khái niệm mới và quyền sở hữu

| Khái niệm | Hợp đồng đề xuất |
|---|---|
| `Artifact` / `ArtifactRef` | Bytes đã commit, bất biến, metadata và stable ID; ref chỉ chứa descriptor có giới hạn (`id/name/mime_type/size_bytes/role`), không mang bytes hay trusted filesystem path. Primary/intermediate là vai trò hiển thị. |
| `ArtifactStore` | Deep module concrete local FS + SQLite metadata; `put/get/list_for_turn` che giấu staging/publication/hash/integrity và authorized reads. Chưa cần trait S3 khi chưa có adapter thứ hai. |
| `AgentResult` | Read projection của logical turn: latest run attempt/status, final text đã xác nhận và authorized refs; không thêm source-of-truth result row hoặc result manager có state riêng. |
| Ownership | SessionStore giữ execution/transcript/invocation và durable manifest pointer; AgentRuntime điều phối recovery/commit; tools giữ nội dung domain manifest và provider reconciliation; ArtifactStore không sửa run/tool state. |

Nguồn: [vocabulary](../17-agent-results-artifacts.md#172-vocabulary-and-ownership), [architecture/interfaces](../17-agent-results-artifacts.md#173-architecture), [result read flow](../17-agent-results-artifacts.md#176-agentresult-projection-and-read-flow).

`AgentResult` chọn attempt bằng `resume_attempt` tăng dần, dùng unique run identity để tie-break; attempt `interrupted` cũ không lấn trạng thái attempt mới. `output` chỉ lấy từ final assistant message đã commit cho terminal attempt được chọn, còn running trả `None` dù có thể đã có artifacts. Danh sách refs cần phân trang khi lớn. Ancestry Root/Child không tự cấp quyền: read/list/download đều phải kiểm tra owner/workspace/session policy. [Nguồn: projection rules](../17-agent-results-artifacts.md#176-agentresult-projection-and-read-flow).

### Publication, trust và ba ranh giới commit

Publication dùng private configured root, server-generated opaque keys, create-new staging, streamed size/hash checks, file fsync, atomic rename cùng filesystem và directory sync nơi hỗ trợ, rồi publish metadata `ready`. Bytes đã commit không bị overwrite; tái tạo sinh ID mới. FS và SQLite không có chung atomic transaction: file thiếu metadata là orphan để xử lý sau grace period; metadata trỏ bytes thiếu/hỏng phải báo integrity error. Đề xuất MVP giữ committed artifacts, chưa có TTL/deletion/reference-aware GC; backup/restore phải gồm cả DB và files. [Nguồn: filesystem/retention](../17-agent-results-artifacts.md#174-persistence-and-filesystem).

**Artifact ready, durable checkpoint và tool completed là ba ranh giới khác nhau.** Artifact có thể publish trước khi runtime commit tool result; khi crash phải reconcile invocation, không suy ra external mutation an toàn để replay. ToolOutcome vẫn là bounded content + trusted `metadata.artifact_ids`; model tự nhắc `artifact://...` không tạo verified ref hay quyền đọc. `ArtifactContext` do runtime dựng từ authenticated identity/permissions; model-facing reads có cap, client download stream qua endpoint authorized; không đưa base64/media đầy đủ vào transcript/SSE. [Nguồn: tool trust contract](../17-agent-results-artifacts.md#175-tool-contract-trust-and-provider-operations), [security/quotas](../17-agent-results-artifacts.md#1712-security-quotas-and-operational-constraints).

Provider `accepted` chỉ là receipt/job ID, chưa phải media artifact hay Knowledge ingest thành công. Persist job ID/idempotency key bằng durable invocation metadata/checkpoint, xác nhận trạng thái và retrieve/verify media trước `put`; Knowledge chỉ báo thành công khi có confirmed `knowledge_id/version` receipt. Crash outcome unknown phải query/reconcile, nếu chưa rõ thì block thay vì submit lại. External receipts không phải local artifacts. [Nguồn: provider operations](../17-agent-results-artifacts.md#175-tool-contract-trust-and-provider-operations), [document flow](../17-agent-results-artifacts.md#178-flow-b--pdfdocx--markdown--summary--knowledge-base), [media flow](../17-agent-results-artifacts.md#179-flow-c--imagevideo-generation).

### Durable manifest khác compaction và SSE

Playlist checkpoint là immutable versioned manifest artifact, không thêm per-item checkpoint table. Sau `put(manifest_vN)`, trusted tool context yêu cầu SessionStore CAS `checkpoint_artifact_id` trên **invocation hiện có**, kiểm tra owner/status/expected previous ID rồi tăng session revision. Chỉ sau CAS mới được báo item completion đã durable. Restart đọc **pointer đã commit**, kiểm chứng manifest và completed-item refs; file mới nhất hoặc SSE progress không có authority. Manifest publish nhưng CAS thất bại là candidate chưa authoritative, giữ để kiểm tra theo chính sách chưa GC. Follow-up invocation mới phải truyền verified manifest ID và commit initial pointer riêng, không tự kế thừa pointer cũ. [Nguồn: durable pointer protocol](../17-agent-results-artifacts.md#177-flow-a--youtube-playlist--transcript--asr--analysis).

Compaction summary giữ objective, manifest IDs, refs, failed IDs và external receipts để model tiếp tục, nhưng không thay durable domain checkpoint. SSE tiếp tục best-effort invalidation theo doc16: không mang artifact bytes, không persist mỗi progress tick; `tool.completed` hoặc optional `session.changed` sau association/metadata commit kích hoạt refresh. Canonical result/list/download endpoints đọc stores đã authorize; disconnect không hủy creation/execution. [Nguồn: recovery/compaction](../17-agent-results-artifacts.md#1710-crash-recovery-idempotency-and-compaction), [SSE integration](../17-agent-results-artifacts.md#1711-sse-http-and-ui), [baseline snapshot](../16-streaming-events.md#167-sqlite-snapshot--state_revision).

### Ba workload minh họa, phases và bằng chứng cần có

| Workload | Kết quả và quy tắc quan trọng |
|---|---|
| Playlist → transcript/ASR → analysis | Transcript có sẵn thì dùng; thiếu thì audio → ASR; lưu transcript/analysis/summary riêng, manifest theo item/revision/model/status/verified refs; resume chỉ skip mục completed đã kiểm chứng. |
| PDF/DOCX → Markdown → summary → Knowledge | Giữ provenance/page mapping/warnings/checksum/conversion version; extracted document và summary riêng; ingest pending/failure không được tạo receipt thành công giả. |
| Image/video generation | Submit bằng idempotency key, persist provider job ID, poll bounded từng operation; chỉ sau retrieve/validate file mới publish artifact; crash không gây duplicate generation. |

Nguồn: [Flow A](../17-agent-results-artifacts.md#177-flow-a--youtube-playlist--transcript--asr--analysis), [Flow B](../17-agent-results-artifacts.md#178-flow-b--pdfdocx--markdown--summary--knowledge-base), [Flow C](../17-agent-results-artifacts.md#179-flow-c--imagevideo-generation).

**Phase A** xây ArtifactStore local + metadata, authorized streaming reads và publication/integrity tests. **Phase B** tích hợp CAS pointer, result projection, ba workloads và provider reconciliation. Đây là plan của đề xuất, chưa thay roadmap M0–M7/AT/ST hay chứng minh có tests pass. Test seam chính: ArtifactStore contracts, CAS stale/wrong-owner rejection, latest-attempt projection; fault injection tại staging/rename/metadata/result/CAS; E2E playlist 18/20, ingest pending, media restart và >100 steps qua compaction/disconnect/restart. [Nguồn: verification plan](../17-agent-results-artifacts.md#1713-verification-plan), [phases](../17-agent-results-artifacts.md#1714-incremental-implementation-plan), [baseline roadmap](../12-mvp-test-plan.md).

Các chi tiết cần chốt khi hiện thực gồm file-size/quota/disk reserve và cách truy nguyên final message về đúng terminal attempt cho projection; schema baseline chưa có direct `run_id/turn_id` trên message rows nên cần concrete lookup/lineage phù hợp invariant, không suy diễn pseudocode là implementation đã có. Internal staging intent chỉ nên thêm khi crash tests chứng minh cần; không mở rộng thành agent-facing state machine, remote adapter hay GC trước nhu cầu. [Nguồn: projection requirement](../17-agent-results-artifacts.md#176-agentresult-projection-and-read-flow), [publication policy](../17-agent-results-artifacts.md#174-persistence-and-filesystem), [open questions](../17-agent-results-artifacts.md#1715-compatibility-notes-and-open-questions), [baseline schema](../examples/001_init.sql).
