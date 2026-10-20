# Báo cáo tổng hợp vấn đề thiết kế Orchestrator Agent RS

> **Historical review snapshot (2026-10-20):** findings bên dưới phản ánh baseline đối chiếu `25f13d4`, **trước v5.0**. Cột “còn mở” là trạng thái khi review, không phải trạng thái hiện hành; xem phần 8 cuối báo cáo.

**Ngày:** 2026-10-20 · **Mốc đối chiếu:** `25f13d4` · **Phạm vi:** toàn bộ **29 file** hiện có trong `docs/`: README, 19 tài liệu `00–18`, 7 file mẫu và 2 báo cáo nghiên cứu (gồm báo cáo này).

Báo cáo kết hợp các phát hiện trước với lượt rà soát mở rộng toàn bộ tài liệu: tính nhất quán của thuật ngữ, public interfaces, prompt/skill/permission, delegation, transaction/recovery, long-running, streaming, artifacts, embedding, tests và bootstrap examples. Baseline v4.1 đã Accepted về thiết kế; tài liệu 17 và 18 còn **Proposed**. Đây là đánh giá hợp đồng thiết kế, chưa phải kết luận có lỗi runtime hay lỗ hổng bảo mật đã khai thác được. Repo chưa có implementation Rust để kiểm chứng các tình huống dưới đây. [Nguồn: trạng thái baseline](../README.md), [đề xuất 17](../17-agent-results-artifacts.md), [đề xuất 18](../18-embeddable-library.md).

## 1. Danh mục và cách đọc

Findings được giữ riêng theo hai trục **Standards** và **Spec**. P2 biểu thị vấn đề thiết kế cần sửa/làm rõ trước khi triển khai seam liên quan, không phải mức độ một exploit đã xác nhận. Các khoảng trống chưa được gán P2 là chi tiết cần đặc tả hoặc kiểm chứng. Thay đổi có chủ đích giữa Proposed và Accepted được ghi riêng, không tự coi là lỗi. Một ID có thể ảnh hưởng nhiều tài liệu; không đếm lại thành nhiều findings.

| ID | Trục / loại | Mức độ | Nội dung | Trạng thái |
|---|---|---|---|---|
| STD-01 | Standards | P2 | Checklist acceptance thiếu ADR superseding | Còn mở |
| STD-02 | Standards | P2 | Skill catalog chưa nhận session scope để lọc quyền thực tế | Còn mở |
| SPEC-01 | Spec | P2 | HostTool thiếu scoped artifact reader | Còn mở |
| SPEC-02 | Spec | P2 | Shutdown chưa buộc tasks ngừng trước khi nhả owner lock | Còn mở; cần làm rõ wording |
| SPEC-03 | Spec | P2 | Delegation thiếu durable mapping parent invocation → child turn | Còn mở |
| GAP-01 | Khoảng trống | Cần đặc tả | Liên kết final message với đúng run attempt | Còn mở |
| GAP-02 | Khoảng trống | Cần thống nhất | Các hình dạng ToolContext chưa đồng nhất | Còn mở |
| GAP-03 | Khoảng trống | Cần đặc tả | Mapping cấu hình YAML sang AgentDefinition | Còn mở |
| GAP-04 | Chứng cứ | Gate trước triển khai | Crate/provider compatibility và cấu hình vận hành | Chưa kiểm chứng |
| GAP-05 | Khoảng trống | Cần đặc tả | Namespace của provider tool-call ID và internal invocation ID | Còn mở |
| GAP-06 | Khoảng trống | Cần đặc tả | Nguồn mandatory base prompt trong bootstrap | Còn mở |
| GAP-07 | Khoảng trống | Cần đặc tả | Hội tụ UI nếu mất notification mà SSE vẫn kết nối | Còn mở |
| GAP-08 | Khoảng trống | Cần rõ thuật ngữ | “Complete turns” và compaction trong active turn | Còn mở |
| DOC-01 | Tài liệu | Ưu tiên thấp | Nhãn migration và thuật ngữ session lock dễ gây hiểu nhầm | Còn mở |
| DOC-02 | Tài liệu | Ưu tiên thấp | Chỉ mục README chưa dẫn tới đề xuất 17/18 | Còn mở |
| ALIGN-01 | Thay đổi scope | Quyết định khi acceptance | ArtifactStore, bảng thứ năm và checkpoint pointer | Proposed; chưa thay baseline |
| ALIGN-02 | Thay đổi kiến trúc | Quyết định khi acceptance | Axum-first sang library-first và transport của host | Proposed; chưa thay baseline |
| ALIGN-03 | Thay đổi public contract | Quyết định khi acceptance | AgentResult và roadmap triển khai | Proposed; cần đồng bộ |

### 1.1. Bảng bao phủ toàn bộ tài liệu

“Không thêm finding độc lập” nghĩa là lượt review này không tìm được vấn đề riêng đủ bằng chứng; không có nghĩa implementation đã an toàn hoặc đã vượt tests. Các báo cáo trong `research/` là nguồn dẫn xuất, không thay thế tài liệu thiết kế gốc.

| File | Nội dung đã rà soát | Phát hiện/liên hệ |
|---|---|---|
| [README](../README.md) | Trạng thái baseline, chỉ mục, bootstrap, quy tắc cập nhật | DOC-02, GAP-06, ALIGN-01–03 |
| [00 Decisions](../00-decisions.md) | D-01–22, scope, defaults, success criteria | ALIGN-01/02, GAP-04; không thêm finding độc lập |
| [01 Architecture](../01-architecture.md) | Modules, ownership, source tree, execution | ALIGN-02, GAP-06; không thêm finding độc lập |
| [02 Agents](../02-agents.md) | Definition, mode, snapshot, lifecycle, permissions | GAP-03/08; không thêm finding độc lập |
| [03 Prompts](../03-prompts.md) | Composition, isolation, project instructions, compaction | GAP-06/08 |
| [04 Skills](../04-skills.md) | Startup snapshot, catalog/load, resource trust | STD-02 |
| [05 Tools](../05-tools.md) | Dispatch, transcript ownership, output/error, unknown effect | GAP-02/05, ALIGN-01 |
| [06 Delegation](../06-delegation.md) | Foreground, resume, error/result, long-running | SPEC-03 |
| [07 Sessions](../07-sessions-context.md) | Identity, ordering, admission, recovery, ownership | GAP-01/08, SPEC-03, DOC-01 |
| [08 Runtime/LLM](../08-runtime-llm.md) | Loop/Finished ordering, adapter, Tokio, provider spike | GAP-04/05, ALIGN-02; không thêm finding độc lập |
| [09 Persistence](../09-persistence.md) | Schema, transactions, recovery, checkpoints, snapshot | GAP-01/05, SPEC-03, ALIGN-01 |
| [10 Security](../10-security-operations.md) | Scope, permissions, secrets, cancellation, event privacy | STD-02; không có exploit runtime được xác nhận |
| [11 Flows](../11-flows.md) | Admission, child resume, crash windows, streaming | SPEC-03 |
| [12 MVP/Test plan](../12-mvp-test-plan.md) | M0–M7, AT-01–37, DoD, evidence boundaries | ALIGN-03; cần bổ sung scenarios theo findings |
| [13 ADRs](../13-adrs.md) | Accepted decisions và superseding policy | STD-01, DOC-01, ALIGN-01/02 |
| [14 Glossary](../14-glossary.md) | Agent/session/turn/step, artifact, streaming terms | GAP-08, ALIGN-01; không thêm finding độc lập |
| [15 Long-running](../15-long-running-sessions.md) | Turn/step, compaction, recovery, stop semantics | SPEC-03, GAP-08 |
| [16 Streaming](../16-streaming-events.md) | Taxonomy, revisions, subscribe/snapshot, lag, auth | GAP-02/07, ALIGN-02 |
| [17 Results/Artifacts](../17-agent-results-artifacts.md) | Projection, publication, CAS, workloads, recovery | GAP-01, SPEC-01, ALIGN-01/03 |
| [18 Embedding](../18-embeddable-library.md) | Host seam, lifecycle, tools, providers, reads, packaging | STD-01, SPEC-01/02; đề xuất bổ sung một số gap cũ |
| [Migration mẫu](../examples/001_init.sql) | Bốn bảng, FK/check/unique, invariants giao cho app | GAP-01/05, SPEC-03, DOC-01 |
| [Agent YAML](../examples/agents.yaml) | Names/modes/permissions, defaults, provider references | GAP-03/04; model IDs chỉ minh họa |
| [Root prompt](../examples/prompts/root.md) | Delegation/assessment, follow-up, unsafe replay, objectives | GAP-06 ở bộ scaffold; không thêm finding trong prompt |
| [Explorer prompt](../examples/prompts/explorer.md) | Isolation, authorized reads, evidence, free text | Không thêm finding độc lập |
| [Reviewer prompt](../examples/prompts/reviewer.md) | Scope, uncertainty, skills/tools, final text | Không thêm finding độc lập |
| [Rust review skill](../examples/skills/rust-review/SKILL.md) | Metadata/workflow, evidence, quyền sửa | Không thêm finding độc lập |
| [Task-planning skill](../examples/skills/task-planning/SKILL.md) | Delegation, reuse child, side-effect caution | Không thêm finding độc lập |
| [Project understanding](project-understanding.md) | Bản tổng hợp 00–17, sources, gaps cũ | Có scope công bố 00–17; báo cáo này bổ sung review 18 và toàn bộ corpus |
| [Báo cáo này](design-review-report.md) | Dẫn nguồn, phân loại, trùng lặp, coverage, giới hạn | Nguồn dẫn xuất; findings cần được xử lý ở tài liệu gốc |

## 2. Standards — finding cần sửa

### STD-01 · P2 · Thiếu ADR superseding trong checklist acceptance

**Bằng chứng:** Mục 18.15 liệt kê các tài liệu cần cập nhật nhưng không có `docs/13-adrs.md`. Trong khi đó, quy tắc ADR yêu cầu ghi quyết định superseding khi thay đổi lịch sử Accepted. EMB-01/02/05 điều chỉnh entry point gọi trực tiếp AgentRuntime trong ADR-009 và vị trí transport Axum/SSE của baseline/ADR-014. [Nguồn: checklist 18.15](../18-embeddable-library.md#1815-đồng-bộ-khi-accept-v50), [quy tắc ADR](../13-adrs.md), [ADR-009](../13-adrs.md#adr-009--deep-modules-one-transcript-writer), [ADR-014](../13-adrs.md#adr-014--streaming-ephemeral-events--durable-session-snapshots).

**Khi phát sinh:** Đề xuất 18 được accepted và người triển khai chỉ làm theo checklist hiện tại.

**Tác động:** Architecture/README mô tả library-first nhưng ADR Accepted vẫn mô tả entry point cũ; người triển khai không có chỉ dẫn rõ phần nào đã được thay thế.

**Đề nghị và điều kiện đóng:** Thêm bước ghi ADR library-first, chỉ rõ phần ADR-009/014 được supersede, giữ nguyên những invariant vẫn áp dụng và lịch sử quyết định. Không cần đánh dấu toàn bộ hai ADR cũ mất hiệu lực.

### STD-02 · P2 · Skill catalog thiếu session scope để lọc effective permissions

**Bằng chứng:** `SkillManager.available(agent_id)` chỉ nhận agent, còn `load(session_id, id)` nhận session. Chính mục 4.4 yêu cầu cả hai tính quyền thực tế; mục 10.2 cho phép session thu hẹp quyền của AgentDefinition và yêu cầu lọc catalog. [Nguồn: SkillManager interface](../04-skills.md#44-interface-một-concrete-module), [permission policy](../10-security-operations.md#102-permission--ownership).

**Khi phát sinh:** Hai session dùng cùng agent nhưng được áp hạn chế skills khác nhau.

**Tác động:** Chữ ký catalog hiện tại không cung cấp scope để phân biệt hai session. Metadata skill bị cấm có thể vẫn vào prompt, trái yêu cầu filtered catalog. `load()` vẫn phải deny nên đây chưa phải bypass execution authorization.

**Đề nghị và điều kiện đóng:** Cho `available()` nhận session hoặc effective permission snapshot đã resolve; PromptBuilder dùng đúng scope đó. Test hai session cùng agent có quyền khác nhau, xác nhận skill bị deny vừa không xuất hiện trong catalog vừa không load được.

## 3. Spec — findings về hợp đồng tích hợp

### SPEC-01 · P2 · HostTool chưa có scoped artifact reader

**Bằng chứng:** Bảng ToolContext chỉ nêu `artifacts().put(...)`; `checkpoint().current()` trả manifest pointer. ArtifactStore là internal module và host không được đọc private filesystem. Tuy nhiên workflow ASR/Knowledge cần tiêu thụ dữ liệu qua artifact ref; recovery manifest còn yêu cầu kiểm chứng bytes và các output refs trước khi bỏ qua mục đã hoàn thành. [Nguồn: ToolContext](../18-embeddable-library.md#1862-toolcontext-là-scoped-capability-không-phải-sessionstore), [host ASR flow](../18-embeddable-library.md#1810-end-to-end-host-integration-flows), [manifest recovery](../17-agent-results-artifacts.md#177-flow-a--youtube-playlist--transcript--asr--analysis).

**Khi phát sinh:** Một tool nhận audio/document artifact ID do tool trước tạo, hoặc đọc lại manifest khi tiếp tục batch.

**Tác động:** Adapter không có đường đọc dữ liệu trong contract hiện tại. Nó phải vòng ngược qua Orchestrator hoặc yêu cầu quyền đọc storage ngoài seam đã thiết kế. `Orchestrator::open_artifact` phục vụ caller của host chưa thay thế được capability scoped cho invocation.

**Đề nghị:** Thêm `context.artifacts().open/get(id)` trả async reader có giới hạn, kiểm tra effective permissions, owner/workspace và chính sách session ancestry; giữ storage path private và báo integrity errors. Đặc tả rõ quyền đọc input từ invocation/session khác trong cùng workflow.

**Điều kiện đóng:** Contract test cho HostTool đọc output của tool trước, đọc manifest sau recovery, từ chối ID ngoài scope và báo dữ liệu hỏng. Adapter không cần SQL, filesystem path hoặc gọi vòng lại Orchestrator.

### SPEC-02 · P2 · Điều kiện nhả owner lock khi shutdown chưa đủ rõ

**Bằng chứng:** Shutdown hết grace chỉ yêu cầu cooperative cancel, sau đó close resources và nhả single-owner lock. Tài liệu chưa ràng buộc rõ mọi owned local task còn khả năng ghi phải ngừng và được join trước bước nhả lock, dù invariant yêu cầu không có hai owners trên cùng run. [Nguồn: lifecycle/shutdown](../18-embeddable-library.md#189-managed-tokio-execution-và-lifecycle), [baseline recovery chỉ khi instance cũ đã dừng](../07-sessions-context.md#75-single-active-run--session).

**Khi phát sinh:** HostTool chưa phản ứng với cancellation, hoặc còn cleanup/write sau khi shutdown grace hết; host process vẫn sống và mở instance mới trên cùng store.

**Tác động có điều kiện:** Nếu lock được nhả khi task cũ vẫn hoạt động, instance mới có thể recovery trong lúc task cũ tiếp tục ghi hoặc thực hiện side effect. Chưa có implementation để kết luận race này thực sự xảy ra; đây là thiếu ràng buộc trong contract.

**Đề nghị:** Quy định shutdown chỉ thành công và chuyển ownership sau khi các task còn quyền ghi đã quiesce và được join. Nếu không bảo đảm được, giữ owner guard và trả lỗi/trạng thái chưa dừng. Làm rõ Drop khi host Tokio runtime vẫn sống. External operation đã gửi ra ngoài nhưng chưa rõ kết quả vẫn cần reconciliation; local task đã dừng không chứng minh external effect đã dừng.

**Điều kiện đóng:** Fault test với tool phản ứng chậm/không cooperative: instance thứ hai chưa acquire store khi task cũ còn quyền ghi; shutdown không báo thành công giả; restart vẫn giữ unknown mutation blocked.

### SPEC-03 · P2 · Parent invocation chưa liên kết bền vững với đúng child turn

**Bằng chứng:** Delegation tạo/link `child_session_id`, rồi mới `submit(child, prompt)` và `wait(turn_id)`. Schema parent invocation chỉ có child session ID; một child có nhiều turns qua các lần follow-up. Chưa nêu durable mapping từ invocation tới đúng child turn hoặc idempotent admission key để recovery suy ra mapping này. [Nguồn: delegation sequence](../06-delegation.md#63-foreground-delegation), [resume flow](../11-flows.md#112-root--child--resume-foreground), [migration](../examples/001_init.sql), [recover child trước parent final](../06-delegation.md#69-subagent-chạy-hàng-giờ).

**Khi phát sinh:** Crash sau link child nhưng trước admission; hoặc child đã hoàn tất nhưng parent chưa commit tool result. Trường hợp follow-up dùng child cũ còn có các child turns đã completed từ trước.

**Tác động:** Recovery chỉ biết child session thì chưa đủ phân biệt lần delegate hiện tại với lần trước: có thể submit trùng objective, ghép nhầm final hoặc phải block một flow lẽ ra có thể reconcile bằng dữ liệu nội bộ. So sánh prompt text không đủ vì user có thể gửi cùng nội dung nhiều lần.

**Đề nghị:** Persist identity parent-invocation → child-turn và làm child admission atomic/idempotent theo identity đó; có thể dùng cột/link hoặc deterministic key đã đặc tả, không bắt buộc thêm bảng tasks. Child recovery tạo attempt mới vẫn giữ cùng child turn.

**Điều kiện đóng:** Fault tests tại hai cửa sổ trên, gồm follow-up có prompt giống lần trước, xác nhận mỗi delegation chỉ admit một child turn và parent nhận đúng confirmed output.

## 4. Các khoảng trống qua rà soát toàn bộ tài liệu

### GAP-01 · Chưa cụ thể hóa final-message lineage cho AgentResult

Tài liệu 17/18 yêu cầu lấy final text của đúng terminal attempt mới nhất; không dùng text của attempt cũ. Migration mẫu chỉ có `messages.session_id/sequence/role/content_json/tool_call_id`, chưa có liên kết trực tiếp tới `run_id/turn_id`. Không loại trừ việc mapping nằm trong content/metadata hoặc boundary khác, nhưng cách tra cứu chưa được đặc tả. [Nguồn: result projection 17](../17-agent-results-artifacts.md#176-agentresult-projection-and-read-flow), [result projection 18](../18-embeddable-library.md#187-agentresult-artifacts-và-storage-ownership), [schema mẫu](../examples/001_init.sql).

**Tác động:** Dễ lấy nhầm assistant text trong cùng session hoặc text cũ sau recovery nếu triển khai bằng truy vấn “assistant message cuối cùng”.

**Đóng khi:** Chọn và ghi rõ mapping message→attempt/turn, transaction boundary và test nhiều turns/recovery attempts, gồm trường hợp attempt mới chưa có final. Không mặc định bắt buộc thêm cột nếu có giải pháp khác đáp ứng invariant.

### GAP-02 · ToolContext có nhiều hình dạng minh họa

Mục 05 dùng owned strings với `agent_id`; mục 16 dùng borrowed strings và `call_id`; mục 18 mở rộng thành scoped capabilities cho HostTool. Đây là pseudocode chưa compile, nên khác biệt không tự là lỗi Rust. Tuy nhiên public seam cần một contract thống nhất về identity, lifetime, cancellation và quyền truy cập artifacts/checkpoints. [Nguồn: interface 05](../05-tools.md#53-interface-nội-bộ), [interface 16](../16-streaming-events.md#166-tool--subagent-lifecycle-progress-ownership), [capabilities 18](../18-embeddable-library.md#1862-toolcontext-là-scoped-capability-không-phải-sessionstore).

**Đóng khi:** Chốt public ToolContext và phân biệt phần internal context nếu cần; đồng bộ ví dụ, giữ private constructors/trusted scope và một owner ghi transcript. Scoped artifact read được theo dõi riêng tại SPEC-01.

### GAP-03 · Mapping YAML → AgentDefinition chưa rõ

YAML mẫu dùng agent map và các tên `prompt/provider/model/tools/skills`; AgentDefinition dùng `id/prompt_path/provider_id/model_id/allowed_tools/allowed_skills`. Nếu deserialize thẳng sẽ cần mapping; chưa có code nên chưa kết luận config không load được. Đề xuất 18 chuyển YAML/TOML loader thành tùy chọn của host, vì vậy điểm này cần giải quyết ở đúng layer thay vì bắt buộc core hỗ trợ YAML. [Nguồn: YAML](../examples/agents.yaml), [AgentDefinition](../02-agents.md#22-agent-definition-thiết-kế), [bootstrap/config 18](../18-embeddable-library.md#185-bootstrap-configuration-và-injection).

**Đóng khi:** Đặc tả DTO/conversion hoặc serde mapping trong loader/example được chọn; xác minh config mẫu resolve đúng IDs, mode, prompts, permissions và provider aliases.

### GAP-04 · Provider spike và operational limits chưa có chứng cứ

Crate `llm` chưa được khóa/kiểm chứng bằng implementation. Khả năng text/tool streaming, call/result IDs, cancellation, usage, context limits và error mapping giữa ít nhất hai providers vẫn là gate. Hard persistence size cap, event payload cap/throttle, artifact quotas và shutdown grace cần giá trị cụ thể phù hợp triển khai. Những số trong examples chưa phải kết quả đo. [Nguồn: provider spike](../08-runtime-llm.md#86-provider-spike-bắt-buộc), [giới hạn tham chiếu](../00-decisions.md#05-giới-hạn-và-cấu-hình), [open questions 18](../18-embeddable-library.md#1816-open-questions-cần-chốt-lúc-implementation-spike), [quotas 17](../17-agent-results-artifacts.md#1712-security-quotas-and-operational-constraints).

**Đóng khi:** Có pinned version, compile/provider smoke evidence và cấu hình giới hạn được validate. Đây là việc xác minh đã được thiết kế yêu cầu, không phải finding rằng crate/provider không đáp ứng.

### GAP-05 · Chưa chốt namespace của tool-call ID

ToolCall dùng stable provider ID, trong khi DB yêu cầu `UNIQUE(session_id, tool_call_id)` suốt hội thoại. Tài liệu chưa quy định adapter phải bảo đảm session-wide uniqueness hoặc ánh xạ provider ID sang internal identity. Nếu hai responses/attempts cùng session trả cùng ID, insert sau sẽ xung đột; cũng không thể tùy ý đổi ID rồi làm sai provider call/result pairing. Chưa kiểm chứng provider thực tế nào phát sinh trường hợp này, nên đây là gap cần fake/provider spike thay vì lỗi provider đã xác nhận. [Nguồn: ToolCall](../05-tools.md#53-interface-nội-bộ), [DB constraint](../09-persistence.md#93-constraints-và-ownership), [multi-provider mapping](../08-runtime-llm.md#84-multi-provider), [SQL](../examples/001_init.sql).

**Đóng khi:** Chốt internal/provider ID mapping và scope uniqueness; fake provider trả trùng ID ở hai logical responses/recovery attempts vẫn lưu/replay history đúng. Nếu contract chọn reject collision, phải mô tả rõ limitation và lỗi kỹ thuật, không nhận nhầm invocation cũ.

### GAP-06 · Bootstrap chưa xác định nguồn mandatory base prompt

README hướng dẫn copy prompts mẫu; bộ mẫu chỉ có Root/Explorer/Reviewer. Architecture minh họa `prompts/base.md`, còn PromptBuilder yêu cầu runtime base mandatory. Chưa rõ base được embed trong library hay phải có file/config do host cung cấp. Không có bằng chứng runtime bỏ qua base vì chưa có implementation. [Nguồn: bootstrap examples](../README.md#tài-liệu-mẫu), [source tree](../01-architecture.md#14-rust-source-tree-tối-thiểu), [mandatory composition](../03-prompts.md#32-các-nguồn-dữ-liệu), [host configuration](../18-embeddable-library.md#185-bootstrap-configuration-và-injection).

**Đóng khi:** Bổ sung base mẫu hoặc quy định embedded default và startup validation; test khởi tạo từ đúng bộ scaffold được công bố luôn có mandatory base trong context.

### GAP-07 · Thiếu trigger resync khi mất notification nhưng connection vẫn sống

Protocol hiện refresh khi nhận committed invalidation, Lagged hoặc reconnect. Nếu worker dừng/panic sau terminal DB commit nhưng trước publish, trong khi process và SSE heartbeat vẫn sống, event chưa publish sẽ không tạo Lagged. Client có thể giữ snapshot running dù canonical state đã terminal. Đây là failure window đã được tài liệu 18 yêu cầu fault test, nhưng trigger sửa trạng thái UI cho tình huống còn kết nối chưa rõ. [Nguồn: reconnect protocol](../16-streaming-events.md#168-host-sse-endpoints-disconnect--reconnect), [artifact crash matrix](../17-agent-results-artifacts.md#1710-crash-recovery-idempotency-and-compaction), [fault injection 18](../18-embeddable-library.md#1812-test-strategy-qua-các-seams).

**Đóng khi:** Chọn periodic revision/snapshot reconciliation hoặc supervisor phát force-resync cho cửa sổ này; test suppress terminal publish khi connection còn sống, UI cuối cùng đọc đúng terminal state. Không cần thêm event log/historical replay chỉ để xử lý trường hợp này.

### GAP-08 · “Complete turns” cần phân biệt với exchanges đã settled trong active turn

Turn được định nghĩa là một user/delegation input; một run/turn có thể có hàng trăm steps. Compaction lại nói summarize “older complete turns”, giữ “recent complete turns”. Nếu hiểu turn theo glossary thì một turn đầu tiên đang chạy chưa có complete turn nào để compact. Các chỗ nhắc complete tool pairs cho thấy ý định có thể là exchange boundary; đây là thuật ngữ cần làm rõ, không khẳng định thuật toán implementation đã lỗi. [Nguồn: identities 15.2](../15-long-running-sessions.md#152-session-turn-run-step), [compaction 15.6](../15-long-running-sessions.md#156-context-compaction), [prompt compaction](../03-prompts.md#37-automatic-context-compaction), [glossary](../14-glossary.md).

**Đóng khi:** Quy định được compact prefix gồm model/tool exchanges đã settled ngay trong active turn; không cắt tool-call/result pair, giữ current objective và recent exchanges. Test một turn duy nhất vượt context threshold nhiều lần trước khi final, không dựa vào history các user turns trước.

### DOC-01 · Nhãn phiên bản và thuật ngữ concurrency cần đồng bộ

Header migration vẫn ghi v3 dù đã có state_revision/streaming invariants; ADR-001 dùng chữ “session lock”, trong khi ADR-007/010 chốt unique partial index cho single-run admission. Đề xuất 18 còn thêm process owner lock với mục đích khác. [Nguồn: SQL header](../examples/001_init.sql), [ADR-001](../13-adrs.md#adr-001--shared-runtime--parentchild-sessions), [ADR-007](../13-adrs.md#adr-007--sqlite--sqlx), [single-owner invariant 18](../18-embeddable-library.md#189-managed-tokio-execution-và-lifecycle).

**Đóng khi:** Đồng bộ nhãn migration và giải thích rõ: partial unique index bảo vệ một running run/session; process lock bảo vệ một engine owner/store; không thêm session mutex như admission guard thứ hai.

### DOC-02 · Chỉ mục chưa dẫn tới các đề xuất mới

README docs hiện chỉ liệt kê 00–16 và root README mô tả 17 mục thiết kế. File 17/18 đã có trong repo nhưng chưa được dẫn từ chỉ mục chính; người đọc theo đường đọc được công bố sẽ bỏ sót hướng artifacts/library-first. Đặt trạng thái Proposed trong chỉ mục không đồng nghĩa chấp nhận đề xuất. [Nguồn: mục lục](../README.md#mục-lục), [root README](../../README.md), [17](../17-agent-results-artifacts.md), [18](../18-embeddable-library.md).

**Đóng khi:** Thêm nhóm hoặc cột trạng thái cho Proposed 17/18 trong chỉ mục, giữ rõ baseline Accepted và cập nhật số lượng tài liệu. Bản tổng hợp 00–17 hiện có công bố đúng phạm vi riêng; nếu muốn dùng làm onboarding toàn dự án thì bổ sung 18 sau khi quyết định scope.

## 5. Các xung đột scope có chủ đích, cần quyết định khi acceptance

### ALIGN-01 · Bốn bảng/post-MVP artifacts so với ArtifactStore

Baseline 05/09 đặt general artifact store sau MVP và giữ raw result trong DB dưới hard cap. Tài liệu 17 đề xuất chuyển đầu ra lớn sang private filesystem + bảng `artifacts`, thêm `checkpoint_artifact_id` trên invocation. Tài liệu 18 dựa vào đề xuất này ở E6. [Nguồn: output baseline](../05-tools.md#55-text-output-lớn), [baseline retention](../09-persistence.md#96-tool-outputs-artifacts-và-retention), [schema 17](../17-agent-results-artifacts.md#174-persistence-and-filesystem), [compatibility 17](../17-agent-results-artifacts.md#1715-compatibility-notes-and-open-questions), [increments 18](../18-embeddable-library.md#1813-implementation-milestones).

**Cách xử lý:** Chốt acceptance và phạm vi ArtifactStore; cập nhật decisions, persistence, tool output policy, migrations, tests và ADR liên quan trong cùng đợt. Hiện chưa được coi schema năm bảng là baseline Accepted hoặc bảng `tasks` được bổ sung.

### ALIGN-02 · Entry point Axum so với library-first

Baseline bắt đầu từ Axum/API gọi AgentRuntime, có `main.rs/api.rs` và SSE adapter trong kiến trúc server. Tài liệu 18 đặt Orchestrator làm public library handle, host sở hữu AuthN/transport và Tokio runtime; engine giữ authorization, supervisor và stores. [Nguồn: baseline architecture](../01-architecture.md), [EMB decisions](../18-embeddable-library.md#182-quyết-định-kiến-trúc-accepted-design), [changes required](../18-embeddable-library.md#1815-đồng-bộ-khi-accept-v50).

**Cách xử lý:** Khi accept, đồng bộ entry point/public types/source tree và đưa Axum thành optional host example. Giữ nguyên shared runtime, isolated histories và SSE snapshot/invalidation semantics. Cập nhật ADR theo STD-01.

### ALIGN-03 · AgentResult và roadmap chưa có baseline hợp nhất

Tài liệu 17 minh họa `AgentResult.artifacts: Vec<ArtifactRef>` nhưng đã yêu cầu pagination khi lớn. Tài liệu 18 cụ thể hóa thành bounded `artifact_preview` + cursor, thêm public history/list/download; đây là refinement cần đồng bộ chứ chưa phải mâu thuẫn hành vi bắt buộc. Roadmap M0–M7, Artifact Phase A/B và embedding E0–E6 cũng chưa được ánh xạ thành một kế hoạch triển khai chung. [Nguồn: projection 17](../17-agent-results-artifacts.md#176-agentresult-projection-and-read-flow), [projection 18](../18-embeddable-library.md#187-agentresult-artifacts-và-storage-ownership), [roadmap baseline](../12-mvp-test-plan.md#121-milestones-canonical-tracer-bullets), [artifact phases](../17-agent-results-artifacts.md#1714-incremental-implementation-notes), [embedding increments](../18-embeddable-library.md#1813-implementation-milestones).

**Cách xử lý:** Chọn public result envelope và ánh xạ các increments/gates khi acceptance; không coi các bảng kế hoạch hoặc pseudocode là implementation đã hoàn tất.

## 6. Phát hiện cũ đã được đề xuất 18 bổ sung

Khoảng trống **reconciliation/unblock API** trong baseline đã được tài liệu 18 bổ sung ở mức thiết kế bằng `resolve_interrupted_invocation`, operator scope, `VerifiedToolResolution`, transaction/audit trước resume và quy tắc không auto-replay. Vì vậy không tiếp tục liệt kê “hoàn toàn chưa có API” như finding còn mở trên toàn bộ docs. Tuy nhiên đề xuất chưa Accepted/chưa triển khai; audit persistence và workflow cụ thể vẫn phải được kiểm chứng khi implementation. [Nguồn: public contract](../18-embeddable-library.md#1842-command-và-read-interface), [side-effect resolution](../18-embeddable-library.md#1863-side-effects-và-idempotency), [test strategy](../18-embeddable-library.md#1812-test-strategy-qua-các-seams).

Tài liệu 18 cũng đã mô tả supervisor reconciliation trong cùng process cho admission commit thành công nhưng miss scheduling, cùng provider fingerprint bỏ qua credential rotation thuần túy. Báo cáo này không ghi hai điểm đó thành lỗ hổng thiếu thiết kế; chúng vẫn cần tests theo kế hoạch. [Nguồn: admission failure ordering](../18-embeddable-library.md#185-bootstrap-configuration-và-injection), [provider identity](../18-embeddable-library.md#1856-durability-provider-fingerprint-và-recovery-compatibility).

## 7. Thứ tự xử lý đề nghị và giới hạn chứng cứ

1. Chốt các contract có thể chặn workflow: scoped artifact reader, shutdown/owner handoff, parent-invocation → child-turn và session-scoped skill catalog; thêm tests tương ứng (SPEC-01–03, STD-02).
2. Bổ sung ADR acceptance checklist (STD-01), rồi quyết định acceptance/scope chung của 17/18 và xử lý ALIGN-01–03.
3. Đặc tả message lineage, ToolContext, config mapping, tool-call namespace, base prompt, resync và active-turn compaction ở đúng seam (GAP-01–03, GAP-05–08).
4. Chạy provider/compile spike, chốt operational limits và đồng bộ wording/chỉ mục (GAP-04, DOC-01/02).

**Kết quả review toàn bộ docs:** Standards có **2 P2**; Spec có **3 P2**, trong đó shutdown là điểm wording/contract cần làm rõ với độ chắc thấp hơn scoped artifact reader. Ngoài ra có **8 gaps**, **2 điểm tài liệu** và **3 nhóm alignment**. Các nhóm sau không được cộng thành findings P2 hoặc lỗi bảo mật đã xác nhận.

**Chưa có chứng cứ runtime:** Không chạy cargo tests, provider smoke, crash fault injection, concurrency/authorization tests hoặc deployment acceptance trong các lượt đọc/review này. AT/ST/EMB-AT và artifact verification plan là yêu cầu kiểm chứng, không phải danh sách đã pass. Nghiên cứu dựa trên nguồn sơ cấp trong repo; các khẳng định upstream OpenCode/crate/provider hiện tại chưa được xác minh độc lập.

**Kiểm tra tài liệu:** Rà soát đường dẫn Markdown nội bộ và heading anchors trong toàn bộ `docs/`; không phát hiện tham chiếu nội bộ không tồn tại sau khi hoàn thiện báo cáo. Kiểm tra này không xác minh URL bên ngoài, Rust pseudocode, migration runtime hoặc độ đúng của provider API.

## 8. Resolution sau Codebase Design × Ponytail Review — baseline v5.0

**Resolved in design documents (not runtime verified):** STD-01 (ADR-015–017), STD-02 (session-scoped skill catalog), SPEC-01 (scoped HostTool artifact open), SPEC-02 (quiesce/join trước owner lock release), SPEC-03 (atomic idempotent invocation→child turn). GAP-01/02/03/05/06/07/08 và DOC-01/02 đã được đặc tả/đồng bộ trong docs 00–18 và examples; **GAP-04 còn pending compile/provider/operational spike**.

ALIGN-01/02/03 đã accept ở mức thiết kế: five-table ArtifactStore MVP, library-first host transport, one AgentResult definition và canonical roadmap/test catalog tại [12](../12-mvp-test-plan.md). Ponytail loại duplicate AgentResult/test tables, redundant artifact ready-state, one-field AgentInput wrapper; không thêm tasks, CheckpointManager, storage/provider interface traits. Tất cả test cases AT-38–50 vẫn **chưa chạy**, cần code và fault-injection evidence.
