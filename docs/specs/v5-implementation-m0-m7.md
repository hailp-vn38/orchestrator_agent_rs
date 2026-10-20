# Spec triển khai baseline v5.0: gate GAP-04 → M0–M7

## Problem Statement

Host Rust cần một library điều phối Root/Subagent có thể nhúng vào Tokio hiện có, chạy tác vụ dài, phục hồi an toàn và truy vấn kết quả/artifacts sau disconnect hoặc restart. Baseline v5.0 đã Accepted về thiết kế nhưng repository hiện chỉ có tài liệu, chưa có Rust implementation, Cargo build, provider smoke hay fault-test evidence. Nếu triển khai thẳng toàn bộ roadmap dựa trên các Rust snippets minh họa, khác biệt API/capability của crate `llm` và provider có thể làm vô hiệu các giả định streaming, tool pairing và lifecycle.

Cần một spec triển khai theo đúng M0–M7 canonical, có gate đầu tiên cho GAP-04, đầu ra review được tại từng mốc và ranh giới rõ giữa hành vi được chứng minh bằng fake LLM với hành vi cần provider thật. `completed` chỉ là execution status, không chứng nhận hoàn tất nghiệp vụ.

## Solution

Thực hiện một compile/provider/operational spike bắt buộc trước M0, rồi triển khai tám vertical slices M0–M7 của baseline v5.0. Host tích hợp qua public `Orchestrator`, đăng ký `HostTool`, truyền typed LLM config và caller scope; library sở hữu managed execution, durable history, recovery và local immutable artifacts. Host tiếp tục sở hữu Tokio, AuthN và transport.

Acceptance có hai lớp độc lập: deterministic integration bằng scripted fake `LlmGateway` + SQLite/filesystem thật để chứng minh invariants; provider acceptance bằng pinned crate với OpenAI và ít nhất một provider khác để chứng minh protocol/capabilities thực tế. Mọi gate ghi rõ `PASS`, `FAIL`, `NOT_RUN` hoặc `BLOCKED`, cùng evidence. Không được đóng GAP-04 hay tuyên bố MVP hoàn tất nếu acceptance provider bắt buộc chưa chạy.

## User Stories

1. As a người tích hợp host Rust, I want nhúng một Cargo library không bắt buộc Axum, so that tôi dùng server và Tokio runtime hiện có.
2. As a người triển khai, I want compile spike với pinned crate trước M0, so that tôi không xây runtime trên API minh họa chưa được kiểm chứng.
3. As a người vận hành, I want smoke trên OpenAI và provider thứ hai, so that tôi biết model nào thật sự hỗ trợ text, tools và streaming.
4. As a người vận hành, I want giới hạn tài nguyên có giá trị cụ thể đã validate, so that ví dụ cấu hình không bị hiểu thành defaults đã đo.
5. As a người tích hợp, I want typed provider config và credentials do host resolve, so that host không phải import provider-specific types.
6. As a người quản trị, I want credentials được che và không persist, so that history, checkpoints, artifacts và traces không lộ secrets.
7. As a người dùng, I want tạo root session đúng quyền và submit trả accepted sau durable commit, so that yêu cầu không mất khi request kết thúc.
8. As a người vận hành, I want supervisor phát hiện claim đã commit nhưng chưa được schedule, so that turn không mắc kẹt trong cùng process.
9. As a người dùng, I want đọc result và history phân trang đúng scope, so that tôi theo dõi được tác vụ dài mà không tải toàn bộ transcript.
10. As a người tích hợp, I want thêm business capabilities qua HostTool, so that tôi không sửa engine cho từng nghiệp vụ.
11. As a người quản trị, I want permissions/schema được enforce trước tool execution, so that model không vượt quyền bằng prompt hoặc arguments.
12. As a người dùng, I want Skill catalog chỉ hiện skill được phép và load theo nhu cầu, so that context gọn và không lộ instructions ngoài scope.
13. As a người dùng, I want Root gọi foreground Subagent có history riêng, so that công việc chuyên môn không làm trộn hội thoại.
14. As a Root agent, I want nhận final text và session ID của child, so that tôi tự đánh giá và follow-up mà không bị ép business JSON.
15. As a người dùng, I want follow-up child cũ bằng session ID, so that child tiếp tục dựa trên history trước đó.
16. As a người vận hành, I want retry admission của cùng parent invocation giữ đúng child turn, so that crash không nhân đôi objective.
17. As a người triển khai, I want provider call ID được giữ nguyên nhưng internal invocation ID độc lập, so that ID lặp qua responses không ghép nhầm results.
18. As a người dùng, I want Root và Child chọn provider/model riêng, so that mỗi agent dùng cấu hình được host cho phép.
19. As a người dùng, I want chạy hơn 100 steps và child lâu hơn 120 giây, so that runtime không tự cắt tác vụ dài bằng giới hạn mặc định.
20. As a người vận hành, I want steps cap tùy chọn kết thúc bằng text-only progress, so that giới hạn không tạo thêm tools hoặc run ngoài ý muốn.
21. As a người dùng, I want compaction nhiều lần ngay trong active turn, so that tác vụ tiếp tục mà vẫn giữ objective và đủ tool pairs.
22. As a người dùng, I want xem live text/progress nhưng chỉ đọc final đã commit như kết quả bền vững, so that stream lỗi không trở thành câu trả lời giả.
23. As a người quản trị, I want tools chỉ chạy sau Finished và durable call admission, so that partial response hoặc lỗi giữa stream không tạo side effects.
24. As a người dùng, I want explicit cancel truyền xuống child đang chạy, so that tôi dừng được execution mà không nhầm disconnect với hủy.
25. As a người vận hành, I want safe restart giữ turn ID và giới hạn recovery attempts, so that tác vụ phục hồi mà không crash-loop vô hạn.
26. As a người vận hành, I want unknown mutation bị block thay vì replay, so that restart không nhân đôi side effects.
27. As a operator, I want resolve interrupted invocation bằng evidence đã xác minh và audit, so that runtime chỉ tiếp tục sau một quyết định bền vững.
28. As a người vận hành, I want owner guard chỉ được nhả sau khi local writers quiesce/join, so that hai instances không cùng ghi execution store.
29. As a người vận hành, I want recovery phát hiện đổi routing fingerprint nhưng chấp nhận rotation secret thuần túy, so that không resume nhầm backend.
30. As a người dùng, I want family snapshot và subscription gồm Root/Child đã authorize, so that tôi theo dõi được delegation qua một giao diện.
31. As a người dùng, I want reconnect, lag hoặc mất terminal notification vẫn hội tụ canonical state, so that UI không mắc kẹt ở running.
32. As a người dùng, I want artifacts được lưu immutable và tải theo stream có giới hạn, so that kết quả lớn không bị nhồi vào model context hoặc RAM.
33. As a HostTool adapter, I want đọc artifact đầu vào bằng trusted scoped context, so that tôi dùng output trước mà không truy cập private filesystem.
34. As a người dùng, I want resume workload từ last committed manifest pointer, so that file mới nhưng chưa CAS không bị tính là completed work.
35. As a người dùng, I want result của latest attempt không lấy final cũ, so that retry đang chạy không hiển thị nhầm thành công trước đó.
36. As a người vận hành, I want backup/restore nhất quán cả SQLite và bytes, so that khôi phục không trả refs thiếu hoặc corrupt.
37. As a người dùng, I want playlist báo đúng 18/20, Knowledge pending và media job accepted, so that execution completed không che mất trạng thái nghiệp vụ thực tế.
38. As a reviewer, I want evidence fake và live tách riêng theo mốc, so that test deterministic không bị coi là chứng cứ tương thích provider.

## Implementation Decisions

### Hợp đồng và ownership

- Giữ baseline v5.0, D-01–32 và ADR-001–017; ADR-015 supersede public entry point/Axum coupling của ADR-009/014, ADR-016 mở rộng persistence/lineage và ADR-017 ràng buộc handoff/reconciliation. Không mở lại các quyết định đã Accepted chỉ vì snippets chưa compile.
- Một Cargo package library-first; public seams là `Orchestrator` control/observation và `HostTool` extension. `LlmGateway` là seam nội bộ phục vụ provider/fake. AgentRuntime là runner/transcript writer duy nhất, PromptBuilder sở hữu context construction; không thêm orchestration facade trùng lặp.
- Public contract gồm builder, root session creation, submit, result, paginated history/list_artifacts, open_artifact, snapshot, subscribe, cancel, operator resolution và shutdown. Không expose SQLx pool, provider crate types hoặc private artifact paths. Host không được tùy ý tạo child ancestry.
- Năm bảng SQLite canonical: sessions, messages, agent_runs, tool_invocations, artifacts; WAL/FK/busy timeout và transactions ngắn. Không thêm tasks, event-log, result-state hay checkpoint table. Có thể dựng schema đích sớm để tránh churn; artifact API chỉ được acceptance đầy đủ ở M7.
- `AgentDefinition` tham chiếu `(provider_id, model_id)`; builder validate local config và tạo clients, không bắt buộc gọi remote. Runtime capabilities được verify trong explicit smoke/request. Không silent provider fallback. Persist non-secret routing fingerprint theo attempt; rotation secret không đổi fingerprint.
- Stable sanitized error codes theo public contract; tracing dùng session/run/invocation/provider call IDs và child linkage khi liên quan. Credentials/hidden reasoning không vào DB, model context, event hay trace.

### Gate G0 — GAP-04, bắt buộc trước M0

G0 là điều kiện tiền đề của roadmap, không đổi tên hay dịch chuyển M0–M7. Chỉ xây spike tối thiểu để kiểm chứng, chưa xây runtime hoàn chỉnh.

1. Pin phiên bản `llm`, Rust toolchain và dependency/features cần thiết bằng lockfile. Compile chương trình host Tokio nhỏ với gateway normalization và typed config; kiểm chứng concrete stream/async reader types và lựa chọn thư viện owner lock. Không copy snippets thiết kế rồi coi là compile evidence.
2. Chạy AT-48 trên **OpenAI + ít nhất một provider khác**, nêu chính xác backend/model/endpoint identity không chứa secret. Verify text completion + multi-turn, tool call → result → continuation, provider IDs, system instructions, text deltas, fragmented arguments nếu backend hỗ trợ, full-response Finished semantics, usage có/không có, request timeout/cancel và sanitized errors.
3. Lập capability matrix từng cặp backend/model: supported, fallback non-streaming tools có kiểm chứng, hoặc unsupported với error cụ thể. Text streaming vẫn là yêu cầu MVP; fallback tool assembly chỉ được chấp nhận khi giữ nguyên Finished/persistence/dispatch ordering. Không thay provider âm thầm. Chọn cặp providers đáp ứng baseline; nếu không đáp ứng, giữ gate fail/blocked và ghi thiết kế cần điều chỉnh/ADR thay vì tiếp tục trên giả định.
4. Kiểm chứng error mid-stream ở gateway bằng fault harness có nhãn rõ; phần lỗi không tái hiện được từ dịch vụ thật phải ghi hạn chế, không gán bằng chứng injection thành live-service evidence. Kiểm tra adapter normalization và runtime ordering tiếp tục ở M2/M4.
5. Chốt deployment profile với giá trị và đơn vị cụ thể cho hard persistence result cap, model-visible caps, event payload cap/progress throttle/broadcast capacity, page size, bounded downloads, artifact max bytes/owner-workspace quotas/disk reserve, request/tool timeouts, recovery attempts, supervisor sweep, active snapshot refresh, shutdown grace và cost/usage policy. Ghi measured hay estimated và hành vi khi vượt cap. Validate các biên hợp lệ/không hợp lệ; đo compile/stream, SQLite migration/contention tối thiểu và storage limits trong phạm vi spike. Các mốc sau phải chạy workload/fault validation trên profile này; không dùng con số ví dụ như kết quả đo.
6. Evidence gồm revision, toolchain/dependency versions, commands/exit status, timestamp, provider/model capability matrix, redacted logs, limits rationale và kết luận `GO`/`NO-GO`. Thiếu credentials/network/model access là `BLOCKED`/`NOT_RUN`, không phải PASS. Chỉ `GO` sau compile, cả hai provider smokes và operational profile validation đạt; GAP-04 không đóng bằng sửa docs đơn thuần.

### M0 — Library shell

- Sau G0 GO, tạo library shell với typed public config/errors, Orchestrator builder, HostTool registration contract, embedded mandatory base instructions và registry validation. Host-side YAML DTO được map sang AgentDefinition, không deserialize trực tiếp tùy tiện vào core.
- Smoke host Tokio build/instantiate library không Axum, không nested Tokio runtime và không remote request khi build. Config sai, agent/provider reference sai, tool trùng/schema sai bị reject có code; secrets được redacted.
- Gate: compile public host example + AT-46 (wiring/config/base contract). Mọi model request thực sự chứa mandatory base được kiểm tra tiếp ở M1/M2. Không hứa API chưa triển khai là đã hoạt động.

### M1 — Single Agent

- SessionStore migration, scoped root sessions, single-active-run partial index; submit atomically persist input + run claim rồi trả AcceptedTurn. Supervisor giữ owned handles, reconcile claim sau commit/trước spawn bằng triggered/periodic sweep, không duplicate task hoặc để running vô hạn.
- Single-agent text execution qua internal gateway; result/history phân trang và latest-attempt/final lineage cơ bản. Final message, final_message_id và terminal state commit cùng transaction; không lấy assistant text cuối toàn session.
- Thiết lập owner guard tối thiểu từ lúc build mở execution store; không trì hoãn an toàn store đến M5. M5 hoàn thiện fault/lifecycle acceptance. Typed config tạo provider clients; fake injection là test support nội bộ.
- Gate fake/SQLite: AT-01/18/42/46/50 cơ bản; thêm crash-window scheduling trong live process, concurrent submit chỉ một claim, history cursor bị bind scope, unauthorized reads denied và close/reopen đọc được dữ liệu. Gate provider: host text/multi-turn smoke trên cặp G0, chứng minh integration qua public Orchestrator.

### M2 — Tools & Skills

- ToolExecutor validate schema và effective permissions trước dispatch, trả outcomes cho AgentRuntime ghi transcript. Model output/repository/skill/tool text là untrusted context. ToolContext do runtime mint, gồm identity, cancellation, progress và scoped capabilities; không cho host tool direct SQL/private files.
- PromptBuilder compose mandatory base + agent + project + permitted skill catalog + active history. Catalog/load cùng session permissions; traversal/symlink escape bị deny. Tool/skill history thuộc session thực thi.
- Chỉ dispatch sau toàn response Finished, tất cả args valid và mọi assistant calls/invocations đã commit; execute tuần tự, settle đủ results trước next LLM step. Provider IDs giữ nguyên trong pairing; internal invocation ID độc lập, namespace theo assistant message.
- Gate fake: AT-07–09/15/28/33/35–36/39/43 và mandatory-base assertion mọi request. Kiểm tra denied tool không chạy/không ghi, lỗi input thành technical result đúng pairing, output cap rõ và không double-write transcript. Giai đoạn này dùng Root để isolate Tool/Skill behavior; bản Child hoàn chỉnh chạy ở M3.
- Gate provider: tool round-trip và skill continuation qua real gateway trên cả hai providers, cùng recorded capability/fallback của G0. Fake reused-ID/error scripts vẫn bắt buộc, không phụ thuộc provider tình cờ sinh lỗi.

### M3 — Parent–Child

- Foreground subagent envelope giữ agent/description/prompt/session_id tùy chọn; output success gồm session_id, completed và text. Child history riêng; Root tự đánh giá plain text, không semantic validator bắt buộc. Depth=1, tối đa một active child, nhiều child calls tuần tự được phép.
- Atomically admit child input/run claim và link child_session_id/child_turn_id bằng **parent internal invocation ID**. Cùng invocation trả lại cùng child turn; invocation mới với prompt giống hệt vẫn tạo logical turn mới trong child cũ. Parent đợi latest confirmed attempt của đúng child turn.
- Ownership/workspace/ancestry bắt buộc, không resume child của parent khác. Partial unique index chặn concurrent runs cùng child. Root/Child routing theo AgentDefinition riêng.
- Gate fake/SQLite: AT-02–10/13–14/16/38; crash ngay trước/sau atomic child admission, child terminal trước parent result, repeated identical follow-ups và missing child final placeholder. AT-13 fake chỉ chứng minh routing selection.
- Gate provider: public host E2E Root A → child B → Skill/HostTool → child final → Root follow-up cùng child ID → Root final; đọc đúng histories/linkage sau reopen. Đây là chứng cứ live multi-provider routing, tách khỏi AT-13 fake.

### M4 — Continuous

- Steps mặc định unset; không overall child deadline. Chỉ áp request/tool timeout, explicit cancellation và configured budgets. Steps cap tùy chọn ngừng tools và trả text-only progress/remaining work, không tự tạo run.
- Auto compaction lấy settled exchange prefix trong active turn, persist summary/frontier, giữ objective và complete call/result pairs; original history còn nguyên. Overflow compact/retry tối đa một lần tại logical step. Summary không thay manifest checkpoint.
- Hoàn thiện streaming text/call assembly, finish/usage/error mapping và discard unconfirmed calls/previews. ToolCallReady không kích hoạt execution. Không persist mỗi delta.
- Gate fake: AT-11/19–22/26–27/35–36/44; >100 steps, repeated compaction trong một active turn, steps=5, thiếu usage và ToolCallReady rồi lỗi. Dùng clock điều khiển để chứng minh >120 giây không phải whole-child timeout, không phải chờ hơn hai phút trong CI.
- Gate provider: cả hai providers text streaming, tool round-trip/continuation và compaction summary dùng model thật; đo usage/timeout/cancellation trên cặp đã pin. Fragment count cụ thể là deterministic fixture; live check assembled content/order và provider capability, không đòi provider luôn phát đúng 20 chunks. Không coi fake >100 steps là bằng chứng provider chạy bền nhiều giờ.

### M5 — Recovery & Lifecycle

- Startup recovery dưới exclusive owner guard: old attempt interrupted, safe new attempt giữ turn_id, resume_attempt bounded; unresolved mutating outcome block trước next model request. Không dangling call/result history và không blind replay.
- Non-secret routing identity thay đổi chặn recovery bằng PROVIDER_CONFIG_CHANGED; credential rotation thuần túy không chặn. Đọc snapshot/result vẫn phản ánh trạng thái thực tế.
- Operator-only resolve_interrupted_invocation dùng internal invocation ID và VerifiedToolResolution: confirmed completed cần outcome/refs hợp lệ; confirmed no effect thành technical failure, không auto replay. Scope/capability và evidence được validate, audit + transcript result + terminal update commit trước supervisor resume.
- Shutdown stop admission → bounded drain → cooperative cancel/interrupt → quiesce/join local write-capable tasks → close/release lock. Non-cooperative tool còn khả năng ghi thì trả incomplete/error và tiếp tục giữ owner guard. Drop không hứa graceful shutdown; external mutation uncertain vẫn cần reconcile dù local task đã dừng.
- Gate fake/SQLite/process faults: AT-12/17/23–25/34/41–42/49–50, bounded attempts, parent cancel tới active child, submit sau shutdown bị reject trước commit, fingerprint/rotation cases. Dùng process harness kiểm tra crash/lock thật, không chỉ object mock.
- Gate provider: cancel/timeout qua adapter thật và safe restart khi không có unknown mutation; operator/fault contracts vẫn được test xác định. Live mutation reconciliation chỉ được công nhận nếu dùng đúng tool adapter/provider receipts thực tế; fake side effects không chứng minh upstream exactly-once.

### M6 — Observation

- Public subscribe/snapshot transport-neutral; ExecutionFeed dùng bounded Tokio broadcast. Family snapshot read transaction nhất quán, per-session state_revision tăng theo durable transactions. Generic tool events bao phủ subagent/skill; không thêm event store.
- Committed events chỉ publish sau commit và chỉ invalidate canonical view, không dùng payload làm patches. Subscribe trước snapshot; lag/reconnect bỏ preview chưa confirmed và refresh. Host adapter/client phải periodic snapshot refresh khi family active kể cả stream/heartbeat vẫn sống, xử lý terminal commit bị miss publication.
- Authorization owner/workspace/ancestry tại subscribe/snapshot, child mới xuất hiện vẫn lọc đúng; không rò secrets/private reasoning. Slow subscriber không block runtime hay reader khác.
- Gate fake/store faults: AT-25–34/37/45 và ST-01–16; revision 16 publish trước 15 không regress, child creation/snapshot race, mất terminal publish nhưng connection còn sống vẫn converge trong configured refresh bound.
- Axum adapter tùy chọn làm smoke HTTP 202, SSE heartbeat/reconnect/auth/disconnect; core không phụ thuộc adapter. Khả năng observation/family snapshot/resync là MVP bắt buộc. Nếu không chọn Axum, host harness transport-neutral vẫn phải chứng minh cùng lifecycle/convergence; không gọi đó là HTTP/SSE wire evidence.
- Gate provider: ít nhất một family live Root/Child cho thấy deltas/progress rồi canonical terminal/result; provider text/tool parity của cả hai vẫn thuộc AT-48/M4, không thay bằng SSE smoke.

### M7 — Artifacts

- Local ArtifactStore immutable bytes + metadata; create-new staging, streaming hash/size, file sync → atomic same-filesystem rename → directory sync theo platform → metadata publication. Staging/unpublished bytes không đọc được; orphan cleanup chỉ cho staging/unpublished bytes sau grace, không phải TTL/GC committed artifacts.
- ToolContext.artifacts().put/open kiểm tra explicit invocation scope/allowed ancestry, owner/workspace, quotas, integrity và bounded backpressure-aware streams. Cùng workspace không tự cấp quyền; paths opaque do engine tạo. Public open_artifact/history/list_artifacts kiểm tra scope mỗi lần/mỗi trang, cursor không cấp quyền.
- SessionStore commit checkpoint CAS trên invocation hiện hữu sau manifest ref đã verified. Chỉ last committed pointer authoritative; stale/wrong-owner updates reject. Invocation follow-up mới phải explicit carry verified manifest ID và CAS initial pointer mới. Artifact publication không đồng nghĩa tool success.
- AgentResult là projection latest attempt + exact terminal final_message lineage + bounded artifact_preview/cursor. Attempt đang chạy output=None dù attempt cũ có final. External Knowledge/job IDs không phải local artifacts.
- Gate fake/real filesystem: AT-40/42/47 cùng artifact/read/pagination quotas và integrity, fault quanh staging/rename/metadata/tool result/CAS; missing/corrupt refs không trả success. Restore consistent SQLite backup và private files, kể cả WAL đang dùng; verify result/history/artifact refs/bytes sau restore.
- Ba workload fixtures bắt buộc: playlist 20 items/18 success/2 ASR failures, resume child skip đúng verified completed refs; document conversion với Knowledge pending/failure không giả receipt; media accepted rồi restart dùng durable job/idempotency identity, không resubmit mù quáng. Kết hợp >100 fake steps, compaction/disconnect/restart để kiểm chứng canonical reconstruction.
- Core MVP có thể chứng minh các workflow contracts bằng fake LLM + scripted HostTool providers. Chỉ claim ASR/Knowledge/media provider integration thực tế khi chạy adapter thật và có job/receipt/download evidence; triển khai dịch vụ nghiệp vụ và credentials của chúng không tự sinh từ spec core này. Provider LLM acceptance bắt buộc vẫn phải đạt riêng.

## Testing Decisions

### Seams đã thống nhất

- Acceptance ưu tiên public Orchestrator từ góc nhìn Rust host: submit, đọc result/history/snapshot, subscribe, cancel, shutdown, mở artifacts; HostTool thử nghiệm tạo outcomes/progress/side effects kiểm soát được. Người dùng đã xác nhận các seam này.
- Fake scripted LlmGateway là seam nội bộ để đặt response/error/order/usage/overflow chính xác. Không buộc production host xây gateway hoặc expose các stores chỉ để tests dễ gọi.
- Chỉ thêm gateway/store contract tests và private fault hooks khi public API không kích thích được crash window, CAS race, commit failure hoặc missed publication. Fault hooks chỉ điều khiển fault; assertions vẫn đánh giá external behavior/durable state, không kiểm tra private helper gọi bao nhiêu lần.
- Dùng SQLite và filesystem temp thật; dùng process harness khi kiểm tra OS owner lock và abrupt crash, controlled clock/barriers cho latency/races. Không mock transaction durability bằng in-memory map.
- Repo chưa có source/tests để tái sử dụng. Prior art là canonical AT-01–50, ST-01–16, public seam strategy và artifact verification scenarios trong baseline, không phải suite đang pass.

### Ma trận bằng chứng

| Lớp | Môi trường | Chứng minh được | Không dùng để kết luận |
|---|---|---|---|
| Compile/config | Pinned toolchain/crates, minimal Rust host | API/types/build, static validation, không cần Axum | Credentials/capability/provider protocol chạy được |
| Deterministic acceptance | Fake LLM + fake HostTool adapters + SQLite/files thật | Loop, permissions, pairing, admission, recovery, ordering, compaction, artifacts, observation | Crate adapter/protocol, provider semantics hay chất lượng model thật |
| Provider acceptance | Pinned crate + OpenAI + provider khác, model identities cụ thể | Real text/tool protocol, capabilities, normalized streams, usage/errors, routing, public-host live flows | Exactly-once external mutations, mọi model/backend hoặc độ bền nhiều giờ nếu chưa đo |
| Business adapter acceptance | HostTool adapter thật + nguồn/job/receipt/download thật | ASR/Knowledge/media integration và reconciliation của adapter đó | Chứng nhận chung cho mọi adapter hoặc thay thế provider LLM gate |

### Quy tắc hoàn thành và báo cáo

- Mỗi mốc có evidence chỉ rõ revision, scenario/AT/ST IDs, fake/live/fault mode, config profile, command/exit status, expected/actual, sanitized logs và unresolved limits. Checklist commit/issue cập nhật sau khi kiểm chứng, không dùng trạng thái Accepted design làm PASS.
- Tất cả AT-01–50 và ST-01–16 phải được map vào suite trước kết thúc MVP; các AT có cả fake/live giữ cả hai evidence, không đổi định nghĩa scenario để né provider. Những kiểm tra không có AT ID riêng như missed scheduling, cursors, fingerprint, cancellation và backup vẫn bắt buộc theo baseline.
- G0 GO là blocker đầu tiên. Từng M0–M7 được triển khai tuần tự; có thể scaffold prerequisite sớm nhưng không đánh dấu một mốc hoàn tất trước gate của nó. Provider gate của mốc chưa chạy thì báo deterministic acceptance đã đạt và live còn BLOCKED/NOT_RUN, không gọi mốc/MVP hoàn tất.
- CI không cần secrets cho compile/config và deterministic/fault suites. Chạy cargo fmt, clippy, tests và SQLite integrations; test live phải gọi riêng rõ ràng, không biến skipped/ignored thành passed. Provider suite phải được chạy lại khi đổi pinned dependency, gateway normalization, capability/routing config hoặc behavior liên quan; final release phải ghi evidence tương ứng final revision.
- Final MVP cần clean-install migrations, toàn bộ mandatory deterministic gates, OpenAI + provider thứ hai AT-48, public-host live Root/Child/Skill/follow-up flow, observation convergence, artifact fault/backup acceptance, no hardcoded secrets và stable errors/tracing. Không yêu cầu model live trả đúng văn phong hoặc tự chọn đúng kế hoạch để thay thế deterministic assertions.

## Out of Scope

- Triển khai Rust trong lần tạo spec này; chạy spike/provider tests chưa có implementation cũng không nằm trong thao tác viết spec.
- Background child/inbox, parallel child jobs, depth >1, scheduler/business tasks table, distributed workers/leases/fencing, network-filesystem multi-owner support.
- Semantic validation/auto-repair bắt buộc, business JSON contract cho child, mandatory reviewer, model substitution/fallback policy.
- Mandatory HTTP server hoặc Axum dependency trong core; UI production, WebSocket, lossless event replay, event log, persist từng token.
- Dynamic plugins/MCP runtime, remote artifact stores, committed-artifact TTL/deletion/GC, skill/provider config hot reload, provider-native compaction.
- Xây các dịch vụ ASR/Knowledge/media/conversion hoàn chỉnh hay cam kết chất lượng model; workload fixtures kiểm chứng contract core, real adapters cần evidence riêng.
- Exactly-once side effects hoặc bảo đảm external operation bị rollback khi cancel/shutdown.

## Further Notes

- Baseline nguồn: commit `241b19cb75cd822522261e3da108b01397ada793`, v5.0 Accepted design, ngày 2026-10-20. Đây là spec triển khai, không phải báo cáo đã compile/test/pass.
- Roadmap/test IDs canonical: [M0–M7, AT-01–50](../12-mvp-test-plan.md); [ST-01–16 và streaming contract](../16-streaming-events.md). Spec này cụ thể hóa gates/evidence, không tạo roadmap E0–E6 hoặc Artifact Phase A/B độc lập.
- Hợp đồng tham chiếu: [Glossary](../14-glossary.md), [ADRs](../13-adrs.md), [Library/HostTool](../18-embeddable-library.md), [Runtime/provider spike](../08-runtime-llm.md), [Artifacts](../17-agent-results-artifacts.md), [Persistence](../09-persistence.md).
- [Design review](../research/design-review-report.md) là historical snapshot trước v5.0; phần Resolution xác nhận các finding khác đã được giải quyết ở thiết kế, chỉ GAP-04 còn pending compile/provider/operational evidence. Không khôi phục roadmap/schema cũ từ historical report hoặc bản project-understanding v4.1.
- Spec được publish dưới một GitHub issue gắn `ready-for-agent`. Nhãn thể hiện spec đủ rõ để bắt đầu G0; không có nghĩa G0 đã pass hay M0 được bỏ qua blocker. Nếu chia ticket con sau này, giữ thứ tự G0 → M0 → … → M7 và tham chiếu cùng canonical AT/ST.
