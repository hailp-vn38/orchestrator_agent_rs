<p align="right">🌐 <a href="README.md">🇬🇧 English</a> · <b>🇻🇳 Tiếng Việt</b></p>

# Orchestrator Agent RS

Thư viện Rust **nhúng vào server hiện có** để điều phối nhiều agent (Root ↔ Subagent) với LLM,
tool và skill do host cung cấp. Core không phụ thuộc web framework: host sở hữu Tokio runtime,
transport, AuthN/AuthZ và đăng ký domain tool qua trait `HostTool`.

- Crate: `orchestrator-agent` v0.4.0 (import là `orchestrator_agent_rs`), edition 2021, Rust ≥ 1.89
- Kiến trúc: Root/Subagent dùng chung `AgentRuntime`, crate [`llm`](https://crates.io/crates/llm) `=1.3.8`,
  SQLx/SQLite làm execution store, `ArtifactStore` cục bộ bất biến
- Thiết kế chi tiết: [docs/README.md](docs/README.md) · Hợp đồng public: [docs/18-embeddable-library.md](docs/18-embeddable-library.md)

## Tính năng

| Nhóm | Mô tả |
|---|---|
| Multi-provider | OpenAI, Anthropic, Gemini, Ollama, OpenAI-compatible; mỗi agent chọn provider/model riêng |
| Agent | Root (`Primary`), Subagent, hoặc `All`; prompt từ file, giới hạn `steps` và context window theo agent |
| Tool | `HostTool` do host đăng ký + built-in `subagent`, `skill`, `artifact_read`; phân loại `ReadOnly`/`Mutating`, timeout, cap output |
| Skill | Quét `skills/<id>/SKILL.md` lúc build (không hot reload) |
| Parent–child | Tool `subagent` tạo child session riêng, admission idempotent theo invocation |
| Bền vững | Durable admission, run attempt, tool checkpoint; recovery sau restart (không replay tool Mutating) |
| Long-running | Auto compaction trong active turn, step cap tùy chọn, cancel hợp tác, shutdown drain/quiesce/join |
| Quan sát | `subscribe` (feed best-effort, bounded) + `snapshot` (canonical, một read transaction) |
| Artifact | Lưu file bất biến, scoped theo owner/workspace/family, kiểm SHA-256, backup nhất quán |
| An toàn | `SecretString` che secret, lỗi public đã sanitize với mã ổn định, chỉ một process sở hữu store (owner lock) |

## Yêu cầu

- Rust ≥ 1.89
- Một endpoint LLM (OpenAI-compatible, Anthropic, Gemini hoặc Ollama)
- `yt-dlp` + `ffmpeg` chỉ cần cho example `video_summary`

## Cài đặt

```toml
[dependencies]
orchestrator-agent = "0.4"
async-trait = "0.1"
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Feature `test-support` (fake `LlmGateway` + fault hooks) chỉ dành cho test, không bật ở production.

## Bắt đầu nhanh

```rust
use std::{sync::Arc, time::Duration};
use orchestrator_agent_rs::*;

struct EchoTool;

#[async_trait::async_trait]
impl HostTool for EchoTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "echo".into(),
            description: "Trả lại tham số".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "query": { "type": "string" } },
                "required": ["query"]
            }),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: serde_json::Value)
        -> Result<ToolOutcome, ToolError>
    {
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: args.to_string(),
            metadata: serde_json::Value::Null,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let llm = LlmConfig {
        providers: vec![LlmProviderConfig {
            id: "openai".into(),
            backend: LlmBackend::OpenAi,
            endpoint: None,
            auth: ProviderAuth::ApiKey(SecretString::new(std::env::var("OPENAI_API_KEY")?)),
            request_timeout: Duration::from_secs(120),
        }],
    };

    let root = AgentDefinition::new("root", "openai", "gpt-4.1-mini")
        .with_mode(AgentMode::Primary)
        .with_prompt_path("prompts/root.md")      // file phải tồn tại
        .with_allowed_tools(["echo"]);

    let orch = Orchestrator::builder(OrchestratorConfig::new(llm, vec![root], "./store"))
        .register_tool(Arc::new(EchoTool))
        .build()
        .await?;

    let caller = CallerContext::new("user-1", "workspace-1");
    let session = orch.create_session(&caller, "root").await?;
    let turn = orch.submit(&caller, &session, "Xin chào".into()).await?;

    // submit không chờ LLM: poll `result` hoặc dùng subscribe/snapshot.
    let result = loop {
        let r = orch.result(&caller, &session, &turn.turn_id).await?;
        if r.status != RunStatus::Running { break r; }
        tokio::time::sleep(Duration::from_millis(300)).await;
    };
    println!("{:?}: {:?}", result.status, result.output);

    orch.shutdown().await?;
    Ok(())
}
```

## Khái niệm chính

- **Owner / workspace** (`CallerContext`): scope định danh do host xác thực; mọi API đều kiểm tra scope.
- **Session**: Root session do host tạo; Child session do runtime tạo khi agent gọi `subagent`. Một session chỉ có một run active.
- **Turn**: một lần `submit`. Mỗi turn có một hoặc nhiều **run attempt** (attempt > 0 là phục hồi sau restart).
- **Invocation**: một lần gọi tool (ID nội bộ, khác `tool_call_id` của provider).
- **Family**: Root session + các Child của nó; đơn vị của `subscribe`/`snapshot`.
- **Artifact**: file bất biến, có `ArtifactRef` (id, name, mime, size, sha256, role).

Thuật ngữ đầy đủ: [docs/14-glossary.md](docs/14-glossary.md).

## API tham chiếu

Mọi hàm async trả `Result<_, OrchestratorError>`. Sau `shutdown()` các hàm (trừ getter tĩnh) trả `SHUTTING_DOWN`.

### Cấu hình

#### `OrchestratorConfig`

| Field / hàm | Ý nghĩa |
|---|---|
| `OrchestratorConfig::new(llm, agents, store_dir)` | Tạo config với `RuntimeConfig::default()` |
| `llm: LlmConfig` | Danh sách `LlmProviderConfig` (để trống khi dùng `with_provider_runtime`) |
| `agents: Vec<AgentDefinition>` | Registry agent |
| `store_dir` | Thư mục SQLite + owner lock + artifact. Dùng DB riêng, không dùng DB nghiệp vụ của host |
| `runtime: RuntimeConfig` | Giới hạn vận hành (bên dưới) |
| `.with_skills_dir(dir)` | Thư mục `skills/<id>/SKILL.md` |
| `.with_project_instructions(path)` | File như `AGENTS.md`, snapshot lúc build, là *untrusted context* |

#### `LlmProviderConfig`

| Field | Ý nghĩa |
|---|---|
| `id` | Alias cục bộ, duy nhất (agent tham chiếu qua alias này) |
| `backend` | `LlmBackend::{OpenAi, Anthropic, Gemini, Ollama, OpenAiCompatible}` |
| `endpoint` | Bắt buộc với `Ollama`, `OpenAiCompatible` |
| `auth` | `ProviderAuth::None` hoặc `ProviderAuth::ApiKey(SecretString)` |
| `request_timeout` | Timeout của *một* request LLM |

`SecretString::new(..)` che giá trị khi `Debug`. Host tự resolve secret trước `build()`; library validate cục bộ, không gọi remote lúc build.

#### Provider runtime của server (thay `LlmProviderConfig`)

Khi host/server đã sở hữu việc gọi model (credential, client, routing), cắm bằng
`Orchestrator::builder(cfg).with_provider_runtime(Arc<dyn ProviderRuntime>)` và để trống `llm.providers`
(nếu không trống sẽ bị từ chối với `ConfigReason::ProviderRuntimeConflict`).

| Mục | Ý nghĩa |
|---|---|
| `ProviderRuntime::providers()` | `Vec<RuntimeProvider { id, fingerprint }>`: alias mà agent tham chiếu qua `provider_id`, kèm fingerprint routing không chứa secret (đổi khi alias trỏ backend khác để recovery không resume nhầm) |
| `ProviderRuntime::complete(&ModelRequest, &CancellationToken, StreamSink)` | Một lượt model hoàn chỉnh. Chỉ trả `Ok` khi response đã xong; lỗi/hủy/timeout trả `Err(GatewayError)`. Nếu thiếu `ModelUsage` thì các trường thiếu được điền bằng ước lượng token từ request/response |

#### `AgentDefinition`

`AgentDefinition::new(id, provider_id, model_id)` rồi chain:

| Builder | Ý nghĩa |
|---|---|
| `.with_mode(AgentMode)` | `Primary` (root), `Subagent` (chỉ làm child), `All` (mặc định) |
| `.with_description(s)` | Mô tả cho model khi chọn subagent |
| `.with_prompt_path(path)` | Prompt agent (bắt buộc, file phải tồn tại) |
| `.with_allowed_tools(iter)` | Tool được phép: tên `HostTool` hoặc built-in `subagent`, `skill`, `artifact_read` |
| `.with_allowed_skills(iter)` | Skill được phép |
| `.with_allowed_subagents(iter)` | Giới hạn danh sách subagent (`mode != Primary`) mà agent này được thấy và ủy quyền qua tool `subagent` (mặc định `None` = mọi agent không phải `Primary`) |
| `.with_steps(n)` | Trần số bước (mặc định không giới hạn; `n > 0`) |
| `.with_context_limit_tokens(n)` | Context window riêng, mặc định `RuntimeConfig::context_limit_tokens` |
| `.with_max_output_tokens(n)` | Trần output token mỗi request LLM (`n > 0`); mặc định gateway 4096 |
| `.with_temperature(t)` | Temperature `0.0..=2.0`; mặc định để provider quyết định |
| `.with_top_p(p)` | Nucleus sampling, `0 < p <= 1` |
| `.with_top_k(k)` | `top_k > 0`; backend không hỗ trợ sẽ bỏ qua |
| `.with_model_params(ModelParams { .. })` | Đặt cả bốn tham số trên một lần |

#### `RuntimeConfig` (giá trị mặc định)

| Field | Mặc định | Ý nghĩa |
|---|---|---|
| `supervisor_sweep` | 30s | Chu kỳ reconcile claim chưa có owner |
| `page_size` | 50 | Trần item mỗi trang |
| `shutdown_grace` | 30s | Chờ task quiesce/join sau khi cancel |
| `shutdown_drain` | 0 | Drain trước khi cancel (0 = cancel ngay) |
| `max_resume_attempts` | 10 | Số lần auto recovery mỗi turn |
| `tool_timeout` | 300s | Timeout một thao tác tool |
| `tool_output_cap_bytes` | 64 KiB | Cắt output model-visible |
| `tool_result_hard_cap_bytes` | 8 MiB | Vượt thì `TOOL_OUTPUT_TOO_LARGE` |
| `skill_max_bytes` | 64 KiB | Trần một `SKILL.md` |
| `context_limit_tokens` | 100 000 | Context window ước lượng |
| `compaction_threshold_percent` | 80 | Ngưỡng auto compaction |
| `compaction_keep_recent` | 2 | Số exchange gần nhất giữ nguyên văn |
| `event_capacity` | 4096 | Sức chứa broadcast feed |
| `event_delta_max_bytes` | 4 KiB | Trần một `text.delta` |
| `progress_min_interval` | 200ms | Khoảng tối thiểu giữa hai `tool.progress` |
| `snapshot_refresh` | 30s | Chu kỳ refresh snapshot khi còn active |
| `artifact_max_bytes` | 128 MiB | Trần một artifact |
| `owner_workspace_quota_bytes` | 4 GiB | Quota mỗi owner/workspace |
| `disk_reserve_bytes` | 1 GiB | Dung lượng đĩa dự trữ |
| `download_max_bytes` | 256 MiB | Trần khi host tải artifact |
| `artifact_preview_limit` | 20 | Số artifact trong `AgentResult::artifact_preview` |
| `artifact_staging_grace` | 300s | Ân hạn trước khi sweep file staging mồ côi |
| `max_concurrent_root_runs` | `None` | Giới hạn root run đồng thời |

### `Orchestrator` builder

| Hàm | Mô tả |
|---|---|
| `Orchestrator::builder(config) -> OrchestratorBuilder` | Khởi tạo builder |
| `.register_tool(Arc<dyn HostTool>)` | Đăng ký tool (tên `[a-z][a-z0-9_]{0,63}`, không trùng tên reserved) |
| `.build().await` | Validate config/registry/skill, mở store, lấy owner lock, chạy startup recovery |

Lỗi build phổ biến: `INVALID_INPUT` kèm `ConfigReason` (`err.reason()`), `STORE_ALREADY_OWNED` nếu process khác đang giữ store.

### Session & Turn

| Hàm | Mô tả |
|---|---|
| `create_session(&caller, agent_id) -> String` | Tạo Root session. Agent `Subagent`-only → `PERMISSION_DENIED` |
| `create_session_with_policy(&caller, agent_id, SessionPolicy)` | Thu hẹp quyền tool/skill (chỉ giao, không mở rộng); `child_allowed_*` đặt trần cho child |
| `submit(&caller, session_id, text) -> AcceptedTurn` | Commit input + run claim rồi trả ngay, không chờ LLM. `SESSION_BUSY`, `SESSION_ARCHIVED`, `INVALID_INPUT` (text rỗng) |
| `result(&caller, session_id, turn_id) -> AgentResult` | Projection attempt mới nhất: `status`, `steps`, `input_tokens`, `output_tokens` (`total_tokens()`), `output`, `final_message_id`, `error`, `artifact_preview`, `artifacts_next_cursor` |
| `history(&caller, session_id, PageRequest) -> Page<MessageView>` | Lịch sử canonical của một session |
| `archive_session(&caller, session_id)` | Chặn admission mới; history vẫn đọc được; idempotent |
| `cancel(&caller, session_id)` | Hủy hợp tác run active (và child đang chờ). Run kết thúc `cancelled`, không tự resume |
| `delete_session(&caller, root_session_id)` | Xoá vĩnh viễn Root + Child, messages, runs, invocations và artifact (cả file). Chỉ nhận Root (`INVALID_INPUT` với Child); `SESSION_BUSY` nếu còn run active → `cancel` trước. Không hoàn tác, không có event xoá trên feed |

`RunStatus`: `Running`, `Completed`, `Failed`, `Cancelled`, `TimedOut`, `Interrupted`.
`Completed` chỉ nghĩa là thực thi xong, không khẳng định yêu cầu nghiệp vụ đã đạt.

### Truy vấn

| Hàm | Trả về |
|---|---|
| `list_sessions(&caller, &SessionFilter, PageRequest)` | `Page<SessionSummary>` — Root session, lọc theo `agent_id`/`archived` |
| `search_sessions(&caller, &SessionSearch, PageRequest)` | `Page<SessionSummary>` — khớp từ khóa trong agent/nội dung cả family |
| `get_session(&caller, session_id)` | `SessionSummary` (Root hoặc Child) |
| `list_turns(&caller, session_id, PageRequest)` | `Page<TurnSummary>` |
| `list_runs(&caller, session_id, Option<turn_id>, PageRequest)` | `Page<RunSummary>` — mọi attempt, kể cả recovery |
| `get_run(&caller, run_id)` | `RunSummary` (provider, model, steps, token in/out, thời gian) |
| `search_history(&caller, &HistorySearch, PageRequest)` | `Page<HistoryHit>` — mới nhất trước, có `snippet` |

Phân trang: `PageRequest::first(limit)` hoặc `PageRequest { cursor, limit }`; `limit` được clamp vào `1..=page_size`.
Cursor là opaque, gắn với đúng đối tượng truy vấn và **không** phải authorization token.

### Quan sát realtime

| Hàm | Mô tả |
|---|---|
| `subscribe(&caller, root_session_id) -> Subscription` | Feed của cả family. `Subscription::recv().await -> Option<FeedItem>` |
| `snapshot(&caller, root_session_id) -> FamilySnapshot` | Trạng thái canonical của Root + Child (một read transaction) |
| `snapshot_refresh_interval() -> Duration` | Chu kỳ refresh khi `FamilySnapshot::has_active_work()` |

`FeedItem` là `Event(ExecutionEvent)` hoặc `ResyncRequired`. `ExecutionEvent::to_json()` trả envelope để host tự đẩy qua SSE/WebSocket:

```json
{ "event_type": "tool.progress", "root_session_id": "…", "session_id": "…",
  "run_id": "…", "state_revision": null, "timestamp_ms": 0, "payload": {} }
```

Loại event: `run.started|completed|failed|cancelled|interrupted`, `step.started|ended`, `text.delta`,
`message.committed`, `tool.started|progress|completed|failed|interrupted`, `session.changed`,
`compaction.started|completed`.

- Payload `tool.started`: `{ "tool_call_id", "invocation_id", "tool_name", "arguments" }`.
- Payload `tool.completed` / `tool.failed`: `{ "tool_call_id", "invocation_id", "tool_name", "arguments", "output", "result", "error_code" }` (`result` là `{ "content", "metadata" }` khi thành công hoặc `{ "error": { "code", "message" } }` khi thất bại).
- `MessageToolCall` (trong `MessageView::tool_calls`) gồm `{ call_id, name, arguments }`.
- `InvocationSnapshot` (trong `SessionSnapshot::invocations`) gồm `{ invocation_id, run_id, tool_call_id, tool_name, arguments, status, result, child_session_id, error_code }`.

Event có `state_revision = Some(..)` là *committed invalidation* (phát sau commit DB, chỉ báo view cũ);
event `None` là ephemeral (`text.delta`, `tool.progress`). Feed là best-effort, bounded, không bao giờ chặn publisher.

**Hợp đồng cho host adapter**
1. `subscribe` **trước** khi gọi `snapshot`.
2. Refresh snapshot (debounce) cho tới khi mọi session đạt revision đã thấy.
3. Nhận `ResyncRequired` hoặc stream kết thúc: bỏ preview chưa xác nhận, subscribe lại rồi snapshot lại.
4. Khi `has_active_work()`: refresh theo `snapshot_refresh_interval()` kể cả khi stream còn sống.

### Artifact

| Hàm | Mô tả |
|---|---|
| `list_artifacts(&caller, session_id, turn_id, PageRequest)` | `Page<ArtifactRef>` ổn định theo `(created_at, id)` |
| `open_artifact(&caller, artifact_id) -> ArtifactDownload` | Stream có giới hạn `download_max_bytes`, kiểm SHA-256; không lộ path. Dùng `metadata()`, `read_chunk`, `read_all`, `into_reader()` |
| `backup_to(dest_dir)` | Backup nhất quán SQLite (`VACUUM INTO`, an toàn khi WAL mở) + artifact committed. `dest_dir` phải khác `store_dir` và chưa có DB |

`ArtifactInput` dựng bằng `primary_bytes`, `intermediate_bytes`, `from_bytes`, `from_reader`.
`ArtifactReader` có `metadata`, `read_chunk`, `read_all`, `read_to_string`.

### Vận hành & phục hồi

| Hàm | Mô tả |
|---|---|
| `resolve_interrupted_invocation(&operator, session_id, invocation_id, VerifiedToolResolution)` | Operator reconcile tool `interrupted` (outcome không xác định) |
| `shutdown()` | Dừng admission → drain → cancel hợp tác → quiesce/join → đóng store, nhả lock |
| `agent(id)`, `agent_ids()` | Đọc registry |
| `tool_descriptors()` | Descriptor các tool đã đăng ký |
| `provider_fingerprint(provider_id)` | Fingerprint routing không chứa secret |
| `base_instructions_version()` | Phiên bản base instructions nhúng sẵn |

- `OperatorContext::new(operator_id, owner_id, workspace_id).with_resolve_capability()` — host mint **sau** khi xác thực; mặc định không có quyền.
- `VerifiedToolResolution::ConfirmedCompleted { outcome, evidence }` ghi kết quả tool; `ConfirmedNoEffect { evidence }` thành lỗi kỹ thuật. Không bao giờ replay tool.
- Gọi `shutdown()` quá `shutdown_grace` mà còn task ghi được: trả lỗi *incomplete* và **giữ** lock. Run bị ngắt thành `interrupted`, phục hồi ở lần khởi động sau.

### Viết `HostTool`

```rust
#[async_trait::async_trait]
pub trait HostTool: Send + Sync {
    fn descriptor(&self) -> ToolDescriptor;
    fn timeout(&self) -> Option<Duration> { None }   // None = RuntimeConfig::tool_timeout
    async fn execute(&self, ctx: ToolContext, args: serde_json::Value)
        -> Result<ToolOutcome, ToolError>;
}
```

- `ToolDescriptor { name, description, input_schema, effect }`.
- `ToolEffect::ReadOnly`: timeout trả lỗi kỹ thuật `TOOL_TIMEOUT`. `ToolEffect::Mutating`: timeout/mất kết nối khiến outcome *unknown* → run `interrupted`, chờ operator.
- `ToolOutcome { call_id, content, metadata }`; `call_id` phải là `ctx.scope().tool_call_id`. `metadata.artifact_ids` được runtime xác minh.
- `ToolError { code, message }`.

**`ToolContext`** (do runtime mint, host không tự dựng):

| Hàm | Mô tả |
|---|---|
| `scope() -> &ToolScope` | `owner_id`, `workspace_id`, `session_id`, `root_session_id`, `turn_id`, `run_id`, `agent_id`, `invocation_id`, `tool_call_id`, `allowed_tools`, `allowed_skills` |
| `is_cancelled()` | Kiểm tra cancel để dừng sớm |
| `progress(text)` / `progress_with_percent(text, pct)` | Phát `tool.progress` (≤ 1 KiB, rate-limit) |
| `artifacts().put(ArtifactInput) -> ArtifactRef` | Lưu artifact bất biến |
| `artifacts().open(artifact_id) -> ArtifactReader` | Đọc artifact trong scope (cùng family / input tường minh / checkpoint) |
| `checkpoint().current()` / `.commit(manifest_id, expected_previous_id)` | Con trỏ checkpoint có CAS cho workflow dài; xung đột → `CHECKPOINT_CONFLICT` |

### Built-in tool

| Tên | Công dụng |
|---|---|
| `subagent` | Giao việc cho subagent (tạo/tái sử dụng child session; danh sách ứng viên có thể giới hạn theo từng caller qua `with_allowed_subagents`) |
| `skill` | Nạp nội dung skill theo id |
| `artifact_read` | Đọc artifact UTF-8 (cap `tool_output_cap_bytes`, kiểm SHA-256) |

Agent phải opt-in qua `allowed_tools`.

### Mã lỗi

`OrchestratorError::code()` trả `ErrorCode`; `.as_str()` là hợp đồng ổn định:

| Mã | Ý nghĩa |
|---|---|
| `INVALID_INPUT` | Input/config không hợp lệ (config: `reason()` trả `ConfigReason`) |
| `PERMISSION_DENIED` | Không đủ quyền / sai scope |
| `NOT_FOUND` | Đối tượng không tồn tại hoặc ngoài scope |
| `SESSION_BUSY` | Session đã có run active |
| `SESSION_ARCHIVED` | Session đã archive |
| `RECOVERY_BLOCKED` | Recovery bị chặn, cần operator |
| `PROVIDER_CONFIG_CHANGED` | Routing provider đổi so với lúc run bắt đầu |
| `STORE_ALREADY_OWNED` | Process khác đang giữ owner lock |
| `SHUTTING_DOWN` | Orchestrator đang/đã shutdown |
| `UNAVAILABLE` | Dịch vụ phụ thuộc tạm thời không dùng được |
| `INTERNAL` | Lỗi nội bộ (đã sanitize) |

`ErrorCode` là `#[non_exhaustive]`: luôn có nhánh `_`.

## Mô hình bảo mật

- Mọi API kiểm tra `owner_id`/`workspace_id`; scope tool do runtime mint, không lấy từ LLM.
- Secret chỉ nằm trong `SecretString`, không vào log/event/lỗi (các API key đã cấu hình được che đệ quy trên mọi payload của `ExecutionEvent`).
- Artifact không lộ filesystem path; kiểm toàn vẹn SHA-256 khi đọc.
- Project instructions và nội dung skill là untrusted, không thay thế base instructions hay quyền thực thi.
- Chi tiết: [docs/10-security-operations.md](docs/10-security-operations.md).

## Cấu trúc repo

```
src/
  orchestrator/  public handle + builder (session, query, observe, artifacts, lifecycle)
  runtime/       agent loop, compaction, settle
  executor/      chạy tool, timeout, schema, permission
  subagent/      tool subagent, child admission
  store/         SQLite execution store, recovery
  feed/          event feed, subscription, redaction
  artifact/      ArtifactStore, backup, cursor
  gateway/       adapter crate `llm`
  agent.rs config.rs tool.rs skill.rs prompt.rs types.rs error.rs ...
migrations/      SQL migration (0001–0004)
examples/        host, live_flow, load_skills, playlist_notes, url_summary, video_summary
tests/           m0–m7 (fake + live) và read_api
docs/            thiết kế, ADR, evidence, spec
spikes/g0/       spike độc lập (không thuộc library)
```

## Chạy example

```sh
cargo run --example host        # build Orchestrator từ host Tokio (không gọi LLM thật)
```

### `live_flow` — gọi model thật

Điền `G0_OPENAI_BASE_URL`, `G0_OPENAI_MODEL`, `G0_OPENAI_API_KEY` trong `.env.live`
(copy từ [.env.live.example](.env.live.example); file này đã nằm trong `.gitignore`):

```sh
cp .env.live.example .env.live
cargo run --example live_flow
cargo run --example live_flow -- --env-file .env.live --output-dir target/live-flow
```

[Example live_flow](examples/live_flow.rs): Root giao việc cho analyst → analyst gọi `lookup_code` có progress →
Root trả lời → Root hỏi tiếp cùng child session. Cấu hình provider chỉ lấy từ file được chọn
(`--env-file`, `LIVE_ENV_FILE`, mặc định `.env.live`); biến `G0_OPENAI_*` của shell không ghi đè.
Log JSONL ở `target/live-flow/<UUID>/events.jsonl` (API key được che). Exit 0 khi cả hai lượt completed,
tool đã chạy và child session được tái sử dụng; mỗi lượt chờ tối đa 240 giây.

### `load_skills` — agent nạp skill qua tool `skill(id)`

[Example load_skills](examples/load_skills.rs): `OrchestratorConfig::with_skills_dir` trỏ tới
[docs/examples/skills](docs/examples/skills) (scan một lần lúc `build()`); agent `reviewer` có tool `skill` và
`with_allowed_skills(["rust-review"])`. Agent được yêu cầu nạp skill `rust-review` rồi liệt kê các bước workflow.

```sh
cp .env.live.example .env.live   # nếu chưa có; điền G0_OPENAI_*
cargo run --example load_skills
cargo run --example load_skills -- --env-file .env.live
```

Exit 0 khi turn completed và invocation `skill` ở trạng thái `Completed`; kết quả in ra stdout.
Store tạm nằm trong thư mục temp và được xóa sau khi chạy.

### `video_summary` — video → transcript → tóm tắt → Markdown

[Example video_summary](examples/video_summary.rs): root `coordinator` chỉ có tool `subagent`;
`explorer` lấy transcript (yt-dlp) và tóm tắt; `writer` ghi Markdown (`markdown_create`, `markdown_append_section`, `markdown_read`).

```sh
brew install yt-dlp ffmpeg
cargo run --example video_summary -- "<URL>" --lang en
# Tùy chọn: --lang vi,en  --env-file .env.live  --output-dir target/video-summary  --json
cargo test --example video_summary
```

Kết quả: `target/video-summary/<UUID>/markdown/summary.md` và `events.jsonl`. Phụ đề auto-dịch hay bị YouTube trả 429
nên tool ưu tiên `<lang>-orig`; video không phụ đề báo `NO_TRANSCRIPT`. Model nhỏ làm root có thể gọi `subagent` sai tham số, xem `tool.failed` trong log.

## Phát triển & kiểm thử

```sh
scripts/check.sh          # fast: fmt --check + clippy (-D warnings) + test lib/integration
scripts/check.sh full     # + build examples + doc-test
cargo test                # test deterministic (fake gateway, SQLite/filesystem thật)

# Test live với provider thật (cần .env.live)
cargo test --test m1_live_provider -- --ignored --nocapture   # tương tự m2…m7_live_provider
cargo test --test m8_live_hard_flow -- --ignored --nocapture  # flow khó: 2 child, retry tool, restart, recall
cargo test --test m8_live_max_tokens -- --ignored --nocapture # trần tool output, compaction theo agent, max_output_tokens
```

Output của `scripts/check.sh` rút gọn; đặt `CHECK_MAX_LINES` để tăng số dòng lỗi.

| Test | Phạm vi |
|---|---|
| `m0_library_shell` | Builder, config, registry validation |
| `m1_single_agent` | Admission, single-agent run, `result`/`history` |
| `m2_tools_skills` | Tool loop, skill, prompt |
| `m3_parent_child` | `subagent`, child session |
| `m4_continuous` | Step cap, compaction, stream assembly |
| `m5_recovery`, `m5_process` | Recovery, cancel, shutdown, owner lock |
| `m6_observation` | `subscribe`/`snapshot` |
| `m7_artifacts` | Artifact, quota, backup, fault injection |
| `read_api` | Query API |
| `m8_live_hard_flow` (live) | Root → 2 child, chuỗi tool phụ thuộc, retry lỗi tạm thời, restart engine, follow-up cùng child |
| `m8_live_max_tokens` (live) | `tool_output_cap_bytes`, `tool_result_hard_cap_bytes`, compaction theo `context_limit_tokens` của agent, `max_output_tokens` |

Bằng chứng từng milestone: [docs/evidence/](docs/evidence/) · Spec triển khai: [docs/specs/v5-implementation-m0-m7.md](docs/specs/v5-implementation-m0-m7.md).

## Tài liệu

| Tài liệu | Nội dung |
|---|---|
| [docs/README.md](docs/README.md) | Mục lục thiết kế |
| [01-architecture](docs/01-architecture.md) | Kiến trúc, module |
| [05-tools](docs/05-tools.md) · [06-delegation](docs/06-delegation.md) | Tool, giao tiếp Root ↔ Subagent |
| [09-persistence](docs/09-persistence.md) | Schema SQLite, recovery |
| [13-adrs](docs/13-adrs.md) · [14-glossary](docs/14-glossary.md) | ADR, thuật ngữ |
| [16-streaming-events](docs/16-streaming-events.md) | Event, snapshot/resync |
| [17-agent-results-artifacts](docs/17-agent-results-artifacts.md) | Artifact |
| [18-embeddable-library](docs/18-embeddable-library.md) | Hợp đồng public |
| [Ví dụ cấu hình](docs/examples/agents.yaml) | Host-side DTO mẫu (core không parse YAML) |

[Review report](docs/research/design-review-report.md) là snapshot lịch sử trước khi chốt v5.0.

## License

Dual-licensed under MIT OR Apache-2.0 (xem `LICENSE-MIT`, `LICENSE-APACHE`).
