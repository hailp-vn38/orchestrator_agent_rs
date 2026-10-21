<p align="right">🌐 <b>🇬🇧 English</b> · <a href="README.vi.md">🇻🇳 Tiếng Việt</a></p>

# Orchestrator Agent RS

A Rust library you **embed into an existing server** to orchestrate multiple agents (Root ↔ Subagent) with
host-provided LLMs, tools and skills. The core does not depend on any web framework: the host owns the Tokio
runtime, transport, AuthN/AuthZ, and registers domain tools through the `HostTool` trait.

- Crate: `orchestrator-agent` v0.1.0 (imported as `orchestrator_agent_rs`), edition 2021, Rust ≥ 1.89
- Architecture: Root/Subagent share one `AgentRuntime`, the [`llm`](https://crates.io/crates/llm) crate `=1.3.8`,
  SQLx/SQLite as the execution store, and an immutable local `ArtifactStore`
- Detailed design: [docs/README.md](docs/README.md) · Public contract: [docs/18-embeddable-library.md](docs/18-embeddable-library.md)

## Features

| Area | Description |
|---|---|
| Multi-provider | OpenAI, Anthropic, Gemini, Ollama, OpenAI-compatible; each agent picks its own provider/model |
| Agent | Root (`Primary`), Subagent, or `All`; prompt from file, per-agent `steps` limit and context window |
| Tool | Host-registered `HostTool` + built-in `subagent`, `skill`, `artifact_read`; `ReadOnly`/`Mutating` classification, timeout, output cap |
| Skill | Scans `skills/<id>/SKILL.md` at build time (no hot reload) |
| Parent–child | The `subagent` tool creates a dedicated child session, with idempotent admission per invocation |
| Durable | Durable admission, run attempts, tool checkpoints; recovery after restart (`Mutating` tools are never replayed) |
| Long-running | Auto compaction within an active turn, optional step cap, cooperative cancel, shutdown drain/quiesce/join |
| Observability | `subscribe` (best-effort, bounded feed) + `snapshot` (canonical, single read transaction) |
| Artifact | Immutable files scoped by owner/workspace/family, SHA-256 verification, consistent backup |
| Safety | `SecretString` masks secrets, sanitized public errors with stable codes, single process owns the store (owner lock) |

## Requirements

- Rust ≥ 1.89
- An LLM endpoint (OpenAI-compatible, Anthropic, Gemini or Ollama)
- `yt-dlp` + `ffmpeg` are only needed for the `video_summary` example

## Installation

```toml
[dependencies]
orchestrator-agent = "0.1"
async-trait = "0.1"
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

The `test-support` feature (fake `LlmGateway` + fault hooks) is for tests only; do not enable it in production.

## Quick start

```rust
use std::{sync::Arc, time::Duration};
use orchestrator_agent_rs::*;

struct EchoTool;

#[async_trait::async_trait]
impl HostTool for EchoTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "echo".into(),
            description: "Echo the arguments back".into(),
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
        .with_prompt_path("prompts/root.md")      // the file must exist
        .with_allowed_tools(["echo"]);

    let orch = Orchestrator::builder(OrchestratorConfig::new(llm, vec![root], "./store"))
        .register_tool(Arc::new(EchoTool))
        .build()
        .await?;

    let caller = CallerContext::new("user-1", "workspace-1");
    let session = orch.create_session(&caller, "root").await?;
    let turn = orch.submit(&caller, &session, "Hello".into()).await?;

    // submit does not wait for the LLM: poll `result` or use subscribe/snapshot.
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

## Core concepts

- **Owner / workspace** (`CallerContext`): identity scope authenticated by the host; every API checks the scope.
- **Session**: a Root session is created by the host; a Child session is created by the runtime when an agent calls `subagent`. A session has at most one active run.
- **Turn**: one `submit`. Each turn has one or more **run attempts** (attempt > 0 is a recovery after restart).
- **Invocation**: one tool call (internal ID, distinct from the provider's `tool_call_id`).
- **Family**: a Root session plus its Children; the unit of `subscribe`/`snapshot`.
- **Artifact**: an immutable file with an `ArtifactRef` (id, name, mime, size, sha256, role).

Full glossary: [docs/14-glossary.md](docs/14-glossary.md).

## API reference

Every async function returns `Result<_, OrchestratorError>`. After `shutdown()`, all functions (except static getters) return `SHUTTING_DOWN`.

### Configuration

#### `OrchestratorConfig`

| Field / function | Meaning |
|---|---|
| `OrchestratorConfig::new(llm, agents, store_dir)` | Creates a config with `RuntimeConfig::default()` |
| `llm: LlmConfig` | List of `LlmProviderConfig` |
| `agents: Vec<AgentDefinition>` | Agent registry |
| `store_dir` | Directory for SQLite + owner lock + artifacts. Use a dedicated DB, not the host's business DB |
| `runtime: RuntimeConfig` | Operational limits (below) |
| `.with_skills_dir(dir)` | Directory containing `skills/<id>/SKILL.md` |
| `.with_project_instructions(path)` | A file such as `AGENTS.md`, snapshotted at build time, treated as *untrusted context* |

#### `LlmProviderConfig`

| Field | Meaning |
|---|---|
| `id` | Unique local alias (agents reference providers through it) |
| `backend` | `LlmBackend::{OpenAi, Anthropic, Gemini, Ollama, OpenAiCompatible}` |
| `endpoint` | Required for `Ollama`, `OpenAiCompatible` |
| `auth` | `ProviderAuth::None` or `ProviderAuth::ApiKey(SecretString)` |
| `request_timeout` | Timeout of a *single* LLM request |

`SecretString::new(..)` masks the value in `Debug`. The host resolves secrets before `build()`; the library validates locally and makes no remote calls at build time.

#### Server provider runtime (instead of `LlmProviderConfig`)

When the host/server already owns model access (credentials, clients, routing), plug it in with
`Orchestrator::builder(cfg).with_provider_runtime(Arc<dyn ProviderRuntime>)` and leave `llm.providers` empty
(a non-empty list is rejected with `ConfigReason::ProviderRuntimeConflict`).

| Item | Meaning |
|---|---|
| `ProviderRuntime::providers()` | `Vec<RuntimeProvider { id, fingerprint }>`: aliases agents reference via `provider_id`, plus a non-secret routing fingerprint (change it when the alias points to another backend, so recovery will not resume on the wrong one) |
| `ProviderRuntime::complete(&ModelRequest, &CancellationToken, StreamSink)` | One complete model turn. Return `Ok` only for a fully finished response; errors/cancel/timeout return `Err(GatewayError)`. Missing `ModelUsage` fields are filled with request/response token estimates |

#### `AgentDefinition`

`AgentDefinition::new(id, provider_id, model_id)`, then chain:

| Builder | Meaning |
|---|---|
| `.with_mode(AgentMode)` | `Primary` (root), `Subagent` (child only), `All` (default) |
| `.with_description(s)` | Description shown to the model when choosing a subagent |
| `.with_prompt_path(path)` | Agent prompt (required, the file must exist) |
| `.with_allowed_tools(iter)` | Allowed tools: `HostTool` names or built-ins `subagent`, `skill`, `artifact_read` |
| `.with_allowed_skills(iter)` | Allowed skills |
| `.with_steps(n)` | Step cap (unlimited by default; `n > 0`) |
| `.with_context_limit_tokens(n)` | Per-agent context window, defaults to `RuntimeConfig::context_limit_tokens` |
| `.with_max_output_tokens(n)` | Output-token cap per LLM request (`n > 0`); gateway default 4096 |
| `.with_temperature(t)` | Temperature `0.0..=2.0`; defaults to the provider's choice |
| `.with_top_p(p)` | Nucleus sampling, `0 < p <= 1` |
| `.with_top_k(k)` | `top_k > 0`; ignored by backends that do not support it |
| `.with_model_params(ModelParams { .. })` | Sets all four parameters above at once |

#### `RuntimeConfig` (defaults)

| Field | Default | Meaning |
|---|---|---|
| `supervisor_sweep` | 30s | Interval for reconciling unowned claims |
| `page_size` | 50 | Max items per page |
| `shutdown_grace` | 30s | Wait for tasks to quiesce/join after cancel |
| `shutdown_drain` | 0 | Drain before cancel (0 = cancel immediately) |
| `max_resume_attempts` | 10 | Auto-recovery attempts per turn |
| `tool_timeout` | 300s | Timeout of one tool operation |
| `tool_output_cap_bytes` | 64 KiB | Truncation of model-visible output |
| `tool_result_hard_cap_bytes` | 8 MiB | Exceeding it yields `TOOL_OUTPUT_TOO_LARGE` |
| `skill_max_bytes` | 64 KiB | Cap for one `SKILL.md` |
| `context_limit_tokens` | 100 000 | Estimated context window |
| `compaction_threshold_percent` | 80 | Auto-compaction threshold |
| `compaction_keep_recent` | 2 | Number of most recent exchanges kept verbatim |
| `event_capacity` | 4096 | Broadcast feed capacity |
| `event_delta_max_bytes` | 4 KiB | Cap for one `text.delta` |
| `progress_min_interval` | 200ms | Minimum gap between two `tool.progress` events |
| `snapshot_refresh` | 30s | Snapshot refresh interval while active |
| `artifact_max_bytes` | 128 MiB | Cap for one artifact |
| `owner_workspace_quota_bytes` | 4 GiB | Quota per owner/workspace |
| `disk_reserve_bytes` | 1 GiB | Reserved disk space |
| `download_max_bytes` | 256 MiB | Cap when the host downloads an artifact |
| `artifact_preview_limit` | 20 | Artifacts in `AgentResult::artifact_preview` |
| `artifact_staging_grace` | 300s | Grace period before sweeping orphaned staging files |
| `max_concurrent_root_runs` | `None` | Limit on concurrent root runs |

### `Orchestrator` builder

| Function | Description |
|---|---|
| `Orchestrator::builder(config) -> OrchestratorBuilder` | Creates the builder |
| `.register_tool(Arc<dyn HostTool>)` | Registers a tool (name `[a-z][a-z0-9_]{0,63}`, must not collide with reserved names) |
| `.build().await` | Validates config/registry/skills, opens the store, takes the owner lock, runs startup recovery |

Common build errors: `INVALID_INPUT` with a `ConfigReason` (`err.reason()`), `STORE_ALREADY_OWNED` if another process holds the store.

### Session & Turn

| Function | Description |
|---|---|
| `create_session(&caller, agent_id) -> String` | Creates a Root session. `Subagent`-only agent → `PERMISSION_DENIED` |
| `create_session_with_policy(&caller, agent_id, SessionPolicy)` | Narrows tool/skill permissions (intersect only, never widen); `child_allowed_*` sets a ceiling for children |
| `submit(&caller, session_id, text) -> AcceptedTurn` | Commits input + run claim and returns immediately, without waiting for the LLM. `SESSION_BUSY`, `SESSION_ARCHIVED`, `INVALID_INPUT` (empty text) |
| `result(&caller, session_id, turn_id) -> AgentResult` | Latest-attempt projection: `status`, `steps`, `input_tokens`, `output_tokens` (`total_tokens()`), `output`, `final_message_id`, `error`, `artifact_preview`, `artifacts_next_cursor` |
| `history(&caller, session_id, PageRequest) -> Page<MessageView>` | Canonical history of one session |
| `archive_session(&caller, session_id)` | Blocks new admission; history stays readable; idempotent |
| `cancel(&caller, session_id)` | Cooperatively cancels the active run (and waiting children). The run ends `cancelled` and does not auto-resume |
| `delete_session(&caller, root_session_id)` | Permanently deletes Root + Children, messages, runs, invocations and artifacts (including files). Accepts Root only (`INVALID_INPUT` for a Child); `SESSION_BUSY` if a run is still active → `cancel` first. Irreversible, no delete event on the feed |

`RunStatus`: `Running`, `Completed`, `Failed`, `Cancelled`, `TimedOut`, `Interrupted`.
`Completed` only means execution finished; it does not assert that the business request was satisfied.

### Queries

| Function | Returns |
|---|---|
| `list_sessions(&caller, &SessionFilter, PageRequest)` | `Page<SessionSummary>` — Root sessions, filtered by `agent_id`/`archived` |
| `search_sessions(&caller, &SessionSearch, PageRequest)` | `Page<SessionSummary>` — keyword match over agent/content of the whole family |
| `get_session(&caller, session_id)` | `SessionSummary` (Root or Child) |
| `list_turns(&caller, session_id, PageRequest)` | `Page<TurnSummary>` |
| `list_runs(&caller, session_id, Option<turn_id>, PageRequest)` | `Page<RunSummary>` — every attempt, including recovery |
| `get_run(&caller, run_id)` | `RunSummary` (provider, model, steps, tokens in/out, timing) |
| `search_history(&caller, &HistorySearch, PageRequest)` | `Page<HistoryHit>` — newest first, with `snippet` |

Pagination: `PageRequest::first(limit)` or `PageRequest { cursor, limit }`; `limit` is clamped to `1..=page_size`.
Cursors are opaque, bound to the exact queried object, and are **not** authorization tokens.

### Realtime observation

| Function | Description |
|---|---|
| `subscribe(&caller, root_session_id) -> Subscription` | Feed for the whole family. `Subscription::recv().await -> Option<FeedItem>` |
| `snapshot(&caller, root_session_id) -> FamilySnapshot` | Canonical state of Root + Children (single read transaction) |
| `snapshot_refresh_interval() -> Duration` | Refresh interval while `FamilySnapshot::has_active_work()` |

`FeedItem` is `Event(ExecutionEvent)` or `ResyncRequired`. `ExecutionEvent::to_json()` returns an envelope the host can push over SSE/WebSocket:

```json
{ "event_type": "tool.progress", "root_session_id": "…", "session_id": "…",
  "run_id": "…", "state_revision": null, "timestamp_ms": 0, "payload": {} }
```

Event types: `run.started|completed|failed|cancelled|interrupted`, `step.started|ended`, `text.delta`,
`message.committed`, `tool.started|progress|completed|failed|interrupted`, `session.changed`,
`compaction.started|completed`.

An event with `state_revision = Some(..)` is a *committed invalidation* (emitted after DB commit, signalling a stale view);
an event with `None` is ephemeral (`text.delta`, `tool.progress`). The feed is best-effort, bounded, and never blocks the publisher.

**Contract for host adapters**
1. `subscribe` **before** calling `snapshot`.
2. Refresh the snapshot (debounced) until every session reaches the revision already seen.
3. On `ResyncRequired` or end of stream: discard unconfirmed previews, subscribe again, then snapshot again.
4. While `has_active_work()`: refresh every `snapshot_refresh_interval()` even if the stream is alive.

### Artifacts

| Function | Description |
|---|---|
| `list_artifacts(&caller, session_id, turn_id, PageRequest)` | `Page<ArtifactRef>` stable by `(created_at, id)` |
| `open_artifact(&caller, artifact_id) -> ArtifactDownload` | Stream bounded by `download_max_bytes`, SHA-256 verified; never exposes paths. Use `metadata()`, `read_chunk`, `read_all`, `into_reader()` |
| `backup_to(dest_dir)` | Consistent SQLite backup (`VACUUM INTO`, safe with WAL open) + committed artifacts. `dest_dir` must differ from `store_dir` and contain no DB yet |

Build an `ArtifactInput` with `primary_bytes`, `intermediate_bytes`, `from_bytes`, `from_reader`.
`ArtifactReader` offers `metadata`, `read_chunk`, `read_all`, `read_to_string`.

### Operations & recovery

| Function | Description |
|---|---|
| `resolve_interrupted_invocation(&operator, session_id, invocation_id, VerifiedToolResolution)` | Operator reconciles an `interrupted` tool (unknown outcome) |
| `shutdown()` | Stops admission → drain → cooperative cancel → quiesce/join → closes store, releases lock |
| `agent(id)`, `agent_ids()` | Read the registry |
| `tool_descriptors()` | Descriptors of registered tools |
| `provider_fingerprint(provider_id)` | Routing fingerprint without secrets |
| `base_instructions_version()` | Version of the embedded base instructions |

- `OperatorContext::new(operator_id, owner_id, workspace_id).with_resolve_capability()` — minted by the host **after** authentication; no capability by default.
- `VerifiedToolResolution::ConfirmedCompleted { outcome, evidence }` records the tool result; `ConfirmedNoEffect { evidence }` becomes a technical error. Tools are never replayed.
- Calling `shutdown()` past `shutdown_grace` while writable tasks remain: returns an *incomplete* error and **keeps** the lock. The interrupted run becomes `interrupted` and is recovered on next startup.

### Writing a `HostTool`

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
- `ToolEffect::ReadOnly`: a timeout returns the technical error `TOOL_TIMEOUT`. `ToolEffect::Mutating`: a timeout/lost connection leaves the outcome *unknown* → run `interrupted`, waiting for an operator.
- `ToolOutcome { call_id, content, metadata }`; `call_id` must be `ctx.scope().tool_call_id`. `metadata.artifact_ids` is verified by the runtime.
- `ToolError { code, message }`.

**`ToolContext`** (minted by the runtime, never built by the host):

| Function | Description |
|---|---|
| `scope() -> &ToolScope` | `owner_id`, `workspace_id`, `session_id`, `root_session_id`, `turn_id`, `run_id`, `agent_id`, `invocation_id`, `tool_call_id`, `allowed_tools`, `allowed_skills` |
| `is_cancelled()` | Check for cancellation to stop early |
| `progress(text)` / `progress_with_percent(text, pct)` | Emits `tool.progress` (≤ 1 KiB, rate-limited) |
| `artifacts().put(ArtifactInput) -> ArtifactRef` | Stores an immutable artifact |
| `artifacts().open(artifact_id) -> ArtifactReader` | Reads an in-scope artifact (same family / explicit input / checkpoint) |
| `checkpoint().current()` / `.commit(manifest_id, expected_previous_id)` | CAS checkpoint pointer for long workflows; conflict → `CHECKPOINT_CONFLICT` |

### Built-in tools

| Name | Purpose |
|---|---|
| `subagent` | Delegates work to a subagent (creates / reuses a child session) |
| `skill` | Loads skill content by id |
| `artifact_read` | Reads a UTF-8 artifact (cap `tool_output_cap_bytes`, SHA-256 verified) |

Agents must opt in via `allowed_tools`.

### Error codes

`OrchestratorError::code()` returns an `ErrorCode`; `.as_str()` is the stable contract:

| Code | Meaning |
|---|---|
| `INVALID_INPUT` | Invalid input/config (for config, `reason()` returns a `ConfigReason`) |
| `PERMISSION_DENIED` | Insufficient permission / wrong scope |
| `NOT_FOUND` | Object does not exist or is out of scope |
| `SESSION_BUSY` | Session already has an active run |
| `SESSION_ARCHIVED` | Session is archived |
| `RECOVERY_BLOCKED` | Recovery is blocked, operator needed |
| `PROVIDER_CONFIG_CHANGED` | Provider routing changed since the run started |
| `STORE_ALREADY_OWNED` | Another process holds the owner lock |
| `SHUTTING_DOWN` | Orchestrator is shutting down or already shut down |
| `UNAVAILABLE` | A dependency is temporarily unavailable |
| `INTERNAL` | Internal error (sanitized) |

`ErrorCode` is `#[non_exhaustive]`: always include a `_` arm.

## Security model

- Every API checks `owner_id`/`workspace_id`; tool scope is minted by the runtime, never taken from the LLM.
- Secrets live only in `SecretString`, never in logs/events/errors.
- Events never contain tool arguments or results.
- Artifacts never expose filesystem paths; SHA-256 integrity is verified on read.
- Project instructions and skill content are untrusted; they do not replace base instructions or execution permissions.
- Details: [docs/10-security-operations.md](docs/10-security-operations.md).

## Repo layout

```
src/
  orchestrator/  public handle + builder (session, query, observe, artifacts, lifecycle)
  runtime/       agent loop, compaction, settle
  executor/      tool execution, timeout, schema, permission
  subagent/      subagent tool, child admission
  store/         SQLite execution store, recovery
  feed/          event feed, subscription, redaction
  artifact/      ArtifactStore, backup, cursor
  gateway/       adapter for the `llm` crate
  agent.rs config.rs tool.rs skill.rs prompt.rs types.rs error.rs ...
migrations/      SQL migrations (0001–0004)
examples/        host, live_flow, load_skills, video_summary
tests/           m0–m7 (fake + live) and read_api
docs/            design, ADRs, evidence, specs
spikes/g0/       standalone spike (not part of the library)
```

## Running the examples

```sh
cargo run --example host        # build an Orchestrator from a host Tokio runtime (no real LLM call)
```

### `live_flow` — calls a real model

Fill in `G0_OPENAI_BASE_URL`, `G0_OPENAI_MODEL`, `G0_OPENAI_API_KEY` in `.env.live`
(copy from [.env.live.example](.env.live.example); this file is already in `.gitignore`):

```sh
cp .env.live.example .env.live
cargo run --example live_flow
cargo run --example live_flow -- --env-file .env.live --output-dir target/live-flow
```

[Example live_flow](examples/live_flow.rs): Root delegates to analyst → analyst calls `lookup_code` with progress →
Root replies → Root follows up with the same child session. Provider config is read only from the chosen file
(`--env-file`, `LIVE_ENV_FILE`, default `.env.live`); shell `G0_OPENAI_*` variables do not override it.
JSONL log at `target/live-flow/<UUID>/events.jsonl` (API key masked). Exits 0 when both turns completed,
the tool ran and the child session was reused; each turn waits at most 240 seconds.

### `load_skills` — agent loads a skill via the `skill(id)` tool

[Example load_skills](examples/load_skills.rs): `OrchestratorConfig::with_skills_dir` points to
[docs/examples/skills](docs/examples/skills) (scanned once at `build()`); the `reviewer` agent has the `skill` tool and
`with_allowed_skills(["rust-review"])`. The agent is asked to load the `rust-review` skill and list the workflow steps.

```sh
cp .env.live.example .env.live   # if not present yet; fill in G0_OPENAI_*
cargo run --example load_skills
cargo run --example load_skills -- --env-file .env.live
```

Exits 0 when the turn completed and the `skill` invocation is `Completed`; the result is printed to stdout.
The temporary store lives in the temp directory and is removed after the run.

### `video_summary` — video → transcript → summary → Markdown

[Example video_summary](examples/video_summary.rs): the `coordinator` root has only the `subagent` tool;
`explorer` fetches the transcript (yt-dlp) and summarizes; `writer` writes Markdown (`markdown_create`, `markdown_append_section`, `markdown_read`).

```sh
brew install yt-dlp ffmpeg
cargo run --example video_summary -- "<URL>" --lang en
# Options: --lang vi,en  --env-file .env.live  --output-dir target/video-summary  --json
cargo test --example video_summary
```

Output: `target/video-summary/<UUID>/markdown/summary.md` and `events.jsonl`. YouTube often returns 429 for auto-translated
subtitles, so the tool prefers `<lang>-orig`; videos without subtitles report `NO_TRANSCRIPT`. Small models as root may call `subagent` with wrong arguments; check `tool.failed` in the log.

## Development & testing

```sh
scripts/check.sh          # fast: fmt --check + clippy (-D warnings) + lib/integration tests
scripts/check.sh full     # + build examples + doc-test
cargo test                # deterministic tests (fake gateway, real SQLite/filesystem)

# Live tests against a real provider (needs .env.live)
cargo test --test m1_live_provider -- --ignored --nocapture   # likewise m2…m7_live_provider
cargo test --test m8_live_hard_flow -- --ignored --nocapture  # hard flow: 2 children, tool retry, restart, recall
cargo test --test m8_live_max_tokens -- --ignored --nocapture # tool output cap, per-agent compaction, max_output_tokens
```

`scripts/check.sh` output is condensed; set `CHECK_MAX_LINES` to show more error lines.

| Test | Scope |
|---|---|
| `m0_library_shell` | Builder, config, registry validation |
| `m1_single_agent` | Admission, single-agent run, `result`/`history` |
| `m2_tools_skills` | Tool loop, skill, prompt |
| `m3_parent_child` | `subagent`, child session |
| `m4_continuous` | Step cap, compaction, stream assembly |
| `m5_recovery`, `m5_process` | Recovery, cancel, shutdown, owner lock |
| `m6_observation` | `subscribe`/`snapshot` |
| `m7_artifacts` | Artifacts, quota, backup, fault injection |
| `read_api` | Query API |
| `m8_live_hard_flow` (live) | Root → 2 children, dependent tool chain, transient-error retry, engine restart, follow-up on same child |
| `m8_live_max_tokens` (live) | `tool_output_cap_bytes`, `tool_result_hard_cap_bytes`, compaction by agent `context_limit_tokens`, `max_output_tokens` |

Per-milestone evidence: [docs/evidence/](docs/evidence/) · Implementation spec: [docs/specs/v5-implementation-m0-m7.md](docs/specs/v5-implementation-m0-m7.md).

## Documentation

| Document | Content |
|---|---|
| [docs/README.md](docs/README.md) | Design index |
| [01-architecture](docs/01-architecture.md) | Architecture, modules |
| [05-tools](docs/05-tools.md) · [06-delegation](docs/06-delegation.md) | Tools, Root ↔ Subagent communication |
| [09-persistence](docs/09-persistence.md) | SQLite schema, recovery |
| [13-adrs](docs/13-adrs.md) · [14-glossary](docs/14-glossary.md) | ADRs, glossary |
| [16-streaming-events](docs/16-streaming-events.md) | Events, snapshot/resync |
| [17-agent-results-artifacts](docs/17-agent-results-artifacts.md) | Artifacts |
| [18-embeddable-library](docs/18-embeddable-library.md) | Public contract |
| [Config example](docs/examples/agents.yaml) | Sample host-side DTO (the core does not parse YAML) |

The [review report](docs/research/design-review-report.md) is a historical snapshot from before v5.0 was finalized.

## License

Dual-licensed under MIT OR Apache-2.0 (see `LICENSE-MIT`, `LICENSE-APACHE`).

