# 11 — End-to-end Flows (MVP)

## 11.1. Chat → Root → LLM

```mermaid
sequenceDiagram
    autonumber
    participant User
    participant API as Host transport
    participant RT as AgentRuntime
    participant Store as SessionStore
    participant PB as PromptBuilder
    participant L as LlmGateway
    User->>API: POST /sessions/{id}/messages
    API->>RT: submit(session_id, text)
    RT->>Store: durable input + start_run (unique admission)
    RT-->>API: Accepted(session_id, turn_id)
    API-->>User: 202 Accepted
    Note over RT: Managed execution survives HTTP disconnect
    RT->>PB: build(session_id)
    PB->>Store: load history
    PB-->>RT: system + messages + tools
    RT->>L: completion request
    L-->>RT: final text
    RT->>Store: append assistant final + finish run
    Note over User,Store: User fetches session status/history after completion
```

## 11.2. Root → Child → Resume (foreground)

```mermaid
sequenceDiagram
    autonumber
    participant R as Root Runtime
    participant T as ToolExecutor
    participant TM as TaskManager
    participant S as SessionStore
    participant C as Child Runtime
    R->>S: persist subagent tool call + invocation
    R->>T: execute subagent(reviewer, prompt)
    T->>TM: delegate(request, borrowed runtime)
    TM->>S: atomic admit_child_turn(parent_invocation_id, child, prompt)
    S-->>TM: durable child_session_id + child_turn_id
    TM->>C: schedule/wait(child_turn_id)
    C->>S: persist child messages + run
    C-->>TM: final text
    TM-->>T: {session_id, completed, output}
    T-->>R: ToolOutcome
    R->>S: persist parent tool result + invocation completion
    R->>R: assess text
    R->>T: subagent(reviewer, session_id, followup)
    T->>TM: delegate/resume
    TM->>S: atomic admit new child_turn_id (same child session)
    TM->>C: schedule/wait(new child_turn_id)
    C-->>TM: final text
    TM-->>T: SubagentOutput
    T-->>R: ToolOutcome
    R->>S: persist second parent tool result
```

Mỗi delegation là **một tool invocation**. Không có bảng `tasks` và không chuyển output child trực tiếp sang Root session. `AgentRuntime` là nơi ghi transcript của đúng session.

## 11.3. Skill loading during child run

```mermaid
sequenceDiagram
    autonumber
    participant C as Child AgentRuntime
    participant T as ToolExecutor
    participant SM as SkillManager
    participant DB as SessionStore
    C->>DB: persist skill tool call
    C->>T: execute skill(id=rust-review)
    T->>SM: load(child_session_id, rust-review)
    SM->>SM: permission + safe read SKILL.md
    SM-->>T: skill_content + references
    T-->>C: ToolOutcome
    C->>DB: persist tool result in child history
    C->>C: continue with skill instructions
```

## 11.4. Crash giữa side-effect tool và result

```text
Persist assistant tool call + invocation(requested)
  → execute tool (write_file/shell)
  → process crashes BEFORE durable tool result
  → startup scans stale run and pending invocation
  → mark invocation interrupted; do NOT replay / pretend succeeded
  → block for reconciliation; once safe, resume same turn in a new attempt
```

## 11.5. Errors

- Invalid tool input / permission denied → ToolError, LLM có thể sửa request nếu run vẫn còn budget.
- Child failed/provider operation timeout/cancelled → ToolError; không có tổng `child_timeout_s`, giữ child session ID nếu đã tạo.
- Child trả text sai ý → **không là runtime error**; Root có thể follow up same child session.
- Child không có text → placeholder, Root tự đánh giá.
- Context gần đầy → automatic summary compaction + bounded overflow retry; giữ full original history, trả recoverable error nếu compact thất bại.
- Incomplete side-effect call sau crash → yêu cầu reconciliation; không chạy lại mù quáng.

## 11.6. Prompt lifecycle

```text
Resolve Agent Definition + permission snapshot
  → PromptBuilder builds base/agent/project instructions
  → filter Skill Catalog metadata
  → load Session History (including previously loaded skill results)
  → attach tool definitions
  → LlmGateway converts request for chosen provider
  → Model emits final text or tool calls
  → AgentRuntime writes transcript and loops while model asks for tools
  → auto-compaction/recovery at safe boundaries; no default step cap
  → final text / explicit cancel / terminal error
```

## 11.7. Long-running + safe restart

```mermaid
sequenceDiagram
    autonumber
    participant API as Client
    participant RT as Managed AgentRuntime
    participant S as SQLite
    API->>RT: submit(session, objective)
    RT->>S: persist input + run claim
    RT-->>API: 202 Accepted
    loop Model continues requesting tools
        RT->>S: persist tool calls/results
        opt Near context limit
            RT->>S: persist compaction checkpoint
        end
    end
    Note over RT,S: Server restart may interrupt
    RT->>S: scan stale running run
    alt Safe to resume
        RT->>S: mark interrupted; create same turn new attempt
        RT->>RT: resume session runner
    else Unknown side effect
        RT->>S: mark invocation interrupted; block
    end
```

Xem [15](15-long-running-sessions.md) cho admission/stop/recovery.

## 11.8. Sau MVP

Background Subagent notification + durable parent inbox, parallel child jobs và provider-native compaction. Local ArtifactStore đã thuộc MVP. Summary compaction và restart recovery thuộc lõi MVP.

## 11.9. SSE streaming & snapshot/resync (MVP)

```mermaid
sequenceDiagram
    autonumber
    participant U as UI
    participant E as Axum SSE
    participant B as ExecutionFeed
    participant R as AgentRuntime
    participant D as SQLite
    U->>E: GET /sessions/root/events?include_children=true
    E->>B: subscribe live before snapshot
    E-->>U: stream.connected
    U->>D: GET /sessions/root/snapshot
    D-->>U: family + per-session revisions
    R->>D: persist tool requested + bump revision
    R->>B: tool.started (committed revision)
    B-->>U: tool.started
    R->>B: text.delta / tool.progress (ephemeral)
    B-->>U: text.delta / tool.progress
    R->>D: persist result + bump revision
    R->>B: tool.completed (committed revision)
    B-->>U: tool.completed
    Note over U,E: Lag/reconnect -> re-subscribe + snapshot; active runs -> periodic refresh even if connected
```

Child Tool/LLM events sử dụng session riêng nhưng cùng root family. Các committed event revisions **invalidation → debounce snapshot refresh**, không merge state patches. SQLite không lưu token fragments; SSE disconnect không điều khiển run. Xem [16](16-streaming-events.md).
