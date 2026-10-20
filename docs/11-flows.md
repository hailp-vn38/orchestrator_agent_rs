# 11 — End-to-end Flows (MVP)

## 11.1. Chat → Root → LLM

```mermaid
sequenceDiagram
    autonumber
    participant User
    participant API as Axum
    participant RT as AgentRuntime
    participant Store as SessionStore
    participant PB as PromptBuilder
    participant L as LlmGateway
    User->>API: POST /sessions/{id}/messages
    API->>RT: run(session_id, text)
    RT->>Store: start_run (unique admission)
    RT->>Store: append user message
    RT->>PB: build(session_id)
    PB->>Store: load history
    PB-->>RT: system + messages + tools
    RT->>L: completion request
    L-->>RT: final text
    RT->>Store: append assistant final + finish run
    RT-->>API: response
    API-->>User: result
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
    TM->>S: create child + link invocation.child_session_id
    TM->>C: run(child_session, prompt)
    C->>S: persist child messages + run
    C-->>TM: final text
    TM-->>T: {session_id, completed, output}
    T-->>R: ToolOutcome
    R->>S: persist parent tool result + invocation completion
    R->>R: assess text
    R->>T: subagent(reviewer, session_id, followup)
    T->>TM: delegate/resume
    TM->>S: verify ownership + link new invocation to same child
    TM->>C: run(child_session, followup)
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
  → startup marks run/tool invocation interrupted
  → do NOT replay / pretend succeeded
  → operator or Root/User reconciles outcome before LLM continuation
```

## 11.5. Errors

- Invalid tool input / permission denied → ToolError, LLM có thể sửa request nếu run vẫn còn budget.
- Child failed/timeout/cancelled → ToolError; giữ child session ID nếu đã tạo.
- Child trả text sai ý → **không là runtime error**; Root có thể follow up same child session.
- Child không có text → placeholder, Root tự đánh giá.
- Context vượt giới hạn → error có thể khôi phục; không tự tóm tắt/xóa lịch sử trong MVP.
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
  → AgentRuntime writes transcript and loops until finished/budget exhausted
```

## 11.7. Sau MVP

Background subagent notification + synthetic parent inbox, parallel child execution, advanced compaction and artifact store. Không có đường chạy ẩn nào cho các features này trong MVP.
