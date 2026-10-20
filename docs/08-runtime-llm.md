# 08 — Shared AgentRuntime & LlmGateway

## 8.1. Runtime loop

```mermaid
flowchart TD
    A[AgentRuntime.run(session, input)] --> B[SessionStore.start_run - unique index admission]
    B --> C[Persist input message]
    C --> D[PromptBuilder.build]
    D --> E[LlmGateway.complete]
    E --> F{Tool calls?}
    F -->|No| G[Persist final + finish run]
    G --> H[Return final text]
    F -->|Yes| I[Persist assistant calls + requested invocations]
    I --> J[ToolExecutor.execute]
    J --> K[Persist tool result/error + finish invocation]
    K --> L{Budget left?}
    L -->|Yes| D
    L -->|No| M[Fail run with technical error]
```

`AgentRuntime` dùng chung cho Root và mọi Subagent, sở hữu LLM/tool loop **và mọi transcript writes**; `ToolExecutor` chỉ thực thi tools và trả outcomes. Nếu gặp dangling invocation sau restart, không gọi LLM lại khi tool outcome chưa được reconcile.

## 8.2. Interface

```rust
pub struct RunRequest {
    pub session_id: String,
    pub input: String,
}

pub struct RunResult {
    pub run_id: String,
    pub session_id: String,
    pub final_text: String,
}

pub struct AgentRuntime { /* injected deps */ }

impl AgentRuntime {
    pub async fn run(&self, req: RunRequest) -> Result<RunResult, RuntimeError> {
        todo!()
    }
}
```

`Completed` là execution success, không hàm ý semantic quality. Empty final child text được biến thành fallback string **tại `subagent` boundary**, history lưu đúng trạng thái text gốc.

## 8.3. LlmGateway — seam nhỏ

```rust
pub struct ModelRef {
    pub provider_id: String,
    pub model_id: String,
}

pub struct CompletionRequest {
    pub model: ModelRef,
    pub context: PromptContext,
}

pub struct CompletionResult {
    pub final_text: String,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Option<TokenUsage>,
}
```

Adapter chuyển mô hình messages/tool calls/results sang crate `llm`. Tránh xây hệ provider wrappers mới trên một thư viện vốn đã hỗ trợ đa provider. Trong test, có thể thay adapter bằng fake LLM; không cần trait cho mỗi provider.

## 8.4. Multi-provider

- `(provider_id, model_id)` là định danh model, có thể khác nhau cho parent/child.
- Key lấy từ secrets/env; không lưu vào DB/log/prompt.
- Chỉ cho phép model có tool-calling capability nếu agent dùng tools; không tự động fallback provider.
- Tool call/result ID phải được bảo toàn qua history và mapping riêng cho từng provider.

## 8.5. Tokio và resource limits

- MVP foreground: `TaskManager.delegate` await child run; **không spawn untracked tasks**. Runtime có thể box recursive future tại nhánh `subagent` để thỏa Rust async sizing.
- `CancellationToken` và `tokio::time::timeout` áp dụng cho run/tool với propagation parent → child. Không giữ SQLite transaction qua LLM latency.
- Budget: max steps, depth=1, children per root run, wall-clock timeout và output display bytes/lines. Global child semaphore và parallelism chỉ cần thêm khi triển khai concurrent children.
- Usage có thể ước lượng nếu provider không trả số liệu; ghi rõ measured/estimated.

## 8.6. Provider spike bắt buộc

Trước khi phụ thuộc sâu crate `llm`, khóa phiên bản rồi kiểm chứng với **OpenAI + một provider khác**:

1. Text completion và multi-turn history.
2. Tool call → tool result → continuation (giữ nguyên IDs).
3. Provider-specific error mapping, tool capability, timeout/cancellation.
4. Token usage; streaming khảo sát cho giai đoạn sau.

Ví dụ Rust trong tài liệu là **illustrative interface**, chưa phải code đã compile.
