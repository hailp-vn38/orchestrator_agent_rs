use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::error::GatewayError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelRole {
    User,
    Assistant,
    /// Kết quả của một tool call trước đó (`tool_call_id` + `tool_name` được điền).
    Tool,
}

/// Tool call do model đề xuất; `id` là provider call ID (chỉ duy nhất trong một response).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelMessage {
    pub role: ModelRole,
    pub text: String,
    /// Chỉ dùng với `Assistant`: các tool call của message đó.
    pub tool_calls: Vec<ModelToolCall>,
    /// Chỉ dùng với `Tool`: provider call ID mà result này ghép với.
    pub tool_call_id: Option<String>,
    /// Chỉ dùng với `Tool`: tên tool đã được gọi.
    pub tool_name: Option<String>,
}

impl ModelMessage {
    pub fn text(role: ModelRole, text: impl Into<String>) -> Self {
        Self {
            role,
            text: text.into(),
            tool_calls: Vec::new(),
            tool_call_id: None,
            tool_name: None,
        }
    }

    pub fn assistant_with_calls(text: impl Into<String>, calls: Vec<ModelToolCall>) -> Self {
        Self {
            tool_calls: calls,
            ..Self::text(ModelRole::Assistant, text)
        }
    }

    pub fn tool_result(
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            tool_call_id: Some(call_id.into()),
            tool_name: Some(tool_name.into()),
            ..Self::text(ModelRole::Tool, text)
        }
    }
}

/// Tool được cung cấp cho model ở request này (đã lọc theo effective permissions).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelTool {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// Request đã resolve theo agent: `system` = mandatory base + prompt của agent (+ project
/// instructions + skill catalog được phép).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRequest {
    pub provider_id: String,
    pub model_id: String,
    pub system: String,
    pub messages: Vec<ModelMessage>,
    pub tools: Vec<ModelTool>,
}

/// Usage do provider báo; `None` ở cấp response nghĩa là provider không trả số liệu
/// (runtime không bịa số và không coi đó là lỗi).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModelUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelResponse {
    pub text: String,
    pub finish_reason: String,
    /// Chỉ có khi toàn response đã `Done` và mọi arguments là JSON hợp lệ.
    pub tool_calls: Vec<ModelToolCall>,
    pub usage: Option<ModelUsage>,
}

/// Preview ephemeral trong lúc stream. Không phải trạng thái bền vững: lỗi/hủy giữa chừng
/// khiến mọi preview bị bỏ, và `ToolCallReady` **không** cho phép execute tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    TextDelta(String),
    /// Một call đã đủ arguments hợp lệ nhưng response chưa `Finished` (chưa được xác nhận).
    ToolCallReady {
        id: String,
        name: String,
    },
}

/// Nơi nhận preview; gọi đồng bộ theo đúng thứ tự provider phát.
pub type StreamSink<'a> = &'a (dyn Fn(StreamEvent) + Send + Sync);

#[async_trait]
pub trait LlmGateway: Send + Sync {
    /// Một lượt model hoàn chỉnh. Chỉ trả `Ok` khi toàn response đã `Done`; mọi
    /// lỗi/hủy/timeout giữa chừng loại bỏ phần text và call đã nhận. `sink` chỉ nhận preview.
    async fn complete(
        &self,
        request: &ModelRequest,
        cancel: &CancellationToken,
        sink: StreamSink<'_>,
    ) -> Result<ModelResponse, GatewayError>;
}

