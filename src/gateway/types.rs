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

/// Tham số sinh của model; `None` = không gửi (provider tự chọn), riêng `max_output_tokens`
/// `None` = trần mặc định của gateway.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ModelParams {
    pub max_output_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub top_k: Option<u32>,
}

impl ModelParams {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.max_output_tokens == Some(0) {
            return Err("max_output_tokens must be > 0 when set");
        }
        if self.temperature.is_some_and(|t| !(0.0..=2.0).contains(&t)) {
            return Err("temperature must be within 0.0..=2.0");
        }
        if self.top_p.is_some_and(|p| !(p > 0.0 && p <= 1.0)) {
            return Err("top_p must be within (0.0, 1.0]");
        }
        if self.top_k == Some(0) {
            return Err("top_k must be > 0 when set");
        }
        Ok(())
    }

    /// Khoá ổn định để tái sử dụng client theo tham số.
    pub(crate) fn cache_key(&self) -> String {
        format!(
            "{:?}|{:?}|{:?}|{:?}",
            self.max_output_tokens,
            self.temperature.map(f32::to_bits),
            self.top_p.map(f32::to_bits),
            self.top_k
        )
    }
}

/// Request đã resolve theo agent: `system` = mandatory base + prompt của agent (+ project
/// instructions + skill catalog được phép).
#[derive(Debug, Clone, PartialEq)]
pub struct ModelRequest {
    pub provider_id: String,
    pub model_id: String,
    pub system: String,
    pub messages: Vec<ModelMessage>,
    pub tools: Vec<ModelTool>,
    pub params: ModelParams,
}

/// Usage do provider báo hoặc được gateway ước lượng khi stream không kèm usage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModelUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

impl ModelUsage {
    pub const fn new(input_tokens: u64, output_tokens: u64) -> Self {
        Self {
            input_tokens: Some(input_tokens),
            output_tokens: Some(output_tokens),
        }
    }

    /// Ước lượng token đầu vào và đầu ra từ request/response (~4 byte/token) khi provider
    /// không trả số liệu usage trực tiếp trong stream.
    pub fn estimate(request: &ModelRequest, response: &ModelResponse) -> Self {
        Self {
            input_tokens: Some(crate::compaction::estimate_tokens(request) as u64),
            output_tokens: Some(crate::compaction::estimate_output_tokens(response) as u64),
        }
    }

    /// Giữ số đo của provider nếu có, phần thiếu điền bằng ước lượng từ request/response.
    pub fn or_estimated(
        usage: Option<Self>,
        request: &ModelRequest,
        response: &ModelResponse,
    ) -> Self {
        let est = Self::estimate(request, response);
        match usage {
            Some(u) => Self {
                input_tokens: u.input_tokens.or(est.input_tokens),
                output_tokens: u.output_tokens.or(est.output_tokens),
            },
            None => est,
        }
    }

    /// Tổng token (`input_tokens + output_tokens`) đã ghi nhận.
    pub fn total_tokens(&self) -> u64 {
        self.input_tokens
            .unwrap_or_default()
            .saturating_add(self.output_tokens.unwrap_or_default())
    }
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
