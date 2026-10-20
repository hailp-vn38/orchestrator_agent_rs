use std::pin::Pin;

use futures::{Stream, StreamExt};
use llm::chat::StreamChunk;
use llm::error::LLMError;

use super::error::{map_error, GatewayError};
use super::types::{ModelResponse, ModelToolCall, StreamEvent, StreamSink};

pub(crate) type RawStream = Pin<Box<dyn Stream<Item = Result<StreamChunk, LLMError>> + Send>>;

/// Gom stream thành một response đã `Done`. Stream cụt (không `Done`) là lỗi protocol.
/// Tool call chỉ được trả khi **toàn** response kết thúc thành công: lỗi/hủy/timeout sau
/// `ToolUseComplete` vẫn loại bỏ mọi call đã nhận (không có gì để dispatch).
pub(crate) async fn collect(
    mut raw: RawStream,
    sink: StreamSink<'_>,
) -> Result<ModelResponse, GatewayError> {
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    loop {
        match raw.next().await {
            None => return Err(GatewayError::Protocol),
            Some(Err(e)) => return Err(map_error(&e)),
            Some(Ok(chunk)) => match chunk {
                StreamChunk::Text(t) => {
                    if !t.is_empty() {
                        sink(StreamEvent::TextDelta(t.clone()));
                    }
                    text.push_str(&t);
                }
                StreamChunk::ToolUseStart { .. } | StreamChunk::ToolUseInputDelta { .. } => {}
                StreamChunk::ToolUseComplete { tool_call, .. } => {
                    let raw_args = tool_call.function.arguments.trim();
                    let arguments = if raw_args.is_empty() {
                        serde_json::json!({})
                    } else {
                        serde_json::from_str(raw_args).map_err(|_| GatewayError::Protocol)?
                    };
                    sink(StreamEvent::ToolCallReady {
                        id: tool_call.id.clone(),
                        name: tool_call.function.name.clone(),
                    });
                    tool_calls.push(ModelToolCall {
                        id: tool_call.id,
                        name: tool_call.function.name,
                        arguments,
                    });
                }
                StreamChunk::Done { stop_reason } => {
                    return Ok(ModelResponse {
                        text,
                        finish_reason: stop_reason,
                        tool_calls,
                        // `llm` 1.3.8 không phát usage trong stream: thiếu usage không phải lỗi.
                        usage: None,
                    });
                }
            },
        }
    }
}
