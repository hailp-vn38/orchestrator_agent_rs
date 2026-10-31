use std::pin::Pin;

use futures::{Stream, StreamExt};
use llm::chat::StreamChunk;
use llm::error::LLMError;

use super::error::{error_detail, map_error, GatewayError};
use super::types::{ModelResponse, ModelToolCall, StreamEvent, StreamSink};

pub(crate) type RawStream = Pin<Box<dyn Stream<Item = Result<StreamChunk, LLMError>> + Send>>;

const THINK_OPEN: &str = "<think>";
const THINK_CLOSE: &str = "</think>";

/// Loại bỏ mọi khối `<think>...</think>` (kể cả khối `<think>` bị cắt cụt cuối chuỗi do chạm
/// `max_output_tokens` hoặc phần suy nghĩ đầu chuỗi chỉ có thẻ đóng `</think>`).
pub fn strip_thinking_blocks(text: &str) -> String {
    if !text.contains(THINK_OPEN) && !text.contains(THINK_CLOSE) {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;

    // Trường hợp prompt template của model mở sẵn `<think>` nên phần sinh ra bắt đầu bằng
    // nội dung suy nghĩ rồi đóng `</think>` mà không có thẻ mở `<think>` phía trước.
    if let Some(close_idx) = rest.find(THINK_CLOSE) {
        let open_idx = rest.find(THINK_OPEN);
        if open_idx.is_none_or(|o| close_idx < o) {
            rest = &rest[close_idx + THINK_CLOSE.len()..];
            rest = rest.trim_start();
        }
    }

    while let Some(open_idx) = rest.find(THINK_OPEN) {
        let prefix = &rest[..open_idx];
        if !(out.is_empty() && prefix.trim().is_empty()) {
            out.push_str(prefix);
        }
        let after_open = &rest[open_idx + THINK_OPEN.len()..];
        match after_open.find(THINK_CLOSE) {
            Some(close_idx) => {
                rest = &after_open[close_idx + THINK_CLOSE.len()..];
                if out.is_empty() {
                    rest = rest.trim_start();
                } else if out.ends_with('\n') {
                    rest = rest.trim_start_matches(['\r', '\n']);
                } else if out.ends_with(' ') && rest.starts_with(' ') {
                    rest = rest.trim_start_matches(' ');
                }
            }
            None => {
                // Khối `<think>` chưa đóng (bị cắt bởi max_output_tokens) -> bỏ toàn bộ phần còn lại.
                rest = "";
                break;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Bộ lọc `<think>...</think>` theo từng mảnh stream để không phát suy nghĩ nháp ra `TextDelta`.
pub(crate) struct ThinkStreamFilter {
    buf: String,
    in_think: bool,
    emitted_any: bool,
    trim_after_think: bool,
}

impl ThinkStreamFilter {
    pub(crate) fn new() -> Self {
        Self {
            buf: String::new(),
            in_think: false,
            emitted_any: false,
            trim_after_think: false,
        }
    }

    pub(crate) fn push(&mut self, chunk: &str) -> String {
        self.buf.push_str(chunk);
        let mut out = String::new();
        loop {
            if self.in_think {
                if let Some(idx) = self.buf.find(THINK_CLOSE) {
                    self.buf.drain(..idx + THINK_CLOSE.len());
                    self.in_think = false;
                    self.trim_after_think = true;
                    continue;
                }
                let keep = partial_tag_suffix(&self.buf, THINK_CLOSE);
                let drop_len = self.buf.len() - keep;
                if drop_len > 0 {
                    self.buf.drain(..drop_len);
                }
                break;
            }

            if self.trim_after_think {
                let trimmed = if !self.emitted_any {
                    self.buf.trim_start()
                } else {
                    self.buf.trim_start_matches(['\r', '\n'])
                };
                let trimmed_len = self.buf.len() - trimmed.len();
                if trimmed_len > 0 {
                    self.buf.drain(..trimmed_len);
                }
                if self.buf.is_empty() {
                    break;
                }
                self.trim_after_think = false;
            }

            if let Some(idx) = self.buf.find(THINK_OPEN) {
                let prefix = &self.buf[..idx];
                if !prefix.is_empty()
                    && !(out.is_empty() && !self.emitted_any && prefix.trim().is_empty())
                {
                    out.push_str(prefix);
                    self.emitted_any = true;
                }
                self.buf.drain(..idx + THINK_OPEN.len());
                self.in_think = true;
                continue;
            }

            let hold = partial_tag_suffix(&self.buf, THINK_OPEN);
            let safe_len = self.buf.len() - hold;
            if safe_len == 0 {
                break;
            }
            out.push_str(&self.buf[..safe_len]);
            self.emitted_any = true;
            self.buf.drain(..safe_len);
            break;
        }
        out
    }

    pub(crate) fn flush(&mut self) -> String {
        if self.in_think || self.buf.is_empty() {
            self.buf.clear();
            return String::new();
        }
        let tail = std::mem::take(&mut self.buf);
        if self.trim_after_think {
            self.trim_after_think = false;
            if !self.emitted_any {
                return tail.trim_start().to_owned();
            }
            return tail.trim_start_matches(['\r', '\n']).to_owned();
        }
        tail
    }
}

/// Độ dài hậu tố dài nhất của `text` trùng với tiền tố của `tag` (nhỏ hơn `tag.len()`).
fn partial_tag_suffix(text: &str, tag: &str) -> usize {
    let max_len = (tag.len() - 1).min(text.len());
    for len in (1..=max_len).rev() {
        let start = text.len() - len;
        if text.is_char_boundary(start) && tag.starts_with(&text[start..]) {
            return len;
        }
    }
    0
}

/// Gom stream thành một response đã `Done`. Stream cụt (không `Done`) hoặc kết thúc rỗng
/// (không có cả text lẫn tool call, thường do thinking ngốn hết `max_output_tokens`) là lỗi protocol.
/// Tool call chỉ được trả khi **toàn** response kết thúc thành công: lỗi/hủy/timeout sau
/// `ToolUseComplete` vẫn loại bỏ mọi call đã nhận (không có gì để dispatch).
#[allow(dead_code)]
pub(crate) async fn collect(
    raw: RawStream,
    sink: StreamSink<'_>,
) -> Result<ModelResponse, GatewayError> {
    collect_with_detail(raw, sink).await.map_err(|(e, _)| e)
}

pub(crate) async fn collect_with_detail(
    mut raw: RawStream,
    sink: StreamSink<'_>,
) -> Result<ModelResponse, (GatewayError, String)> {
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    let mut think_filter = ThinkStreamFilter::new();
    loop {
        match raw.next().await {
            None => {
                return Err((
                    GatewayError::Protocol,
                    format!(
                        "SSE stream ended prematurely before Done chunk (received {} chars, {} tool calls)",
                        text.len(),
                        tool_calls.len()
                    ),
                ))
            }
            Some(Err(e)) => {
                let detail =
                    error_detail(&e).unwrap_or_else(|| "SSE stream chunk error".to_owned());
                return Err((map_error(&e), detail));
            }
            Some(Ok(chunk)) => {
                sink(StreamEvent::Activity);
                match chunk {
                    StreamChunk::Text(t) => {
                        if !t.is_empty() {
                            let visible = think_filter.push(&t);
                            if !visible.is_empty() {
                                sink(StreamEvent::TextDelta(visible));
                            }
                        }
                        text.push_str(&t);
                    }
                    StreamChunk::ToolUseStart { .. } | StreamChunk::ToolUseInputDelta { .. } => {}
                    StreamChunk::ToolUseComplete { tool_call, .. } => {
                        let raw_args = tool_call.function.arguments.trim();
                        let arguments = if raw_args.is_empty() {
                            serde_json::json!({})
                        } else {
                            serde_json::from_str(raw_args).map_err(|_| {
                                (
                                    GatewayError::Protocol,
                                    format!(
                                        "tool '{}' returned malformed JSON arguments",
                                        tool_call.function.name
                                    ),
                                )
                            })?
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
                        let tail = think_filter.flush();
                        if !tail.is_empty() {
                            sink(StreamEvent::TextDelta(tail));
                        }
                        let clean_text = strip_thinking_blocks(&text);
                        if clean_text.trim().is_empty() && tool_calls.is_empty() {
                            let detail = if !text.trim().is_empty() {
                                format!(
                                    "model output contained only <think> reasoning ({} chars, stop_reason='{stop_reason}') with no final text or tool calls",
                                    text.len()
                                )
                            } else {
                                format!(
                                    "model finished with empty text and no tool calls (stop_reason='{stop_reason}')"
                                )
                            };
                            return Err((GatewayError::Protocol, detail));
                        }
                        return Ok(ModelResponse {
                            text: clean_text,
                            finish_reason: stop_reason,
                            tool_calls,
                            // `llm` 1.3.8 không phát usage trong stream: thiếu usage không phải lỗi.
                            usage: None,
                        });
                    }
                }
            }
        }
    }
}
