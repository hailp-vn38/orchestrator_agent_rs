//! Chuẩn hóa lỗi và giới hạn output trước khi runtime persist.
use super::{Settled, ToolLimits, CODE_TOO_LARGE};
use crate::tool::{ToolError, ToolOutcome};
use serde_json::json;

const MAX_ERROR_CHARS: usize = 1000;
const MAX_METADATA_BYTES: usize = 16 * 1024;

impl Settled {
    pub(crate) fn failure(code: &str, message: &str) -> Self {
        let message = sanitize_message(message);
        Self {
            failed: true,
            model_text: format!("error {code}: {message}"),
            result_json: json!({ "error": { "code": code, "message": message } }).to_string(),
            error_code: Some(code.to_owned()),
        }
    }
}

impl ToolLimits {
    /// Chốt một outcome thành `Settled` (hard cap, model cap, metadata cap). Dùng chung cho
    /// tool chạy bình thường và outcome do operator xác minh.
    pub(crate) fn settle(&self, result: Result<ToolOutcome, ToolError>) -> Settled {
        match result {
            Err(e) => {
                let code = if valid_code(&e.code) {
                    e.code.as_str()
                } else {
                    "TOOL_ERROR"
                };
                Settled::failure(code, &e.message)
            }
            Ok(outcome) => {
                let len = outcome.content.len();
                if len > self.hard_cap_bytes {
                    return Settled::failure(
                        CODE_TOO_LARGE,
                        &format!(
                            "tool output is {len} bytes, over the {} byte persistence limit",
                            self.hard_cap_bytes
                        ),
                    );
                }
                let metadata = if outcome.metadata.to_string().len() > MAX_METADATA_BYTES {
                    json!({ "truncated": true })
                } else {
                    outcome.metadata
                };
                Settled {
                    failed: false,
                    model_text: truncate_utf8(&outcome.content, self.model_cap_bytes),
                    result_json: json!({ "content": outcome.content, "metadata": metadata })
                        .to_string(),
                    error_code: None,
                }
            }
        }
    }
}

pub(super) fn valid_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

/// Tên tool do model sinh: cắt ngắn và bỏ ký tự điều khiển trước khi echo lại.
pub(super) fn safe_name(name: &str) -> String {
    name.chars().filter(|c| !c.is_control()).take(64).collect()
}

fn sanitize_message(msg: &str) -> String {
    msg.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(MAX_ERROR_CHARS)
        .collect()
}

/// Cắt ở ranh giới ký tự và **luôn** kèm marker nêu rõ phần bị bỏ.
pub(crate) fn truncate_utf8(s: &str, cap: usize) -> String {
    if s.len() <= cap {
        return s.to_owned();
    }
    let mut end = cap;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n[output truncated: {end} of {} bytes shown; the full result is stored]",
        &s[..end],
        s.len()
    )
}
