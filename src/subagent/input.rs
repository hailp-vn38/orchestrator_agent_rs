//! Envelope kỹ thuật của lời gọi subagent.
use serde_json::{json, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubagentInput {
    pub agent: String,
    /// Nhãn ngắn cho UI/log; không đưa vào child transcript.
    pub description: String,
    pub prompt: String,
    /// `None` => child mới; `Some` => tiếp tục child cũ của chính parent.
    pub session_id: Option<String>,
}

impl SubagentInput {
    /// Gọi sau khi `args` đã qua [`input_schema`] (envelope kỹ thuật).
    pub(crate) fn from_args(args: &Value) -> Self {
        let text = |k: &str| args[k].as_str().unwrap_or_default().to_owned();
        Self {
            agent: text("agent"),
            description: text("description"),
            prompt: text("prompt"),
            session_id: args["session_id"]
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned),
        }
    }
}

/// Envelope kỹ thuật của `subagent`: không có `background`, không business JSON.
pub(crate) fn input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "agent": { "type": "string" },
            "description": { "type": "string" },
            "prompt": { "type": "string" },
            "session_id": { "type": ["string", "null"] }
        },
        "required": ["agent", "description", "prompt"],
        "additionalProperties": false
    })
}
