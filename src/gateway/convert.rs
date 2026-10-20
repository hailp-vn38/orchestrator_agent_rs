use llm::chat::ChatMessage;
use llm::{FunctionCall, ToolCall};

use super::types::{ModelMessage, ModelRole};

/// Chuyển transcript nội bộ sang message của `llm`. Các tool result liên tiếp (kết quả của
/// cùng một assistant response) gộp thành **một** message để mọi provider chấp nhận.
/// Nội dung result đi trong `function.arguments` (quy ước của crate `llm`).
pub(crate) fn to_chat_messages(messages: &[ModelMessage]) -> Vec<ChatMessage> {
    let mut out = Vec::with_capacity(messages.len());
    let mut i = 0;
    while i < messages.len() {
        let m = &messages[i];
        match m.role {
            ModelRole::User => out.push(ChatMessage::user().content(m.text.clone()).build()),
            ModelRole::Assistant if m.tool_calls.is_empty() => {
                out.push(ChatMessage::assistant().content(m.text.clone()).build());
            }
            ModelRole::Assistant => {
                let calls = m
                    .tool_calls
                    .iter()
                    .map(|c| ToolCall {
                        id: c.id.clone(),
                        call_type: "function".to_string(),
                        function: FunctionCall {
                            name: c.name.clone(),
                            arguments: c.arguments.to_string(),
                        },
                    })
                    .collect();
                out.push(
                    ChatMessage::assistant()
                        .content(m.text.clone())
                        .tool_use(calls)
                        .build(),
                );
            }
            ModelRole::Tool => {
                let mut results = Vec::new();
                while i < messages.len() && messages[i].role == ModelRole::Tool {
                    let r = &messages[i];
                    results.push(ToolCall {
                        id: r.tool_call_id.clone().unwrap_or_default(),
                        call_type: "function".to_string(),
                        function: FunctionCall {
                            name: r.tool_name.clone().unwrap_or_default(),
                            arguments: r.text.clone(),
                        },
                    });
                    i += 1;
                }
                out.push(ChatMessage::user().tool_result(results).build());
                continue;
            }
        }
        i += 1;
    }
    out
}

