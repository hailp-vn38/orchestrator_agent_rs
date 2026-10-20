//! Helper mã hóa/giải mã JSON và lỗi DB dùng chung trong `store`.

use super::types::StoredToolCall;
use crate::error::OrchestratorError;
use crate::types::SessionPolicy;

/// Trần độ dài `error_message` được persist.
pub(super) const MAX_ERROR_MESSAGE: usize = 500;

pub(super) fn db(context: &'static str) -> impl Fn(sqlx::Error) -> OrchestratorError {
    move |e| OrchestratorError::storage(context, &e)
}

pub(crate) fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub(super) fn text_json(text: &str) -> String {
    serde_json::json!({ "text": text }).to_string()
}

/// JSON của policy để lưu vào `sessions.permissions_json`; `None` khi không thu hẹp gì.
pub(super) fn policy_json(policy: &SessionPolicy) -> Option<String> {
    (policy != &SessionPolicy::default()).then(|| {
        serde_json::json!({
            "allowed_tools": policy.allowed_tools,
            "allowed_skills": policy.allowed_skills,
            "child_allowed_tools": policy.child_allowed_tools,
            "child_allowed_skills": policy.child_allowed_skills,
        })
        .to_string()
    })
}

pub(super) fn parse_policy(permissions_json: Option<&str>) -> SessionPolicy {
    let list = |v: &serde_json::Value, key: &str| {
        v.get(key).and_then(|a| a.as_array()).map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        })
    };
    permissions_json
        .and_then(|j| serde_json::from_str::<serde_json::Value>(j).ok())
        .map(|v| SessionPolicy {
            allowed_tools: list(&v, "allowed_tools"),
            allowed_skills: list(&v, "allowed_skills"),
            child_allowed_tools: list(&v, "child_allowed_tools"),
            child_allowed_skills: list(&v, "child_allowed_skills"),
        })
        .unwrap_or_default()
}

/// Tool calls trong `content_json` của assistant message (rỗng nếu là câu trả lời cuối).
pub(super) fn calls_of(content_json: &str) -> Vec<StoredToolCall> {
    serde_json::from_str::<serde_json::Value>(content_json)
        .ok()
        .and_then(|v| v.get("tool_calls").and_then(|c| c.as_array()).cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|c| {
            Some(StoredToolCall {
                id: c.get("id")?.as_str()?.to_owned(),
                name: c.get("name")?.as_str()?.to_owned(),
                arguments: c.get("arguments").cloned().unwrap_or(serde_json::json!({})),
            })
        })
        .collect()
}

pub(super) fn text_of(content_json: &str) -> String {
    serde_json::from_str::<serde_json::Value>(content_json)
        .ok()
        .and_then(|v| v.get("text").and_then(|t| t.as_str()).map(str::to_owned))
        .unwrap_or_default()
}

pub(super) fn sanitize_message(msg: &str) -> String {
    msg.chars().take(MAX_ERROR_MESSAGE).collect()
}
