//! Kiểm tra tên, mime và storage key.

use super::errors::CODE_ARTIFACT_INVALID_INPUT;
use super::{MAX_MIME_CHARS, MAX_NAME_CHARS};
use crate::tool::ToolError;

pub(super) fn validate_name(raw: &str) -> Result<String, ToolError> {
    let trimmed = raw.trim();
    let bad = |msg: &str| ToolError {
        code: CODE_ARTIFACT_INVALID_INPUT.into(),
        message: msg.to_owned(),
    };
    if trimmed.is_empty() || trimmed.chars().count() > MAX_NAME_CHARS {
        return Err(bad("artifact name must be 1..=255 characters"));
    }
    if trimmed == "."
        || trimmed == ".."
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.chars().any(char::is_control)
    {
        return Err(bad(
            "artifact name must be a plain display filename without path separators or control characters",
        ));
    }
    Ok(trimmed.to_owned())
}

pub(super) fn validate_mime(raw: &str) -> Result<String, ToolError> {
    let trimmed = raw.trim();
    let bad = |msg: &str| ToolError {
        code: CODE_ARTIFACT_INVALID_INPUT.into(),
        message: msg.to_owned(),
    };
    if trimmed.is_empty()
        || trimmed.len() > MAX_MIME_CHARS
        || !trimmed.contains('/')
        || trimmed.starts_with('/')
        || trimmed.ends_with('/')
        || trimmed
            .chars()
            .any(|c| !c.is_ascii_graphic() && c != ' ' && c != ';')
    {
        return Err(bad(
            "mime_type must be a valid media type (e.g. text/markdown)",
        ));
    }
    Ok(trimmed.to_owned())
}

pub(super) fn is_safe_storage_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 128
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
