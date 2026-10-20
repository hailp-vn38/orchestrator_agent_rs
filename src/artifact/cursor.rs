//! Cursor phân trang artifact.

use super::CURSOR_VERSION;
use crate::error::{ErrorCode, OrchestratorError};

pub(super) fn encode_artifact_cursor(
    session_id: &str,
    turn_id: &str,
    created_at: i64,
    last_id: &str,
) -> String {
    let raw = format!("{CURSOR_VERSION}|{session_id}|{turn_id}|{created_at}|{last_id}");
    raw.bytes().map(|b| format!("{b:02x}")).collect()
}

pub(super) fn decode_artifact_cursor(
    cursor: &str,
    session_id: &str,
    turn_id: &str,
) -> Result<(i64, String), OrchestratorError> {
    let bad = || OrchestratorError::new(ErrorCode::InvalidInput, "invalid cursor for this query");
    if cursor.is_empty() || !cursor.len().is_multiple_of(2) || !cursor.is_ascii() {
        return Err(bad());
    }
    let bytes: Result<Vec<u8>, _> = (0..cursor.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&cursor[i..i + 2], 16))
        .collect();
    let raw = String::from_utf8(bytes.map_err(|_| bad())?).map_err(|_| bad())?;
    let mut parts = raw.splitn(5, '|');
    match (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        (Some(v), Some(s), Some(t), Some(ts), Some(id))
            if v == CURSOR_VERSION && s == session_id && t == turn_id && !id.is_empty() =>
        {
            let parsed_ts = ts.parse::<i64>().map_err(|_| bad())?;
            Ok((parsed_ts, id.to_owned()))
        }
        _ => Err(bad()),
    }
}
