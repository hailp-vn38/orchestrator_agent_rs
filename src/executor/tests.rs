use super::outcome::valid_code;
use super::*;
use crate::agent::AgentDefinition;
use crate::types::SessionPolicy;
use serde_json::{json, Value};
use std::collections::BTreeSet;

fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "path": { "type": "string" },
            "n": { "type": "integer" },
            "mode": { "enum": ["r", "w"] },
            "tags": { "type": "array", "items": { "type": "string" } }
        },
        "required": ["path"],
        "additionalProperties": false
    })
}

#[test]
fn schema_accepts_valid_and_rejects_invalid_arguments() {
    assert!(validate_args(&schema(), &json!({"path": "a", "n": 3, "mode": "r"})).is_ok());
    let err = |v: Value| validate_args(&schema(), &v).unwrap_err();
    assert!(err(json!({})).contains("arguments.path is required"));
    assert!(err(json!({"path": 1})).contains("arguments.path must be of type string"));
    assert!(err(json!({"path": "a", "n": 1.5})).contains("arguments.n"));
    assert!(err(json!({"path": "a", "mode": "x"})).contains("allowed values"));
    assert!(err(json!({"path": "a", "tags": ["x", 2]})).contains("arguments.tags[1]"));
    assert!(err(json!({"path": "a", "extra": 1})).contains("arguments.extra is not allowed"));
    assert!(err(json!("not an object")).contains("must be of type object"));
    assert!(validate_args(&schema(), &json!({"path": "a", "n": 2.0})).is_ok());
}

#[test]
fn truncation_is_marked_and_respects_char_boundaries() {
    assert_eq!(truncate_utf8("abc", 10), "abc");
    let t = truncate_utf8("ééééé", 3); // mỗi 'é' = 2 byte → cắt còn 1 ký tự
    assert!(t.starts_with("é\n[output truncated: 2 of 10 bytes shown"));
}

#[test]
fn effective_permissions_only_narrow() {
    let agent = AgentDefinition::new("a", "p", "m")
        .with_allowed_tools(["skill", "read"])
        .with_allowed_skills(["x", "y"])
        .with_allowed_subagents(["sub1", "sub2"]);
    let all = Effective::resolve(&agent, &SessionPolicy::default());
    assert_eq!(all.tools.len(), 2);
    assert_eq!(all.skills.len(), 2);
    assert_eq!(
        all.subagents,
        Some(BTreeSet::from(["sub1".to_string(), "sub2".to_string()]))
    );
    // Policy đòi thêm quyền ngoài agent → bị bỏ; chỉ phần giao còn lại.
    let narrowed = Effective::resolve(
        &agent,
        &SessionPolicy::default()
            .with_allowed_tools(["read", "write_file"])
            .with_allowed_skills(["y", "z"]),
    );
    assert_eq!(narrowed.tools, BTreeSet::from(["read".to_string()]));
    assert_eq!(narrowed.skills, BTreeSet::from(["y".to_string()]));
    let none = Effective::resolve(
        &agent,
        &SessionPolicy::default().with_allowed_skills::<_, String>([]),
    );
    assert!(none.skills.is_empty());
}

#[test]
fn error_codes_are_validated_and_messages_sanitized() {
    assert!(valid_code("NOT_FOUND_1"));
    assert!(!valid_code("lower"));
    assert!(!valid_code(""));
    let s = Settled::failure("X", "bad\nline\u{7}");
    assert!(!s.model_text.contains('\n'));
}
