use super::outcome::{outcome_of, EMPTY_TEXT_PLACEHOLDER};
use super::*;
use crate::tool::ToolScope;
use crate::types::{AgentResult, RunStatus};
use serde_json::json;

#[test]
fn input_parses_optional_session_id() {
    let a = SubagentInput::from_args(&json!({
        "agent": "reviewer", "description": "d", "prompt": "p"
    }));
    assert_eq!(a.session_id, None);
    let b = SubagentInput::from_args(&json!({
        "agent": "reviewer", "description": "d", "prompt": "p", "session_id": " s1 "
    }));
    assert_eq!(b.session_id.as_deref(), Some("s1"));
    for blank in [json!(null), json!(""), json!("  ")] {
        let c = SubagentInput::from_args(&json!({
            "agent": "r", "description": "d", "prompt": "p", "session_id": blank
        }));
        assert_eq!(c.session_id, None);
    }
}

fn scope() -> ToolScope {
    ToolScope {
        owner_id: "o".into(),
        workspace_id: "w".into(),
        session_id: "parent".into(),
        root_session_id: "parent".into(),
        turn_id: "t".into(),
        run_id: "run".into(),
        agent_id: "root".into(),
        invocation_id: "inv".into(),
        tool_call_id: "call".into(),
        allowed_tools: Vec::new(),
        allowed_skills: Vec::new(),
    }
}

fn result(status: RunStatus, output: Option<&str>, code: Option<&str>) -> AgentResult {
    AgentResult {
        session_id: "child".into(),
        turn_id: "turn".into(),
        run_id: "r".into(),
        attempt: 0,
        status,
        steps: 1,
        input_tokens: 0,
        output_tokens: 0,
        output: output.map(str::to_owned),
        final_message_id: output.map(|_| "m".into()),
        error: code.map(|c| crate::types::ResultError {
            code: c.into(),
            message: "m".into(),
        }),
        artifact_preview: Vec::new(),
        artifacts_next_cursor: None,
    }
}

#[test]
fn missing_or_blank_final_text_becomes_a_placeholder_not_a_panic() {
    for output in [None, Some(""), Some("  \n ")] {
        let out = outcome_of(&scope(), result(RunStatus::Completed, output, None)).unwrap();
        assert!(
            out.content.contains(EMPTY_TEXT_PLACEHOLDER),
            "{}",
            out.content
        );
        assert_eq!(out.metadata["empty_text"], json!(true));
    }
    let out = outcome_of(&scope(), result(RunStatus::Completed, Some("real"), None)).unwrap();
    assert_eq!(
        out.content,
        "<subagent sessionID=\"child\" state=\"completed\">\nreal\n</subagent>"
    );
    assert_eq!(out.metadata["empty_text"], json!(false));
}

#[test]
fn non_completed_children_map_to_stable_tool_error_codes() {
    let code = |status, child_code| {
        outcome_of(&scope(), result(status, None, child_code))
            .unwrap_err()
            .code
    };
    assert_eq!(
        code(RunStatus::TimedOut, Some("PROVIDER_TIMEOUT")),
        "PROVIDER_TIMEOUT"
    );
    assert_eq!(code(RunStatus::Cancelled, None), "CHILD_CANCELLED");
    assert_eq!(
        code(RunStatus::Failed, Some("PROVIDER_UNAVAILABLE")),
        "CHILD_FAILED"
    );
    assert_eq!(
        code(RunStatus::Interrupted, Some("TOOL_OUTCOME_UNKNOWN")),
        "CHILD_FAILED"
    );
    assert_eq!(
        code(RunStatus::Failed, Some("PROVIDER_AUTH_FAILED")),
        "MODEL_UNAVAILABLE"
    );
    assert_eq!(
        code(RunStatus::Failed, Some("MODEL_NOT_CONFIGURED")),
        "MODEL_UNAVAILABLE"
    );
    let e = outcome_of(&scope(), result(RunStatus::Failed, None, Some("X"))).unwrap_err();
    assert!(
        e.message.contains("child") && e.message.contains("session_id"),
        "{}",
        e.message
    );
}
