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
        partial_output: None,
        final_message_id: output.map(|_| "m".into()),
        error: code.map(|c| crate::types::ResultError {
            code: c.into(),
            message: "m".into(),
        }),
        next_attempt_at: None,
        retry_reason: None,
        artifact_preview: Vec::new(),
        artifacts_next_cursor: None,
    }
}

#[test]
fn missing_or_blank_final_text_becomes_a_placeholder_not_a_panic() {
    for output in [None, Some(""), Some("  \n ")] {
        let Delegation::Done(Ok(out)) =
            outcome_of(&scope(), result(RunStatus::Completed, output, None))
        else {
            panic!("expected Delegation::Done(Ok)");
        };
        assert!(
            out.content.contains(EMPTY_TEXT_PLACEHOLDER),
            "{}",
            out.content
        );
        assert_eq!(out.metadata["empty_text"], json!(true));
    }
    let Delegation::Done(Ok(out)) =
        outcome_of(&scope(), result(RunStatus::Completed, Some("real"), None))
    else {
        panic!("expected Delegation::Done(Ok)");
    };
    assert_eq!(
        out.content,
        "<subagent sessionID=\"child\" state=\"completed\">\nreal\n</subagent>"
    );
    assert_eq!(out.metadata["empty_text"], json!(false));
}

#[test]
fn non_completed_children_map_to_stable_tool_error_codes() {
    let check = |status, child_code| match outcome_of(&scope(), result(status, None, child_code)) {
        Delegation::Failed(f) => (f.stop_reason, f.retryable),
        _ => panic!("expected Delegation::Failed"),
    };
    assert_eq!(
        check(RunStatus::TimedOut, Some("PROVIDER_TIMEOUT")),
        ("PROVIDER_TIMEOUT".into(), true)
    );
    assert_eq!(
        check(RunStatus::Cancelled, None),
        ("CANCELLED".into(), false)
    );
    assert_eq!(
        check(RunStatus::Failed, Some("RECOVERY_ATTEMPTS_EXHAUSTED")),
        ("RECOVERY_ATTEMPTS_EXHAUSTED".into(), true)
    );
    assert_eq!(
        check(RunStatus::Interrupted, Some("TOOL_OUTCOME_UNKNOWN")),
        ("TOOL_OUTCOME_UNKNOWN".into(), false)
    );
    assert_eq!(
        check(RunStatus::Failed, Some("PROVIDER_INVALID_REQUEST")),
        ("PROVIDER_INVALID_REQUEST".into(), false)
    );
    assert_eq!(
        check(RunStatus::Failed, Some("MODEL_NOT_CONFIGURED")),
        ("MODEL_NOT_CONFIGURED".into(), false)
    );
}
