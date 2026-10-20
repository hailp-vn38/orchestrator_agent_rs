use std::sync::Mutex;

use llm::chat::StreamChunk;
use llm::error::LLMError;
use llm::{FunctionCall, ToolCall};

use super::collect::{collect, RawStream};
use super::convert::to_chat_messages;
use super::error::{map_error, GatewayError};
use super::types::{
    ModelMessage, ModelParams, ModelResponse, ModelRole, ModelToolCall, StreamEvent,
};

async fn collect_q(raw: RawStream) -> Result<ModelResponse, GatewayError> {
    collect(raw, &|_| {}).await
}

/// Chạy `collect` và trả kèm preview theo đúng thứ tự nhận được.
async fn collect_events(raw: RawStream) -> (Result<ModelResponse, GatewayError>, Vec<StreamEvent>) {
    let seen = Mutex::new(Vec::new());
    let sink = |e: StreamEvent| seen.lock().unwrap().push(e);
    let r = collect(raw, &sink).await;
    let events = seen.into_inner().unwrap();
    (r, events)
}

fn stream(items: Vec<Result<StreamChunk, LLMError>>) -> RawStream {
    Box::pin(futures::stream::iter(items))
}

#[tokio::test]
async fn collects_text_until_done() {
    let r = collect_q(stream(vec![
        Ok(StreamChunk::Text("he".into())),
        Ok(StreamChunk::Text("llo".into())),
        Ok(StreamChunk::Done {
            stop_reason: "end_turn".into(),
        }),
    ]))
    .await
    .unwrap();
    assert_eq!(r.text, "hello");
    assert_eq!(r.finish_reason, "end_turn");
}

#[tokio::test]
async fn truncated_or_failed_stream_discards_text() {
    let cut = collect_q(stream(vec![Ok(StreamChunk::Text("x".into()))])).await;
    assert_eq!(cut, Err(GatewayError::Protocol));
    let failed = collect_q(stream(vec![
        Ok(StreamChunk::Text("x".into())),
        Err(LLMError::HttpError("boom sk-SECRET".into())),
    ]))
    .await;
    assert_eq!(failed, Err(GatewayError::Transport));
}

fn tool_call(id: &str, args: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        call_type: "function".into(),
        function: FunctionCall {
            name: "t".into(),
            arguments: args.into(),
        },
    }
}

#[tokio::test]
async fn tool_calls_are_returned_only_after_done() {
    let r = collect_q(stream(vec![
        Ok(StreamChunk::ToolUseStart {
            index: 0,
            id: "c1".into(),
            name: "t".into(),
        }),
        Ok(StreamChunk::ToolUseInputDelta {
            index: 0,
            partial_json: "{\"a\":".into(),
        }),
        Ok(StreamChunk::ToolUseInputDelta {
            index: 0,
            partial_json: "1}".into(),
        }),
        Ok(StreamChunk::ToolUseComplete {
            index: 0,
            tool_call: tool_call("c1", "{\"a\":1}"),
        }),
        Ok(StreamChunk::ToolUseComplete {
            index: 1,
            tool_call: tool_call("c2", ""),
        }),
        Ok(StreamChunk::Done {
            stop_reason: "tool_use".into(),
        }),
    ]))
    .await
    .unwrap();
    assert_eq!(r.tool_calls.len(), 2);
    assert_eq!(r.tool_calls[0].id, "c1");
    assert_eq!(r.tool_calls[0].arguments, serde_json::json!({"a": 1}));
    assert_eq!(r.tool_calls[1].arguments, serde_json::json!({}));
}

/// AT-35 (mức gateway): lỗi sau `ToolUseComplete` mà chưa `Done` → không trả call nào.
#[tokio::test]
async fn error_after_complete_tool_call_yields_no_calls() {
    let failed = collect_q(stream(vec![
        Ok(StreamChunk::ToolUseComplete {
            index: 0,
            tool_call: tool_call("c1", "{}"),
        }),
        Err(LLMError::HttpError("boom".into())),
    ]))
    .await;
    assert_eq!(failed, Err(GatewayError::Transport));
    let cut = collect_q(stream(vec![Ok(StreamChunk::ToolUseComplete {
        index: 0,
        tool_call: tool_call("c1", "{}"),
    })]))
    .await;
    assert_eq!(cut, Err(GatewayError::Protocol));
}

#[tokio::test]
async fn malformed_tool_arguments_are_a_protocol_error() {
    let r = collect_q(stream(vec![
        Ok(StreamChunk::ToolUseComplete {
            index: 0,
            tool_call: tool_call("c1", "{bad"),
        }),
        Ok(StreamChunk::Done {
            stop_reason: "tool_use".into(),
        }),
    ]))
    .await;
    assert_eq!(r, Err(GatewayError::Protocol));
}

#[test]
fn consecutive_tool_results_are_grouped_into_one_message() {
    let call = |id: &str| ModelToolCall {
        id: id.into(),
        name: "t".into(),
        arguments: serde_json::json!({}),
    };
    let msgs = vec![
        ModelMessage::text(ModelRole::User, "go"),
        ModelMessage::assistant_with_calls("", vec![call("a"), call("b")]),
        ModelMessage::tool_result("a", "t", "ra"),
        ModelMessage::tool_result("b", "t", "rb"),
        ModelMessage::text(ModelRole::Assistant, "done"),
    ];
    let out = to_chat_messages(&msgs);
    assert_eq!(out.len(), 4);
    match &out[2].message_type {
        llm::chat::MessageType::ToolResult(rs) => {
            let ids: Vec<&str> = rs.iter().map(|r| r.id.as_str()).collect();
            assert_eq!(ids, ["a", "b"]);
            assert_eq!(rs[1].function.arguments, "rb");
        }
        other => panic!("expected tool result, got {other:?}"),
    }
}

#[test]
fn error_mapping_never_exposes_raw_text() {
    assert_eq!(
        map_error(&LLMError::AuthError("sk-1".into())),
        GatewayError::Auth
    );
    let r = |m: &str| LLMError::ResponseFormatError {
        message: m.into(),
        raw_response: "sk-LEAK".into(),
    };
    assert_eq!(
        map_error(&r("API returned error status: 401 Unauthorized")),
        GatewayError::Auth
    );
    assert_eq!(
        map_error(&r("API returned error status: 429 Too Many")),
        GatewayError::Provider
    );
    for e in [
        GatewayError::Auth,
        GatewayError::Provider,
        GatewayError::Transport,
        GatewayError::Protocol,
    ] {
        assert!(!e.message().contains("sk-"));
    }
}

/// AT-26 (mức gateway): 20 fragment được phát đúng thứ tự và gom đúng nội dung.
#[tokio::test]
async fn twenty_text_fragments_stream_in_order_and_assemble() {
    let mut items: Vec<Result<StreamChunk, LLMError>> = (0..20)
        .map(|i| Ok(StreamChunk::Text(format!("[{i}]"))))
        .collect();
    items.push(Ok(StreamChunk::Done {
        stop_reason: "end_turn".into(),
    }));
    let (r, events) = collect_events(stream(items)).await;
    let expected: Vec<String> = (0..20).map(|i| format!("[{i}]")).collect();
    let deltas: Vec<String> = events
        .iter()
        .map(|e| match e {
            StreamEvent::TextDelta(t) => t.clone(),
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(deltas, expected);
    assert_eq!(r.unwrap().text, expected.concat());
}

/// AT-35: `ToolCallReady` chỉ là preview; lỗi trước `Done` không trả call nào.
#[tokio::test]
async fn tool_call_ready_then_error_is_a_preview_only() {
    let (r, events) = collect_events(stream(vec![
        Ok(StreamChunk::ToolUseComplete {
            index: 0,
            tool_call: tool_call("c1", "{}"),
        }),
        Err(LLMError::HttpError("boom".into())),
    ]))
    .await;
    assert_eq!(r, Err(GatewayError::Transport));
    assert_eq!(
        events,
        vec![StreamEvent::ToolCallReady {
            id: "c1".into(),
            name: "t".into()
        }]
    );
}

#[test]
fn context_overflow_is_classified_without_leaking_text() {
    let overflow = LLMError::ResponseFormatError {
        message: "API returned error status: 400 Bad Request".into(),
        raw_response: "{\"error\":{\"code\":\"context_length_exceeded\",\"key\":\"sk-LEAK\"}}"
            .into(),
    };
    assert_eq!(map_error(&overflow), GatewayError::ContextOverflow);
    assert_eq!(
        map_error(&LLMError::InvalidRequest("prompt is too long".into())),
        GatewayError::ContextOverflow
    );
    let plain = LLMError::ResponseFormatError {
        message: "API returned error status: 400 Bad Request".into(),
        raw_response: "bad".into(),
    };
    assert_eq!(map_error(&plain), GatewayError::InvalidRequest);
    assert_eq!(GatewayError::ContextOverflow.code(), "CONTEXT_LIMIT");
    assert!(!GatewayError::ContextOverflow.message().contains("sk-"));
}

#[test]
fn model_params_validation_and_cache_key() {
    assert!(ModelParams::default().validate().is_ok());
    let ok = ModelParams {
        max_output_tokens: Some(256),
        temperature: Some(0.0),
        top_p: Some(1.0),
        top_k: Some(40),
    };
    assert!(ok.validate().is_ok());
    for bad in [
        ModelParams {
            max_output_tokens: Some(0),
            ..Default::default()
        },
        ModelParams {
            temperature: Some(2.5),
            ..Default::default()
        },
        ModelParams {
            temperature: Some(-0.1),
            ..Default::default()
        },
        ModelParams {
            top_p: Some(0.0),
            ..Default::default()
        },
        ModelParams {
            top_p: Some(1.5),
            ..Default::default()
        },
        ModelParams {
            top_k: Some(0),
            ..Default::default()
        },
        ModelParams {
            temperature: Some(f32::NAN),
            ..Default::default()
        },
    ] {
        assert!(bad.validate().is_err(), "{bad:?}");
    }
    assert_ne!(ok.cache_key(), ModelParams::default().cache_key());
    assert_eq!(ok.cache_key(), ok.cache_key());
}
