use std::sync::Mutex;

use llm::chat::StreamChunk;
use llm::error::LLMError;
use llm::{FunctionCall, ToolCall};

use super::collect::{collect, strip_thinking_blocks, RawStream};
use super::convert::to_chat_messages;
use super::error::{map_error, FailureClass, GatewayError};
use super::types::{
    ModelMessage, ModelParams, ModelRequest, ModelResponse, ModelRole, ModelToolCall, ModelUsage,
    ReasoningEffort, StreamEvent,
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
        GatewayError::RateLimited {
            retry_after_secs: None
        }
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
        .filter_map(|e| match e {
            StreamEvent::TextDelta(t) => Some(t.clone()),
            StreamEvent::Activity => None,
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
        vec![
            StreamEvent::Activity,
            StreamEvent::ToolCallReady {
                id: "c1".into(),
                name: "t".into()
            }
        ]
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
        reasoning_effort: Some(ReasoningEffort::Medium),
        reasoning_budget_tokens: Some(2048),
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
            reasoning_budget_tokens: Some(0),
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

#[tokio::test]
async fn collect_strips_thinking_blocks_from_stream_and_final_text() {
    let (r, events) = collect_events(stream(vec![
        Ok(StreamChunk::Text("<thi".into())),
        Ok(StreamChunk::Text("nk>internal reasoning...".into())),
        Ok(StreamChunk::Text(" still thinking</th".into())),
        Ok(StreamChunk::Text("ink>\n\nFinal ".into())),
        Ok(StreamChunk::Text("answer.".into())),
        Ok(StreamChunk::Done {
            stop_reason: "end_turn".into(),
        }),
    ]))
    .await;
    let resp = r.unwrap();
    assert_eq!(resp.text, "Final answer.");
    let streamed: String = events
        .into_iter()
        .filter_map(|e| match e {
            StreamEvent::TextDelta(t) => Some(t),
            StreamEvent::Activity => None,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(streamed, "Final answer.");
}

#[tokio::test]
async fn collect_rejects_empty_or_thinking_only_completion_without_tools() {
    let empty = collect_q(stream(vec![Ok(StreamChunk::Done {
        stop_reason: "end_turn".into(),
    })]))
    .await;
    assert_eq!(empty, Err(GatewayError::Protocol));

    let only_think = collect_q(stream(vec![
        Ok(StreamChunk::Text("<think>exhausted token budget".into())),
        Ok(StreamChunk::Done {
            stop_reason: "length".into(),
        }),
    ]))
    .await;
    assert_eq!(only_think, Err(GatewayError::Protocol));
}

#[test]
fn strip_thinking_handles_prefilled_and_multiple_blocks() {
    assert_eq!(
        strip_thinking_blocks("prefilled reasoning\n</think>\n\nVisible output"),
        "Visible output"
    );
    assert_eq!(
        strip_thinking_blocks("Part 1 <think>hidden</think> Part 2"),
        "Part 1 Part 2"
    );
    assert_eq!(strip_thinking_blocks("<think>unclosed"), "");
}

#[test]
fn model_usage_estimates_missing_fields_and_preserves_measured_tokens() {
    let req = ModelRequest {
        provider_id: "p".into(),
        model_id: "m".into(),
        system: "abcd".repeat(10), // 40 bytes -> 10 tokens
        messages: vec![ModelMessage::text(ModelRole::User, "efgh".repeat(5))], // 20 bytes -> 5 tokens
        tools: Vec::new(),
        params: ModelParams::default(),
    };
    let resp = ModelResponse {
        text: "12345678".into(), // 8 bytes -> 2 tokens
        finish_reason: "stop".into(),
        tool_calls: Vec::new(),
        usage: None,
    };

    let estimated = ModelUsage::or_estimated(None, &req, &resp);
    assert_eq!(estimated, ModelUsage::new(15, 2));
    assert_eq!(estimated.total_tokens(), 17);

    let measured = ModelUsage::or_estimated(Some(ModelUsage::new(100, 42)), &req, &resp);
    assert_eq!(measured, ModelUsage::new(100, 42));
    assert_eq!(measured.total_tokens(), 142);

    let partial = ModelUsage::or_estimated(
        Some(ModelUsage {
            input_tokens: Some(80),
            output_tokens: None,
        }),
        &req,
        &resp,
    );
    assert_eq!(partial, ModelUsage::new(80, 2));
    assert_eq!(partial.total_tokens(), 82);
}

#[tokio::test]
async fn client_gateway_delegates_to_custom_executor_with_usage_and_timeout() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    use super::client::ClientGateway;
    use super::types::{LlmGateway, StreamSink};
    use crate::config::{
        CustomModelExecutor, LlmBackend, LlmProviderConfig, ProviderAuth, RequestLimits,
    };

    #[derive(Debug)]
    struct TestCustomExecutor {
        fingerprint: String,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl CustomModelExecutor for TestCustomExecutor {
        fn routing_fingerprint(&self) -> String {
            self.fingerprint.clone()
        }

        async fn complete(
            &self,
            request: &ModelRequest,
            _cancel: &CancellationToken,
            sink: StreamSink<'_>,
        ) -> Result<ModelResponse, GatewayError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            sink(StreamEvent::TextDelta(format!(
                "model={}:",
                request.model_id
            )));
            sink(StreamEvent::TextDelta("ok".to_string()));
            Ok(ModelResponse {
                text: format!("model={}:ok", request.model_id),
                finish_reason: "stop".to_string(),
                tool_calls: Vec::new(),
                usage: None,
            })
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let exec = Arc::new(TestCustomExecutor {
        fingerprint: "gw:pool:anthropic".to_string(),
        calls: Arc::clone(&calls),
    });
    let cfg = LlmProviderConfig {
        id: "gw_claude".to_string(),
        backend: LlmBackend::Anthropic,
        endpoint: Some("https://api.anthropic.com/v1".to_string()),
        auth: ProviderAuth::CustomExecutor(exec),
        limits: RequestLimits::uniform(Duration::from_secs(5)),
    };
    assert!(cfg.validate().is_ok());
    let fp1 = cfg.routing_fingerprint();
    assert!(fp1.starts_with("v1:"));

    let gw = ClientGateway::new(std::slice::from_ref(&cfg));
    let req = ModelRequest {
        provider_id: "gw_claude".to_string(),
        model_id: "claude-sonnet-4-20250514".to_string(),
        system: "You are helpful.".to_string(),
        messages: vec![ModelMessage::text(ModelRole::User, "Hello")],
        tools: Vec::new(),
        params: ModelParams::default(),
    };
    let streamed = Mutex::new(Vec::new());
    let sink = |ev: StreamEvent| streamed.lock().unwrap().push(ev);
    let resp = gw
        .complete(&req, &CancellationToken::new(), &sink)
        .await
        .expect("custom executor complete");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(resp.text, "model=claude-sonnet-4-20250514:ok");
    assert!(resp.usage.is_some());
    assert!(resp.usage.unwrap().total_tokens() > 0);
    assert_eq!(
        streamed.into_inner().unwrap(),
        vec![
            StreamEvent::TextDelta("model=claude-sonnet-4-20250514:".to_string()),
            StreamEvent::TextDelta("ok".to_string()),
        ]
    );
}

mod watchdog_tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    use super::super::client::ClientGateway;
    use super::super::error::GatewayError;
    use super::super::types::{
        LlmGateway, ModelMessage, ModelParams, ModelRequest, ModelResponse, ModelRole, StreamEvent,
        StreamSink,
    };
    use crate::config::{
        CustomModelExecutor, LlmBackend, LlmProviderConfig, ProviderAuth, RequestLimits,
    };

    #[derive(Debug, Clone)]
    enum Scenario {
        /// Phát `count` đoạn cách nhau `interval`, rồi hoàn tất.
        Steady { count: usize, interval: Duration },
        /// Phát 1 đoạn tại `first_at`, rồi treo lâu hơn `idle`.
        StallAfterFirst { first_at: Duration },
        /// Không phát bất kỳ tín hiệu nào.
        NeverFirstByte,
        /// Phát `Activity` liên tục mỗi `interval` vượt quá `max_total`.
        EndlessActivity { interval: Duration },
        /// Phát `<think>` mỗi `interval` trong `count` lần, rồi phát câu trả lời cuối.
        LongThinkBlock { count: usize, interval: Duration },
    }

    #[derive(Debug)]
    struct ScenarioExecutor(Scenario);

    #[async_trait::async_trait]
    impl CustomModelExecutor for ScenarioExecutor {
        fn routing_fingerprint(&self) -> String {
            "test:watchdog".into()
        }

        async fn complete(
            &self,
            _request: &ModelRequest,
            _cancel: &CancellationToken,
            sink: StreamSink<'_>,
        ) -> Result<ModelResponse, GatewayError> {
            match self.0.clone() {
                Scenario::Steady { count, interval } => {
                    let mut text = String::new();
                    for i in 0..count {
                        tokio::time::sleep(interval).await;
                        let part = format!("chunk-{i} ");
                        sink(StreamEvent::TextDelta(part.clone()));
                        text.push_str(&part);
                    }
                    Ok(ModelResponse {
                        text,
                        finish_reason: "stop".into(),
                        tool_calls: Vec::new(),
                        usage: None,
                    })
                }
                Scenario::StallAfterFirst { first_at } => {
                    tokio::time::sleep(first_at).await;
                    sink(StreamEvent::TextDelta("start".into()));
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    unreachable!()
                }
                Scenario::NeverFirstByte => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    unreachable!()
                }
                Scenario::EndlessActivity { interval } => loop {
                    tokio::time::sleep(interval).await;
                    sink(StreamEvent::Activity);
                },
                Scenario::LongThinkBlock { count, interval } => {
                    let mut full = String::from("<think>");
                    sink(StreamEvent::TextDelta("<think>".into()));
                    for i in 0..count {
                        tokio::time::sleep(interval).await;
                        let step = format!("reasoning step {i}... ");
                        sink(StreamEvent::TextDelta(step.clone()));
                        full.push_str(&step);
                    }
                    let tail = "</think>Final answer after thinking.";
                    sink(StreamEvent::TextDelta(tail.into()));
                    full.push_str(tail);
                    Ok(ModelResponse {
                        text: full,
                        finish_reason: "stop".into(),
                        tool_calls: Vec::new(),
                        usage: None,
                    })
                }
            }
        }
    }

    fn build_gw(limits: RequestLimits, scenario: Scenario) -> (ClientGateway, ModelRequest) {
        let cfg = LlmProviderConfig {
            id: "wd".into(),
            backend: LlmBackend::OpenAi,
            endpoint: None,
            auth: ProviderAuth::CustomExecutor(Arc::new(ScenarioExecutor(scenario))),
            limits,
        };
        let gw = ClientGateway::new(&[cfg]);
        let req = ModelRequest {
            provider_id: "wd".into(),
            model_id: "m".into(),
            system: "sys".into(),
            messages: vec![ModelMessage::text(ModelRole::User, "hi")],
            tools: Vec::new(),
            params: ModelParams::default(),
        };
        (gw, req)
    }

    #[tokio::test(start_paused = true)]
    async fn watchdog_steady_stream_five_minutes_completes() {
        let limits = RequestLimits {
            first_byte: Duration::from_secs(60),
            idle: Duration::from_secs(60),
            max_total: Duration::from_secs(600),
            fast_retries: 2,
        };
        let (gw, req) = build_gw(
            limits,
            Scenario::Steady {
                count: 10,
                interval: Duration::from_secs(30),
            },
        );
        let start = Instant::now();
        let resp = gw
            .complete(&req, &CancellationToken::new(), &|_| {})
            .await
            .expect("5 minute steady stream should complete");
        assert!(resp.text.contains("chunk-9"));
        assert_eq!(start.elapsed(), Duration::from_secs(300));
    }

    #[tokio::test(start_paused = true)]
    async fn watchdog_stalls_at_idle_limit() {
        let limits = RequestLimits {
            first_byte: Duration::from_secs(60),
            idle: Duration::from_secs(45),
            max_total: Duration::from_secs(600),
            fast_retries: 2,
        };
        let (gw, req) = build_gw(
            limits,
            Scenario::StallAfterFirst {
                first_at: Duration::from_secs(10),
            },
        );
        let start = Instant::now();
        let err = gw
            .complete(&req, &CancellationToken::new(), &|_| {})
            .await
            .expect_err("stream stalling after first byte must fail");
        assert_eq!(err, GatewayError::Stalled);
        assert_eq!(start.elapsed(), Duration::from_secs(55));
    }

    #[tokio::test(start_paused = true)]
    async fn watchdog_stalls_at_first_byte_limit() {
        let limits = RequestLimits {
            first_byte: Duration::from_secs(40),
            idle: Duration::from_secs(90),
            max_total: Duration::from_secs(600),
            fast_retries: 2,
        };
        let (gw, req) = build_gw(limits, Scenario::NeverFirstByte);
        let start = Instant::now();
        let err = gw
            .complete(&req, &CancellationToken::new(), &|_| {})
            .await
            .expect_err("silent stream must fail at first_byte");
        assert_eq!(err, GatewayError::Stalled);
        assert_eq!(start.elapsed(), Duration::from_secs(40));
    }

    #[tokio::test(start_paused = true)]
    async fn watchdog_enforces_max_total_despite_activity() {
        let limits = RequestLimits {
            first_byte: Duration::from_secs(60),
            idle: Duration::from_secs(60),
            max_total: Duration::from_secs(180),
            fast_retries: 2,
        };
        let (gw, req) = build_gw(
            limits,
            Scenario::EndlessActivity {
                interval: Duration::from_secs(20),
            },
        );
        let start = Instant::now();
        let err = gw
            .complete(&req, &CancellationToken::new(), &|_| {})
            .await
            .expect_err("endless activity must still hit max_total");
        assert_eq!(err, GatewayError::RequestLimitExceeded);
        assert_eq!(start.elapsed(), Duration::from_secs(180));
    }

    #[tokio::test(start_paused = true)]
    async fn watchdog_think_block() {
        let limits = RequestLimits {
            first_byte: Duration::from_secs(60),
            idle: Duration::from_secs(60),
            max_total: Duration::from_secs(600),
            fast_retries: 2,
        };
        let (gw, req) = build_gw(
            limits,
            Scenario::LongThinkBlock {
                count: 6,
                interval: Duration::from_secs(30),
            },
        );
        let seen = Mutex::new(Vec::new());
        let start = Instant::now();
        let resp = gw
            .complete(&req, &CancellationToken::new(), &|ev| {
                seen.lock().unwrap().push(ev);
            })
            .await
            .expect("3 minutes of <think> must not stall");
        assert_eq!(resp.text, "Final answer after thinking.");
        assert_eq!(start.elapsed(), Duration::from_secs(180));
        let events = seen.into_inner().unwrap();
        assert!(
            events.iter().any(|e| matches!(e, StreamEvent::Activity)),
            "thinking chunks must emit Activity to downstream sink"
        );
    }
}

#[test]
fn gateway_error_class_matches_spec_table() {
    use std::time::Duration;
    for err in [
        GatewayError::Stalled,
        GatewayError::Timeout,
        GatewayError::Transport,
        GatewayError::Provider,
        GatewayError::Protocol,
    ] {
        assert_eq!(
            err.classify(),
            FailureClass::Retryable { retry_after: None }
        );
    }
    assert_eq!(
        GatewayError::RateLimited {
            retry_after_secs: Some(45)
        }
        .classify(),
        FailureClass::Retryable {
            retry_after: Some(Duration::from_secs(45))
        }
    );
    assert_eq!(
        GatewayError::RateLimited {
            retry_after_secs: None
        }
        .code(),
        "PROVIDER_RATE_LIMITED"
    );
    assert_eq!(
        GatewayError::RequestLimitExceeded.classify(),
        FailureClass::RetryOnceThenSplit
    );
    assert_eq!(GatewayError::Auth.classify(), FailureClass::NeedsUser);
    assert_eq!(
        GatewayError::NotConfigured.classify(),
        FailureClass::Permanent
    );
    assert_eq!(
        GatewayError::InvalidRequest.classify(),
        FailureClass::Permanent
    );
    assert_eq!(GatewayError::Cancelled.classify(), FailureClass::Cancelled);
}
