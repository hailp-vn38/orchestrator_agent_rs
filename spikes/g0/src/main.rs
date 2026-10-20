//! Explicit live provider smoke (AT-48). Run: `cargo run --release -- live`.
//! Missing credentials => BLOCKED (never PASS). Output is redacted: only error *codes*,
//! endpoint host identity and model ids are printed; never keys or raw provider text on errors.

use std::time::{Duration, Instant};

use g0_spike::gateway::*;
use llm::chat::{ChatMessage, Tool};
use llm::{FunctionCall, ToolCall};
use tokio_util::sync::CancellationToken;

struct Target {
    label: &'static str,
    cfg: ProviderConfig,
    key: Option<String>,
}

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.trim().is_empty())
}

fn targets() -> Vec<Target> {
    let t = Duration::from_secs(60);
    vec![
        Target {
            label: "openai",
            key: env("G0_OPENAI_API_KEY").or_else(|| env("OPENAI_API_KEY")),
            cfg: ProviderConfig {
                provider_id: "openai".into(),
                backend: Backend::OpenAi,
                model_id: env("G0_OPENAI_MODEL").unwrap_or_else(|| "gpt-4o-mini".into()),
                base_url: env("G0_OPENAI_BASE_URL"),
                request_timeout: t,
                max_tokens: 256,
            },
        },
        Target {
            label: "anthropic",
            key: env("G0_ANTHROPIC_API_KEY")
                .or_else(|| env("ANTHROPIC_API_KEY"))
                .or_else(|| env("ANTHROPIC_AUTH_TOKEN")),
            cfg: ProviderConfig {
                provider_id: "anthropic".into(),
                backend: Backend::Anthropic,
                model_id: env("G0_ANTHROPIC_MODEL").unwrap_or_else(|| "claude-sonnet-4-5".into()),
                base_url: env("G0_ANTHROPIC_BASE_URL").or_else(|| env("ANTHROPIC_BASE_URL")),
                request_timeout: t,
                max_tokens: 256,
            },
        },
    ]
}

fn weather_tool() -> Tool {
    Tool {
        tool_type: "function".into(),
        function: llm::chat::FunctionTool {
            name: "get_weather".into(),
            description: "Get the weather for a city".into(),
            parameters: serde_json::json!({
                "type":"object",
                "properties":{"city":{"type":"string","description":"City name"}},
                "required":["city"]
            }),
        },
        cache_control: None,
    }
}

type R = Result<String, String>;

async fn run_stream(
    c: &dyn llm::LLMProvider,
    msgs: &[ChatMessage],
    tools: Option<&[Tool]>,
    timeout: Duration,
) -> Result<CollectedResponse, GatewayError> {
    let s = stream_with_tools(c, msgs, tools).await?;
    collect(s, &CancellationToken::new(), timeout).await
}

fn fmt_err(e: GatewayError) -> String {
    format!("FAIL({})", e.code())
}

async fn smoke(t: &Target, c: &dyn llm::LLMProvider) -> Vec<(&'static str, R)> {
    let to = t.cfg.request_timeout;
    let mut out: Vec<(&'static str, R)> = vec![];

    // 1. text + system instruction + streaming deltas + Finished
    let m1 = vec![ChatMessage::user()
        .content("Reply with exactly the word: PINEAPPLE")
        .build()];
    let r1 = run_stream(c, &m1, None, to).await;
    let _text1 = match &r1 {
        Ok(r) => {
            let ok = r.text.to_uppercase().contains("PINEAPPLE") && !r.finish_reason.is_empty();
            out.push((
                "text+system+finished",
                if ok {
                    Ok(format!("deltas={} finish={}", r.deltas, r.finish_reason))
                } else {
                    Err("FAIL(unexpected_content)".into())
                },
            ));
            r.text.clone()
        }
        Err(e) => {
            out.push(("text+system+finished", Err(fmt_err(e.clone()))));
            String::new()
        }
    };

    // 2. multi-turn memory
    let m2 = vec![
        ChatMessage::user()
            .content("Remember the secret code word: ZEBRA42. Say OK.")
            .build(),
        ChatMessage::assistant().content("OK").build(),
        ChatMessage::user()
            .content("What was the code word? Answer with the word only.")
            .build(),
    ];
    out.push((
        "multi-turn",
        match run_stream(c, &m2, None, to).await {
            Ok(r) if r.text.to_uppercase().contains("ZEBRA42") => {
                Ok(format!("deltas={}", r.deltas))
            }
            Ok(_) => Err("FAIL(unexpected_content)".into()),
            Err(e) => Err(fmt_err(e)),
        },
    ));

    // 3. tool call -> result -> continuation, IDs preserved
    let tools = [weather_tool()];
    let m3 = vec![ChatMessage::user()
        .content("What's the weather in Hanoi? Use the get_weather tool.")
        .build()];
    let first = run_stream(c, &m3, Some(&tools), to).await;
    match first {
        Err(e) => {
            out.push(("tool-call-assembly", Err(fmt_err(e))));
            out.push((
                "tool-result-continuation",
                Err("NOT_RUN(previous step failed)".into()),
            ));
        }
        Ok(r) if r.tool_calls.is_empty() => {
            out.push((
                "tool-call-assembly",
                Err("FAIL(no_tool_call_emitted)".into()),
            ));
            out.push((
                "tool-result-continuation",
                Err("NOT_RUN(previous step failed)".into()),
            ));
        }
        Ok(r) => {
            let call = r.tool_calls[0].clone();
            out.push((
                "tool-call-assembly",
                Ok(format!(
                    "calls={} id_len={} name={} fragments_seen={} finish={}",
                    r.tool_calls.len(),
                    call.id.len(),
                    call.function.name,
                    r.fragments_seen,
                    r.finish_reason
                )),
            ));
            let result_call = ToolCall {
                id: call.id.clone(),
                call_type: "function".into(),
                function: FunctionCall {
                    name: call.function.name.clone(),
                    arguments: "{\"temp_c\":31,\"sky\":\"sunny\"}".into(),
                },
            };
            let mut m4 = m3.clone();
            m4.push(
                ChatMessage::assistant()
                    .tool_use(r.tool_calls.clone())
                    .build(),
            );
            m4.push(ChatMessage::user().tool_result(vec![result_call]).build());
            out.push((
                "tool-result-continuation",
                match run_stream(c, &m4, Some(&tools), to).await {
                    Ok(r2) if !r2.text.is_empty() => {
                        Ok(format!("deltas={} finish={}", r2.deltas, r2.finish_reason))
                    }
                    Ok(_) => Err("FAIL(empty_continuation)".into()),
                    Err(e) => Err(fmt_err(e)),
                },
            ));
        }
    }

    // 4. cancel mid-flight
    let tok = CancellationToken::new();
    let tok2 = tok.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        tok2.cancel();
    });
    let m5 = vec![ChatMessage::user()
        .content("Write a 500-word essay about the ocean.")
        .build()];
    out.push((
        "cancel",
        match stream_with_tools(c, &m5, None).await {
            Ok(s) => match collect(s, &tok, to).await {
                Err(GatewayError::Cancelled) => Ok("cancelled".into()),
                Ok(_) => Err("INCONCLUSIVE(finished before cancel)".into()),
                Err(e) => Err(fmt_err(e)),
            },
            Err(e) => Err(fmt_err(e)),
        },
    ));

    // 5. gateway timeout
    out.push((
        "timeout",
        match stream_with_tools(c, &m5, None).await {
            Ok(s) => match collect(s, &CancellationToken::new(), Duration::from_millis(20)).await {
                Err(GatewayError::Timeout) => Ok("llm_timeout".into()),
                Ok(_) => Err("INCONCLUSIVE(finished in 20ms)".into()),
                Err(e) => Err(fmt_err(e)),
            },
            Err(e) => Err(fmt_err(e)),
        },
    ));

    out
}

#[tokio::main]
async fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    if mode != "live" {
        eprintln!("usage: g0-spike live [openai|anthropic]");
        std::process::exit(2);
    }
    let selected = std::env::args().nth(2);
    if selected
        .as_deref()
        .is_some_and(|v| !matches!(v, "openai" | "anthropic"))
    {
        eprintln!("unknown provider selector");
        std::process::exit(2);
    }
    let mut any_fail = false;
    for t in targets()
        .into_iter()
        .filter(|t| selected.as_deref().is_none_or(|v| v == t.label))
    {
        println!(
            "== provider={} backend={:?} model={} endpoint={} fingerprint={}",
            t.label,
            t.cfg.backend,
            t.cfg.model_id,
            t.cfg.endpoint_identity(),
            t.cfg.fingerprint()
        );
        let Some(key) = &t.key else {
            println!(
                "BLOCKED: no credentials in env for {} (checked G0_*/standard vars)",
                t.label
            );
            any_fail = true;
            continue;
        };
        let client = match build_client(
            &t.cfg,
            key,
            Some("You are a terse assistant. Follow instructions exactly."),
        ) {
            Ok(c) => c,
            Err(e) => {
                println!("FAIL(build:{})", map_error(&e).code());
                any_fail = true;
                continue;
            }
        };
        let started = Instant::now();
        for (name, r) in smoke(&t, client.as_ref()).await {
            match r {
                Ok(d) => println!("PASS {name}: {d}"),
                Err(d) => {
                    any_fail = true;
                    println!(
                        "{} {name}: {d}",
                        if d.starts_with("NOT_RUN") {
                            "NOT_RUN"
                        } else {
                            "FAIL"
                        }
                    );
                }
            }
        }
        println!("elapsed_ms={}", started.elapsed().as_millis());
    }
    println!(
        "live_overall={}",
        if any_fail { "NOT_ALL_PASS" } else { "ALL_PASS" }
    );
    if any_fail {
        std::process::exit(1);
    }
}
