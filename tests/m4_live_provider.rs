//! M4 provider gate: streaming text, tool round-trip/continuation và compaction summary dùng
//! model thật, qua **public Orchestrator** với gateway thật (`llm` 1.3.8).
//!
//! `#[ignore]`, gọi riêng:
//!   cargo test --test m4_live_provider -- --ignored --nocapture
//!
//! Cấu hình như `m2_live_provider.rs` (`.env.live` hoặc `G0_OPENAI_*`). Thiếu credentials →
//! panic "BLOCKED". Chỉ kiểm assembled content/order và capability của provider; số fragment
//! cụ thể là fixture deterministic (fake), không đòi provider phát đúng 20 chunk.
//! Usage: `llm` 1.3.8 không phát usage trong stream → ghi nhận "thiếu usage" (không lỗi).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, CallerContext, HostTool, LlmBackend, LlmConfig,
    LlmProviderConfig, Orchestrator, OrchestratorConfig, ProviderAuth, RunStatus, SecretString,
    ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};

const CODE: &str = "LIME-7731";

fn load_env_file() -> std::collections::HashMap<String, String> {
    let path = std::env::var("LIVE_ENV_FILE")
        .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/.env.live").to_string());
    let Ok(text) = std::fs::read_to_string(path) else {
        return Default::default();
    };
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (k, v) = line.split_once('=')?;
            let v = v.trim();
            let v = v
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .or_else(|| v.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
                .unwrap_or(v);
            Some((k.trim().to_string(), v.to_string()))
        })
        .collect()
}

fn env(k: &str) -> Option<String> {
    static FILE: std::sync::OnceLock<std::collections::HashMap<String, String>> =
        std::sync::OnceLock::new();
    std::env::var(k)
        .ok()
        .or_else(|| FILE.get_or_init(load_env_file).get(k).cloned())
        .filter(|v| !v.trim().is_empty())
}

/// Tool trả output dài, để vài exchange là đủ vượt ngưỡng compaction đã hạ thấp.
struct LookupCode(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl HostTool for LookupCode {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "lookup_code".into(),
            description: "Return the secret project code for a topic.".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "topic": { "type": "string" } },
                "required": ["topic"]
            }),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let topic = args["topic"].as_str().unwrap_or("?");
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!(
                "The secret code for {topic} is {CODE}. {}",
                "Background notes: none of this text matters. ".repeat(60)
            ),
            metadata: Value::Null,
        })
    }
}

async fn run(o: &Orchestrator, who: &CallerContext, sid: &str, text: &str) -> AgentResult {
    let turn = o.submit(who, sid, text.into()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        let r = o.result(who, sid, &turn.turn_id).await.unwrap();
        if r.status != RunStatus::Running {
            return r;
        }
        assert!(tokio::time::Instant::now() < deadline, "live run stuck");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_streaming_tool_continuation_and_compaction_summary() {
    let (Some(key), Some(base), Some(model)) = (
        env("G0_OPENAI_API_KEY").or_else(|| env("OPENAI_API_KEY")),
        env("G0_OPENAI_BASE_URL"),
        env("G0_OPENAI_MODEL"),
    ) else {
        panic!("BLOCKED: fill .env.live (see .env.live.example) or set G0_OPENAI_BASE_URL, G0_OPENAI_MODEL, G0_OPENAI_API_KEY");
    };

    let dir = tempfile::tempdir().unwrap();
    let prompt = dir.path().join("root.md");
    std::fs::write(
        &prompt,
        "You are a terse assistant. Use the lookup_code tool whenever the user asks for a code.",
    )
    .unwrap();
    let store = dir.path().join("store");

    let mut cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "g0-openai".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(base),
                auth: ProviderAuth::ApiKey(SecretString::new(key)),
                request_timeout: Duration::from_secs(90),
            }],
        },
        vec![AgentDefinition::new("root", "g0-openai", model)
            .with_mode(AgentMode::Primary)
            .with_prompt_path(&prompt)
            .with_allowed_tools(["lookup_code"])],
        &store,
    );
    // Hạ ngưỡng để compaction chạy với vài exchange: ~1.5k token ước lượng.
    cfg.runtime.context_limit_tokens = 2_000;
    cfg.runtime.compaction_threshold_percent = 60;
    cfg.runtime.compaction_keep_recent = 2;

    let runs = Arc::new(AtomicUsize::new(0));
    let o = Orchestrator::builder(cfg)
        .register_tool(Arc::new(LookupCode(runs.clone())))
        .build()
        .await
        .unwrap();
    let who = CallerContext::new("live", "g0");
    let sid = o.create_session(&who, "root").await.unwrap();

    // 1. Text streaming: response gom đúng và commit một message.
    let r = run(&o, &who, &sid, "Reply with exactly the word: pong").await;
    assert_eq!(r.status, RunStatus::Completed, "error: {:?}", r.error);
    let out = r.output.expect("final output");
    println!("text turn: completed, output_chars={}", out.chars().count());
    assert!(out.to_lowercase().contains("pong"), "unexpected: {out}");

    // 2. Tool round-trip + continuation.
    let r = run(
        &o,
        &who,
        &sid,
        "Use lookup_code for topic 'alpha' and then for topic 'beta' (one call at a time), \
         then tell me the code.",
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "error: {:?}", r.error);
    let out = r.output.expect("final output");
    println!(
        "tool turn: completed, tool_runs={}, output_chars={}",
        runs.load(Ordering::SeqCst),
        out.chars().count()
    );
    assert!(
        runs.load(Ordering::SeqCst) >= 1,
        "model never called the tool"
    );
    assert!(out.contains(CODE), "answer did not use the tool result");

    // 3. Compaction summary bằng model thật: context đã vượt ngưỡng → checkpoint được ghi và
    //    run vẫn hoàn tất, vẫn nhớ được thông tin trong summary/phần giữ nguyên.
    let r = run(
        &o,
        &who,
        &sid,
        "Use lookup_code for topic 'gamma', then repeat the secret code.",
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "error: {:?}", r.error);
    assert!(r.output.expect("final output").contains(CODE));

    let opts = SqliteConnectOptions::new()
        .filename(store.join("orchestrator.db"))
        .foreign_keys(true);
    let mut c = SqliteConnection::connect_with(&opts).await.unwrap();
    let checkpoints: i64 = sqlx::query("SELECT COUNT(*) FROM messages WHERE role = 'compaction'")
        .fetch_one(&mut c)
        .await
        .unwrap()
        .get(0);
    let usage: (i64, i64) = {
        let row = sqlx::query("SELECT SUM(input_tokens), SUM(output_tokens) FROM agent_runs")
            .fetch_one(&mut c)
            .await
            .unwrap();
        (row.get(0), row.get(1))
    };
    println!("compaction checkpoints={checkpoints}, usage(input,output)={usage:?} (0 = provider/crate reports no usage)");
    assert!(checkpoints >= 1, "no compaction checkpoint was produced");
    o.shutdown().await.unwrap();
}
