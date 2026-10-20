//! M8 live: các trần "max token/byte" qua public `Orchestrator` + model thật (`.env.live`).
//!
//!   1. `tool_output_cap_bytes`: tool trả output lớn → model chỉ thấy phần đầu + marker cắt;
//!      phần đuôi không bao giờ tới model, run vẫn hoàn tất.
//!   2. `tool_result_hard_cap_bytes`: vượt trần persist → `TOOL_OUTPUT_TOO_LARGE`, invocation
//!      `failed`, model nhận lỗi sạch và run vẫn hoàn tất (không crash).
//!   3. `AgentDefinition::context_limit_tokens` (override theo agent) thấp → auto compaction
//!      chạy dù `RuntimeConfig::context_limit_tokens` lớn; fact ở đầu hội thoại vẫn nhớ được.
//!
//! Trần output token mỗi request (`MAX_OUTPUT_TOKENS`) hiện hardcode 4096 trong gateway, chưa
//! cấu hình được nên không có live test riêng (xem phần báo cáo).
//!
//! Chạy: cargo test --test m8_live_max_tokens -- --ignored --nocapture

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, CallerContext, HostTool, LlmBackend, LlmConfig,
    LlmProviderConfig, MessageRole, Orchestrator, OrchestratorConfig, PageRequest, ProviderAuth,
    RunStatus, SecretString, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};

const HEAD: &str = "HEAD-6241";
const TAIL: &str = "TAIL-9087";

fn load_env_file() -> HashMap<String, String> {
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
    static FILE: std::sync::OnceLock<HashMap<String, String>> = std::sync::OnceLock::new();
    std::env::var(k)
        .ok()
        .or_else(|| FILE.get_or_init(load_env_file).get(k).cloned())
        .filter(|v| !v.trim().is_empty())
}

/// Tool trả `HEAD` + `filler_bytes` byte đệm + `TAIL`.
struct Blob {
    name: &'static str,
    filler_bytes: usize,
}

#[async_trait::async_trait]
impl HostTool for Blob {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: self.name.into(),
            description: format!("Return a large report ({}). Takes no input.", self.name),
            input_schema: json!({"type":"object","properties":{},"additionalProperties":false}),
            effect: ToolEffect::ReadOnly,
        }
    }
    async fn execute(&self, ctx: ToolContext, _: Value) -> Result<ToolOutcome, ToolError> {
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!("{HEAD} {} {TAIL}", ".".repeat(self.filler_bytes)),
            metadata: Value::Null,
        })
    }
}

async fn run(o: &Orchestrator, who: &CallerContext, sid: &str, text: &str) -> AgentResult {
    let turn = o.submit(who, sid, text.into()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
    loop {
        let r = o.result(who, sid, &turn.turn_id).await.unwrap();
        if r.status != RunStatus::Running {
            return r;
        }
        assert!(tokio::time::Instant::now() < deadline, "live run stuck");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

struct Live {
    base: String,
    key: String,
    model: String,
}

fn live() -> Live {
    let (Some(key), Some(base), Some(model)) = (
        env("G0_OPENAI_API_KEY").or_else(|| env("OPENAI_API_KEY")),
        env("G0_OPENAI_BASE_URL"),
        env("G0_OPENAI_MODEL"),
    ) else {
        panic!("BLOCKED: fill .env.live (see .env.live.example)");
    };
    Live { base, key, model }
}

fn config(l: &Live, dir: &std::path::Path, agent: AgentDefinition) -> OrchestratorConfig {
    OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "live".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(l.base.clone()),
                auth: ProviderAuth::ApiKey(SecretString::new(l.key.clone())),
                request_timeout: Duration::from_secs(90),
            }],
        },
        vec![agent],
        dir.join("store"),
    )
}

fn prompt(dir: &std::path::Path, text: &str) -> std::path::PathBuf {
    let p = dir.join("root.md");
    std::fs::write(&p, text).unwrap();
    p
}

async fn db(dir: &std::path::Path) -> SqliteConnection {
    let opts = SqliteConnectOptions::new().filename(dir.join("store/orchestrator.db"));
    SqliteConnection::connect_with(&opts).await.unwrap()
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_tool_output_is_capped_for_the_model_and_oversize_is_rejected() {
    let l = live();
    let dir = tempfile::tempdir().unwrap();
    let agent = AgentDefinition::new("root", "live", &l.model)
        .with_mode(AgentMode::Primary)
        .with_prompt_path(prompt(
            dir.path(),
            "You are terse. Call the requested report tool, then quote verbatim the first \
             token of its output and the last token of its output. If the output looks cut \
             off, or the tool errors, say so plainly.",
        ))
        .with_allowed_tools(["big_report", "huge_report"]);
    let mut cfg = config(&l, dir.path(), agent);
    cfg.runtime.tool_output_cap_bytes = 2 * 1024;
    cfg.runtime.tool_result_hard_cap_bytes = 16 * 1024;
    let o = Orchestrator::builder(cfg)
        .register_tool(Arc::new(Blob {
            name: "big_report",
            filler_bytes: 8 * 1024,
        }))
        .register_tool(Arc::new(Blob {
            name: "huge_report",
            filler_bytes: 32 * 1024,
        }))
        .build()
        .await
        .unwrap();
    let who = CallerContext::new("live", "m8");
    let sid = o.create_session(&who, "root").await.unwrap();

    // 1. Vượt cap model-visible (2 KiB) nhưng dưới hard cap (16 KiB): bị cắt + marker.
    let r = run(
        &o,
        &who,
        &sid,
        "Call big_report and give me its first and last token.",
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    let out = r.output.expect("final");
    println!("capped turn: {out}");
    assert!(out.contains(HEAD), "head must reach the model: {out}");
    assert!(
        !out.contains(TAIL),
        "tail was cut, model cannot know it: {out}"
    );
    let hist = o
        .history(&who, &sid, PageRequest::first(50))
        .await
        .unwrap()
        .items;
    let tool = hist
        .iter()
        .find(|m| m.role == MessageRole::Tool)
        .expect("tool msg");
    assert!(
        tool.text.contains("[output truncated"),
        "marker missing: {}",
        tool.text
    );
    assert!(
        tool.text.len() < 2 * 1024 + 200,
        "model-visible text exceeds cap"
    );
    assert!(!tool.text.contains(TAIL));

    // 2. Vượt hard cap: từ chối, không persist phần dư; run vẫn xong.
    let r = run(
        &o,
        &who,
        &sid,
        "Now call huge_report and give me its first and last token.",
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    println!("oversize turn: {}", r.output.unwrap_or_default());
    let mut c = db(dir.path()).await;
    let row = sqlx::query(
        "SELECT status, error_code, result_json FROM tool_invocations \
         WHERE tool_name = 'huge_report'",
    )
    .fetch_one(&mut c)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>(0), "failed");
    assert_eq!(
        row.get::<Option<String>, _>(1).as_deref(),
        Some("TOOL_OUTPUT_TOO_LARGE")
    );
    let result_json: Option<String> = row.get(2);
    assert!(
        result_json.is_none_or(|j| !j.contains(TAIL)),
        "oversize payload must not be persisted"
    );
    o.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_per_agent_context_limit_triggers_compaction_and_keeps_early_fact() {
    let l = live();
    let dir = tempfile::tempdir().unwrap();
    let agent = AgentDefinition::new("root", "live", &l.model)
        .with_mode(AgentMode::Primary)
        .with_prompt_path(prompt(
            dir.path(),
            "You are terse. Remember facts the user tells you. Call tools when asked.",
        ))
        .with_allowed_tools(["big_report"])
        // Override theo agent: rất thấp so với RuntimeConfig bên dưới.
        .with_context_limit_tokens(1_500);
    let mut cfg = config(&l, dir.path(), agent);
    cfg.runtime.context_limit_tokens = 1_000_000; // nếu override bị bỏ qua sẽ không compact
    cfg.runtime.compaction_threshold_percent = 60;
    cfg.runtime.compaction_keep_recent = 1;
    let o = Orchestrator::builder(cfg)
        .register_tool(Arc::new(Blob {
            name: "big_report",
            filler_bytes: 3 * 1024,
        }))
        .build()
        .await
        .unwrap();
    let who = CallerContext::new("live", "m8");
    let sid = o.create_session(&who, "root").await.unwrap();

    let r = run(
        &o,
        &who,
        &sid,
        "Remember: my project codename is MANGO-3305. Just say OK.",
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    for i in 0..3 {
        let r = run(
            &o,
            &who,
            &sid,
            &format!("Call big_report (round {i}), then say DONE."),
        )
        .await;
        assert_eq!(r.status, RunStatus::Completed, "round {i}: {:?}", r.error);
    }
    let r = run(
        &o,
        &who,
        &sid,
        "What is my project codename? Answer with the codename only.",
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    let out = r.output.expect("final");
    println!("recall after compaction: {out}");
    assert!(
        out.contains("MANGO-3305"),
        "early fact lost after compaction: {out}"
    );

    let mut c = db(dir.path()).await;
    let checkpoints: i64 = sqlx::query("SELECT COUNT(*) FROM messages WHERE role = 'compaction'")
        .fetch_one(&mut c)
        .await
        .unwrap()
        .get(0);
    println!("compaction checkpoints={checkpoints}");
    assert!(
        checkpoints >= 1,
        "per-agent context limit did not trigger compaction"
    );
    o.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_agent_max_output_tokens_caps_the_reply_length() {
    let l = live();
    let dir = tempfile::tempdir().unwrap();
    let agent = AgentDefinition::new("root", "live", &l.model)
        .with_mode(AgentMode::Primary)
        .with_prompt_path(prompt(dir.path(), "You are a verbose writer."))
        .with_max_output_tokens(24)
        .with_temperature(0.0);
    let o = Orchestrator::builder(config(&l, dir.path(), agent))
        .build()
        .await
        .unwrap();
    let who = CallerContext::new("live", "m8");
    let sid = o.create_session(&who, "root").await.unwrap();
    let r = run(
        &o,
        &who,
        &sid,
        "Write a very long essay (at least 1500 words) about the history of the bicycle.",
    )
    .await;
    // Bị cắt bởi trần output: run hoàn tất với text ngắn, hoặc provider/gateway báo lỗi sạch.
    println!("status={:?} error={:?}", r.status, r.error);
    if r.status == RunStatus::Completed {
        let out = r.output.expect("final");
        println!("capped reply ({} chars): {out}", out.chars().count());
        assert!(
            out.chars().count() < 400,
            "max_output_tokens=24 was ignored"
        );
        let mut c = db(dir.path()).await;
        let used: i64 = sqlx::query("SELECT COALESCE(SUM(output_tokens), 0) FROM agent_runs")
            .fetch_one(&mut c)
            .await
            .unwrap()
            .get(0);
        println!("output_tokens={used} (0 = provider reports no usage)");
        assert!(
            used <= 40,
            "provider generated {used} tokens despite cap 24"
        );
    } else {
        assert!(r.error.is_some(), "non-completed run must carry an error");
    }
    o.shutdown().await.unwrap();
}
