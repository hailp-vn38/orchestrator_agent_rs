//! M2 provider gate: tool round-trip + skill continuation qua **public Orchestrator** với
//! gateway thật (`llm` 1.3.8), trên cặp G0 (OpenAI-compatible LAN).
//!
//! Test này `#[ignore]` và PHẢI được gọi riêng:
//!   cargo test --test m2_live_provider -- --ignored --nocapture
//!
//! Cấu hình như `m1_live_provider.rs` (`.env.live` hoặc `G0_OPENAI_*`). Thiếu credentials →
//! panic "BLOCKED" (không bao giờ báo PASS khi chưa chạy). Secret không được in ra.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, CallerContext, HostTool, LlmBackend, LlmConfig,
    LlmProviderConfig, MessageRole, Orchestrator, OrchestratorConfig, PageRequest, ProviderAuth,
    RunStatus, SecretString, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};

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

/// Tool read-only trả về mã mà model không thể đoán nếu không gọi tool.
struct LookupCode(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl HostTool for LookupCode {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "lookup_code".into(),
            description: "Return the secret project code. Takes no meaningful input.".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "topic": { "type": "string" } },
                "required": ["topic"]
            }),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(&self, ctx: ToolContext, _args: Value) -> Result<ToolOutcome, ToolError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!("The secret project code is {CODE}."),
            metadata: Value::Null,
        })
    }
}

async fn run(o: &Orchestrator, who: &CallerContext, sid: &str, text: &str) -> AgentResult {
    let turn = o.submit(who, sid, text.into()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
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
async fn live_tool_round_trip_and_skill_continuation() {
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
        "You are a terse assistant. Always use a tool or skill when the user asks you to.",
    )
    .unwrap();
    let skill_dir = dir.path().join("skills/pirate");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: pirate\ndescription: Rules for answering in the voice-code format. Load it when asked to follow the pirate skill.\n---\nWhen you have loaded this skill, end your next answer with the exact token SKILL-OK-4410.\n",
    )
    .unwrap();

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
            .with_allowed_tools(["lookup_code", "skill"])
            .with_allowed_skills(["pirate"])],
        dir.path().join("store"),
    );
    cfg.skills_dir = Some(dir.path().join("skills"));

    let runs = Arc::new(AtomicUsize::new(0));
    let o = Orchestrator::builder(cfg)
        .register_tool(Arc::new(LookupCode(runs.clone())))
        .build()
        .await
        .unwrap();
    let who = CallerContext::new("live", "g0");
    let sid = o.create_session(&who, "root").await.unwrap();

    // 1. Tool round-trip: model phải gọi tool, nhận kết quả, rồi trả lời dựa vào kết quả.
    let r = run(
        &o,
        &who,
        &sid,
        "Use the lookup_code tool with topic 'project' and tell me the code it returns.",
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "error: {:?}", r.error);
    let out = r.output.expect("final output");
    println!("tool turn: completed, output_chars={}", out.chars().count());
    assert!(
        runs.load(Ordering::SeqCst) >= 1,
        "model never called the tool"
    );
    assert!(out.contains(CODE), "answer did not use the tool result");

    // 2. Skill continuation: nạp skill rồi tiếp tục làm theo chỉ dẫn của skill.
    let r = run(
        &o,
        &who,
        &sid,
        "Load the pirate skill with the skill tool, then follow its instructions in your answer.",
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "error: {:?}", r.error);
    let out = r.output.expect("final output");
    println!(
        "skill turn: completed, output_chars={}",
        out.chars().count()
    );
    assert!(
        out.contains("SKILL-OK-4410"),
        "model did not continue with the skill"
    );

    let hist = o.history(&who, &sid, PageRequest::first(50)).await.unwrap();
    let tools = hist
        .items
        .iter()
        .filter(|m| m.role == MessageRole::Tool)
        .count();
    assert!(
        tools >= 2,
        "expected tool + skill results in the session history"
    );
    o.shutdown().await.unwrap();
}
