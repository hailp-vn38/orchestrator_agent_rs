//! M1 provider gate: host text + multi-turn smoke qua **public Orchestrator** với gateway
//! thật (`llm` 1.3.8), trên cặp G0 (OpenAI-compatible LAN).
//!
//! Test này `#[ignore]` và PHẢI được gọi riêng:
//!   cargo test --test m1_live_provider -- --ignored --nocapture
//!
//! Cấu hình: file `.env.live` ở root repo (copy từ `.env.live.example`, đã gitignore; đổi
//! đường dẫn bằng `LIVE_ENV_FILE`) hoặc biến môi trường `G0_OPENAI_*` (env ưu tiên hơn file).
//!
//! Thiếu credentials → panic "BLOCKED" (không bao giờ báo PASS khi chưa chạy). Secret không
//! được in ra.

use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, CallerContext, LlmBackend, LlmConfig, LlmProviderConfig,
    MessageRole, Orchestrator, OrchestratorConfig, PageRequest, ProviderAuth, RunStatus,
    SecretString,
};

/// Parse `KEY=VALUE` (bỏ dòng trống/comment, bỏ cặp nháy bao ngoài).
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

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_text_and_multi_turn_through_public_orchestrator() {
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
        "Bạn là trợ lý ngắn gọn. Trả lời đúng yêu cầu, không giải thích thêm.",
    )
    .unwrap();
    let cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "g0-openai".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(base),
                auth: ProviderAuth::ApiKey(SecretString::new(key)),
                request_timeout: Duration::from_secs(60),
            }],
        },
        vec![AgentDefinition::new("root", "g0-openai", model)
            .with_mode(AgentMode::Primary)
            .with_prompt_path(&prompt)],
        dir.path().join("store"),
    );
    let o = Orchestrator::builder(cfg).build().await.unwrap();
    let who = CallerContext::new("live", "g0");
    let sid = o.create_session(&who, "root").await.unwrap();

    let mut texts = Vec::new();
    for prompt in [
        "Hãy nhớ mã bí mật: ZEBRA42. Chỉ trả lời OK.",
        "Mã bí mật tôi vừa đưa là gì? Chỉ trả lời đúng mã.",
    ] {
        let turn = o.submit(&who, &sid, prompt.into()).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        let r = loop {
            let r = o.result(&who, &sid, &turn.turn_id).await.unwrap();
            if r.status != RunStatus::Running {
                break r;
            }
            assert!(tokio::time::Instant::now() < deadline, "live run stuck");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(r.status, RunStatus::Completed, "error: {:?}", r.error);
        let out = r.output.expect("final output");
        println!("turn status=completed output_chars={}", out.chars().count());
        texts.push(out);
    }
    assert!(
        texts[1].to_uppercase().contains("ZEBRA42"),
        "model did not use history from the store"
    );

    let hist = o.history(&who, &sid, PageRequest::first(10)).await.unwrap();
    let roles: Vec<MessageRole> = hist.items.iter().map(|m| m.role).collect();
    assert_eq!(
        roles,
        [
            MessageRole::User,
            MessageRole::Assistant,
            MessageRole::User,
            MessageRole::Assistant
        ]
    );
    o.shutdown().await.unwrap();
}
