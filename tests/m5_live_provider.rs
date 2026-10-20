//! M5 provider gate: cancel, request timeout và safe restart qua **adapter thật** (`llm` 1.3.8),
//! đi qua public `Orchestrator`.
//!
//! `#[ignore]`, gọi riêng:
//!   cargo test --test m5_live_provider -- --ignored --nocapture --test-threads=1
//!
//! Cấu hình như `m2_live_provider.rs` (`.env.live` hoặc `G0_OPENAI_*`). Thiếu credentials →
//! panic "BLOCKED". Chỉ chứng minh hành vi của **một** backend OpenAI-compatible; không chứng
//! minh reconcile mutation thật (không có tool Mutating thật ở đây) và không thay AT-48.

use std::path::Path;
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, CallerContext, LlmBackend, LlmConfig,
    LlmProviderConfig, Orchestrator, OrchestratorConfig, ProviderAuth, RunStatus, SecretString,
};

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

struct Live {
    key: String,
    base: String,
    model: String,
}

fn live() -> Live {
    let (Some(key), Some(base), Some(model)) = (
        env("G0_OPENAI_API_KEY").or_else(|| env("OPENAI_API_KEY")),
        env("G0_OPENAI_BASE_URL"),
        env("G0_OPENAI_MODEL"),
    ) else {
        panic!("BLOCKED: fill .env.live (see .env.live.example) or set G0_OPENAI_BASE_URL, G0_OPENAI_MODEL, G0_OPENAI_API_KEY");
    };
    Live { key, base, model }
}

fn config(l: &Live, dir: &Path, request_timeout: Duration) -> OrchestratorConfig {
    let prompt = dir.join("root.md");
    std::fs::write(&prompt, "You are a helpful assistant.").unwrap();
    let mut cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "g0-openai".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(l.base.clone()),
                auth: ProviderAuth::ApiKey(SecretString::new(l.key.clone())),
                request_timeout,
            }],
        },
        vec![AgentDefinition::new("root", "g0-openai", l.model.clone())
            .with_mode(AgentMode::Primary)
            .with_prompt_path(&prompt)],
        dir.join("store"),
    );
    cfg.runtime.supervisor_sweep = Duration::from_millis(200);
    cfg.runtime.shutdown_grace = Duration::from_secs(30);
    cfg
}

async fn wait_terminal(
    o: &Orchestrator,
    who: &CallerContext,
    sid: &str,
    turn: &str,
) -> AgentResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
    loop {
        let r = o.result(who, sid, turn).await.unwrap();
        if r.status != RunStatus::Running {
            return r;
        }
        assert!(tokio::time::Instant::now() < deadline, "live run stuck");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

const LONG_PROMPT: &str = "Write a very long, detailed essay (at least 1500 words) about the \
history of the printing press. Do not stop early.";

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_cancel_stops_an_in_flight_request_and_the_session_stays_usable() {
    let l = live();
    let dir = tempfile::tempdir().unwrap();
    let o = Orchestrator::builder(config(&l, dir.path(), Duration::from_secs(120)))
        .build()
        .await
        .unwrap();
    let who = CallerContext::new("live", "g0");
    let sid = o.create_session(&who, "root").await.unwrap();
    let t = o.submit(&who, &sid, LONG_PROMPT.into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let started = std::time::Instant::now();
    o.cancel(&who, &sid).await.unwrap();
    let r = wait_terminal(&o, &who, &sid, &t.turn_id).await;
    println!(
        "cancel: status={:?} code={:?} stopped_in={:?}",
        r.status,
        r.error.as_ref().map(|e| e.code.clone()),
        started.elapsed()
    );
    assert_eq!(r.status, RunStatus::Cancelled, "{:?}", r.error);
    assert_eq!(r.output, None);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the real adapter must stop promptly on cancel"
    );

    let t2 = o
        .submit(&who, &sid, "Reply with exactly the word: pong".into())
        .await
        .unwrap();
    let r = wait_terminal(&o, &who, &sid, &t2.turn_id).await;
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert!(r.output.unwrap().to_lowercase().contains("pong"));
    o.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_request_timeout_is_a_sanitized_timed_out_result() {
    let l = live();
    let dir = tempfile::tempdir().unwrap();
    // Timeout cực ngắn cho *một* request: không được chờ mãi, không lộ endpoint/key.
    let o = Orchestrator::builder(config(&l, dir.path(), Duration::from_millis(1)))
        .build()
        .await
        .unwrap();
    let who = CallerContext::new("live", "g0");
    let sid = o.create_session(&who, "root").await.unwrap();
    let t = o.submit(&who, &sid, LONG_PROMPT.into()).await.unwrap();
    let r = wait_terminal(&o, &who, &sid, &t.turn_id).await;
    let e = r.error.clone().expect("an error is reported");
    println!(
        "timeout: status={:?} code={} message={}",
        r.status, e.code, e.message
    );
    assert_eq!(r.status, RunStatus::TimedOut, "{e:?}");
    assert_eq!(e.code, "PROVIDER_TIMEOUT");
    assert!(!e.message.contains(&l.key) && !e.message.contains(&l.base));
    assert_eq!(r.output, None);
    o.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_shutdown_mid_request_then_safe_restart_resumes_the_same_turn() {
    let l = live();
    let dir = tempfile::tempdir().unwrap();
    let who = CallerContext::new("live", "g0");
    let o = Orchestrator::builder(config(&l, dir.path(), Duration::from_secs(120)))
        .build()
        .await
        .unwrap();
    let sid = o.create_session(&who, "root").await.unwrap();
    let t = o
        .submit(&who, &sid, "Reply with exactly the word: pong".into())
        .await
        .unwrap();
    // Ngắt giữa request thật (cancel hợp tác qua adapter) rồi dựng lại engine mới.
    o.shutdown().await.unwrap();
    drop(o);

    let o = Orchestrator::builder(config(&l, dir.path(), Duration::from_secs(120)))
        .build()
        .await
        .unwrap();
    let r = wait_terminal(&o, &who, &sid, &t.turn_id).await;
    println!(
        "restart: status={:?} attempt={} output_chars={}",
        r.status,
        r.attempt,
        r.output.as_ref().map_or(0, |s| s.chars().count())
    );
    assert_eq!(r.status, RunStatus::Completed, "{:?}", r.error);
    assert_eq!(r.turn_id, t.turn_id);
    // Shutdown có thể đến sau khi run đã xong (attempt 0) hoặc ngắt nó (attempt 1): cả hai
    // đều hợp lệ; điều không được xảy ra là mất turn hoặc nhân đôi câu trả lời.
    assert!(r.output.unwrap().to_lowercase().contains("pong"));
    let history = o
        .history(&who, &sid, orchestrator_agent_rs::PageRequest::first(50))
        .await
        .unwrap()
        .items;
    assert_eq!(history.len(), 2, "one user input, one final answer");
    o.shutdown().await.unwrap();
}
