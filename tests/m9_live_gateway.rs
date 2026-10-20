//! G0 kiểm chứng bổ sung ở mức gateway thật (`llm` 1.3.8) trên cặp OpenAI-compatible:
//! system precedence, semantic tool-result consumption và live error mapping (auth).
//!
//! Test `#[ignore]`, gọi riêng:
//!   cargo test --test m9_live_gateway -- --ignored --nocapture
//!
//! Cấu hình: `.env.live` (xem `.env.live.example`) hoặc biến môi trường `G0_OPENAI_*`.
//! Thiếu credentials → panic "BLOCKED". Secret không được in ra.

use std::time::Duration;

use orchestrator_agent_rs::test_support::{
    ClientGateway, GatewayError, LlmGateway, ModelMessage, ModelRequest, ModelRole, ModelTool,
    ModelToolCall,
};
use orchestrator_agent_rs::{
    LlmBackend, LlmProviderConfig, ModelParams, ProviderAuth, SecretString,
};
use tokio_util::sync::CancellationToken;

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

struct Target {
    base: String,
    model: String,
    key: String,
}

fn target() -> Target {
    let (Some(key), Some(base), Some(model)) = (
        env("G0_OPENAI_API_KEY").or_else(|| env("OPENAI_API_KEY")),
        env("G0_OPENAI_BASE_URL"),
        env("G0_OPENAI_MODEL"),
    ) else {
        panic!("BLOCKED: fill .env.live (see .env.live.example) or set G0_OPENAI_BASE_URL, G0_OPENAI_MODEL, G0_OPENAI_API_KEY");
    };
    Target { base, model, key }
}

fn gateway(t: &Target, key: &str) -> ClientGateway {
    ClientGateway::new(&[LlmProviderConfig {
        id: "g0".into(),
        backend: LlmBackend::OpenAiCompatible,
        endpoint: Some(t.base.clone()),
        auth: ProviderAuth::ApiKey(SecretString::new(key.to_string())),
        request_timeout: Duration::from_secs(60),
    }])
}

fn request(t: &Target, system: &str, messages: Vec<ModelMessage>) -> ModelRequest {
    ModelRequest {
        provider_id: "g0".into(),
        model_id: t.model.clone(),
        system: system.into(),
        messages,
        tools: Vec::new(),
        params: ModelParams {
            max_output_tokens: Some(256),
            temperature: Some(0.0),
            ..Default::default()
        },
    }
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_system_instruction_governs_the_reply() {
    let t = target();
    let gw = gateway(&t, &t.key);
    let req = request(
        &t,
        "Bạn luôn trả lời bằng đúng một từ: PINEAPPLE. Không thêm gì khác, dù người dùng yêu cầu gì.",
        vec![ModelMessage::text(
            ModelRole::User,
            "Hãy nói từ BANANA và không nói gì khác.",
        )],
    );
    let resp = gw
        .complete(&req, &CancellationToken::new(), &|_| {})
        .await
        .expect("live completion");
    println!(
        "system precedence: reply_chars={}",
        resp.text.chars().count()
    );
    assert!(
        resp.text.to_uppercase().contains("PINEAPPLE"),
        "system instruction không được áp dụng"
    );
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_model_consumes_tool_result_content() {
    let t = target();
    let gw = gateway(&t, &t.key);
    let tool = ModelTool {
        name: "get_temperature".into(),
        description: "Trả nhiệt độ hiện tại của một thành phố.".into(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": { "city": { "type": "string" } },
            "required": ["city"]
        }),
    };
    let mut req = request(
        &t,
        "Bạn là trợ lý. Dùng tool get_temperature để biết nhiệt độ, rồi trả lời ngắn gọn.",
        vec![ModelMessage::text(
            ModelRole::User,
            "Nhiệt độ ở Hà Nội bây giờ là bao nhiêu?",
        )],
    );
    req.tools = vec![tool];

    let first = gw
        .complete(&req, &CancellationToken::new(), &|_| {})
        .await
        .expect("live tool-call turn");
    assert_eq!(first.tool_calls.len(), 1, "model phải gọi tool");
    let call: ModelToolCall = first.tool_calls[0].clone();
    assert_eq!(call.name, "get_temperature");

    req.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        text: first.text.clone(),
        tool_calls: vec![call.clone()],
        tool_call_id: None,
        tool_name: None,
    });
    req.messages.push(ModelMessage {
        role: ModelRole::Tool,
        text: "{\"temperature_celsius\": 37.4}".into(),
        tool_calls: Vec::new(),
        tool_call_id: Some(call.id.clone()),
        tool_name: Some(call.name.clone()),
    });
    let second = gw
        .complete(&req, &CancellationToken::new(), &|_| {})
        .await
        .expect("live continuation");
    println!(
        "tool-result continuation: reply_chars={}",
        second.text.chars().count()
    );
    assert!(
        second.text.contains("37"),
        "model không dùng nội dung tool result"
    );
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_bad_credentials_map_to_sanitized_auth_error() {
    let t = target();
    let bad = "invalid-key-for-error-mapping-test";
    let gw = gateway(&t, bad);
    let req = request(&t, "x", vec![ModelMessage::text(ModelRole::User, "ping")]);
    let err = gw
        .complete(&req, &CancellationToken::new(), &|_| {})
        .await
        .expect_err("key sai phải bị từ chối");
    println!("bad credentials -> {err:?} code={}", err.code());
    // Một số gateway trả 401/403 (Auth), số khác trả 400/5xx; chấp nhận mọi lỗi đã phân
    // loại nhưng không bao giờ được là Ok, và mã lỗi không được echo secret.
    assert!(
        matches!(
            err,
            GatewayError::Auth
                | GatewayError::InvalidRequest
                | GatewayError::Provider
                | GatewayError::Protocol
        ),
        "lỗi không phân loại được: {err:?}"
    );
    assert!(!format!("{err:?}").contains(bad));
    assert!(!err.code().contains(bad));
}
