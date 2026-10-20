//! M0 gate: AT-46 (wiring/config/base contract) + rejection cases.

use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, ConfigReason, ErrorCode, HostTool, LlmBackend, LlmConfig,
    LlmProviderConfig, Orchestrator, OrchestratorConfig, OrchestratorError, ProviderAuth,
    SecretString, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};

const KEY: &str = "sk-SUPER-SECRET-VALUE";

type Case = (Box<dyn Fn(&mut OrchestratorConfig)>, ConfigReason);

struct FakeTool {
    name: String,
    schema: serde_json::Value,
}

impl FakeTool {
    fn ok(name: &str) -> Arc<dyn HostTool> {
        Arc::new(Self {
            name: name.into(),
            schema: serde_json::json!({
                "type": "object",
                "properties": { "q": { "type": "string" } },
                "required": ["q"]
            }),
        })
    }

    fn with_schema(name: &str, schema: serde_json::Value) -> Arc<dyn HostTool> {
        Arc::new(Self {
            name: name.into(),
            schema,
        })
    }
}

#[async_trait::async_trait]
impl HostTool for FakeTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: self.name.clone(),
            description: "fake".into(),
            input_schema: self.schema.clone(),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(
        &self,
        _context: ToolContext,
        _args: serde_json::Value,
    ) -> Result<ToolOutcome, ToolError> {
        unreachable!("M0 never dispatches tools")
    }
}

fn provider(id: &str, backend: LlmBackend, endpoint: Option<&str>) -> LlmProviderConfig {
    LlmProviderConfig {
        id: id.into(),
        backend,
        endpoint: endpoint.map(Into::into),
        auth: ProviderAuth::ApiKey(SecretString::new(KEY)),
        request_timeout: Duration::from_secs(30),
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    prompt: std::path::PathBuf,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let prompt = dir.path().join("root.md");
    std::fs::write(&prompt, "You are root.").unwrap();
    Fixture { _dir: dir, prompt }
}

fn agent(f: &Fixture, id: &str, provider: &str) -> AgentDefinition {
    AgentDefinition::new(id, provider, "model-x").with_prompt_path(&f.prompt)
}

fn config(f: &Fixture) -> OrchestratorConfig {
    OrchestratorConfig {
        llm: LlmConfig {
            providers: vec![provider("openai-main", LlmBackend::OpenAi, None)],
        },
        agents: vec![agent(f, "root", "openai-main").with_mode(AgentMode::Primary)],
    }
}

async fn build(
    cfg: OrchestratorConfig,
    tools: Vec<Arc<dyn HostTool>>,
) -> Result<Orchestrator, OrchestratorError> {
    let mut b = Orchestrator::builder(cfg);
    for t in tools {
        b = b.register_tool(t);
    }
    b.build().await
}

async fn reason(cfg: OrchestratorConfig, tools: Vec<Arc<dyn HostTool>>) -> OrchestratorError {
    match build(cfg, tools).await {
        Ok(_) => panic!("expected build to fail"),
        Err(e) => e,
    }
}

#[tokio::test]
async fn at46_valid_wiring_builds_without_remote_calls() {
    let f = fixture();
    let mut cfg = config(&f);
    // Endpoint không tồn tại: build chỉ validate cục bộ, không được gọi mạng.
    cfg.llm.providers.push(provider(
        "lan",
        LlmBackend::OpenAiCompatible,
        Some("http://203.0.113.1:9/v1"),
    ));
    cfg.agents.push(
        agent(&f, "helper", "lan")
            .with_mode(AgentMode::Subagent)
            .with_allowed_tools(["subagent", "skill", "read_file"]),
    );
    let o = build(cfg, vec![FakeTool::ok("read_file")]).await.unwrap();

    assert_eq!(o.agent_ids(), vec!["helper", "root"]);
    assert_eq!(o.agent("helper").unwrap().provider_id, "lan");
    assert_eq!(o.tool_descriptors().len(), 1);
    assert_eq!(o.base_instructions_version(), 1);
    let fp = o.provider_fingerprint("lan").unwrap();
    assert!(fp.starts_with("v1:") && fp.len() == 3 + 64);
    assert_ne!(fp, o.provider_fingerprint("openai-main").unwrap());
}

#[tokio::test]
async fn fingerprint_ignores_secret_and_timeout_but_tracks_endpoint() {
    let a = provider("a", LlmBackend::OpenAiCompatible, Some("http://h:1/v1"));
    let mut b = a.clone();
    b.auth = ProviderAuth::ApiKey(SecretString::new("rotated"));
    b.request_timeout = Duration::from_secs(999);
    assert_eq!(a.routing_fingerprint(), b.routing_fingerprint());
    let c = provider("a", LlmBackend::OpenAiCompatible, Some("http://h:2/v1"));
    assert_ne!(a.routing_fingerprint(), c.routing_fingerprint());
    let d = provider("a", LlmBackend::OpenAiCompatible, Some("HTTP://H:1/v1/"));
    assert_eq!(a.routing_fingerprint(), d.routing_fingerprint());
}

#[tokio::test]
async fn rejects_invalid_llm_config_with_codes() {
    let f = fixture();
    let cases: Vec<Case> = vec![
        (
            Box::new(|c| c.llm.providers.clear()),
            ConfigReason::NoProviders,
        ),
        (
            Box::new(|c| {
                c.llm
                    .providers
                    .push(provider("openai-main", LlmBackend::OpenAi, None))
            }),
            ConfigReason::DuplicateProviderId,
        ),
        (
            Box::new(|c| c.llm.providers[0].id = "  ".into()),
            ConfigReason::EmptyProviderId,
        ),
        (
            Box::new(|c| c.llm.providers[0].request_timeout = Duration::ZERO),
            ConfigReason::InvalidTimeout,
        ),
        (
            Box::new(|c| c.llm.providers[0].auth = ProviderAuth::None),
            ConfigReason::MissingApiKey,
        ),
        (
            Box::new(|c| c.llm.providers[0].auth = ProviderAuth::ApiKey(SecretString::new(" "))),
            ConfigReason::MissingApiKey,
        ),
        (
            Box::new(|c| c.llm.providers[0].endpoint = Some("http://u:p@host/v1".into())),
            ConfigReason::InvalidEndpoint,
        ),
        (
            Box::new(|c| c.llm.providers[0].endpoint = Some("http://host/v1?api_key=x".into())),
            ConfigReason::InvalidEndpoint,
        ),
        (
            Box::new(|c| {
                c.llm
                    .providers
                    .push(provider("o", LlmBackend::Ollama, None))
            }),
            ConfigReason::EndpointRequired,
        ),
    ];
    for (mutate, expected) in cases {
        let mut cfg = config(&f);
        mutate(&mut cfg);
        let e = reason(cfg, vec![]).await;
        assert_eq!(e.code(), ErrorCode::InvalidInput);
        assert_eq!(e.reason(), Some(expected), "{e}");
    }
}

#[tokio::test]
async fn rejects_invalid_agents_with_codes() {
    let f = fixture();
    let cases: Vec<Case> = vec![
        (Box::new(|c| c.agents.clear()), ConfigReason::NoAgents),
        (
            Box::new({
                let f2 = f.prompt.clone();
                move |c| {
                    c.agents.push(
                        AgentDefinition::new("root", "openai-main", "m").with_prompt_path(&f2),
                    )
                }
            }),
            ConfigReason::DuplicateAgentId,
        ),
        (
            Box::new(|c| c.agents[0].id = String::new()),
            ConfigReason::EmptyAgentId,
        ),
        (
            Box::new(|c| c.agents[0].provider_id = "missing".into()),
            ConfigReason::UnknownProvider,
        ),
        (
            Box::new(|c| c.agents[0].model_id = " ".into()),
            ConfigReason::EmptyModelId,
        ),
        (
            Box::new(|c| c.agents[0].steps = Some(0)),
            ConfigReason::InvalidSteps,
        ),
        (
            Box::new(|c| c.agents[0].prompt_path = "/definitely/not/here.md".into()),
            ConfigReason::PromptPathInvalid,
        ),
        (
            Box::new(|c| c.agents[0].prompt_path = std::path::PathBuf::new()),
            ConfigReason::PromptPathInvalid,
        ),
        (
            Box::new(|c| c.agents[0].allowed_tools = vec!["nope".into()]),
            ConfigReason::UnknownTool,
        ),
    ];
    for (mutate, expected) in cases {
        let mut cfg = config(&f);
        mutate(&mut cfg);
        let e = reason(cfg, vec![]).await;
        assert_eq!(e.code(), ErrorCode::InvalidInput);
        assert_eq!(e.reason(), Some(expected), "{e}");
    }
}

#[tokio::test]
async fn rejects_bad_tools_with_codes() {
    let f = fixture();
    let e = reason(config(&f), vec![FakeTool::ok("dup"), FakeTool::ok("dup")]).await;
    assert_eq!(e.reason(), Some(ConfigReason::DuplicateTool));

    for reserved in ["subagent", "skill"] {
        let e = reason(config(&f), vec![FakeTool::ok(reserved)]).await;
        assert_eq!(e.reason(), Some(ConfigReason::ReservedToolName));
    }
    for name in ["", "Bad", "1x", "has-dash"] {
        let e = reason(config(&f), vec![FakeTool::ok(name)]).await;
        assert_eq!(e.reason(), Some(ConfigReason::InvalidToolName), "{name}");
    }

    let schemas = [
        serde_json::json!("string"),
        serde_json::json!({ "type": "array" }),
        serde_json::json!({ "type": "object", "properties": [] }),
        serde_json::json!({ "type": "object", "required": "q" }),
        serde_json::json!({ "type": "object", "properties": {}, "required": ["q"] }),
        serde_json::json!({ "type": "object", "properties": {"q": {}}, "required": [1] }),
    ];
    for s in schemas {
        let e = reason(config(&f), vec![FakeTool::with_schema("t", s.clone())]).await;
        assert_eq!(e.reason(), Some(ConfigReason::InvalidToolSchema), "{s}");
    }
}

#[tokio::test]
async fn secrets_are_redacted_everywhere() {
    let f = fixture();
    let cfg = config(&f);
    assert!(!format!("{cfg:?}").contains(KEY));
    assert!(format!("{cfg:?}").contains("<redacted>"));

    // Mọi lỗi không được chứa secret hoặc endpoint thô.
    let mut bad = config(&f);
    bad.llm.providers[0].endpoint = Some("http://user:pw@leak.example/v1?api_key=abc".into());
    let e = reason(bad, vec![]).await;
    let text = format!("{e} {e:?}");
    for leak in [KEY, "pw@", "leak.example", "api_key=abc"] {
        assert!(!text.contains(leak), "{text}");
    }

    let o = build(config(&f), vec![]).await.unwrap();
    assert!(!o.provider_fingerprint("openai-main").unwrap().contains(KEY));
}

#[test]
fn error_codes_are_stable() {
    assert_eq!(ErrorCode::InvalidInput.as_str(), "INVALID_INPUT");
    assert_eq!(ErrorCode::StoreAlreadyOwned.as_str(), "STORE_ALREADY_OWNED");
    assert_eq!(ConfigReason::DuplicateTool.as_str(), "DUPLICATE_TOOL");
}

#[test]
fn build_inside_existing_runtime_does_not_nest_a_runtime() {
    // Build trong current_thread runtime của host: nếu library tự block_on/tạo runtime lồng
    // thì sẽ panic. Test này chứng minh build chỉ await trên runtime hiện có.
    let f = fixture();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async { build(config(&f), vec![]).await.unwrap() });
}
