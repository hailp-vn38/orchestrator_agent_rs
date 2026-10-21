//! Tích hợp provider runtime của server: model được gọi qua `ProviderRuntime` do host cắm,
//! không dùng `LlmProviderConfig`/`ClientGateway`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, CallerContext, ConfigReason, GatewayError, LlmBackend, LlmConfig,
    LlmProviderConfig, ModelRequest, ModelResponse, Orchestrator, OrchestratorConfig, ProviderAuth,
    ProviderRuntime, RunStatus, RuntimeProvider, SecretString, StreamSink,
};
use tokio_util::sync::CancellationToken;

struct ServerRuntime {
    providers: Vec<RuntimeProvider>,
    seen: Mutex<Vec<(String, String)>>,
}

impl ServerRuntime {
    fn new(providers: Vec<RuntimeProvider>) -> Arc<Self> {
        Arc::new(Self {
            providers,
            seen: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait::async_trait]
impl ProviderRuntime for ServerRuntime {
    fn providers(&self) -> Vec<RuntimeProvider> {
        self.providers.clone()
    }

    async fn complete(
        &self,
        request: &ModelRequest,
        _cancel: &CancellationToken,
        _sink: StreamSink<'_>,
    ) -> Result<ModelResponse, GatewayError> {
        self.seen
            .lock()
            .unwrap()
            .push((request.provider_id.clone(), request.model_id.clone()));
        Ok(ModelResponse {
            text: "from server runtime".into(),
            finish_reason: "stop".into(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

fn config(dir: &tempfile::TempDir, llm: LlmConfig, provider: &str) -> OrchestratorConfig {
    let prompt = dir.path().join("root.md");
    std::fs::write(&prompt, "root").unwrap();
    OrchestratorConfig::new(
        llm,
        vec![AgentDefinition::new("root", provider, "srv-model")
            .with_mode(AgentMode::Primary)
            .with_prompt_path(prompt)],
        dir.path().join("store"),
    )
}

#[tokio::test]
async fn runs_through_server_runtime_without_llm_config() {
    let dir = tempfile::tempdir().unwrap();
    let rt = ServerRuntime::new(vec![RuntimeProvider::new("server", "fp-1")]);
    let o = Orchestrator::builder(config(&dir, LlmConfig::default(), "server"))
        .with_provider_runtime(rt.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(o.provider_fingerprint("server"), Some("fp-1"));

    let who = CallerContext::new("alice", "ws");
    let sid = o.create_session(&who, "root").await.unwrap();
    let turn = o.submit(&who, &sid, "hi".into()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let r = loop {
        let r = o.result(&who, &sid, &turn.turn_id).await.unwrap();
        if r.status != RunStatus::Running {
            break r;
        }
        assert!(tokio::time::Instant::now() < deadline, "run stuck");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(r.status, RunStatus::Completed);
    assert_eq!(r.output.as_deref(), Some("from server runtime"));
    assert_eq!(r.steps, 1);
    assert!(
        r.input_tokens > 0,
        "estimated input_tokens must be recorded"
    );
    assert!(
        r.output_tokens > 0,
        "estimated output_tokens must be recorded"
    );
    assert_eq!(r.total_tokens(), r.input_tokens + r.output_tokens);

    let snap = o.snapshot(&who, &sid).await.unwrap();
    let latest = snap.root.latest_run.expect("latest run");
    assert_eq!(latest.steps, r.steps);
    assert_eq!(latest.input_tokens, r.input_tokens);
    assert_eq!(latest.output_tokens, r.output_tokens);
    assert_eq!(latest.total_tokens(), r.total_tokens());

    let run_summary = o.get_run(&who, &r.run_id).await.unwrap();
    assert_eq!(run_summary.steps, r.steps);
    assert_eq!(run_summary.input_tokens, r.input_tokens);
    assert_eq!(run_summary.output_tokens, r.output_tokens);
    assert_eq!(run_summary.total_tokens(), r.total_tokens());

    assert_eq!(
        rt.seen.lock().unwrap().as_slice(),
        [("server".to_string(), "srv-model".to_string())]
    );
    o.shutdown().await.unwrap();
}

async fn build_err(
    llm: LlmConfig,
    providers: Vec<RuntimeProvider>,
    agent_provider: &str,
) -> ConfigReason {
    let dir = tempfile::tempdir().unwrap();
    let err = match Orchestrator::builder(config(&dir, llm, agent_provider))
        .with_provider_runtime(ServerRuntime::new(providers))
        .build()
        .await
    {
        Ok(_) => panic!("build must fail"),
        Err(e) => e,
    };
    err.reason().expect("config reason")
}

#[tokio::test]
async fn rejects_invalid_runtime_configuration() {
    let p = |id: &str, fp: &str| RuntimeProvider::new(id, fp);
    assert_eq!(
        build_err(LlmConfig::default(), vec![], "server").await,
        ConfigReason::NoProviders
    );
    assert_eq!(
        build_err(LlmConfig::default(), vec![p(" ", "fp")], "server").await,
        ConfigReason::EmptyProviderId
    );
    assert_eq!(
        build_err(LlmConfig::default(), vec![p("a", "1"), p("a", "2")], "a").await,
        ConfigReason::DuplicateProviderId
    );
    assert_eq!(
        build_err(LlmConfig::default(), vec![p("a", "1")], "missing").await,
        ConfigReason::UnknownProvider
    );
    let llm = LlmConfig {
        providers: vec![LlmProviderConfig {
            id: "x".into(),
            backend: LlmBackend::OpenAi,
            endpoint: None,
            auth: ProviderAuth::ApiKey(SecretString::new("sk-test")),
            request_timeout: Duration::from_secs(5),
        }],
    };
    assert_eq!(
        build_err(llm, vec![p("a", "1")], "a").await,
        ConfigReason::ProviderRuntimeConflict
    );
}
