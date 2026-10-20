use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use llm::builder::{LLMBackend as LlmCrateBackend, LLMBuilder};
use llm::chat::{FunctionTool, Tool};
use llm::LLMProvider;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::config::{LlmBackend, LlmProviderConfig, ProviderAuth};

use super::collect::{collect, RawStream};
use super::convert::to_chat_messages;
use super::error::{map_error, GatewayError};
use super::types::{LlmGateway, ModelRequest, ModelResponse, StreamSink};

/// Trần output token mỗi request khi config provider chưa có tùy chọn riêng (M1).
const MAX_OUTPUT_TOKENS: u32 = 4096;

/// Gateway thật: dựng client `llm` lười theo `(provider, model, system)` và tái sử dụng.
pub struct ClientGateway {
    providers: HashMap<String, LlmProviderConfig>,
    clients: Mutex<HashMap<String, Arc<dyn LLMProvider>>>,
}

impl ClientGateway {
    pub fn new(providers: &[LlmProviderConfig]) -> Self {
        Self {
            providers: providers
                .iter()
                .map(|p| (p.id.trim().to_string(), p.clone()))
                .collect(),
            clients: Mutex::new(HashMap::new()),
        }
    }

    fn client(
        &self,
        cfg: &LlmProviderConfig,
        request: &ModelRequest,
    ) -> Result<Arc<dyn LLMProvider>, GatewayError> {
        let key = {
            let mut h = Sha256::new();
            h.update(request.system.as_bytes());
            let digest: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
            format!("{}\n{}\n{digest}", cfg.id.trim(), request.model_id)
        };
        let mut clients = self.clients.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(c) = clients.get(&key) {
            return Ok(Arc::clone(c));
        }
        let client: Arc<dyn LLMProvider> = Arc::from(build_client(cfg, request)?);
        clients.insert(key, Arc::clone(&client));
        Ok(client)
    }
}

fn build_client(
    cfg: &LlmProviderConfig,
    request: &ModelRequest,
) -> Result<Box<dyn LLMProvider>, GatewayError> {
    let backend = match cfg.backend {
        LlmBackend::OpenAi | LlmBackend::OpenAiCompatible => LlmCrateBackend::OpenAI,
        LlmBackend::Anthropic => LlmCrateBackend::Anthropic,
        LlmBackend::Gemini => LlmCrateBackend::Google,
        LlmBackend::Ollama => LlmCrateBackend::Ollama,
    };
    let mut builder = LLMBuilder::new()
        .backend(backend)
        .model(request.model_id.clone())
        .max_tokens(MAX_OUTPUT_TOKENS)
        .timeout_seconds(cfg.request_timeout.as_secs().max(1))
        .system(request.system.clone());
    if let ProviderAuth::ApiKey(key) = &cfg.auth {
        builder = builder.api_key(key.expose());
    }
    if let Some(endpoint) = cfg.normalized_endpoint() {
        builder = builder.base_url(endpoint);
    }
    builder.build().map_err(|e| map_error(&e))
}

#[async_trait]
impl LlmGateway for ClientGateway {
    async fn complete(
        &self,
        request: &ModelRequest,
        cancel: &CancellationToken,
        sink: StreamSink<'_>,
    ) -> Result<ModelResponse, GatewayError> {
        let cfg = self
            .providers
            .get(request.provider_id.trim())
            .ok_or(GatewayError::NotConfigured)?;
        if request.model_id.trim().is_empty() {
            return Err(GatewayError::NotConfigured);
        }
        let client = self.client(cfg, request)?;
        let messages = to_chat_messages(&request.messages);
        let tools: Vec<Tool> = request
            .tools
            .iter()
            .map(|t| Tool {
                tool_type: "function".to_string(),
                function: FunctionTool {
                    name: t.name.clone(),
                    description: t.description.clone(),
                    parameters: t.input_schema.clone(),
                },
                cache_control: None,
            })
            .collect();

        // Deadline bao trùm cả bước tạo stream lẫn đọc stream: đây là timeout của *một*
        // request LLM, không phải của cả run.
        let work = async {
            let raw: RawStream = client
                .chat_stream_with_tools(&messages, (!tools.is_empty()).then_some(&tools[..]))
                .await
                .map_err(|e| map_error(&e))?;
            collect(raw, sink).await
        };
        tokio::select! {
            () = cancel.cancelled() => Err(GatewayError::Cancelled),
            r = tokio::time::timeout(cfg.request_timeout, work) => {
                r.unwrap_or(Err(GatewayError::Timeout))
            }
        }
    }
}

