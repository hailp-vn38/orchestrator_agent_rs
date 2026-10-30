use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use llm::builder::{LLMBackend as LlmCrateBackend, LLMBuilder};
use llm::chat::{FunctionTool, ReasoningEffort as LlmReasoningEffort, Tool};
use llm::LLMProvider;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::config::{LlmBackend, LlmProviderConfig, ProviderAuth};

use super::collect::{collect_with_detail, strip_thinking_blocks, RawStream, ThinkStreamFilter};
use super::convert::to_chat_messages;
use super::error::{error_detail, map_error, sanitize_diagnostic_detail, GatewayError};
use super::types::{
    LlmGateway, ModelRequest, ModelResponse, ModelUsage, ReasoningEffort, StreamEvent, StreamSink,
};

/// Trần output token mỗi request khi agent không đặt `ModelParams::max_output_tokens`.
/// Đặt ở mức 16_384 để các model có thinking/reasoning không bị cạn ngân sách token trước khi
/// sinh `output_text` hoặc `function_call`.
const MAX_OUTPUT_TOKENS: u32 = 16_384;

/// Gateway thật: dựng client `llm` lười theo `(provider, model, system)` và tái sử dụng.
pub struct ClientGateway {
    providers: RwLock<HashMap<String, LlmProviderConfig>>,
    clients: Mutex<HashMap<String, Arc<dyn LLMProvider>>>,
    last_errors: Mutex<HashMap<String, String>>,
}

impl ClientGateway {
    pub fn new(providers: &[LlmProviderConfig]) -> Self {
        Self {
            providers: RwLock::new(
                providers
                    .iter()
                    .map(|p| (p.id.trim().to_string(), p.clone()))
                    .collect(),
            ),
            clients: Mutex::new(HashMap::new()),
            last_errors: Mutex::new(HashMap::new()),
        }
    }

    fn record_error(&self, provider_id: &str, detail: impl Into<String>) {
        let sanitized = sanitize_diagnostic_detail(&detail.into());
        if !sanitized.is_empty() {
            self.last_errors
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(provider_id.trim().to_owned(), sanitized);
        }
    }

    fn clear_error(&self, provider_id: &str) {
        self.last_errors
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(provider_id.trim());
    }

    pub fn replace_providers(&self, providers: &[LlmProviderConfig]) {
        let next: HashMap<String, LlmProviderConfig> = providers
            .iter()
            .map(|p| (p.id.trim().to_string(), p.clone()))
            .collect();
        let mut invalidated_prefixes = Vec::new();
        {
            let mut guard = self.providers.write().unwrap_or_else(|p| p.into_inner());
            for (id, old) in guard.iter() {
                let changed = match next.get(id) {
                    Some(new_cfg) => !same_provider_config(old, new_cfg),
                    None => true,
                };
                if changed {
                    invalidated_prefixes.push(format!("{id}\n"));
                }
            }
            for (id, new_cfg) in next {
                guard.insert(id, new_cfg);
            }
        }
        if !invalidated_prefixes.is_empty() {
            let mut clients = self.clients.lock().unwrap_or_else(|p| p.into_inner());
            clients.retain(|key, _| !invalidated_prefixes.iter().any(|p| key.starts_with(p)));
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
            format!(
                "{}\n{}\n{digest}\n{}",
                cfg.id.trim(),
                request.model_id,
                request.params.cache_key()
            )
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

fn same_provider_config(a: &LlmProviderConfig, b: &LlmProviderConfig) -> bool {
    let same_auth = match (&a.auth, &b.auth) {
        (ProviderAuth::None, ProviderAuth::None) => true,
        (ProviderAuth::ApiKey(ka), ProviderAuth::ApiKey(kb)) => ka.expose() == kb.expose(),
        (ProviderAuth::CustomExecutor(ea), ProviderAuth::CustomExecutor(eb)) => {
            ea.routing_fingerprint() == eb.routing_fingerprint()
        }
        _ => false,
    };
    a.backend == b.backend
        && a.endpoint == b.endpoint
        && a.request_timeout == b.request_timeout
        && same_auth
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
        .max_tokens(
            request
                .params
                .max_output_tokens
                .unwrap_or(MAX_OUTPUT_TOKENS),
        )
        .timeout_seconds(cfg.request_timeout.as_secs().max(1))
        .system(request.system.clone());
    if let Some(t) = request.params.temperature {
        builder = builder.temperature(t);
    }
    if let Some(p) = request.params.top_p {
        builder = builder.top_p(p);
    }
    if let Some(k) = request.params.top_k {
        builder = builder.top_k(k);
    }
    if let Some(effort) = request.params.reasoning_effort {
        let mapped = match effort {
            ReasoningEffort::Low => LlmReasoningEffort::Low,
            ReasoningEffort::Medium => LlmReasoningEffort::Medium,
            ReasoningEffort::High => LlmReasoningEffort::High,
        };
        builder = builder.reasoning(true).reasoning_effort(mapped);
    }
    if let Some(budget) = request.params.reasoning_budget_tokens {
        builder = builder.reasoning(true).reasoning_budget_tokens(budget);
    }
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
    fn take_last_error_detail(&self, provider_id: &str) -> Option<String> {
        self.last_errors
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(provider_id.trim())
    }

    async fn complete(
        &self,
        request: &ModelRequest,
        cancel: &CancellationToken,
        sink: StreamSink<'_>,
    ) -> Result<ModelResponse, GatewayError> {
        let provider_key = request.provider_id.trim();
        self.clear_error(provider_key);
        let cfg = self
            .providers
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(provider_key)
            .cloned()
            .ok_or_else(|| {
                self.record_error(
                    provider_key,
                    format!("provider '{provider_key}' is not registered in LlmConfig"),
                );
                GatewayError::NotConfigured
            })?;
        if request.model_id.trim().is_empty() {
            self.record_error(provider_key, "model_id is empty");
            return Err(GatewayError::NotConfigured);
        }
        // Deadline bao trùm cả bước tạo stream lẫn đọc stream: đây là timeout của *một*
        // request LLM, không phải của cả run.
        let work = async {
            if let ProviderAuth::CustomExecutor(ref executor) = cfg.auth {
                let filter = Mutex::new(ThinkStreamFilter::new());
                let filtered_sink = |ev: StreamEvent| match ev {
                    StreamEvent::TextDelta(t) => {
                        if !t.is_empty() {
                            let visible = filter.lock().unwrap_or_else(|p| p.into_inner()).push(&t);
                            if !visible.is_empty() {
                                sink(StreamEvent::TextDelta(visible));
                            }
                        }
                    }
                    other => sink(other),
                };
                let mut resp = match executor.complete(request, cancel, &filtered_sink).await {
                    Ok(r) => r,
                    Err(err) => {
                        if let Some(detail) = executor.take_last_error_detail() {
                            self.record_error(provider_key, detail);
                        }
                        return Err(err);
                    }
                };
                let tail = filter.lock().unwrap_or_else(|p| p.into_inner()).flush();
                if !tail.is_empty() {
                    sink(StreamEvent::TextDelta(tail));
                }
                let raw_len = resp.text.len();
                resp.text = strip_thinking_blocks(&resp.text);
                if resp.text.trim().is_empty() && resp.tool_calls.is_empty() {
                    if let Some(detail) = executor.take_last_error_detail() {
                        self.record_error(provider_key, detail);
                    } else if raw_len > 0 {
                        self.record_error(
                            provider_key,
                            format!(
                                "model output contained only <think> reasoning ({raw_len} chars) with no final text or tool calls"
                            ),
                        );
                    } else {
                        self.record_error(
                            provider_key,
                            "model finished with empty text and no tool calls",
                        );
                    }
                    return Err(GatewayError::Protocol);
                }
                return Ok(resp);
            }

            let client = self.client(&cfg, request)?;
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
            let raw: RawStream = client
                .chat_stream_with_tools(&messages, (!tools.is_empty()).then_some(&tools[..]))
                .await
                .map_err(|e| {
                    if let Some(detail) = error_detail(&e) {
                        self.record_error(provider_key, detail);
                    }
                    map_error(&e)
                })?;
            collect_with_detail(raw, sink).await.map_err(|(err, detail)| {
                self.record_error(provider_key, detail);
                err
            })
        };
        let mut response = tokio::select! {
            () = cancel.cancelled() => return Err(GatewayError::Cancelled),
            r = tokio::time::timeout(cfg.request_timeout, work) => {
                match r {
                    Ok(inner) => inner?,
                    Err(_) => {
                        self.record_error(
                            provider_key,
                            format!("request timed out after {}ms", cfg.request_timeout.as_millis()),
                        );
                        return Err(GatewayError::Timeout);
                    }
                }
            }
        };
        response.usage = Some(ModelUsage::or_estimated(response.usage, request, &response));
        Ok(response)
    }
}
