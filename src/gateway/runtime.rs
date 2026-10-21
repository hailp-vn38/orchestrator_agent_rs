use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::error::GatewayError;
use super::types::{LlmGateway, ModelRequest, ModelResponse, StreamSink};

/// Provider do runtime của server/host sở hữu (thay vì `LlmProviderConfig` để library tự gọi LLM).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeProvider {
    /// Alias mà `AgentDefinition::provider_id` tham chiếu.
    pub id: String,
    /// Fingerprint routing non-secret (EMB-16): phải đổi khi alias trỏ sang backend/endpoint
    /// khác, để recovery không resume nhầm. Không chứa credential.
    pub fingerprint: String,
}

impl RuntimeProvider {
    pub fn new(id: impl Into<String>, fingerprint: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            fingerprint: fingerprint.into(),
        }
    }
}

/// Runtime gọi model do host/server cung cấp. Host tự quản credential, client và routing;
/// library chỉ gửi [`ModelRequest`] đã resolve theo agent và nhận lại một response hoàn chỉnh.
///
/// Hợp đồng giống `LlmGateway`: chỉ trả `Ok` khi toàn response đã hoàn tất; lỗi/hủy/timeout
/// giữa chừng phải trả `Err` (không trả phần dở). `sink` chỉ nhận preview ephemeral.
#[async_trait]
pub trait ProviderRuntime: Send + Sync {
    /// Danh sách provider alias mà runtime phục vụ. Được đọc một lần lúc build.
    fn providers(&self) -> Vec<RuntimeProvider>;

    async fn complete(
        &self,
        request: &ModelRequest,
        cancel: &CancellationToken,
        sink: StreamSink<'_>,
    ) -> Result<ModelResponse, GatewayError>;
}

/// Adapter `ProviderRuntime` → `LlmGateway`; từ chối alias runtime không khai báo.
pub(crate) struct RuntimeGateway {
    runtime: Arc<dyn ProviderRuntime>,
    known: HashMap<String, ()>,
}

impl RuntimeGateway {
    pub(crate) fn new(runtime: Arc<dyn ProviderRuntime>, providers: &[RuntimeProvider]) -> Self {
        Self {
            runtime,
            known: providers
                .iter()
                .map(|p| (p.id.trim().to_string(), ()))
                .collect(),
        }
    }
}

#[async_trait]
impl LlmGateway for RuntimeGateway {
    async fn complete(
        &self,
        request: &ModelRequest,
        cancel: &CancellationToken,
        sink: StreamSink<'_>,
    ) -> Result<ModelResponse, GatewayError> {
        if !self.known.contains_key(request.provider_id.trim()) {
            return Err(GatewayError::NotConfigured);
        }
        self.runtime.complete(request, cancel, sink).await
    }
}
