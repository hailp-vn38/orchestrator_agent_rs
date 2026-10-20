//! HostTool: external extension seam (docs/18 §18.6). M0 cung cấp contract đăng ký và
//! validate; M2 thêm dispatch có scope/permissions (`executor`); M7 thêm scoped capabilities
//! `artifacts().put/open` và `checkpoint().current/commit` (docs/17, docs/18 §18.6.2).

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::artifact::{
    ArtifactInput, ArtifactReader, ArtifactStore, ArtifactWriteScope, CODE_ARTIFACT_UNAVAILABLE,
};
use crate::error::{ConfigReason, OrchestratorError};
use crate::feed::{event, Emitter, ProgressSink};
use crate::store::SessionStore;
use crate::types::ArtifactRef;

/// Tên tool dành riêng cho built-in; host không được đăng ký trùng.
pub(crate) const RESERVED_TOOL_NAMES: &[&str] = &["subagent", "skill", "artifact_read"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolEffect {
    ReadOnly,
    Mutating,
}

#[derive(Debug, Clone)]
pub struct ToolDescriptor {
    pub name: String,
    pub description: String,
    /// JSON Schema (object) cho arguments.
    pub input_schema: serde_json::Value,
    pub effect: ToolEffect,
}

/// Identity + effective permission snapshot do runtime mint (không lấy từ LLM). Host chỉ
/// đọc; `non_exhaustive` để host không tự dựng scope giả.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ToolScope {
    pub owner_id: String,
    pub workspace_id: String,
    pub session_id: String,
    /// Root của session family (chính `session_id` nếu là Root); engine resolve từ ancestry.
    pub root_session_id: String,
    pub turn_id: String,
    pub run_id: String,
    pub agent_id: String,
    /// Internal `tool_invocations.id`, khác provider `tool_call_id`.
    pub invocation_id: String,
    pub tool_call_id: String,
    /// Effective tools của session (agent ∩ session policy), đã sắp xếp.
    pub allowed_tools: Vec<String>,
    /// Effective skills của session (agent ∩ session policy), đã sắp xếp.
    pub allowed_skills: Vec<String>,
}

/// Trần byte của một progress update (cắt tại ranh giới ký tự).
const MAX_PROGRESS_BYTES: usize = 1024;

/// Các dịch vụ nội bộ mà runtime cấp cho `ToolContext` (luôn đi cùng nhau).
#[derive(Clone)]
pub(crate) struct ToolServices {
    pub artifacts: ArtifactStore,
    pub store: SessionStore,
    pub emitter: Emitter,
}

/// Capability có phạm vi do runtime mint; host không tự dựng được ngoài test của crate.
#[derive(Clone)]
pub struct ToolContext {
    scope: ToolScope,
    cancelled: Arc<AtomicBool>,
    progress: Option<ProgressSink>,
    services: Option<ToolServices>,
}

impl fmt::Debug for ToolContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolContext")
            .field("scope", &self.scope)
            .field("cancelled", &self.cancelled.load(Ordering::Relaxed))
            .finish()
    }
}

impl ToolContext {
    pub(crate) fn new(
        scope: ToolScope,
        cancelled: Arc<AtomicBool>,
        progress: Option<ProgressSink>,
        services: Option<ToolServices>,
    ) -> Self {
        Self {
            scope,
            cancelled,
            progress,
            services,
        }
    }

    pub fn scope(&self) -> &ToolScope {
        &self.scope
    }

    /// Hủy hợp tác; tool tự quyết định dừng.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Scoped capability để xuất bản và mở các artifact bất biến trong phạm vi cho phép
    /// của invocation hiện tại (docs/18 §18.6.2).
    pub fn artifacts(&self) -> ToolArtifacts<'_> {
        ToolArtifacts { ctx: self }
    }

    /// Scoped capability để đọc và cập nhật compare-and-swap (CAS) durable manifest pointer
    /// trên `tool_invocations.checkpoint_artifact_id` của đúng invocation hiện tại.
    pub fn checkpoint(&self) -> ToolCheckpoint<'_> {
        ToolCheckpoint { ctx: self }
    }

    /// Progress ephemeral, best-effort, không chặn và không ghi SQLite. Bị cắt ở
    /// `MAX_PROGRESS_BYTES`, bị throttle theo `RuntimeConfig::progress_min_interval` và đi vào
    /// event feed (`tool.progress`, secret đã cấu hình bị che) cùng tracing.
    pub fn progress(&self, update: &str) {
        self.report(update, None);
    }

    /// Như [`Self::progress`] kèm phần trăm hoàn thành (kẹp vào `0..=100`).
    pub fn progress_with_percent(&self, update: &str, percent: u8) {
        self.report(update, Some(percent.min(100)));
    }

    fn report(&self, update: &str, percent: Option<u8>) {
        let mut end = update.len().min(MAX_PROGRESS_BYTES);
        while !update.is_char_boundary(end) {
            end -= 1;
        }
        tracing::debug!(
            session_id = %self.scope.session_id,
            run_id = %self.scope.run_id,
            invocation_id = %self.scope.invocation_id,
            progress_bytes = end,
            "tool progress"
        );
        if let Some(sink) = &self.progress {
            sink.send(&update[..end], percent);
        }
    }
}

/// Handle thao tác artifact có phạm vi gắn với [`ToolContext`].
#[derive(Debug, Clone, Copy)]
pub struct ToolArtifacts<'a> {
    ctx: &'a ToolContext,
}

impl ToolArtifacts<'_> {
    /// Ghi bất biến một artifact mới qua `ArtifactStore` và trả về [`ArtifactRef`] đã xác minh.
    pub async fn put(&self, input: ArtifactInput) -> Result<ArtifactRef, ToolError> {
        let Some(ToolServices {
            artifacts: store, ..
        }) = &self.ctx.services
        else {
            return Err(ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "artifact store is not available in this context".into(),
            });
        };
        let write_scope = ArtifactWriteScope::from_tool_scope(&self.ctx.scope);
        let reference = store.put(&write_scope, input).await?;
        if let Some(ToolServices { emitter, .. }) = &self.ctx.services {
            emitter
                .committed_session(
                    &self.ctx.scope.session_id,
                    Some(&self.ctx.scope.run_id),
                    event::SESSION_CHANGED,
                    json!({
                        "reason": "artifact_published",
                        "artifact_id": reference.id,
                        "invocation_id": self.ctx.scope.invocation_id,
                    }),
                )
                .await;
        }
        Ok(reference)
    }

    /// Mở một committed artifact để đọc theo luồng có giới hạn. Chỉ cho phép khi `artifact_id`
    /// thuộc cùng owner/workspace, cùng session family **và** nằm trong explicit invocation
    /// input/checkpoint scope. Cùng workspace đơn thuần không tự cấp quyền đọc (AT-40).
    pub async fn open(&self, artifact_id: &str) -> Result<ArtifactReader, ToolError> {
        let Some(ToolServices {
            artifacts: store, ..
        }) = &self.ctx.services
        else {
            return Err(ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "artifact store is not available in this context".into(),
            });
        };
        store.open_for_tool(&self.ctx.scope, artifact_id).await
    }
}

/// Handle đọc/ghi durable manifest pointer gắn với [`ToolContext`] (docs/17 §17.7, docs/18 §18.6.2).
#[derive(Debug, Clone, Copy)]
pub struct ToolCheckpoint<'a> {
    ctx: &'a ToolContext,
}

impl ToolCheckpoint<'_> {
    /// Đọc manifest ID đã commit cuối cùng của **đúng** internal `invocation_id` hiện tại
    /// và xác minh tính toàn vẹn của artifact đó. Trả `Ok(None)` nếu invocation chưa commit
    /// pointer nào (không tự kế thừa từ invocation trước trong cùng child session).
    pub async fn current(&self) -> Result<Option<String>, ToolError> {
        let Some(ToolServices {
            store, artifacts, ..
        }) = &self.ctx.services
        else {
            return Err(ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "checkpoint store is not available in this context".into(),
            });
        };
        store.current_checkpoint(&self.ctx.scope, artifacts).await
    }

    /// Cập nhật compare-and-swap (CAS) `tool_invocations.checkpoint_artifact_id` của đúng
    /// internal `invocation_id` hiện tại sau khi đã xác minh `manifest_id` là một artifact
    /// đã commit và nguyên vẹn trong scope. Trả lỗi `CHECKPOINT_CONFLICT` nếu pointer hiện
    /// tại không khớp `expected_previous_id`.
    pub async fn commit(
        &self,
        manifest_id: &str,
        expected_previous_id: Option<&str>,
    ) -> Result<(), ToolError> {
        let Some(ToolServices {
            store, artifacts, ..
        }) = &self.ctx.services
        else {
            return Err(ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "checkpoint store is not available in this context".into(),
            });
        };
        store
            .commit_checkpoint(
                &self.ctx.scope,
                artifacts,
                manifest_id,
                expected_previous_id,
            )
            .await?;
        if let Some(ToolServices { emitter, .. }) = &self.ctx.services {
            emitter
                .committed_session(
                    &self.ctx.scope.session_id,
                    Some(&self.ctx.scope.run_id),
                    event::SESSION_CHANGED,
                    json!({
                        "reason": "checkpoint_committed",
                        "checkpoint_artifact_id": manifest_id,
                        "invocation_id": self.ctx.scope.invocation_id,
                    }),
                )
                .await;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ToolOutcome {
    pub call_id: String,
    pub content: String,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError {
    pub code: String,
    pub message: String,
}

impl From<OrchestratorError> for ToolError {
    fn from(err: OrchestratorError) -> Self {
        let msg = err.message().to_owned();
        let code = if msg.starts_with(crate::artifact::CODE_ARTIFACT_INTEGRITY) {
            crate::artifact::CODE_ARTIFACT_INTEGRITY.to_owned()
        } else {
            err.code().as_str().to_owned()
        };
        Self { code, message: msg }
    }
}

#[async_trait::async_trait]
pub trait HostTool: Send + Sync {
    fn descriptor(&self) -> ToolDescriptor;

    /// Timeout riêng của một thao tác tool này; `None` = `RuntimeConfig::tool_timeout`.
    /// Phải `> 0` (kiểm tra lúc build). Quá hạn được phân loại theo `effect` như timeout chung.
    fn timeout(&self) -> Option<Duration> {
        None
    }

    async fn execute(
        &self,
        context: ToolContext,
        args: serde_json::Value,
    ) -> Result<ToolOutcome, ToolError>;
}

fn valid_tool_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && name.len() <= 64
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

pub(crate) fn validate_descriptor(d: &ToolDescriptor) -> Result<(), OrchestratorError> {
    if !valid_tool_name(&d.name) {
        return Err(OrchestratorError::config(
            ConfigReason::InvalidToolName,
            "tool name must match [a-z][a-z0-9_]{0,63}",
        ));
    }
    if RESERVED_TOOL_NAMES.contains(&d.name.as_str()) {
        return Err(OrchestratorError::config(
            ConfigReason::ReservedToolName,
            format!("tool name '{}' is reserved for a built-in tool", d.name),
        ));
    }
    if d.description.trim().is_empty() {
        return Err(OrchestratorError::config(
            ConfigReason::InvalidToolDescriptor,
            format!("tool '{}': description must not be empty", d.name),
        ));
    }
    validate_schema(&d.name, &d.input_schema)
}

/// Kiểm tra cấu trúc tối thiểu: object schema, `properties` là object,
/// `required` là danh sách tên nằm trong `properties`.
fn validate_schema(tool: &str, schema: &serde_json::Value) -> Result<(), OrchestratorError> {
    let bad = |msg: &str| {
        Err(OrchestratorError::config(
            ConfigReason::InvalidToolSchema,
            format!("tool '{tool}': {msg}"),
        ))
    };
    let Some(obj) = schema.as_object() else {
        return bad("input_schema must be a JSON object");
    };
    if obj.get("type").and_then(|t| t.as_str()) != Some("object") {
        return bad("input_schema.type must be \"object\"");
    }
    let props = match obj.get("properties") {
        None => None,
        Some(p) => match p.as_object() {
            Some(m) => Some(m),
            None => return bad("input_schema.properties must be an object"),
        },
    };
    if let Some(req) = obj.get("required") {
        let Some(list) = req.as_array() else {
            return bad("input_schema.required must be an array");
        };
        for r in list {
            let Some(name) = r.as_str() else {
                return bad("input_schema.required entries must be strings");
            };
            if !props.is_some_and(|p| p.contains_key(name)) {
                return bad("input_schema.required references an undeclared property");
            }
        }
    }
    Ok(())
}
