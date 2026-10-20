//! HostTool: external extension seam (docs/18 §18.6). M0 cung cấp contract đăng ký và
//! validate; M2 thêm dispatch có scope/permissions (`executor`). Artifacts/checkpoint thuộc M7.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::error::{ConfigReason, OrchestratorError};

/// Tên tool dành riêng cho built-in; host không được đăng ký trùng.
pub(crate) const RESERVED_TOOL_NAMES: &[&str] = &["subagent", "skill"];

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

/// Capability có phạm vi do runtime mint; host không tự dựng được ngoài test của crate.
#[derive(Debug, Clone)]
pub struct ToolContext {
    scope: ToolScope,
    cancelled: Arc<AtomicBool>,
}

impl ToolContext {
    pub(crate) fn new(scope: ToolScope, cancelled: Arc<AtomicBool>) -> Self {
        Self { scope, cancelled }
    }

    pub fn scope(&self) -> &ToolScope {
        &self.scope
    }

    /// Hủy hợp tác; tool tự quyết định dừng.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Progress ephemeral, best-effort, không chặn và không ghi SQLite. Bị cắt ở
    /// `MAX_PROGRESS_BYTES`; hiện chỉ đi vào tracing, event feed gắn ở M6.
    pub fn progress(&self, update: &str) {
        let mut end = update.len().min(MAX_PROGRESS_BYTES);
        while !update.is_char_boundary(end) {
            end -= 1;
        }
        tracing::debug!(
            session_id = %self.scope.session_id,
            run_id = %self.scope.run_id,
            invocation_id = %self.scope.invocation_id,
            progress = &update[..end],
            "tool progress"
        );
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

#[async_trait::async_trait]
pub trait HostTool: Send + Sync {
    fn descriptor(&self) -> ToolDescriptor;

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
