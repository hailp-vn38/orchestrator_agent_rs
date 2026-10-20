//! Scope ghi artifact đã được runtime xác thực.

use crate::tool::ToolScope;

/// Scope đã được runtime xác thực khi một tool ghi artifact mới.
#[derive(Debug, Clone)]
pub(crate) struct ArtifactWriteScope {
    pub owner_id: String,
    pub workspace_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub run_id: String,
    pub producer_invocation_id: Option<String>,
}

impl ArtifactWriteScope {
    pub(crate) fn from_tool_scope(scope: &ToolScope) -> Self {
        Self {
            owner_id: scope.owner_id.clone(),
            workspace_id: scope.workspace_id.clone(),
            session_id: scope.session_id.clone(),
            turn_id: scope.turn_id.clone(),
            run_id: scope.run_id.clone(),
            producer_invocation_id: Some(scope.invocation_id.clone()),
        }
    }
}
