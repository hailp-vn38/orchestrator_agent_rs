//! Quyền hiệu lực và tập tool có thể replay an toàn.
use super::{ARTIFACT_READ_TOOL, SKILL_TOOL};
use crate::agent::AgentDefinition;
use crate::subagent::SUBAGENT_TOOL;
use crate::tool::{HostTool, ToolEffect};
use crate::types::SessionPolicy;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Quyền hiệu lực của một session: `AgentDefinition` ∩ `SessionPolicy` (chỉ thu hẹp).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Effective {
    pub tools: BTreeSet<String>,
    pub skills: BTreeSet<String>,
    pub subagents: Option<BTreeSet<String>>,
    /// Session là child (depth 1): `subagent` không được quảng bá và bị chặn.
    pub child: bool,
}

impl Effective {
    pub(crate) fn resolve(agent: &AgentDefinition, policy: &SessionPolicy) -> Self {
        fn narrow(base: &[String], limit: &Option<Vec<String>>) -> BTreeSet<String> {
            base.iter()
                .filter(|x| limit.as_ref().is_none_or(|l| l.contains(x)))
                .cloned()
                .collect()
        }
        Self {
            tools: narrow(&agent.allowed_tools, &policy.allowed_tools),
            skills: narrow(&agent.allowed_skills, &policy.allowed_skills),
            subagents: agent
                .allowed_subagents
                .as_ref()
                .map(|list| list.iter().cloned().collect()),
            child: false,
        }
    }

    pub(crate) fn for_session(mut self, is_child: bool) -> Self {
        self.child = is_child;
        self
    }
}

/// Tên tool có thể chạy lại an toàn khi outcome của lần chạy trước không xác định: `skill`
/// (đọc), `subagent` (idempotent theo invocation ID) và tool khai báo `ReadOnly`. Tool Mutating
/// hoặc không còn đăng ký không nằm trong tập này: phải reconcile, không replay.
pub(crate) fn replay_safe_names(tools: &BTreeMap<String, Arc<dyn HostTool>>) -> BTreeSet<String> {
    let mut names: BTreeSet<String> = tools
        .iter()
        .filter(|(_, t)| t.descriptor().effect == ToolEffect::ReadOnly)
        .map(|(name, _)| name.clone())
        .collect();
    names.insert(SKILL_TOOL.to_owned());
    names.insert(ARTIFACT_READ_TOOL.to_owned());
    names.insert(SUBAGENT_TOOL.to_owned());
    names
}
