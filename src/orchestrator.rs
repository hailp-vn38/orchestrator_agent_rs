//! Orchestrator handle + builder (M0 shell). Chỉ những gì đã hoạt động mới được expose:
//! build/validate và accessor đọc. `create_session`/`submit`/... thuộc M1+.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::agent::{AgentDefinition, AgentRegistry};
use crate::config::OrchestratorConfig;
use crate::error::{ConfigReason, OrchestratorError};
use crate::tool::{validate_descriptor, HostTool, ToolDescriptor};

/// Mandatory base, embed lúc compile; host không cung cấp file.
const BASE_INSTRUCTIONS: &str = include_str!("base_instructions.md");
const BASE_VERSION_PREFIX: &str = "<!-- base-instructions-version: ";

pub struct Orchestrator {
    base_version: u32,
    registry: AgentRegistry,
    tools: BTreeMap<String, Arc<dyn HostTool>>,
    fingerprints: BTreeMap<String, String>,
}

pub struct OrchestratorBuilder {
    config: OrchestratorConfig,
    tools: Vec<Arc<dyn HostTool>>,
}

impl Orchestrator {
    pub fn builder(config: OrchestratorConfig) -> OrchestratorBuilder {
        OrchestratorBuilder {
            config,
            tools: Vec::new(),
        }
    }

    pub fn agent(&self, id: &str) -> Option<&AgentDefinition> {
        self.registry.get(id)
    }

    /// Danh sách agent id đã sắp xếp (ổn định cho test/log).
    pub fn agent_ids(&self) -> Vec<&str> {
        let mut ids: Vec<&str> = self.registry.list().map(|a| a.id.as_str()).collect();
        ids.sort_unstable();
        ids
    }

    pub fn tool_descriptors(&self) -> Vec<ToolDescriptor> {
        self.tools.values().map(|t| t.descriptor()).collect()
    }

    /// Non-secret routing fingerprint của provider alias (EMB-16).
    pub fn provider_fingerprint(&self, provider_id: &str) -> Option<&str> {
        self.fingerprints.get(provider_id).map(String::as_str)
    }

    pub fn base_instructions_version(&self) -> u32 {
        self.base_version
    }

    /// Mandatory base để PromptBuilder (M2) đưa vào mọi model request.
    #[allow(dead_code)] // PromptBuilder dùng từ M1/M2
    pub(crate) fn base_instructions(&self) -> &'static str {
        BASE_INSTRUCTIONS
    }
}

impl OrchestratorBuilder {
    pub fn register_tool(mut self, tool: Arc<dyn HostTool>) -> Self {
        self.tools.push(tool);
        self
    }

    /// Validate cấu hình cục bộ và dựng handle. Không tạo Tokio runtime, không gọi
    /// remote provider; chạy trong runtime hiện có của host.
    pub async fn build(self) -> Result<Orchestrator, OrchestratorError> {
        let base_version = parse_base_version(BASE_INSTRUCTIONS)?;

        self.config.llm.validate()?;

        let mut tools: BTreeMap<String, Arc<dyn HostTool>> = BTreeMap::new();
        for tool in self.tools {
            let desc = tool.descriptor();
            validate_descriptor(&desc)?;
            if tools.contains_key(&desc.name) {
                return Err(OrchestratorError::config(
                    ConfigReason::DuplicateTool,
                    format!("duplicate tool '{}'", desc.name),
                ));
            }
            tools.insert(desc.name, tool);
        }

        let names: HashSet<String> = tools.keys().cloned().collect();
        let registry = AgentRegistry::build(self.config.agents, &self.config.llm, &names).await?;

        let fingerprints = self
            .config
            .llm
            .providers
            .iter()
            .map(|p| (p.id.trim().to_string(), p.routing_fingerprint()))
            .collect();

        Ok(Orchestrator {
            base_version,
            registry,
            tools,
            fingerprints,
        })
    }
}

fn parse_base_version(base: &str) -> Result<u32, OrchestratorError> {
    let bad = || {
        OrchestratorError::config(
            ConfigReason::BaseInstructionsInvalid,
            "embedded base instructions are missing a valid version header",
        )
    };
    let first = base.lines().next().ok_or_else(bad)?;
    let version = first
        .strip_prefix(BASE_VERSION_PREFIX)
        .and_then(|r| r.strip_suffix(" -->"))
        .and_then(|v| v.trim().parse::<u32>().ok())
        .ok_or_else(bad)?;
    if base.lines().skip(1).all(|l| l.trim().is_empty()) {
        return Err(bad());
    }
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_base_is_valid_and_versioned() {
        assert_eq!(parse_base_version(BASE_INSTRUCTIONS), Ok(1));
    }

    #[test]
    fn base_version_rejects_malformed() {
        assert!(parse_base_version("").is_err());
        assert!(parse_base_version("no header\ntext").is_err());
        assert!(parse_base_version("<!-- base-instructions-version: x -->\ntext").is_err());
        assert!(parse_base_version("<!-- base-instructions-version: 1 -->\n\n  \n").is_err());
    }
}
