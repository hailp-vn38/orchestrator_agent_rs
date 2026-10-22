//! AgentDefinition + AgentRegistry (docs/02 §2.2–2.3). Host DTO (YAML/TOML) được host
//! map sang đây; core không deserialize trực tiếp.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::error::{ConfigReason, OrchestratorError};
use crate::gateway::ModelParams;
use crate::tool::RESERVED_TOOL_NAMES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentMode {
    Primary,
    Subagent,
    All,
}

#[derive(Debug, Clone)]
pub struct AgentDefinition {
    pub id: String,
    pub mode: AgentMode,
    pub description: String,
    pub prompt_path: PathBuf,
    pub provider_id: String,
    pub model_id: String,
    pub allowed_tools: Vec<String>,
    pub allowed_skills: Vec<String>,
    /// Danh sách subagent được phép ủy quyền qua tool `subagent`.
    /// `None` = cho phép mọi agent có `mode != AgentMode::Primary` (mặc định);
    /// `Some(list)` = chỉ quảng bá và cho phép gọi các subagent nằm trong danh sách này.
    pub allowed_subagents: Option<Vec<String>>,
    /// `None` = không giới hạn; `Some(n)` yêu cầu `n > 0`.
    pub steps: Option<u32>,
    /// Context window (token ước lượng) của model agent này, dùng cho compaction. `None` =
    /// `RuntimeConfig::context_limit_tokens`; `Some(n)` yêu cầu `n > 0`. Root và Child có thể
    /// dùng model có window khác nhau.
    pub context_limit_tokens: Option<usize>,
    /// Tham số sinh của model (output token cap, temperature, ...). Mặc định: để provider quyết
    /// định, riêng trần output giữ mặc định của gateway.
    pub llm: ModelParams,
}

impl AgentDefinition {
    /// Mặc định mode `All`, không tool/skill, không step cap. `prompt_path` phải đặt
    /// qua [`AgentDefinition::with_prompt_path`] (build kiểm tra file tồn tại).
    pub fn new(
        id: impl Into<String>,
        provider_id: impl Into<String>,
        model_id: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            mode: AgentMode::All,
            description: String::new(),
            prompt_path: PathBuf::new(),
            provider_id: provider_id.into(),
            model_id: model_id.into(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            allowed_subagents: None,
            steps: None,
            context_limit_tokens: None,
            llm: ModelParams::default(),
        }
    }

    pub fn with_mode(mut self, mode: AgentMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    pub fn with_prompt_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.prompt_path = path.into();
        self
    }

    pub fn with_allowed_tools<I, S>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_tools = tools.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_allowed_skills<I, S>(mut self, skills: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_skills = skills.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_allowed_subagents<I, S>(mut self, subagents: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_subagents = Some(subagents.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_steps(mut self, steps: u32) -> Self {
        self.steps = Some(steps);
        self
    }

    pub fn with_context_limit_tokens(mut self, tokens: usize) -> Self {
        self.context_limit_tokens = Some(tokens);
        self
    }

    /// Đặt toàn bộ tham số sinh của model cho agent này.
    pub fn with_model_params(mut self, params: ModelParams) -> Self {
        self.llm = params;
        self
    }

    /// Trần output token của mỗi request LLM (`> 0`).
    pub fn with_max_output_tokens(mut self, tokens: u32) -> Self {
        self.llm.max_output_tokens = Some(tokens);
        self
    }

    /// Temperature lấy mẫu (`0.0..=2.0`).
    pub fn with_temperature(mut self, temperature: f32) -> Self {
        self.llm.temperature = Some(temperature);
        self
    }

    /// Nucleus sampling `top_p` (`0.0 < p <= 1.0`).
    pub fn with_top_p(mut self, top_p: f32) -> Self {
        self.llm.top_p = Some(top_p);
        self
    }

    /// `top_k` (`> 0`); không phải backend nào cũng hỗ trợ.
    pub fn with_top_k(mut self, top_k: u32) -> Self {
        self.llm.top_k = Some(top_k);
        self
    }
}

/// Registry bất biến sau khi build; mỗi run sẽ snapshot definition đã resolve (M1+).
#[derive(Debug, Clone)]
pub struct AgentRegistry {
    definitions: HashMap<String, AgentDefinition>,
}

impl AgentRegistry {
    /// Validate toàn bộ definitions. `registered_tools` là tên HostTool đã đăng ký;
    /// built-in (`subagent`, `skill`) luôn hợp lệ trong `allowed_tools`.
    pub(crate) async fn build(
        definitions: Vec<AgentDefinition>,
        provider_ids: &HashSet<String>,
        registered_tools: &HashSet<String>,
    ) -> Result<Self, OrchestratorError> {
        if definitions.is_empty() {
            return Err(OrchestratorError::config(
                ConfigReason::NoAgents,
                "at least one agent definition is required",
            ));
        }
        let providers: HashSet<&str> = provider_ids.iter().map(String::as_str).collect();
        let mut map = HashMap::with_capacity(definitions.len());
        for def in definitions {
            validate_definition(&def, &providers, registered_tools).await?;
            if map.contains_key(&def.id) {
                return Err(OrchestratorError::config(
                    ConfigReason::DuplicateAgentId,
                    format!("duplicate agent id '{}'", def.id),
                ));
            }
            map.insert(def.id.clone(), def);
        }
        for def in map.values() {
            if let Some(ref allowed) = def.allowed_subagents {
                for sub_id in allowed {
                    match map.get(sub_id) {
                        None => {
                            return Err(OrchestratorError::config(
                                ConfigReason::InvalidRuntimeConfig,
                                format!("agent '{}' allows unknown subagent '{sub_id}'", def.id),
                            ));
                        }
                        Some(target) if target.mode == AgentMode::Primary => {
                            return Err(OrchestratorError::config(
                                ConfigReason::InvalidRuntimeConfig,
                                format!(
                                    "agent '{}' cannot allow primary agent '{sub_id}' as subagent",
                                    def.id
                                ),
                            ));
                        }
                        Some(_) => {}
                    }
                }
            }
        }
        Ok(Self { definitions: map })
    }

    pub fn get(&self, id: &str) -> Option<&AgentDefinition> {
        self.definitions.get(id)
    }

    pub fn list(&self) -> impl Iterator<Item = &AgentDefinition> {
        self.definitions.values()
    }
}

async fn validate_definition(
    def: &AgentDefinition,
    providers: &HashSet<&str>,
    registered_tools: &HashSet<String>,
) -> Result<(), OrchestratorError> {
    let err = |r, m: String| Err(OrchestratorError::config(r, m));
    if def.id.trim().is_empty() {
        return err(
            ConfigReason::EmptyAgentId,
            "agent id must not be empty".into(),
        );
    }
    let id = &def.id;
    if !providers.contains(def.provider_id.trim()) {
        return err(
            ConfigReason::UnknownProvider,
            format!(
                "agent '{id}' references unknown provider '{}'",
                def.provider_id
            ),
        );
    }
    if def.model_id.trim().is_empty() {
        return err(
            ConfigReason::EmptyModelId,
            format!("agent '{id}': model_id must not be empty"),
        );
    }
    if def.steps == Some(0) {
        return err(
            ConfigReason::InvalidSteps,
            format!("agent '{id}': steps must be > 0 when set"),
        );
    }
    if def.context_limit_tokens == Some(0) {
        return err(
            ConfigReason::InvalidRuntimeConfig,
            format!("agent '{id}': context_limit_tokens must be > 0 when set"),
        );
    }
    if let Err(reason) = def.llm.validate() {
        return err(
            ConfigReason::InvalidRuntimeConfig,
            format!("agent '{id}': {reason}"),
        );
    }
    for tool in &def.allowed_tools {
        if !RESERVED_TOOL_NAMES.contains(&tool.as_str()) && !registered_tools.contains(tool) {
            return err(
                ConfigReason::UnknownTool,
                format!("agent '{id}' allows unregistered tool '{tool}'"),
            );
        }
    }
    let is_file = if def.prompt_path.as_os_str().is_empty() {
        false
    } else {
        tokio::fs::metadata(&def.prompt_path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
    };
    if !is_file {
        // Không echo đường dẫn: có thể lộ cấu trúc filesystem của host.
        return err(
            ConfigReason::PromptPathInvalid,
            format!("agent '{id}': prompt_path must be an existing file"),
        );
    }
    Ok(())
}
