//! Public host example (M0 gate): host Tokio build/instantiate, không Axum.
//!
//! Chạy: `cargo run --example host`
//!
//! Host-side DTO mô phỏng `docs/examples/agents.yaml`; host tự map sang `AgentDefinition`
//! (core không deserialize YAML trực tiếp).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, HostTool, LlmBackend, LlmConfig, LlmProviderConfig, Orchestrator,
    OrchestratorConfig, ProviderAuth, SecretString, ToolContext, ToolDescriptor, ToolEffect,
    ToolError, ToolOutcome,
};

struct AgentDto {
    key: &'static str,
    mode: &'static str,
    description: &'static str,
    prompt: &'static str,
    provider: &'static str,
    model: &'static str,
    tools: &'static [&'static str],
    skills: &'static [&'static str],
}

fn map_dto(dto: &AgentDto, root: &std::path::Path) -> AgentDefinition {
    let mode = match dto.mode {
        "primary" => AgentMode::Primary,
        "subagent" => AgentMode::Subagent,
        _ => AgentMode::All,
    };
    AgentDefinition::new(dto.key, dto.provider, dto.model)
        .with_mode(mode)
        .with_description(dto.description)
        .with_prompt_path(root.join(dto.prompt))
        .with_allowed_tools(dto.tools.iter().copied())
        .with_allowed_skills(dto.skills.iter().copied())
}

struct EchoTool(&'static str);

#[async_trait::async_trait]
impl HostTool for EchoTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: self.0.to_string(),
            description: "Example read-only tool".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "query": { "type": "string" } },
                "required": ["query"]
            }),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(
        &self,
        context: ToolContext,
        args: serde_json::Value,
    ) -> Result<ToolOutcome, ToolError> {
        Ok(ToolOutcome {
            call_id: context.scope().tool_call_id.clone(),
            content: args.to_string(),
            metadata: serde_json::Value::Null,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docs/examples");

    // Host resolve secrets trước build(); ở đây là giá trị giả cho ví dụ.
    let llm = LlmConfig {
        providers: vec![
            LlmProviderConfig {
                id: "openai".into(),
                backend: LlmBackend::OpenAi,
                endpoint: None,
                auth: ProviderAuth::ApiKey(SecretString::new("example-not-a-real-key")),
                request_timeout: Duration::from_secs(120),
            },
            LlmProviderConfig {
                id: "anthropic".into(),
                backend: LlmBackend::Anthropic,
                endpoint: None,
                auth: ProviderAuth::ApiKey(SecretString::new("example-not-a-real-key")),
                request_timeout: Duration::from_secs(120),
            },
            LlmProviderConfig {
                id: "google".into(),
                backend: LlmBackend::Gemini,
                endpoint: None,
                auth: ProviderAuth::ApiKey(SecretString::new("example-not-a-real-key")),
                request_timeout: Duration::from_secs(120),
            },
        ],
    };

    let dtos = [
        AgentDto {
            key: "root",
            mode: "primary",
            description: "Root orchestrator",
            prompt: "prompts/root.md",
            provider: "openai",
            model: "gpt-4.1-mini",
            tools: &["subagent", "skill", "read_file", "search_code"],
            skills: &["task-planning"],
        },
        AgentDto {
            key: "explorer",
            mode: "subagent",
            description: "Explore codebase and report findings",
            prompt: "prompts/explorer.md",
            provider: "google",
            model: "gemini-2.5-flash",
            tools: &["skill", "read_file", "search_code"],
            skills: &["rust-review"],
        },
        AgentDto {
            key: "reviewer",
            mode: "subagent",
            description: "Review code correctness",
            prompt: "prompts/reviewer.md",
            provider: "anthropic",
            model: "claude-sonnet-4-20250514",
            tools: &["skill", "read_file", "search_code"],
            skills: &["rust-review"],
        },
    ];

    // Execution store do library sở hữu: dùng thư mục riêng, tách khỏi DB nghiệp vụ của host.
    let store_dir =
        std::env::temp_dir().join(format!("orchestrator-host-example-{}", std::process::id()));
    let config = OrchestratorConfig::new(
        llm,
        dtos.iter().map(|d| map_dto(d, &root)).collect(),
        &store_dir,
    );

    let orchestrator = Orchestrator::builder(config)
        .register_tool(Arc::new(EchoTool("read_file")))
        .register_tool(Arc::new(EchoTool("search_code")))
        .build()
        .await?;

    println!("agents: {:?}", orchestrator.agent_ids());
    println!(
        "base instructions v{}",
        orchestrator.base_instructions_version()
    );
    orchestrator.shutdown().await?;
    std::fs::remove_dir_all(&store_dir)?;
    Ok(())
}
