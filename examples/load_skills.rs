//! Live example: agent load skill qua tool `skill(id)` từ `docs/examples/skills`.
//!
//! Chạy: `cargo run --example load_skills -- [--env-file .env.live]`
//!
//! Skills được scan một lần lúc `build()` (`OrchestratorConfig::with_skills_dir`); agent chỉ
//! thấy các skill trong `with_allowed_skills` và phải có tool `skill` trong `allowed_tools`.
//! Cấu hình provider đọc từ file giống `live_flow` (G0_OPENAI_BASE_URL/MODEL/API_KEY).

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, CallerContext, InvocationStatus, LlmBackend, LlmConfig,
    LlmProviderConfig, Orchestrator, OrchestratorConfig, ProviderAuth, RunStatus, SecretString,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn read_config(path: &std::path::Path) -> Result<HashMap<String, String>> {
    let text = std::fs::read_to_string(path)?;
    Ok(text
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            let value = value.trim();
            let value = value
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .unwrap_or(value);
            Some((key.trim().into(), value.into()))
        })
        .collect())
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let env_file = std::env::args()
        .skip(1)
        .skip_while(|a| a != "--env-file")
        .nth(1)
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("LIVE_ENV_FILE").map(PathBuf::from))
        .unwrap_or_else(|| root.join(".env.live"));
    let config = read_config(&env_file)?;
    let required = |key: &str| -> Result<String> {
        config
            .get(key)
            .filter(|s| !s.trim().is_empty())
            .cloned()
            .ok_or_else(|| io::Error::other(format!("Thiếu {key} trong file cấu hình")).into())
    };
    let (endpoint, model, key) = (
        required("G0_OPENAI_BASE_URL")?,
        required("G0_OPENAI_MODEL")?,
        required("G0_OPENAI_API_KEY")?,
    );

    let work =
        std::env::temp_dir().join(format!("orchestrator-load-skills-{}", std::process::id()));
    std::fs::create_dir_all(&work)?;
    let prompt = work.join("reviewer.md");
    std::fs::write(
        &prompt,
        "You are a reviewer. When asked for a workflow, call skill(id) to load the relevant \
         skill, then summarize its steps briefly.",
    )?;

    let cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "live".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(endpoint),
                auth: ProviderAuth::ApiKey(SecretString::new(key)),
                request_timeout: Duration::from_secs(60),
            }],
        },
        vec![AgentDefinition::new("reviewer", "live", &model)
            .with_mode(AgentMode::Primary)
            .with_prompt_path(prompt)
            .with_allowed_tools(["skill"])
            .with_allowed_skills(["rust-review"])],
        work.join("store"),
    )
    // Thư mục chứa `<id>/SKILL.md`; id = tên thư mục.
    .with_skills_dir(root.join("docs/examples/skills"));

    let o = Orchestrator::builder(cfg).build().await?;
    let outcome = run(&o).await;
    o.shutdown().await?;
    std::fs::remove_dir_all(&work)?;
    outcome
}

async fn run(o: &Orchestrator) -> Result<()> {
    let caller = CallerContext::new("demo", "load-skills");
    let sid = o.create_session(&caller, "reviewer").await?;
    let turn = o
        .submit(
            &caller,
            &sid,
            "Load the rust-review skill and list its workflow steps.".into(),
        )
        .await?;

    let result = loop {
        let r = o.result(&caller, &sid, &turn.turn_id).await?;
        if r.status != RunStatus::Running {
            break r;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    println!("status: {}", result.status.as_str());
    println!("output: {}", result.output.unwrap_or_default());

    let snapshot = o.snapshot(&caller, &sid).await?;
    let loaded = snapshot.sessions().any(|s| {
        s.invocations
            .iter()
            .any(|i| i.tool_name == "skill" && i.status == InvocationStatus::Completed)
    });
    println!("skill tool completed: {loaded}");
    if !loaded {
        return Err(io::Error::other("Agent chưa load skill thành công").into());
    }
    Ok(())
}
