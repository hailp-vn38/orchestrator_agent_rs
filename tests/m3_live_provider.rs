//! M3 provider gate (LIVE): public host E2E qua `Orchestrator` thật, không fake gateway.
//!
//! Root A → child B (provider alias riêng) → Skill/HostTool trong child → child final → Root
//! follow-up **cùng child session_id** → Root final; sau reopen đọc đúng histories/linkage.
//!
//! Chạy tường minh (cần mạng + credentials trong `.env.live`, xem `.env.live.example`):
//!   cargo test --test m3_live_provider -- --ignored --nocapture
//!
//! Provider child mặc định trùng backend G0; có thể trỏ child sang backend/model khác bằng
//! `M3_CHILD_BASE_URL`, `M3_CHILD_MODEL`, `M3_CHILD_API_KEY`. Nếu không đặt, hai alias cùng
//! trỏ một backend: chứng minh routing theo alias/AgentDefinition nhưng **không** chứng minh
//! nhiều backend khác nhau — evidence phải ghi rõ giới hạn này.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, CallerContext, HostTool, LlmBackend, LlmConfig,
    LlmProviderConfig, MessageRole, Orchestrator, OrchestratorConfig, PageRequest, ProviderAuth,
    RunStatus, SecretString, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};

const CODE: &str = "LIME-7731";
const SKILL_TOKEN: &str = "SKILL-OK-4410";

fn load_env_file() -> std::collections::HashMap<String, String> {
    let path = std::env::var("LIVE_ENV_FILE")
        .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/.env.live").to_string());
    let Ok(text) = std::fs::read_to_string(path) else {
        return Default::default();
    };
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (k, v) = line.split_once('=')?;
            let v = v.trim();
            let v = v
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .or_else(|| v.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
                .unwrap_or(v);
            Some((k.trim().to_string(), v.to_string()))
        })
        .collect()
}

fn env(k: &str) -> Option<String> {
    static FILE: std::sync::OnceLock<std::collections::HashMap<String, String>> =
        std::sync::OnceLock::new();
    std::env::var(k)
        .ok()
        .or_else(|| FILE.get_or_init(load_env_file).get(k).cloned())
        .filter(|v| !v.trim().is_empty())
}

/// Tool read-only trả về mã mà model không thể đoán nếu không gọi tool.
struct LookupCode(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl HostTool for LookupCode {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "lookup_code".into(),
            description: "Return the secret project code. Takes no meaningful input.".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "topic": { "type": "string" } },
                "required": ["topic"]
            }),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(&self, ctx: ToolContext, _args: Value) -> Result<ToolOutcome, ToolError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!("The secret project code is {CODE}."),
            metadata: Value::Null,
        })
    }
}

async fn run(o: &Orchestrator, who: &CallerContext, sid: &str, text: &str) -> AgentResult {
    let turn = o.submit(who, sid, text.into()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
    loop {
        let r = o.result(who, sid, &turn.turn_id).await.unwrap();
        if r.status != RunStatus::Running {
            return r;
        }
        assert!(tokio::time::Instant::now() < deadline, "live run stuck");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

struct Link {
    child_session_id: Option<String>,
    child_turn_id: Option<String>,
}

async fn links(path: &std::path::Path, session_id: &str) -> Vec<Link> {
    let opts = SqliteConnectOptions::new().filename(path.join("orchestrator.db"));
    let mut c = SqliteConnection::connect_with(&opts).await.unwrap();
    sqlx::query(
        "SELECT child_session_id, child_turn_id FROM tool_invocations \
         WHERE session_id = ?1 AND tool_name = 'subagent' ORDER BY rowid",
    )
    .bind(session_id)
    .fetch_all(&mut c)
    .await
    .unwrap()
    .iter()
    .map(|r| Link {
        child_session_id: r.get(0),
        child_turn_id: r.get(1),
    })
    .collect()
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_root_delegates_to_child_with_skill_and_tool_then_follows_up_same_child() {
    let (Some(key), Some(base), Some(model)) = (
        env("G0_OPENAI_API_KEY").or_else(|| env("OPENAI_API_KEY")),
        env("G0_OPENAI_BASE_URL"),
        env("G0_OPENAI_MODEL"),
    ) else {
        panic!("BLOCKED: fill .env.live (see .env.live.example) or set G0_OPENAI_BASE_URL, G0_OPENAI_MODEL, G0_OPENAI_API_KEY");
    };
    let child_base = env("M3_CHILD_BASE_URL").unwrap_or_else(|| base.clone());
    let child_model = env("M3_CHILD_MODEL").unwrap_or_else(|| model.clone());
    let child_key = env("M3_CHILD_API_KEY").unwrap_or_else(|| key.clone());
    let distinct_backend = child_base != base || child_model != model;
    println!("child routing: distinct backend/model from root = {distinct_backend}");

    let dir = tempfile::tempdir().unwrap();
    let root_prompt = dir.path().join("root.md");
    std::fs::write(
        &root_prompt,
        "You are a coordinator. You never know the project code yourself: to learn anything \
         about the project you MUST delegate to a subagent with the `subagent` tool, and you \
         report what the subagent returned. To continue an earlier subagent conversation you \
         MUST pass the same `session_id` that the earlier subagent result gave you.",
    )
    .unwrap();
    let analyst_prompt = dir.path().join("analyst.md");
    std::fs::write(
        &analyst_prompt,
        "You are the analyst subagent. Use your tools and skills when the task asks for them, \
         then answer tersely with the facts you found.",
    )
    .unwrap();
    let skill_dir = dir.path().join("skills/pirate");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(
        skill_dir.join("SKILL.md"),
        format!("---\nname: pirate\ndescription: Answer-format rules. Load it when the task asks for the pirate skill.\n---\nWhen you have loaded this skill, end your next answer with the exact token {SKILL_TOKEN}.\n"),
    )
    .unwrap();

    let provider = |id: &str, endpoint: &str, key: &str| LlmProviderConfig {
        id: id.into(),
        backend: LlmBackend::OpenAiCompatible,
        endpoint: Some(endpoint.to_owned()),
        auth: ProviderAuth::ApiKey(SecretString::new(key)),
        request_timeout: Duration::from_secs(90),
    };
    let store = dir.path().join("store");
    let mut cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![
                provider("root-llm", &base, &key),
                provider("child-llm", &child_base, &child_key),
            ],
        },
        vec![
            AgentDefinition::new("coordinator", "root-llm", model.clone())
                .with_mode(AgentMode::Primary)
                .with_prompt_path(&root_prompt)
                .with_allowed_tools(["subagent"]),
            AgentDefinition::new("analyst", "child-llm", child_model.clone())
                .with_mode(AgentMode::Subagent)
                .with_description(
                    "Looks up the secret project code with its lookup_code tool and can load the pirate skill",
                )
                .with_prompt_path(&analyst_prompt)
                .with_allowed_tools(["lookup_code", "skill"])
                .with_allowed_skills(["pirate"]),
        ],
        &store,
    );
    cfg.skills_dir = Some(dir.path().join("skills"));

    let lookups = Arc::new(AtomicUsize::new(0));
    let o = Orchestrator::builder(cfg)
        .register_tool(Arc::new(LookupCode(lookups.clone())))
        .build()
        .await
        .unwrap();
    let who = CallerContext::new("live", "m3");
    let sid = o.create_session(&who, "coordinator").await.unwrap();

    // 1. Root → child B; child dùng HostTool + Skill rồi trả final.
    let r = run(
        &o,
        &who,
        &sid,
        "Delegate to the subagent with agent id 'analyst'. Tell it to use its lookup_code tool \
         with topic 'project' and to load the 'pirate' skill with its skill tool, then report \
         the code and end with the token the skill demands. Tell me what the analyst said.",
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "root error: {:?}", r.error);
    let out1 = r.output.expect("root final");
    println!(
        "root turn 1: completed, output_chars={}",
        out1.chars().count()
    );
    assert!(
        lookups.load(Ordering::SeqCst) >= 1,
        "child never used the HostTool"
    );
    let l = links(&store, &sid).await;
    assert!(!l.is_empty(), "root never called the subagent tool");
    let child = l[0].child_session_id.clone().expect("child link");
    assert!(l[0].child_turn_id.is_some());
    let child_hist = o
        .history(&who, &child, PageRequest::first(100))
        .await
        .unwrap()
        .items;
    let child_tools: Vec<_> = child_hist
        .iter()
        .filter(|m| m.role == MessageRole::Tool)
        .collect();
    assert!(
        child_tools.iter().any(|m| m.text.contains(CODE)),
        "child history lacks the HostTool result"
    );
    assert!(
        child_tools.iter().any(|m| m.text.contains(SKILL_TOKEN)),
        "child history lacks the loaded skill"
    );
    assert!(
        out1.contains(CODE),
        "root answer did not carry the child's finding"
    );

    // 2. Root follow-up với **cùng child session_id**.
    let r = run(
        &o,
        &who,
        &sid,
        "Now ask the SAME analyst subagent again by resuming its earlier session (pass the \
         session_id from its previous result, agent id 'analyst') and ask it: which project \
         code did you look up earlier? Tell me its one-line answer.",
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "root error: {:?}", r.error);
    let out2 = r.output.expect("root final 2");
    println!(
        "root turn 2: completed, output_chars={}",
        out2.chars().count()
    );
    let l = links(&store, &sid).await;
    assert!(l.len() >= 2, "expected a second subagent invocation");
    let resumed = l
        .iter()
        .skip(1)
        .find(|x| x.child_session_id.as_deref() == Some(child.as_str()));
    let resumed = resumed.expect("follow-up did not reuse the same child session_id");
    assert_ne!(
        resumed.child_turn_id, l[0].child_turn_id,
        "a new logical turn"
    );
    assert!(
        out2.contains(CODE),
        "follow-up answer lost the child's context"
    );
    o.shutdown().await.unwrap();
    drop(o);

    // 3. Reopen: histories + linkage đọc đúng, không gọi model.
    let o2 = Orchestrator::builder({
        let mut c = OrchestratorConfig::new(
            LlmConfig {
                providers: vec![
                    provider("root-llm", &base, &key),
                    provider("child-llm", &child_base, &child_key),
                ],
            },
            vec![
                AgentDefinition::new("coordinator", "root-llm", model)
                    .with_mode(AgentMode::Primary)
                    .with_prompt_path(&root_prompt)
                    .with_allowed_tools(["subagent"]),
                AgentDefinition::new("analyst", "child-llm", child_model)
                    .with_mode(AgentMode::Subagent)
                    .with_description("analyst")
                    .with_prompt_path(&analyst_prompt),
            ],
            &store,
        );
        c.skills_dir = None;
        c
    })
    .build()
    .await
    .unwrap();
    let l2 = links(&store, &sid).await;
    assert_eq!(l2.len(), l.len());
    let root_hist = o2
        .history(&who, &sid, PageRequest::first(100))
        .await
        .unwrap()
        .items;
    assert!(
        root_hist
            .iter()
            .any(|m| m.role == MessageRole::Tool
                && m.text.contains(&format!("sessionID=\"{child}\"")))
    );
    let child_hist2 = o2
        .history(&who, &child, PageRequest::first(100))
        .await
        .unwrap()
        .items;
    assert!(
        child_hist2.len() >= child_hist.len(),
        "child history shrank after reopen"
    );
    let turn = resumed.child_turn_id.clone().unwrap();
    let cr = o2.result(&who, &child, &turn).await.unwrap();
    assert_eq!(cr.status, RunStatus::Completed);
    assert!(cr.output.is_some_and(|t| t.contains(CODE)));
    println!("reopen: child history messages={}", child_hist2.len());
    o2.shutdown().await.unwrap();
}
