//! M6 provider gate (LIVE): một family Root/Child thật cho thấy `text.delta`/`tool.progress` rồi
//! canonical terminal/result, qua `Orchestrator::subscribe`/`snapshot` (không fake gateway).
//!
//! Chạy tường minh (cần mạng + credentials trong `.env.live`, xem `.env.live.example`):
//!   cargo test --test m6_live_provider -- --ignored --nocapture
//!
//! Đây là evidence **transport-neutral** (không phải HTTP/SSE wire) và **không** thay AT-48 /
//! parity text-tool của hai provider: một backend OpenAI-compatible, hai alias cùng backend.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, CallerContext, ExecutionEvent, FeedItem, HostTool, LlmBackend,
    LlmConfig, LlmProviderConfig, Orchestrator, OrchestratorConfig, ProviderAuth, RunStatus,
    SecretString, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};

const CODE: &str = "LIME-7731";

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

/// Tool read-only chạy vài nhịp progress rồi trả mã.
struct SlowLookup(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl HostTool for SlowLookup {
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
        for i in 1..=3 {
            ctx.progress(&format!("searching archive, pass {i}/3"));
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!("The secret project code is {CODE}."),
            metadata: Value::Null,
        })
    }
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_family_shows_deltas_and_progress_then_canonical_terminal() {
    let (Some(key), Some(base), Some(model)) = (
        env("G0_OPENAI_API_KEY").or_else(|| env("OPENAI_API_KEY")),
        env("G0_OPENAI_BASE_URL"),
        env("G0_OPENAI_MODEL"),
    ) else {
        panic!("BLOCKED: fill .env.live (see .env.live.example) or set G0_OPENAI_BASE_URL, G0_OPENAI_MODEL, G0_OPENAI_API_KEY");
    };

    let dir = tempfile::tempdir().unwrap();
    let root_prompt = dir.path().join("root.md");
    std::fs::write(
        &root_prompt,
        "You are a coordinator. You never know the project code yourself: to learn anything \
         about the project you MUST delegate to a subagent with the `subagent` tool, and you \
         report what the subagent returned.",
    )
    .unwrap();
    let analyst_prompt = dir.path().join("analyst.md");
    std::fs::write(
        &analyst_prompt,
        "You are the analyst subagent. Use your lookup_code tool when the task asks for the \
         code, then answer tersely with the facts you found.",
    )
    .unwrap();

    let provider = |id: &str| LlmProviderConfig {
        id: id.into(),
        backend: LlmBackend::OpenAiCompatible,
        endpoint: Some(base.clone()),
        auth: ProviderAuth::ApiKey(SecretString::new(key.clone())),
        request_timeout: Duration::from_secs(90),
    };
    let cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![provider("root-llm"), provider("child-llm")],
        },
        vec![
            AgentDefinition::new("coordinator", "root-llm", model.clone())
                .with_mode(AgentMode::Primary)
                .with_prompt_path(&root_prompt)
                .with_allowed_tools(["subagent"]),
            AgentDefinition::new("analyst", "child-llm", model.clone())
                .with_mode(AgentMode::Subagent)
                .with_description("Looks up the secret project code with its lookup_code tool")
                .with_prompt_path(&analyst_prompt)
                .with_allowed_tools(["lookup_code"]),
        ],
        dir.path().join("store"),
    );

    let lookups = Arc::new(AtomicUsize::new(0));
    let o = Orchestrator::builder(cfg)
        .register_tool(Arc::new(SlowLookup(lookups.clone())))
        .build()
        .await
        .unwrap();
    let who = CallerContext::new("live", "m6");
    let sid = o.create_session(&who, "coordinator").await.unwrap();

    // Subscribe TRƯỚC khi submit/snapshot.
    let mut sub = o.subscribe(&who, &sid).await.unwrap();
    let events: Arc<Mutex<Vec<ExecutionEvent>>> = Arc::default();
    let sink = events.clone();
    let reader = tokio::spawn(async move {
        while let Some(item) = sub.recv().await {
            if let FeedItem::Event(e) = item {
                sink.lock().unwrap().push(e);
            }
        }
    });

    let turn = o
        .submit(
            &who,
            &sid,
            "Delegate to the subagent with agent id 'analyst'. Tell it to use its lookup_code \
             tool with topic 'project' and report the code. Tell me what the analyst said."
                .into(),
        )
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
    let result = loop {
        let r = o.result(&who, &sid, &turn.turn_id).await.unwrap();
        if r.status != RunStatus::Running {
            break r;
        }
        assert!(tokio::time::Instant::now() < deadline, "live run stuck");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(result.status, RunStatus::Completed, "{:?}", result.error);
    assert!(result.output.as_deref().is_some_and(|t| t.contains(CODE)));
    assert!(
        lookups.load(Ordering::SeqCst) >= 1,
        "child never used the tool"
    );

    // Chờ event terminal của Root rồi dừng reader.
    let wait_until = tokio::time::Instant::now() + Duration::from_secs(10);
    while !events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e.event_type == "run.completed" && e.session_id == sid)
    {
        assert!(
            tokio::time::Instant::now() < wait_until,
            "no root run.completed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Snapshot canonical hội tụ: Root và Child đều terminal, child được khám phá.
    let snap = o.snapshot(&who, &sid).await.unwrap();
    assert!(!snap.has_active_work());
    assert_eq!(snap.children.len(), 1);
    assert_eq!(
        snap.children[0].latest_run.as_ref().map(|r| r.status),
        Some(RunStatus::Completed)
    );
    o.shutdown().await.unwrap();
    let _ = reader.await;

    let events = events.lock().unwrap().clone();
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    println!("event sequence ({} events): {types:?}", events.len());
    let child = events
        .iter()
        .find(|e| e.event_type == "run.started" && e.session_id != sid)
        .expect("child run.started")
        .session_id
        .clone();
    let pos = |pred: &dyn Fn(&ExecutionEvent) -> bool| events.iter().position(pred).unwrap();
    let changed = pos(&|e| e.event_type == "session.changed" && e.session_id == sid);
    let child_started = pos(&|e| e.event_type == "run.started" && e.session_id == child);
    let progress = pos(&|e| e.event_type == "tool.progress" && e.session_id == child);
    let child_done = pos(&|e| e.event_type == "run.completed" && e.session_id == child);
    let parent_tool_done = pos(&|e| e.event_type == "tool.completed" && e.session_id == sid);
    let root_done = pos(&|e| e.event_type == "run.completed" && e.session_id == sid);
    assert!(changed < child_started && child_started < progress);
    assert!(progress < child_done && child_done < parent_tool_done && parent_tool_done < root_done);
    assert!(events.iter().all(|e| e.root_session_id == sid));
    let deltas = events
        .iter()
        .filter(|e| e.event_type == "text.delta")
        .count();
    println!("text.delta events: {deltas}");
    assert!(deltas > 0, "live provider phải stream text");
    assert!(events
        .iter()
        .filter(|e| e.event_type == "text.delta" || e.event_type == "tool.progress")
        .all(|e| e.state_revision.is_none()));
    let wire: String = events.iter().map(|e| e.to_json().to_string()).collect();
    assert!(!wire.contains(&key), "API key lọt vào event");
}
