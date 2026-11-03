//! SPIKE (vứt đi): Task Recipe chạy như một adapter `ProviderRuntime` xác định.
//!
//! Root dùng provider `recipe`: adapter đọc transcript và phát tool call kế tiếp bằng code,
//! không gọi model. Child dùng provider `llm` (giả). Câu hỏi: runtime hiện có có chạy được
//! chuỗi bước cố định, Agent Step qua `subagent`, và chạy tiếp sau restart không?

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestrator_agent_rs::{
    ModelUsage,
    AgentDefinition, AgentMode, AgentResult, CallerContext, GatewayError, HostTool, LlmConfig,
    ModelRequest, ModelResponse, ModelRole, ModelToolCall, Orchestrator, OrchestratorConfig,
    ProviderRuntime, RunStatus, RuntimeProvider, StreamSink, ToolContext, ToolDescriptor,
    ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

// ───────────── Recipe = hàm thuần từ transcript sang bước kế tiếp ─────────────

/// Bước kế tiếp của recipe "collect": fetch_list → subagent(classifier) → save_item → xong.
/// Tiến độ đọc từ tool result trong transcript, không có state riêng.
fn next_step(req: &ModelRequest) -> ModelResponse {
    let results: Vec<(&str, &str)> = req
        .messages
        .iter()
        .filter(|m| m.role == ModelRole::Tool)
        .map(|m| (m.tool_name.as_deref().unwrap_or(""), m.text.as_str()))
        .collect();
    let call = |name: &str, args: Value| ModelResponse {
        text: String::new(),
        finish_reason: "tool_calls".into(),
        tool_calls: vec![ModelToolCall {
            id: format!("r{}", results.len() + 1),
            name: name.into(),
            arguments: args,
        }],
        usage: None,
    };
    match results.as_slice() {
        [] => call("fetch_list", json!({ "text": "PL1" })),
        [(_, list)] => call(
            "subagent",
            json!({
                "agent": "classifier",
                "description": "classify",
                "prompt": format!("Choose domain/topic for: {list}"),
            }),
        ),
        [_, (_, child)] => {
            let topic = child
                .lines()
                .find_map(|l| l.strip_prefix("TOPIC="))
                .unwrap_or("unknown");
            call("save_item", json!({ "text": topic }))
        }
        _ => ModelResponse {
            text: format!("recipe done: {} steps", results.len()),
            finish_reason: "stop".into(),
            tool_calls: Vec::new(),
            usage: None,
        },
    }
}

struct Runtime {
    llm_calls: AtomicUsize,
    recipe_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl ProviderRuntime for Runtime {
    fn providers(&self) -> Vec<RuntimeProvider> {
        vec![
            RuntimeProvider::new("recipe", "recipe-v1"),
            RuntimeProvider::new("llm", "llm-fp"),
        ]
    }

    async fn complete(
        &self,
        request: &ModelRequest,
        _cancel: &CancellationToken,
        _sink: StreamSink<'_>,
    ) -> Result<ModelResponse, GatewayError> {
        if request.provider_id == "recipe" {
            self.recipe_calls.fetch_add(1, Ordering::SeqCst);
            if request.model_id == "fanout@1" {
                return Ok(fanout_step(request));
            }
            return Ok(next_step(request));
        }
        self.llm_calls.fetch_add(1, Ordering::SeqCst);
        Ok(ModelResponse {
            text: "TOPIC=trading/ict".into(),
            finish_reason: "stop".into(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

// ───────────── Tool ghi lại lời gọi ─────────────

struct Probe {
    name: &'static str,
    effect: ToolEffect,
    delay: Duration,
    calls: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl HostTool for Probe {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: self.name.into(),
            description: format!("probe {}", self.name),
            input_schema: json!({
                "type": "object",
                "properties": { "text": { "type": "string" } },
                "required": ["text"],
                "additionalProperties": false
            }),
            effect: self.effect,
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        let text = args["text"].as_str().unwrap_or_default().to_owned();
        self.calls.lock().unwrap().push(format!("{}:{text}", self.name));
        tokio::time::sleep(self.delay).await;
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!("{}:{text}:vid-a,vid-b", self.name),
            metadata: Value::Null,
        })
    }
}

fn config(dir: &tempfile::TempDir) -> OrchestratorConfig {
    std::fs::write(dir.path().join("root.md"), "recipe root").unwrap();
    std::fs::write(dir.path().join("classifier.md"), "classifier").unwrap();
    let mut cfg = OrchestratorConfig::new(
        LlmConfig::default(),
        vec![
            AgentDefinition::new("root", "recipe", "collect@1")
                .with_mode(AgentMode::Primary)
                .with_prompt_path(dir.path().join("root.md"))
                .with_allowed_tools(["fetch_list", "subagent", "save_item"])
                .with_allowed_subagents(["classifier"]),
            AgentDefinition::new("classifier", "llm", "m")
                .with_mode(AgentMode::Subagent)
                .with_description("choose topic")
                .with_prompt_path(dir.path().join("classifier.md")),
        ],
        dir.path().join("store"),
    );
    cfg.runtime.supervisor_sweep = Duration::from_millis(50);
    cfg.runtime.shutdown_grace = Duration::from_millis(200);
    cfg
}

async fn build(
    dir: &tempfile::TempDir,
    rt: &Arc<Runtime>,
    calls: &Arc<Mutex<Vec<String>>>,
    fetch_delay: Duration,
) -> Orchestrator {
    let tool = |name, effect, delay| -> Arc<dyn HostTool> {
        Arc::new(Probe {
            name,
            effect,
            delay,
            calls: calls.clone(),
        })
    };
    Orchestrator::builder(config(dir))
        .with_provider_runtime(rt.clone())
        .register_tool(tool("fetch_list", ToolEffect::ReadOnly, fetch_delay))
        .register_tool(tool("save_item", ToolEffect::Mutating, Duration::ZERO))
        .build()
        .await
        .unwrap()
}

fn who() -> CallerContext {
    CallerContext::new("alice", "ws")
}

async fn wait_terminal(o: &Orchestrator, s: &str, t: &str) -> AgentResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let r = o.result(&who(), s, t).await.unwrap();
        if r.status != RunStatus::Running && r.status != RunStatus::Interrupted {
            return r;
        }
        assert!(tokio::time::Instant::now() < deadline, "run stuck: {r:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn runtime() -> Arc<Runtime> {
    Arc::new(Runtime {
        llm_calls: AtomicUsize::new(0),
        recipe_calls: AtomicUsize::new(0),
    })
}

#[tokio::test]
async fn recipe_runs_fixed_steps_and_agent_step_through_existing_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let o = build(&dir, &rt, &calls, Duration::ZERO).await;

    let sid = o.create_session(&who(), "root").await.unwrap();
    let turn = o.submit(&who(), &sid, r#"{"playlist":"PL1"}"#.into()).await.unwrap();
    let r = wait_terminal(&o, &sid, &turn.turn_id).await;

    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(r.output.as_deref(), Some("recipe done: 3 steps"));
    assert_eq!(
        *calls.lock().unwrap(),
        ["fetch_list:PL1", "save_item:trading/ict"],
        "tool steps in order, Agent Step output fed into the next step"
    );
    assert_eq!(rt.llm_calls.load(Ordering::SeqCst), 1, "only the Agent Step hit an LLM");
    spike_log(format!(
        "SPIKE steps={} recipe_calls={} tokens={}/{}",
        r.steps,
        rt.recipe_calls.load(Ordering::SeqCst),
        r.input_tokens,
        r.output_tokens
    ));
    o.shutdown().await.unwrap();
}

#[tokio::test]
async fn recipe_continues_after_restart_mid_tool_step() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let o = build(&dir, &rt, &calls, Duration::from_secs(3)).await;

    let sid = o.create_session(&who(), "root").await.unwrap();
    let turn = o.submit(&who(), &sid, r#"{"playlist":"PL1"}"#.into()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while calls.lock().unwrap().is_empty() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    o.shutdown().await.unwrap();

    let o2 = build(&dir, &rt, &calls, Duration::ZERO).await;
    let r = wait_terminal(&o2, &sid, &turn.turn_id).await;
    spike_log(format!("SPIKE restart status={:?} attempt={} steps={} calls={:?}", r.status, r.attempt, r.steps, calls.lock().unwrap()));
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(calls.lock().unwrap().last().map(String::as_str), Some("save_item:trading/ict"));
    o2.shutdown().await.unwrap();
}

fn spike_log(line: String) {
    use std::io::Write;
    if let Ok(path) = std::env::var("SPIKE_OUT") {
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap();
        writeln!(f, "{line}").unwrap();
    }
}

/// Lô 5 mục trong một bước: một response chứa nhiều tool call, usage = 0.
fn fanout_step(req: &ModelRequest) -> ModelResponse {
    let done = req.messages.iter().any(|m| m.role == ModelRole::Tool);
    if done {
        return ModelResponse {
            text: "fanout done".into(),
            finish_reason: "stop".into(),
            tool_calls: Vec::new(),
            usage: Some(ModelUsage::new(0, 0)),
        };
    }
    ModelResponse {
        text: String::new(),
        finish_reason: "tool_calls".into(),
        tool_calls: (1..=5)
            .map(|i| ModelToolCall {
                id: format!("f{i}"),
                name: "save_item".into(),
                arguments: json!({ "text": format!("vid-{i}") }),
            })
            .collect(),
        usage: Some(ModelUsage::new(0, 0)),
    }
}

#[tokio::test]
async fn recipe_batch_is_many_tool_calls_in_one_step_with_zero_usage() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut cfg = config(&dir);
    cfg.agents[0] = AgentDefinition::new("root", "recipe", "fanout@1")
        .with_mode(AgentMode::Primary)
        .with_prompt_path(dir.path().join("root.md"))
        .with_allowed_tools(["save_item"])
        .with_steps(2);
    let o = Orchestrator::builder(cfg)
        .with_provider_runtime(rt.clone())
        .register_tool(Arc::new(Probe {
            name: "save_item",
            effect: ToolEffect::Mutating,
            delay: Duration::ZERO,
            calls: calls.clone(),
        }))
        .build()
        .await
        .unwrap();
    let sid = o.create_session(&who(), "root").await.unwrap();
    let turn = o.submit(&who(), &sid, "{}".into()).await.unwrap();
    let r = wait_terminal(&o, &sid, &turn.turn_id).await;
    spike_log(format!(
        "SPIKE fanout status={:?} steps={} tokens={}/{} calls={}",
        r.status, r.steps, r.input_tokens, r.output_tokens, calls.lock().unwrap().len()
    ));
    assert_eq!(r.status, RunStatus::Completed, "{r:?}");
    assert_eq!(calls.lock().unwrap().len(), 5);
    o.shutdown().await.unwrap();
}
