//! Live Root → Child → HostTool → follow-up, có log JSONL và SQLite được giữ lại.
//! Chạy: cargo run --example live_flow

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, CallerContext, FamilySnapshot, FeedItem, HostTool,
    InvocationStatus, LlmBackend, LlmConfig, LlmProviderConfig, Orchestrator, OrchestratorConfig,
    ProviderAuth, RunStatus, SecretString, Subscription, ToolContext, ToolDescriptor, ToolEffect,
    ToolError, ToolOutcome,
};
use serde_json::{json, Value};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const CODE: &str = "ORCH-DEMO-7731";

struct Log {
    file: std::fs::File,
    start: Instant,
    secret: String,
}

impl Log {
    fn write(&mut self, kind: &str, data: Value) -> io::Result<()> {
        let line =
            json!({"elapsed_ms": self.start.elapsed().as_millis(), "kind": kind, "data": data})
                .to_string()
                .replace(&self.secret, "[redacted]");
        writeln!(self.file, "{line}")?;
        self.file.flush()?;
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "{line}")?;
        stdout.flush()
    }

    fn snapshot(&mut self, snapshot: &FamilySnapshot) -> io::Result<()> {
        let sessions: Vec<_> = snapshot
            .sessions()
            .map(|s| {
                json!({
                    "session_id": s.session_id, "parent_session_id": s.parent_session_id,
                    "agent_id": s.agent_id, "revision": s.state_revision,
                    "run": s.latest_run.as_ref().map(|r| json!({
                        "run_id": r.run_id, "turn_id": r.turn_id,
                        "status": r.status.as_str(), "steps": r.steps, "attempt": r.attempt,
                    })),
                    "tools": s.invocations.iter().map(|i| json!({
                        "name": i.tool_name, "status": i.status.as_str(),
                        "child_session_id": i.child_session_id,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect();
        self.write(
            "snapshot",
            json!({"active": snapshot.has_active_work(), "sessions": sessions}),
        )
    }
}

struct Lookup;

#[async_trait::async_trait]
impl HostTool for Lookup {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "lookup_code".into(),
            description: "Look up the demo project code in the host's local fixture.".into(),
            input_schema: json!({"type": "object", "properties": {}, "additionalProperties": false}),
            effect: ToolEffect::ReadOnly,
        }
    }

    async fn execute(
        &self,
        ctx: ToolContext,
        _: Value,
    ) -> std::result::Result<ToolOutcome, ToolError> {
        for step in 1..=3 {
            ctx.progress(&format!("Đang tra cứu dữ liệu demo: {step}/3"));
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        Ok(ToolOutcome {
            call_id: ctx.scope().tool_call_id.clone(),
            content: format!("Project code: {CODE}"),
            metadata: Value::Null,
        })
    }
}

// Cùng định dạng đơn giản với các live test; file được đọc như dữ liệu, không source vào shell.
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
                .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
                .unwrap_or(value);
            Some((key.trim().into(), value.into()))
        })
        .collect())
}

async fn run_turn(
    o: &Orchestrator,
    caller: &CallerContext,
    sid: &str,
    sub: &mut Subscription,
    log: &mut Log,
    prompt: &str,
) -> Result<()> {
    log.write("submit", json!({"session_id": sid, "prompt": prompt}))?;
    let turn = o.submit(caller, sid, prompt.into()).await?;
    log.write("accepted", json!({"turn_id": turn.turn_id}))?;
    let deadline = tokio::time::sleep(Duration::from_secs(240));
    tokio::pin!(deadline);
    let mut refresh = tokio::time::interval(o.snapshot_refresh_interval());
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            item = sub.recv() => match item {
                Some(FeedItem::Event(event)) => {
                    log.write("event", event.to_json())?;
                    // Committed events invalidate state; lấy lại từ SQLite, không merge payload.
                    if event.is_committed() {
                        log.snapshot(&o.snapshot(caller, sid).await?)?;
                    }
                }
                Some(FeedItem::ResyncRequired) | None => {
                    log.write("resync", json!({"reason": "feed lagged or closed"}))?;
                    *sub = o.subscribe(caller, sid).await?;
                    log.snapshot(&o.snapshot(caller, sid).await?)?;
                }
            },
            _ = refresh.tick() => {
                let snapshot = o.snapshot(caller, sid).await?;
                log.snapshot(&snapshot)?;
                let result = o.result(caller, sid, &turn.turn_id).await?;
                if result.status != RunStatus::Running {
                    log.write("result", json!({
                        "turn_id": turn.turn_id, "status": result.status.as_str(),
                        "output": result.output, "error_code": result.error.as_ref().map(|e| &e.code),
                    }))?;
                    if result.status != RunStatus::Completed {
                        return Err(io::Error::other("Turn không completed; xem result trong log").into());
                    }
                    if !result.output.as_deref().is_some_and(|s| s.contains(CODE)) {
                        return Err(io::Error::other("Câu trả lời thiếu mã từ HostTool").into());
                    }
                    return Ok(());
                }
            },
            _ = &mut deadline => {
                o.cancel(caller, sid).await?;
                return Err(io::Error::new(io::ErrorKind::TimedOut, "Turn quá 240 giây; đã yêu cầu cancel").into());
            }
        }
    }
}

async fn flow(o: &Orchestrator, log: &mut Log) -> Result<()> {
    let caller = CallerContext::new("demo", "live-flow");
    let sid = o.create_session(&caller, "coordinator").await?;
    let mut sub = o.subscribe(&caller, &sid).await?;
    log.snapshot(&o.snapshot(&caller, &sid).await?)?;
    run_turn(o, &caller, &sid, &mut sub, log,
        "Delegate to analyst using the subagent tool. Ask it to call lookup_code and report the project code. Tell me its answer in Vietnamese.").await?;
    let first = o.snapshot(&caller, &sid).await?;
    let child = first
        .children
        .first()
        .ok_or_else(|| io::Error::other("Không tạo child session"))?;
    let child_id = child.session_id.clone();
    if !child
        .invocations
        .iter()
        .any(|i| i.tool_name == "lookup_code" && i.status == InvocationStatus::Completed)
    {
        return Err(io::Error::other("Child chưa thực thi lookup_code thành công").into());
    }
    let first_turn = child.latest_run.as_ref().map(|r| r.turn_id.clone());
    run_turn(o, &caller, &sid, &mut sub, log,
        "Ask the SAME analyst again using subagent with the earlier session_id: what project code did you look up earlier? Have it answer from its history. Tell me its answer in Vietnamese.").await?;
    let final_snapshot = o.snapshot(&caller, &sid).await?;
    let reused = final_snapshot
        .session(&child_id)
        .and_then(|s| s.latest_run.as_ref());
    if final_snapshot.has_active_work()
        || final_snapshot.children.len() != 1
        || reused.is_none_or(|r| {
            Some(&r.turn_id) == first_turn.as_ref() || r.status != RunStatus::Completed
        })
    {
        return Err(
            io::Error::other("Follow-up chưa hội tụ về cùng child với turn mới completed").into(),
        );
    }
    log.write(
        "verified",
        json!({"same_child_session": child_id, "two_turns_completed": true}),
    )?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut args = std::env::args().skip(1);
    let mut env_file = std::env::var_os("LIVE_ENV_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".env.live"));
    let mut output = root.join("target/live-flow");
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--env-file" | "--output-dir" => {
                let value = args
                    .next()
                    .ok_or_else(|| io::Error::other("Thiếu giá trị option"))?;
                if arg == "--env-file" {
                    env_file = value.into();
                } else {
                    output = value.into();
                }
            }
            "--help" | "-h" => {
                println!("cargo run --example live_flow -- [--env-file .env.live] [--output-dir target/live-flow]\nCấu hình chỉ lấy từ file; mỗi lần chạy tạo thư mục UUID chứa events.jsonl và store/.");
                return Ok(());
            }
            _ => return Err(io::Error::other("Option không hợp lệ; dùng --help").into()),
        }
    }
    let config = read_config(&env_file)?;
    let required = |key: &str| -> Result<String> {
        config
            .get(key)
            .filter(|s| !s.trim().is_empty())
            .cloned()
            .ok_or_else(|| io::Error::other(format!("Thiếu {key} trong file cấu hình")).into())
    };
    let endpoint = required("G0_OPENAI_BASE_URL")?;
    let model = required("G0_OPENAI_MODEL")?;
    let key = required("G0_OPENAI_API_KEY")?;
    let output = output.join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&output)?;
    let mut log = Log {
        file: std::fs::File::create(output.join("events.jsonl"))?,
        start: Instant::now(),
        secret: key.clone(),
    };
    log.write("startup", json!({"output_dir": output, "model": model, "config_source": "file", "tool_data": "local demo fixture"}))?;
    let root_prompt = output.join("root.md");
    let child_prompt = output.join("analyst.md");
    std::fs::write(&root_prompt, "You coordinate work. Always delegate project questions to analyst using subagent. For follow-ups reuse the session_id returned by the prior subagent result. Report the exact code found by the analyst. Keep answers short.")?;
    std::fs::write(&child_prompt, "You are analyst. For the first project question call lookup_code. For follow-ups recall the code from history. Answer briefly with the exact project code.")?;
    let mut cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "live".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(endpoint),
                auth: ProviderAuth::ApiKey(SecretString::new(key)),
                request_timeout: Duration::from_secs(60),
            }],
        },
        vec![
            AgentDefinition::new("coordinator", "live", &model)
                .with_mode(AgentMode::Primary)
                .with_prompt_path(root_prompt)
                .with_allowed_tools(["subagent"]),
            AgentDefinition::new("analyst", "live", &model)
                .with_mode(AgentMode::Subagent)
                .with_description("Looks up project codes and remembers them for follow-ups")
                .with_prompt_path(child_prompt)
                .with_allowed_tools(["lookup_code"]),
        ],
        output.join("store"),
    );
    cfg.runtime.snapshot_refresh = Duration::from_secs(1);
    let o = Orchestrator::builder(cfg)
        .register_tool(Arc::new(Lookup))
        .build()
        .await?;
    let result = flow(&o, &mut log).await;
    // Shutdown được gọi cả khi flow lỗi, tránh bỏ managed tasks đang chạy.
    let shutdown = o.shutdown().await;
    if let Err(error) = &result {
        log.write("failed", json!({"message": error.to_string()}))?;
    }
    log.write("shutdown", json!({"success": shutdown.is_ok()}))?;
    shutdown?;
    result
}
