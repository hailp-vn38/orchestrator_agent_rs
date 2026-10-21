//! Flow hoàn chỉnh: Cho URL (hoặc prompt chứa URL) → Root (`coordinator`) uỷ quyền cho
//! Subagent (`explorer`) → fetch HTML & bóc tách nội dung bằng bộ tool HTML → tóm tắt.
//!
//! Cấu trúc module:
//! - `main.rs`: điểm vào CLI, khởi tạo `Orchestrator`, chạy flow và xác minh kết quả.
//! - `tools.rs`: `HtmlWorkspace` (tải/cache HTML qua `curl`) và 5 `HostTool` lấy nội dung HTML.
//! - `html_parser.rs`: bộ parser/extractor HTML thuần cùng unit test.
//! - `host.rs`: đọc cấu hình `.env.live`, logger JSONL/terminal và prompt cho `coordinator`/`explorer`.
//!
//! Chạy:
//!   cargo run --example url_summary
//!   cargo run --example url_summary -- 'hãy tóm tắt "https://www.24h.com.vn/tin-tuc-trong-ngay/don-khong-khi-lanh-mien-bac-giam-may-do-c46a1804579.html"'
//!   cargo test --example url_summary

mod host;
mod html_parser;
mod tools;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, CallerContext, FeedItem, InvocationStatus, LlmBackend, LlmConfig,
    LlmProviderConfig, Orchestrator, OrchestratorConfig, ProviderAuth, RunStatus, SecretString,
};
use serde_json::json;

use crate::host::{read_config, Log, Result, COORDINATOR_PROMPT, EXPLORER_PROMPT};
use crate::tools::{HtmlTool, HtmlToolKind, HtmlWorkspace, HTML_TOOL_NAMES};

const DEFAULT_PROMPT: &str =
    "hãy tóm tắt \"https://www.24h.com.vn/tin-tuc-trong-ngay/don-khong-khi-lanh-mien-bac-giam-may-do-c46a1804579.html\"";

async fn run_flow(
    o: &Orchestrator,
    log: &mut Log,
    prompt: &str,
    summary_path: &Path,
) -> Result<()> {
    let caller = CallerContext::new("demo", "url-summary");
    let sid = o.create_session(&caller, "coordinator").await?;
    let mut sub = o.subscribe(&caller, &sid).await?;
    log.write("submit", json!({"session_id": sid, "prompt": prompt}))?;
    let turn = o.submit(&caller, &sid, prompt.to_string()).await?;
    let deadline = tokio::time::sleep(Duration::from_secs(300));
    tokio::pin!(deadline);
    let mut refresh = tokio::time::interval(o.snapshot_refresh_interval());
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let final_output = loop {
        tokio::select! {
            item = sub.recv() => match item {
                Some(FeedItem::Event(event)) => log.write("event", event.to_json())?,
                Some(FeedItem::ResyncRequired) | None => {
                    log.write("resync", json!({"reason": "feed lagged or closed"}))?;
                    sub = o.subscribe(&caller, &sid).await?;
                }
            },
            _ = refresh.tick() => {
                let result = o.result(&caller, &sid, &turn.turn_id).await?;
                if result.status == RunStatus::Running {
                    continue;
                }
                log.write("result", json!({
                    "status": result.status.as_str(),
                    "output": result.output,
                    "error_code": result.error.as_ref().map(|e| &e.code),
                }))?;
                if result.status != RunStatus::Completed {
                    return Err(io::Error::other("Turn không completed; xem result trong log").into());
                }
                break result.output.unwrap_or_default();
            },
            _ = &mut deadline => {
                o.cancel(&caller, &sid).await?;
                return Err(io::Error::new(io::ErrorKind::TimedOut, "Turn quá 300 giây; đã cancel").into());
            }
        }
    };

    // Xác minh Root đã uỷ quyền cho child session `explorer` và child đã chạy tool HTML thành công.
    let snapshot = o.snapshot(&caller, &sid).await?;
    let child = snapshot
        .children
        .first()
        .ok_or_else(|| io::Error::other("Root chưa gọi subagent explorer"))?;
    let html_tool_used = child.invocations.iter().any(|i| {
        HTML_TOOL_NAMES.contains(&i.tool_name.as_str()) && i.status == InvocationStatus::Completed
    });
    if !html_tool_used {
        return Err(io::Error::other(
            "Subagent explorer chưa gọi tool lấy nội dung HTML nào thành công",
        )
        .into());
    }
    if final_output.trim().is_empty() {
        return Err(io::Error::other("Bản tóm tắt rỗng").into());
    }

    std::fs::write(summary_path, &final_output)?;
    log.write(
        "verified",
        json!({
            "child_session_id": child.session_id,
            "summary_file": summary_path,
            "bytes": final_output.len()
        }),
    )?;
    println!("\n===== BẢN TÓM TẮT =====\n{final_output}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut input_arg: Option<String> = None;
    let mut pretty = true;
    let mut env_file = std::env::var_os("LIVE_ENV_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".env.live"));
    let mut output = root.join("target/url-summary");

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--prompt" | "--env-file" | "--output-dir" => {
                let v = args
                    .next()
                    .ok_or_else(|| io::Error::other("Thiếu giá trị option"))?;
                match arg.as_str() {
                    "--prompt" => input_arg = Some(v),
                    "--env-file" => env_file = v.into(),
                    _ => output = v.into(),
                }
            }
            "--json" => pretty = false,
            "--help" | "-h" => {
                println!(
                    "cargo run --example url_summary -- [PROMPT_HOẶC_URL] [--env-file .env.live] [--output-dir target/url-summary] [--json]\n\
                     Ví dụ:\n  \
                     cargo run --example url_summary -- 'hãy tóm tắt \"https://www.24h.com.vn/tin-tuc-trong-ngay/don-khong-khi-lanh-mien-bac-giam-may-do-c46a1804579.html\"'"
                );
                return Ok(());
            }
            s if !s.starts_with('-') && input_arg.is_none() => input_arg = Some(s.to_string()),
            _ => return Err(io::Error::other("Option không hợp lệ; dùng --help").into()),
        }
    }

    let prompt = match input_arg {
        None => DEFAULT_PROMPT.to_string(),
        Some(s) if s.starts_with("http://") || s.starts_with("https://") => {
            format!("hãy tóm tắt \"{s}\"")
        }
        Some(s) => s,
    };

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

    let run_dir = output.join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&run_dir)?;
    let mut log = Log::new(
        std::fs::File::create(run_dir.join("events.jsonl"))?,
        key.clone(),
        pretty,
    );
    log.write(
        "startup",
        json!({"run_dir": run_dir, "model": model, "prompt": prompt}),
    )?;

    let coordinator_prompt = run_dir.join("coordinator.md");
    let explorer_prompt = run_dir.join("explorer.md");
    std::fs::write(&coordinator_prompt, COORDINATOR_PROMPT)?;
    std::fs::write(&explorer_prompt, EXPLORER_PROMPT)?;

    let cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "live".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(endpoint),
                auth: ProviderAuth::ApiKey(SecretString::new(key)),
                request_timeout: Duration::from_secs(120),
            }],
        },
        vec![
            AgentDefinition::new("coordinator", "live", &model)
                .with_mode(AgentMode::Primary)
                .with_prompt_path(coordinator_prompt)
                .with_allowed_tools(["subagent"]),
            AgentDefinition::new("explorer", "live", &model)
                .with_mode(AgentMode::Subagent)
                .with_description(
                    "Fetches a web URL and extracts HTML metadata, article content, links and elements",
                )
                .with_prompt_path(explorer_prompt)
                .with_allowed_tools(HTML_TOOL_NAMES),
        ],
        run_dir.join("store"),
    );

    let ws = Arc::new(HtmlWorkspace::new(run_dir.join("raw_html")));
    let o = Orchestrator::builder(cfg)
        .register_tool(HtmlTool::new(ws.clone(), HtmlToolKind::FetchHtml))
        .register_tool(HtmlTool::new(ws.clone(), HtmlToolKind::ExtractMetadata))
        .register_tool(HtmlTool::new(ws.clone(), HtmlToolKind::ExtractArticle))
        .register_tool(HtmlTool::new(ws.clone(), HtmlToolKind::ExtractFeed))
        .register_tool(HtmlTool::new(ws.clone(), HtmlToolKind::ExtractLinks))
        .register_tool(HtmlTool::new(ws, HtmlToolKind::SelectElements))
        .build()
        .await?;

    let summary_path = run_dir.join("summary.md");
    let result = run_flow(&o, &mut log, &prompt, &summary_path).await;
    let shutdown = o.shutdown().await;
    if let Err(error) = &result {
        log.write("failed", json!({"message": error.to_string()}))?;
    }
    log.write("shutdown", json!({"success": shutdown.is_ok()}))?;
    shutdown?;
    result
}
