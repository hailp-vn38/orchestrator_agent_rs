//! M8 hard live flow: "Incident triage" qua public `Orchestrator` + model thật (`.env.live`).
//!
//! Một flow khó, mỗi bước chỉ đúng nếu thư viện giữ đúng state:
//!   1. Root → `collector`: phụ thuộc tuần tự `open_ticket` → `fetch_shard` ×3 (ticket id không đoán được).
//!   2. `fetch_shard` lỗi `SHARD_BUSY` ở lần gọi đầu mỗi shard → model phải đọc lỗi và retry.
//!   3. Root → `auditor` (child thứ hai, tool khác): `checksum` trên các giá trị collector tìm được;
//!      đáp án chỉ test biết (giá trị shard sinh ngẫu nhiên theo ticket).
//!   4. Follow-up cùng collector session (nhớ ticket id, không gọi lại tool).
//!   5. Shutdown + dựng lại engine trên cùng store, follow-up tiếp: history còn nguyên.
//!   6. Kiểm tra DB: 2 child riêng, link parent→child, số lần gọi tool, không nhân đôi message.
//!
//! Chạy (cần mạng + `.env.live`, xem `.env.live.example`):
//!   cargo test --test m8_live_hard_flow -- --ignored --nocapture

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, AgentResult, CallerContext, HostTool, LlmBackend, LlmConfig,
    LlmProviderConfig, MessageRole, Orchestrator, OrchestratorConfig, PageRequest, ProviderAuth,
    RunStatus, SecretString, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};

const TICKET: &str = "TKT-58213";
const SHARDS: [(u32, i64); 3] = [(1, 417), (2, 903), (3, 1259)];

fn load_env_file() -> HashMap<String, String> {
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
    static FILE: std::sync::OnceLock<HashMap<String, String>> = std::sync::OnceLock::new();
    std::env::var(k)
        .ok()
        .or_else(|| FILE.get_or_init(load_env_file).get(k).cloned())
        .filter(|v| !v.trim().is_empty())
}

fn err(code: &str, message: impl Into<String>) -> ToolError {
    ToolError {
        code: code.into(),
        message: message.into(),
    }
}

fn ok(ctx: &ToolContext, content: String) -> Result<ToolOutcome, ToolError> {
    Ok(ToolOutcome {
        call_id: ctx.scope().tool_call_id.clone(),
        content,
        metadata: Value::Null,
    })
}

#[derive(Default)]
struct Counters {
    open: AtomicUsize,
    fetch_ok: AtomicUsize,
    fetch_busy: AtomicUsize,
    checksum: AtomicUsize,
}

struct OpenTicket(Arc<Counters>);
struct FetchShard(Arc<Counters>, Mutex<HashMap<u32, usize>>);
struct Checksum(Arc<Counters>);

fn desc(name: &str, description: &str, schema: Value) -> ToolDescriptor {
    ToolDescriptor {
        name: name.into(),
        description: description.into(),
        input_schema: schema,
        effect: ToolEffect::ReadOnly,
    }
}

#[async_trait::async_trait]
impl HostTool for OpenTicket {
    fn descriptor(&self) -> ToolDescriptor {
        desc(
            "open_ticket",
            "Open an incident ticket for a region and return its ticket id.",
            json!({"type":"object","properties":{"region":{"type":"string"}},"required":["region"]}),
        )
    }
    async fn execute(&self, ctx: ToolContext, _: Value) -> Result<ToolOutcome, ToolError> {
        self.0.open.fetch_add(1, Ordering::SeqCst);
        ok(&ctx, format!("Opened ticket {TICKET}."))
    }
}

#[async_trait::async_trait]
impl HostTool for FetchShard {
    fn descriptor(&self) -> ToolDescriptor {
        desc(
            "fetch_shard",
            "Fetch the numeric value of a shard (1, 2 or 3) of an open ticket. \
             May fail transiently with SHARD_BUSY: then call it again with the same arguments.",
            json!({"type":"object","properties":{
                "ticket_id":{"type":"string"},"shard":{"type":"integer"}
            },"required":["ticket_id","shard"]}),
        )
    }
    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        let shard = args["shard"].as_u64().unwrap_or(0) as u32;
        if args["ticket_id"].as_str() != Some(TICKET) {
            return Err(err(
                "TICKET_UNKNOWN",
                "Unknown ticket id; call open_ticket first and use the id it returned.",
            ));
        }
        let Some((_, value)) = SHARDS.iter().find(|(s, _)| *s == shard) else {
            return Err(err("SHARD_INVALID", "Shard must be 1, 2 or 3."));
        };
        let attempt = {
            let mut seen = self.1.lock().unwrap();
            let n = seen.entry(shard).or_insert(0);
            *n += 1;
            *n
        };
        if attempt == 1 {
            self.0.fetch_busy.fetch_add(1, Ordering::SeqCst);
            return Err(err("SHARD_BUSY", "Shard is busy, retry the same call."));
        }
        self.0.fetch_ok.fetch_add(1, Ordering::SeqCst);
        ok(&ctx, format!("shard {shard} value = {value}"))
    }
}

/// Host tính đáp án; model không thể tự đoán nếu không gọi tool.
fn expected_checksum() -> i64 {
    SHARDS.iter().map(|(_, v)| v).sum::<i64>() * 7 + 13
}

#[async_trait::async_trait]
impl HostTool for Checksum {
    fn descriptor(&self) -> ToolDescriptor {
        desc(
            "checksum",
            "Compute the audit checksum of a list of integers. Never compute it yourself.",
            json!({"type":"object","properties":{
                "values":{"type":"array","items":{"type":"integer"}}
            },"required":["values"]}),
        )
    }
    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        self.0.checksum.fetch_add(1, Ordering::SeqCst);
        let mut got: Vec<i64> = args["values"]
            .as_array()
            .ok_or_else(|| err("BAD_ARGS", "values must be an array of integers"))?
            .iter()
            .filter_map(Value::as_i64)
            .collect();
        got.sort();
        let mut want: Vec<i64> = SHARDS.iter().map(|(_, v)| *v).collect();
        want.sort();
        if got != want {
            return Err(err(
                "VALUES_MISMATCH",
                "Values do not match the ticket shards; use exactly the three shard values.",
            ));
        }
        ok(&ctx, format!("checksum = {}", expected_checksum()))
    }
}

async fn run(o: &Orchestrator, who: &CallerContext, sid: &str, text: &str) -> AgentResult {
    let turn = o.submit(who, sid, text.into()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    loop {
        let r = o.result(who, sid, &turn.turn_id).await.unwrap();
        if r.status != RunStatus::Running {
            return r;
        }
        assert!(tokio::time::Instant::now() < deadline, "live run stuck");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn child_links(path: &std::path::Path, sid: &str) -> Vec<(String, String)> {
    let opts = SqliteConnectOptions::new().filename(path.join("orchestrator.db"));
    let mut c = SqliteConnection::connect_with(&opts).await.unwrap();
    sqlx::query(
        "SELECT child_session_id, child_turn_id FROM tool_invocations \
         WHERE session_id = ?1 AND tool_name = 'subagent' AND child_session_id IS NOT NULL \
         ORDER BY rowid",
    )
    .bind(sid)
    .fetch_all(&mut c)
    .await
    .unwrap()
    .iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect()
}

fn build_config(dir: &std::path::Path, base: &str, key: &str, model: &str) -> OrchestratorConfig {
    let mut cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "live".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(base.to_owned()),
                auth: ProviderAuth::ApiKey(SecretString::new(key)),
                request_timeout: Duration::from_secs(90),
            }],
        },
        vec![
            AgentDefinition::new("coordinator", "live", model)
                .with_mode(AgentMode::Primary)
                .with_prompt_path(dir.join("root.md"))
                .with_allowed_tools(["subagent"]),
            AgentDefinition::new("collector", "live", model)
                .with_mode(AgentMode::Subagent)
                .with_description("Opens an incident ticket and fetches its three shard values")
                .with_prompt_path(dir.join("collector.md"))
                .with_allowed_tools(["open_ticket", "fetch_shard"]),
            AgentDefinition::new("auditor", "live", model)
                .with_mode(AgentMode::Subagent)
                .with_description("Computes the audit checksum of a list of integers")
                .with_prompt_path(dir.join("auditor.md"))
                .with_allowed_tools(["checksum"]),
        ],
        dir.join("store"),
    );
    cfg.runtime.snapshot_refresh = Duration::from_secs(1);
    cfg
}

async fn open(
    dir: &std::path::Path,
    c: &Arc<Counters>,
    (base, key, model): (&str, &str, &str),
) -> Orchestrator {
    Orchestrator::builder(build_config(dir, base, key, model))
        .register_tool(Arc::new(OpenTicket(c.clone())))
        .register_tool(Arc::new(FetchShard(c.clone(), Default::default())))
        .register_tool(Arc::new(Checksum(c.clone())))
        .build()
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "live provider: gọi tường minh với --ignored"]
async fn live_incident_triage_two_children_retry_restart_and_recall() {
    let (Some(key), Some(base), Some(model)) = (
        env("G0_OPENAI_API_KEY").or_else(|| env("OPENAI_API_KEY")),
        env("G0_OPENAI_BASE_URL"),
        env("G0_OPENAI_MODEL"),
    ) else {
        panic!("BLOCKED: fill .env.live (see .env.live.example)");
    };
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path();
    std::fs::write(
        p.join("root.md"),
        "You are a coordinator. You know nothing about incidents yourself: always delegate with \
         the `subagent` tool. Pass data between subagents explicitly in the task text. To \
         continue an earlier subagent conversation pass the same `session_id` its result gave \
         you. Report exact numbers and ids; never invent them.",
    )
    .unwrap();
    std::fs::write(
        p.join("collector.md"),
        "You are the collector. For a region: call open_ticket once, then call fetch_shard for \
         shards 1, 2 and 3 using the ticket id returned. If a call fails with SHARD_BUSY, retry \
         the same call. Answer with the ticket id and the three shard values. For follow-ups, \
         answer from your conversation history without calling tools again.",
    )
    .unwrap();
    std::fs::write(
        p.join("auditor.md"),
        "You are the auditor. Call checksum with the exact integer values you are given and \
         report the checksum it returns. Never compute it yourself.",
    )
    .unwrap();

    let counters = Arc::new(Counters::default());
    let cfg = (base.as_str(), key.as_str(), model.as_str());
    let o = open(p, &counters, cfg).await;
    let who = CallerContext::new("live", "m8");
    let sid = o.create_session(&who, "coordinator").await.unwrap();

    // 1+2. Chuỗi phụ thuộc + retry sau lỗi tool tạm thời.
    let r = run(
        &o,
        &who,
        &sid,
        "Delegate to the subagent 'collector' for region 'eu-west': it must open a ticket and \
         fetch shards 1, 2 and 3. Tell me the ticket id and the three shard values.",
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "turn1: {:?}", r.error);
    let out1 = r.output.expect("final");
    for (_, v) in SHARDS {
        assert!(out1.contains(&v.to_string()), "missing shard {v}: {out1}");
    }
    assert!(out1.contains(TICKET), "missing ticket: {out1}");
    assert!(
        counters.fetch_busy.load(Ordering::SeqCst) >= 3,
        "busy path not hit"
    );
    assert!(
        counters.fetch_ok.load(Ordering::SeqCst) >= 3,
        "retries did not succeed"
    );

    // 3. Child thứ hai, dữ liệu truyền qua Root.
    let r = run(
        &o,
        &who,
        &sid,
        "Now delegate to the subagent 'auditor' (a NEW subagent, not the collector): give it the \
         three shard values you just reported and ask for the audit checksum. Tell me the checksum.",
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "turn2: {:?}", r.error);
    let out2 = r.output.expect("final");
    assert!(
        out2.contains(&expected_checksum().to_string()),
        "checksum wrong/hallucinated: {out2}"
    );
    assert!(counters.checksum.load(Ordering::SeqCst) >= 1);

    let links = child_links(&p.join("store"), &sid).await;
    let collector = links.first().expect("collector link").0.clone();
    let auditor = links.last().expect("auditor link").0.clone();
    assert_ne!(collector, auditor, "two distinct child sessions expected");

    // 4. Follow-up cùng collector session: nhớ ticket, không gọi lại tool.
    let opens = counters.open.load(Ordering::SeqCst);
    let fetches =
        counters.fetch_ok.load(Ordering::SeqCst) + counters.fetch_busy.load(Ordering::SeqCst);
    let r = run(
        &o,
        &who,
        &sid,
        &format!(
            "Ask the collector again, continuing its earlier conversation (session_id \
             {collector}): which ticket id did it open? Do not make it call tools."
        ),
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "turn3: {:?}", r.error);
    assert!(r.output.expect("final").contains(TICKET));
    assert_eq!(
        counters.open.load(Ordering::SeqCst),
        opens,
        "collector re-ran a tool"
    );
    assert_eq!(
        counters.fetch_ok.load(Ordering::SeqCst) + counters.fetch_busy.load(Ordering::SeqCst),
        fetches
    );

    // 5. Restart engine trên cùng store; history/link phải còn nguyên.
    o.shutdown().await.unwrap();
    drop(o);
    let o = open(p, &counters, cfg).await;
    let r = run(
        &o,
        &who,
        &sid,
        &format!(
            "Ask the collector again (session_id {collector}) for the sum of its three shard \
             values, and report it. Do not make it call tools."
        ),
    )
    .await;
    assert_eq!(r.status, RunStatus::Completed, "turn4: {:?}", r.error);
    let sum: i64 = SHARDS.iter().map(|(_, v)| v).sum();
    assert!(r.output.expect("final").contains(&sum.to_string()));
    assert_eq!(links_after(&o, &who, p, &sid).await, collector);

    // 6. Kiểm tra canonical history: child còn tool results, root không nhân đôi message.
    let hist = o
        .history(&who, &collector, PageRequest::first(200))
        .await
        .unwrap()
        .items;
    let tool_msgs = hist.iter().filter(|m| m.role == MessageRole::Tool).count();
    assert!(
        tool_msgs >= 7,
        "open + 3 busy + 3 ok expected, got {tool_msgs}"
    );
    let root_hist = o
        .history(&who, &sid, PageRequest::first(200))
        .await
        .unwrap()
        .items;
    let users = root_hist
        .iter()
        .filter(|m| m.role == MessageRole::User)
        .count();
    assert_eq!(
        users, 4,
        "one user input per turn, none duplicated after restart"
    );
    println!(
        "OK: open={} busy={} ok={} checksum_calls={} child_tool_msgs={tool_msgs}",
        counters.open.load(Ordering::SeqCst),
        counters.fetch_busy.load(Ordering::SeqCst),
        counters.fetch_ok.load(Ordering::SeqCst),
        counters.checksum.load(Ordering::SeqCst),
    );
    o.shutdown().await.unwrap();
}

/// Sau restart, link root→collector vẫn trỏ đúng child đầu tiên (không tạo child mới).
async fn links_after(
    _o: &Orchestrator,
    _w: &CallerContext,
    p: &std::path::Path,
    sid: &str,
) -> String {
    let links = child_links(&p.join("store"), sid).await;
    let distinct: std::collections::BTreeSet<_> = links.iter().map(|l| l.0.clone()).collect();
    assert_eq!(
        distinct.len(),
        2,
        "restart must not spawn extra children: {distinct:?}"
    );
    links[0].0.clone()
}
