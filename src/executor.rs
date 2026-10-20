//! `ToolExecutor` (docs/05): validate schema + effective permissions **trước** dispatch,
//! chạy HostTool/`skill` có timeout và cancel hợp tác, trả outcome cho `AgentRuntime` ghi
//! transcript. Executor không bao giờ mở transaction hay ghi history (một writer duy nhất).
//!
//! Output của tool/skill là untrusted context: runtime không suy luận gì từ văn bản đó.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::agent::AgentDefinition;
use crate::feed::{ExecutionFeed, ProgressSink};
use crate::gateway::ModelTool;
use crate::skill::{render, SkillManager};
use crate::subagent::{self, Delegation, SubagentInput, TaskManager, SUBAGENT_TOOL};
use crate::tool::{
    HostTool, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome, ToolScope,
    ToolServices, RESERVED_TOOL_NAMES,
};
use crate::types::SessionPolicy;

/// Tên built-in tool nạp skill theo nhu cầu.
pub(crate) const SKILL_TOOL: &str = "skill";
/// Tên built-in tool đọc đoạn văn bản UTF-8 có giới hạn của committed artifact.
pub(crate) const ARTIFACT_READ_TOOL: &str = "artifact_read";
/// Mã lỗi kỹ thuật ổn định gửi lại cho model.
pub(crate) const CODE_NOT_ALLOWED: &str = "TOOL_NOT_ALLOWED";
pub(crate) const CODE_UNAVAILABLE: &str = "TOOL_UNAVAILABLE";
pub(crate) const CODE_INVALID_ARGS: &str = "INVALID_ARGUMENTS";
/// Child (depth 1) không được ủy quyền tiếp.
pub(crate) const CODE_DEPTH_LIMIT: &str = "DEPTH_LIMIT";
pub(crate) const CODE_TIMEOUT: &str = "TOOL_TIMEOUT";
pub(crate) const CODE_PANICKED: &str = "TOOL_PANICKED";
pub(crate) const CODE_TOO_LARGE: &str = "TOOL_OUTPUT_TOO_LARGE";
/// Outcome không xác định (timeout/panic của tool Mutating): không được tiếp tục hay replay.
pub(crate) const CODE_OUTCOME_UNKNOWN: &str = "TOOL_OUTCOME_UNKNOWN";

const MAX_ERROR_CHARS: usize = 1000;
const MAX_METADATA_BYTES: usize = 16 * 1024;
const MAX_SCHEMA_DEPTH: usize = 16;

/// Quyền hiệu lực của một session: `AgentDefinition` ∩ `SessionPolicy` (chỉ thu hẹp).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Effective {
    pub tools: BTreeSet<String>,
    pub skills: BTreeSet<String>,
    /// Session là child (depth 1): `subagent` không được quảng bá và bị chặn.
    pub child: bool,
}

impl Effective {
    pub(crate) fn resolve(agent: &AgentDefinition, policy: &SessionPolicy) -> Self {
        fn narrow(base: &[String], limit: &Option<Vec<String>>) -> BTreeSet<String> {
            base.iter()
                .filter(|x| limit.as_ref().is_none_or(|l| l.contains(x)))
                .cloned()
                .collect()
        }
        Self {
            tools: narrow(&agent.allowed_tools, &policy.allowed_tools),
            skills: narrow(&agent.allowed_skills, &policy.allowed_skills),
            child: false,
        }
    }

    pub(crate) fn for_session(mut self, is_child: bool) -> Self {
        self.child = is_child;
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ToolLimits {
    pub timeout: Duration,
    pub model_cap_bytes: usize,
    pub hard_cap_bytes: usize,
    /// Khoảng tối thiểu giữa hai `tool.progress` của một invocation.
    pub progress_interval: Duration,
}

/// Kết quả đã chốt của một invocation, sẵn sàng để runtime commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Settled {
    pub failed: bool,
    /// Văn bản model-visible (đã áp cap) ghi vào tool-result message.
    pub model_text: String,
    /// Kết quả bounded ghi vào `tool_invocations.result_json`.
    pub result_json: String,
    pub error_code: Option<String>,
}

impl Settled {
    pub(crate) fn failure(code: &str, message: &str) -> Self {
        let message = sanitize_message(message);
        Self {
            failed: true,
            model_text: format!("error {code}: {message}"),
            result_json: json!({ "error": { "code": code, "message": message } }).to_string(),
            error_code: Some(code.to_owned()),
        }
    }
}

pub(crate) enum Runnable {
    Skill {
        id: String,
    },
    ArtifactRead {
        artifact_id: String,
        offset_bytes: u64,
        max_bytes: Option<u64>,
    },
    Host {
        tool: Arc<dyn HostTool>,
        effect: ToolEffect,
        args: Value,
    },
    Subagent {
        input: SubagentInput,
    },
}

pub(crate) enum Prepared {
    /// Bị chặn trước dispatch: không có side effect, chỉ cần ghi kết quả lỗi đúng pairing.
    Rejected(Settled),
    Run(Runnable),
}

pub(crate) enum Dispatch {
    Settled(Settled),
    /// Không biết tool đã tạo side effect hay chưa; runtime phải dừng run, không replay.
    Unknown(&'static str),
    /// Run bị hủy hợp tác (shutdown) khi đang ủy quyền: không có result để ghi, run
    /// `interrupted`. Child turn đã admit vẫn được giữ và đọc được qua invocation link.
    Cancelled,
}

pub(crate) struct ToolExecutor {
    tools: BTreeMap<String, Arc<dyn HostTool>>,
    descriptors: BTreeMap<String, ToolDescriptor>,
    skills: Arc<SkillManager>,
    tasks: TaskManager,
    limits: ToolLimits,
    feed: ExecutionFeed,
    services: ToolServices,
}

impl ToolExecutor {
    pub(crate) fn new(
        tools: BTreeMap<String, Arc<dyn HostTool>>,
        skills: Arc<SkillManager>,
        tasks: TaskManager,
        limits: ToolLimits,
        feed: ExecutionFeed,
        services: ToolServices,
    ) -> Self {
        let descriptors = tools
            .iter()
            .map(|(name, t)| (name.clone(), t.descriptor()))
            .collect();
        Self {
            tools,
            descriptors,
            skills,
            tasks,
            limits,
            feed,
            services,
        }
    }

    pub(crate) fn skills(&self) -> &SkillManager {
        &self.skills
    }

    /// Chỉ tool được phép: tool bị từ chối không xuất hiện trong request tới model.
    /// `skill` chỉ được cung cấp khi session thực sự có skill để nạp.
    pub(crate) fn definitions(&self, eff: &Effective) -> Vec<ModelTool> {
        let mut out: Vec<ModelTool> = self
            .descriptors
            .values()
            .filter(|d| eff.tools.contains(&d.name))
            .map(|d| ModelTool {
                name: d.name.clone(),
                description: d.description.clone(),
                input_schema: d.input_schema.clone(),
            })
            .collect();
        if eff.tools.contains(SKILL_TOOL) && !self.skills.available(&eff.skills).is_empty() {
            out.push(skill_tool());
        }
        if eff.tools.contains(ARTIFACT_READ_TOOL) {
            out.push(artifact_read_tool());
        }
        if self.subagent_offered(eff) {
            out.push(self.tasks.tool_definition());
        }
        out
    }

    /// `subagent` chỉ được quảng bá khi session được phép, không phải child và có agent
    /// làm child được.
    fn subagent_offered(&self, eff: &Effective) -> bool {
        eff.tools.contains(SUBAGENT_TOOL) && !eff.child && self.tasks.has_candidates()
    }

    /// Resolve + kiểm quyền + validate schema. Chưa có side effect nào.
    pub(crate) fn prepare(&self, name: &str, args: Value, eff: &Effective) -> Prepared {
        let reject = |code: &str, msg: String| Prepared::Rejected(Settled::failure(code, &msg));
        let shown = safe_name(name);
        if !eff.tools.contains(name) {
            return reject(
                CODE_NOT_ALLOWED,
                format!("tool '{shown}' is not available to this agent"),
            );
        }
        if name == SKILL_TOOL {
            return match validate_args(&skill_tool().input_schema, &args) {
                Ok(()) => Prepared::Run(Runnable::Skill {
                    id: args["id"].as_str().unwrap_or_default().to_owned(),
                }),
                Err(e) => reject(CODE_INVALID_ARGS, format!("invalid arguments: {e}")),
            };
        }
        if name == ARTIFACT_READ_TOOL {
            return match validate_args(&artifact_read_tool().input_schema, &args) {
                Ok(()) => {
                    let Some(offset_bytes) =
                        args.get("offset_bytes").map_or(Some(0), |v| v.as_u64())
                    else {
                        return reject(
                            CODE_INVALID_ARGS,
                            "invalid arguments: offset_bytes must be a non-negative integer".into(),
                        );
                    };
                    let max_bytes = match args.get("max_bytes") {
                        None => None,
                        Some(v) => match v.as_u64() {
                            Some(n) if n > 0 => Some(n),
                            _ => {
                                return reject(
                                    CODE_INVALID_ARGS,
                                    "invalid arguments: max_bytes must be a positive integer"
                                        .into(),
                                )
                            }
                        },
                    };
                    Prepared::Run(Runnable::ArtifactRead {
                        artifact_id: args["artifact_id"].as_str().unwrap_or_default().to_owned(),
                        offset_bytes,
                        max_bytes,
                    })
                }
                Err(e) => reject(CODE_INVALID_ARGS, format!("invalid arguments: {e}")),
            };
        }
        if name == SUBAGENT_TOOL {
            if eff.child {
                return reject(CODE_DEPTH_LIMIT, "subagents cannot delegate further".into());
            }
            if !self.tasks.has_candidates() {
                return reject(CODE_UNAVAILABLE, format!("tool '{shown}' is not available"));
            }
            return match validate_args(&subagent::input_schema(), &args) {
                Ok(()) => Prepared::Run(Runnable::Subagent {
                    input: SubagentInput::from_args(&args),
                }),
                Err(e) => reject(CODE_INVALID_ARGS, format!("invalid arguments: {e}")),
            };
        }
        let (Some(tool), Some(desc)) = (self.tools.get(name), self.descriptors.get(name)) else {
            // Tên built-in dành riêng nhưng chưa có handler.
            let code = if RESERVED_TOOL_NAMES.contains(&name) {
                CODE_UNAVAILABLE
            } else {
                CODE_NOT_ALLOWED
            };
            return reject(code, format!("tool '{shown}' is not available"));
        };
        match validate_args(&desc.input_schema, &args) {
            Ok(()) => Prepared::Run(Runnable::Host {
                tool: Arc::clone(tool),
                effect: desc.effect,
                args,
            }),
            Err(e) => reject(CODE_INVALID_ARGS, format!("invalid arguments: {e}")),
        }
    }

    /// Chạy một invocation đã `prepare`. Cancel là hợp tác: tool nhận cờ `is_cancelled` và tự
    /// dừng; executor không bỏ tool đang chạy (shutdown chờ trong grace). Timeout là per-op.
    pub(crate) async fn run(
        &self,
        runnable: Runnable,
        scope: ToolScope,
        eff: &Effective,
        cancel: &CancellationToken,
    ) -> Dispatch {
        match runnable {
            Runnable::Skill { id } => Dispatch::Settled(match self.skills.load(&eff.skills, &id) {
                Ok(loaded) => {
                    let content = render(&loaded);
                    // Skill không bị cắt theo tool cap: `skill_max_bytes` đã chặn lỗi rõ ràng.
                    Settled {
                        failed: false,
                        result_json: json!({
                            "content": content,
                            "metadata": { "skill_id": loaded.id },
                        })
                        .to_string(),
                        model_text: content,
                        error_code: None,
                    }
                }
                Err(e) => Settled::failure(e.code(), &e.message()),
            }),
            Runnable::ArtifactRead {
                artifact_id,
                offset_bytes,
                max_bytes,
            } => {
                let cap = max_bytes
                    .and_then(|n| usize::try_from(n).ok())
                    .map_or(self.limits.model_cap_bytes, |n| {
                        n.clamp(1, self.limits.model_cap_bytes)
                    });
                let result = match self
                    .services
                    .artifacts
                    .read_model_excerpt(&scope, &artifact_id, offset_bytes, cap)
                    .await
                {
                    Ok((art, excerpt, has_more)) => Ok(ToolOutcome {
                        call_id: scope.tool_call_id,
                        content: excerpt,
                        metadata: json!({
                            "artifact_id": art.id,
                            "name": art.name,
                            "mime_type": art.mime_type,
                            "size_bytes": art.size_bytes,
                            "offset_bytes": offset_bytes,
                            "truncated": has_more,
                        }),
                    }),
                    Err(e) => Err(e),
                };
                Dispatch::Settled(self.settle(result))
            }
            Runnable::Host { tool, effect, args } => {
                self.run_host(tool, effect, args, scope, cancel).await
            }
            // Không áp `tool_timeout` lên cả child (không deadline tổng); timeout theo từng
            // request/tool diễn ra bên trong run của child.
            Runnable::Subagent { input } => {
                match self.tasks.delegate(&scope, input, cancel).await {
                    Delegation::Done(result) => Dispatch::Settled(self.settle(result)),
                    Delegation::Cancelled => Dispatch::Cancelled,
                }
            }
        }
    }

    async fn run_host(
        &self,
        tool: Arc<dyn HostTool>,
        effect: ToolEffect,
        args: Value,
        scope: ToolScope,
        cancel: &CancellationToken,
    ) -> Dispatch {
        let cancelled = Arc::new(AtomicBool::new(cancel.is_cancelled()));
        let progress = ProgressSink::new(&self.feed, &scope, self.limits.progress_interval);
        let context = ToolContext::new(
            scope.clone(),
            Arc::clone(&cancelled),
            Some(progress),
            Some(self.services.clone()),
        );
        let guarded = AssertUnwindSafe(tool.execute(context, args)).catch_unwind();
        let watcher = async {
            cancel.cancelled().await;
            cancelled.store(true, Ordering::Release);
            std::future::pending::<()>().await;
        };
        let timed = tokio::time::timeout(self.limits.timeout, async {
            tokio::select! {
                r = guarded => r,
                () = watcher => unreachable!("the cancel watcher never completes"),
            }
        });
        match timed.await {
            Ok(Ok(Ok(outcome))) => {
                if let Err(err) = self.verify_outcome_artifacts(&scope, &outcome).await {
                    return Dispatch::Settled(self.settle(Err(err)));
                }
                Dispatch::Settled(self.settle(Ok(outcome)))
            }
            Ok(Ok(Err(err))) => Dispatch::Settled(self.settle(Err(err))),
            Err(_) => self.abnormal(effect, CODE_TIMEOUT, "the tool exceeded its time limit"),
            Ok(Err(_panic)) => self.abnormal(effect, CODE_PANICKED, "the tool panicked"),
        }
    }

    async fn verify_outcome_artifacts(
        &self,
        scope: &ToolScope,
        outcome: &ToolOutcome,
    ) -> Result<(), ToolError> {
        let Some(ids) = outcome.metadata.get("artifact_ids") else {
            return Ok(());
        };
        let Some(list) = ids.as_array() else {
            return Err(ToolError {
                code: crate::artifact::CODE_ARTIFACT_INVALID_INPUT.into(),
                message: "metadata.artifact_ids must be an array of strings".into(),
            });
        };
        for entry in list {
            let Some(aid) = entry.as_str().map(str::trim).filter(|s| !s.is_empty()) else {
                return Err(ToolError {
                    code: crate::artifact::CODE_ARTIFACT_INVALID_INPUT.into(),
                    message: "metadata.artifact_ids entries must be non-empty strings".into(),
                });
            };
            self.services
                .artifacts
                .verify_in_family(
                    &scope.owner_id,
                    &scope.workspace_id,
                    &scope.root_session_id,
                    aid,
                )
                .await?;
        }
        Ok(())
    }

    /// Timeout/panic: tool ReadOnly có thể báo lỗi kỹ thuật; tool Mutating có outcome unknown.
    fn abnormal(&self, effect: ToolEffect, code: &str, message: &str) -> Dispatch {
        match effect {
            ToolEffect::ReadOnly => Dispatch::Settled(Settled::failure(code, message)),
            ToolEffect::Mutating => Dispatch::Unknown(CODE_OUTCOME_UNKNOWN),
        }
    }

    fn settle(&self, result: Result<ToolOutcome, ToolError>) -> Settled {
        self.limits.settle(result)
    }
}

/// Tên tool có thể chạy lại an toàn khi outcome của lần chạy trước không xác định: `skill`
/// (đọc), `subagent` (idempotent theo invocation ID) và tool khai báo `ReadOnly`. Tool Mutating
/// hoặc không còn đăng ký không nằm trong tập này: phải reconcile, không replay.
pub(crate) fn replay_safe_names(tools: &BTreeMap<String, Arc<dyn HostTool>>) -> BTreeSet<String> {
    let mut names: BTreeSet<String> = tools
        .iter()
        .filter(|(_, t)| t.descriptor().effect == ToolEffect::ReadOnly)
        .map(|(name, _)| name.clone())
        .collect();
    names.insert(SKILL_TOOL.to_owned());
    names.insert(ARTIFACT_READ_TOOL.to_owned());
    names.insert(SUBAGENT_TOOL.to_owned());
    names
}

impl ToolLimits {
    /// Chốt một outcome thành `Settled` (hard cap, model cap, metadata cap). Dùng chung cho
    /// tool chạy bình thường và outcome do operator xác minh.
    pub(crate) fn settle(&self, result: Result<ToolOutcome, ToolError>) -> Settled {
        match result {
            Err(e) => {
                let code = if valid_code(&e.code) {
                    e.code.as_str()
                } else {
                    "TOOL_ERROR"
                };
                Settled::failure(code, &e.message)
            }
            Ok(outcome) => {
                let len = outcome.content.len();
                if len > self.hard_cap_bytes {
                    return Settled::failure(
                        CODE_TOO_LARGE,
                        &format!(
                            "tool output is {len} bytes, over the {} byte persistence limit",
                            self.hard_cap_bytes
                        ),
                    );
                }
                let metadata = if outcome.metadata.to_string().len() > MAX_METADATA_BYTES {
                    json!({ "truncated": true })
                } else {
                    outcome.metadata
                };
                Settled {
                    failed: false,
                    model_text: truncate_utf8(&outcome.content, self.model_cap_bytes),
                    result_json: json!({ "content": outcome.content, "metadata": metadata })
                        .to_string(),
                    error_code: None,
                }
            }
        }
    }
}

fn skill_tool() -> ModelTool {
    ModelTool {
        name: SKILL_TOOL.to_owned(),
        description: "Load the full instructions of a specialist skill by id. Only skills \
                      listed in <available_skills> can be loaded."
            .to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false
        }),
    }
}

fn artifact_read_tool() -> ModelTool {
    ModelTool {
        name: ARTIFACT_READ_TOOL.to_owned(),
        description: "Read a bounded UTF-8 text excerpt from a committed artifact in the \
                      current session family by artifact_id."
            .to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "artifact_id": { "type": "string" },
                "offset_bytes": { "type": "integer" },
                "max_bytes": { "type": "integer" }
            },
            "required": ["artifact_id"],
            "additionalProperties": false
        }),
    }
}

fn valid_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

/// Tên tool do model sinh: cắt ngắn và bỏ ký tự điều khiển trước khi echo lại.
fn safe_name(name: &str) -> String {
    name.chars().filter(|c| !c.is_control()).take(64).collect()
}

fn sanitize_message(msg: &str) -> String {
    msg.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(MAX_ERROR_CHARS)
        .collect()
}

/// Cắt ở ranh giới ký tự và **luôn** kèm marker nêu rõ phần bị bỏ.
pub(crate) fn truncate_utf8(s: &str, cap: usize) -> String {
    if s.len() <= cap {
        return s.to_owned();
    }
    let mut end = cap;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n[output truncated: {end} of {} bytes shown; the full result is stored]",
        &s[..end],
        s.len()
    )
}

/// Validate envelope theo JSON Schema tối thiểu: `type`, `enum`, `required`, `properties`,
/// `additionalProperties:false`, `items`. Đây là kiểm tra kỹ thuật, không phải semantic.
pub(crate) fn validate_args(schema: &Value, args: &Value) -> Result<(), String> {
    validate(schema, args, "arguments", 0)
}

fn type_matches(t: &str, v: &Value) -> bool {
    match t {
        "string" => v.is_string(),
        "number" => v.is_number(),
        "integer" => {
            v.is_i64()
                || v.is_u64()
                || v.as_f64()
                    .is_some_and(|f| f.is_finite() && f.fract() == 0.0)
        }
        "boolean" => v.is_boolean(),
        "object" => v.is_object(),
        "array" => v.is_array(),
        "null" => v.is_null(),
        _ => true, // type lạ: không ràng buộc
    }
}

fn validate(schema: &Value, v: &Value, path: &str, depth: usize) -> Result<(), String> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(format!("{path} is nested too deeply"));
    }
    let Some(s) = schema.as_object() else {
        return Ok(());
    };
    if let Some(t) = s.get("type") {
        let ok = match t {
            Value::String(t) => type_matches(t, v),
            Value::Array(ts) => ts
                .iter()
                .filter_map(Value::as_str)
                .any(|t| type_matches(t, v)),
            _ => true,
        };
        if !ok {
            let want = t.as_str().map_or_else(|| t.to_string(), str::to_owned);
            return Err(format!("{path} must be of type {want}"));
        }
    }
    if let Some(allowed) = s.get("enum").and_then(Value::as_array) {
        if !allowed.contains(v) {
            return Err(format!("{path} is not one of the allowed values"));
        }
    }
    if let Some(obj) = v.as_object() {
        if let Some(required) = s.get("required").and_then(Value::as_array) {
            for name in required.iter().filter_map(Value::as_str) {
                if !obj.contains_key(name) {
                    return Err(format!("{path}.{name} is required"));
                }
            }
        }
        let props = s.get("properties").and_then(Value::as_object);
        let closed = s.get("additionalProperties") == Some(&Value::Bool(false));
        for (k, val) in obj {
            match props.and_then(|p| p.get(k)) {
                Some(ps) => validate(ps, val, &format!("{path}.{k}"), depth + 1)?,
                None if closed => return Err(format!("{path}.{k} is not allowed")),
                None => {}
            }
        }
    }
    if let (Some(items), Some(arr)) = (s.get("items"), v.as_array()) {
        for (i, x) in arr.iter().enumerate() {
            validate(items, x, &format!("{path}[{i}]"), depth + 1)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "n": { "type": "integer" },
                "mode": { "enum": ["r", "w"] },
                "tags": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["path"],
            "additionalProperties": false
        })
    }

    #[test]
    fn schema_accepts_valid_and_rejects_invalid_arguments() {
        assert!(validate_args(&schema(), &json!({"path": "a", "n": 3, "mode": "r"})).is_ok());
        let err = |v: Value| validate_args(&schema(), &v).unwrap_err();
        assert!(err(json!({})).contains("arguments.path is required"));
        assert!(err(json!({"path": 1})).contains("arguments.path must be of type string"));
        assert!(err(json!({"path": "a", "n": 1.5})).contains("arguments.n"));
        assert!(err(json!({"path": "a", "mode": "x"})).contains("allowed values"));
        assert!(err(json!({"path": "a", "tags": ["x", 2]})).contains("arguments.tags[1]"));
        assert!(err(json!({"path": "a", "extra": 1})).contains("arguments.extra is not allowed"));
        assert!(err(json!("not an object")).contains("must be of type object"));
        assert!(validate_args(&schema(), &json!({"path": "a", "n": 2.0})).is_ok());
    }

    #[test]
    fn truncation_is_marked_and_respects_char_boundaries() {
        assert_eq!(truncate_utf8("abc", 10), "abc");
        let t = truncate_utf8("ééééé", 3); // mỗi 'é' = 2 byte → cắt còn 1 ký tự
        assert!(t.starts_with("é\n[output truncated: 2 of 10 bytes shown"));
    }

    #[test]
    fn effective_permissions_only_narrow() {
        let agent = AgentDefinition::new("a", "p", "m")
            .with_allowed_tools(["skill", "read"])
            .with_allowed_skills(["x", "y"]);
        let all = Effective::resolve(&agent, &SessionPolicy::default());
        assert_eq!(all.tools.len(), 2);
        assert_eq!(all.skills.len(), 2);
        // Policy đòi thêm quyền ngoài agent → bị bỏ; chỉ phần giao còn lại.
        let narrowed = Effective::resolve(
            &agent,
            &SessionPolicy::default()
                .with_allowed_tools(["read", "write_file"])
                .with_allowed_skills(["y", "z"]),
        );
        assert_eq!(narrowed.tools, BTreeSet::from(["read".to_string()]));
        assert_eq!(narrowed.skills, BTreeSet::from(["y".to_string()]));
        let none = Effective::resolve(
            &agent,
            &SessionPolicy::default().with_allowed_skills::<_, String>([]),
        );
        assert!(none.skills.is_empty());
    }

    #[test]
    fn error_codes_are_validated_and_messages_sanitized() {
        assert!(valid_code("NOT_FOUND_1"));
        assert!(!valid_code("lower"));
        assert!(!valid_code(""));
        let s = Settled::failure("X", "bad\nline\u{7}");
        assert!(!s.model_text.contains('\n'));
    }
}
