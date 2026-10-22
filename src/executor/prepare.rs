//! Quảng bá tool và kiểm tra arguments trước side effect.
use super::outcome::safe_name;
use super::{
    validate_args, Effective, Prepared, Runnable, Settled, ToolExecutor, ARTIFACT_READ_TOOL,
    CODE_DEPTH_LIMIT, CODE_INVALID_ARGS, CODE_NOT_ALLOWED, CODE_UNAVAILABLE, SKILL_TOOL,
};
use crate::gateway::ModelTool;
use crate::subagent::{self, SubagentInput, SUBAGENT_TOOL};
use crate::tool::RESERVED_TOOL_NAMES;
use serde_json::{json, Value};
use std::sync::Arc;

impl ToolExecutor {
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
            out.push(self.tasks.tool_definition(eff.subagents.as_ref()));
        }
        out
    }

    /// `subagent` chỉ được quảng bá khi session được phép, không phải child và có agent
    /// làm child được.
    fn subagent_offered(&self, eff: &Effective) -> bool {
        eff.tools.contains(SUBAGENT_TOOL)
            && !eff.child
            && self.tasks.has_candidates(eff.subagents.as_ref())
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
            if !self.tasks.has_candidates(eff.subagents.as_ref()) {
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
