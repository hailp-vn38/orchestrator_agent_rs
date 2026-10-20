//! `ToolExecutor` (docs/05): validate schema + effective permissions **trước** dispatch,
//! chạy HostTool/`skill` có timeout và cancel hợp tác, trả outcome cho `AgentRuntime` ghi
//! transcript. Executor không bao giờ mở transaction hay ghi history (một writer duy nhất).
//!
//! Output của tool/skill là untrusted context: runtime không suy luận gì từ văn bản đó.

use crate::feed::ExecutionFeed;
use crate::skill::SkillManager;
use crate::subagent::{SubagentInput, TaskManager};
use crate::tool::{HostTool, ToolDescriptor, ToolEffect, ToolServices};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

mod dispatch;
mod outcome;
mod permissions;
mod prepare;
mod schema;
#[cfg(test)]
mod tests;

pub(crate) use outcome::truncate_utf8;
pub(crate) use permissions::{replay_safe_names, Effective};
pub(crate) use schema::validate_args;

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
}
