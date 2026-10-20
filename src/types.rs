//! Public value types của command/read interface (docs/18 §18.4.2).

/// Principal đã được host xác thực. Không bao giờ deserialize từ input của LLM/client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallerContext {
    pub owner_id: String,
    pub workspace_id: String,
}

impl CallerContext {
    pub fn new(owner_id: impl Into<String>, workspace_id: impl Into<String>) -> Self {
        Self {
            owner_id: owner_id.into(),
            workspace_id: workspace_id.into(),
        }
    }
}

/// Input + run claim đã commit bền vững. Không có nghĩa LLM đã bắt đầu.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedTurn {
    pub session_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    Interrupted,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Interrupted => "interrupted",
        }
    }

    pub(crate) fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "running" => Self::Running,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            "timed_out" => Self::TimedOut,
            "interrupted" => Self::Interrupted,
            _ => return None,
        })
    }

    pub fn is_terminal(self) -> bool {
        self != Self::Running
    }
}

/// Lỗi kỹ thuật của run, đã sanitize (không chứa body/secret của provider).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultError {
    pub code: String,
    pub message: String,
}

/// Read projection của attempt mới nhất cho `(session_id, turn_id)`. `Completed` chỉ là
/// trạng thái thực thi, không khẳng định yêu cầu nghiệp vụ đã đạt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentResult {
    pub session_id: String,
    pub turn_id: String,
    pub run_id: String,
    /// `resume_attempt` của run mới nhất.
    pub attempt: u32,
    pub status: RunStatus,
    /// Chỉ có khi attempt mới nhất đã commit `final_message_id`; không lấy assistant
    /// text cuối của session hay của attempt trước.
    pub output: Option<String>,
    pub final_message_id: Option<String>,
    pub error: Option<ResultError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MessageRole {
    User,
    Assistant,
    Tool,
}

impl MessageRole {
    pub(crate) fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "user" => Self::User,
            "assistant" => Self::Assistant,
            "tool" => Self::Tool,
            _ => return None,
        })
    }
}

/// Canonical message đã authorize/redact của một session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageView {
    pub id: String,
    pub sequence: u64,
    pub role: MessageRole,
    pub text: String,
    /// Attempt tạo ra message (assistant/tool); `None` với input của user.
    pub run_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageRequest {
    /// Cursor opaque từ trang trước; gắn với đúng session, không phải authorization token.
    pub cursor: Option<String>,
    /// Được engine clamp vào `1..=page_size`.
    pub limit: u32,
}

impl PageRequest {
    pub fn first(limit: u32) -> Self {
        Self {
            cursor: None,
            limit,
        }
    }

    pub fn after(cursor: impl Into<String>, limit: u32) -> Self {
        Self {
            cursor: Some(cursor.into()),
            limit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}
