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
    /// Provider call ID của tool-result message; `None` với role khác.
    pub tool_call_id: Option<String>,
    /// Tool calls mà assistant message này yêu cầu (rỗng nếu là câu trả lời cuối).
    pub tool_calls: Vec<MessageToolCall>,
}

/// Tool call đã persist trong một assistant message (không kèm arguments).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageToolCall {
    /// Provider call ID, chỉ duy nhất trong assistant message chứa nó.
    pub call_id: String,
    pub name: String,
}

/// Thu hẹp quyền của **một** session so với `AgentDefinition`. `None` = kế thừa agent;
/// `Some(list)` = chỉ giữ phần giao với quyền của agent. Policy không bao giờ mở rộng quyền.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionPolicy {
    pub allowed_tools: Option<Vec<String>>,
    pub allowed_skills: Option<Vec<String>>,
    /// Trần quyền cho child do session này tạo: quyền của child là
    /// `AgentDefinition(child) ∩ trần`. `None` = child dùng đủ quyền của định nghĩa agent
    /// (do host duyệt lúc cấu hình). `allowed_tools`/`allowed_skills` ở trên chỉ áp cho chính
    /// session này, không tự áp xuống child.
    pub child_allowed_tools: Option<Vec<String>>,
    pub child_allowed_skills: Option<Vec<String>>,
}

impl SessionPolicy {
    pub fn with_allowed_tools<I, S>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_tools = Some(tools.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_allowed_skills<I, S>(mut self, skills: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_skills = Some(skills.into_iter().map(Into::into).collect());
        self
    }

    /// Trần tool cho mọi child do session này tạo (xem [`SessionPolicy::child_allowed_tools`]).
    pub fn with_child_allowed_tools<I, S>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.child_allowed_tools = Some(tools.into_iter().map(Into::into).collect());
        self
    }

    /// Trần skill cho mọi child do session này tạo.
    pub fn with_child_allowed_skills<I, S>(mut self, skills: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.child_allowed_skills = Some(skills.into_iter().map(Into::into).collect());
        self
    }
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
