//! `PromptBuilder` (docs/03): dựng context của một model request từ các nguồn có thứ tự ổn
//! định — mandatory base, prompt của agent, project instructions, skill catalog được phép và
//! lịch sử đang hoạt động. `LlmGateway` chỉ chuyển đổi sang giao thức provider.
//!
//! Base luôn đứng đầu và không thể bị agent/project/skill thay thế. Project instructions và
//! skill là untrusted context: chúng chỉ là văn bản, quyền thực thi nằm ở Rust.

use std::collections::HashMap;

use tracing::warn;

use crate::compaction::summary_message;
use crate::gateway::{ModelMessage, ModelRole, ModelToolCall};
use crate::skill::{xml_escape, SkillSummary};
use crate::store::{Context, TranscriptEntry};

/// Ghép system prompt tĩnh: base luôn đứng đầu, prompt của agent không thay thế được.
pub(crate) fn build_system(base: &str, agent_prompt: &str) -> String {
    format!("{}\n\n{}", base.trim_end(), agent_prompt.trim())
}

pub(crate) struct PromptBuilder {
    /// agent_id → base + agent prompt (+ project instructions), snapshot lúc build.
    systems: HashMap<String, String>,
}

impl PromptBuilder {
    pub(crate) fn new<'a>(
        base: &str,
        project_instructions: Option<&str>,
        agent_prompts: impl Iterator<Item = (&'a str, &'a str)>,
    ) -> Self {
        let systems = agent_prompts
            .map(|(id, prompt)| {
                let mut system = build_system(base, prompt);
                if let Some(project) = project_instructions {
                    system.push_str(&format!(
                        "\n\n<project_instructions>\n{}\n</project_instructions>",
                        project.trim()
                    ));
                }
                (id.to_owned(), system)
            })
            .collect();
        Self { systems }
    }

    /// System prompt của một request: phần tĩnh + catalog skill **đã lọc theo quyền** của
    /// session (skill bị từ chối không lộ metadata). `None` nếu agent không còn cấu hình.
    pub(crate) fn system(&self, agent_id: &str, catalog: &[SkillSummary]) -> Option<String> {
        let mut system = self.systems.get(agent_id)?.clone();
        if !catalog.is_empty() {
            system.push_str("\n\n<available_skills>\n");
            for s in catalog {
                system.push_str(&format!(
                    "  <skill>\n    <id>{}</id>\n    <name>{}</name>\n    \
                     <description>{}</description>\n  </skill>\n",
                    xml_escape(&s.id),
                    xml_escape(&s.name),
                    xml_escape(&s.description)
                ));
            }
            system.push_str("</available_skills>");
        }
        Some(system)
    }
}

/// Context của session → model messages: summary của checkpoint (nếu có) rồi các exchange
/// settled từ frontier. Transcript gốc trong SQLite không bị thay đổi.
pub(crate) fn context_messages(context: Context) -> Vec<ModelMessage> {
    let mut out = Vec::new();
    if let Some(checkpoint) = &context.checkpoint {
        out.push(summary_message(checkpoint));
    }
    out.extend(model_messages(
        context.entries.into_iter().map(|(_, e)| e).collect(),
    ));
    out
}

/// Transcript → model messages. Một assistant tool-call message chỉ được gửi khi **mọi** call
/// của nó đã có result; exchange dang dở (run bị interrupted giữa chừng) bị loại khỏi context
/// thay vì gửi dangling call/result tới provider. Transcript gốc vẫn được giữ nguyên.
pub(crate) fn model_messages(entries: Vec<TranscriptEntry>) -> Vec<ModelMessage> {
    let mut out = Vec::with_capacity(entries.len());
    let mut i = 0;
    while i < entries.len() {
        match &entries[i] {
            TranscriptEntry::User(text) => out.push(ModelMessage::text(ModelRole::User, text)),
            // Result mồ côi (assistant call của nó đã bị loại): không bao giờ gửi riêng lẻ.
            TranscriptEntry::Tool { .. } => {}
            TranscriptEntry::Assistant { text, calls } if calls.is_empty() => {
                out.push(ModelMessage::text(ModelRole::Assistant, text));
            }
            TranscriptEntry::Assistant { text, calls } => {
                let mut end = i + 1;
                while matches!(entries.get(end), Some(TranscriptEntry::Tool { .. })) {
                    end += 1;
                }
                let results: Vec<&TranscriptEntry> = entries[i + 1..end].iter().collect();
                let paired: Option<Vec<ModelMessage>> = calls
                    .iter()
                    .map(|c| {
                        results.iter().find_map(|r| match r {
                            TranscriptEntry::Tool {
                                call_id,
                                tool_name,
                                text,
                            } if call_id == &c.id => {
                                Some(ModelMessage::tool_result(call_id, tool_name, text))
                            }
                            _ => None,
                        })
                    })
                    .collect();
                match paired {
                    Some(tool_msgs) => {
                        out.push(ModelMessage::assistant_with_calls(
                            text,
                            calls
                                .iter()
                                .map(|c| ModelToolCall {
                                    id: c.id.clone(),
                                    name: c.name.clone(),
                                    arguments: c.arguments.clone(),
                                })
                                .collect(),
                        ));
                        out.extend(tool_msgs);
                    }
                    None => warn!("an unsettled tool exchange was omitted from the model context"),
                }
                i = end;
                continue;
            }
        }
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::StoredToolCall;

    fn call(id: &str) -> StoredToolCall {
        StoredToolCall {
            id: id.into(),
            name: "t".into(),
            arguments: serde_json::json!({}),
        }
    }

    fn tool(id: &str, text: &str) -> TranscriptEntry {
        TranscriptEntry::Tool {
            call_id: id.into(),
            tool_name: "t".into(),
            text: text.into(),
        }
    }

    #[test]
    fn base_always_precedes_agent_prompt() {
        let s = build_system("BASE\n", "  agent prompt \n");
        assert!(s.starts_with("BASE"));
        assert!(s.ends_with("agent prompt"));
    }

    #[test]
    fn system_without_extras_equals_base_plus_agent() {
        let p = PromptBuilder::new("BASE", None, [("a", "prompt")].into_iter());
        assert_eq!(p.system("a", &[]).unwrap(), build_system("BASE", "prompt"));
        assert!(p.system("missing", &[]).is_none());
    }

    #[test]
    fn project_and_catalog_follow_base_and_escape_metadata() {
        let p = PromptBuilder::new(
            "BASE",
            Some(" project rules "),
            [("a", "prompt")].into_iter(),
        );
        let s = p
            .system(
                "a",
                &[SkillSummary {
                    id: "x".into(),
                    name: "X".into(),
                    description: "</available_skills><b>".into(),
                }],
            )
            .unwrap();
        assert!(s.starts_with("BASE"));
        let project = s.find("<project_instructions>").unwrap();
        let catalog = s.find("<available_skills>").unwrap();
        assert!(project < catalog);
        assert!(s.contains("project rules"));
        assert_eq!(s.matches("</available_skills>").count(), 1);
        assert!(s.contains("&lt;/available_skills&gt;&lt;b&gt;"));
    }

    #[test]
    fn complete_exchange_is_kept_in_order() {
        let msgs = model_messages(vec![
            TranscriptEntry::User("go".into()),
            TranscriptEntry::Assistant {
                text: String::new(),
                calls: vec![call("a"), call("b")],
            },
            tool("b", "rb"),
            tool("a", "ra"),
            TranscriptEntry::Assistant {
                text: "done".into(),
                calls: vec![],
            },
        ]);
        let roles: Vec<ModelRole> = msgs.iter().map(|m| m.role).collect();
        assert_eq!(
            roles,
            [
                ModelRole::User,
                ModelRole::Assistant,
                ModelRole::Tool,
                ModelRole::Tool,
                ModelRole::Assistant
            ]
        );
        // Results theo thứ tự call của assistant, ghép bằng provider ID.
        assert_eq!(msgs[2].tool_call_id.as_deref(), Some("a"));
        assert_eq!(msgs[3].tool_call_id.as_deref(), Some("b"));
    }

    #[test]
    fn dangling_exchange_is_never_sent_to_the_model() {
        let msgs = model_messages(vec![
            TranscriptEntry::User("one".into()),
            TranscriptEntry::Assistant {
                text: String::new(),
                calls: vec![call("a"), call("b")],
            },
            tool("a", "ra"), // thiếu result của "b"
            TranscriptEntry::User("two".into()),
        ]);
        let texts: Vec<&str> = msgs.iter().map(|m| m.text.as_str()).collect();
        assert_eq!(texts, ["one", "two"]);
    }
}
