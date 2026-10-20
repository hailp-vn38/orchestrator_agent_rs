//! `SkillManager` (docs/04): index bất biến scan **một lần** lúc build từ `skills/<id>/SKILL.md`.
//!
//! - Snapshot metadata + nội dung trong memory; thay đổi cần restart (không hot reload).
//! - `available` và `load` dùng cùng effective permissions của session; catalog không phải
//!   cơ chế bảo mật nên `load` kiểm tra lại quyền tại invocation.
//! - Skill id đến từ LLM chỉ được tra trong index, không bao giờ ghép vào đường dẫn. Entry là
//!   symlink hoặc `SKILL.md` là symlink bị từ chối ngay lúc build; file liên quan (symlink)
//!   không được liệt kê.
//! - Nội dung skill là untrusted context: không cấp thêm quyền và không tự thực thi.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::error::{ConfigReason, OrchestratorError};

const SKILL_FILE: &str = "SKILL.md";
/// Trần đọc `SKILL.md` lúc index (để lấy frontmatter); file lớn hơn bị từ chối.
const MAX_INDEX_READ_BYTES: u64 = 1024 * 1024;
const MAX_RELATED_FILES: usize = 20;
const MAX_RELATED_DEPTH: usize = 3;
/// Dừng duyệt thư mục khi đã gom đủ ứng viên (giữ IO bounded với thư mục khổng lồ).
const MAX_RELATED_SCAN: usize = 200;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SkillSummary {
    pub id: String,
    pub name: String,
    pub description: String,
}

struct SkillEntry {
    summary: SkillSummary,
    dir: PathBuf,
    /// `None` khi `SKILL.md` vượt `max_bytes`: vẫn có trong catalog nhưng `load` báo lỗi rõ.
    body: Option<String>,
    size: usize,
    related_files: Vec<String>,
}

pub(crate) struct LoadedSkill {
    pub id: String,
    pub name: String,
    pub directory: PathBuf,
    pub content: String,
    pub related_files: Vec<String>,
}

/// Lỗi kỹ thuật của `skill(id)`; mã ổn định theo docs/04 §4.5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SkillError {
    NotFound,
    PermissionDenied,
    Invalid(String),
}

impl SkillError {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::NotFound => "SKILL_NOT_FOUND",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::Invalid(_) => "INVALID_SKILL",
        }
    }

    pub(crate) fn message(&self) -> String {
        match self {
            Self::NotFound => "skill not found".into(),
            Self::PermissionDenied => "the skill is not permitted for this session".into(),
            Self::Invalid(m) => m.clone(),
        }
    }
}

pub(crate) struct SkillManager {
    skills: BTreeMap<String, SkillEntry>,
    max_bytes: usize,
}

pub(crate) fn valid_skill_id(id: &str) -> bool {
    let mut chars = id.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit())
        && id.len() <= 64
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

fn invalid(id: &str, msg: &str) -> OrchestratorError {
    OrchestratorError::config(ConfigReason::InvalidSkill, format!("skill '{id}': {msg}"))
}

impl SkillManager {
    pub(crate) fn empty(max_bytes: usize) -> Self {
        Self {
            skills: BTreeMap::new(),
            max_bytes,
        }
    }

    /// Scan `root`; lỗi (duplicate, thiếu frontmatter, symlink, id sai) báo ngay, không merge.
    pub(crate) async fn discover(root: &Path, max_bytes: usize) -> Result<Self, OrchestratorError> {
        let bad_root = |m: &str| OrchestratorError::config(ConfigReason::InvalidSkillsDir, m);
        let root = tokio::fs::canonicalize(root)
            .await
            .map_err(|_| bad_root("skills_dir must be an existing directory"))?;
        if !tokio::fs::metadata(&root)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            return Err(bad_root("skills_dir must be an existing directory"));
        }

        let mut names = Vec::new();
        let mut rd = tokio::fs::read_dir(&root)
            .await
            .map_err(|_| bad_root("skills_dir is not readable"))?;
        while let Some(entry) = rd
            .next_entry()
            .await
            .map_err(|_| bad_root("skills_dir is not readable"))?
        {
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| bad_root("skills_dir contains a non-UTF-8 entry name"))?;
            names.push(name);
        }
        names.sort();

        let mut skills = BTreeMap::new();
        let mut by_name: BTreeMap<String, String> = BTreeMap::new();
        for id in names {
            let path = root.join(&id);
            let meta = tokio::fs::symlink_metadata(&path)
                .await
                .map_err(|_| invalid(&id, "entry is not readable"))?;
            if meta.file_type().is_symlink() {
                return Err(invalid(&id, "skill entries must not be symlinks"));
            }
            if !meta.is_dir() {
                continue; // README, v.v.: không phải skill
            }
            if !valid_skill_id(&id) {
                return Err(invalid(
                    &id,
                    "id must match [a-z0-9][a-z0-9_-]{0,63} (directory name)",
                ));
            }
            let dir = tokio::fs::canonicalize(&path)
                .await
                .map_err(|_| invalid(&id, "directory is not readable"))?;
            if !dir.starts_with(&root) {
                return Err(invalid(&id, "directory escapes the skills root"));
            }
            let entry = Self::index_one(&id, dir, max_bytes).await?;
            if let Some(other) = by_name.insert(entry.summary.name.clone(), id.clone()) {
                return Err(OrchestratorError::config(
                    ConfigReason::DuplicateSkill,
                    format!(
                        "skills '{other}' and '{id}' declare the same name '{}'",
                        entry.summary.name
                    ),
                ));
            }
            skills.insert(id, entry);
        }
        Ok(Self { skills, max_bytes })
    }

    async fn index_one(
        id: &str,
        dir: PathBuf,
        max_bytes: usize,
    ) -> Result<SkillEntry, OrchestratorError> {
        let file = dir.join(SKILL_FILE);
        let meta = tokio::fs::symlink_metadata(&file)
            .await
            .map_err(|_| invalid(id, "SKILL.md is missing"))?;
        if meta.file_type().is_symlink() || !meta.is_file() {
            return Err(invalid(
                id,
                "SKILL.md must be a regular file, not a symlink",
            ));
        }
        if meta.len() > MAX_INDEX_READ_BYTES {
            return Err(invalid(id, "SKILL.md is too large to index"));
        }
        let canon = tokio::fs::canonicalize(&file)
            .await
            .map_err(|_| invalid(id, "SKILL.md is not readable"))?;
        if !canon.starts_with(&dir) {
            return Err(invalid(id, "SKILL.md escapes the skill directory"));
        }
        let text = tokio::fs::read_to_string(&file)
            .await
            .map_err(|_| invalid(id, "SKILL.md must be readable UTF-8 text"))?;
        let (front, body) = parse_frontmatter(&text)
            .ok_or_else(|| invalid(id, "SKILL.md needs a frontmatter with name and description"))?;
        let field = |key: &str| {
            front
                .get(key)
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
        };
        let name = field("name").ok_or_else(|| invalid(id, "frontmatter is missing 'name'"))?;
        let description = field("description")
            .ok_or_else(|| invalid(id, "frontmatter is missing 'description'"))?;
        let size = text.len();
        let related_files = list_related(&dir).await;
        Ok(SkillEntry {
            summary: SkillSummary {
                id: id.to_owned(),
                name,
                description,
            },
            dir,
            body: (size <= max_bytes).then(|| body.trim().to_owned()),
            size,
            related_files,
        })
    }

    pub(crate) fn contains(&self, id: &str) -> bool {
        self.skills.contains_key(id)
    }

    /// Catalog theo effective permissions; skill bị từ chối không lộ metadata.
    pub(crate) fn available(&self, allowed: &BTreeSet<String>) -> Vec<SkillSummary> {
        self.skills
            .values()
            .filter(|e| allowed.contains(&e.summary.id))
            .map(|e| e.summary.clone())
            .collect()
    }

    /// Kiểm tra quyền **tại invocation** (cùng permission snapshot với `available`).
    pub(crate) fn load(
        &self,
        allowed: &BTreeSet<String>,
        id: &str,
    ) -> Result<LoadedSkill, SkillError> {
        if !valid_skill_id(id) {
            return Err(SkillError::Invalid("skill id is not valid".into()));
        }
        if !allowed.contains(id) {
            return Err(SkillError::PermissionDenied);
        }
        let entry = self.skills.get(id).ok_or(SkillError::NotFound)?;
        let Some(body) = &entry.body else {
            return Err(SkillError::Invalid(format!(
                "SKILL.md is {} bytes, over the {} byte limit; it is not truncated silently",
                entry.size, self.max_bytes
            )));
        };
        Ok(LoadedSkill {
            id: entry.summary.id.clone(),
            name: entry.summary.name.clone(),
            directory: entry.dir.clone(),
            content: body.clone(),
            related_files: entry.related_files.clone(),
        })
    }
}

pub(crate) fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// `<skill_content>` theo docs/04 §4.5; `skill_files` chỉ là tham chiếu, không tự đọc.
pub(crate) fn render(skill: &LoadedSkill) -> String {
    let mut out = format!(
        "<skill_content name=\"{}\">\n# Skill: {}\n\n{}\n\nBase directory: {}\n",
        xml_escape(&skill.name),
        skill.id,
        skill.content,
        skill.directory.display()
    );
    if !skill.related_files.is_empty() {
        out.push_str("<skill_files>\n");
        for f in &skill.related_files {
            out.push_str(&format!("  <file>{}</file>\n", xml_escape(f)));
        }
        out.push_str("</skill_files>\n");
    }
    out.push_str("</skill_content>");
    out
}

/// Frontmatter `---` … `---` với dòng `key: value` (không dùng parser YAML đầy đủ).
fn parse_frontmatter(text: &str) -> Option<(BTreeMap<String, String>, &str)> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut lines = text.split_inclusive('\n');
    if lines.next()?.trim() != "---" {
        return None;
    }
    let mut consumed = text.split_inclusive('\n').next()?.len();
    let mut map = BTreeMap::new();
    for line in lines {
        consumed += line.len();
        if line.trim() == "---" {
            return Some((map, &text[consumed..]));
        }
        if let Some((k, v)) = line.split_once(':') {
            let v = v.trim();
            let v = v
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .or_else(|| v.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
                .unwrap_or(v);
            map.insert(k.trim().to_owned(), v.to_owned());
        }
    }
    None
}

/// File tham khảo (bounded, không symlink); chỉ là danh sách đường dẫn tương đối.
async fn list_related(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![(dir.to_path_buf(), String::new(), 0usize)];
    while let Some((path, rel, depth)) = stack.pop() {
        let Ok(mut rd) = tokio::fs::read_dir(&path).await else {
            continue;
        };
        while let Ok(Some(entry)) = rd.next_entry().await {
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            let Ok(ft) = entry.file_type().await else {
                continue;
            };
            if ft.is_symlink() {
                continue;
            }
            let child_rel = if rel.is_empty() {
                name.clone()
            } else {
                format!("{rel}/{name}")
            };
            if ft.is_dir() {
                if depth + 1 < MAX_RELATED_DEPTH {
                    stack.push((entry.path(), child_rel, depth + 1));
                }
            } else if ft.is_file() && child_rel != SKILL_FILE {
                out.push(child_rel);
            }
            if out.len() >= MAX_RELATED_SCAN {
                break;
            }
        }
    }
    out.sort();
    out.truncate(MAX_RELATED_FILES);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_id_rules() {
        for ok in ["rust-review", "a", "plan_2", "0day"] {
            assert!(valid_skill_id(ok), "{ok}");
        }
        for bad in [
            "",
            "../x",
            "a/b",
            "A",
            "-a",
            "a b",
            ".hidden",
            &"a".repeat(65),
        ] {
            assert!(!valid_skill_id(bad), "{bad}");
        }
    }

    #[test]
    fn frontmatter_parsing() {
        let (m, body) =
            parse_frontmatter("---\nname: x\ndescription: \"d: e\"\n---\n# Body\ntext\n").unwrap();
        assert_eq!(m["name"], "x");
        assert_eq!(m["description"], "d: e");
        assert_eq!(body, "# Body\ntext\n");
        assert!(parse_frontmatter("# no frontmatter").is_none());
        assert!(parse_frontmatter("---\nname: x\n").is_none());
    }

    #[test]
    fn xml_escape_blocks_tag_injection() {
        assert_eq!(xml_escape("<a>&\""), "&lt;a&gt;&amp;&quot;");
    }
}
