# 04 — Skill Discovery & Loading (MVP)

## 4.1. Khái niệm

Skill là gói hướng dẫn chuyên biệt trong `SKILL.md` với tài nguyên tham khảo. **Skill không phải Agent hoặc Tool** và nội dung skill không được tự thực thi. Root/child có skill catalogs độc lập theo quyền của session.

## 4.2. Quy trình (theo OpenCode)

1. `SkillManager` scan và đọc `skills/<id>/SKILL.md` **một lần khi startup**, giữ snapshot metadata + content trong memory; chỉ inject content vào LLM context khi gọi tool.
2. PromptBuilder chỉ đưa `<available_skills>` (`id/name/description`) của agent được phép vào context.
3. Agent gọi `skill(id)` nếu cần.
4. ToolExecutor xác thực input/quyền rồi gọi `SkillManager.load()` để lấy nội dung `SKILL.md` từ snapshot, kèm base directory và sample resource paths.
5. ToolExecutor trả text; **AgentRuntime persist** tool call/result vào đúng session rồi tiếp tục model loop.

Tham khảo: https://github.com/anomalyco/opencode/blob/dev/packages/core/src/skill/instructions.ts và https://github.com/anomalyco/opencode/blob/dev/packages/core/src/tool/plugin/skill.ts.

## 4.3. Discovery trong MVP

Chỉ hỗ trợ `skills/` của project, một ID duy nhất. Duplicate ID hoặc frontmatter thiếu tên/mô tả được báo ngay ở startup; không merge. Không có user-global source, remote URL skill, version manager hay hot reload. Thay đổi skill cần restart.

```markdown
---
name: rust-review
description: Review Rust code for correctness and safety.
---
# Workflow
1. Inspect affected files.
2. Read relevant tests.
3. Report findings with file references.
```

Cấm unsafe ID, traversal và symlink escape. Scripts trong skill chỉ là tham chiếu; không auto-run.

## 4.4. Interface: một concrete module

```rust
pub struct SkillSummary {
    pub id: String,
    pub name: String,
    pub description: String,
}

pub struct LoadedSkill {
    pub id: String,
    pub directory: std::path::PathBuf,
    pub content: String,
    pub related_files: Vec<std::path::PathBuf>,
}

pub struct SkillManager { /* immutable startup index */ }

impl SkillManager {
    pub fn available(&self, agent_id: &str) -> Vec<SkillSummary> {
        todo!() // filter permissions
    }
    pub async fn load(&self, session_id: &str, id: &str) -> Result<LoadedSkill, SkillError> {
        todo!() // session permissions + safe read
    }
}
```

Không thêm trait `SkillRegistry` / `SkillLoader` riêng. `available()` và `load()` **đều** tính quyền thực tế; không coi catalog là cơ chế bảo mật. `session_id` được dùng để resolve permissions; chính `AgentRuntime` sở hữu việc persist tool result.

## 4.5. Skill Tool

Input: `{ "id": "rust-review" }`. Output có `name`, `directory`, `content` (rendered `<skill_content>`). Lỗi kỹ thuật: `SKILL_NOT_FOUND`, `PERMISSION_DENIED`, `SKILL_UNREADABLE`, `INVALID_SKILL`.

```xml
<skill_content name="rust-review">
# Skill: rust-review
...SKILL.md contents...
Base directory: /project/skills/rust-review
<skill_files>
  <file>references/checklist.md</file>
</skill_files>
</skill_content>
```

`skill_files` chỉ là các đường dẫn tham khảo được liệt kê có giới hạn; muốn đọc thêm, agent dùng tool đọc file riêng. Nội dung vượt hard size limit → tool error rõ ràng; **không âm thầm cắt mất instructions**.

## 4.6. Trust, persistence, tests

- Child không tự kế thừa skill contents của Root; load vào đúng session hiện tại.
- SKILL.md không cấp thêm quyền tool, không ghi đè runtime policy. Bất kỳ resource read đều đi qua tool permission/path confinement.
- Truy vết tool call ID, skill ID và path. Startup snapshot giúp cố định nội dung đã nạp trong một process lifetime; tham chiếu file ngoài `SKILL.md` vẫn được đọc bằng tool với quyền riêng.
- Test: startup discovery/snapshot, duplicate ID, missing frontmatter, deny-by-agent, deny-on-load, successful `skill()` và child history isolation.

### Sau MVP

Nguồn skills user/global, URL pull, hot reload, version/hash lineage và smart compaction-aware reinjection. Chỉ triển khai khi có nhu cầu thực.
