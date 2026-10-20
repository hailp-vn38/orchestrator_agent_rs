# 14 — Glossary

| Thuật ngữ | Định nghĩa |
|---|---|
| **AgentDefinition** | Cấu hình agent: ID, mode, model, prompt, permissions, tool/skill allowlists. Không chứa history. |
| **Root / Primary Agent** | Agent tiếp nhận yêu cầu user, phân việc và tự đánh giá Subagent text. |
| **Subagent** | Agent chạy trong child session, được gọi qua tool `subagent`. |
| **Session** | Hội thoại bền vững có `session_id`, agent, parent ID và history. |
| **Child Session** | Session có `parent_session_id`; quyền sở hữu gắn với parent/workspace. |
| **Run** | Một execution attempt của AgentRuntime trong session, có thể gồm rất nhiều LLM/tool steps. |
| **Delegation** | Một `subagent` tool invocation của parent; `child_session_id` có thể trỏ tới child được resume nhiều lần. MVP không có bảng `tasks` riêng. |
| **Tool** | Chức năng mà LLM có thể yêu cầu hệ thống thực thi qua ToolExecutor. |
| **Tool Call ID** | Mã ghép tool call/result trong model history; phải giữ nguyên. |
| **Skill** | Gói instructions/workflow (`SKILL.md` và tài nguyên phụ), load theo nhu cầu. |
| **Skill Catalog** | Metadata ID/name/description cho agent khám phá skill. |
| **Prompt Builder** | Module dựng system context, instructions, skill catalog, history, available tool definitions. |
| **LlmGateway** | Adapter độc lập với crate `llm`, định tuyến provider, chuẩn hóa tool calling/streaming. |
| **Final Text** | Nội dung assistant text cuối cùng của child run; chưa hàm ý đúng nghiệp vụ. |
| **Tool Result** | Message đưa kết quả tool vào history session đang gọi. |
| **Semantic Validation** | Kiểm tra chất lượng nội dung nhiệm vụ; **không bắt buộc** với Subagent. |
| **Technical Validation** | Check quyền, schema tool inputs, session ownership và giới hạn runtime; **bắt buộc**. |
| **Progressive Disclosure** | Chỉ load toàn nội dung Skill khi agent chọn `skill(id)` thay vì nhồi tất cả vào prompt. |
| **Artifact** | Cơ chế lưu dữ liệu lớn ngoài model-visible result (mở rộng sau MVP). |
| **Foreground** | Parent chờ Subagent hoàn thành trước khi có final tool result. |
| **Background** | Child chạy độc lập và gửi completion event vào parent inbox; chưa có input/output trong schema MVP. |
| **Module** | Phần có interface và implementation, bất kể cỡ lớn nhỏ. |
| **Interface** | Toàn bộ điều caller phải biết, gồm signatures, invariants, errors, latency và ordering. |
| **Seam** | Vị trí đặt interface, nơi có thể thay đổi adapter mà không sửa caller. |
| **Adapter** | Implementation khớp interface ở seam (ví dụ SQLiteSessionStore). |
| **Transcript Writer** | `AgentRuntime` là owner việc ghi user/assistant/tool call/tool result qua SessionStore. |
| **Run Admission** | Unique partial index trên `agent_runs` ngăn hai `running` runs cùng `session_id`. |
| **Turn** | Một user/delegation input; `turn_id` ổn định qua restart recovery attempts. |
| **Step** | Một iteration LLM và tools; không tương đương Run hay business Task. |
| **Optional Steps Limit** | `agent.steps` mặc định unset; nếu set sẽ buộc model text-only progress tại giới hạn. |
| **Continuous Execution** | Session runner lặp LLM → Tool/Subagent → LLM đến final/cancel/error, không tự giới hạn 20 steps. |
| **Compaction Checkpoint** | Summary lưu SQLite, model thấy summary + recent turns; full history vẫn ở DB. |
| **Synthetic Message** | Lời nhắc kỹ thuật của runtime, không phải user input. |
| **Recovery Attempt** | Run mới cùng `turn_id` sau restart, `resume_attempt` bounded để tránh crash-loop. |
| **Execution Claim** | Durable `agent_runs.status=running` dùng cho startup recovery + single-run admission. |
| **Outcome Unknown** | Trạng thái tool side-effect đã được yêu cầu nhưng chưa có durable result sau crash; không tự replay. |
