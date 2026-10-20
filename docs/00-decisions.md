# 00 — Quyết định và phạm vi

## 0.1. Mục tiêu

Xây **Multi-Agent Orchestrator** hỗ trợ Root Agent chủ động phân việc cho Subagent, mỗi agent có cấu hình model, prompt, tool permissions, skill catalog và session riêng. Tất cả chạy trong một **Shared Runtime** Rust, lưu trạng thái bền vững với SQLite và hỗ trợ nhiều provider thông qua crate `llm`.

## 0.2. Các quyết định đã chốt

| ID | Quyết định |
|---|---|
| D-01 | Modular monolith bằng Rust; Tokio là async runtime. |
| D-02 | Dùng crate `llm` làm thư viện truy cập model; bọc bởi module `LlmGateway` nhỏ. |
| D-03 | SQLx + SQLite (WAL, foreign keys, busy timeout), không yêu cầu Redis/queue broker trong MVP. |
| D-04 | **Root/primary** quyết định phân việc, đánh giá kết quả và tự chọn có gọi tiếp Subagent không. |
| D-05 | **Subagent** chạy trong child session; mọi agent dùng chung Agent Runtime. |
| D-06 | Giao việc bằng tool `subagent(agent, description, prompt, session_id?)`. |
| D-07 | Mặc định Subagent trả **final text** trong tool result cùng child session ID. Không ép JSON nghiệp vụ. |
| D-08 | **Không bắt buộc** ResultValidator ngữ nghĩa, Reviewer Agent, TaskContract hay auto-retry vì thiếu format. |
| D-09 | Kiểm tra kỹ thuật vẫn bắt buộc: permissions, session ownership, tool input schema, trạng thái, giới hạn tài nguyên. |
| D-10 | Root có thể tiếp tục child session cũ bằng `session_id`; không sao chép toàn bộ Root history sang child. |
| D-11 | Prompt gồm base/agent/project instructions, skill catalog và session history. |
| D-12 | Skill discovery theo metadata; toàn bộ `SKILL.md` nạp qua `skill(id)` khi agent cần, trong đúng session. |
| D-13 | Foreground delegation làm trước; background, parallel children, nested depth >1 và distributed sau MVP. |

## 0.3. MVP bao gồm

- Axum endpoint gửi message và nhận kết quả; streaming là tối ưu hoá sau khi text/tool calling đã chạy được.
- Registry định nghĩa Root/Subagent, model, tools, skills, permissions.
- Prompt Builder, project instructions, skill catalog, `skill` loader.
- LLM tool-calling loop có persist messages/tool call/tool result.
- `subagent` tạo child session, chạy foreground, trả final text, tiếp tục session cũ.
- Các lỗi kỹ thuật và resource budgets cơ bản.
- SQLite migrations, một active run/session, crash-safe transcript writes, tracing tối thiểu.

## 0.4. Không thuộc MVP

- Semantic validation bắt buộc, auto-repair kết quả nghiệp vụ.
- Agent-to-agent peer messaging trực tiếp; Redis, Kafka, remote workers.
- Arbitrary nested subagents; mặc định depth = 1.
- Durable background notification pipeline, scheduling graph, distributed HA, hot reload skill, automatic compaction.
- Agent auto-execute nội dung shell/code bên trong skill mà không qua tool permission.

## 0.5. Các tham số **mặc định đề xuất**, cần benchmark

| Tham số | Đề xuất | Diễn giải |
|---|---:|---|
| `max_subagent_depth` | 1 | Root depth 0, child depth 1; child không spawn tiếp. |
| `max_steps_per_run` | 20 | Mỗi LLM/tool cycle tính theo quy tắc runtime thống nhất. |
| `max_child_steps` | 15 | Giới hạn child run. |
| `max_concurrent_children` | 1 | Foreground MVP thực hiện một child tại một thời điểm trên mỗi Root run; song song sau MVP. |
| `child_timeout_s` | 120 | Timeout tính theo đồng hồ tường (wall-clock). |
| `max_tool_output_bytes` | 50 KiB | Giới hạn phần text đưa vào model; raw result giữ trong SQLite tới hard size cap. |
| `max_tool_output_lines` | 2.000 | Giới hạn phần text đưa vào model, không giới hạn child session. |
| `max_children_per_root_run` | 10 | Tránh vòng phân việc vô hạn. |

## 0.6. Định nghĩa thành công

Một yêu cầu đi qua Root (provider A), Root gọi `subagent` (provider B), child dùng `skill` và tool đọc file, trả final text, Root nhận tool result, có thể gọi child lần nữa với cùng session ID, rồi trả câu trả lời cuối. Restart process vẫn đọc được parent/child histories đúng thứ tự.

## 0.7. Điều cần xác minh trước implementation

- Feature, tool-call và streaming support thực tế của phiên bản `llm` khoá trong `Cargo.lock`.
- Cách crate `llm` biểu diễn assistant tool call/result theo từng provider; không tự đoán API chưa kiểm chứng.
- Token usage, model capabilities, prompt/system support, cancellation/error classes.
- SQLite concurrency và thời gian migrate trên môi trường triển khai.
