# Orchestrator Agent RS — Design Documents (v5.0, Embeddable Library)

Tài liệu thiết kế **Rust library nhúng vào server hiện có**: shared Root/Subagent AgentRuntime, Tokio của host, crate `llm`, SQLx/SQLite và immutable local ArtifactStore. Host sở hữu transport/AuthN và đăng ký domain tools qua `HostTool`; core không phụ thuộc Axum.

Bắt đầu ở [docs/README.md](docs/README.md), [architecture](docs/01-architecture.md), [ADRs](docs/13-adrs.md), [artifact workflows](docs/17-agent-results-artifacts.md), [public library contract](docs/18-embeddable-library.md). [Review report](docs/research/design-review-report.md) là historical snapshot trước khi chốt v5.0.

v5.0 chốt ở mức **thiết kế**: idempotent parent-invocation → child-turn, final message lineage của run attempt, scoped artifact reads, safe owner shutdown handoff, active-turn compaction và periodic canonical snapshot reconciliation. Không có source code Rust hay provider/migration/fault-test evidence được xác nhận trong repo tại thời điểm cập nhật.

## Chạy example live có log

Điền `G0_OPENAI_BASE_URL`, `G0_OPENAI_MODEL`, `G0_OPENAI_API_KEY` trong `.env.live`
(xem `.env.live.example`), rồi chạy từ root repo:

```sh
cargo run --example live_flow
# Chỉ định file cấu hình và thư mục lưu kết quả:
cargo run --example live_flow -- --env-file .env.live --output-dir target/live-flow
```

[Example live_flow](examples/live_flow.rs) gọi model thật qua public `Orchestrator`:
Root giao việc cho analyst → analyst gọi `lookup_code` có progress → Root trả lời →
Root hỏi tiếp cùng child session. Dữ liệu tra cứu của HostTool là fixture cục bộ.
Cấu hình provider chỉ lấy từ file được chọn (`--env-file`, `LIVE_ENV_FILE`, mặc định `.env.live`);
các biến `G0_OPENAI_*` của shell không ghi đè file trong example này.

Terminal và `target/live-flow/<UUID>/events.jsonl` ghi từng dòng JSON gồm thời gian đã chạy,
session/run ID, tool lifecycle/progress, text delta, snapshot SQLite và kết quả cuối.
Thư mục UUID được in ở dòng `startup`, chứa cả `store/` và prompts để kiểm tra sau chạy.
API key được che trong log; log vẫn chứa nội dung hội thoại demo.
Stream là best-effort: example subscribe trước snapshot, resubscribe khi lag và refresh snapshot định kỳ.
Exit 0 khi cả hai lượt completed, có mã tra cứu đúng, tool đã chạy và child session được tái sử dụng;
lỗi/timeout trả exit khác 0, shutdown vẫn được gọi. Mỗi lượt chờ tối đa 240 giây.

## Example: video → transcript → tóm tắt → Markdown

[Example video_summary](examples/video_summary.rs) chạy flow hoàn chỉnh qua `Orchestrator`
với root chỉ điều phối và hai subagent:

- `coordinator` (root): chỉ có tool `subagent`.
- `explorer`: `get_transcript` (yt-dlp) → tóm tắt + điểm chính.
- `writer`: `markdown_create` → `markdown_read` xác minh.

Tool Markdown: `markdown_create`, `markdown_append_section`, `markdown_read` (tên file bị giới hạn trong thư mục output, không ghi đè file có sẵn).

```sh
brew install yt-dlp ffmpeg   # yt-dlp cần để tải phụ đề
cargo run --example video_summary -- "<URL>" --lang en
# Tùy chọn: --lang vi,en  --env-file .env.live  --output-dir target/video-summary  --json
cargo test --example video_summary   # unit test parse VTT / tool Markdown
```

Terminal hiển thị stream dễ đọc, mỗi dòng gắn tag nguồn: `[root]` hoặc `[sub:<8 ký tự session_id>]`; `--json` in nguyên dòng JSONL.
Kết quả: `target/video-summary/<UUID>/markdown/summary.md` và `events.jsonl`. Dùng cấu hình `G0_OPENAI_*` như `live_flow`.
Lưu ý: phụ đề auto-dịch hay bị YouTube trả 429 nên tool ưu tiên bản gốc `<lang>-orig`; video không có phụ đề sẽ báo lỗi `NO_TRANSCRIPT`.
Model nhỏ làm root có thể gọi `subagent` sai tham số; xem `tool.failed` trong log nếu flow không hội tụ.
