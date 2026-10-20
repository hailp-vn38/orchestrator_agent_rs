# Orchestrator Agent RS — Design Documents (v5.0, Embeddable Library)

Tài liệu thiết kế **Rust library nhúng vào server hiện có**: shared Root/Subagent AgentRuntime, Tokio của host, crate `llm`, SQLx/SQLite và immutable local ArtifactStore. Host sở hữu transport/AuthN và đăng ký domain tools qua `HostTool`; core không phụ thuộc Axum.

Bắt đầu ở [docs/README.md](docs/README.md), [architecture](docs/01-architecture.md), [ADRs](docs/13-adrs.md), [artifact workflows](docs/17-agent-results-artifacts.md), [public library contract](docs/18-embeddable-library.md). [Review report](docs/research/design-review-report.md) là historical snapshot trước khi chốt v5.0.

Baseline v5.0 có mã Rust cho M0–M7, test fake gateway, SQLite và ArtifactStore; các smoke test live được chạy tường minh và không phải chứng cứ multi-provider parity nếu chưa được xác nhận.

## Video transcript → Markdown example

Có ví dụ host end-to-end `examples/youtube_flow.rs` dùng `yt-dlp` để lấy phụ đề YouTube, Orchestrator gọi LLM tóm tắt và publish Markdown artifact. Xem [hướng dẫn offline/live](docs/examples/youtube-flow.md) và test `tests/youtube_flow.rs`.
