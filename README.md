# Orchestrator Agent RS — Design Documents (v4.1 — Streaming Review)

Tài liệu thiết kế cho Rust + Tokio + crate `llm` + SQLx/SQLite; Shared Runtime + Parent–Child Sessions theo hướng OpenCode.

**Bắt đầu tại [docs/README.md](docs/README.md)** để xem 17 mục thiết kế, migration SQLite và cấu hình mẫu.

Bản v4.1 giữ session-driven continuous execution của v3 và bổ sung SSE theo dõi Root/Child/Tools, live text/progress, SQLite state revision và snapshot/resync. Không có step cap mặc định, deadline toàn child hay auto-replay mutating tool outcome chưa rõ.

Đây là tài liệu thiết kế, chưa phải source code triển khai. Xem [long-running sessions](docs/15-long-running-sessions.md) để hiểu flow chạy nhiều giờ.

Bản v4.1 đã review và commit, bổ sung [docs/16-streaming-events.md](docs/16-streaming-events.md): SSE observation cho Root/Child, live text/tool progress, session-family snapshot/reconnect, và `sessions.state_revision`. Chỉ stream realtime; SQLite lưu committed state, không lưu mỗi token hay hứa lịch sử SSE replay. Đọc [ADR-014](docs/13-adrs.md#adr-014--streaming-ephemeral-events--durable-session-snapshots).
