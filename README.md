# Orchestrator Agent RS — Design Documents (v3 — Continuous Sessions)

Tài liệu thiết kế cho Rust + Tokio + crate `llm` + SQLx/SQLite; Shared Runtime + Parent–Child Sessions theo hướng OpenCode.

**Bắt đầu tại [docs/README.md](docs/README.md)** để xem 16 mục thiết kế, migration SQLite và cấu hình mẫu.

Bản v3 giữ deep modules của v2 và bổ sung session-driven continuous execution theo OpenCode: không có step cap mặc định, không có deadline toàn child, auto compaction có checkpoint và bounded startup recovery không replay side effects không rõ kết quả.

Đây là tài liệu thiết kế, chưa phải source code triển khai. Xem [long-running sessions](docs/15-long-running-sessions.md) để hiểu flow chạy nhiều giờ.
