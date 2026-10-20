# Orchestrator Agent RS — Design Documents (v2)

Tài liệu thiết kế cho Rust + Tokio + crate `llm` + SQLx/SQLite; Shared Runtime + Parent–Child Sessions theo hướng OpenCode.

**Bắt đầu tại [docs/README.md](docs/README.md)** để xem 15 mục thiết kế, migration SQLite và cấu hình mẫu.

Bản v2 đã áp dụng Ponytail Review + Codebase Design: cắt abstraction chưa cần cho MVP, xác định một transcript writer, bỏ bảng tasks trùng lặp, đồng bộ SQLite session/run states và không tự replay side-effect tool sau crash.

Đây là tài liệu thiết kế, chưa phải source code triển khai.
