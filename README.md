# Orchestrator Agent RS — Design Documents (v5.0, Embeddable Library)

Tài liệu thiết kế **Rust library nhúng vào server hiện có**: shared Root/Subagent AgentRuntime, Tokio của host, crate `llm`, SQLx/SQLite và immutable local ArtifactStore. Host sở hữu transport/AuthN và đăng ký domain tools qua `HostTool`; core không phụ thuộc Axum.

Bắt đầu ở [docs/README.md](docs/README.md), [architecture](docs/01-architecture.md), [ADRs](docs/13-adrs.md), [artifact workflows](docs/17-agent-results-artifacts.md), [public library contract](docs/18-embeddable-library.md). [Review report](docs/research/design-review-report.md) là historical snapshot trước khi chốt v5.0.

v5.0 chốt ở mức **thiết kế**: idempotent parent-invocation → child-turn, final message lineage của run attempt, scoped artifact reads, safe owner shutdown handoff, active-turn compaction và periodic canonical snapshot reconciliation. Không có source code Rust hay provider/migration/fault-test evidence được xác nhận trong repo tại thời điểm cập nhật.
