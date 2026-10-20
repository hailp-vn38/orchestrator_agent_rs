# Final review — issue #1 (2026-10-21)

Base revision: `c3b2004` + thay đổi chưa commit (fmt `examples/playlist_notes.rs`, `tests/playlist_live.rs`; export `ClientGateway` trong `test_support`; `tests/m9_live_gateway.rs`; sửa flaky ở `tests/m1_single_agent.rs`).
Toolchain/crates: theo `Cargo.lock` (`llm =1.3.8`).

## Deterministic (fake LLM + SQLite/filesystem thật)

| Lệnh | Kết quả |
|---|---|
| `scripts/check.sh full` | ALL OK (fmt, clippy, clippy-lib, test, examples, doctest) |
| `cargo test` | 0 fail; test live hiển thị `ignored`, không tính passed |

Sửa trong lần review: `crash_window_claim_without_spawn_is_rescheduled_in_live_process` flaky (sweep 50ms đua với assertion "chưa có task" khi máy bận) → sweep riêng 1s cho test này; 6/6 lần chạy riêng đạt, check full đạt sau sửa.

## Live (OpenAI-compatible LAN, cấu hình `.env.live`, chạy `--ignored`)

| Test | Kết quả |
|---|---|
| m1_live_provider | PASS |
| m2_live_provider | PASS |
| m3_live_provider | PASS |
| m4_live_provider | PASS |
| m5_live_provider (cancel, request timeout, shutdown/restart) | PASS 3/3 |
| m6_live_provider | PASS 2/2 |
| m7_live_provider | PASS |
| m8_live_hard_flow | PASS |
| m8_live_max_tokens | PASS 3/3 |
| m9_live_gateway (mới): system precedence | PASS |
| m9_live_gateway: model dùng nội dung tool result | PASS |
| m9_live_gateway: key sai → `Auth` / `PROVIDER_AUTH_FAILED`, không lộ secret | PASS |

Hoàn tất các kiểm chứng G0 còn thiếu: system precedence, semantic tool-result consumption, request-level timeout (m5), live error mapping.

## Bảo mật

`git grep` mẫu `sk-…`/`Bearer …`: không có kết quả. `.env.live` nằm trong `.gitignore`; chỉ `.env.live.example` được track.

## Giới hạn còn lại (ghi nhận, không chặn đóng theo quyết định của người dùng)

- **AT-48 provider thứ hai**: bỏ qua theo quyết định của người dùng (không có API key). Chỉ chứng minh trên một gateway OpenAI-compatible, không chứng minh upstream OpenAI trực tiếp hay Anthropic/Gemini. GAP-04/AT-48 **không** được coi là PASS đầy đủ.
- **Operational profile**: `spikes/g0/src/profile.rs` vẫn là giá trị estimated (trừ migration/contention đã đo); chưa đo storage/disk reserve/download/quota. Knob M6 (`event_capacity` 4096, delta 4 KiB, progress 200ms) là estimated.
- Chưa có Axum adapter nên không có evidence HTTP 202/SSE wire.
- Mid-stream failure chỉ có evidence fault harness, không phải live.
