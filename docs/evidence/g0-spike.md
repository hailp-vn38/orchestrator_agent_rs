# G0 evidence (GAP-04, issue #1)

- Ngày: 2026-10-20 (UTC+7) · Base revision khi chạy evidence: `d92d426ff70fa516d82b588f821c612bddd6ea7a` + spike trong working tree tại thời điểm đo (`spikes/g0/`).
- Toolchain: rustc/cargo 1.98.1 · macOS · crate `llm =1.3.8` (pin `=`; `spikes/g0/Cargo.lock`).
- Features: `llm` = `openai, anthropic, rustls-tls` (không bật `default-tls`/`cli`); tokio, sqlx 0.8 (sqlite), sha2, tokio-util.

## Kết luận: **G0 HOÀN THÀNH — chấp thuận ngoại lệ, được chuyển sang M0**

Ngày 2026-10-20 (UTC+7), người dùng yêu cầu “đánh dấu G0 hoàn thành” sau khi đã được thông báo provider thứ hai và operational limits chưa đạt. G0 được đóng theo chấp thuận ngoại lệ này và không còn chặn M0; không thay đổi kết quả kiểm thử thành PASS.

- [x] G0 hoàn thành theo quyết định của người dùng.
- [x] Compile và fake/fault 12/12 đạt; OpenAI-compatible LAN smoke 6/6 đạt.
- [ ] Provider thứ hai: chưa đạt; ngoại lệ cho G0, vẫn cần evidence cho multi-provider/final MVP.
- [ ] Operational profile đầy đủ: chưa kiểm chứng; ngoại lệ cho G0, vẫn phải validate ở các mốc liên quan/final MVP.
- [ ] Các kiểm tra bổ sung system precedence, tool-result consumption, request-level timeout và live error mapping: giữ trạng thái chưa kiểm chứng như phần dưới.

Đánh giá kỹ thuật theo tiêu chí gốc vẫn **NO-GO** vì chưa có cả hai provider smoke và operational profile đầy đủ. Quyết định hoàn thành G0 là chấp thuận ngoại lệ, không phải xác nhận GAP-04/AT-48 được kiểm chứng toàn bộ. Gateway routing/model alias không tự chứng minh upstream OpenAI trực tiếp hoặc provider diversity. Các acceptance bắt buộc của M0–M7/final MVP giữ nguyên.

## 1. Compile — PASS

| Lệnh (`spikes/g0`) | Kết quả |
|---|---|
| `cargo build --all-targets` | exit 0 |
| `cargo clippy --all-targets` | exit 0, 0 warning |

Đã kiểm chứng bằng compile:
- Stream thật của `llm`: `Pin<Box<dyn Stream<Item = Result<StreamChunk, LLMError>> + Send>>` từ `chat_stream_with_tools`.
- `StreamChunk::Done { stop_reason }` **không mang usage** → `ModelEvent::Finished.usage` luôn `None` ở đường streaming-with-tools (cần ghi estimated, hoặc ADR).
- Owner lock: dùng `std::fs::File::try_lock` (std ≥1.89). `fs4 1.1.0` đổi API (không còn `fs_std`); không cần dependency này.
- Tool args được `llm` tự ghép (`ToolUseInputDelta` → `ToolUseComplete`).

## 2. Fake/fault (nhãn: injection, KHÔNG phải live evidence) — PASS 12/12

`cargo test -- --test-threads=1`: 12 passed.
- Gateway: thứ tự event + Finished; lỗi sau `ToolCallReady` → không trả call nào; stream cụt không `Done` → lỗi; args JSON sai bị từ chối; cancel/timeout; mapping lỗi không rò secret (kể cả body 401/429/400).
- Store: migrate 5 bảng ~1 ms; WAL + FK bật; 32 claim đồng thời → đúng 1 thắng (unique partial index, ~5 ms); close/reopen đọc được; owner lock độc quyền và nhả khi drop.
- Profile: mặc định hợp lệ; 11 biên không hợp lệ bị từ chối; biên hợp lệ chấp nhận; page clamp.

Giới hạn: contention chỉ 1 process, 32 task; chưa đo storage limits/disk reserve thật, chưa đo compile time chính thức.

## 3. Provider thật — một OpenAI-compatible target PASS; chưa đủ toàn bộ tiêu chí kỹ thuật gốc

| Provider | Backend/model | Endpoint | Kết quả |
|---|---|---|---|
| OpenAI-compatible | `OpenAI` adapter / `gh/gpt-4o-mini` | `http://192.168.1.157:20128/v1` | **PASS 6/6 smoke checks**, qua gateway LAN; không phải chứng cứ upstream trực tiếp |
| Provider 2 | `Anthropic` / `claude-sonnet-4-5` | `https://api.anthropic.com` (thực tế) | **FAIL** `llm_auth` (401 invalid x-api-key) mọi bước; tool-result-continuation NOT_RUN |

Nguyên nhân provider 2: biến môi trường `ANTHROPIC_*` trỏ tới gateway bên thứ ba (`agentrouter.org`), nhưng backend Anthropic của `llm 1.3.8` **hard-code** `https://api.anthropic.com/v1/messages` và bỏ qua `base_url`. Key proxy bị Anthropic từ chối. Đây chưa phải bằng chứng về capability của model.

Capability matrix hiện có một target, chưa đủ hai providers của AT-48:

| Capability | `OpenAI` adapter / `gh/gpt-4o-mini` qua gateway LAN | Evidence |
|---|---|---|
| Text streaming + Finished | PASS | 3 text deltas, `end_turn`; system instruction được gửi nhưng chưa có test riêng phân biệt system precedence |
| Multi-turn history | PASS | Recall đúng code word từ history, 4 deltas |
| Streamed tool-call assembly | PASS | 1 `get_weather` call, ID dài 29 ký tự, 6 argument fragments, `tool_use` |
| Tool-result continuation | PASS | Replay call/result cùng ID, server chấp nhận và trả text không rỗng, 14 deltas; chưa assert nội dung temperature cụ thể |
| Cancellation | PASS trong stream collection | Token cancel sau 300 ms; không chứng minh upstream operation đã dừng/rollback |
| Timeout | PASS trong stream collection | Deadline 20 ms trả `llm_timeout`; không bao trùm bước tạo stream/đợi response headers |
| Token usage | Không có ở normalized streaming API | `llm 1.3.8` `StreamChunk::Done` không mang usage; không có measured token count |
| Mid-stream failure | Fake/fault PASS, live NOT_RUN | Không gán injection thành live-service evidence |
| Auth/error mapping với gateway này | Live NOT_RUN | Mapping được kiểm tra trong fake tests; chưa chủ động gửi credentials sai |

Lần chạy LAN ngày 2026-10-20 (UTC+7): `cargo run --locked -- live openai`, với `G0_OPENAI_BASE_URL` và `G0_OPENAI_MODEL` như bảng, key chỉ truyền qua process environment. Exit 0, smoke elapsed **5.584 ms** (tức 5,584 giây). GET `/v1/models` trả HTTP 200 trước khi chọn model. Chỉ `openai` target được chạy; provider 2 giữ nguyên kết quả lịch sử, không được chạy lại bằng credentials môi trường khác.

Redacted output: [g0-openai-lan.log](g0-openai-lan.log). Không lưu key hoặc response body. Chạy lại fake/fault tests **12 passed**, clippy `--locked --all-targets -- -D warnings` exit 0.

Runner hỗ trợ `live [openai|anthropic]` để chọn target, trả exit 1 nếu check fail/blocked. `ALL_PASS` khi có selector chỉ nói về target được chọn, không thay kết luận G0. Đã bỏ debug raw-provider error output khỏi gateway để tránh raw body chứa secrets.

Phát hiện phụ (sửa trong spike): `llm` báo HTTP 401 Anthropic là `ResponseFormatError` → ban đầu bị map thành `llm_protocol`; đã map theo status (401/403→auth, 429/5xx→provider, 400…→invalid).
Lưu ý bảo mật: `LLMError::ResponseFormatError` chứa raw body; gateway không được log/lưu nguyên văn.

## 4. Operational profile — ĐỀ XUẤT, chưa chốt

Giá trị/đơn vị nằm trong `spikes/g0/src/profile.rs` (estimated, trừ migration/contention là measured). Chưa đo storage limits, download, quotas → chưa đủ điều kiện GO.

## Kiểm chứng còn thiếu sau khi G0 được chấp thuận hoàn thành

1. OpenAI-compatible LAN smoke đã chạy; bổ sung kiểm tra system precedence, semantic tool-result consumption và request-level timeout (bao gồm stream creation). Nếu cần claim upstream OpenAI trực tiếp, cần endpoint/credentials và smoke tương ứng; gateway alias hiện tại chưa đủ chứng minh claim đó.
2. Provider 2 đi được qua `llm`: key Anthropic thật `G0_ANTHROPIC_API_KEY` (+`G0_ANTHROPIC_MODEL`), **hoặc** dùng backend OpenAI-compatible với `base_url` (gateway hiện có) — cần chọn; hoặc ADR về việc thay/bọc `llm`.
3. Chạy target thứ hai và các kiểm tra còn thiếu, đo storage/disk/quota/download và chốt operational profile; cập nhật capability matrix/evidence trước khi claim acceptance tương ứng ở các mốc sau/final MVP. Các mục này không còn chặn bắt đầu M0 theo ngoại lệ đã được người dùng chấp thuận.

## Kiểm tra trước commit G0

- Chuẩn hóa Rust bằng `cargo fmt`; `cargo fmt --check` đạt.
- `cargo test --locked -- --test-threads=1`: 12 passed, 0 failed.
- `cargo clippy --locked --all-targets -- -D warnings`: đạt.
- Provider LAN smoke dùng evidence đã ghi ở trên; không gọi lại dịch vụ trong bước commit.
- Không đưa build artifacts hoặc credentials vào commit.

## Cập nhật 2026-10-21

Đã chạy bổ sung trên gateway OpenAI-compatible (`tests/m9_live_gateway.rs`, m5): system precedence, semantic tool-result consumption, request-level timeout và live auth error mapping đều PASS. Provider thứ hai được bỏ qua theo quyết định của người dùng (không có API key). Operational profile vẫn estimated. Chi tiết: `docs/evidence/final.md`.
