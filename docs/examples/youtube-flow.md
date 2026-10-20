# YouTube end-to-end flow: URL → transcript → summary → Markdown

## Mục tiêu và seams

Use case kiểm thử qua API công khai của library: `Orchestrator::create_session/submit/result/history/open_artifact` và host-owned `HostTool`. Không thêm logic YouTube vào runtime hoặc expose internal SessionStore/ArtifactStore.

- `fetch_youtube_transcript`: gọi **yt-dlp subprocess** (không qua shell), tải phụ đề tự có / tự động ở `vi`, `en`, `en-US` theo thứ tự ưu tiên, parse WebVTT thành plain text; xuất bản `youtube-transcript.txt` (intermediate artifact) và trả transcript trong tool result.
- LLM đọc transcript rồi tóm tắt tiếng Việt thành nội dung Markdown.
- `publish_youtube_markdown`: bắt buộc có `transcript_artifact_id` từ bước fetch, đọc xác minh artifact theo scoped capability trước khi xuất bản `youtube-summary.md` (primary). Root báo thành công sau khi tool publish xác nhận artifact ID.
- Host đọc lại artifact qua API public (size/SHA-256 verification) rồi ghi file Markdown cục bộ; model không được cung cấp đường dẫn file cần ghi.

## Chạy integration test offline

```bash
cargo test --test youtube_flow -- --nocapture
scripts/check.sh full
```

Test sử dụng `ScriptedGateway` và một **executable yt-dlp giả lập** tạo WebVTT cố định, nhưng mọi HostTool, run admission, SQLite, tool result persistence, artifact publish/open đều đi qua Orchestrator thật. Test chạy trên Unix/Linux; không tải video và không cần API key. Các scenario:

1. URL → subprocess caption fixture → parsed transcript → model nhìn thấy transcript → model gọi publish Markdown → `AgentResult`/artifacts → SHA verified download → reopen store sau shutdown không chạy lại.
2. Video không có phụ đề: tool trả `NO_TRANSCRIPT`, không publish summary, Root phải thông báo không thể tóm tắt.
3. URL không thuộc allowlist: `INVALID_URL`, subprocess chưa được chạy, không có artifact.

Kiểm thử này không chứng minh chất lượng summary của model hoặc tính khả dụng của dịch vụ YouTube: phần tóm tắt dùng fake gateway có kết quả định trước.

## Chạy live với YouTube và provider OpenAI-compatible

Điều kiện: Rust 1.89+, Python/yt-dlp bản hiện hành (hoặc yt-dlp binary), truy cập được YouTube, provider có **tool calling** và quyền tạo artifact. Cần video có phụ đề mà yt-dlp đọc được. Dùng video mà bạn có quyền xử lý.

```bash
python3 -m pip install -U yt-dlp
yt-dlp --version
cp .env.live.example .env.live
# Điền G0_OPENAI_BASE_URL, G0_OPENAI_MODEL và G0_OPENAI_API_KEY
cargo run --example youtube_flow -- "https://www.youtube.com/watch?v=YOURVIDEOID" output.md
cat output.md
```

Hoặc export biến môi trường trực tiếp. Có thể dùng `YTDLP_BIN=/absolute/path/to/yt-dlp` và `LIVE_ENV_FILE=/path/to/.env.live`. Video ID phải gồm đúng 11 ký tự. CLI từ chối kết quả nếu run không completed **hoặc** không có primary artifact Markdown, kể cả model đã nói xong. Tool không tải audio/video.

## Giới hạn và lỗi có chủ đích

- URL chỉ cho phép HTTPS YouTube watch hoặc youtu.be với video ID hợp lệ (không playlist, URL tùy ý, file URI hoặc localhost).
- yt-dlp chạy với `--skip-download`, `--no-playlist`, `--write-subs`, `--write-auto-subs`, `--sub-format vtt`, `--ignore-config`; không gọi shell, process được kill khi Future bị drop; timeout per-tool theo runtime.
- Không có subtitle => `NO_TRANSCRIPT` (chưa triển khai nhánh tải audio và gọi host-provided ASR). yt-dlp lỗi/không có binary => `YTDLP_FAILED` / `YTDLP_UNAVAILABLE`. Không tự dựng summary giả.
- Giới hạn: raw VTT 1 MiB; transcript đã làm sạch 128 KiB; Markdown 256 KiB. Ví dụ nâng `tool_output_cap_bytes` lên 512 KiB để tránh model nhận transcript bị cắt âm thầm. Video dài hơn sẽ trả `TRANSCRIPT_TOO_LARGE`; chunking là bước mở rộng riêng.
- Workflow tương tác LLM nên provider live có thể không tuân prompt; test fixture chứng minh đường đi hệ thống, không bảo đảm hành vi của một model bất kỳ.
- Subagent không cần thiết cho use case **một** video. Khi chuyển sang playlist, Root có thể delegate theo video vào child session riêng, giữ chung 2 HostTools, checkpoint/manifest theo `docs/17-agent-results-artifacts.md`.

## Phạm vi kiểm chứng

| Thành phần | Offline integration | Live example |
|---|---|---|
| Orchestrator + SQLite | Thật | Thật |
| HostTool + ArtifactStore | Thật | Thật |
| yt-dlp process | Executable fixture | yt-dlp thật |
| Captions | WebVTT fixture | Subtitles trên YouTube |
| LLM | `ScriptedGateway` | Provider cấu hình trong `.env.live` |
| Report | SHA-verified Markdown | SHA-verified Markdown + file cục bộ |

Chạy live là thao tác opt-in, không thực thi trong test suite mặc định hoặc CI do phụ thuộc mạng, chi phí provider và trạng thái phụ đề/video bên ngoài.
