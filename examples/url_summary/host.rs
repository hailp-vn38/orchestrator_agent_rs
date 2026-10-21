//! Tiện ích phía Host: đọc file cấu hình `.env.live`, ghi log JSONL/terminal và định nghĩa prompt.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;
use std::time::Instant;

use serde_json::{json, Value};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Root chỉ điều phối và trình bày kết quả cuối: giao việc khám phá HTML cho subagent `explorer`.
pub const COORDINATOR_PROMPT: &str = "\
Bạn là agent điều phối (coordinator). Bạn KHÔNG có tool tải web trực tiếp, chỉ có tool `subagent`.
Khi người dùng yêu cầu tóm tắt hoặc hỏi thông tin từ một URL:
1. Gọi tool `subagent` với đúng 3 tham số:
   - `agent`: \"explorer\"
   - `description`: nhãn mô tả ngắn nhiệm vụ (bắt buộc)
   - `prompt`: yêu cầu `explorer` tải và bóc tách toàn bộ thông tin từ URL của người dùng (ghi rõ URL).
2. Dựa trên báo cáo chi tiết mà `explorer` trả về, hãy trình bày bản tóm tắt cuối cùng bằng tiếng Việt gồm:
   - **Tiêu đề & Nguồn**
   - **Tóm tắt chính**
   - **Các ý chính** (dạng gạch đầu dòng)";

/// Subagent `explorer` giữ toàn bộ context nặng (HTML, metadata, bài viết, danh sách tin) trong child session.
pub const EXPLORER_PROMPT: &str = "\
Bạn là subagent `explorer` chuyên khám phá và bóc tách nội dung trang web từ HTML.
Với URL được giao:
1. Gọi `fetch_html` để tải trang HTML và xem thống kê cấu trúc trang (`article_tag_count`, `title`, `description`).
2. Phân biệt 2 loại trang:
   - **Trường hợp A — Trang bài viết đơn lẻ** (URL bài báo cụ thể, hoặc `article_tag_count` nhỏ):
     Gọi `html_extract_metadata` và `html_extract_article` để lấy toàn văn bài viết. Nếu bài rất ngắn (infographic), gọi `html_extract_links(scope='article')` để xem bài liên quan.
   - **Trường hợp B — Trang chuyên mục / danh sách tin** (ví dụ `https://vnexpress.net/thoi-su` có nhiều thẻ `<article>` chứa danh sách tin):
     a) Gọi `html_extract_feed` trên URL chuyên mục để lấy danh sách các bài báo (`title`, `url`, `snippet`).
     b) Chọn 3 bài tiêu biểu nhất trong danh sách và gọi `html_extract_article` trên từng `url` bài viết con đó để lấy nội dung chi tiết từng tin.
3. Trả về báo cáo đầy đủ bằng tiếng Việt cho coordinator gồm tiêu đề, nguồn, link và nội dung chi tiết đã trích xuất.";

pub fn read_config(path: &Path) -> Result<HashMap<String, String>> {
    let text = std::fs::read_to_string(path)?;
    Ok(text
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            let value = value.trim();
            let value = value
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
                .unwrap_or(value);
            Some((key.trim().into(), value.into()))
        })
        .collect())
}

pub struct Log {
    file: std::fs::File,
    start: Instant,
    secret: String,
    pretty: bool,
    last_text_tag: Option<String>,
}

impl Log {
    pub fn new(file: std::fs::File, secret: String, pretty: bool) -> Self {
        Self {
            file,
            start: Instant::now(),
            secret,
            pretty,
            last_text_tag: None,
        }
    }

    fn pretty_print(&mut self, kind: &str, data: &Value) {
        let mut out = io::stdout().lock();
        if kind != "event" {
            let _ = match kind {
                "submit" => writeln!(out, "[prompt] {}", data["prompt"].as_str().unwrap_or("")),
                "result" => writeln!(out, "\n[kết quả] {}", data["status"].as_str().unwrap_or("")),
                "failed" => writeln!(out, "\n[lỗi] {}", data["message"].as_str().unwrap_or("")),
                "verified" => writeln!(
                    out,
                    "[xác minh] đã lưu bản tóm tắt tại {}",
                    data["summary_file"]
                ),
                _ => Ok(()),
            };
            return;
        }
        let payload = &data["payload"];
        let session = data["session_id"].as_str().unwrap_or("");
        let tag = if session == data["root_session_id"].as_str().unwrap_or("") {
            "root".to_string()
        } else {
            format!("sub:{}", session.chars().take(8).collect::<String>())
        };
        let tool = payload["tool_name"].as_str().unwrap_or("?");
        let event_type = data["event_type"].as_str().unwrap_or("");
        if event_type == "text.delta" {
            if self.last_text_tag.as_deref() != Some(tag.as_str()) {
                let _ = write!(out, "\n[{tag}] ");
                self.last_text_tag = Some(tag);
            }
            let _ = write!(out, "{}", payload["delta"].as_str().unwrap_or(""));
            let _ = out.flush();
            return;
        }
        let line = match event_type {
            "tool.started" => {
                let args = &payload["arguments"];
                format!("[{tag}] [tool ▶] {tool} args={args}")
            }
            "tool.progress" => format!(
                "[{tag}] [tool …] {}",
                payload["message"].as_str().unwrap_or("")
            ),
            "tool.completed" => {
                let meta = &payload["result"]["metadata"];
                if meta.is_null() {
                    let preview: String = payload["output"]
                        .as_str()
                        .unwrap_or("")
                        .chars()
                        .take(120)
                        .collect();
                    format!("[{tag}] [tool ✔] {tool} output={preview:?}")
                } else {
                    format!("[{tag}] [tool ✔] {tool} metadata={meta}")
                }
            }
            "tool.failed" => format!(
                "[{tag}] [tool ✘] {tool} ({})",
                payload["error_code"].as_str().unwrap_or("")
            ),
            _ => return,
        };
        self.last_text_tag = None;
        let _ = writeln!(out, "\n{line}");
    }

    pub fn write(&mut self, kind: &str, data: Value) -> io::Result<()> {
        let line =
            json!({"elapsed_ms": self.start.elapsed().as_millis(), "kind": kind, "data": data})
                .to_string()
                .replace(&self.secret, "[redacted]");
        writeln!(self.file, "{line}")?;
        self.file.flush()?;
        if self.pretty {
            self.pretty_print(kind, &data);
        } else {
            println!("{line}");
        }
        Ok(())
    }
}
