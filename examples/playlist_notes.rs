//! Flow agent tự chạy trọn vẹn: URL playlist → danh sách video → transcript (yt-dlp) →
//! tóm tắt + sơ đồ → tìm liên hệ → thư mục ghi chú Markdown kiểu Obsidian (`[[link]]`).
//!
//! Chạy: cargo run --example playlist_notes -- <PLAYLIST_URL> [--lang en,vi] [--limit N]
//!                                             [--env-file .env.live] [--output-dir target/playlist-agent]
//! Yêu cầu: `yt-dlp` trong PATH (brew install yt-dlp) và cấu hình G0_OPENAI_* (như live_flow).
//!
//! Agent `coordinator` gọi `list_playlist`, giao từng video cho subagent `explorer`
//! (`get_transcript` + `note_create`), rồi tự tìm liên hệ bằng `note_link` và chốt bằng
//! `index_create`. Host KHÔNG tự tóm tắt hay tự tạo link: cuối cùng chỉ xác minh trên đĩa.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, CallerContext, FeedItem, HostTool, LlmBackend, LlmConfig,
    LlmProviderConfig, Orchestrator, OrchestratorConfig, ProviderAuth, RunStatus, SecretString,
    ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Giới hạn ký tự transcript trả về cho model (lấy mẫu đầu/giữa/cuối nếu dài hơn).
const MAX_TRANSCRIPT_CHARS: usize = 30_000;

fn tool_err(code: &str, message: impl Into<String>) -> ToolError {
    ToolError {
        code: code.into(),
        message: message.into(),
    }
}

fn outcome(ctx: &ToolContext, content: String, metadata: Value) -> ToolOutcome {
    ToolOutcome {
        call_id: ctx.scope().tool_call_id.clone(),
        content,
        metadata,
    }
}

fn str_arg<'a>(args: &'a Value, key: &str) -> std::result::Result<&'a str, ToolError> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| tool_err("INVALID_ARGUMENT", format!("Thiếu hoặc rỗng: {key}")))
}

fn str_list(args: &Value, key: &str) -> Vec<String> {
    args.get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Tiện ích thuần: slug, vtt, mermaid
// ---------------------------------------------------------------------------

/// Bỏ dấu tiếng Việt (không cần crate normalization); ký tự khác giữ nguyên.
fn fold_vietnamese(c: char) -> char {
    const GROUPS: [(&str, char); 9] = [
        ("àáảãạăằắẳẵặâầấẩẫậÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬ", 'a'),
        ("èéẻẽẹêềếểễệÈÉẺẼẸÊỀẾỂỄỆ", 'e'),
        ("ìíỉĩịÌÍỈĨỊ", 'i'),
        ("òóỏõọôồốổỗộơờớởỡợÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢ", 'o'),
        ("ùúủũụưừứửữựÙÚỦŨỤƯỪỨỬỮỰ", 'u'),
        ("ỳýỷỹỵỲÝỶỸỴ", 'y'),
        ("đĐ", 'd'),
        ("", ' '),
        ("", ' '),
    ];
    GROUPS
        .iter()
        .find(|(set, _)| set.contains(c))
        .map_or(c, |(_, r)| *r)
}

/// Slug ASCII an toàn cho tên file / wikilink (không chứa `/`, `|`, `]`...).
fn slugify(s: &str, max: usize) -> String {
    let mut out = String::new();
    for c in s.chars().map(fold_vietnamese) {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('-') && !out.is_empty() {
            out.push('-');
        }
    }
    let out: String = out.chars().take(max).collect();
    let out = out.trim_matches('-').to_string();
    if out.is_empty() {
        "untitled".into()
    } else {
        out
    }
}

/// Dấu `|` và `]` làm hỏng alias của wikilink.
fn link_label(title: &str) -> String {
    title.replace(['|', '[', ']'], " ")
}

/// WebVTT → văn bản thuần: bỏ header/timestamp/tag và dòng lặp của auto-sub.
fn vtt_to_text(vtt: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    for raw in vtt.lines() {
        let line = raw.trim();
        if line.is_empty()
            || line == "WEBVTT"
            || line.contains("-->")
            || line.starts_with("Kind:")
            || line.starts_with("Language:")
            || line.starts_with("NOTE")
            || line.chars().all(|c| c.is_ascii_digit())
        {
            continue;
        }
        let mut text = String::new();
        let mut in_tag = false;
        for c in line.chars() {
            match c {
                '<' => in_tag = true,
                '>' => in_tag = false,
                _ if !in_tag => text.push(c),
                _ => {}
            }
        }
        let text = text
            .replace("&amp;", "&")
            .replace("&gt;", ">")
            .replace("&lt;", "<")
            .replace("&#39;", "'")
            .replace("&quot;", "\"")
            .trim()
            .to_string();
        if !text.is_empty() && out.last() != Some(&text) {
            out.push(text);
        }
    }
    out.join(" ")
}

/// Giữ transcript trong ngân sách ký tự bằng cách lấy mẫu đầu / giữa / cuối.
fn clip_transcript(text: &str, max: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max {
        return text.to_string();
    }
    let part = max / 3;
    let mid = chars.len() / 2 - part / 2;
    let seg = |a: usize| chars[a..a + part].iter().collect::<String>();
    format!(
        "{}\n[...cắt...]\n{}\n[...cắt...]\n{}",
        seg(0),
        seg(mid),
        seg(chars.len() - part)
    )
}

/// Bỏ rào ``` và chỉ nhận sơ đồ mermaid thuộc loại được hỗ trợ.
fn clean_mermaid(raw: &str) -> Option<String> {
    let code: Vec<&str> = raw
        .lines()
        .filter(|l| !l.trim_start().starts_with("```"))
        .collect();
    let code = code.join("\n").trim().to_string();
    let first = code.lines().next()?.trim();
    ["flowchart", "graph", "mindmap", "sequenceDiagram", "stateDiagram"]
        .iter()
        .any(|k| first.starts_with(k))
        .then_some(code)
}

/// Tên note: chỉ chữ/số/`-`/`_`, không đường dẫn; trả về stem (không có `.md`).
fn note_stem(name: &str) -> std::result::Result<String, ToolError> {
    let stem = name.trim().trim_end_matches(".md");
    let valid = !stem.is_empty()
        && stem.len() <= 100
        && !stem.starts_with(['-', '_'])
        && stem
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'));
    if valid {
        Ok(stem.to_string())
    } else {
        Err(tool_err(
            "INVALID_FILENAME",
            "tên note chỉ gồm chữ/số/'-'/'_' (không đường dẫn); dùng đúng `note` từ list_playlist",
        ))
    }
}

/// Lấy tiêu đề (dòng `# `) của một note.
fn note_title(doc: &str) -> String {
    doc.lines()
        .find_map(|l| l.strip_prefix("# "))
        .unwrap_or("")
        .trim()
        .to_string()
}

// ---------------------------------------------------------------------------
// Trạng thái dùng chung giữa các tool
// ---------------------------------------------------------------------------

struct Shared {
    /// Thư mục ghi note (đổi tên theo chủ đề khi kết thúc).
    notes_dir: PathBuf,
    /// Thư mục tạm cho phụ đề tải về + transcript đã lưu.
    work_dir: PathBuf,
    langs: String,
    limit: usize,
    /// (id, title, note stem) theo thứ tự `list_playlist` đã trả.
    videos: Mutex<Vec<(String, String, String)>>,
    /// (từ, đến, lý do) các liên kết đã ghi.
    edges: Mutex<Vec<(String, String, String)>>,
    /// (tên chủ đề, URL playlist) khi `index_create` thành công.
    topic: Mutex<Option<(String, String)>>,
}

impl Shared {
    fn path(&self, stem: &str) -> PathBuf {
        self.notes_dir.join(format!("{stem}.md"))
    }
}

// ---------------------------------------------------------------------------
// yt-dlp (blocking; chạy trong spawn_blocking). Không qua shell; URL sau `--`.
// ---------------------------------------------------------------------------

fn http_url(url: &str) -> std::result::Result<(), ToolError> {
    if url.starts_with("http://") || url.starts_with("https://") {
        Ok(())
    } else {
        Err(tool_err("INVALID_URL", "Chỉ hỗ trợ URL http(s)"))
    }
}

fn ytdlp_unavailable(e: io::Error) -> ToolError {
    tool_err(
        "YTDLP_UNAVAILABLE",
        format!("Không chạy được yt-dlp (brew install yt-dlp): {e}"),
    )
}

fn stderr_tail(stderr: &[u8]) -> String {
    let s = String::from_utf8_lossy(stderr);
    let tail: Vec<char> = s.chars().rev().take(600).collect();
    tail.into_iter().rev().collect()
}

fn fetch_playlist(url: &str) -> std::result::Result<Vec<(String, String)>, ToolError> {
    http_url(url)?;
    let out = std::process::Command::new("yt-dlp")
        .args(["--flat-playlist", "--print", "%(id)s\t%(title)s", "--"])
        .arg(url)
        .output()
        .map_err(ytdlp_unavailable)?;
    if !out.status.success() {
        return Err(tool_err("YTDLP_FAILED", stderr_tail(&out.stderr)));
    }
    let list: Vec<(String, String)> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| {
            let (id, title) = l.trim().split_once('\t')?;
            (!id.trim().is_empty()).then(|| (id.trim().to_string(), title.trim().to_string()))
        })
        .collect();
    if list.is_empty() {
        return Err(tool_err("EMPTY_PLAYLIST", "Playlist không có video nào"));
    }
    Ok(list)
}

fn fetch_transcript(work: &Path, id: &str, langs: &str) -> std::result::Result<String, ToolError> {
    // Phụ đề auto-dịch hay bị 429; bản gốc `<lang>-orig` ổn định hơn nên thử trước.
    let sub_langs = langs
        .split(',')
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .flat_map(|l| [format!("{l}-orig"), l.to_string()])
        .collect::<Vec<_>>()
        .join(",");
    let dir = work.join(id);
    std::fs::create_dir_all(&dir).map_err(|e| tool_err("IO", e.to_string()))?;
    let output = std::process::Command::new("yt-dlp")
        .args([
            "--skip-download",
            "--no-playlist",
            "--write-subs",
            "--write-auto-subs",
            "--sub-langs",
            &sub_langs,
            "--sub-format",
            "vtt",
            "-o",
        ])
        .arg(dir.join("%(id)s.%(ext)s"))
        .arg("--")
        .arg(format!("https://www.youtube.com/watch?v={id}"))
        .output()
        .map_err(ytdlp_unavailable)?;
    let mut subs: Vec<PathBuf> = std::fs::read_dir(&dir)
        .map_err(|e| tool_err("IO", e.to_string()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "vtt"))
        .collect();
    // yt-dlp thoát lỗi khi một biến thể bị 429; vẫn dùng nếu đã có file .vtt.
    if subs.is_empty() {
        return Err(if output.status.success() {
            tool_err("NO_TRANSCRIPT", format!("Video không có phụ đề cho: {sub_langs}"))
        } else {
            tool_err("YTDLP_FAILED", stderr_tail(&output.stderr))
        });
    }
    let order: Vec<&str> = sub_langs.split(',').collect();
    subs.sort_by_key(|p| {
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        let rank = order
            .iter()
            .position(|l| name.contains(&format!(".{l}.")))
            .unwrap_or(usize::MAX);
        (rank, name)
    });
    let text = vtt_to_text(
        &std::fs::read_to_string(&subs[0]).map_err(|e| tool_err("IO", e.to_string()))?,
    );
    if text.is_empty() {
        return Err(tool_err("NO_TRANSCRIPT", "Phụ đề rỗng"));
    }
    Ok(text)
}

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Kind {
    ListPlaylist,
    GetTranscript,
    NoteCreate,
    NoteLink,
    IndexCreate,
}

struct PlaylistTool {
    shared: Arc<Shared>,
    kind: Kind,
}

fn schema(props: Value, required: &[&str]) -> Value {
    json!({"type": "object", "properties": props, "required": required, "additionalProperties": false})
}

#[async_trait::async_trait]
impl HostTool for PlaylistTool {
    fn descriptor(&self) -> ToolDescriptor {
        let s = json!({"type": "string"});
        let (name, description, input_schema, effect) = match self.kind {
            Kind::ListPlaylist => (
                "list_playlist",
                "List the videos of a playlist URL with yt-dlp. Returns one line per video: `N. id=<video id> note=<note file name> title=<title>`. Use the `note` value verbatim as the note name.",
                schema(json!({"url": {"type": "string", "description": "http(s) playlist URL"}}), &["url"]),
                ToolEffect::ReadOnly,
            ),
            Kind::GetTranscript => (
                "get_transcript",
                "Download the transcript (subtitles) of one video id with yt-dlp. Returns the plain-text transcript.",
                schema(json!({"id": {"type": "string", "description": "video id from list_playlist"}}), &["id"]),
                ToolEffect::ReadOnly,
            ),
            Kind::NoteCreate => (
                "note_create",
                "Create the Markdown note of one video (frontmatter, summary, key points, structure, mermaid diagram). Fails if the note exists.",
                schema(
                    json!({
                        "note": {"type": "string", "description": "note name from list_playlist, verbatim"},
                        "id": {"type": "string", "description": "video id"},
                        "title": s,
                        "summary": {"type": "string", "description": "3-6 sentence summary"},
                        "key_points": {"type": "array", "items": {"type": "string"}},
                        "structure": {"type": "string", "description": "nested '- ' bullet outline of the video"},
                        "mermaid": {"type": "string", "description": "valid mermaid diagram starting with 'flowchart TD' or 'mindmap'; quote node labels"},
                        "topics": {"type": "array", "items": {"type": "string"}}
                    }),
                    &["note", "id", "title", "summary", "key_points", "structure"],
                ),
                ToolEffect::Mutating,
            ),
            Kind::NoteLink => (
                "note_link",
                "Link two existing notes: adds `[[to]]` under '## Liên quan' of `from` and the back-link in `to`. `from` leads to / is the foundation of `to`.",
                schema(
                    json!({
                        "from": {"type": "string", "description": "note name"},
                        "to": {"type": "string", "description": "note name"},
                        "reason": {"type": "string", "description": "short reason in Vietnamese"}
                    }),
                    &["from", "to", "reason"],
                ),
                ToolEffect::Mutating,
            ),
            Kind::IndexCreate => (
                "index_create",
                "Finish: create 00-index.md (topic title, video list with [[links]], relation map). Call once, after all links.",
                schema(
                    json!({
                        "topic": {"type": "string", "description": "short Vietnamese topic name of the playlist"},
                        "url": {"type": "string", "description": "playlist URL"}
                    }),
                    &["topic", "url"],
                ),
                ToolEffect::Mutating,
            ),
        };
        ToolDescriptor {
            name: name.into(),
            description: description.into(),
            input_schema,
            effect,
        }
    }

    async fn execute(
        &self,
        ctx: ToolContext,
        args: Value,
    ) -> std::result::Result<ToolOutcome, ToolError> {
        let sh = self.shared.clone();
        match self.kind {
            Kind::ListPlaylist => {
                let url = str_arg(&args, "url")?.to_string();
                ctx.progress("Đang lấy danh sách playlist bằng yt-dlp...");
                let mut list = tokio::task::spawn_blocking(move || fetch_playlist(&url))
                    .await
                    .map_err(|e| tool_err("INTERNAL", e.to_string()))??;
                list.truncate(sh.limit);
                let videos: Vec<(String, String, String)> = list
                    .into_iter()
                    .enumerate()
                    .map(|(i, (id, title))| {
                        let stem = format!("{:02}-{}", i + 1, slugify(&title, 50));
                        (id, title, stem)
                    })
                    .collect();
                let text: String = videos
                    .iter()
                    .enumerate()
                    .map(|(i, (id, title, stem))| {
                        format!("{}. id={id} note={stem} title={title}\n", i + 1)
                    })
                    .collect();
                ctx.progress(&format!("{} video", videos.len()));
                *sh.videos.lock().unwrap() = videos;
                Ok(outcome(&ctx, text, json!({})))
            }
            Kind::GetTranscript => {
                let id = str_arg(&args, "id")?.to_string();
                if !sh.videos.lock().unwrap().iter().any(|v| v.0 == id) {
                    return Err(tool_err("UNKNOWN_VIDEO", "id không có trong list_playlist"));
                }
                ctx.progress(&format!("Đang tải transcript {id}..."));
                let (work, langs, i) = (sh.work_dir.clone(), sh.langs.clone(), id.clone());
                let text = tokio::task::spawn_blocking(move || fetch_transcript(&work, &i, &langs))
                    .await
                    .map_err(|e| tool_err("INTERNAL", e.to_string()))??;
                let total = text.chars().count();
                let tdir = sh.work_dir.join("transcripts");
                std::fs::create_dir_all(&tdir).map_err(|e| tool_err("IO", e.to_string()))?;
                std::fs::write(tdir.join(format!("{id}.txt")), &text)
                    .map_err(|e| tool_err("IO", e.to_string()))?;
                ctx.progress(&format!("Đã lấy transcript: {total} ký tự"));
                let body = clip_transcript(&text, MAX_TRANSCRIPT_CHARS);
                Ok(outcome(&ctx, body, json!({"id": id, "chars": total})))
            }
            Kind::NoteCreate => {
                let stem = note_stem(str_arg(&args, "note")?)?;
                let id = str_arg(&args, "id")?;
                let known = sh.videos.lock().unwrap().iter().any(|v| v.0 == id && v.2 == stem);
                if !known {
                    return Err(tool_err(
                        "UNKNOWN_NOTE",
                        "cặp id/note phải khớp một dòng của list_playlist",
                    ));
                }
                let (title, summary) = (str_arg(&args, "title")?, str_arg(&args, "summary")?);
                let topics = str_list(&args, "topics");
                let mut doc = format!(
                    "---\ntitle: \"{}\"\nsource: https://www.youtube.com/watch?v={id}\ntags: [{}]\n---\n\n# {title}\n\n> Nguồn: <https://www.youtube.com/watch?v={id}>\n\n## Tóm tắt\n\n{summary}\n\n",
                    title.replace('"', "'"),
                    topics.iter().map(|t| slugify(t, 30)).collect::<Vec<_>>().join(", "),
                );
                let points = str_list(&args, "key_points");
                if !points.is_empty() {
                    doc.push_str("## Ý chính\n\n");
                    for p in points {
                        doc.push_str(&format!("- {p}\n"));
                    }
                    doc.push('\n');
                }
                doc.push_str(&format!("## Cấu trúc\n\n{}\n\n", str_arg(&args, "structure")?));
                if let Some(m) = args.get("mermaid").and_then(Value::as_str).and_then(clean_mermaid)
                {
                    doc.push_str(&format!("## Sơ đồ\n\n```mermaid\n{m}\n```\n"));
                }
                std::fs::create_dir_all(&sh.notes_dir).map_err(|e| tool_err("IO", e.to_string()))?;
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(sh.path(&stem))
                    .map_err(|e| match e.kind() {
                        io::ErrorKind::AlreadyExists => tool_err("ALREADY_EXISTS", "Note đã tồn tại"),
                        _ => tool_err("IO", e.to_string()),
                    })?;
                f.write_all(doc.as_bytes()).map_err(|e| tool_err("IO", e.to_string()))?;
                Ok(outcome(&ctx, format!("Created {stem}.md"), json!({"note": stem})))
            }
            Kind::NoteLink => {
                let (from, to) = (note_stem(str_arg(&args, "from")?)?, note_stem(str_arg(&args, "to")?)?);
                let reason = str_arg(&args, "reason")?.replace('\n', " ");
                if from == to {
                    return Err(tool_err("INVALID_ARGUMENT", "from và to phải khác nhau"));
                }
                let read = |s: &str| {
                    std::fs::read_to_string(sh.path(s))
                        .map_err(|_| tool_err("NOT_FOUND", format!("Note chưa tồn tại: {s}")))
                };
                let (doc_from, doc_to) = (read(&from)?, read(&to)?);
                let (t_from, t_to) = (note_title(&doc_from), note_title(&doc_to));
                let add = |stem: &str, doc: &str, arrow: &str, other: &str, label: &str| {
                    let line = format!("- {arrow} [[{other}|{}]] — {reason}\n", link_label(label));
                    if doc.contains(&format!("[[{other}|")) {
                        return Ok(());
                    }
                    let mut new = doc.trim_end().to_string();
                    new.push_str(if doc.contains("## Liên quan") { "\n" } else { "\n\n## Liên quan\n\n" });
                    new.push_str(&line);
                    std::fs::write(sh.path(stem), new).map_err(|e| tool_err("IO", e.to_string()))
                };
                add(&from, &doc_from, "→", &to, &t_to)?;
                add(&to, &doc_to, "←", &from, &t_from)?;
                sh.edges.lock().unwrap().push((from.clone(), to.clone(), reason));
                Ok(outcome(&ctx, format!("Linked {from} → {to}"), json!({})))
            }
            Kind::IndexCreate => {
                let (topic, url) = (str_arg(&args, "topic")?, str_arg(&args, "url")?);
                let videos = sh.videos.lock().unwrap().clone();
                let edges = sh.edges.lock().unwrap().clone();
                let missing: Vec<&str> = videos
                    .iter()
                    .filter(|v| !sh.path(&v.2).exists())
                    .map(|v| v.2.as_str())
                    .collect();
                if !missing.is_empty() {
                    return Err(tool_err("MISSING_NOTES", format!("Chưa có note: {}", missing.join(", "))));
                }
                let mut doc = format!("# {topic}\n\n> Playlist: <{url}>\n\n## Các video\n\n");
                for (_, title, stem) in &videos {
                    let note = std::fs::read_to_string(sh.path(stem)).unwrap_or_default();
                    let first = note
                        .split("## Tóm tắt\n\n")
                        .nth(1)
                        .and_then(|s| s.lines().next())
                        .unwrap_or("");
                    doc.push_str(&format!("- [[{stem}|{}]] — {first}\n", link_label(title)));
                }
                doc.push_str("\n## Bản đồ liên hệ\n\n```mermaid\nflowchart LR\n");
                for (i, (_, title, _)) in videos.iter().enumerate() {
                    doc.push_str(&format!("    N{}[\"{}\"]\n", i + 1, title.replace('"', "'")));
                }
                let pos = |s: &str| videos.iter().position(|v| v.2 == s).map(|i| i + 1);
                for (a, b, reason) in &edges {
                    if let (Some(a), Some(b)) = (pos(a), pos(b)) {
                        let r: String = reason.replace('"', "'").chars().take(40).collect();
                        doc.push_str(&format!("    N{a} -->|\"{r}\"| N{b}\n"));
                    }
                }
                doc.push_str("```\n");
                std::fs::write(sh.path("00-index"), &doc).map_err(|e| tool_err("IO", e.to_string()))?;
                *sh.topic.lock().unwrap() = Some((topic.to_string(), url.to_string()));
                Ok(outcome(&ctx, "Created 00-index.md".into(), json!({"links": edges.len()})))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Host: cấu hình, hiển thị event, chạy một turn
// ---------------------------------------------------------------------------

fn read_config(path: &Path) -> Result<HashMap<String, String>> {
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

/// In event tool dạng dễ đọc; gắn tag `root` / `sub:<8 ký tự>` theo session.
fn print_event(data: &Value, secret: &str) {
    let payload = &data["payload"];
    let session = data["session_id"].as_str().unwrap_or("");
    let tag = if session == data["root_session_id"].as_str().unwrap_or("") {
        "root".to_string()
    } else {
        format!("sub:{}", session.chars().take(8).collect::<String>())
    };
    let tool = payload["tool_name"].as_str().unwrap_or("?");
    let line = match data["event_type"].as_str().unwrap_or("") {
        "tool.started" => format!("[{tag}] ▶ {tool}"),
        "tool.progress" => format!("[{tag}] … {}", payload["message"].as_str().unwrap_or("")),
        "tool.completed" => format!("[{tag}] ✔ {tool}"),
        "tool.failed" => format!("[{tag}] ✘ {tool} ({})", payload["error_code"].as_str().unwrap_or("")),
        _ => return,
    };
    println!("{}", line.replace(secret, "[redacted]"));
}

const COORDINATOR_PROMPT: &str = "You coordinate building an Obsidian-style note folder from a \
playlist. Steps, in order: \
(1) call list_playlist with the playlist URL from the user. \
(2) For EACH listed video, one at a time, call the subagent tool with agent=explorer, a short \
description and a prompt holding exactly: 'id: <id>', 'note: <note>', 'title: <title>' copied \
verbatim from the list. Never skip a video. If an explorer reports an ERROR, retry once. \
(3) Using the explorers' summaries, find real relationships between videos (series order such as \
part 1 before part 2, same strategy, one video testing what another built). Call note_link for \
each relationship: `from` is the foundation/earlier video, `to` the one that builds on it; \
use `note` names verbatim and a short Vietnamese reason. Make at least one link, only meaningful \
ones. \
(4) Call index_create once with a short Vietnamese topic name and the playlist URL. \
(5) Reply briefly with the topic and the number of notes and links. \
Subagent calls have ONLY these arguments: agent, description (REQUIRED), prompt; no session_id.";

const EXPLORER_PROMPT: &str = "You write the note of one video. Call get_transcript with the \
given id, then call note_create ONCE with note, id and title exactly as given, plus: summary \
(3-6 sentences), key_points (5-8), structure (nested '- ' outline of the video), mermaid (a \
valid 'flowchart TD' of the video's flow; quote node labels) and topics (3-6 short). Write in \
Vietnamese (keep English technical terms) and use only the transcript. Then reply with \
'DONE: <note>' and a 2-sentence summary. If a tool fails, reply 'ERROR: <reason>' and invent \
nothing.";

async fn run(o: &Orchestrator, url: &str, secret: &str, timeout: Duration) -> Result<()> {
    let caller = CallerContext::new("demo", "playlist-notes");
    let sid = o.create_session(&caller, "coordinator").await?;
    let mut sub = o.subscribe(&caller, &sid).await?;
    let turn = o
        .submit(&caller, &sid, format!("Build the notes folder for this playlist.\nURL: {url}"))
        .await?;
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    let mut refresh = tokio::time::interval(o.snapshot_refresh_interval());
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            item = sub.recv() => match item {
                Some(FeedItem::Event(event)) => print_event(&event.to_json(), secret),
                Some(FeedItem::ResyncRequired) | None => sub = o.subscribe(&caller, &sid).await?,
            },
            _ = refresh.tick() => {
                let result = o.result(&caller, &sid, &turn.turn_id).await?;
                if result.status == RunStatus::Running { continue; }
                println!("\n[kết quả] {}: {}", result.status.as_str(), result.output.unwrap_or_default());
                if result.status != RunStatus::Completed {
                    return Err(io::Error::other(format!("Turn không completed: {:?}", result.error)).into());
                }
                return Ok(());
            },
            _ = &mut deadline => {
                o.cancel(&caller, &sid).await?;
                return Err(io::Error::new(io::ErrorKind::TimedOut, "Quá thời gian; đã cancel").into());
            }
        }
    }
}

/// Xác minh trên đĩa (không tin lời model): đủ note, mọi `[[link]]` trỏ tới file có thật.
fn verify(dir: &Path, expected_notes: usize) -> Result<()> {
    let mut linked = 0;
    let mut notes = 0;
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_none_or(|x| x != "md") {
            continue;
        }
        let doc = std::fs::read_to_string(&path)?;
        let mut rest = doc.as_str();
        while let Some(s) = rest.find("[[") {
            let after = &rest[s + 2..];
            let Some(e) = after.find("]]") else { break };
            let target = after[..e].split('|').next().unwrap_or("").trim();
            if !dir.join(format!("{target}.md")).exists() {
                return Err(io::Error::other(format!(
                    "[[{target}]] trong {} trỏ tới file không tồn tại",
                    path.display()
                ))
                .into());
            }
            rest = &after[e + 2..];
        }
        if path.file_name().is_some_and(|n| n != "00-index.md") {
            notes += 1;
            if doc.contains("## Liên quan") {
                linked += 1;
            }
        }
    }
    if notes != expected_notes {
        return Err(io::Error::other(format!("Có {notes}/{expected_notes} note")).into());
    }
    if linked < 2 {
        return Err(io::Error::other("Cần ≥ 2 note có mục Liên quan").into());
    }
    if !dir.join("00-index.md").exists() {
        return Err(io::Error::other("Thiếu 00-index.md").into());
    }
    println!("[xác minh] {notes} note, {linked} note có liên kết, mọi [[link]] hợp lệ");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut url = None;
    let mut langs = "en,vi".to_string();
    let mut limit = usize::MAX;
    let mut env_file = std::env::var_os("LIVE_ENV_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".env.live"));
    let mut output = root.join("target/playlist-agent");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--lang" | "--env-file" | "--output-dir" | "--limit" => {
                let v = args.next().ok_or_else(|| io::Error::other("Thiếu giá trị option"))?;
                match arg.as_str() {
                    "--lang" => langs = v,
                    "--env-file" => env_file = v.into(),
                    "--limit" => limit = v.parse()?,
                    _ => output = v.into(),
                }
            }
            "--help" | "-h" => {
                println!("cargo run --example playlist_notes -- <PLAYLIST_URL> [--lang en,vi] [--limit N] [--env-file .env.live] [--output-dir target/playlist-agent]");
                return Ok(());
            }
            s if !s.starts_with('-') && url.is_none() => url = Some(s.to_string()),
            _ => return Err(io::Error::other("Option không hợp lệ; dùng --help").into()),
        }
    }
    let url = url.ok_or_else(|| io::Error::other("Thiếu URL playlist; dùng --help"))?;
    let config = read_config(&env_file)?;
    let required = |key: &str| -> Result<String> {
        config
            .get(key)
            .filter(|s| !s.trim().is_empty())
            .cloned()
            .ok_or_else(|| io::Error::other(format!("Thiếu {key} trong file cấu hình")).into())
    };
    let (endpoint, model, key) = (
        required("G0_OPENAI_BASE_URL")?,
        required("G0_OPENAI_MODEL")?,
        required("G0_OPENAI_API_KEY")?,
    );
    let run_dir = output.join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&run_dir)?;
    println!("run_dir: {}", run_dir.display());
    let prompt_file = |name: &str, text: &str| -> Result<PathBuf> {
        let path = run_dir.join(format!("{name}.md"));
        std::fs::write(&path, text)?;
        Ok(path)
    };
    let coordinator_prompt = prompt_file("coordinator", COORDINATOR_PROMPT)?;
    let explorer_prompt = prompt_file("explorer", EXPLORER_PROMPT)?;

    let shared = Arc::new(Shared {
        notes_dir: run_dir.join("notes"),
        work_dir: run_dir.join("work"),
        langs,
        limit,
        videos: Mutex::new(Vec::new()),
        edges: Mutex::new(Vec::new()),
        topic: Mutex::new(None),
    });
    let cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "live".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(endpoint),
                auth: ProviderAuth::ApiKey(SecretString::new(key.clone())),
                request_timeout: Duration::from_secs(180),
            }],
        },
        vec![
            AgentDefinition::new("coordinator", "live", &model)
                .with_mode(AgentMode::Primary)
                .with_prompt_path(coordinator_prompt)
                .with_allowed_tools(["subagent", "list_playlist", "note_link", "index_create"]),
            AgentDefinition::new("explorer", "live", &model)
                .with_mode(AgentMode::Subagent)
                .with_description("Fetches one video's transcript and writes its Markdown note")
                .with_prompt_path(explorer_prompt)
                .with_allowed_tools(["get_transcript", "note_create"]),
        ],
        run_dir.join("store"),
    );
    let tool = |kind| Arc::new(PlaylistTool { shared: shared.clone(), kind });
    let o = Orchestrator::builder(cfg)
        .register_tool(tool(Kind::ListPlaylist))
        .register_tool(tool(Kind::GetTranscript))
        .register_tool(tool(Kind::NoteCreate))
        .register_tool(tool(Kind::NoteLink))
        .register_tool(tool(Kind::IndexCreate))
        .build()
        .await?;
    let result = run(&o, &url, &key, Duration::from_secs(1800)).await;
    let shutdown = o.shutdown().await;
    result?;
    shutdown?;

    let expected = shared.videos.lock().unwrap().len();
    verify(&shared.notes_dir, expected)?;
    // Đổi tên thư mục theo chủ đề playlist mà agent đã chốt.
    let (topic, _) = shared
        .topic
        .lock()
        .unwrap()
        .clone()
        .ok_or_else(|| io::Error::other("Agent chưa gọi index_create"))?;
    let mut folder = output.join(slugify(&topic, 60));
    if folder.exists() {
        folder = output.join(format!("{}-{}", slugify(&topic, 50), &uuid::Uuid::new_v4().to_string()[..6]));
    }
    std::fs::rename(&shared.notes_dir, &folder)?;
    println!("[xong] thư mục chủ đề: {}", folder.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slug_folds_vietnamese_and_is_safe() {
        assert_eq!(slugify("Chiến lược DCA của Đạt", 60), "chien-luoc-dca-cua-dat");
        assert_eq!(slugify("???", 10), "untitled");
    }

    #[test]
    fn note_names_cannot_escape_dir() {
        assert!(note_stem("../evil").is_err());
        assert!(note_stem("a/b").is_err());
        assert_eq!(note_stem("01-ok.md").unwrap(), "01-ok");
    }

    #[test]
    fn vtt_is_flattened_and_deduplicated() {
        let vtt = "WEBVTT\n\n00:00:00.000 --> 00:00:02.000\nHello <c>world</c>\n\n00:00:02.000 --> 00:00:03.000\nHello world\nsecond &amp; line\n";
        assert_eq!(vtt_to_text(vtt), "Hello world second & line");
    }

    #[test]
    fn mermaid_kind_is_validated() {
        assert!(clean_mermaid("```mermaid\nflowchart TD\n A-->B\n```").unwrap().starts_with("flowchart"));
        assert!(clean_mermaid("pie\n a").is_none());
    }
}
