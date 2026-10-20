//! Live test: playlist YouTube → transcript (yt-dlp) → tóm tắt + sơ đồ (LLM) → thư mục ghi chú
//! Markdown kiểu Obsidian, các video liên kết với nhau bằng `[[wikilink]]`.
//!
//! Chạy (cần `yt-dlp` trong PATH và `.env.live` có G0_OPENAI_*):
//!   cargo test --test playlist_live -- --ignored --nocapture
//!
//! Biến tùy chọn (env ưu tiên hơn `.env.live`):
//!   PLAYLIST_URL      URL playlist (mặc định: playlist DCA của Nick Shawn)
//!   PLAYLIST_LANGS    thứ tự ngôn ngữ phụ đề, mặc định `en,vi`
//!   PLAYLIST_OUT_DIR  thư mục gốc đầu ra, mặc định `target/playlist-notes`
//!   PLAYLIST_LIMIT    chỉ xử lý N video đầu (mặc định: tất cả)
//!
//! Đầu ra: `<OUT_DIR>/<chủ-đề>/` gồm `00-index.md`, `NN-<slug>.md` mỗi video và
//! `transcripts/<id>.txt`. Secret không bao giờ được in ra.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, CallerContext, LlmBackend, LlmConfig, LlmProviderConfig,
    Orchestrator, OrchestratorConfig, ProviderAuth, RunStatus, SecretString,
};

const DEFAULT_URL: &str =
    "https://youtube.com/playlist?list=PLeQQbTeiG3sCK5IWsgC873S6JCuVKhHBo&si=xrtoo-Wt6S-0J8lW";
/// Số ký tự transcript tối đa gửi cho model (lấy mẫu đầu/giữa/cuối nếu dài hơn).
const MAX_TRANSCRIPT_CHARS: usize = 30_000;

// ---------------------------------------------------------------------------
// Cấu hình `.env.live`
// ---------------------------------------------------------------------------

fn load_env_file() -> HashMap<String, String> {
    let path = std::env::var("LIVE_ENV_FILE")
        .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/.env.live").to_string());
    let Ok(text) = std::fs::read_to_string(path) else {
        return Default::default();
    };
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (k, v) = line.split_once('=')?;
            let v = v.trim();
            let v = v
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .or_else(|| v.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
                .unwrap_or(v);
            Some((k.trim().to_string(), v.to_string()))
        })
        .collect()
}

fn env(k: &str) -> Option<String> {
    static FILE: std::sync::OnceLock<HashMap<String, String>> = std::sync::OnceLock::new();
    std::env::var(k)
        .ok()
        .or_else(|| FILE.get_or_init(load_env_file).get(k).cloned())
        .filter(|v| !v.trim().is_empty())
}

// ---------------------------------------------------------------------------
// yt-dlp
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct PlaylistEntry {
    id: String,
    title: String,
}

/// Parse output `--print "%(id)s\t%(title)s"` của `yt-dlp --flat-playlist`.
fn parse_playlist_listing(out: &str) -> Vec<PlaylistEntry> {
    out.lines()
        .filter_map(|l| {
            let (id, title) = l.trim().split_once('\t')?;
            let (id, title) = (id.trim(), title.trim());
            (!id.is_empty()).then(|| PlaylistEntry {
                id: id.to_string(),
                title: title.to_string(),
            })
        })
        .collect()
}

fn list_playlist(url: &str) -> Vec<PlaylistEntry> {
    let out = std::process::Command::new("yt-dlp")
        .args(["--flat-playlist", "--print", "%(id)s\t%(title)s", "--"])
        .arg(url)
        .output()
        .expect("BLOCKED: không chạy được yt-dlp (brew install yt-dlp)");
    assert!(
        out.status.success(),
        "yt-dlp liệt kê playlist lỗi: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    parse_playlist_listing(&String::from_utf8_lossy(&out.stdout))
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

/// Tải phụ đề một video; trả transcript thuần. Panic nếu không có phụ đề.
fn fetch_transcript(work: &Path, id: &str, langs: &str) -> String {
    // Phụ đề auto-dịch hay bị 429; bản gốc `<lang>-orig` ổn định hơn nên thử trước.
    let sub_langs = langs
        .split(',')
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .flat_map(|l| [format!("{l}-orig"), l.to_string()])
        .collect::<Vec<_>>()
        .join(",");
    let dir = work.join(id);
    std::fs::create_dir_all(&dir).unwrap();
    let url = format!("https://www.youtube.com/watch?v={id}");
    let _ = std::process::Command::new("yt-dlp")
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
        .arg(&url)
        .output()
        .expect("BLOCKED: không chạy được yt-dlp");
    // yt-dlp có thể thoát lỗi khi một biến thể bị 429; vẫn dùng nếu đã có .vtt.
    let order: Vec<&str> = sub_langs.split(',').collect();
    let mut subs: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "vtt"))
        .collect();
    subs.sort_by_key(|p| {
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        let rank = order
            .iter()
            .position(|l| name.contains(&format!(".{l}.")))
            .unwrap_or(usize::MAX);
        (rank, name)
    });
    let sub = subs
        .first()
        .unwrap_or_else(|| panic!("video {id} không có phụ đề cho: {sub_langs}"));
    let text = vtt_to_text(&std::fs::read_to_string(sub).unwrap());
    assert!(!text.is_empty(), "phụ đề rỗng: {id}");
    text
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

// ---------------------------------------------------------------------------
// Phân tích đầu ra của model
// ---------------------------------------------------------------------------

#[derive(Debug, Default, PartialEq)]
struct Analysis {
    summary: String,
    key_points: Vec<String>,
    structure: String,
    mermaid: Option<String>,
    topics: Vec<String>,
}

/// Lấy nội dung dưới heading `### NAME` đến heading `### ` kế tiếp.
fn section(text: &str, name: &str) -> Option<String> {
    let mut body: Vec<&str> = Vec::new();
    let mut inside = false;
    for line in text.lines() {
        if let Some(h) = line.trim().strip_prefix("### ") {
            if inside {
                break;
            }
            inside = h.trim().eq_ignore_ascii_case(name);
            continue;
        }
        if inside {
            body.push(line);
        }
    }
    let s = body.join("\n").trim().to_string();
    (inside && !s.is_empty()).then_some(s)
}

/// Bỏ rào ``` và chỉ nhận sơ đồ mermaid thuộc loại được hỗ trợ.
fn clean_mermaid(raw: &str) -> Option<String> {
    let code: Vec<&str> = raw
        .lines()
        .filter(|l| !l.trim_start().starts_with("```"))
        .collect();
    let code = code.join("\n").trim().to_string();
    let first = code.lines().next()?.trim();
    [
        "flowchart",
        "graph",
        "mindmap",
        "sequenceDiagram",
        "stateDiagram",
    ]
    .iter()
    .any(|k| first.starts_with(k))
    .then_some(code)
}

fn bullets(s: &str) -> Vec<String> {
    s.lines()
        .filter_map(|l| {
            l.trim()
                .strip_prefix("- ")
                .or_else(|| l.trim().strip_prefix("* "))
        })
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

fn parse_analysis(text: &str) -> Analysis {
    Analysis {
        summary: section(text, "SUMMARY").unwrap_or_default(),
        key_points: section(text, "KEY_POINTS")
            .map(|s| bullets(&s))
            .unwrap_or_default(),
        structure: section(text, "STRUCTURE").unwrap_or_default(),
        mermaid: section(text, "DIAGRAM").and_then(|s| clean_mermaid(&s)),
        topics: section(text, "TOPICS")
            .map(|s| {
                s.split(',')
                    .map(|t| t.trim().trim_matches('#').trim().to_string())
                    .filter(|t| !t.is_empty())
                    .collect()
            })
            .unwrap_or_default(),
    }
}

#[derive(Debug, Default, PartialEq)]
struct Relations {
    topic: String,
    /// (từ, đến, lý do) — chỉ số 1-based theo thứ tự đã đưa cho model.
    edges: Vec<(usize, usize, String)>,
}

/// Parse dòng `TOPIC: ...` và `REL: a -> b | lý do`; bỏ qua chỉ số sai.
fn parse_relations(text: &str, n: usize) -> Relations {
    let mut rel = Relations::default();
    for line in text.lines() {
        let line = line.trim().trim_start_matches(['-', '*']).trim();
        if let Some(t) = line.strip_prefix("TOPIC:") {
            rel.topic = t.trim().to_string();
        } else if let Some(r) = line.strip_prefix("REL:") {
            let Some((pair, reason)) = r.split_once('|') else {
                continue;
            };
            let Some((a, b)) = pair.split_once("->") else {
                continue;
            };
            let (Ok(a), Ok(b)) = (a.trim().parse::<usize>(), b.trim().parse::<usize>()) else {
                continue;
            };
            let reason = reason.trim();
            if (1..=n).contains(&a) && (1..=n).contains(&b) && a != b && !reason.is_empty() {
                rel.edges.push((a, b, reason.to_string()));
            }
        }
    }
    rel
}

// ---------------------------------------------------------------------------
// Dựng Markdown kiểu Obsidian
// ---------------------------------------------------------------------------

/// Bỏ dấu tiếng Việt (không cần crate normalization); ký tự khác giữ nguyên.
fn fold_vietnamese(c: char) -> char {
    const GROUPS: [(&str, char); 9] = [
        ("àáảãạăằắẳẵặâầấẩẫậ", 'a'),
        ("ÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬ", 'a'),
        ("èéẻẽẹêềếểễệÈÉẺẼẸÊỀẾỂỄỆ", 'e'),
        ("ìíỉĩịÌÍỈĨỊ", 'i'),
        ("òóỏõọôồốổỗộơờớởỡợ", 'o'),
        ("ÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢ", 'o'),
        ("ùúủũụưừứửữựÙÚỦŨỤƯỪỨỬỮỰ", 'u'),
        ("ỳýỷỹỵỲÝỶỸỴ", 'y'),
        ("đĐ", 'd'),
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

struct Note {
    /// Tên file không có `.md` (đích của `[[link]]`).
    stem: String,
    title: String,
    id: String,
    analysis: Analysis,
}

fn render_note(
    note: &Note,
    playlist_topic: &str,
    index_stem: &str,
    notes: &[Note],
    me: usize,
    edges: &[(usize, usize, String)],
) -> String {
    let a = &note.analysis;
    let mut doc = format!(
        "---\ntitle: \"{}\"\nsource: https://www.youtube.com/watch?v={}\nplaylist: \"{}\"\ntags: [{}]\n---\n\n# {}\n\n",
        note.title.replace('"', "'"),
        note.id,
        playlist_topic.replace('"', "'"),
        a.topics
            .iter()
            .map(|t| slugify(t, 30))
            .collect::<Vec<_>>()
            .join(", "),
        note.title
    );
    doc.push_str(&format!(
        "> Nguồn: <https://www.youtube.com/watch?v={}> · Thuộc [[{index_stem}|{}]]\n\n",
        note.id,
        link_label(playlist_topic)
    ));
    doc.push_str(&format!("## Tóm tắt\n\n{}\n\n", a.summary));
    if !a.key_points.is_empty() {
        doc.push_str("## Ý chính\n\n");
        for p in &a.key_points {
            doc.push_str(&format!("- {p}\n"));
        }
        doc.push('\n');
    }
    if !a.structure.is_empty() {
        doc.push_str(&format!("## Cấu trúc\n\n{}\n\n", a.structure));
    }
    if let Some(m) = &a.mermaid {
        doc.push_str(&format!("## Sơ đồ\n\n```mermaid\n{m}\n```\n\n"));
    }
    let mut links = String::new();
    for (from, to, reason) in edges {
        let (other, arrow) = if *from == me {
            (*to, "→")
        } else if *to == me {
            (*from, "←")
        } else {
            continue;
        };
        let o = &notes[other - 1];
        links.push_str(&format!(
            "- {arrow} [[{}|{}]] — {reason}\n",
            o.stem,
            link_label(&o.title)
        ));
    }
    if !links.is_empty() {
        doc.push_str(&format!("## Liên quan\n\n{links}"));
    }
    doc
}

fn render_index(
    topic: &str,
    url: &str,
    notes: &[Note],
    edges: &[(usize, usize, String)],
) -> String {
    let mut doc = format!("# {topic}\n\n> Playlist: <{url}>\n\n## Các video\n\n");
    for n in notes {
        let first = n.analysis.summary.lines().next().unwrap_or("");
        doc.push_str(&format!(
            "- [[{}|{}]] — {first}\n",
            n.stem,
            link_label(&n.title)
        ));
    }
    doc.push_str("\n## Bản đồ liên hệ\n\n```mermaid\nflowchart LR\n");
    for (i, n) in notes.iter().enumerate() {
        doc.push_str(&format!(
            "    N{}[\"{}\"]\n",
            i + 1,
            n.title.replace('"', "'")
        ));
    }
    for (a, b, reason) in edges {
        doc.push_str(&format!(
            "    N{a} -->|\"{}\"| N{b}\n",
            reason
                .replace('"', "'")
                .chars()
                .take(40)
                .collect::<String>()
        ));
    }
    doc.push_str("```\n");
    doc
}

// ---------------------------------------------------------------------------
// Gọi model qua public Orchestrator
// ---------------------------------------------------------------------------

const ANALYST_PROMPT: &str = "Bạn là biên tập viên ghi chú kỹ thuật. Dựa HOÀN TOÀN vào transcript \
được đưa, không bịa. Trả lời bằng tiếng Việt (giữ nguyên thuật ngữ tiếng Anh), đúng định dạng \
sau, mỗi mục bắt đầu bằng dòng heading y hệt:\n\
### SUMMARY\n(3-6 câu tóm tắt)\n\
### KEY_POINTS\n(5-8 gạch đầu dòng '- ' ý chính)\n\
### STRUCTURE\n(danh sách lồng nhau '- ' thể hiện cấu trúc/mục lớn của video)\n\
### DIAGRAM\n(một sơ đồ mermaid hợp lệ, bắt đầu bằng 'flowchart TD' hoặc 'mindmap'; nhãn nút đặt \
trong dấu nháy kép; không dùng ký tự đặc biệt khác)\n\
### TOPICS\n(3-6 chủ đề ngắn, ngăn cách bằng dấu phẩy)";

const RELATOR_PROMPT: &str = "Bạn tìm mối liên hệ giữa các video trong một playlist. Đầu vào là \
danh sách video đánh số từ 1. Trả lời CHỈ gồm các dòng, không giải thích thêm:\n\
TOPIC: <tên chủ đề chung của playlist, tiếng Việt, ngắn>\n\
REL: <số> -> <số> | <lý do ngắn bằng tiếng Việt>\n\
Mỗi REL nghĩa là video đầu dẫn tới/làm nền cho video sau (vd phần trước → phần sau, cùng chiến \
lược, một video kiểm chứng video kia). Chỉ dùng số có trong danh sách, ít nhất một REL.";

async fn ask(o: &Orchestrator, agent: &str, prompt: String) -> String {
    let who = CallerContext::new("live", "playlist");
    let sid = o.create_session(&who, agent).await.unwrap();
    let turn = o.submit(&who, &sid, prompt).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(400);
    loop {
        let r = o.result(&who, &sid, &turn.turn_id).await.unwrap();
        if r.status != RunStatus::Running {
            assert_eq!(r.status, RunStatus::Completed, "error: {:?}", r.error);
            return r.output.expect("final output");
        }
        assert!(tokio::time::Instant::now() < deadline, "live run stuck");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
#[ignore = "live provider + yt-dlp + mạng: gọi tường minh với --ignored"]
async fn live_playlist_to_obsidian_notes() {
    let (Some(key), Some(base), Some(model)) = (
        env("G0_OPENAI_API_KEY").or_else(|| env("OPENAI_API_KEY")),
        env("G0_OPENAI_BASE_URL"),
        env("G0_OPENAI_MODEL"),
    ) else {
        panic!("BLOCKED: fill .env.live (see .env.live.example) or set G0_OPENAI_BASE_URL, G0_OPENAI_MODEL, G0_OPENAI_API_KEY");
    };
    let url = env("PLAYLIST_URL").unwrap_or_else(|| DEFAULT_URL.to_string());
    let langs = env("PLAYLIST_LANGS").unwrap_or_else(|| "en,vi".to_string());
    let out_root = env("PLAYLIST_OUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("target/playlist-notes"));
    let limit = env("PLAYLIST_LIMIT")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(usize::MAX);

    // 1. Liệt kê video + tải transcript.
    let mut entries = list_playlist(&url);
    entries.truncate(limit);
    assert!(entries.len() >= 2, "playlist cần ≥ 2 video: {entries:?}");
    println!("playlist: {} video", entries.len());
    let work = tempfile::tempdir().unwrap();
    let transcripts: Vec<String> = entries
        .iter()
        .map(|e| {
            let t = fetch_transcript(work.path(), &e.id, &langs);
            println!("transcript {}: {} ký tự", e.id, t.chars().count());
            t
        })
        .collect();

    // 2. Orchestrator với hai agent không tool.
    let dir = tempfile::tempdir().unwrap();
    let prompt = |name: &str, text: &str| {
        let p = dir.path().join(format!("{name}.md"));
        std::fs::write(&p, text).unwrap();
        p
    };
    let cfg = OrchestratorConfig::new(
        LlmConfig {
            providers: vec![LlmProviderConfig {
                id: "live".into(),
                backend: LlmBackend::OpenAiCompatible,
                endpoint: Some(base),
                auth: ProviderAuth::ApiKey(SecretString::new(key)),
                request_timeout: Duration::from_secs(180),
            }],
        },
        vec![
            AgentDefinition::new("analyst", "live", &model)
                .with_mode(AgentMode::Primary)
                .with_prompt_path(prompt("analyst", ANALYST_PROMPT)),
            AgentDefinition::new("relator", "live", &model)
                .with_mode(AgentMode::Primary)
                .with_prompt_path(prompt("relator", RELATOR_PROMPT)),
        ],
        dir.path().join("store"),
    );
    let o = Orchestrator::builder(cfg).build().await.unwrap();

    // 3. Tóm tắt từng video (mỗi video một session riêng, không dồn context).
    let mut notes: Vec<Note> = Vec::new();
    for (i, (e, t)) in entries.iter().zip(&transcripts).enumerate() {
        let out = ask(
            &o,
            "analyst",
            format!(
                "Video: {}\n\nTranscript:\n{}",
                e.title,
                clip_transcript(t, MAX_TRANSCRIPT_CHARS)
            ),
        )
        .await;
        let analysis = parse_analysis(&out);
        assert!(
            !analysis.summary.is_empty() && !analysis.key_points.is_empty(),
            "model trả sai định dạng cho {}: {out}",
            e.id
        );
        notes.push(Note {
            stem: format!("{:02}-{}", i + 1, slugify(&e.title, 60)),
            title: e.title.clone(),
            id: e.id.clone(),
            analysis,
        });
        println!("đã tóm tắt {}/{}: {}", i + 1, entries.len(), e.title);
    }

    // 4. Tìm mối liên hệ giữa các video.
    let listing: String = notes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            format!(
                "{}. {}\n   Tóm tắt: {}\n   Chủ đề: {}\n",
                i + 1,
                n.title,
                n.analysis.summary,
                n.analysis.topics.join(", ")
            )
        })
        .collect();
    let rel = parse_relations(&ask(&o, "relator", listing).await, notes.len());
    o.shutdown().await.unwrap();
    assert!(!rel.edges.is_empty(), "model không đề xuất liên hệ nào");
    let topic = if rel.topic.is_empty() {
        entries[0].title.clone()
    } else {
        rel.topic.clone()
    };

    // 5. Ghi thư mục chủ đề.
    let folder = out_root.join(slugify(&topic, 60));
    std::fs::create_dir_all(folder.join("transcripts")).unwrap();
    let index_stem = "00-index";
    for (i, n) in notes.iter().enumerate() {
        std::fs::write(
            folder.join(format!("{}.md", n.stem)),
            render_note(n, &topic, index_stem, &notes, i + 1, &rel.edges),
        )
        .unwrap();
        std::fs::write(
            folder.join("transcripts").join(format!("{}.txt", n.id)),
            &transcripts[i],
        )
        .unwrap();
    }
    std::fs::write(
        folder.join(format!("{index_stem}.md")),
        render_index(&topic, &url, &notes, &rel.edges),
    )
    .unwrap();
    println!("đã ghi: {}", folder.display());

    // 6. Xác minh trên đĩa: mọi [[link]] trỏ tới file tồn tại; mỗi video có liên kết.
    let mut files: Vec<PathBuf> = notes
        .iter()
        .map(|n| folder.join(format!("{}.md", n.stem)))
        .collect();
    files.push(folder.join(format!("{index_stem}.md")));
    let mut linked_notes = 0;
    for f in &files {
        let doc = std::fs::read_to_string(f).unwrap();
        for target in wikilink_targets(&doc) {
            assert!(
                folder.join(format!("{target}.md")).exists(),
                "[[{target}]] trong {} trỏ tới file không tồn tại",
                f.display()
            );
        }
        if doc.contains("## Liên quan") {
            linked_notes += 1;
        }
    }
    assert!(linked_notes >= 2, "cần ≥ 2 note có mục Liên quan");
}

/// Đích của mọi `[[target]]` / `[[target|alias]]` trong tài liệu.
fn wikilink_targets(doc: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = doc;
    while let Some(s) = rest.find("[[") {
        let after = &rest[s + 2..];
        let Some(e) = after.find("]]") else { break };
        out.push(after[..e].split('|').next().unwrap().trim().to_string());
        rest = &after[e + 2..];
    }
    out
}

// ---------------------------------------------------------------------------
// Unit test cho phần thuần (chạy không cần mạng)
// ---------------------------------------------------------------------------

#[test]
fn playlist_listing_is_parsed() {
    let got = parse_playlist_listing("abc\tTitle A\n\nxyz\tB | C\nbad line\n");
    assert_eq!(
        got,
        vec![
            PlaylistEntry {
                id: "abc".into(),
                title: "Title A".into()
            },
            PlaylistEntry {
                id: "xyz".into(),
                title: "B | C".into()
            },
        ]
    );
}

#[test]
fn vtt_is_flattened_and_deduplicated() {
    let vtt = "WEBVTT\nKind: captions\nLanguage: en\n\n1\n00:00:00.000 --> 00:00:02.000\nHello <c>world</c>\n\n00:00:02.000 --> 00:00:03.000\nHello world\nsecond &amp; line\n";
    assert_eq!(vtt_to_text(vtt), "Hello world second & line");
}

#[test]
fn analysis_sections_are_parsed() {
    let text = "### SUMMARY\nMột. Hai.\n### KEY_POINTS\n- a\n* b\n### STRUCTURE\n- x\n  - y\n### DIAGRAM\n```mermaid\nflowchart TD\n  A[\"a\"] --> B[\"b\"]\n```\n### TOPICS\nDCA, #MT5 , trading";
    let a = parse_analysis(text);
    assert_eq!(a.summary, "Một. Hai.");
    assert_eq!(a.key_points, ["a", "b"]);
    assert_eq!(a.structure, "- x\n  - y");
    assert!(a.mermaid.unwrap().starts_with("flowchart TD"));
    assert_eq!(a.topics, ["DCA", "MT5", "trading"]);
    assert!(clean_mermaid("pie\n a").is_none());
}

#[test]
fn relations_ignore_invalid_lines() {
    let r = parse_relations(
        "TOPIC: Chiến lược DCA\n- REL: 1 -> 2 | phần tiếp\nREL: 2 -> 9 | sai\nREL: 3 -> 3 | tự trỏ\nREL: 1 -> 2 |\nrác",
        3,
    );
    assert_eq!(r.topic, "Chiến lược DCA");
    assert_eq!(r.edges, vec![(1, 2, "phần tiếp".to_string())]);
}

#[test]
fn notes_link_each_other_obsidian_style() {
    let mk = |i: usize, t: &str| Note {
        stem: format!("{i:02}-{}", slugify(t, 60)),
        title: t.into(),
        id: format!("id{i}"),
        analysis: Analysis {
            summary: "S".into(),
            key_points: vec!["k".into()],
            ..Default::default()
        },
    };
    let notes = vec![mk(1, "Part 1 | Code"), mk(2, "Part 2")];
    let edges = vec![(1, 2, "tiếp theo".to_string())];
    let a = render_note(&notes[0], "Chủ đề", "00-index", &notes, 1, &edges);
    let b = render_note(&notes[1], "Chủ đề", "00-index", &notes, 2, &edges);
    assert!(a.contains("→ [[02-part-2|Part 2]] — tiếp theo"), "{a}");
    assert!(
        b.contains("← [[01-part-1-code|Part 1   Code]] — tiếp theo"),
        "{b}"
    );
    assert_eq!(wikilink_targets(&a), ["00-index", "02-part-2"]);
}

#[test]
fn long_transcript_is_clipped_and_slug_is_safe() {
    let t = "x".repeat(1000);
    assert!(clip_transcript(&t, 300).chars().count() < 400);
    assert_eq!(clip_transcript("short", 300), "short");
    assert_eq!(
        slugify("I Coded: 99% Winrate/DCA!", 60),
        "i-coded-99-winrate-dca"
    );
    assert_eq!(slugify("???", 10), "untitled");
    assert_eq!(slugify("Chiến lược DCA của Đạt", 60), "chien-luoc-dca-cua-dat");
}
