//! Các `HostTool` tải và trích xuất nội dung HTML (`fetch_html`, `html_extract_*`, `html_select_elements`).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use orchestrator_agent_rs::{
    ArtifactInput, HostTool, ToolContext, ToolDescriptor, ToolEffect, ToolError, ToolOutcome,
};
use serde_json::{json, Value};

use crate::html_parser::{
    clean_noise_tags, collapse_ws, extract_article, extract_feed_items, extract_links,
    extract_metadata, find_elements, strip_tags,
};

/// Giới hạn ký tự nội dung bài viết trả về cho model để không tràn context.
const MAX_ARTICLE_CHARS: usize = 24_000;

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

fn str_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, ToolError> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| tool_err("INVALID_ARGUMENT", format!("Thiếu hoặc rỗng: {key}")))
}

fn validate_http_url(url: &str) -> Result<(), ToolError> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(tool_err(
            "INVALID_URL",
            "Chỉ hỗ trợ URL http:// hoặc https://",
        ));
    }
    if url.chars().any(char::is_whitespace) {
        return Err(tool_err("INVALID_URL", "URL không được chứa khoảng trắng"));
    }
    Ok(())
}

/// Bộ nhớ đệm và thư mục lưu HTML nguyên bản cho các tool HTML dùng chung.
pub struct HtmlWorkspace {
    raw_dir: PathBuf,
    cache: Mutex<HashMap<String, String>>,
}

impl HtmlWorkspace {
    pub fn new(raw_dir: PathBuf) -> Self {
        Self {
            raw_dir,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Lấy HTML từ cache hoặc tải mới bằng `curl` (blocking).
    fn fetch_blocking(&self, url: &str) -> Result<(String, bool), ToolError> {
        validate_http_url(url)?;
        if let Some(cached) = self.cache.lock().unwrap().get(url).cloned() {
            return Ok((cached, true));
        }
        std::fs::create_dir_all(&self.raw_dir).map_err(|e| tool_err("IO", e.to_string()))?;
        let output = std::process::Command::new("curl")
            .args([
                "-sSL",
                "--compressed",
                "--max-time",
                "30",
                "-A",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                "--",
            ])
            .arg(url)
            .output()
            .map_err(|e| tool_err("CURL_UNAVAILABLE", format!("Không chạy được curl: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(tool_err(
                "FETCH_FAILED",
                format!("curl thất bại ({}): {}", output.status, stderr.trim()),
            ));
        }
        let html = String::from_utf8_lossy(&output.stdout).into_owned();
        if html.trim().is_empty() {
            return Err(tool_err("EMPTY_HTML", "Trang trả về nội dung rỗng"));
        }
        let file_path = self.raw_dir.join(format!("{}.html", uuid::Uuid::new_v4()));
        let _ = std::fs::write(&file_path, &html);
        self.cache
            .lock()
            .unwrap()
            .insert(url.to_string(), html.clone());
        Ok((html, false))
    }

    pub async fn get_or_fetch(
        self: &Arc<Self>,
        ctx: &ToolContext,
        url: &str,
    ) -> Result<(String, bool), ToolError> {
        let ws = self.clone();
        let u = url.to_string();
        let (html, from_cache) = tokio::task::spawn_blocking(move || ws.fetch_blocking(&u))
            .await
            .map_err(|e| tool_err("INTERNAL", e.to_string()))??;
        if !from_cache {
            ctx.progress(&format!("Đã tải HTML ({} bytes)", html.len()));
        }
        Ok((html, from_cache))
    }
}

#[derive(Clone, Copy)]
pub enum HtmlToolKind {
    FetchHtml,
    ExtractMetadata,
    ExtractArticle,
    ExtractFeed,
    ExtractLinks,
    SelectElements,
}

pub struct HtmlTool {
    ws: Arc<HtmlWorkspace>,
    kind: HtmlToolKind,
}

impl HtmlTool {
    pub fn new(ws: Arc<HtmlWorkspace>, kind: HtmlToolKind) -> Arc<Self> {
        Arc::new(Self { ws, kind })
    }
}

/// Danh sách toàn bộ tên tool HTML để khai báo `with_allowed_tools`.
pub const HTML_TOOL_NAMES: [&str; 6] = [
    "fetch_html",
    "html_extract_metadata",
    "html_extract_article",
    "html_extract_feed",
    "html_extract_links",
    "html_select_elements",
];

#[async_trait::async_trait]
impl HostTool for HtmlTool {
    fn descriptor(&self) -> ToolDescriptor {
        let url_prop = json!({"type": "string", "description": "http(s) URL of the web page"});
        match self.kind {
            HtmlToolKind::FetchHtml => ToolDescriptor {
                name: "fetch_html".into(),
                description: "Fetch raw HTML from an http(s) URL, cache it for subsequent HTML tools, store it as an artifact, and return a structural overview of the page (including article_tag_count to distinguish single articles from category/listing pages).".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"url": url_prop},
                    "required": ["url"],
                    "additionalProperties": false
                }),
                effect: ToolEffect::ReadOnly,
            },
            HtmlToolKind::ExtractMetadata => ToolDescriptor {
                name: "html_extract_metadata".into(),
                description: "Extract metadata from the HTML page at URL: <title>, meta description, OpenGraph tags, canonical URL, published date, author, and JSON-LD NewsArticle fields.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"url": url_prop},
                    "required": ["url"],
                    "additionalProperties": false
                }),
                effect: ToolEffect::ReadOnly,
            },
            HtmlToolKind::ExtractArticle => ToolDescriptor {
                name: "html_extract_article".into(),
                description: "Extract the main readable article content (title, sapo/lead, headings, paragraphs, bullet points, and content images) from a single article HTML page.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"url": url_prop},
                    "required": ["url"],
                    "additionalProperties": false
                }),
                effect: ToolEffect::ReadOnly,
            },
            HtmlToolKind::ExtractFeed => ToolDescriptor {
                name: "html_extract_feed".into(),
                description: "Extract a list of news/article cards ({title, url, snippet}) from a category or listing page (e.g. https://vnexpress.net/thoi-su). Use the returned article URLs with fetch_html + html_extract_article to read each article's full detail.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "url": url_prop,
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of news items to return (1..=30, default 10)"
                        }
                    },
                    "required": ["url"],
                    "additionalProperties": false
                }),
                effect: ToolEffect::ReadOnly,
            },
            HtmlToolKind::ExtractLinks => ToolDescriptor {
                name: "html_extract_links".into(),
                description: "Extract hyperlinks (<a href>) with anchor text from the article body or the entire HTML page. Useful for finding related articles on the same topic.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "url": url_prop,
                        "scope": {
                            "type": "string",
                            "enum": ["article", "all"],
                            "description": "Where to extract links from: 'article' (main content region) or 'all' (whole page)"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of links to return (1..=30)"
                        }
                    },
                    "required": ["url"],
                    "additionalProperties": false
                }),
                effect: ToolEffect::ReadOnly,
            },
            HtmlToolKind::SelectElements => ToolDescriptor {
                name: "html_select_elements".into(),
                description: "Select HTML elements by tag name (e.g. 'h1', 'h2', 'p', 'article', 'div') and optional attribute substring filter, returning their cleaned text content.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "url": url_prop,
                        "tag": {
                            "type": "string",
                            "description": "HTML tag name to extract, e.g. h1, h2, p, article, div"
                        },
                        "attr_contains": {
                            "type": "string",
                            "description": "Optional substring that the opening tag attributes must contain (e.g. id or class name)"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of matching elements to return (1..=30)"
                        }
                    },
                    "required": ["url", "tag"],
                    "additionalProperties": false
                }),
                effect: ToolEffect::ReadOnly,
            },
        }
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutcome, ToolError> {
        let url = str_arg(&args, "url")?.to_string();
        let (html, from_cache) = self.ws.get_or_fetch(&ctx, &url).await?;

        match self.kind {
            HtmlToolKind::FetchHtml => {
                let artifact = ctx
                    .artifacts()
                    .put(ArtifactInput::intermediate_bytes(
                        "page.html",
                        "text/html; charset=utf-8",
                        html.as_bytes().to_vec(),
                    ))
                    .await
                    .ok();
                let meta = extract_metadata(&html);
                let article_count = find_elements(&html, "article").len();
                let overview = json!({
                    "url": url,
                    "cached": from_cache,
                    "bytes": html.len(),
                    "title": meta["title"],
                    "description": meta["description"],
                    "article_tag_count": article_count,
                    "has_main_tag": !find_elements(&html, "main").is_empty(),
                    "h1_count": find_elements(&html, "h1").len(),
                    "h2_count": find_elements(&html, "h2").len(),
                    "h3_count": find_elements(&html, "h3").len(),
                    "p_count": find_elements(&html, "p").len(),
                    "artifact_id": artifact.as_ref().map(|a| &a.id),
                });
                let meta_out = match artifact {
                    Some(a) => json!({"url": url, "bytes": html.len(), "artifact_ids": [a.id]}),
                    None => json!({"url": url, "bytes": html.len()}),
                };
                Ok(outcome(
                    &ctx,
                    serde_json::to_string_pretty(&overview).unwrap_or_default(),
                    meta_out,
                ))
            }
            HtmlToolKind::ExtractMetadata => {
                ctx.progress("Đang trích xuất metadata từ HTML...");
                let meta = extract_metadata(&html);
                Ok(outcome(
                    &ctx,
                    serde_json::to_string_pretty(&meta).unwrap_or_default(),
                    json!({"url": url}),
                ))
            }
            HtmlToolKind::ExtractArticle => {
                ctx.progress("Đang bóc tách nội dung bài viết từ HTML...");
                let article = extract_article(&html);
                let mut body = String::new();
                if !article.title.is_empty() {
                    body.push_str(&format!("# {}\n\n", article.title));
                }
                if !article.sapo.is_empty() {
                    body.push_str(&format!("**Tóm lược (Sapo):** {}\n\n", article.sapo));
                }
                if !article.sections.is_empty() {
                    body.push_str("## Nội dung trong trang\n\n");
                    for s in &article.sections {
                        body.push_str(s);
                        body.push_str("\n\n");
                    }
                }
                if !article.images.is_empty() {
                    body.push_str("## Ảnh / Infographic trong bài\n\n");
                    for img in &article.images {
                        let alt = img["alt"].as_str().unwrap_or("");
                        let src = img["url"].as_str().unwrap_or("");
                        body.push_str(&format!("- {alt}: {src}\n"));
                    }
                }
                let total_chars = body.chars().count();
                let truncated: String = body.chars().take(MAX_ARTICLE_CHARS).collect();
                Ok(outcome(
                    &ctx,
                    truncated,
                    json!({
                        "url": url,
                        "title": article.title,
                        "paragraph_count": article.sections.len(),
                        "image_count": article.images.len(),
                        "chars": total_chars
                    }),
                ))
            }
            HtmlToolKind::ExtractFeed => {
                ctx.progress("Đang trích xuất danh sách bài viết từ trang chuyên mục...");
                let limit = args
                    .get("limit")
                    .and_then(Value::as_u64)
                    .unwrap_or(10)
                    .clamp(1, 30) as usize;
                let items = extract_feed_items(&html, &url, limit);
                Ok(outcome(
                    &ctx,
                    serde_json::to_string_pretty(&json!({
                        "url": url,
                        "count": items.len(),
                        "items": items
                    }))
                    .unwrap_or_default(),
                    json!({"url": url, "count": items.len()}),
                ))
            }
            HtmlToolKind::ExtractLinks => {
                let scope = args
                    .get("scope")
                    .and_then(Value::as_str)
                    .unwrap_or("article");
                let limit = args
                    .get("limit")
                    .and_then(Value::as_u64)
                    .unwrap_or(15)
                    .clamp(1, 30) as usize;
                let article_only = scope != "all";
                let links = extract_links(&html, &url, article_only, limit);
                Ok(outcome(
                    &ctx,
                    serde_json::to_string_pretty(&json!({
                        "url": url,
                        "scope": if article_only { "article" } else { "all" },
                        "count": links.len(),
                        "links": links
                    }))
                    .unwrap_or_default(),
                    json!({"url": url, "count": links.len()}),
                ))
            }
            HtmlToolKind::SelectElements => {
                let tag = str_arg(&args, "tag")?.to_ascii_lowercase();
                let filter = args
                    .get("attr_contains")
                    .and_then(Value::as_str)
                    .map(|s| s.to_ascii_lowercase());
                let limit = args
                    .get("limit")
                    .and_then(Value::as_u64)
                    .unwrap_or(15)
                    .clamp(1, 30) as usize;
                let cleaned = clean_noise_tags(&html);
                let mut items = Vec::new();
                for (attrs, inner) in find_elements(&cleaned, &tag) {
                    if let Some(ref needle) = filter {
                        if !attrs.to_ascii_lowercase().contains(needle) {
                            continue;
                        }
                    }
                    let text = strip_tags(&inner);
                    if !text.is_empty() {
                        items.push(json!({
                            "attrs": collapse_ws(&attrs),
                            "text": text
                        }));
                        if items.len() >= limit {
                            break;
                        }
                    }
                }
                Ok(outcome(
                    &ctx,
                    serde_json::to_string_pretty(&json!({
                        "url": url,
                        "tag": tag,
                        "count": items.len(),
                        "elements": items
                    }))
                    .unwrap_or_default(),
                    json!({"url": url, "tag": tag, "count": items.len()}),
                ))
            }
        }
    }
}
