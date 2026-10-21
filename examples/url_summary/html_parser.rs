//! Bộ xử lý và bóc tách HTML thuần (không thêm dependency ngoài).

use std::collections::HashMap;

use serde_json::{json, Value};

/// Giải mã các HTML entities phổ biến và mã số (`&#...;`, `&#x...;`).
pub fn decode_html_entities(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        let after = &rest[amp + 1..];
        let Some(semi_rel) = after.bytes().take(12).position(|b| b == b';') else {
            out.push('&');
            rest = after;
            continue;
        };
        let entity = &after[..semi_rel];
        let decoded = match entity {
            "nbsp" | "#160" => Some(" ".to_string()),
            "amp" => Some("&".to_string()),
            "lt" => Some("<".to_string()),
            "gt" => Some(">".to_string()),
            "quot" => Some("\"".to_string()),
            "apos" | "#39" | "#039" => Some("'".to_string()),
            "ndash" => Some("–".to_string()),
            "mdash" => Some("—".to_string()),
            "laquo" => Some("«".to_string()),
            "raquo" => Some("»".to_string()),
            "hellip" => Some("…".to_string()),
            _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                u32::from_str_radix(&entity[2..], 16)
                    .ok()
                    .and_then(char::from_u32)
                    .map(|c| c.to_string())
            }
            _ if entity.starts_with('#') => entity[1..]
                .parse::<u32>()
                .ok()
                .and_then(char::from_u32)
                .map(|c| c.to_string()),
            _ => None,
        };
        if let Some(ch) = decoded {
            out.push_str(&ch);
            rest = &after[semi_rel + 1..];
        } else {
            out.push('&');
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

/// Gộp mọi chuỗi khoảng trắng liên tiếp thành một dấu cách đơn.
pub fn collapse_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Bỏ toàn bộ thẻ HTML trong đoạn văn bản và giải mã entity.
pub fn strip_tags(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for c in html.chars() {
        match c {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                out.push(' ');
            }
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    collapse_ws(&decode_html_entities(&out))
}

/// Kiểm tra biên tên thẻ trong chuỗi chữ thường (sau `<` hoặc `</`).
fn is_tag_boundary(b: Option<u8>) -> bool {
    matches!(b, None | Some(b' ' | b'\t' | b'\r' | b'\n' | b'>' | b'/'))
}

/// Tìm vị trí kết thúc `>` của một thẻ mở bắt đầu tại `start`.
fn find_tag_end(html: &str, start: usize) -> Option<usize> {
    html[start..].find('>').map(|i| start + i + 1)
}

/// Xóa comment HTML `<!-- ... -->`.
pub fn remove_html_comments(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut rest = html;
    while let Some(start) = rest.find("<!--") {
        out.push_str(&rest[..start]);
        if let Some(end) = rest[start + 4..].find("-->") {
            rest = &rest[start + 4 + end + 3..];
        } else {
            return out;
        }
    }
    out.push_str(rest);
    out
}

/// Xóa mọi khối `<tag ...>...</tag>` (có tính độ sâu lồng nhau).
pub fn remove_tag_blocks(html: &str, tag: &str) -> String {
    let lower = html.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let open_pat = format!("<{tag}");
    let close_pat = format!("</{tag}");
    let mut out = String::with_capacity(html.len());
    let mut cursor = 0usize;

    while cursor < html.len() {
        let Some(rel) = lower[cursor..].find(&open_pat) else {
            out.push_str(&html[cursor..]);
            break;
        };
        let start = cursor + rel;
        if !is_tag_boundary(bytes.get(start + open_pat.len()).copied()) {
            out.push_str(&html[cursor..start + open_pat.len()]);
            cursor = start + open_pat.len();
            continue;
        }
        out.push_str(&html[cursor..start]);
        let Some(mut scan) = find_tag_end(&lower, start) else {
            break;
        };
        if lower[start..scan].trim_end().ends_with("/>") {
            cursor = scan;
            continue;
        }
        let mut depth = 1usize;
        while depth > 0 && scan < html.len() {
            let next_open = lower[scan..].find(&open_pat).map(|i| scan + i);
            let next_close = lower[scan..].find(&close_pat).map(|i| scan + i);
            match (next_open, next_close) {
                (Some(o), Some(c)) if o < c => {
                    if is_tag_boundary(bytes.get(o + open_pat.len()).copied()) {
                        if let Some(end_o) = find_tag_end(&lower, o) {
                            if !lower[o..end_o].trim_end().ends_with("/>") {
                                depth += 1;
                            }
                            scan = end_o;
                            continue;
                        }
                    }
                    scan = o + open_pat.len();
                }
                (_, Some(c)) => {
                    if is_tag_boundary(bytes.get(c + close_pat.len()).copied()) {
                        depth -= 1;
                        scan = find_tag_end(&lower, c).unwrap_or(html.len());
                    } else {
                        scan = c + close_pat.len();
                    }
                }
                _ => {
                    scan = html.len();
                    break;
                }
            }
        }
        cursor = scan;
    }
    out
}

/// Tìm các phần tử `<tag ...>inner</tag>` (trả về `(open_tag_attrs, inner_html)`).
pub fn find_elements(html: &str, tag: &str) -> Vec<(String, String)> {
    let lower = html.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let open_pat = format!("<{tag}");
    let close_pat = format!("</{tag}");
    let mut results = Vec::new();
    let mut cursor = 0usize;

    while cursor < html.len() {
        let Some(rel) = lower[cursor..].find(&open_pat) else {
            break;
        };
        let start = cursor + rel;
        if !is_tag_boundary(bytes.get(start + open_pat.len()).copied()) {
            cursor = start + open_pat.len();
            continue;
        }
        let Some(open_end) = find_tag_end(&lower, start) else {
            break;
        };
        let open_tag = html[start + open_pat.len()..open_end - 1].to_string();
        if open_tag.trim_end().ends_with('/') {
            results.push((open_tag, String::new()));
            cursor = open_end;
            continue;
        }
        let mut scan = open_end;
        let mut depth = 1usize;
        let mut close_start = html.len();
        let mut after_close = html.len();
        while depth > 0 && scan < html.len() {
            let next_open = lower[scan..].find(&open_pat).map(|i| scan + i);
            let next_close = lower[scan..].find(&close_pat).map(|i| scan + i);
            match (next_open, next_close) {
                (Some(o), Some(c)) if o < c => {
                    if is_tag_boundary(bytes.get(o + open_pat.len()).copied()) {
                        if let Some(end_o) = find_tag_end(&lower, o) {
                            if !lower[o..end_o].trim_end().ends_with("/>") {
                                depth += 1;
                            }
                            scan = end_o;
                            continue;
                        }
                    }
                    scan = o + open_pat.len();
                }
                (_, Some(c)) => {
                    if is_tag_boundary(bytes.get(c + close_pat.len()).copied()) {
                        depth -= 1;
                        close_start = c;
                        after_close = find_tag_end(&lower, c).unwrap_or(html.len());
                        scan = after_close;
                    } else {
                        scan = c + close_pat.len();
                    }
                }
                _ => break,
            }
        }
        let inner = html[open_end..close_start].to_string();
        results.push((open_tag, inner));
        cursor = after_close;
    }
    results
}

/// Lấy giá trị thuộc tính `attr="..."` hoặc `attr='...'` từ chuỗi thuộc tính của thẻ.
pub fn get_attr(attrs: &str, name: &str) -> Option<String> {
    let lower = attrs.to_ascii_lowercase();
    let mut search_from = 0usize;
    while search_from < lower.len() {
        let Some(rel) = lower[search_from..].find(name) else {
            break;
        };
        let pos = search_from + rel;
        let before_ok = pos == 0
            || matches!(
                lower.as_bytes()[pos - 1],
                b' ' | b'\t' | b'\r' | b'\n' | b'"' | b'\''
            );
        let after_name = &attrs[pos + name.len()..];
        let trimmed = after_name.trim_start();
        if !before_ok || !trimmed.starts_with('=') {
            search_from = pos + name.len();
            continue;
        }
        let val_part = trimmed[1..].trim_start();
        let val = if let Some(rest) = val_part.strip_prefix('"') {
            rest.split('"').next().unwrap_or("")
        } else if let Some(rest) = val_part.strip_prefix('\'') {
            rest.split('\'').next().unwrap_or("")
        } else {
            val_part.split_whitespace().next().unwrap_or("")
        };
        return Some(decode_html_entities(val.trim()));
    }
    None
}

/// Tìm tất cả các thẻ đơn/mở `<tag ...>` và trả về chuỗi thuộc tính bên trong.
pub fn find_open_tags(html: &str, tag: &str) -> Vec<String> {
    let lower = html.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let pat = format!("<{tag}");
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while let Some(rel) = lower[cursor..].find(&pat) {
        let start = cursor + rel;
        if !is_tag_boundary(bytes.get(start + pat.len()).copied()) {
            cursor = start + pat.len();
            continue;
        }
        let Some(end) = find_tag_end(&lower, start) else {
            break;
        };
        out.push(html[start + pat.len()..end - 1].to_string());
        cursor = end;
    }
    out
}

/// Trích xuất metadata từ HTML (`<title>`, `<meta>`, `<link rel="canonical">`, JSON-LD).
pub fn extract_metadata(html: &str) -> Value {
    let title = find_elements(html, "title")
        .into_iter()
        .next()
        .map(|(_, inner)| strip_tags(&inner))
        .unwrap_or_default();

    let mut meta_map: HashMap<String, String> = HashMap::new();
    for attrs in find_open_tags(html, "meta") {
        let key = get_attr(&attrs, "property")
            .or_else(|| get_attr(&attrs, "name"))
            .or_else(|| get_attr(&attrs, "itemprop"));
        let content = get_attr(&attrs, "content");
        if let (Some(k), Some(v)) = (key, content) {
            if !v.is_empty() {
                meta_map.entry(k.to_ascii_lowercase()).or_insert(v);
            }
        }
    }

    let canonical = find_open_tags(html, "link")
        .into_iter()
        .find(|a| get_attr(a, "rel").is_some_and(|r| r.eq_ignore_ascii_case("canonical")))
        .and_then(|a| get_attr(&a, "href"));

    let mut ld_headlines = Vec::new();
    let mut ld_descriptions = Vec::new();
    let mut ld_authors = Vec::new();
    let mut ld_dates = Vec::new();
    for (attrs, inner) in find_elements(html, "script") {
        let is_ld =
            get_attr(&attrs, "type").is_some_and(|t| t.to_ascii_lowercase().contains("ld+json"));
        if !is_ld {
            continue;
        }
        if let Ok(val) = serde_json::from_str::<Value>(inner.trim()) {
            if let Some(h) = val.get("headline").and_then(Value::as_str) {
                ld_headlines.push(decode_html_entities(h.trim()));
            }
            if let Some(d) = val.get("description").and_then(Value::as_str) {
                ld_descriptions.push(decode_html_entities(d.trim()));
            }
            if let Some(dt) = val.get("datePublished").and_then(Value::as_str) {
                ld_dates.push(dt.trim().to_string());
            }
            if let Some(author) = val.get("author") {
                if let Some(name) = author.get("name").and_then(Value::as_str) {
                    ld_authors.push(decode_html_entities(name.trim()));
                } else if let Some(s) = author.as_str() {
                    ld_authors.push(decode_html_entities(s.trim()));
                }
            }
        }
    }

    let origin_data_src = find_elements(html, "span")
        .into_iter()
        .find(|(attrs, _)| {
            get_attr(attrs, "id")
                .is_some_and(|id| id == "url_origin_full" || id == "origin_name_full")
        })
        .and_then(|(attrs, _)| get_attr(&attrs, "data-src"));

    json!({
        "title": if !title.is_empty() { Some(title) } else { meta_map.get("og:title").cloned() },
        "og_title": meta_map.get("og:title"),
        "description": meta_map.get("description").or_else(|| meta_map.get("og:description")),
        "site_name": meta_map.get("og:site_name"),
        "keywords": meta_map.get("keywords").or_else(|| meta_map.get("news_keywords")),
        "canonical_url": canonical.or_else(|| meta_map.get("og:url").cloned()),
        "published_at": meta_map.get("pubdate")
            .or_else(|| meta_map.get("datepublished"))
            .cloned()
            .or_else(|| ld_dates.into_iter().next()),
        "author": meta_map.get("author").cloned().or_else(|| ld_authors.into_iter().next()),
        "ld_headline": ld_headlines.into_iter().next(),
        "ld_description": ld_descriptions.into_iter().next(),
        "origin_data_src": origin_data_src,
    })
}

/// Chọn vùng HTML chứa nội dung chính của bài viết (`<article>`, `<main>`, hoặc `<body>`).
pub fn locate_article_region(html: &str) -> String {
    let articles = find_elements(html, "article");
    if let Some((_, inner)) = articles.iter().find(|(attrs, _)| {
        get_attr(attrs, "id").is_some_and(|id| id.contains("article"))
            || get_attr(attrs, "class").is_some_and(|c| c.contains("content-main"))
    }) {
        return inner.clone();
    }
    if let Some((_, inner)) = articles.into_iter().find(|(_, inner)| inner.len() > 200) {
        return inner;
    }
    if let Some((_, inner)) = find_elements(html, "main").into_iter().next() {
        return inner;
    }
    if let Some((_, inner)) = find_elements(html, "body").into_iter().next() {
        return inner;
    }
    html.to_string()
}

/// Làm sạch các thẻ nhiễu (script, style, svg, nav, footer, form, iframe, button).
pub fn clean_noise_tags(html: &str) -> String {
    let mut cleaned = remove_html_comments(html);
    for noisy in [
        "script", "style", "noscript", "svg", "nav", "footer", "aside", "form", "iframe", "button",
    ] {
        cleaned = remove_tag_blocks(&cleaned, noisy);
    }
    cleaned
}

#[derive(Debug)]
pub struct ExtractedArticle {
    pub title: String,
    pub sapo: String,
    pub sections: Vec<String>,
    pub images: Vec<Value>,
}

/// Trích xuất bài viết có cấu trúc từ HTML.
pub fn extract_article(html: &str) -> ExtractedArticle {
    let meta = extract_metadata(html);
    let region = clean_noise_tags(&locate_article_region(html));

    let h1_title = find_elements(&region, "h1")
        .into_iter()
        .map(|(_, inner)| strip_tags(&inner))
        .find(|t| !t.is_empty())
        .or_else(|| {
            find_elements(html, "h1")
                .into_iter()
                .map(|(_, inner)| strip_tags(&inner))
                .find(|t| !t.is_empty())
        })
        .or_else(|| {
            meta.get("title")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default();

    let sapo = find_elements(&region, "h2")
        .into_iter()
        .map(|(_, inner)| strip_tags(&inner))
        .find(|t| !t.is_empty())
        .or_else(|| {
            meta.get("description")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default();

    let mut sections: Vec<String> = Vec::new();
    let mut push_unique = |line: String| {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return;
        }
        if trimmed == "Lưu bài viết thành công"
            || trimmed.starts_with("Bạn có thể xem lại bài viết đã lưu")
            || trimmed == "Bấm xem >>"
        {
            return;
        }
        if !sections.iter().any(|existing| existing == trimmed) {
            sections.push(trimmed.to_string());
        }
    };

    for (attrs, inner) in find_elements(&region, "h2") {
        let text = strip_tags(&inner);
        let is_sapo = get_attr(&attrs, "id").is_some_and(|id| id.contains("sapo")) || text == sapo;
        if !text.is_empty() && !is_sapo {
            push_unique(format!("## {text}"));
        }
    }
    for (_, inner) in find_elements(&region, "h3") {
        let text = strip_tags(&inner);
        if !text.is_empty() {
            push_unique(format!("### {text}"));
        }
    }
    for (_, inner) in find_elements(&region, "p") {
        let text = strip_tags(&inner);
        if !text.is_empty() && text != h1_title && text != sapo {
            push_unique(text);
        }
    }
    for (_, inner) in find_elements(&region, "li") {
        let text = strip_tags(&inner);
        if !text.is_empty() {
            push_unique(format!("- {text}"));
        }
    }

    let mut images = Vec::new();
    for attrs in find_open_tags(&region, "img") {
        let url = get_attr(&attrs, "data-original")
            .or_else(|| get_attr(&attrs, "data-src"))
            .or_else(|| get_attr(&attrs, "src"))
            .unwrap_or_default();
        if url.is_empty()
            || url.starts_with("data:image/")
            || url.contains("in-image-close")
            || url.contains("logo")
            || url.contains("loading.gif")
        {
            continue;
        }
        let alt = get_attr(&attrs, "alt").unwrap_or_default();
        images.push(json!({"alt": alt, "url": url}));
    }

    ExtractedArticle {
        title: h1_title,
        sapo,
        sections,
        images,
    }
}

/// Chuẩn hóa URL liên kết (hỗ trợ URL tương đối `/path` hoặc `//host/path`).
pub fn resolve_url(base_url: &str, href: &str) -> Option<String> {
    let href = href.trim();
    if href.is_empty()
        || href.starts_with('#')
        || href.starts_with("javascript:")
        || href.starts_with("mailto:")
        || href.starts_with("tel:")
    {
        return None;
    }
    if href.starts_with("http://") || href.starts_with("https://") {
        return Some(href.to_string());
    }
    let (scheme, rest) = base_url.split_once("://")?;
    if href.starts_with("//") {
        return Some(format!("{scheme}:{href}"));
    }
    let host = rest.split('/').next()?;
    if href.starts_with('/') {
        return Some(format!("{scheme}://{host}{href}"));
    }
    Some(format!("{scheme}://{host}/{href}"))
}

/// Trích xuất các liên kết `<a href="...">text</a>` từ vùng bài viết hoặc toàn trang.
pub fn extract_links(html: &str, base_url: &str, article_only: bool, limit: usize) -> Vec<Value> {
    let target = if article_only {
        clean_noise_tags(&locate_article_region(html))
    } else {
        clean_noise_tags(html)
    };
    let mut links = Vec::new();
    let mut seen = Vec::new();
    for (attrs, inner) in find_elements(&target, "a") {
        let Some(raw_href) = get_attr(&attrs, "href") else {
            continue;
        };
        let Some(url) = resolve_url(base_url, &raw_href) else {
            continue;
        };
        let text = strip_tags(&inner);
        let title_attr = get_attr(&attrs, "title").unwrap_or_default();
        let label = if !text.is_empty() && text != "Bấm xem >>" {
            text
        } else if !title_attr.is_empty() {
            title_attr
        } else {
            continue;
        };
        if seen.iter().any(|u| u == &url) {
            continue;
        }
        seen.push(url.clone());
        links.push(json!({"text": label, "url": url}));
        if links.len() >= limit {
            break;
        }
    }
    links
}

/// Trích xuất danh sách các mục tin (`title`, `url`, `snippet`) trên trang chuyên mục / trang chủ
/// (ví dụ `https://vnexpress.net/thoi-su`). Quét từng thẻ `<article>` có chứa tiêu đề + link.
pub fn extract_feed_items(html: &str, base_url: &str, limit: usize) -> Vec<Value> {
    let cleaned = clean_noise_tags(html);
    let mut items = Vec::new();
    let mut seen_urls = Vec::new();

    for (_, article_inner) in find_elements(&cleaned, "article") {
        // Tìm tiêu đề và URL từ h2/h3/h4 trong mỗi thẻ `<article>`, hoặc thẻ `<a>` có text dài.
        let mut found_title = String::new();
        let mut found_url = String::new();

        for h_tag in ["h1", "h2", "h3", "h4"] {
            for (_, h_inner) in find_elements(&article_inner, h_tag) {
                for (a_attrs, a_inner) in find_elements(&h_inner, "a") {
                    if let Some(href) =
                        get_attr(&a_attrs, "href").and_then(|h| resolve_url(base_url, &h))
                    {
                        let t = strip_tags(&a_inner);
                        let title = if !t.is_empty() {
                            t
                        } else {
                            get_attr(&a_attrs, "title").unwrap_or_default()
                        };
                        if !title.is_empty() {
                            found_title = title;
                            found_url = href;
                            break;
                        }
                    }
                }
                if !found_url.is_empty() {
                    break;
                }
            }
            if !found_url.is_empty() {
                break;
            }
        }

        if found_url.is_empty() || seen_urls.iter().any(|u| u == &found_url) {
            continue;
        }

        // Lấy đoạn mô tả ngắn (sapo/snippet) từ thẻ `<p>` đầu tiên không trùng tiêu đề.
        let snippet = find_elements(&article_inner, "p")
            .into_iter()
            .map(|(_, p_inner)| strip_tags(&p_inner))
            .find(|text| !text.is_empty() && text != &found_title)
            .unwrap_or_default();

        seen_urls.push(found_url.clone());
        items.push(json!({
            "title": found_title,
            "url": found_url,
            "snippet": snippet,
        }));
        if items.len() >= limit {
            break;
        }
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_HTML: &str = r#"<!DOCTYPE html>
<html lang="vi">
<head>
  <title>Đón không khí lạnh, miền Bắc giảm mấy độ?</title>
  <meta name="description" content="Theo Trung tâm Dự báo khí tượng thủy văn quốc gia, ngày 21 và 22/10 không khí lạnh tràn về." />
  <meta property="og:site_name" content="Tin tức 24h" />
  <meta name="pubdate" content="2026-10-21T18:00:00+07:00" />
  <link rel="canonical" href="https://www.24h.com.vn/tin-tuc-trong-ngay/don-khong-khi-lanh-mien-bac-giam-may-do-c46a1804579.html" />
  <script type="application/ld+json">
  {
    "@context": "http://schema.org",
    "@type": "NewsArticle",
    "headline": "Đón không khí lạnh, miền Bắc giảm mấy độ?",
    "author": { "@type": "Person", "name": "Theo Nam Vũ" }
  }
  </script>
</head>
<body>
  <nav><a href="/home">Trang chủ</a></nav>
  <article class="cate-24h-foot-arti-deta-content-main" id="article_body">
    <header>
      <h1 id="article_title">Đón không khí lạnh, miền Bắc giảm mấy độ?</h1>
    </header>
    <script>var noisy = "should be removed";</script>
    <style>.popup { display: none; }</style>
    <article class="cate-24h-foot-arti-deta-info">
      <h2 id="article_sapo"><strong>Theo Trung tâm Dự báo khí tượng thủy văn quốc gia, ngày 21 và 22/10 không khí lạnh tràn về.</strong></h2>
      <p><img class="news-image" alt="Infographic không khí lạnh" src="data:image/gif;base64,R0lGODlhAQAB" data-original="https://icdn.24h.com.vn/upload/infographic.png" /></p>
      <div class="bv-lq">
        <a href="/tin-tuc-trong-ngay/tac-dong-khong-khi-lanh-c46a1803457.html">Tác động của đợt không khí lạnh sắp tới</a>
        <p>Ngày 21-22/10, khu vực Đông Bắc Bộ và Thanh Hóa có mưa rào &amp; dông rải rác.</p>
      </div>
    </article>
  </article>
</body>
</html>"#;

    #[test]
    fn entities_and_tags_are_cleaned() {
        assert_eq!(
            strip_tags("<p>Nhiệt độ &lt; 20&#176;C &amp; mưa</p>"),
            "Nhiệt độ < 20°C & mưa"
        );
    }

    #[test]
    fn metadata_is_extracted_from_head_and_json_ld() {
        let meta = extract_metadata(SAMPLE_HTML);
        assert_eq!(meta["title"], "Đón không khí lạnh, miền Bắc giảm mấy độ?");
        assert_eq!(meta["site_name"], "Tin tức 24h");
        assert_eq!(meta["author"], "Theo Nam Vũ");
        assert_eq!(meta["published_at"], "2026-10-21T18:00:00+07:00");
    }

    #[test]
    fn nested_article_strips_noise_and_keeps_content_and_images() {
        let article = extract_article(SAMPLE_HTML);
        assert_eq!(article.title, "Đón không khí lạnh, miền Bắc giảm mấy độ?");
        assert!(article
            .sapo
            .contains("ngày 21 và 22/10 không khí lạnh tràn về"));
        assert_eq!(
            article.sections,
            vec!["Ngày 21-22/10, khu vực Đông Bắc Bộ và Thanh Hóa có mưa rào & dông rải rác."]
        );
        assert_eq!(article.images.len(), 1);
        assert_eq!(
            article.images[0]["url"],
            "https://icdn.24h.com.vn/upload/infographic.png"
        );
    }

    #[test]
    fn links_are_resolved_against_base_url() {
        let links = extract_links(
            SAMPLE_HTML,
            "https://www.24h.com.vn/tin-tuc-trong-ngay/bai-goc.html",
            true,
            10,
        );
        assert_eq!(links.len(), 1);
        assert_eq!(
            links[0]["url"],
            "https://www.24h.com.vn/tin-tuc-trong-ngay/tac-dong-khong-khi-lanh-c46a1803457.html"
        );
    }

    #[test]
    fn feed_items_are_extracted_from_category_listing() {
        let category_html = r#"
        <section>
          <article class="item-news">
            <h3 class="title-news"><a href="https://vnexpress.net/tin-so-1-5001.html">Tin thời sự số 1</a></h3>
            <p class="description"><a href="https://vnexpress.net/tin-so-1-5001.html">Mô tả ngắn tin số 1.</a></p>
          </article>
          <article class="item-news">
            <h3 class="title-news"><a href="/tin-so-2-5002.html">Tin thời sự số 2</a></h3>
            <p class="description">Mô tả ngắn tin số 2.</p>
          </article>
        </section>"#;
        let items = extract_feed_items(category_html, "https://vnexpress.net/thoi-su", 10);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["title"], "Tin thời sự số 1");
        assert_eq!(items[0]["snippet"], "Mô tả ngắn tin số 1.");
        assert_eq!(items[1]["url"], "https://vnexpress.net/tin-so-2-5002.html");
    }
}
