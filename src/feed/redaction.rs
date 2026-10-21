//! Che secret và cắt text tại ranh giới UTF-8.
/// Thay cho giá trị secret bị che.
const REDACTED: &str = "[redacted]";
/// Secret ngắn hơn ngưỡng này không bị che: che chuỗi 1–2 ký tự sẽ phá nội dung hợp lệ.
pub(super) const MIN_SECRET_LEN: usize = 8;

pub(super) fn redact_all(secrets: &[String], text: &str) -> String {
    let mut out = text.to_owned();
    for secret in secrets {
        if out.contains(secret.as_str()) {
            out = out.replace(secret.as_str(), REDACTED);
        }
    }
    out
}

/// Che secret đệ quy trên mọi chuỗi trong JSON payload trước khi publish.
pub(super) fn redact_value(secrets: &[String], value: &mut serde_json::Value) {
    if secrets.is_empty() {
        return;
    }
    match value {
        serde_json::Value::String(s) => {
            *s = redact_all(secrets, s);
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_value(secrets, item);
            }
        }
        serde_json::Value::Object(map) => {
            for v in map.values_mut() {
                redact_value(secrets, v);
            }
        }
        _ => {}
    }
}

/// Che secret trong một stream text kể cả khi secret bị cắt qua nhiều fragment: giữ lại phần
/// đuôi (độ dài secret dài nhất − 1 byte) cho tới khi fragment kế tiếp tới hoặc `flush`.
pub(crate) struct DeltaFilter {
    secrets: Vec<String>,
    hold: usize,
    pending: String,
}

impl DeltaFilter {
    pub(super) fn new(secrets: Vec<String>) -> Self {
        let hold = secrets.iter().map(String::len).max().map_or(0, |n| n - 1);
        Self {
            secrets,
            hold,
            pending: String::new(),
        }
    }

    /// Phần text đã an toàn để phát ngay (có thể rỗng khi đang giữ đuôi).
    pub(crate) fn push(&mut self, fragment: &str) -> String {
        if self.secrets.is_empty() {
            return fragment.to_owned();
        }
        self.pending.push_str(fragment);
        let redacted = redact_all(&self.secrets, &self.pending);
        if redacted.len() <= self.hold {
            self.pending = redacted;
            return String::new();
        }
        let mut cut = redacted.len() - self.hold;
        while !redacted.is_char_boundary(cut) {
            cut -= 1;
        }
        self.pending = redacted[cut..].to_owned();
        redacted[..cut].to_owned()
    }

    /// Phát nốt phần đang giữ (cuối stream hoặc khi stream lỗi).
    pub(crate) fn flush(&mut self) -> String {
        redact_all(&self.secrets, &std::mem::take(&mut self.pending))
    }
}

/// Cắt `text` thành các phần ≤ `max` byte tại ranh giới ký tự, giữ nguyên thứ tự.
pub(crate) fn chunks(text: &str, max: usize) -> Vec<&str> {
    let max = max.max(4);
    let mut out = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let mut end = rest.len().min(max);
        while !rest.is_char_boundary(end) {
            end -= 1;
        }
        let (head, tail) = rest.split_at(end);
        out.push(head);
        rest = tail;
    }
    out
}
