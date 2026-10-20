//! Secret wrapper: không `Display`, `Debug` bị redact, không serialize (docs/18 §18.5.5).

use std::fmt;

#[derive(Clone)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn is_empty(&self) -> bool {
        self.0.trim().is_empty()
    }

    /// Chỉ nội bộ library (gateway) được đọc giá trị thật.
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString(<redacted>)")
    }
}
