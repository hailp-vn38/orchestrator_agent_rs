//! Tests cho phần embed base instructions.

use super::builder::{parse_base_version, BASE_INSTRUCTIONS};

#[test]
fn embedded_base_is_valid_and_versioned() {
    assert_eq!(parse_base_version(BASE_INSTRUCTIONS), Ok(1));
}

#[test]
fn base_version_rejects_malformed() {
    assert!(parse_base_version("").is_err());
    assert!(parse_base_version("no header\ntext").is_err());
    assert!(parse_base_version("<!-- base-instructions-version: x -->\ntext").is_err());
    assert!(parse_base_version("<!-- base-instructions-version: 1 -->\n\n  \n").is_err());
}
