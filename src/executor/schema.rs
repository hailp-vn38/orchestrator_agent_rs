//! Kiểm tra envelope JSON Schema kỹ thuật.
use serde_json::Value;
const MAX_SCHEMA_DEPTH: usize = 16;

/// Validate envelope theo JSON Schema tối thiểu: `type`, `enum`, `required`, `properties`,
/// `additionalProperties:false`, `items`. Đây là kiểm tra kỹ thuật, không phải semantic.
pub(crate) fn validate_args(schema: &Value, args: &Value) -> Result<(), String> {
    validate(schema, args, "arguments", 0)
}

fn type_matches(t: &str, v: &Value) -> bool {
    match t {
        "string" => v.is_string(),
        "number" => v.is_number(),
        "integer" => {
            v.is_i64()
                || v.is_u64()
                || v.as_f64()
                    .is_some_and(|f| f.is_finite() && f.fract() == 0.0)
        }
        "boolean" => v.is_boolean(),
        "object" => v.is_object(),
        "array" => v.is_array(),
        "null" => v.is_null(),
        _ => true, // type lạ: không ràng buộc
    }
}

fn validate(schema: &Value, v: &Value, path: &str, depth: usize) -> Result<(), String> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(format!("{path} is nested too deeply"));
    }
    let Some(s) = schema.as_object() else {
        return Ok(());
    };
    if let Some(t) = s.get("type") {
        let ok = match t {
            Value::String(t) => type_matches(t, v),
            Value::Array(ts) => ts
                .iter()
                .filter_map(Value::as_str)
                .any(|t| type_matches(t, v)),
            _ => true,
        };
        if !ok {
            let want = t.as_str().map_or_else(|| t.to_string(), str::to_owned);
            return Err(format!("{path} must be of type {want}"));
        }
    }
    if let Some(allowed) = s.get("enum").and_then(Value::as_array) {
        if !allowed.contains(v) {
            return Err(format!("{path} is not one of the allowed values"));
        }
    }
    if let Some(obj) = v.as_object() {
        if let Some(required) = s.get("required").and_then(Value::as_array) {
            for name in required.iter().filter_map(Value::as_str) {
                if !obj.contains_key(name) {
                    return Err(format!("{path}.{name} is required"));
                }
            }
        }
        let props = s.get("properties").and_then(Value::as_object);
        let closed = s.get("additionalProperties") == Some(&Value::Bool(false));
        for (k, val) in obj {
            match props.and_then(|p| p.get(k)) {
                Some(ps) => validate(ps, val, &format!("{path}.{k}"), depth + 1)?,
                None if closed => return Err(format!("{path}.{k} is not allowed")),
                None => {}
            }
        }
    }
    if let (Some(items), Some(arr)) = (s.get("items"), v.as_array()) {
        for (i, x) in arr.iter().enumerate() {
            validate(items, x, &format!("{path}[{i}]"), depth + 1)?;
        }
    }
    Ok(())
}
