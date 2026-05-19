use axum::{
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// StandardResponse — the unified JSON envelope every Fluxx API
/// endpoint emits. Locked to this exact field set + casing:
///
/// ```json
/// {
///   "transactionUrn": "urn:fluxx:tx:<nano>",
///   "status": "SUCCESS" | "FAILED",
///   "responseMessage": "human-readable message",
///   "responseKey": "CODE_LIKE_THIS",
///   "errors": [{ "message": "…" }, { "field": "x", "message": "…" }],
///   "timestamp": "RFC3339 with offset",
///   "metadata": {},
///   "data": object | array,
///   "reference_urn": "<user reference, snake_case key on purpose>"
/// }
/// ```
///
/// Everything is camelCase except `reference_urn`, which keeps its
/// snake_case key per the family spec. `errors` is a list of dicts
/// (objects) so callers can attach structure (`field`, `code`, …);
/// the `String`-flavoured constructor below wraps each message into
/// `{ "message": "…" }` for ergonomics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StandardResponse<T> {
    pub transaction_urn: String,
    pub status: String,
    pub response_message: String,
    pub response_key: String,
    pub errors: Vec<Value>,
    pub timestamp: String,
    pub metadata: Value,
    pub data: T,
    /// Snake-case override — every other field is camelCase, this one
    /// is intentionally not, per the family envelope contract.
    #[serde(rename = "reference_urn")]
    pub reference_urn: String,
}

impl<T: Serialize> StandardResponse<T> {
    /// Build a successful envelope.
    pub fn ok(data: T, message: &str) -> Self {
        Self::new(data, "SUCCESS", message, "OK", Vec::new())
    }

    /// Build a failure envelope from human-readable strings. Each
    /// string is wrapped as `{"message": "…"}` so the wire shape is
    /// always a list of objects, never a list of bare strings.
    pub fn error(data: T, message: &str, key: &str, errors: Vec<String>) -> Self {
        let wrapped = errors
            .into_iter()
            .map(|m| serde_json::json!({ "message": m }))
            .collect();
        Self::with_error_objects(data, "FAILED", message, key, wrapped)
    }

    /// Build a failure envelope with full structured error objects
    /// (typically `{ "field": "...", "message": "..." }`).
    pub fn error_objects(data: T, message: &str, key: &str, errors: Vec<Value>) -> Self {
        Self::with_error_objects(data, "FAILED", message, key, errors)
    }

    fn new(data: T, status: &str, message: &str, key: &str, errors: Vec<String>) -> Self {
        let wrapped = errors
            .into_iter()
            .map(|m| serde_json::json!({ "message": m }))
            .collect();
        Self::with_error_objects(data, status, message, key, wrapped)
    }

    fn with_error_objects(
        data: T,
        status: &str,
        message: &str,
        key: &str,
        errors: Vec<Value>,
    ) -> Self {
        let now: DateTime<Local> = Local::now();
        let nano = now.timestamp_nanos_opt().unwrap_or(0);
        Self {
            transaction_urn: format!("urn:fluxx:tx:{}", nano),
            status: status.to_string(),
            response_message: message.to_string(),
            response_key: key.to_string(),
            errors,
            timestamp: now.to_rfc3339(),
            metadata: serde_json::json!({}),
            data,
            reference_urn: String::new(),
        }
    }
}

impl<T: Serialize> IntoResponse for StandardResponse<T> {
    fn into_response(self) -> Response {
        Json(self).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_envelope_has_empty_errors() {
        let resp = StandardResponse::ok("test_data", "Success message");
        assert_eq!(resp.status, "SUCCESS");
        assert_eq!(resp.response_message, "Success message");
        assert_eq!(resp.response_key, "OK");
        assert!(resp.transaction_urn.starts_with("urn:fluxx:tx:"));
        assert!(resp.errors.is_empty());
    }

    #[test]
    fn error_string_inputs_are_wrapped_as_message_dicts() {
        let resp = StandardResponse::error(
            serde_json::json!({}),
            "Validation failed",
            "VALIDATION_ERROR",
            vec!["email is required".to_string(), "name too short".to_string()],
        );
        assert_eq!(resp.status, "FAILED");
        assert_eq!(resp.response_key, "VALIDATION_ERROR");
        assert_eq!(resp.errors.len(), 2);
        assert_eq!(resp.errors[0], serde_json::json!({ "message": "email is required" }));
        assert_eq!(resp.errors[1], serde_json::json!({ "message": "name too short" }));
    }

    #[test]
    fn error_objects_pass_through_unchanged() {
        let resp = StandardResponse::error_objects(
            serde_json::json!({}),
            "Validation failed",
            "VALIDATION_ERROR",
            vec![serde_json::json!({ "field": "email", "message": "required" })],
        );
        assert_eq!(resp.errors.len(), 1);
        assert_eq!(resp.errors[0]["field"], "email");
        assert_eq!(resp.errors[0]["message"], "required");
    }

    #[test]
    fn wire_format_matches_family_spec() {
        let resp = StandardResponse::error(
            serde_json::json!({}),
            "Validation failed",
            "VALIDATION_ERROR",
            vec!["bad input".into()],
        );
        let json = serde_json::to_value(&resp).unwrap();
        // camelCase for every field except reference_urn
        assert!(json.get("transactionUrn").is_some());
        assert!(json.get("status").is_some());
        assert!(json.get("responseMessage").is_some());
        assert!(json.get("responseKey").is_some());
        assert!(json.get("errors").is_some());
        assert!(json.get("timestamp").is_some());
        assert!(json.get("metadata").is_some());
        assert!(json.get("data").is_some());
        // The one snake_case key, as specified.
        assert!(json.get("reference_urn").is_some());
        assert!(json.get("referenceUrn").is_none(), "reference_urn must NOT be camelCased");
        // errors is a list of dicts, not strings.
        let errs = json.get("errors").unwrap().as_array().unwrap();
        assert!(errs[0].is_object());
        assert_eq!(errs[0]["message"], "bad input");
    }

    #[test]
    fn timestamp_is_rfc3339_with_offset() {
        let resp = StandardResponse::ok("ok", "msg");
        assert!(
            resp.timestamp.contains('T')
                && (resp.timestamp.contains('+') || resp.timestamp.contains('Z')),
            "timestamp should be RFC3339: {}",
            resp.timestamp,
        );
    }

    #[test]
    fn metadata_defaults_to_empty_object() {
        let resp = StandardResponse::ok("ok", "msg");
        assert_eq!(resp.metadata, serde_json::json!({}));
    }
}
