//! StandardResponse — the unified JSON envelope every Fluxx API endpoint
//! emits. Wire-locked to this exact field set + casing:
//!
//! ```json
//! {
//!   "transactionUrn": "urn:fluxx:tx:<nano>",
//!   "status": "SUCCESS" | "FAILED",
//!   "responseMessage": "human-readable message",
//!   "responseKey": "CODE_LIKE_THIS",
//!   "errors": [{ "message": "…" }, { "field": "x", "message": "…" }],
//!   "timestamp": "RFC3339 with offset",
//!   "metadata": {},
//!   "data": object | array,
//!   "reference_urn": "<user reference, snake_case key on purpose>"
//! }
//! ```
//!
//! Everything is camelCase except `reference_urn`, which keeps its
//! snake_case key per the family spec. `errors` is a list of dicts
//! (objects) so callers can attach structure (`field`, `code`, …).
//!
//! ## Implementation
//!
//! The envelope's shape, constructors, and serialization all live in
//! `utilx::http::envelope::StandardResponse`. This file is a thin newtype
//! wrapper that:
//!   1. Bakes the fluxx-specific URN format (`urn:fluxx:tx:<nano>`) into
//!      every constructor so 139 call sites stay source-compatible.
//!   2. Implements axum's `IntoResponse` so handlers can return the type
//!      directly. (utilx stays framework-agnostic.)
//!
//! Adding a new constructor flavour? Add it to `utilx::http::envelope` so
//! every product picks it up, then thread it through here.

use axum::{
    Json,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::Value;

/// Default transaction-URN prefix for this product. Other vexarr gateways
/// own their own slug; fluxx's family-spec contract is `urn:fluxx:tx:<nano>`.
const FLUXX_URN_PRODUCT: &str = "fluxx";
const FLUXX_URN_SEGMENT: &str = "tx";

fn fresh_urn() -> String {
    utilx::http::envelope::gen_nano_urn(FLUXX_URN_PRODUCT, FLUXX_URN_SEGMENT)
}

/// The fluxx response envelope. Newtype wrapper around the shared
/// `utilx::http::envelope::StandardResponse<T>` — `serde(transparent)`
/// makes the wire shape identical to the inner struct, so existing
/// dashboard / SDK consumers see no diff. `Clone` / `Debug` are
/// deliberately not derived: no production caller uses them, and adding
/// them would force `T: Clone` / `T: Debug` bounds on every handler.
#[derive(Serialize)]
#[serde(transparent)]
pub struct StandardResponse<T: Serialize>(utilx::http::envelope::StandardResponse<T>);

impl<T: Serialize> StandardResponse<T> {
    /// Build a successful envelope.
    pub fn ok(data: T, message: &str) -> Self {
        Self(utilx::http::envelope::StandardResponse::ok(
            fresh_urn(),
            data,
            message,
        ))
    }

    /// Build a failure envelope from human-readable strings. Each string is
    /// wrapped as `{"message": "…"}` so the wire shape is always a list of
    /// objects, never a list of bare strings.
    pub fn error(data: T, message: &str, key: &str, errors: Vec<String>) -> Self {
        Self(utilx::http::envelope::StandardResponse::error(
            fresh_urn(),
            data,
            message,
            key,
            errors,
        ))
    }

    /// Build a failure envelope with full structured error objects (typically
    /// `{ "field": "...", "message": "..." }`).
    pub fn error_objects(data: T, message: &str, key: &str, errors: Vec<Value>) -> Self {
        Self(utilx::http::envelope::StandardResponse::error_objects(
            fresh_urn(),
            data,
            message,
            key,
            errors,
        ))
    }
}

impl<T: Serialize> IntoResponse for StandardResponse<T> {
    fn into_response(self) -> Response {
        Json(self.0).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_envelope_has_empty_errors() {
        let resp = StandardResponse::ok("test_data", "Success message");
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["status"], "SUCCESS");
        assert_eq!(json["responseMessage"], "Success message");
        assert_eq!(json["responseKey"], "OK");
        assert!(
            json["transactionUrn"]
                .as_str()
                .unwrap()
                .starts_with("urn:fluxx:tx:")
        );
        assert!(json["errors"].as_array().unwrap().is_empty());
    }

    #[test]
    fn error_string_inputs_are_wrapped_as_message_dicts() {
        let resp = StandardResponse::error(
            serde_json::json!({}),
            "Validation failed",
            "VALIDATION_ERROR",
            vec!["email is required".to_string(), "name too short".to_string()],
        );
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["status"], "FAILED");
        assert_eq!(json["responseKey"], "VALIDATION_ERROR");
        let errs = json["errors"].as_array().unwrap();
        assert_eq!(errs.len(), 2);
        assert_eq!(errs[0], serde_json::json!({ "message": "email is required" }));
        assert_eq!(errs[1], serde_json::json!({ "message": "name too short" }));
    }

    #[test]
    fn error_objects_pass_through_unchanged() {
        let resp = StandardResponse::error_objects(
            serde_json::json!({}),
            "Validation failed",
            "VALIDATION_ERROR",
            vec![serde_json::json!({ "field": "email", "message": "required" })],
        );
        let json = serde_json::to_value(&resp).unwrap();
        let errs = json["errors"].as_array().unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0]["field"], "email");
        assert_eq!(errs[0]["message"], "required");
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
        assert!(
            json.get("referenceUrn").is_none(),
            "reference_urn must NOT be camelCased"
        );
        // errors is a list of dicts, not strings.
        let errs = json.get("errors").unwrap().as_array().unwrap();
        assert!(errs[0].is_object());
        assert_eq!(errs[0]["message"], "bad input");
    }

    #[test]
    fn timestamp_is_rfc3339_with_offset() {
        let resp = StandardResponse::ok("ok", "msg");
        let json = serde_json::to_value(&resp).unwrap();
        let ts = json["timestamp"].as_str().unwrap();
        assert!(
            ts.contains('T') && (ts.contains('+') || ts.contains('-') || ts.contains('Z')),
            "timestamp should be RFC3339: {ts}",
        );
    }

    #[test]
    fn metadata_defaults_to_empty_object() {
        let resp = StandardResponse::ok("ok", "msg");
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["metadata"], serde_json::json!({}));
    }
}
