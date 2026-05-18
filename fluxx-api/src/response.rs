use serde::{Deserialize, Serialize};
use serde_json::Value;
use axum::{
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Local};

/// StandardResponse: The unified JSON wrapper for all Fluxx API communications.
/// Enforces a consistent schema across REST and WebSocket endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StandardResponse<T> {
    pub transaction_urn: String,
    pub status: String,
    pub response_message: String,
    pub response_key: String,
    pub errors: Vec<String>,
    pub timestamp: String,
    pub metadata: Value,
    pub data: T,
    pub reference_urn: String,
}

impl<T: Serialize> StandardResponse<T> {
    pub fn ok(data: T, message: &str) -> Self {
        Self::new(data, "SUCCESS", message, "OK", Vec::new())
    }

    pub fn error(data: T, message: &str, key: &str, errors: Vec<String>) -> Self {
        Self::new(data, "FAILED", message, key, errors)
    }

    fn new(data: T, status: &str, message: &str, key: &str, errors: Vec<String>) -> Self {
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
            reference_urn: "".to_string(), // Future: Populate from request context
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
    fn test_standard_response_ok() {
        let data = "test_data";
        let resp = StandardResponse::ok(data, "Success message");
        
        assert_eq!(resp.status, "SUCCESS");
        assert_eq!(resp.response_message, "Success message");
        assert!(resp.transaction_urn.starts_with("urn:fluxx:tx:"));
        assert!(resp.errors.is_empty());
    }

    #[test]
    fn test_standard_response_error() {
        let resp = StandardResponse::error(
            serde_json::json!({}), 
            "Error occurred", 
            "ERR_CODE", 
            vec!["Reason 1".to_string()]
        );
        
        assert_eq!(resp.status, "FAILED");
        assert_eq!(resp.response_key, "ERR_CODE");
        assert_eq!(resp.errors.len(), 1);
        assert_eq!(resp.errors[0], "Reason 1");
    }

    #[test]
    fn test_json_serialization_camel_case() {
        let resp = StandardResponse::ok(100, "OK");
        let json = serde_json::to_string(&resp).unwrap();
        
        assert!(json.contains("\"transactionUrn\":"));
        assert!(json.contains("\"responseMessage\":"));
        assert!(json.contains("\"data\":100"));
    }

    // Test 199: StandardResponse timestamp is set
    #[test]
    fn test_standard_response_timestamp() {
        let resp = StandardResponse::ok("test", "message");
        assert!(!resp.timestamp.is_empty());
        assert!(resp.timestamp.contains('T') || resp.timestamp.contains('+') || resp.timestamp.contains('Z'));
    }

    // Test 200: StandardResponse metadata is empty object by default
    #[test]
    fn test_standard_response_default_metadata() {
        let resp = StandardResponse::ok("test", "message");
        assert_eq!(resp.metadata, serde_json::json!({}));
    }
}
