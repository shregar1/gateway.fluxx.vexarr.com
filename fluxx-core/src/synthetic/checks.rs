//! Synthetic check definitions and results.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// A synthetic check definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyntheticCheck {
    pub id: String,
    pub name: String,
    pub check_type: CheckType,
    pub interval_secs: u64,
    pub timeout_secs: u64,
    pub enabled: bool,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CheckType {
    /// Simple HTTP uptime check.
    Http {
        url: String,
        method: String,
        expected_status: u16,
        headers: HashMap<String, String>,
        body: Option<String>,
    },
    /// Multi-step API test: sequential HTTP requests with assertions.
    ApiTest {
        steps: Vec<ApiTestStep>,
    },
    /// SSL certificate expiry check.
    Ssl {
        hostname: String,
        port: u16,
        warn_days: u32,
        critical_days: u32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiTestStep {
    pub name: String,
    pub url: String,
    pub method: String,
    pub headers: HashMap<String, String>,
    pub body: Option<String>,
    pub expected_status: u16,
    /// JSONPath assertions on the response body.
    pub assertions: Vec<Assertion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assertion {
    pub field: String,
    pub operator: String, // "eq", "ne", "contains", "gt", "lt"
    pub value: String,
}

/// Result of a synthetic check execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub check_id: String,
    pub check_name: String,
    pub success: bool,
    pub status_code: Option<u16>,
    pub latency_ms: f64,
    pub error: Option<String>,
    pub timestamp_us: u64,
    pub details: HashMap<String, String>,
}

impl CheckResult {
    pub fn success(check: &SyntheticCheck, status: u16, latency: Duration) -> Self {
        Self {
            check_id: check.id.clone(),
            check_name: check.name.clone(),
            success: true,
            status_code: Some(status),
            latency_ms: latency.as_secs_f64() * 1000.0,
            error: None,
            timestamp_us: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
            details: HashMap::new(),
        }
    }

    pub fn failure(check: &SyntheticCheck, error: String, latency: Duration) -> Self {
        Self {
            check_id: check.id.clone(),
            check_name: check.name.clone(),
            success: false,
            status_code: None,
            latency_ms: latency.as_secs_f64() * 1000.0,
            error: Some(error),
            timestamp_us: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
            details: HashMap::new(),
        }
    }
}
