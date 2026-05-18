//! W3C TraceContext (traceparent / tracestate) parsing and generation.
//!
//! Implements the W3C Trace Context specification (https://www.w3.org/TR/trace-context/)
//! for distributed trace propagation across service boundaries.
//!
//! ## Format
//! `traceparent: {version}-{trace-id}-{parent-id}-{trace-flags}`
//! Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`

use serde::{Deserialize, Serialize};

/// Parsed W3C TraceContext from HTTP headers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TraceContext {
    pub version: u8,
    pub trace_id: String,
    pub parent_id: String,
    pub trace_flags: u8,
    pub tracestate: Option<String>,
}

impl TraceContext {
    /// Parse a `traceparent` header value.
    ///
    /// Format: `{version}-{trace-id}-{parent-id}-{trace-flags}`
    /// Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
    pub fn parse(traceparent: &str) -> Result<Self, String> {
        let parts: Vec<&str> = traceparent.trim().split('-').collect();
        if parts.len() != 4 {
            return Err(format!("Invalid traceparent: expected 4 parts, got {}", parts.len()));
        }

        let version = u8::from_str_radix(parts[0], 16)
            .map_err(|_| format!("Invalid version: '{}'", parts[0]))?;

        let trace_id = parts[1].to_string();
        if trace_id.len() != 32 {
            return Err(format!("Invalid trace-id length: expected 32, got {}", trace_id.len()));
        }
        if trace_id.chars().all(|c| c == '0') {
            return Err("Invalid trace-id: all zeros".to_string());
        }

        let parent_id = parts[2].to_string();
        if parent_id.len() != 16 {
            return Err(format!("Invalid parent-id length: expected 16, got {}", parent_id.len()));
        }
        if parent_id.chars().all(|c| c == '0') {
            return Err("Invalid parent-id: all zeros".to_string());
        }

        let trace_flags = u8::from_str_radix(parts[3], 16)
            .map_err(|_| format!("Invalid trace-flags: '{}'", parts[3]))?;

        Ok(Self {
            version,
            trace_id,
            parent_id,
            trace_flags,
            tracestate: None,
        })
    }

    /// Parse with optional tracestate header.
    pub fn parse_with_state(traceparent: &str, tracestate: Option<&str>) -> Result<Self, String> {
        let mut ctx = Self::parse(traceparent)?;
        ctx.tracestate = tracestate.map(|s| s.to_string());
        Ok(ctx)
    }

    /// Generate a new trace context with random IDs.
    pub fn new_root() -> Self {
        Self {
            version: 0,
            trace_id: generate_trace_id(),
            parent_id: generate_span_id(),
            trace_flags: 1, // sampled
            tracestate: None,
        }
    }

    /// Create a child span context (same trace_id, new parent_id).
    pub fn child(&self) -> Self {
        Self {
            version: self.version,
            trace_id: self.trace_id.clone(),
            parent_id: generate_span_id(),
            trace_flags: self.trace_flags,
            tracestate: self.tracestate.clone(),
        }
    }

    /// Format as a `traceparent` header value.
    pub fn to_traceparent(&self) -> String {
        format!(
            "{:02x}-{}-{}-{:02x}",
            self.version, self.trace_id, self.parent_id, self.trace_flags
        )
    }

    /// Check if this trace is sampled.
    pub fn is_sampled(&self) -> bool {
        self.trace_flags & 0x01 != 0
    }

    /// Extract from HTTP headers (case-insensitive lookup).
    pub fn from_headers(headers: &std::collections::HashMap<String, String>) -> Option<Self> {
        let traceparent = headers.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("traceparent"))
            .map(|(_, v)| v.as_str())?;

        let tracestate = headers.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("tracestate"))
            .map(|(_, v)| v.as_str());

        Self::parse_with_state(traceparent, tracestate).ok()
    }

    /// Inject into HTTP headers.
    pub fn inject_headers(&self, headers: &mut std::collections::HashMap<String, String>) {
        headers.insert("traceparent".to_string(), self.to_traceparent());
        if let Some(ref state) = self.tracestate {
            headers.insert("tracestate".to_string(), state.clone());
        }
    }
}

fn generate_trace_id() -> String {
    let mut bytes = [0u8; 16];
    for b in &mut bytes {
        *b = rand::random();
    }
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn generate_span_id() -> String {
    let mut bytes = [0u8; 8];
    for b in &mut bytes {
        *b = rand::random();
    }
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_traceparent() {
        let ctx = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap();
        assert_eq!(ctx.version, 0);
        assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.parent_id, "00f067aa0ba902b7");
        assert_eq!(ctx.trace_flags, 1);
        assert!(ctx.is_sampled());
    }

    #[test]
    fn test_parse_not_sampled() {
        let ctx = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00").unwrap();
        assert!(!ctx.is_sampled());
    }

    #[test]
    fn test_parse_invalid_parts() {
        assert!(TraceContext::parse("00-abc-def").is_err());
    }

    #[test]
    fn test_parse_all_zero_trace_id() {
        assert!(TraceContext::parse("00-00000000000000000000000000000000-00f067aa0ba902b7-01").is_err());
    }

    #[test]
    fn test_parse_all_zero_parent_id() {
        assert!(TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01").is_err());
    }

    #[test]
    fn test_roundtrip() {
        let original = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ctx = TraceContext::parse(original).unwrap();
        assert_eq!(ctx.to_traceparent(), original);
    }

    #[test]
    fn test_new_root() {
        let ctx = TraceContext::new_root();
        assert_eq!(ctx.version, 0);
        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.parent_id.len(), 16);
        assert!(ctx.is_sampled());
    }

    #[test]
    fn test_child_preserves_trace_id() {
        let parent = TraceContext::new_root();
        let child = parent.child();
        assert_eq!(parent.trace_id, child.trace_id);
        assert_ne!(parent.parent_id, child.parent_id);
    }

    #[test]
    fn test_with_tracestate() {
        let ctx = TraceContext::parse_with_state(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            Some("vendor1=value1,vendor2=value2"),
        ).unwrap();
        assert_eq!(ctx.tracestate, Some("vendor1=value1,vendor2=value2".to_string()));
    }

    #[test]
    fn test_from_headers() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("traceparent".to_string(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string());
        headers.insert("tracestate".to_string(), "fluxx=abcd".to_string());

        let ctx = TraceContext::from_headers(&headers).unwrap();
        assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.tracestate, Some("fluxx=abcd".to_string()));
    }

    #[test]
    fn test_inject_headers() {
        let ctx = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap();
        let mut headers = std::collections::HashMap::new();
        ctx.inject_headers(&mut headers);
        assert!(headers.contains_key("traceparent"));
    }
}
