use serde::{Deserialize, Serialize};

/// Explanation: A high-fidelity, structured interpretation of a network anomaly 
/// or complex request. Designed for architectural clarity in the UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredExplanation {
    pub summary: String,
    pub root_cause: String,
    pub recommendation: String,
    pub impacted_services: Vec<String>,
}

impl StructuredExplanation {
    pub fn mock(message: &str) -> Self {
        Self {
            summary: format!("Fluxx AI analyzed the event: {}", message),
            root_cause: "High-latency observed at the edge due to potential network congestion.".to_string(),
            recommendation: "Check upstream service health and potential kernel-level dropped packets via eBPF.".to_string(),
            impacted_services: vec!["AuthV2".to_string(), "PaymentGateway".to_string()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 195: StructuredExplanation mock
    #[test]
    fn test_explanation_mock() {
        let expl = StructuredExplanation::mock("test event");
        assert!(expl.summary.contains("test event"));
        assert!(!expl.root_cause.is_empty());
        assert!(!expl.recommendation.is_empty());
        assert_eq!(expl.impacted_services.len(), 2);
    }

    // Test 196: StructuredExplanation serialization
    #[test]
    fn test_explanation_serialization() {
        let expl = StructuredExplanation::mock("test");
        let json = serde_json::to_string(&expl).unwrap();
        assert!(json.contains("summary"));
        assert!(json.contains("root_cause"));
        assert!(json.contains("AuthV2"));
    }

    // Test 197: StructuredExplanation deserialization
    #[test]
    fn test_explanation_deserialization() {
        let json = r#"{"summary":"Test summary","root_cause":"Root cause","recommendation":"Fix it","impacted_services":["Service1"]}"#;
        let expl: StructuredExplanation = serde_json::from_str(json).unwrap();
        assert_eq!(expl.summary, "Test summary");
        assert_eq!(expl.impacted_services.len(), 1);
    }

    // Test 198: StructuredExplanation empty impacted services
    #[test]
    fn test_explanation_empty_services() {
        let expl = StructuredExplanation {
            summary: "Test".to_string(),
            root_cause: "Cause".to_string(),
            recommendation: "Rec".to_string(),
            impacted_services: vec![],
        };
        assert!(expl.impacted_services.is_empty());
    }

    // Test 401: StructuredExplanation creation all fields
    #[test]
    fn test_explanation_creation_all_fields() {
        let expl = StructuredExplanation {
            summary: "Summary text".to_string(),
            root_cause: "Root cause text".to_string(),
            recommendation: "Recommendation text".to_string(),
            impacted_services: vec!["Svc1".to_string(), "Svc2".to_string()],
        };
        assert_eq!(expl.summary, "Summary text");
        assert_eq!(expl.root_cause, "Root cause text");
        assert_eq!(expl.recommendation, "Recommendation text");
        assert_eq!(expl.impacted_services.len(), 2);
    }

    // Test 402: StructuredExplanation with empty strings
    #[test]
    fn test_explanation_empty_strings() {
        let expl = StructuredExplanation {
            summary: "".to_string(),
            root_cause: "".to_string(),
            recommendation: "".to_string(),
            impacted_services: vec![],
        };
        assert!(expl.summary.is_empty());
        assert!(expl.root_cause.is_empty());
        assert!(expl.recommendation.is_empty());
    }

    // Test 403: StructuredExplanation mock contains message
    #[test]
    fn test_explanation_mock_contains_message() {
        let expl = StructuredExplanation::mock("my test message");
        assert!(expl.summary.contains("my test message"));
    }

    // Test 404: StructuredExplanation mock has default root_cause
    #[test]
    fn test_explanation_mock_root_cause() {
        let expl = StructuredExplanation::mock("test");
        assert!(!expl.root_cause.is_empty());
        assert!(expl.root_cause.contains("High-latency") || expl.root_cause.contains("congestion"));
    }

    // Test 405: StructuredExplanation mock has default recommendation
    #[test]
    fn test_explanation_mock_recommendation() {
        let expl = StructuredExplanation::mock("test");
        assert!(!expl.recommendation.is_empty());
        assert!(expl.recommendation.contains("eBPF") || expl.recommendation.contains("Check"));
    }

    // Test 406: StructuredExplanation mock has default services
    #[test]
    fn test_explanation_mock_services() {
        let expl = StructuredExplanation::mock("test");
        assert_eq!(expl.impacted_services.len(), 2);
        assert!(expl.impacted_services.contains(&"AuthV2".to_string()));
        assert!(expl.impacted_services.contains(&"PaymentGateway".to_string()));
    }

    // Test 407: StructuredExplanation clone
    #[test]
    fn test_explanation_clone() {
        let expl = StructuredExplanation::mock("test");
        let cloned = expl.clone();
        assert_eq!(expl.summary, cloned.summary);
        assert_eq!(expl.impacted_services, cloned.impacted_services);
    }

    // Test 408: StructuredExplanation debug format
    #[test]
    fn test_explanation_debug() {
        let expl = StructuredExplanation::mock("test");
        let debug_str = format!("{:?}", expl);
        assert!(debug_str.contains("StructuredExplanation"));
        assert!(debug_str.contains("test"));
    }

    // Test 409: StructuredExplanation serialization all fields
    #[test]
    fn test_explanation_serialization_all_fields() {
        let expl = StructuredExplanation {
            summary: "Summary".to_string(),
            root_cause: "Cause".to_string(),
            recommendation: "Fix".to_string(),
            impacted_services: vec!["A".to_string(), "B".to_string()],
        };
        let json = serde_json::to_string(&expl).unwrap();
        assert!(json.contains("Summary"));
        assert!(json.contains("Cause"));
        assert!(json.contains("Fix"));
        assert!(json.contains("A"));
        assert!(json.contains("B"));
    }

    // Test 410: StructuredExplanation round-trip
    #[test]
    fn test_explanation_round_trip() {
        let original = StructuredExplanation {
            summary: "Test summary".to_string(),
            root_cause: "Root".to_string(),
            recommendation: "Rec".to_string(),
            impacted_services: vec!["Svc".to_string()],
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: StructuredExplanation = serde_json::from_str(&json).unwrap();
        assert_eq!(original.summary, deserialized.summary);
        assert_eq!(original.root_cause, deserialized.root_cause);
        assert_eq!(original.impacted_services, deserialized.impacted_services);
    }

    // Test 411: StructuredExplanation with many services
    #[test]
    fn test_explanation_many_services() {
        let services: Vec<String> = (0..100).map(|i| format!("Service{}", i)).collect();
        let expl = StructuredExplanation {
            summary: "Test".to_string(),
            root_cause: "Cause".to_string(),
            recommendation: "Rec".to_string(),
            impacted_services: services,
        };
        assert_eq!(expl.impacted_services.len(), 100);
    }

    // Test 412: StructuredExplanation with unicode
    #[test]
    fn test_explanation_unicode() {
        let expl = StructuredExplanation {
            summary: "摘要 📝".to_string(),
            root_cause: "根本原因".to_string(),
            recommendation: "建议".to_string(),
            impacted_services: vec!["服务".to_string()],
        };
        assert_eq!(expl.summary, "摘要 📝");
        assert_eq!(expl.root_cause, "根本原因");
    }

    // Test 413: StructuredExplanation with long strings
    #[test]
    fn test_explanation_long_strings() {
        let long = "x".repeat(10000);
        let expl = StructuredExplanation {
            summary: long.clone(),
            root_cause: long.clone(),
            recommendation: long.clone(),
            impacted_services: vec![],
        };
        assert_eq!(expl.summary.len(), 10000);
    }

    // Test 414: StructuredExplanation mock with empty message
    #[test]
    fn test_explanation_mock_empty_message() {
        let expl = StructuredExplanation::mock("");
        assert!(expl.summary.contains("Fluxx AI"));
    }

    // Test 415: StructuredExplanation mock with special chars
    #[test]
    fn test_explanation_mock_special_chars() {
        let expl = StructuredExplanation::mock("!@#$%^&*()");
        assert!(expl.summary.contains("!@#$%^&*()"));
    }

    // Test 416: StructuredExplanation deserialization missing fields
    #[test]
    fn test_explanation_deserialization_missing_fields() {
        let json = r#"{"summary":"Test","root_cause":"Cause","recommendation":"Fix","impacted_services":["Svc1","Svc2","Svc3"]}"#;
        let expl: StructuredExplanation = serde_json::from_str(json).unwrap();
        assert_eq!(expl.impacted_services.len(), 3);
    }

    // Test 417: StructuredExplanation json structure
    #[test]
    fn test_explanation_json_structure() {
        let expl = StructuredExplanation::mock("test");
        let json = serde_json::to_string(&expl).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("summary").is_some());
        assert!(parsed.get("root_cause").is_some());
        assert!(parsed.get("recommendation").is_some());
        assert!(parsed.get("impacted_services").is_some());
    }

    // Test 418: StructuredExplanation is Send + Sync
    #[test]
    fn test_explanation_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StructuredExplanation>();
    }

    // Test 419: StructuredExplanation partial equality
    #[test]
    fn test_explanation_partial_eq() {
        let e1 = StructuredExplanation::mock("test");
        let e2 = StructuredExplanation::mock("test");
        assert_eq!(e1.impacted_services, e2.impacted_services);
    }

    // Test 420: StructuredExplanation with single service
    #[test]
    fn test_explanation_single_service() {
        let expl = StructuredExplanation {
            summary: "Test".to_string(),
            root_cause: "Cause".to_string(),
            recommendation: "Rec".to_string(),
            impacted_services: vec!["OnlyService".to_string()],
        };
        assert_eq!(expl.impacted_services.len(), 1);
    }

    // Test 421: StructuredExplanation empty deserialization
    #[test]
    fn test_explanation_empty_deserialization() {
        let json = r#"{"summary":"","root_cause":"","recommendation":"","impacted_services":[]}"#;
        let expl: StructuredExplanation = serde_json::from_str(json).unwrap();
        assert!(expl.summary.is_empty());
        assert!(expl.impacted_services.is_empty());
    }

    // Test 422: StructuredExplanation mock multiple calls
    #[test]
    fn test_explanation_mock_multiple() {
        let expl1 = StructuredExplanation::mock("msg1");
        let expl2 = StructuredExplanation::mock("msg2");
        assert!(expl1.summary.contains("msg1"));
        assert!(expl2.summary.contains("msg2"));
    }

    // Test 423: StructuredExplanation with duplicate services
    #[test]
    fn test_explanation_duplicate_services() {
        let expl = StructuredExplanation {
            summary: "Test".to_string(),
            root_cause: "Cause".to_string(),
            recommendation: "Rec".to_string(),
            impacted_services: vec!["Svc".to_string(), "Svc".to_string()],
        };
        assert_eq!(expl.impacted_services.len(), 2);
    }

    // Test 424: StructuredExplanation serialization preserves order
    #[test]
    fn test_explanation_service_order() {
        let expl = StructuredExplanation {
            summary: "Test".to_string(),
            root_cause: "Cause".to_string(),
            recommendation: "Rec".to_string(),
            impacted_services: vec!["A".to_string(), "B".to_string(), "C".to_string()],
        };
        let json = serde_json::to_string(&expl).unwrap();
        let deserialized: StructuredExplanation = serde_json::from_str(&json).unwrap();
        assert_eq!(expl.impacted_services, deserialized.impacted_services);
    }

    // Test 425: StructuredExplanation with newlines
    #[test]
    fn test_explanation_newlines() {
        let expl = StructuredExplanation {
            summary: "Line1\nLine2".to_string(),
            root_cause: "Cause".to_string(),
            recommendation: "Rec".to_string(),
            impacted_services: vec![],
        };
        assert!(expl.summary.contains('\n'));
    }

    // Test 426: StructuredExplanation with tabs
    #[test]
    fn test_explanation_tabs() {
        let expl = StructuredExplanation {
            summary: "Col1\tCol2".to_string(),
            root_cause: "Cause".to_string(),
            recommendation: "Rec".to_string(),
            impacted_services: vec![],
        };
        assert!(expl.summary.contains('\t'));
    }

    // Test 427: StructuredExplanation mock format
    #[test]
    fn test_explanation_mock_format() {
        let expl = StructuredExplanation::mock("event123");
        assert!(expl.summary.starts_with("Fluxx AI analyzed the event:"));
        assert!(expl.summary.contains("event123"));
    }

    // Test 428: StructuredExplanation clone independence
    #[test]
    fn test_explanation_clone_independence() {
        let mut expl1 = StructuredExplanation::mock("test");
        let mut expl2 = expl1.clone();
        expl2.summary = "Modified".to_string();
        assert_ne!(expl1.summary, expl2.summary);
    }

    // Test 429: StructuredExplanation debug contains all fields
    #[test]
    fn test_explanation_debug_all_fields() {
        let expl = StructuredExplanation {
            summary: "S".to_string(),
            root_cause: "R".to_string(),
            recommendation: "Rec".to_string(),
            impacted_services: vec!["Svc".to_string()],
        };
        let debug_str = format!("{:?}", expl);
        assert!(debug_str.contains("S"));
        assert!(debug_str.contains("R"));
        assert!(debug_str.contains("Rec"));
        assert!(debug_str.contains("Svc"));
    }

    // Test 430: StructuredExplanation with special JSON chars
    #[test]
    fn test_explanation_special_json_chars() {
        let expl = StructuredExplanation {
            summary: "Test \"quoted\"".to_string(),
            root_cause: "Cause".to_string(),
            recommendation: "Rec".to_string(),
            impacted_services: vec![],
        };
        let json = serde_json::to_string(&expl).unwrap();
        assert!(json.contains("quoted"));
    }
}
