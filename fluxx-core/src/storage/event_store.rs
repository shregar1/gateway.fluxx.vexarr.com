use crate::models::event::Event;
use crate::core::error::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// TimeWindow: Defines the boundaries for chronological event processing.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeWindow {
    pub start_us: u64,
    pub end_us: u64,
}

impl TimeWindow {
    pub fn new(start_us: u64, end_us: u64) -> Self {
        Self { start_us, end_us }
    }
}

/// EventStore: Interface for high-performance event persistence.
/// Purely focused on storage and range retrieval to ensure pluggable backends.
#[async_trait]
pub trait EventStore: Send + Sync + std::fmt::Debug {
    /// Inserts an event into the store.
    async fn insert(&self, event: Event) -> Result<()>;
    
    /// Queries a block of events within the specified time range.
    async fn query(&self, window: TimeWindow) -> Result<Vec<Event>>;
    
    /// Returns the total volume of events in storage.
    async fn len(&self) -> Result<usize>;

    /// Lists events with optional filtering.
    async fn list_events(&self, project_id: Option<String>, protocol: Option<crate::models::event::ProtocolType>, severity: Option<crate::models::event::EventSeverity>, service_id: Option<String>) -> Vec<Event>;

    /// Lists events with timestamp_us strictly greater than `since_us`.
    /// Used by the cloud forwarder to avoid full-table scans on every tick.
    async fn list_events_since(&self, since_us: u64) -> Vec<Event>;

    /// Returns aggregate statistics for the event store.
    async fn get_stats(&self) -> serde_json::Value;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 701: TimeWindow new with zero values
    #[test]
    fn test_time_window_new_zero() {
        let window = TimeWindow::new(0, 0);
        assert_eq!(window.start_us, 0);
        assert_eq!(window.end_us, 0);
    }

    // Test 702: TimeWindow new with normal values
    #[test]
    fn test_time_window_new_normal() {
        let window = TimeWindow::new(1000, 5000);
        assert_eq!(window.start_us, 1000);
        assert_eq!(window.end_us, 5000);
    }

    // Test 703: TimeWindow new with large values
    #[test]
    fn test_time_window_new_large() {
        let window = TimeWindow::new(u64::MAX - 1000, u64::MAX);
        assert_eq!(window.start_us, u64::MAX - 1000);
        assert_eq!(window.end_us, u64::MAX);
    }

    // Test 704: TimeWindow clone
    #[test]
    fn test_time_window_clone() {
        let window = TimeWindow::new(100, 200);
        let cloned = window.clone();
        assert_eq!(cloned.start_us, 100);
        assert_eq!(cloned.end_us, 200);
    }

    // Test 705: TimeWindow copy
    #[test]
    fn test_time_window_copy() {
        let window = TimeWindow::new(100, 200);
        let copied = window;
        assert_eq!(window.start_us, copied.start_us);
        assert_eq!(window.end_us, copied.end_us);
    }

    // Test 706: TimeWindow debug format
    #[test]
    fn test_time_window_debug() {
        let window = TimeWindow::new(100, 200);
        let debug = format!("{:?}", window);
        assert!(debug.contains("TimeWindow"));
        assert!(debug.contains("100"));
        assert!(debug.contains("200"));
    }

    // Test 707: TimeWindow serialization
    #[test]
    fn test_time_window_serialize() {
        let window = TimeWindow::new(1000, 2000);
        let json = serde_json::to_string(&window).unwrap();
        assert!(json.contains("1000"));
        assert!(json.contains("2000"));
    }

    // Test 708: TimeWindow deserialization
    #[test]
    fn test_time_window_deserialize() {
        let json = r#"{"start_us":1000,"end_us":2000}"#;
        let window: TimeWindow = serde_json::from_str(json).unwrap();
        assert_eq!(window.start_us, 1000);
        assert_eq!(window.end_us, 2000);
    }

    // Test 709: TimeWindow roundtrip serialize/deserialize
    #[test]
    fn test_time_window_roundtrip() {
        let original = TimeWindow::new(500, 1500);
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: TimeWindow = serde_json::from_str(&json).unwrap();
        assert_eq!(original.start_us, deserialized.start_us);
        assert_eq!(original.end_us, deserialized.end_us);
    }

    // Test 710: TimeWindow with inverted range (start > end)
    #[test]
    fn test_time_window_inverted_range() {
        let window = TimeWindow::new(2000, 1000);
        assert_eq!(window.start_us, 2000);
        assert_eq!(window.end_us, 1000);
    }

    // Test 711: TimeWindow single microsecond window
    #[test]
    fn test_time_window_single_us() {
        let window = TimeWindow::new(100, 101);
        assert_eq!(window.start_us, 100);
        assert_eq!(window.end_us, 101);
    }

    // Test 712: TimeWindow at u64 boundaries
    #[test]
    fn test_time_window_u64_boundaries() {
        let window = TimeWindow::new(0, u64::MAX);
        assert_eq!(window.start_us, 0);
        assert_eq!(window.end_us, u64::MAX);
    }

    // Test 713: TimeWindow with equal start and end
    #[test]
    fn test_time_window_equal_bounds() {
        let window = TimeWindow::new(5000, 5000);
        assert_eq!(window.start_us, window.end_us);
    }

    // Test 714: TimeWindow JSON with missing field fails
    #[test]
    fn test_time_window_deserialize_missing_field() {
        let json = r#"{"start_us":1000}"#;
        let result: std::result::Result<TimeWindow, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    // Test 715: TimeWindow JSON with extra fields
    #[test]
    fn test_time_window_deserialize_extra_fields() {
        let json = r#"{"start_us":1000,"end_us":2000,"extra":999}"#;
        let window: TimeWindow = serde_json::from_str(json).unwrap();
        assert_eq!(window.start_us, 1000);
        assert_eq!(window.end_us, 2000);
    }

    // Test 716: TimeWindow deserialization with string numbers
    #[test]
    fn test_time_window_deserialize_string_numbers() {
        let json = r#"{"start_us":"1000","end_us":"2000"}"#;
        let result: std::result::Result<TimeWindow, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    // Test 717: EventStore trait object safety check
    #[test]
    fn test_event_store_trait_object_safety() {
        fn _check_object_safety(_: &dyn EventStore) {}
    }

    // Test 718: TimeWindow serde default behavior
    #[test]
    fn test_time_window_serde_struct() {
        let window = TimeWindow { start_us: 100, end_us: 200 };
        assert_eq!(window.start_us, 100);
        assert_eq!(window.end_us, 200);
    }

    // Test 719: TimeWindow struct field mutability
    #[test]
    fn test_time_window_mutable_fields() {
        let mut window = TimeWindow::new(100, 200);
        window.start_us = 150;
        window.end_us = 250;
        assert_eq!(window.start_us, 150);
        assert_eq!(window.end_us, 250);
    }

    // Test 720: EventStore Send + Sync bounds
    #[test]
    fn test_event_store_send_sync_bounds() {
        fn _assert_send<T: Send>() {}
        fn _assert_sync<T: Sync>() {}
        fn _assert_event_store<T: EventStore>() {
            _assert_send::<T>();
            _assert_sync::<T>();
        }
    }

    // Test 721: TimeWindow very small range
    #[test]
    fn test_time_window_very_small_range() {
        let window = TimeWindow::new(0, 1);
        assert_eq!(window.end_us - window.start_us, 1);
    }

    // Test 722: TimeWindow very large range
    #[test]
    fn test_time_window_very_large_range() {
        let window = TimeWindow::new(0, u64::MAX / 2);
        assert_eq!(window.end_us, u64::MAX / 2);
    }

    // Test 723: TimeWindow serialization pretty print
    #[test]
    fn test_time_window_serialize_pretty() {
        let window = TimeWindow::new(100, 200);
        let json = serde_json::to_string_pretty(&window).unwrap();
        assert!(json.contains("start_us"));
        assert!(json.contains("end_us"));
    }

    // Test 724: TimeWindow deserialize from compact JSON
    #[test]
    fn test_time_window_deserialize_compact() {
        let json = r#"{"start_us":1,"end_us":2}"#;
        let window: TimeWindow = serde_json::from_str(json).unwrap();
        assert_eq!(window.start_us, 1);
        assert_eq!(window.end_us, 2);
    }

    // Test 725: TimeWindow partial Eq via struct equality
    #[test]
    fn test_time_window_struct_equality() {
        let w1 = TimeWindow::new(100, 200);
        let w2 = TimeWindow::new(100, 200);
        let w3 = TimeWindow::new(100, 300);
        assert_eq!(w1.start_us, w2.start_us);
        assert_eq!(w1.end_us, w2.end_us);
        assert_ne!(w1.end_us, w3.end_us);
    }

    // Test 726: TimeWindow JSON array format deserializes successfully
    #[test]
    fn test_time_window_deserialize_array() {
        let json = r#"[100, 200]"#;
        let result: std::result::Result<TimeWindow, _> = serde_json::from_str(json);
        assert!(result.is_ok());
        let window = result.unwrap();
        assert_eq!(window.start_us, 100);
        assert_eq!(window.end_us, 200);
    }

    // Test 727: TimeWindow deserialize null values fails
    #[test]
    fn test_time_window_deserialize_null_fails() {
        let json = r#"{"start_us":null,"end_us":200}"#;
        let result: std::result::Result<TimeWindow, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    // Test 728: TimeWindow deserialize float fails
    #[test]
    fn test_time_window_deserialize_float_fails() {
        let json = r#"{"start_us":100.5,"end_us":200}"#;
        let result: std::result::Result<TimeWindow, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    // Test 729: TimeWindow deserialize negative fails
    #[test]
    fn test_time_window_deserialize_negative_fails() {
        let json = r#"{"start_us":-100,"end_us":200}"#;
        let result: std::result::Result<TimeWindow, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    // Test 730: TimeWindow max values serialization
    #[test]
    fn test_time_window_max_values_serialize() {
        let window = TimeWindow::new(u64::MAX, u64::MAX);
        let json = serde_json::to_string(&window).unwrap();
        let deserialized: TimeWindow = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.start_us, u64::MAX);
        assert_eq!(deserialized.end_us, u64::MAX);
    }
}
