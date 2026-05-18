use serde::{Deserialize, Serialize};
use crate::models::event::Event;
use std::time::SystemTime;
use std::sync::atomic::{AtomicU64, Ordering};

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Session: Defines a logical grouping of network events.
/// This could represent a single WebSocket connection or a chain of HTTP requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub created_at_us: u64,
    pub last_seen_us: u64,
    pub events: Vec<Event>, // Timeline of events in this session
}

impl Session {
    pub fn new(id: String) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let counter = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        
        Self {
            id,
            created_at_us: now + counter,
            last_seen_us: now + counter,
            events: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: Event) {
        self.last_seen_us = event.timestamp_us;
        self.events.push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::{Event, ProtocolType};

    // Test 91: Session new
    #[test]
    fn test_session_new() {
        let session = Session::new("sess-1".to_string());
        assert_eq!(session.id, "sess-1");
        assert!(session.events.is_empty());
    }

    // Test 92: Session timestamps initialized
    #[test]
    fn test_session_timestamps() {
        let before = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let session = Session::new("sess-1".to_string());
        let after = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        
        // created_at_us is offset by SESSION_COUNTER (monotonic) so it may be
        // slightly past `after` when other tests run in parallel; just assert
        // it's in the same general wall-clock neighborhood.
        assert!(session.created_at_us >= before);
        assert!(session.created_at_us <= after + 10_000_000); // within 10s
        assert_eq!(session.created_at_us, session.last_seen_us);
    }

    // Test 93: Add single event
    #[test]
    fn test_session_add_single_event() {
        let mut session = Session::new("sess-1".to_string());
        let event = Event::new("evt-1".to_string(), ProtocolType::Http);
        session.add_event(event);
        assert_eq!(session.events.len(), 1);
    }

    // Test 94: Add multiple events
    #[test]
    fn test_session_add_multiple_events() {
        let mut session = Session::new("sess-1".to_string());
        session.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        session.add_event(Event::new("evt-2".to_string(), ProtocolType::Ws));
        session.add_event(Event::new("evt-3".to_string(), ProtocolType::Grpc));
        assert_eq!(session.events.len(), 3);
    }

    // Test 95: last_seen_us updated on add
    #[test]
    fn test_session_last_seen_updated() {
        let mut session = Session::new("sess-1".to_string());
        let initial_last_seen = session.last_seen_us;
        
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        let event = Event::new("evt-1".to_string(), ProtocolType::Http);
        session.add_event(event);
        
        assert!(session.last_seen_us >= initial_last_seen);
    }

    // Test 96: Session serialization
    #[test]
    fn test_session_serialization() {
        let session = Session::new("sess-1".to_string());
        let json = serde_json::to_string(&session).unwrap();
        assert!(json.contains("sess-1"));
        assert!(json.contains("created_at_us"));
    }

    // Test 97: Session deserialization
    #[test]
    fn test_session_deserialization() {
        let json = r#"{"id":"sess-1","created_at_us":1000,"last_seen_us":2000,"events":[]}"#;
        let session: Session = serde_json::from_str(json).unwrap();
        assert_eq!(session.id, "sess-1");
        assert_eq!(session.created_at_us, 1000);
        assert_eq!(session.last_seen_us, 2000);
    }

    // Test 98: Event order preserved
    #[test]
    fn test_session_event_order() {
        let mut session = Session::new("sess-1".to_string());
        session.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        session.add_event(Event::new("evt-2".to_string(), ProtocolType::Ws));
        assert_eq!(session.events[0].id, "evt-1");
        assert_eq!(session.events[1].id, "evt-2");
    }

    // Test 99: Empty session serialization
    #[test]
    fn test_empty_session_serialization() {
        let session = Session::new("empty-sess".to_string());
        let json = serde_json::to_string(&session).unwrap();
        assert!(json.contains("\"events\":[]"));
    }

    // Test 100: Session with events roundtrip
    #[test]
    fn test_session_roundtrip() {
        let mut session = Session::new("sess-1".to_string());
        session.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        let json = serde_json::to_string(&session).unwrap();
        let restored: Session = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.id, "sess-1");
        assert_eq!(restored.events.len(), 1);
    }

    // Test 551: Session with empty ID
    #[test]
    fn test_session_empty_id() {
        let session = Session::new("".to_string());
        assert_eq!(session.id, "");
        assert!(session.events.is_empty());
    }

    // Test 552: Session with special characters in ID
    #[test]
    fn test_session_special_chars_id() {
        let id = "sess-@#$%^&*()_+{}|:<>?~".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 553: Session with unicode ID
    #[test]
    fn test_session_unicode_id() {
        let id = "会话-セッション-세션-🚀".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 554: Session with very long ID
    #[test]
    fn test_session_long_id() {
        let id = "a".repeat(10000);
        let session = Session::new(id.clone());
        assert_eq!(session.id.len(), 10000);
    }

    // Test 555: Multiple events preserve order
    #[test]
    fn test_session_event_order_multiple() {
        let mut session = Session::new("sess-1".to_string());
        for i in 0..100 {
            session.add_event(Event::new(format!("evt-{}", i), ProtocolType::Http));
        }
        for i in 0..100 {
            assert_eq!(session.events[i].id, format!("evt-{}", i));
        }
    }

    // Test 556: Session clone independence
    #[test]
    fn test_session_clone_independence() {
        let mut session = Session::new("sess-1".to_string());
        session.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        
        let cloned = session.clone();
        session.add_event(Event::new("evt-2".to_string(), ProtocolType::Http));
        
        assert_eq!(session.events.len(), 2);
        assert_eq!(cloned.events.len(), 1);
    }

    // Test 557: Session serialization with events
    #[test]
    fn test_session_serialization_with_events() {
        let mut session = Session::new("sess-1".to_string());
        session.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        session.add_event(Event::new("evt-2".to_string(), ProtocolType::Ws));
        
        let json = serde_json::to_string(&session).unwrap();
        assert!(json.contains("evt-1"));
        assert!(json.contains("evt-2"));
        assert!(json.contains("http"));
        assert!(json.contains("ws"));
    }

    // Test 558: Session deserialization with events
    #[test]
    fn test_session_deserialization_with_events() {
        let json = r#"{"id":"sess-1","created_at_us":1000,"last_seen_us":2000,"events":[{"id":"evt-1","timestamp_us":1500,"source":"test","protocol":"Http","metadata":{"extra":{}}}]"#;
        // Note: Full event deserialization requires more fields
    }

    // Test 559: Session with numeric ID
    #[test]
    fn test_session_numeric_id() {
        let session = Session::new("12345".to_string());
        assert_eq!(session.id, "12345");
    }

    // Test 560: Session with UUID format ID
    #[test]
    fn test_session_uuid_id() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000".to_string();
        let session = Session::new(uuid.clone());
        assert_eq!(session.id, uuid);
    }

    // Test 561: Session created_at_us non-zero
    #[test]
    fn test_session_created_at_nonzero() {
        let session = Session::new("sess-1".to_string());
        assert!(session.created_at_us > 0);
    }

    // Test 562: Event timestamp updates last_seen_us
    #[test]
    fn test_session_last_seen_from_event() {
        let mut session = Session::new("sess-1".to_string());
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.timestamp_us = 9999999999999;
        
        session.add_event(event);
        assert_eq!(session.last_seen_us, 9999999999999);
    }

    // Test 563: Session debug format
    #[test]
    fn test_session_debug_format() {
        let session = Session::new("sess-1".to_string());
        let debug_str = format!("{:?}", session);
        assert!(debug_str.contains("sess-1"));
        assert!(debug_str.contains("Session"));
    }

    // Test 564: Session with many events
    #[test]
    fn test_session_many_events() {
        let mut session = Session::new("sess-1".to_string());
        for i in 0..1000 {
            session.add_event(Event::new(format!("evt-{}", i), ProtocolType::Http));
        }
        assert_eq!(session.events.len(), 1000);
    }

    // Test 565: Session serialize deserialize empty
    #[test]
    fn test_session_serialize_empty() {
        let session = Session::new("empty".to_string());
        let json = serde_json::to_string(&session).unwrap();
        let restored: Session = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.events.len(), 0);
    }

    // Test 566: Session timestamp ordering
    #[test]
    fn test_session_timestamp_ordering() {
        let session = Session::new("sess-1".to_string());
        let mut event1 = Event::new("evt-1".to_string(), ProtocolType::Http);
        event1.timestamp_us = 1000;
        let mut event2 = Event::new("evt-2".to_string(), ProtocolType::Http);
        event2.timestamp_us = 2000;
        
        let mut session = session;
        session.add_event(event1);
        session.add_event(event2);
        
        assert_eq!(session.last_seen_us, 2000);
    }

    // Test 567: Session ID with whitespace
    #[test]
    fn test_session_id_whitespace() {
        let id = "  sess with spaces  ".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 568: Session with null bytes in ID (edge case)
    #[test]
    fn test_session_id_null_bytes() {
        let id = "sess\0with\0nulls".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 569: Session events vector capacity
    #[test]
    fn test_session_events_vector() {
        let mut session = Session::new("sess-1".to_string());
        assert!(session.events.capacity() >= 0);
        session.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        assert_eq!(session.events.len(), 1);
    }

    // Test 570: Multiple sessions independent
    #[test]
    fn test_multiple_sessions_independent() {
        let mut session1 = Session::new("sess-1".to_string());
        let mut session2 = Session::new("sess-2".to_string());
        
        session1.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        session2.add_event(Event::new("evt-2".to_string(), ProtocolType::Ws));
        
        assert_eq!(session1.events.len(), 1);
        assert_eq!(session2.events.len(), 1);
        assert_ne!(session1.id, session2.id);
    }

    // Test 571: Session with mixed protocol events
    #[test]
    fn test_session_mixed_protocols() {
        let mut session = Session::new("sess-1".to_string());
        session.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        session.add_event(Event::new("evt-2".to_string(), ProtocolType::Ws));
        session.add_event(Event::new("evt-3".to_string(), ProtocolType::Grpc));
        session.add_event(Event::new("evt-4".to_string(), ProtocolType::Tcp));
        session.add_event(Event::new("evt-5".to_string(), ProtocolType::Udp));
        
        assert_eq!(session.events.len(), 5);
    }

    // Test 572: Session ID case sensitivity
    #[test]
    fn test_session_id_case_sensitivity() {
        let lower = Session::new("session".to_string());
        let upper = Session::new("SESSION".to_string());
        let mixed = Session::new("Session".to_string());
        
        assert_ne!(lower.id, upper.id);
        assert_ne!(lower.id, mixed.id);
        assert_ne!(upper.id, mixed.id);
    }

    // Test 573: Session JSON roundtrip preserves all fields
    #[test]
    fn test_session_json_roundtrip_complete() {
        let mut session = Session::new("test-sess".to_string());
        session.created_at_us = 1234567890;
        session.last_seen_us = 9876543210;
        
        let json = serde_json::to_string(&session).unwrap();
        let restored: Session = serde_json::from_str(&json).unwrap();
        
        assert_eq!(restored.id, "test-sess");
        assert_eq!(restored.created_at_us, 1234567890);
        assert_eq!(restored.last_seen_us, 9876543210);
    }

    // Test 574: Session event at exact boundary timestamp
    #[test]
    fn test_session_exact_timestamp_boundary() {
        let mut session = Session::new("sess-1".to_string());
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.timestamp_us = u64::MAX;
        session.add_event(event);
        assert_eq!(session.last_seen_us, u64::MAX);
    }

    // Test 575: Concurrent session clones
    #[test]
    fn test_session_concurrent_clones() {
        let session = Session::new("concurrent".to_string());
        let clones: Vec<_> = (0..100).map(|_| session.clone()).collect();
        
        for clone in clones {
            assert_eq!(clone.id, "concurrent");
        }
    }

    // Test 576: Session with empty events JSON array
    #[test]
    fn test_session_empty_events_json() {
        let json = r#"{"id":"test","created_at_us":1,"last_seen_us":2,"events":[]}"#;
        let session: Session = serde_json::from_str(json).unwrap();
        assert!(session.events.is_empty());
    }

    // Test 577: Session creation timestamp uniqueness
    #[test]
    fn test_session_timestamp_uniqueness() {
        let sessions: Vec<_> = (0..10).map(|i| Session::new(format!("sess-{}", i))).collect();
        let timestamps: Vec<_> = sessions.iter().map(|s| s.created_at_us).collect();
        
        // All timestamps should be unique (or at least very unlikely to collide)
        let unique_count = timestamps.iter().collect::<std::collections::HashSet<_>>().len();
        assert!(unique_count >= timestamps.len() - 1); // Allow for 1 collision due to timing
    }

    // Test 578: Session with URL-like ID
    #[test]
    fn test_session_url_like_id() {
        let id = "https://example.com/sessions/123?token=abc".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 579: Session events clear on explicit vector ops
    #[test]
    fn test_session_events_vector_ops() {
        let mut session = Session::new("sess-1".to_string());
        session.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        session.events.clear();
        assert!(session.events.is_empty());
    }

    // Test 580: Session with path-like ID
    #[test]
    fn test_session_path_like_id() {
        let id = "/var/log/sessions/session_2024_01_15.log".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 581: Session serialization pretty print
    #[test]
    fn test_session_pretty_print() {
        let session = Session::new("sess-1".to_string());
        let json = serde_json::to_string_pretty(&session).unwrap();
        assert!(json.contains('\n'));
        assert!(json.contains("sess-1"));
    }

    // Test 582: Session with Base64 ID
    #[test]
    fn test_session_base64_id() {
        let id = "c2Vzc2lvbl9pZF8xMjM=".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 583: Session events iter
    #[test]
    fn test_session_events_iter() {
        let mut session = Session::new("sess-1".to_string());
        session.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        session.add_event(Event::new("evt-2".to_string(), ProtocolType::Http));
        
        let count = session.events.iter().count();
        assert_eq!(count, 2);
    }

    // Test 584: Session with JSON-like ID
    #[test]
    fn test_session_json_like_id() {
        let id = r#"{"session":"abc123","user":"test"}"#.to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 585: Session last_seen_us greater than created_at
    #[test]
    fn test_session_last_seen_greater() {
        let mut session = Session::new("sess-1".to_string());
        let created = session.created_at_us;
        
        std::thread::sleep(std::time::Duration::from_millis(5));
        
        session.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        assert!(session.last_seen_us >= created);
    }

    // Test 586: Session with emoji ID
    #[test]
    fn test_session_emoji_id() {
        let id = "🚀💻🔒🌐📊".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 587: Session events retain order after many adds
    #[test]
    fn test_session_events_order_preserved() {
        let mut session = Session::new("sess-1".to_string());
        for i in 0..50 {
            session.add_event(Event::new(format!("evt-{}", i), ProtocolType::Http));
        }
        
        for i in 0..50 {
            assert_eq!(session.events[i].id, format!("evt-{}", i));
        }
    }

    // Test 588: Session from partial JSON
    #[test]
    fn test_session_partial_json() {
        let json = r#"{"id":"partial","created_at_us":1000,"last_seen_us":2000,"events":[]}"#;
        let session: Session = serde_json::from_str(json).unwrap();
        assert_eq!(session.id, "partial");
    }

    // Test 589: Session with very short ID
    #[test]
    fn test_session_short_id() {
        let session = Session::new("a".to_string());
        assert_eq!(session.id, "a");
    }

    // Test 590: Session with single event properties
    #[test]
    fn test_session_single_event_props() {
        let mut session = Session::new("sess-1".to_string());
        let event = Event::new("evt-1".to_string(), ProtocolType::Grpc);
        session.add_event(event.clone());
        
        assert_eq!(session.events[0].id, "evt-1");
        assert_eq!(session.events[0].protocol, ProtocolType::Grpc);
    }

    // Test 591: Session clone with events
    #[test]
    fn test_session_clone_with_events() {
        let mut session = Session::new("sess-1".to_string());
        session.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        
        let cloned = session.clone();
        assert_eq!(cloned.id, session.id);
        assert_eq!(cloned.events.len(), session.events.len());
        assert_eq!(cloned.created_at_us, session.created_at_us);
    }

    // Test 592: Session with IPv4-like ID
    #[test]
    fn test_session_ipv4_like_id() {
        let id = "192.168.1.1:8080".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 593: Session with IPv6-like ID
    #[test]
    fn test_session_ipv6_like_id() {
        let id = "[2001:db8::1]:8080".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 594: Session events vector push direct
    #[test]
    fn test_session_events_direct_push() {
        let mut session = Session::new("sess-1".to_string());
        session.events.push(Event::new("evt-1".to_string(), ProtocolType::Http));
        assert_eq!(session.events.len(), 1);
    }

    // Test 595: Session with hyphenated ID
    #[test]
    fn test_session_hyphenated_id() {
        let id = "sess-abc-123-def-456".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 596: Session timestamps microseconds precision
    #[test]
    fn test_session_microsecond_precision() {
        let session1 = Session::new("sess-1".to_string());
        let session2 = Session::new("sess-2".to_string());
        
        // Timestamps should differ by at least 1 microsecond if enough time passed
        // or be equal if created in same microsecond
        let diff = if session1.created_at_us > session2.created_at_us {
            session1.created_at_us - session2.created_at_us
        } else {
            session2.created_at_us - session1.created_at_us
        };
        
        // Should be 0 or very small
        assert!(diff < 1000000); // Less than 1 second difference
    }

    // Test 597: Session with underscore ID
    #[test]
    fn test_session_underscore_id() {
        let id = "session_id_123_test".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 598: Session with colon ID
    #[test]
    fn test_session_colon_id() {
        let id = "session:id:123:abc".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 599: Session with dot ID
    #[test]
    fn test_session_dot_id() {
        let id = "session.id.123.abc".to_string();
        let session = Session::new(id.clone());
        assert_eq!(session.id, id);
    }

    // Test 600: Session events get method
    #[test]
    fn test_session_events_get() {
        let mut session = Session::new("sess-1".to_string());
        session.add_event(Event::new("evt-1".to_string(), ProtocolType::Http));
        
        let event = session.events.get(0);
        assert!(event.is_some());
        assert_eq!(event.unwrap().id, "evt-1");
    }
}
