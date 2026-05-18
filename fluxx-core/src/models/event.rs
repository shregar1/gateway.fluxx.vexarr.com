use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// Common metadata fields for all Fluxx events.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Metadata {
    pub source_ip: Option<String>,
    pub destination_ip: Option<String>,
    pub request_id: Option<String>,
    pub user_agent: Option<String>,
    
    /// Any protocol-specific extra metadata.
    #[serde(flatten)]
    pub extra: HashMap<String, String>,
}

impl Metadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, key: &str, value: &str) {
        self.extra.insert(key.to_string(), value.to_string());
    }
}

/// Strong typed protocol identifiers for all Fluxx network traffic.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProtocolType {
    Http,
    Ws,
    Tcp,
    Udp,
    Grpc,
    Quic,
    Ebpf,
    Mqtt,
    Modbus,
    System,
    Otlp,
    StatsD,
    Log,
    Other,
}

/// Criticality levels for operational health monitoring.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EventSeverity {
    Info,
    Low,
    Medium,
    High,
    Critical,
}

impl std::fmt::Display for EventSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventSeverity::Info => write!(f, "info"),
            EventSeverity::Low => write!(f, "low"),
            EventSeverity::Medium => write!(f, "medium"),
            EventSeverity::High => write!(f, "high"),
            EventSeverity::Critical => write!(f, "critical"),
        }
    }
}

impl EventSeverity {
    /// Translate the agent's operational-health vocabulary (Info / Low /
    /// Medium / High / Critical) into the log-trace vocabulary the
    /// dashboard schemas and cloud `/security/alerts` filter expect
    /// (info / warn / error / critical).
    ///
    /// Mapping:
    ///   Info, Low    → "info"
    ///   Medium       → "warn"
    ///   High         → "error"
    ///   Critical     → "critical"
    ///
    /// Why this lives here and not in the cloud forwarder or the
    /// integration spawner: both used to carry their own copy of this
    /// table, with the obvious "they'll drift" risk. Putting it on the
    /// enum itself means there is exactly one source of truth — and
    /// the unit tests below pin every variant so a future enum addition
    /// either has to extend this mapping or be deliberately rejected.
    pub fn to_log_severity(&self) -> &'static str {
        match self {
            EventSeverity::Info | EventSeverity::Low => "info",
            EventSeverity::Medium => "warn",
            EventSeverity::High => "error",
            EventSeverity::Critical => "critical",
        }
    }
}

/// The centralized, high-performance event definition for Fluxx.
/// Using 'String' for payloads to ensure seamless serialization across the fleet.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Event {
    pub id: String,
    pub session_id: Option<String>, 
    pub project_id: Option<String>,
    pub environment_id: Option<String>,
    pub service_id: Option<String>,
    pub timestamp_us: u64,
    pub protocol: ProtocolType,
    pub latency_us: u64,
    pub severity: EventSeverity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>, // Optimized for high-fidelity serialization
    pub metadata: Metadata,
}

impl Event {
    pub fn new(id: String, protocol: ProtocolType) -> Self {
        Self {
            id,
            session_id: None,
            project_id: None,
            environment_id: None,
            service_id: None,
            timestamp_us: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
            protocol,
            latency_us: 0,
            severity: EventSeverity::Info,
            payload: None,
            metadata: Metadata::new(),
        }
    }

    pub fn with_session(mut self, session_id: String) -> Self {
        self.session_id = Some(session_id);
        self
    }

    pub fn with_payload(mut self, data: String) -> Self {
        self.payload = Some(data);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── EventSeverity::to_log_severity ────────────────────────────────────
    // Pins the agent-operational → log-trace severity mapping. Both the
    // cloud forwarder and the integration spawner used to carry their own
    // copies of this table; consolidating them here means a change has
    // to land in exactly one place. Anyone adding a new EventSeverity
    // variant gets a compile error from the exhaustive match below,
    // forcing them to think about how it maps.

    #[test]
    fn severity_info_maps_to_info() {
        assert_eq!(EventSeverity::Info.to_log_severity(), "info");
    }

    #[test]
    fn severity_low_maps_to_info() {
        // Low is operational-only; the dashboard's log/trace schema
        // doesn't have a "low" bucket, so we fold it into info.
        assert_eq!(EventSeverity::Low.to_log_severity(), "info");
    }

    #[test]
    fn severity_medium_maps_to_warn() {
        assert_eq!(EventSeverity::Medium.to_log_severity(), "warn");
    }

    #[test]
    fn severity_high_maps_to_error() {
        // Critical for the cloud's /security/alerts filter, which uses
        // severity IN ('error','critical','high') and depends on this
        // being the string the cloud sees.
        assert_eq!(EventSeverity::High.to_log_severity(), "error");
    }

    #[test]
    fn severity_critical_maps_to_critical() {
        assert_eq!(EventSeverity::Critical.to_log_severity(), "critical");
    }

    #[test]
    fn severity_log_severity_is_one_of_the_canonical_set() {
        // Belt-and-braces: every variant must map into the canonical
        // vocabulary the dashboard's LogLevelSchema accepts.
        const CANON: &[&str] = &["trace", "debug", "info", "warn", "error", "fatal", "critical"];
        for s in [
            EventSeverity::Info,
            EventSeverity::Low,
            EventSeverity::Medium,
            EventSeverity::High,
            EventSeverity::Critical,
        ] {
            assert!(
                CANON.contains(&s.to_log_severity()),
                "{:?}.to_log_severity() = {:?} is not in canonical set",
                s,
                s.to_log_severity()
            );
        }
    }

    // Test 1: Event creation with HTTP protocol
    #[test]
    fn test_event_new_http() {
        let event = Event::new("test-1".to_string(), ProtocolType::Http);
        assert_eq!(event.id, "test-1");
        assert_eq!(event.protocol, ProtocolType::Http);
        assert!(event.session_id.is_none());
        assert_eq!(event.latency_us, 0);
    }

    // Test 2: Event creation with WebSocket protocol
    #[test]
    fn test_event_new_ws() {
        let event = Event::new("test-2".to_string(), ProtocolType::Ws);
        assert_eq!(event.protocol, ProtocolType::Ws);
    }

    // Test 3: Event creation with TCP protocol
    #[test]
    fn test_event_new_tcp() {
        let event = Event::new("test-3".to_string(), ProtocolType::Tcp);
        assert_eq!(event.protocol, ProtocolType::Tcp);
    }

    // Test 4: Event creation with UDP protocol
    #[test]
    fn test_event_new_udp() {
        let event = Event::new("test-4".to_string(), ProtocolType::Udp);
        assert_eq!(event.protocol, ProtocolType::Udp);
    }

    // Test 5: Event creation with gRPC protocol
    #[test]
    fn test_event_new_grpc() {
        let event = Event::new("test-5".to_string(), ProtocolType::Grpc);
        assert_eq!(event.protocol, ProtocolType::Grpc);
    }

    // Test 6: Event creation with QUIC protocol
    #[test]
    fn test_event_new_quic() {
        let event = Event::new("test-6".to_string(), ProtocolType::Quic);
        assert_eq!(event.protocol, ProtocolType::Quic);
    }

    // Test 7: Event creation with eBPF protocol
    #[test]
    fn test_event_new_ebpf() {
        let event = Event::new("test-7".to_string(), ProtocolType::Ebpf);
        assert_eq!(event.protocol, ProtocolType::Ebpf);
    }

    // Test 8: Event creation with MQTT protocol
    #[test]
    fn test_event_new_mqtt() {
        let event = Event::new("test-8".to_string(), ProtocolType::Mqtt);
        assert_eq!(event.protocol, ProtocolType::Mqtt);
    }

    // Test 9: Event creation with Modbus protocol
    #[test]
    fn test_event_new_modbus() {
        let event = Event::new("test-9".to_string(), ProtocolType::Modbus);
        assert_eq!(event.protocol, ProtocolType::Modbus);
    }

    // Test 10: Event creation with Other protocol
    #[test]
    fn test_event_new_other() {
        let event = Event::new("test-10".to_string(), ProtocolType::Other);
        assert_eq!(event.protocol, ProtocolType::Other);
    }

    // Test 11: Event with session
    #[test]
    fn test_event_with_session() {
        let event = Event::new("test-11".to_string(), ProtocolType::Http)
            .with_session("session-123".to_string());
        assert_eq!(event.session_id, Some("session-123".to_string()));
    }

    #[test]
    fn test_event_with_payload() {
        let data = "test payload";
        let event = Event::new("test-12".to_string(), ProtocolType::Http)
            .with_payload(data.to_string());
        assert!(event.payload.is_some());
        assert_eq!(event.payload.as_ref().unwrap(), data);
    }

    #[test]
    fn test_event_with_empty_payload() {
        let event = Event::new("test-13".to_string(), ProtocolType::Http)
            .with_payload("".to_string());
        assert!(event.payload.is_some());
        assert!(event.payload.as_ref().unwrap().is_empty());
    }

    // Test 14: Event timestamp is set
    #[test]
    fn test_event_timestamp_set() {
        let before = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let event = Event::new("test-14".to_string(), ProtocolType::Http);
        let after = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        assert!(event.timestamp_us >= before);
        assert!(event.timestamp_us <= after);
    }

    // Test 15: Event serialization
    #[test]
    fn test_event_serialization() {
        let event = Event::new("test-15".to_string(), ProtocolType::Http);
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("test-15"));
        assert!(json.contains("http"));
    }

    // Test 16: Event deserialization
    #[test]
    fn test_event_deserialization() {
        let json = r#"{"id":"test-16","protocol":"http","timestamp_us":12345,"latency_us":0,"severity":"info","metadata":{"source_ip":null,"destination_ip":null,"request_id":null,"user_agent":null}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        assert_eq!(event.id, "test-16");
        assert_eq!(event.protocol, ProtocolType::Http);
        assert_eq!(event.severity, EventSeverity::Info);
    }

    // Test 17: Metadata new
    #[test]
    fn test_metadata_new() {
        let meta = Metadata::new();
        assert!(meta.source_ip.is_none());
        assert!(meta.destination_ip.is_none());
        assert!(meta.extra.is_empty());
    }

    // Test 18: Metadata add extra field
    #[test]
    fn test_metadata_add() {
        let mut meta = Metadata::new();
        meta.add("key1", "value1");
        assert_eq!(meta.extra.get("key1"), Some(&"value1".to_string()));
    }

    // Test 19: Metadata add multiple fields
    #[test]
    fn test_metadata_add_multiple() {
        let mut meta = Metadata::new();
        meta.add("key1", "value1");
        meta.add("key2", "value2");
        assert_eq!(meta.extra.len(), 2);
    }

    // Test 20: Metadata serialization
    #[test]
    fn test_metadata_serialization() {
        let mut meta = Metadata::new();
        meta.add("custom_key", "custom_value");
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("custom_key"));
        assert!(json.contains("custom_value"));
    }

    // Test 201: Metadata default initialization
    #[test]
    fn test_metadata_default() {
        let meta: Metadata = Default::default();
        assert!(meta.source_ip.is_none());
        assert!(meta.destination_ip.is_none());
        assert!(meta.request_id.is_none());
        assert!(meta.user_agent.is_none());
        assert!(meta.extra.is_empty());
    }

    // Test 202: Metadata with all fields set
    #[test]
    fn test_metadata_with_all_fields() {
        let mut meta = Metadata::new();
        meta.source_ip = Some("192.168.1.1".to_string());
        meta.destination_ip = Some("10.0.0.1".to_string());
        meta.request_id = Some("req-123".to_string());
        meta.user_agent = Some("Mozilla/5.0".to_string());
        assert_eq!(meta.source_ip, Some("192.168.1.1".to_string()));
        assert_eq!(meta.destination_ip, Some("10.0.0.1".to_string()));
        assert_eq!(meta.request_id, Some("req-123".to_string()));
        assert_eq!(meta.user_agent, Some("Mozilla/5.0".to_string()));
    }

    // Test 203: Metadata add empty key and value
    #[test]
    fn test_metadata_add_empty() {
        let mut meta = Metadata::new();
        meta.add("", "");
        assert_eq!(meta.extra.get(""), Some(&"".to_string()));
    }

    // Test 204: Metadata add overwrite existing key
    #[test]
    fn test_metadata_add_overwrite() {
        let mut meta = Metadata::new();
        meta.add("key", "value1");
        meta.add("key", "value2");
        assert_eq!(meta.extra.get("key"), Some(&"value2".to_string()));
        assert_eq!(meta.extra.len(), 1);
    }

    // Test 205: Metadata add special characters
    #[test]
    fn test_metadata_add_special_chars() {
        let mut meta = Metadata::new();
        meta.add("key!@#$%", "value&*()");
        assert_eq!(meta.extra.get("key!@#$%"), Some(&"value&*()".to_string()));
    }

    // Test 206: Metadata add unicode
    #[test]
    fn test_metadata_add_unicode() {
        let mut meta = Metadata::new();
        meta.add("密钥", "值");
        assert_eq!(meta.extra.get("密钥"), Some(&"值".to_string()));
    }

    // Test 207: Metadata clone
    #[test]
    fn test_metadata_clone() {
        let mut meta = Metadata::new();
        meta.source_ip = Some("1.1.1.1".to_string());
        meta.add("key", "value");
        let cloned = meta.clone();
        assert_eq!(cloned.source_ip, meta.source_ip);
        assert_eq!(cloned.extra.get("key"), Some(&"value".to_string()));
    }

    // Test 208: Metadata debug format
    #[test]
    fn test_metadata_debug() {
        let meta = Metadata::new();
        let debug_str = format!("{:?}", meta);
        assert!(debug_str.contains("Metadata"));
    }

    // Test 209: Metadata deserialization
    #[test]
    fn test_metadata_deserialization() {
        let json = r#"{"source_ip":"1.1.1.1","destination_ip":"2.2.2.2","custom_key":"custom_value"}"#;
        let meta: Metadata = serde_json::from_str(json).unwrap();
        assert_eq!(meta.source_ip, Some("1.1.1.1".to_string()));
        assert_eq!(meta.destination_ip, Some("2.2.2.2".to_string()));
        assert_eq!(meta.extra.get("custom_key"), Some(&"custom_value".to_string()));
    }

    // Test 210: ProtocolType equality
    #[test]
    fn test_protocol_type_equality() {
        assert_eq!(ProtocolType::Http, ProtocolType::Http);
        assert_ne!(ProtocolType::Http, ProtocolType::Ws);
    }

    // Test 211: ProtocolType copy
    #[test]
    fn test_protocol_type_copy() {
        let p1 = ProtocolType::Tcp;
        let p2 = p1;
        assert_eq!(p1, p2);
    }

    // Test 212: ProtocolType debug
    #[test]
    fn test_protocol_type_debug() {
        let p = ProtocolType::Grpc;
        let debug_str = format!("{:?}", p);
        assert_eq!(debug_str, "Grpc");
    }

    // Test 213: ProtocolType serialization lowercase
    #[test]
    fn test_protocol_type_serialization_lowercase() {
        let p = ProtocolType::Http;
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, "\"http\"");
    }

    // Test 214: ProtocolType deserialization lowercase
    #[test]
    fn test_protocol_type_deserialization_lowercase() {
        let json = "\"websocket\"";
        let p: Result<ProtocolType, _> = serde_json::from_str(json);
        assert!(p.is_err());
    }

    // Test 215: ProtocolType all variants serialization
    #[test]
    fn test_protocol_type_all_variants() {
        let protocols = vec![
            ProtocolType::Http,
            ProtocolType::Ws,
            ProtocolType::Tcp,
            ProtocolType::Udp,
            ProtocolType::Grpc,
            ProtocolType::Quic,
            ProtocolType::Ebpf,
            ProtocolType::Mqtt,
            ProtocolType::Modbus,
            ProtocolType::Other,
        ];
        for p in protocols {
            let json = serde_json::to_string(&p).unwrap();
            let deserialized: ProtocolType = serde_json::from_str(&json).unwrap();
            assert_eq!(p, deserialized);
        }
    }

    // Test 216: Event with all optional fields set
    #[test]
    fn test_event_with_all_fields() {
        let mut event = Event::new("test-all".to_string(), ProtocolType::Http);
        event.session_id = Some("session-123".to_string());
        event.project_id = Some("proj-456".to_string());
        event.environment_id = Some("env-789".to_string());
        event.service_id = Some("svc-abc".to_string());
        event.latency_us = 5000;
        event.payload = Some("payload".to_string());
        assert_eq!(event.session_id, Some("session-123".to_string()));
        assert_eq!(event.project_id, Some("proj-456".to_string()));
        assert_eq!(event.environment_id, Some("env-789".to_string()));
        assert_eq!(event.service_id, Some("svc-abc".to_string()));
        assert_eq!(event.latency_us, 5000);
        assert_eq!(event.payload, Some("payload".to_string()));
    }

    // Test 217: Event builder pattern chaining
    #[test]
    fn test_event_builder_chaining() {
        let event = Event::new("chain".to_string(), ProtocolType::Grpc)
            .with_session("sess".to_string())
            .with_payload("data".to_string());
        assert_eq!(event.session_id, Some("sess".to_string()));
        assert_eq!(event.payload, Some("data".to_string()));
    }

    // Test 218: Event with empty id
    #[test]
    fn test_event_empty_id() {
        let event = Event::new("".to_string(), ProtocolType::Other);
        assert_eq!(event.id, "");
    }

    // Test 219: Event with zero latency
    #[test]
    fn test_event_zero_latency() {
        let event = Event::new("test".to_string(), ProtocolType::Tcp);
        assert_eq!(event.latency_us, 0);
    }

    // Test 220: Event with max latency
    #[test]
    fn test_event_max_latency() {
        let mut event = Event::new("test".to_string(), ProtocolType::Udp);
        event.latency_us = u64::MAX;
        assert_eq!(event.latency_us, u64::MAX);
    }

    // Test 221: Event clone
    #[test]
    fn test_event_clone() {
        let event = Event::new("orig".to_string(), ProtocolType::Http)
            .with_session("sess".to_string())
            .with_payload("data".to_string());
        let cloned = event.clone();
        assert_eq!(cloned.id, event.id);
        assert_eq!(cloned.session_id, event.session_id);
        assert_eq!(cloned.payload, event.payload);
        assert_eq!(cloned.protocol, event.protocol);
    }

    // Test 222: Event debug format
    #[test]
    fn test_event_debug() {
        let event = Event::new("dbg".to_string(), ProtocolType::Http);
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("dbg"));
        assert!(debug_str.contains("Http"));
    }

    // Test 223: Event serialization with all fields
    #[test]
    fn test_event_serialization_all_fields() {
        let mut event = Event::new("full".to_string(), ProtocolType::Ws);
        event.session_id = Some("sess".to_string());
        event.project_id = Some("proj".to_string());
        event.environment_id = Some("env".to_string());
        event.service_id = Some("svc".to_string());
        event.latency_us = 1000;
        event.payload = Some("payload".to_string());
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("full"));
        assert!(json.contains("ws"));
        assert!(json.contains("sess"));
        assert!(json.contains("proj"));
    }

    // Test 224: Event deserialization with all fields
    #[test]
    fn test_event_deserialization_all_fields() {
        let json = r#"{"id":"full","session_id":"sess","project_id":"proj","environment_id":"env","service_id":"svc","timestamp_us":12345,"protocol":"http","latency_us":1000,"severity":"critical","payload":"data","metadata":{}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        assert_eq!(event.id, "full");
        assert_eq!(event.session_id, Some("sess".to_string()));
        assert_eq!(event.project_id, Some("proj".to_string()));
        assert_eq!(event.environment_id, Some("env".to_string()));
        assert_eq!(event.service_id, Some("svc".to_string()));
        assert_eq!(event.timestamp_us, 12345);
        assert_eq!(event.latency_us, 1000);
        assert_eq!(event.severity, EventSeverity::Critical);
        assert_eq!(event.payload, Some("data".to_string()));
    }

    // Test 225: Event serialization skips None payload
    #[test]
    fn test_event_serialization_skips_none_payload() {
        let event = Event::new("test-event".to_string(), ProtocolType::Http);
        let json = serde_json::to_string(&event).unwrap();
        // Check that "payload" field is not present (look for the key, not substring)
        assert!(!json.contains("\"payload\":"));
    }

    // Test 226: Event with large payload
    #[test]
    fn test_event_large_payload() {
        let large = "x".repeat(10000);
        let event = Event::new("large".to_string(), ProtocolType::Http)
            .with_payload(large.clone());
        assert_eq!(event.payload, Some(large));
    }

    // Test 227: Event with unicode payload
    #[test]
    fn test_event_unicode_payload() {
        let event = Event::new("unicode".to_string(), ProtocolType::Http)
            .with_payload("Hello 世界 🌍".to_string());
        assert_eq!(event.payload, Some("Hello 世界 🌍".to_string()));
    }

    // Test 228: Event with special characters in id
    #[test]
    fn test_event_special_chars_id() {
        let id = "test-123_abc.xyz!@#".to_string();
        let event = Event::new(id.clone(), ProtocolType::Other);
        assert_eq!(event.id, id);
    }

    // Test 229: Event with empty session
    #[test]
    fn test_event_empty_session() {
        let event = Event::new("test".to_string(), ProtocolType::Http)
            .with_session("".to_string());
        assert_eq!(event.session_id, Some("".to_string()));
    }

    // Test 230: Event metadata is initialized
    #[test]
    fn test_event_metadata_initialized() {
        let event = Event::new("test".to_string(), ProtocolType::Http);
        assert!(event.metadata.extra.is_empty());
    }

    // Test 231: Event with metadata modifications
    #[test]
    fn test_event_with_metadata_modifications() {
        let mut event = Event::new("test".to_string(), ProtocolType::Http);
        event.metadata.add("key", "value");
        assert_eq!(event.metadata.extra.get("key"), Some(&"value".to_string()));
    }

    // Test 232: Multiple events have unique timestamps
    #[test]
    fn test_event_unique_timestamps() {
        let event1 = Event::new("e1".to_string(), ProtocolType::Http);
        std::thread::sleep(std::time::Duration::from_millis(1));
        let event2 = Event::new("e2".to_string(), ProtocolType::Http);
        assert!(event2.timestamp_us >= event1.timestamp_us);
    }

    // Test 233: Event deserialization missing optional fields
    #[test]
    fn test_event_deserialization_missing_optional() {
        let json = r#"{"id":"minimal","timestamp_us":12345,"protocol":"tcp","latency_us":0,"severity":"low","metadata":{}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        assert_eq!(event.id, "minimal");
        assert!(event.session_id.is_none());
        assert!(event.payload.is_none());
    }

    // Test 234: Event round-trip serialization
    #[test]
    fn test_event_round_trip() {
        let mut original = Event::new("roundtrip".to_string(), ProtocolType::Grpc);
        original.session_id = Some("sess".to_string());
        original.latency_us = 500;
        original.payload = Some("data".to_string());
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(original.id, deserialized.id);
        assert_eq!(original.session_id, deserialized.session_id);
        assert_eq!(original.protocol, deserialized.protocol);
        assert_eq!(original.latency_us, deserialized.latency_us);
    }

    // Test 235: ProtocolType all variants debug format
    #[test]
    fn test_protocol_type_all_variants_debug() {
        assert_eq!(format!("{:?}", ProtocolType::Http), "Http");
        assert_eq!(format!("{:?}", ProtocolType::Ws), "Ws");
        assert_eq!(format!("{:?}", ProtocolType::Tcp), "Tcp");
        assert_eq!(format!("{:?}", ProtocolType::Udp), "Udp");
        assert_eq!(format!("{:?}", ProtocolType::Grpc), "Grpc");
        assert_eq!(format!("{:?}", ProtocolType::Quic), "Quic");
        assert_eq!(format!("{:?}", ProtocolType::Ebpf), "Ebpf");
        assert_eq!(format!("{:?}", ProtocolType::Mqtt), "Mqtt");
        assert_eq!(format!("{:?}", ProtocolType::Modbus), "Modbus");
        assert_eq!(format!("{:?}", ProtocolType::Other), "Other");
    }

    // Test 236: Metadata serialization with all standard fields
    #[test]
    fn test_metadata_serialization_all_fields() {
        let mut meta = Metadata::new();
        meta.source_ip = Some("1.1.1.1".to_string());
        meta.destination_ip = Some("2.2.2.2".to_string());
        meta.request_id = Some("req-id".to_string());
        meta.user_agent = Some("agent".to_string());
        meta.add("custom", "value");
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("1.1.1.1"));
        assert!(json.contains("2.2.2.2"));
        assert!(json.contains("req-id"));
        assert!(json.contains("agent"));
        assert!(json.contains("custom"));
    }

    // Test 237: Metadata round-trip
    #[test]
    fn test_metadata_round_trip() {
        let mut original = Metadata::new();
        original.source_ip = Some("1.1.1.1".to_string());
        original.add("key", "value");
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Metadata = serde_json::from_str(&json).unwrap();
        assert_eq!(original.source_ip, deserialized.source_ip);
        assert_eq!(original.extra.get("key"), deserialized.extra.get("key"));
    }

    // Test 238: Event with ProtocolType Quic
    #[test]
    fn test_event_quic() {
        let event = Event::new("quic-test".to_string(), ProtocolType::Quic);
        assert_eq!(event.protocol, ProtocolType::Quic);
    }

    // Test 239: Event with ProtocolType Ebpf
    #[test]
    fn test_event_ebpf() {
        let event = Event::new("ebpf-test".to_string(), ProtocolType::Ebpf);
        assert_eq!(event.protocol, ProtocolType::Ebpf);
    }

    // Test 240: Event with ProtocolType Mqtt
    #[test]
    fn test_event_mqtt() {
        let event = Event::new("mqtt-test".to_string(), ProtocolType::Mqtt);
        assert_eq!(event.protocol, ProtocolType::Mqtt);
    }

    // Test 241: Event with ProtocolType Modbus
    #[test]
    fn test_event_modbus() {
        let event = Event::new("modbus-test".to_string(), ProtocolType::Modbus);
        assert_eq!(event.protocol, ProtocolType::Modbus);
    }

    // Test 242: Event payload None by default
    #[test]
    fn test_event_payload_none_default() {
        let event = Event::new("test".to_string(), ProtocolType::Http);
        assert!(event.payload.is_none());
    }

    // Test 243: Event timestamp_us is u64
    #[test]
    fn test_event_timestamp_u64() {
        let event = Event::new("test".to_string(), ProtocolType::Http);
        assert!(event.timestamp_us > 0);
    }

    // Test 244: Metadata extra field flatten behavior
    #[test]
    fn test_metadata_extra_flatten() {
        let mut meta = Metadata::new();
        meta.add("flattened_key", "flattened_value");
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("flattened_key"));
        assert!(!json.contains("extra"));
    }

    // Test 245: Event partial equality check
    #[test]
    fn test_event_clone_equality() {
        let event1 = Event::new("same".to_string(), ProtocolType::Http);
        let event2 = event1.clone();
        assert_eq!(event1.id, event2.id);
        assert_eq!(event1.protocol, event2.protocol);
    }

    // Test 246: Metadata partial equality
    #[test]
    fn test_metadata_partial_eq() {
        let m1 = Metadata::new();
        let m2 = Metadata::new();
        assert_eq!(m1.source_ip, m2.source_ip);
        assert_eq!(m1.extra, m2.extra);
    }

    // Test 247: ProtocolType ordering check
    #[test]
    fn test_protocol_type_variants_distinct() {
        let variants = vec![
            ProtocolType::Http,
            ProtocolType::Ws,
            ProtocolType::Tcp,
            ProtocolType::Udp,
            ProtocolType::Grpc,
            ProtocolType::Quic,
            ProtocolType::Ebpf,
            ProtocolType::Mqtt,
            ProtocolType::Modbus,
            ProtocolType::Other,
        ];
        for i in 0..variants.len() {
            for j in (i+1)..variants.len() {
                assert_ne!(variants[i], variants[j]);
            }
        }
    }

    // Test 248: Event with multiple builder calls
    #[test]
    fn test_event_multiple_builder_calls() {
        let event = Event::new("test".to_string(), ProtocolType::Http)
            .with_session("s1".to_string())
            .with_session("s2".to_string());
        assert_eq!(event.session_id, Some("s2".to_string()));
    }

    // Test 249: Metadata with many extra fields
    #[test]
    fn test_metadata_many_extra_fields() {
        let mut meta = Metadata::new();
        for i in 0..100 {
            meta.add(&format!("key{}", i), &format!("value{}", i));
        }
        assert_eq!(meta.extra.len(), 100);
    }

    // Test 250: Event serialization json structure
    #[test]
    fn test_event_json_structure() {
        let event = Event::new("struct-test".to_string(), ProtocolType::Http);
        let json = serde_json::to_string(&event).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("id").is_some());
        assert!(parsed.get("protocol").is_some());
        assert!(parsed.get("timestamp_us").is_some());
    }
}
