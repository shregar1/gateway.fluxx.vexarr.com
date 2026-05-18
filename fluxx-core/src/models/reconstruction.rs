use crate::models::event::Event;
use crate::models::event::ProtocolType;

/// Unified logic for reconstructing protocols from events to avoid logic duplication
/// across capture and replay modules.
pub struct EventReconstruction;

impl EventReconstruction {
    /// Maps generic Fluxx events to human-readable protocol descriptions.
    pub fn describe(event: &Event) -> String {
        match event.protocol {
            ProtocolType::Http => format!("HTTP POST ({} bytes)", event.payload.as_ref().map(|p| p.len()).unwrap_or(0)),
            ProtocolType::Ws => format!("WebSocket Message ({} bytes)", event.payload.as_ref().map(|p| p.len()).unwrap_or(0)),
            ProtocolType::Tcp => format!("TCP Segment from {}", event.metadata.source_ip.as_deref().unwrap_or("unknown")),
            ProtocolType::Udp => format!("UDP Datagram from {}", event.metadata.source_ip.as_deref().unwrap_or("unknown")),
            _ => format!("Protocol {:?} event", event.protocol),
        }
    }

    // Future: Reconstruct full multi-event HTTP streams or WebSocket sessions here.
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 188: Describe HTTP event
    #[test]
    fn test_describe_http() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("test body".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("HTTP POST"));
        assert!(desc.contains("9 bytes"));
    }

    // Test 189: Describe HTTP with no payload
    #[test]
    fn test_describe_http_no_payload() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Http);
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("0 bytes"));
    }

    // Test 190: Describe WebSocket
    #[test]
    fn test_describe_ws() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Ws)
            .with_payload("ws data".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("WebSocket Message"));
    }

    // Test 191: Describe TCP with source IP
    #[test]
    fn test_describe_tcp_with_ip() {
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Tcp);
        event.metadata.source_ip = Some("192.168.1.1".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("192.168.1.1"));
    }

    // Test 192: Describe TCP without source IP
    #[test]
    fn test_describe_tcp_no_ip() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Tcp);
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("unknown"));
    }

    // Test 193: Describe UDP
    #[test]
    fn test_describe_udp() {
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Udp);
        event.metadata.source_ip = Some("10.0.0.1".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("UDP Datagram"));
        assert!(desc.contains("10.0.0.1"));
    }

    // Test 194: Describe other protocols
    #[test]
    fn test_describe_other() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Grpc);
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("Grpc"));
    }

    // Test 381: Describe Quic protocol
    #[test]
    fn test_describe_quic() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Quic);
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("Quic"));
    }

    // Test 382: Describe Ebpf protocol
    #[test]
    fn test_describe_ebpf() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("Ebpf"));
    }

    // Test 383: Describe Mqtt protocol
    #[test]
    fn test_describe_mqtt() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Mqtt);
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("Mqtt"));
    }

    // Test 384: Describe Modbus protocol
    #[test]
    fn test_describe_modbus() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("Modbus"));
    }

    // Test 385: Describe Other protocol
    #[test]
    fn test_describe_other_protocol() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Other);
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("Other"));
    }

    // Test 386: Describe HTTP with large payload
    #[test]
    fn test_describe_http_large_payload() {
        let large = "x".repeat(10000);
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload(large);
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("10000 bytes"));
    }

    // Test 387: Describe HTTP with unicode payload
    #[test]
    fn test_describe_http_unicode_payload() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("Hello 世界".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("HTTP POST"));
    }

    // Test 388: Describe TCP without metadata
    #[test]
    fn test_describe_tcp_no_metadata() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Tcp);
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("unknown"));
    }

    // Test 389: Describe TCP with empty source IP
    #[test]
    fn test_describe_tcp_empty_ip() {
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Tcp);
        event.metadata.source_ip = Some("".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("TCP Segment"));
    }

    // Test 390: Describe UDP with empty source IP
    #[test]
    fn test_describe_udp_empty_ip() {
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Udp);
        event.metadata.source_ip = Some("".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("UDP Datagram"));
    }

    // Test 391: Describe WebSocket with empty payload
    #[test]
    fn test_describe_ws_empty_payload() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Ws)
            .with_payload("".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("0 bytes"));
    }

    // Test 392: Describe HTTP with single character payload
    #[test]
    fn test_describe_http_single_char() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("x".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("1 bytes"));
    }

    // Test 393: Describe all protocols have output
    #[test]
    fn test_describe_all_protocols_output() {
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
            let event = Event::new("evt".to_string(), p);
            let desc = EventReconstruction::describe(&event);
            assert!(!desc.is_empty());
        }
    }

    // Test 394: Describe WebSocket with special chars
    #[test]
    fn test_describe_ws_special_chars() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Ws)
            .with_payload("!@#$%^&*()".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("WebSocket Message"));
    }

    // Test 395: Describe TCP with IPv6 address
    #[test]
    fn test_describe_tcp_ipv6() {
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Tcp);
        event.metadata.source_ip = Some("2001:db8::1".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("2001:db8::1"));
    }

    // Test 396: Describe UDP with IPv6 address
    #[test]
    fn test_describe_udp_ipv6() {
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Udp);
        event.metadata.source_ip = Some("fe80::1".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.contains("fe80::1"));
    }

    // Test 397: EventReconstruction is Send + Sync
    #[test]
    fn test_event_reconstruction_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EventReconstruction>();
    }

    // Test 398: Describe HTTP format check
    #[test]
    fn test_describe_http_format() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("test".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.starts_with("HTTP POST"));
        assert!(desc.contains("bytes)"));
    }

    // Test 399: Describe WebSocket format check
    #[test]
    fn test_describe_ws_format() {
        let event = Event::new("evt-1".to_string(), ProtocolType::Ws)
            .with_payload("data".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.starts_with("WebSocket Message"));
    }

    // Test 400: Describe TCP format check
    #[test]
    fn test_describe_tcp_format() {
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Tcp);
        event.metadata.source_ip = Some("1.1.1.1".to_string());
        let desc = EventReconstruction::describe(&event);
        assert!(desc.starts_with("TCP Segment from"));
    }
}
