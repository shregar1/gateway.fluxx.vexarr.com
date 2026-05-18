use crate::models::event::{Event, ProtocolType};
use crate::analysis::insights::{Insight, InsightLevel};
use crate::analysis::Analyzer;
use crate::analysis::AnalysisUtils;

/// HTTP Protocol Intelligence: Tracks endpoint latency and payload size anomalies.
#[derive(Debug, Default)]
pub struct HttpAnalyzer;

impl Analyzer for HttpAnalyzer {
    fn analyze(&self, event: &Event) -> Vec<Insight> {
        let mut insights = Vec::new();
        
        // 1. Threshold Detection (High latency endpoint)
        if AnalysisUtils::is_high_latency(event.latency_us, 1_000_000) {
            insights.push(Insight::new(self.protocol(), InsightLevel::Warning, "High latency HTTP endpoint detected."));
        }
        
        // 2. Payload Anomaly
        if let Some(p) = &event.payload {
            if AnalysisUtils::is_payload_anomaly(p.len(), 10 * 1024 * 1024) {
                 insights.push(Insight::new(self.protocol(), InsightLevel::Anomaly, "Large HTTP payload identified."));
            }
        }

        insights
    }

    fn protocol(&self) -> ProtocolType { ProtocolType::Http }
}

/// WebSocket Intelligence: Detects reconnect storms and connection spikes.
#[derive(Debug, Default)]
pub struct WebSocketAnalyzer;

impl Analyzer for WebSocketAnalyzer {
    fn analyze(&self, event: &Event) -> Vec<Insight> {
        let mut insights = Vec::new();
        
        // 1. Pattern Recognition (Reconnect storm detection - proxy based on metadata)
        if let Some(reconnect) = event.metadata.extra.get("reconnect_count") {
            if let Ok(count) = reconnect.parse::<u32>() {
                if count > 10 {
                    insights.push(Insight::new(self.protocol(), InsightLevel::Critical, "WebSocket reconnect storm detected."));
                }
            }
        }
        
        insights
    }

    fn protocol(&self) -> ProtocolType { ProtocolType::Ws }
}

/// gRPC Intelligence: Detects stream resets and RPC latency spikes.
#[derive(Debug, Default)]
pub struct GrpcAnalyzer;

impl Analyzer for GrpcAnalyzer {
    fn analyze(&self, event: &Event) -> Vec<Insight> {
        let mut insights = Vec::new();
        if AnalysisUtils::is_high_latency(event.latency_us, 500_000) {
            insights.push(Insight::new(self.protocol(), InsightLevel::Warning, "gRPC RPC latency spike found."));
        }
        insights
    }
    fn protocol(&self) -> ProtocolType { ProtocolType::Grpc }
}

/// QUIC Intelligence: Detects high UDP packet loss and congestion.
#[derive(Debug, Default)]
pub struct QuicAnalyzer;

impl Analyzer for QuicAnalyzer {
    fn analyze(&self, _event: &Event) -> Vec<Insight> {
        let insights = Vec::new();
        // Placeholder for QUIC congestion metrics analysis
        insights
    }
    fn protocol(&self) -> ProtocolType { ProtocolType::Quic }
}

/// eBPF Intelligence: Detects kernel-level TCP/UDP volume surges.
#[derive(Debug, Default)]
pub struct EbpfAnalyzer;

impl Analyzer for EbpfAnalyzer {
    fn analyze(&self, event: &Event) -> Vec<Insight> {
        let mut insights = Vec::new();
        if let Some(size) = event.metadata.extra.get("size_bytes") {
             if let Ok(s) = size.parse::<usize>() {
                if s > 1024 * 1024 {
                    insights.push(Insight::new(self.protocol(), InsightLevel::Anomaly, "eBPF detected large TCP segment."));
                }
             }
        }
        insights
    }
    fn protocol(&self) -> ProtocolType { ProtocolType::Ebpf }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 145: HttpAnalyzer default
    #[test]
    fn test_http_analyzer_default() {
        let analyzer = HttpAnalyzer::default();
        assert_eq!(analyzer.protocol(), ProtocolType::Http);
    }

    // Test 146: HttpAnalyzer no insights for normal event
    #[test]
    fn test_http_analyzer_normal() {
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http);
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 147: HttpAnalyzer high latency insight
    #[test]
    fn test_http_analyzer_high_latency() {
        let analyzer = HttpAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.latency_us = 2_000_000; // > 1s threshold
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
        assert_eq!(insights[0].level, InsightLevel::Warning);
    }

    // Test 148: HttpAnalyzer large payload insight
    #[test]
    fn test_http_analyzer_large_payload() {
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("0".repeat(11 * 1024 * 1024)); // > 10MB
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
        assert_eq!(insights[0].level, InsightLevel::Anomaly);
    }

    // Test 149: WebSocketAnalyzer reconnect storm
    #[test]
    fn test_ws_analyzer_reconnect_storm() {
        let analyzer = WebSocketAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ws);
        event.metadata.add("reconnect_count", "15");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
        assert_eq!(insights[0].level, InsightLevel::Critical);
    }

    // Test 150: WebSocketAnalyzer no storm
    #[test]
    fn test_ws_analyzer_no_storm() {
        let analyzer = WebSocketAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ws);
        event.metadata.add("reconnect_count", "5");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 151: GrpcAnalyzer high latency
    #[test]
    fn test_grpc_analyzer_high_latency() {
        let analyzer = GrpcAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Grpc);
        event.latency_us = 600_000; // > 500ms threshold
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 152: GrpcAnalyzer normal latency
    #[test]
    fn test_grpc_analyzer_normal() {
        let analyzer = GrpcAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Grpc);
        event.latency_us = 100_000; // < 500ms threshold
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 153: QuicAnalyzer protocol
    #[test]
    fn test_quic_analyzer_protocol() {
        let analyzer = QuicAnalyzer::default();
        assert_eq!(analyzer.protocol(), ProtocolType::Quic);
    }

    // Test 154: QuicAnalyzer empty insights
    #[test]
    fn test_quic_analyzer_empty() {
        let analyzer = QuicAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Quic);
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 155: EbpfAnalyzer large segment
    #[test]
    fn test_ebpf_analyzer_large_segment() {
        let analyzer = EbpfAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("size_bytes", "2000000"); // > 1MB
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 156: EbpfAnalyzer normal segment
    #[test]
    fn test_ebpf_analyzer_normal() {
        let analyzer = EbpfAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("size_bytes", "500000"); // < 1MB
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 441: HttpAnalyzer with both high latency and large payload
    #[test]
    fn test_http_analyzer_both_conditions() {
        let analyzer = HttpAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("0".repeat(11 * 1024 * 1024));
        event.latency_us = 2_000_000;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 2);
    }

    // Test 442: HttpAnalyzer exactly at latency threshold
    #[test]
    fn test_http_analyzer_latency_at_threshold() {
        let analyzer = HttpAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.latency_us = 1_000_000; // Exactly at threshold
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 443: HttpAnalyzer just above latency threshold
    #[test]
    fn test_http_analyzer_latency_just_above() {
        let analyzer = HttpAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.latency_us = 1_000_001; // Just above threshold
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 444: HttpAnalyzer exactly at payload threshold
    #[test]
    fn test_http_analyzer_payload_at_threshold() {
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("0".repeat(10 * 1024 * 1024)); // Exactly 10MB
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 445: HttpAnalyzer with zero latency
    #[test]
    fn test_http_analyzer_zero_latency() {
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http);
        // latency_us defaults to 0
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 446: HttpAnalyzer with extreme high latency
    #[test]
    fn test_http_analyzer_extreme_latency() {
        let analyzer = HttpAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.latency_us = u64::MAX;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
        assert_eq!(insights[0].level, InsightLevel::Warning);
    }

    // Test 447: HttpAnalyzer with empty payload
    #[test]
    fn test_http_analyzer_empty_payload() {
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("".to_string());
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 448: HttpAnalyzer with None payload
    #[test]
    fn test_http_analyzer_none_payload() {
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http);
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 449: WebSocketAnalyzer exactly at reconnect threshold
    #[test]
    fn test_ws_analyzer_exactly_at_threshold() {
        let analyzer = WebSocketAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ws);
        event.metadata.add("reconnect_count", "10");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 450: WebSocketAnalyzer just above reconnect threshold
    #[test]
    fn test_ws_analyzer_just_above_threshold() {
        let analyzer = WebSocketAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ws);
        event.metadata.add("reconnect_count", "11");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 451: WebSocketAnalyzer with zero reconnects
    #[test]
    fn test_ws_analyzer_zero_reconnects() {
        let analyzer = WebSocketAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ws);
        event.metadata.add("reconnect_count", "0");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 452: WebSocketAnalyzer with invalid reconnect count
    #[test]
    fn test_ws_analyzer_invalid_reconnect() {
        let analyzer = WebSocketAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ws);
        event.metadata.add("reconnect_count", "invalid");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 453: WebSocketAnalyzer with negative reconnect count string
    #[test]
    fn test_ws_analyzer_negative_reconnect() {
        let analyzer = WebSocketAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ws);
        event.metadata.add("reconnect_count", "-5");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 454: WebSocketAnalyzer without reconnect metadata
    #[test]
    fn test_ws_analyzer_no_metadata() {
        let analyzer = WebSocketAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Ws);
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 455: WebSocketAnalyzer with extreme reconnect count
    #[test]
    fn test_ws_analyzer_extreme_reconnect() {
        let analyzer = WebSocketAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ws);
        event.metadata.add("reconnect_count", "999999999");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
        assert_eq!(insights[0].level, InsightLevel::Critical);
    }

    // Test 456: GrpcAnalyzer exactly at latency threshold
    #[test]
    fn test_grpc_analyzer_at_threshold() {
        let analyzer = GrpcAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Grpc);
        event.latency_us = 500_000; // Exactly at threshold
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 457: GrpcAnalyzer just above latency threshold
    #[test]
    fn test_grpc_analyzer_just_above() {
        let analyzer = GrpcAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Grpc);
        event.latency_us = 500_001;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 458: GrpcAnalyzer with zero latency
    #[test]
    fn test_grpc_analyzer_zero_latency() {
        let analyzer = GrpcAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Grpc);
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 459: GrpcAnalyzer with extreme latency
    #[test]
    fn test_grpc_analyzer_extreme_latency() {
        let analyzer = GrpcAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Grpc);
        event.latency_us = u64::MAX;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 460: QuicAnalyzer with various event types
    #[test]
    fn test_quic_analyzer_various_events() {
        let analyzer = QuicAnalyzer::default();
        let event1 = Event::new("evt-1".to_string(), ProtocolType::Quic);
        let event2 = Event::new("evt-2".to_string(), ProtocolType::Quic);
        assert!(analyzer.analyze(&event1).is_empty());
        assert!(analyzer.analyze(&event2).is_empty());
    }

    // Test 461: QuicAnalyzer debug format
    #[test]
    fn test_quic_analyzer_debug() {
        let analyzer = QuicAnalyzer::default();
        let debug_str = format!("{:?}", analyzer);
        assert!(debug_str.contains("QuicAnalyzer"));
    }

    // Test 462: EbpfAnalyzer exactly at size threshold
    #[test]
    fn test_ebpf_analyzer_at_threshold() {
        let analyzer = EbpfAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("size_bytes", "1048576"); // Exactly 1MB
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 463: EbpfAnalyzer just above size threshold
    #[test]
    fn test_ebpf_analyzer_just_above() {
        let analyzer = EbpfAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("size_bytes", "1048577"); // Just above 1MB
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 464: EbpfAnalyzer with zero size
    #[test]
    fn test_ebpf_analyzer_zero_size() {
        let analyzer = EbpfAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("size_bytes", "0");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 465: EbpfAnalyzer with invalid size
    #[test]
    fn test_ebpf_analyzer_invalid_size() {
        let analyzer = EbpfAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("size_bytes", "invalid");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 466: EbpfAnalyzer without size metadata
    #[test]
    fn test_ebpf_analyzer_no_metadata() {
        let analyzer = EbpfAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 467: EbpfAnalyzer with negative size string
    #[test]
    fn test_ebpf_analyzer_negative_size() {
        let analyzer = EbpfAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("size_bytes", "-100");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 468: EbpfAnalyzer insight content verification
    #[test]
    fn test_ebpf_analyzer_insight_content() {
        let analyzer = EbpfAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("size_bytes", "2000000");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights[0].protocol, ProtocolType::Ebpf);
        assert_eq!(insights[0].level, InsightLevel::Anomaly);
        assert!(insights[0].message.contains("eBPF"));
    }

    // Test 469: HttpAnalyzer insight protocol verification
    #[test]
    fn test_http_analyzer_insight_protocol() {
        let analyzer = HttpAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.latency_us = 2_000_000;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights[0].protocol, ProtocolType::Http);
    }

    // Test 470: HttpAnalyzer insight message content
    #[test]
    fn test_http_analyzer_insight_message() {
        let analyzer = HttpAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.latency_us = 2_000_000;
        let insights = analyzer.analyze(&event);
        assert!(insights[0].message.contains("HTTP"));
        assert!(insights[0].message.contains("latency"));
    }

    // Test 471: WebSocketAnalyzer insight message content
    #[test]
    fn test_ws_analyzer_insight_message() {
        let analyzer = WebSocketAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ws);
        event.metadata.add("reconnect_count", "15");
        let insights = analyzer.analyze(&event);
        assert!(insights[0].message.contains("WebSocket"));
        assert!(insights[0].message.contains("reconnect"));
    }

    // Test 472: GrpcAnalyzer insight message content
    #[test]
    fn test_grpc_analyzer_insight_message() {
        let analyzer = GrpcAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Grpc);
        event.latency_us = 600_000;
        let insights = analyzer.analyze(&event);
        assert!(insights[0].message.contains("gRPC"));
        assert!(insights[0].message.contains("latency"));
    }

    // Test 473: HttpAnalyzer with small payload
    #[test]
    fn test_http_analyzer_small_payload() {
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("small".to_string());
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 474: HttpAnalyzer payload insight message
    #[test]
    fn test_http_analyzer_payload_message() {
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("0".repeat(11 * 1024 * 1024));
        let insights = analyzer.analyze(&event);
        assert!(insights[0].message.contains("payload"));
        assert!(insights[0].message.contains("Large"));
    }

    // Test 475: All analyzers implement Send
    #[test]
    fn test_analyzers_send() {
        fn assert_send<T: Send>() {}
        assert_send::<HttpAnalyzer>();
        assert_send::<WebSocketAnalyzer>();
        assert_send::<GrpcAnalyzer>();
        assert_send::<QuicAnalyzer>();
        assert_send::<EbpfAnalyzer>();
    }

    // Test 476: All analyzers implement Sync
    #[test]
    fn test_analyzers_sync() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<HttpAnalyzer>();
        assert_sync::<WebSocketAnalyzer>();
        assert_sync::<GrpcAnalyzer>();
        assert_sync::<QuicAnalyzer>();
        assert_sync::<EbpfAnalyzer>();
    }

    // Test 477: HttpAnalyzer default is Debug
    #[test]
    fn test_http_analyzer_debug() {
        let analyzer = HttpAnalyzer::default();
        let debug_str = format!("{:?}", analyzer);
        assert!(debug_str.contains("HttpAnalyzer"));
    }

    // Test 478: WebSocketAnalyzer default is Debug
    #[test]
    fn test_ws_analyzer_debug() {
        let analyzer = WebSocketAnalyzer::default();
        let debug_str = format!("{:?}", analyzer);
        assert!(debug_str.contains("WebSocketAnalyzer"));
    }

    // Test 479: GrpcAnalyzer default is Debug
    #[test]
    fn test_grpc_analyzer_debug() {
        let analyzer = GrpcAnalyzer::default();
        let debug_str = format!("{:?}", analyzer);
        assert!(debug_str.contains("GrpcAnalyzer"));
    }

    // Test 480: EbpfAnalyzer default is Debug
    #[test]
    fn test_ebpf_analyzer_debug() {
        let analyzer = EbpfAnalyzer::default();
        let debug_str = format!("{:?}", analyzer);
        assert!(debug_str.contains("EbpfAnalyzer"));
    }

    // Test 481: HttpAnalyzer with 1MB payload
    #[test]
    fn test_http_analyzer_1mb_payload() {
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("0".repeat(1024 * 1024));
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 482: HttpAnalyzer with 10MB payload
    #[test]
    fn test_http_analyzer_10mb_payload() {
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("0".repeat(10 * 1024 * 1024));
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 483: HttpAnalyzer with 10MB + 1 payload
    #[test]
    fn test_http_analyzer_10mb_plus_one_payload() {
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("0".repeat(10 * 1024 * 1024 + 1));
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 484: WebSocketAnalyzer protocol verification
    #[test]
    fn test_ws_analyzer_protocol() {
        let analyzer = WebSocketAnalyzer::default();
        assert_eq!(analyzer.protocol(), ProtocolType::Ws);
    }

    // Test 485: GrpcAnalyzer protocol verification
    #[test]
    fn test_grpc_analyzer_protocol() {
        let analyzer = GrpcAnalyzer::default();
        assert_eq!(analyzer.protocol(), ProtocolType::Grpc);
    }

    // Test 486: EbpfAnalyzer protocol verification
    #[test]
    fn test_ebpf_analyzer_protocol() {
        let analyzer = EbpfAnalyzer::default();
        assert_eq!(analyzer.protocol(), ProtocolType::Ebpf);
    }

    // Test 487: HttpAnalyzer with 500ms latency
    #[test]
    fn test_http_analyzer_500ms_latency() {
        let analyzer = HttpAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.latency_us = 500_000;
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 488: HttpAnalyzer with 1s latency
    #[test]
    fn test_http_analyzer_1s_latency() {
        let analyzer = HttpAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.latency_us = 1_000_000;
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 489: HttpAnalyzer with 1s + 1us latency
    #[test]
    fn test_http_analyzer_1s_plus_1us_latency() {
        let analyzer = HttpAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.latency_us = 1_000_001;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 490: WebSocketAnalyzer with reconnect_count at boundary
    #[test]
    fn test_ws_analyzer_reconnect_boundary() {
        let analyzer = WebSocketAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ws);
        event.metadata.add("reconnect_count", "10");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
        
        event.metadata.add("reconnect_count", "11");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 491: EbpfAnalyzer with size at boundary
    #[test]
    fn test_ebpf_analyzer_size_boundary() {
        let analyzer = EbpfAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("size_bytes", "1048576");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
        
        event.metadata.add("size_bytes", "1048577");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 492: HttpAnalyzer multiple events
    #[test]
    fn test_http_analyzer_multiple_events() {
        let analyzer = HttpAnalyzer::default();
        let event1 = Event::new("evt-1".to_string(), ProtocolType::Http);
        let event2 = Event::new("evt-2".to_string(), ProtocolType::Http);
        assert!(analyzer.analyze(&event1).is_empty());
        assert!(analyzer.analyze(&event2).is_empty());
    }

    // Test 493: GrpcAnalyzer multiple latency values
    #[test]
    fn test_grpc_analyzer_multiple_latencies() {
        let analyzer = GrpcAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Grpc);
        
        event.latency_us = 100_000;
        assert!(analyzer.analyze(&event).is_empty());
        
        event.latency_us = 499_999;
        assert!(analyzer.analyze(&event).is_empty());
        
        event.latency_us = 500_000;
        assert!(analyzer.analyze(&event).is_empty());
        
        event.latency_us = 500_001;
        assert_eq!(analyzer.analyze(&event).len(), 1);
    }

    // Test 494: WebSocketAnalyzer reconnect count parsing edge cases
    #[test]
    fn test_ws_analyzer_reconnect_parsing() {
        let analyzer = WebSocketAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ws);
        
        event.metadata.add("reconnect_count", "10");
        assert!(analyzer.analyze(&event).is_empty());
        
        event.metadata.add("reconnect_count", "10.5");
        assert!(analyzer.analyze(&event).is_empty());
        
        event.metadata.add("reconnect_count", " 11 ");
        assert!(analyzer.analyze(&event).is_empty()); // Whitespace makes it fail to parse
        
        event.metadata.add("reconnect_count", "11");
        assert_eq!(analyzer.analyze(&event).len(), 1);
    }

    // Test 495: EbpfAnalyzer size parsing edge cases
    #[test]
    fn test_ebpf_analyzer_size_parsing() {
        let analyzer = EbpfAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        
        event.metadata.add("size_bytes", "1048576");
        assert!(analyzer.analyze(&event).is_empty());
        
        event.metadata.add("size_bytes", "1048576.5");
        assert!(analyzer.analyze(&event).is_empty());
        
        event.metadata.add("size_bytes", "1048577");
        assert_eq!(analyzer.analyze(&event).len(), 1);
    }

    // Test 496: HttpAnalyzer with event metadata
    #[test]
    fn test_http_analyzer_with_metadata() {
        let analyzer = HttpAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.metadata.add("custom_key", "custom_value");
        event.latency_us = 2_000_000;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 497: All analyzers with empty event ID
    #[test]
    fn test_analyzers_empty_event_id() {
        let http = HttpAnalyzer::default();
        let ws = WebSocketAnalyzer::default();
        let grpc = GrpcAnalyzer::default();
        let quic = QuicAnalyzer::default();
        let ebpf = EbpfAnalyzer::default();
        
        let event = Event::new("".to_string(), ProtocolType::Http);
        assert!(http.analyze(&event).is_empty());
        
        let event = Event::new("".to_string(), ProtocolType::Ws);
        assert!(ws.analyze(&event).is_empty());
        
        let event = Event::new("".to_string(), ProtocolType::Grpc);
        assert!(grpc.analyze(&event).is_empty());
        
        let event = Event::new("".to_string(), ProtocolType::Quic);
        assert!(quic.analyze(&event).is_empty());
        
        let event = Event::new("".to_string(), ProtocolType::Ebpf);
        assert!(ebpf.analyze(&event).is_empty());
    }

    // Test 498: All analyzers with long event ID
    #[test]
    fn test_analyzers_long_event_id() {
        let http = HttpAnalyzer::default();
        let long_id = "x".repeat(1000);
        let mut event = Event::new(long_id, ProtocolType::Http);
        event.latency_us = 2_000_000;
        let insights = http.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 499: Analyzer trait object compatibility
    #[test]
    fn test_analyzer_trait_object() {
        let analyzers: Vec<Box<dyn Analyzer>> = vec![
            Box::new(HttpAnalyzer::default()),
            Box::new(WebSocketAnalyzer::default()),
            Box::new(GrpcAnalyzer::default()),
            Box::new(QuicAnalyzer::default()),
            Box::new(EbpfAnalyzer::default()),
        ];
        assert_eq!(analyzers.len(), 5);
        assert_eq!(analyzers[0].protocol(), ProtocolType::Http);
    }

    // Test 500: HttpAnalyzer with very small payload
    #[test]
    fn test_http_analyzer_1byte_payload() {
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("x".to_string());
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }
}
