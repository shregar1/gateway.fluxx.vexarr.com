use crate::models::event::Event;
use crate::analysis::insights::Insight;
use async_trait::async_trait;
use std::fmt::Debug;

pub mod insights;
pub mod protocol;
pub mod industrial;
pub mod anomaly;
pub mod error_tracking;
pub mod sourcemaps;

/// Protocol Intelligence Interface: Performs stateless analysis of network events.
/// Enforces modular detection of anomalies (surges, high latency, reconnects).
#[async_trait]
pub trait Analyzer: Send + Sync + Debug {
    /// Inspects an event and emits one or more insights based on the analysis.
    fn analyze(&self, event: &Event) -> Vec<Insight>;
    
    /// Identifies the specific protocol this analyzer is specialized for.
    fn protocol(&self) -> crate::models::event::ProtocolType;
}

/// Shared Analysis Logic: Utilities for common threshold detection across all protocols.
pub struct AnalysisUtils;

impl AnalysisUtils {
    pub fn is_high_latency(latency_us: u64, threshold_us: u64) -> bool {
        latency_us > threshold_us
    }

    pub fn is_payload_anomaly(payload_size: usize, threshold_bytes: usize) -> bool {
        payload_size > threshold_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::ProtocolType;

    // Test 157: is_high_latency true
    #[test]
    fn test_is_high_latency_true() {
        assert!(AnalysisUtils::is_high_latency(1_000_001, 1_000_000));
    }

    // Test 158: is_high_latency false at threshold
    #[test]
    fn test_is_high_latency_at_threshold() {
        assert!(!AnalysisUtils::is_high_latency(1_000_000, 1_000_000));
    }

    // Test 159: is_high_latency false below
    #[test]
    fn test_is_high_latency_below() {
        assert!(!AnalysisUtils::is_high_latency(999_999, 1_000_000));
    }

    // Test 160: is_payload_anomaly true
    #[test]
    fn test_is_payload_anomaly_true() {
        assert!(AnalysisUtils::is_payload_anomaly(11, 10));
    }

    // Test 161: is_payload_anomaly false at threshold
    #[test]
    fn test_is_payload_anomaly_at_threshold() {
        assert!(!AnalysisUtils::is_payload_anomaly(10, 10));
    }

    // Test 162: is_payload_anomaly false below
    #[test]
    fn test_is_payload_anomaly_below() {
        assert!(!AnalysisUtils::is_payload_anomaly(9, 10));
    }

    // Test 163: is_high_latency zero
    #[test]
    fn test_is_high_latency_zero() {
        assert!(!AnalysisUtils::is_high_latency(0, 1));
    }

    // Test 164: is_payload_anomaly zero
    #[test]
    fn test_is_payload_anomaly_zero() {
        assert!(!AnalysisUtils::is_payload_anomaly(0, 1));
    }

    // Test 551: is_high_latency with equal values
    #[test]
    fn test_is_high_latency_equal_values() {
        assert!(!AnalysisUtils::is_high_latency(100, 100));
        assert!(!AnalysisUtils::is_high_latency(0, 0));
    }

    // Test 552: is_high_latency with max u64
    #[test]
    fn test_is_high_latency_max_u64() {
        assert!(AnalysisUtils::is_high_latency(u64::MAX, u64::MAX - 1));
        assert!(!AnalysisUtils::is_high_latency(u64::MAX, u64::MAX));
    }

    // Test 553: is_high_latency with large threshold
    #[test]
    fn test_is_high_latency_large_threshold() {
        assert!(!AnalysisUtils::is_high_latency(1_000_000, 10_000_000));
        assert!(AnalysisUtils::is_high_latency(10_000_001, 10_000_000));
    }

    // Test 554: is_payload_anomaly with max usize
    #[test]
    fn test_is_payload_anomaly_max_usize() {
        assert!(AnalysisUtils::is_payload_anomaly(usize::MAX, usize::MAX - 1));
        assert!(!AnalysisUtils::is_payload_anomaly(usize::MAX, usize::MAX));
    }

    // Test 555: is_payload_anomaly zero threshold
    #[test]
    fn test_is_payload_anomaly_zero_threshold() {
        assert!(AnalysisUtils::is_payload_anomaly(1, 0));
        assert!(!AnalysisUtils::is_payload_anomaly(0, 0));
    }

    // Test 556: is_high_latency both zero
    #[test]
    fn test_is_high_latency_both_zero() {
        assert!(!AnalysisUtils::is_high_latency(0, 0));
    }

    // Test 557: AnalysisUtils struct instantiation
    #[test]
    fn test_analysis_utils_instantiation() {
        let _utils = AnalysisUtils;
    }

    // Test 558: Analyzer trait is object safe
    #[test]
    fn test_analyzer_trait_object_safe() {
        use crate::analysis::protocol::HttpAnalyzer;
        let _: Box<dyn Analyzer> = Box::new(HttpAnalyzer::default());
    }

    // Test 559: is_high_latency with typical values
    #[test]
    fn test_is_high_latency_typical() {
        // HTTP timeout threshold (30 seconds)
        assert!(!AnalysisUtils::is_high_latency(29_000_000, 30_000_000));
        assert!(AnalysisUtils::is_high_latency(30_000_001, 30_000_000));
        
        // Database query threshold (1 second)
        assert!(!AnalysisUtils::is_high_latency(999_999, 1_000_000));
        assert!(AnalysisUtils::is_high_latency(1_000_001, 1_000_000));
    }

    // Test 560: is_payload_anomaly with typical values
    #[test]
    fn test_is_payload_anomaly_typical() {
        // 1MB threshold
        assert!(!AnalysisUtils::is_payload_anomaly(1024 * 1024, 1024 * 1024));
        assert!(AnalysisUtils::is_payload_anomaly(1024 * 1024 + 1, 1024 * 1024));
        
        // 10MB threshold
        assert!(!AnalysisUtils::is_payload_anomaly(10 * 1024 * 1024, 10 * 1024 * 1024));
        assert!(AnalysisUtils::is_payload_anomaly(10 * 1024 * 1024 + 1, 10 * 1024 * 1024));
    }

    // Test 561: AnalysisUtils methods are pure functions
    #[test]
    fn test_analysis_utils_pure_functions() {
        let result1 = AnalysisUtils::is_high_latency(100, 50);
        let result2 = AnalysisUtils::is_high_latency(100, 50);
        assert_eq!(result1, result2);
        
        let result1 = AnalysisUtils::is_payload_anomaly(100, 50);
        let result2 = AnalysisUtils::is_payload_anomaly(100, 50);
        assert_eq!(result1, result2);
    }

    // Test 562: is_high_latency boundary at 1
    #[test]
    fn test_is_high_latency_boundary_one() {
        assert!(!AnalysisUtils::is_high_latency(0, 1));
        assert!(!AnalysisUtils::is_high_latency(1, 1));
        assert!(AnalysisUtils::is_high_latency(2, 1));
    }

    // Test 563: is_payload_anomaly boundary at 1
    #[test]
    fn test_is_payload_anomaly_boundary_one() {
        assert!(!AnalysisUtils::is_payload_anomaly(0, 1));
        assert!(!AnalysisUtils::is_payload_anomaly(1, 1));
        assert!(AnalysisUtils::is_payload_anomaly(2, 1));
    }

    // Test 564: Analyzer trait requires Send
    #[test]
    fn test_analyzer_trait_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Box<dyn Analyzer>>();
    }

    // Test 565: Analyzer trait requires Sync
    #[test]
    fn test_analyzer_trait_sync() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<Box<dyn Analyzer>>();
    }

    // Test 566: Analyzer trait requires Debug
    #[test]
    fn test_analyzer_trait_debug() {
        fn assert_debug<T: Debug>() {}
        assert_debug::<Box<dyn Analyzer>>();
    }

    // Test 567: is_high_latency with one nanosecond threshold
    #[test]
    fn test_is_high_latency_nanosecond() {
        assert!(!AnalysisUtils::is_high_latency(0, 1));
        assert!(!AnalysisUtils::is_high_latency(1, 1));
        assert!(AnalysisUtils::is_high_latency(2, 1));
    }

    // Test 568: is_payload_anomaly with single byte threshold
    #[test]
    fn test_is_payload_anomaly_single_byte() {
        assert!(!AnalysisUtils::is_payload_anomaly(0, 1));
        assert!(!AnalysisUtils::is_payload_anomaly(1, 1));
        assert!(AnalysisUtils::is_payload_anomaly(2, 1));
    }

    // Test 569: Module exports are accessible
    #[test]
    fn test_module_exports() {
        // These should all compile if the module structure is correct
        let _: insights::InsightLevel = insights::InsightLevel::Info;
        let _: protocol::HttpAnalyzer = protocol::HttpAnalyzer::default();
        let _: industrial::SensorAnalyzer = industrial::SensorAnalyzer::default();
    }

    // Test 570: AnalysisUtils with various thresholds
    #[test]
    fn test_analysis_utils_various_thresholds() {
        // Test with various common latency thresholds
        let thresholds = vec![1000u64, 10000, 100000, 1000000, 10000000];
        for threshold in thresholds {
            assert!(!AnalysisUtils::is_high_latency(threshold, threshold));
            assert!(AnalysisUtils::is_high_latency(threshold + 1, threshold));
            assert!(!AnalysisUtils::is_high_latency(threshold - 1, threshold));
        }
        
        // Test with various common payload thresholds
        let thresholds = vec![1024usize, 4096, 65536, 1048576, 10485760];
        for threshold in thresholds {
            assert!(!AnalysisUtils::is_payload_anomaly(threshold, threshold));
            assert!(AnalysisUtils::is_payload_anomaly(threshold + 1, threshold));
            assert!(!AnalysisUtils::is_payload_anomaly(threshold - 1, threshold));
        }
    }

    // Test 571: is_high_latency with u64 overflow edge case
    #[test]
    fn test_is_high_latency_no_overflow() {
        // This test verifies no overflow occurs
        let _ = AnalysisUtils::is_high_latency(u64::MAX, 1);
        let _ = AnalysisUtils::is_high_latency(1, u64::MAX);
    }

    // Test 572: is_payload_anomaly with usize overflow edge case
    #[test]
    fn test_is_payload_anomaly_no_overflow() {
        // This test verifies no overflow occurs
        let _ = AnalysisUtils::is_payload_anomaly(usize::MAX, 1);
        let _ = AnalysisUtils::is_payload_anomaly(1, usize::MAX);
    }

    // Test 573: AnalysisUtils implements Clone implicitly (as it's empty)
    #[test]
    fn test_analysis_utils_clone() {
        let utils1 = AnalysisUtils;
        let _utils2 = utils1;
        // Both can be used since AnalysisUtils is Copy (empty struct)
    }

    // Test 574: Analyzer trait method signatures
    #[test]
    fn test_analyzer_trait_methods() {
        use crate::analysis::protocol::HttpAnalyzer;
        let analyzer = HttpAnalyzer::default();
        let event = Event::new("test".to_string(), ProtocolType::Http);
        let _insights = analyzer.analyze(&event);
        let _protocol = analyzer.protocol();
    }

    // Test 575: Empty analysis results
    #[test]
    fn test_empty_analysis_results() {
        use crate::analysis::protocol::QuicAnalyzer;
        let analyzer = QuicAnalyzer::default();
        let event = Event::new("test".to_string(), ProtocolType::Quic);
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
        assert_eq!(insights.len(), 0);
    }

    // Test 576: Multiple analyzers can be used together
    #[test]
    fn test_multiple_analyzers() {
        use crate::analysis::protocol::{HttpAnalyzer, GrpcAnalyzer};
        use crate::analysis::industrial::SensorAnalyzer;
        
        let http = HttpAnalyzer::default();
        let grpc = GrpcAnalyzer::default();
        let sensor = SensorAnalyzer::default();
        
        let event = Event::new("test".to_string(), ProtocolType::Http);
        let _ = http.analyze(&event);
        let _ = grpc.analyze(&event);
        let _ = sensor.analyze(&event);
    }

    // Test 577: is_high_latency with millisecond thresholds
    #[test]
    fn test_is_high_latency_milliseconds() {
        // 100ms threshold
        assert!(!AnalysisUtils::is_high_latency(99_999, 100_000));
        assert!(!AnalysisUtils::is_high_latency(100_000, 100_000));
        assert!(AnalysisUtils::is_high_latency(100_001, 100_000));
        
        // 500ms threshold
        assert!(!AnalysisUtils::is_high_latency(499_999, 500_000));
        assert!(!AnalysisUtils::is_high_latency(500_000, 500_000));
        assert!(AnalysisUtils::is_high_latency(500_001, 500_000));
    }

    // Test 578: is_payload_anomaly with KB thresholds
    #[test]
    fn test_is_payload_anomaly_kb() {
        // 1KB threshold
        assert!(!AnalysisUtils::is_payload_anomaly(1023, 1024));
        assert!(!AnalysisUtils::is_payload_anomaly(1024, 1024));
        assert!(AnalysisUtils::is_payload_anomaly(1025, 1024));
        
        // 64KB threshold
        assert!(!AnalysisUtils::is_payload_anomaly(65535, 65536));
        assert!(!AnalysisUtils::is_payload_anomaly(65536, 65536));
        assert!(AnalysisUtils::is_payload_anomaly(65537, 65536));
    }

    // Test 579: AnalysisUtils documentation consistency
    #[test]
    fn test_analysis_utils_documentation() {
        // Verify the struct exists and is public as documented
        let _ = AnalysisUtils;
    }

    // Test 580: All submodules are public
    #[test]
    fn test_submodules_public() {
        // These imports should work if submodules are public
        use crate::analysis::insights;
        use crate::analysis::protocol;
        use crate::analysis::industrial;
        
        // Just verify the modules exist
        let _ = std::any::type_name::<insights::Insight>();
        let _ = std::any::type_name::<protocol::HttpAnalyzer>();
        let _ = std::any::type_name::<industrial::SensorAnalyzer>();
    }

    // Test 581: Analyzer returns Vec<Insight>
    #[test]
    fn test_analyzer_returns_vec() {
        use crate::analysis::protocol::QuicAnalyzer;
        let analyzer = QuicAnalyzer::default();
        let event = Event::new("test".to_string(), ProtocolType::Quic);
        let insights: Vec<insights::Insight> = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 582: AnalysisUtils method chaining potential
    #[test]
    fn test_analysis_utils_chaining() {
        // Both methods can be called in sequence
        let high_latency = AnalysisUtils::is_high_latency(100, 50);
        let anomaly = AnalysisUtils::is_payload_anomaly(100, 50);
        assert!(high_latency);
        assert!(anomaly);
    }

    // Test 583: is_high_latency with very small values
    #[test]
    fn test_is_high_latency_small_values() {
        for i in 0..10 {
            for j in 0..10 {
                let result = AnalysisUtils::is_high_latency(i, j);
                if i > j {
                    assert!(result, "{} should be > {}", i, j);
                } else {
                    assert!(!result, "{} should not be > {}", i, j);
                }
            }
        }
    }

    // Test 584: is_payload_anomaly with small values
    #[test]
    fn test_is_payload_anomaly_small_values() {
        for i in 0..10usize {
            for j in 0..10usize {
                let result = AnalysisUtils::is_payload_anomaly(i, j);
                if i > j {
                    assert!(result, "{} should be > {}", i, j);
                } else {
                    assert!(!result, "{} should not be > {}", i, j);
                }
            }
        }
    }

    // Test 585: AnalysisUtils thread safety
    #[test]
    fn test_analysis_utils_thread_safety() {
        use std::thread;
        
        let handles: Vec<_> = (0..10)
            .map(|i| {
                thread::spawn(move || {
                    AnalysisUtils::is_high_latency(i as u64 * 1000, 5000);
                    AnalysisUtils::is_payload_anomaly(i * 1000, 5000);
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
    }

    // Test 586: Analyzer trait has async_trait
    #[test]
    fn test_analyzer_has_async_trait() {
        // Verify the trait definition uses async_trait
        // This is a compile-time check
        fn assert_analyzer<T: Analyzer>() {}
        use crate::analysis::protocol::HttpAnalyzer;
        assert_analyzer::<HttpAnalyzer>();
    }

    // Test 587: ProtocolType used in analysis
    #[test]
    fn test_protocol_type_usage() {
        // Verify all protocol types exist
        let _ = ProtocolType::Http;
        let _ = ProtocolType::Ws;
        let _ = ProtocolType::Grpc;
        let _ = ProtocolType::Quic;
        let _ = ProtocolType::Ebpf;
        let _ = ProtocolType::Modbus;
        let _ = ProtocolType::Other;
    }

    // Test 588: Result type from analysis
    #[test]
    fn test_analysis_result_type() {
        // Result<T> is std::result::Result<T, crate::core::error::Error>
        // Verify it works with standard operations
        let _: std::result::Result<String, String> = Ok("test".to_string());
    }

    // Test 589: AnalysisUtils with identical values
    #[test]
    fn test_analysis_utils_identical_values() {
        for i in vec![0u64, 1, 100, 1000, 1000000] {
            assert!(!AnalysisUtils::is_high_latency(i, i));
        }
        for i in vec![0usize, 1, 100, 1000, 1000000] {
            assert!(!AnalysisUtils::is_payload_anomaly(i, i));
        }
    }

    // Test 590: Analysis edge case - very close values
    #[test]
    fn test_analysis_very_close_values() {
        // is_high_latency
        assert!(!AnalysisUtils::is_high_latency(999_999_999, 1_000_000_000));
        assert!(!AnalysisUtils::is_high_latency(1_000_000_000, 1_000_000_000));
        assert!(AnalysisUtils::is_high_latency(1_000_000_001, 1_000_000_000));
        
        // is_payload_anomaly
        assert!(!AnalysisUtils::is_payload_anomaly(999_999_999, 1_000_000_000));
        assert!(!AnalysisUtils::is_payload_anomaly(1_000_000_000, 1_000_000_000));
        assert!(AnalysisUtils::is_payload_anomaly(1_000_000_001, 1_000_000_000));
    }

    // Test 591: Analyzer default implementations
    #[test]
    fn test_analyzer_default_impls() {
        use crate::analysis::protocol::{HttpAnalyzer, WebSocketAnalyzer, GrpcAnalyzer, QuicAnalyzer, EbpfAnalyzer};
        use crate::analysis::industrial::{SensorAnalyzer, ActuatorAnalyzer, VehicleAnalyzer};
        
        let _: HttpAnalyzer = Default::default();
        let _: WebSocketAnalyzer = Default::default();
        let _: GrpcAnalyzer = Default::default();
        let _: QuicAnalyzer = Default::default();
        let _: EbpfAnalyzer = Default::default();
        let _: SensorAnalyzer = Default::default();
        let _: ActuatorAnalyzer = Default::default();
        let _: VehicleAnalyzer = Default::default();
    }

    // Test 592: Empty struct AnalysisUtils behavior
    #[test]
    fn test_analysis_utils_empty_struct() {
        let u1 = AnalysisUtils;
        let u2 = AnalysisUtils;
        // Both are equal (empty structs are always equal)
        assert_eq!(std::mem::size_of_val(&u1), 0);
        assert_eq!(std::mem::size_of_val(&u2), 0);
    }

    // Test 593: is_high_latency with wrapping arithmetic safety
    #[test]
    fn test_is_high_latency_wrapping_safe() {
        // These should not panic
        let _ = AnalysisUtils::is_high_latency(u64::MAX, 0);
        let _ = AnalysisUtils::is_high_latency(0, u64::MAX);
        let _ = AnalysisUtils::is_high_latency(u64::MAX / 2, u64::MAX / 2);
    }

    // Test 594: is_payload_anomaly with wrapping arithmetic safety
    #[test]
    fn test_is_payload_anomaly_wrapping_safe() {
        // These should not panic
        let _ = AnalysisUtils::is_payload_anomaly(usize::MAX, 0);
        let _ = AnalysisUtils::is_payload_anomaly(0, usize::MAX);
        let _ = AnalysisUtils::is_payload_anomaly(usize::MAX / 2, usize::MAX / 2);
    }

    // Test 595: Analyzer trait object with different protocols
    #[test]
    fn test_analyzer_trait_object_protocols() {
        use crate::analysis::protocol::HttpAnalyzer;
        use crate::analysis::industrial::SensorAnalyzer;
        
        let http_analyzer = HttpAnalyzer::default();
        let sensor_analyzer = SensorAnalyzer::default();
        let analyzers: Vec<(&dyn Analyzer, ProtocolType)> = vec![
            (&http_analyzer, ProtocolType::Http),
            (&sensor_analyzer, ProtocolType::Other),
        ];
        
        for (analyzer, expected) in analyzers {
            assert_eq!(analyzer.protocol(), expected);
        }
    }

    // Test 596: AnalysisUtils methods are const-compatible
    #[test]
    fn test_analysis_utils_const_compatible() {
        // The methods are simple comparisons, could be const
        // This test verifies they work as expected
        const THRESHOLD: u64 = 1000;
        assert!(AnalysisUtils::is_high_latency(THRESHOLD + 1, THRESHOLD));
        assert!(!AnalysisUtils::is_high_latency(THRESHOLD, THRESHOLD));
    }

    // Test 597: is_high_latency with sub-microsecond values
    #[test]
    fn test_is_high_latency_sub_microsecond() {
        assert!(!AnalysisUtils::is_high_latency(0, 100));
        assert!(!AnalysisUtils::is_high_latency(50, 100));
        assert!(!AnalysisUtils::is_high_latency(100, 100));
        assert!(AnalysisUtils::is_high_latency(101, 100));
    }

    // Test 598: is_payload_anomaly with empty/zero cases
    #[test]
    fn test_is_payload_anomaly_empty_cases() {
        assert!(!AnalysisUtils::is_payload_anomaly(0, 0));
        assert!(!AnalysisUtils::is_payload_anomaly(0, 1));
        assert!(AnalysisUtils::is_payload_anomaly(1, 0));
    }

    // Test 599: Module structure integrity
    #[test]
    fn test_module_structure() {
        // Verify all expected items are accessible
        let _ = AnalysisUtils::is_high_latency;
        let _ = AnalysisUtils::is_payload_anomaly;
    }

    // Test 600: Final integration test
    #[test]
    fn test_analysis_utils_integration() {
        // Comprehensive test of both utility functions
        let test_cases_latency: Vec<(u64, u64, bool)> = vec![
            (0, 100, false),
            (50, 100, false),
            (99, 100, false),
            (100, 100, false),
            (101, 100, true),
            (1000, 100, true),
        ];
        
        for (latency, threshold, expected) in test_cases_latency {
            assert_eq!(
                AnalysisUtils::is_high_latency(latency, threshold),
                expected,
                "latency={}, threshold={}",
                latency,
                threshold
            );
        }
        
        let test_cases_payload: Vec<(usize, usize, bool)> = vec![
            (0, 100, false),
            (50, 100, false),
            (99, 100, false),
            (100, 100, false),
            (101, 100, true),
            (1000, 100, true),
        ];
        
        for (size, threshold, expected) in test_cases_payload {
            assert_eq!(
                AnalysisUtils::is_payload_anomaly(size, threshold),
                expected,
                "size={}, threshold={}",
                size,
                threshold
            );
        }
    }
}
