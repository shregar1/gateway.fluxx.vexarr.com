use serde::{Deserialize, Serialize};

/// MetricPoint: A point-in-time snapshot of service performance.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetricPoint {
    pub avg_latency_us: u64,
    pub error_rate: f64,
    pub throughput_rps: u64,
}

/// EnvDiff: High-fidelity comparison between two deployment environments.
/// Enforces structural identification of performance regressions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvDiff {
    pub source_env: String,
    pub target_env: String,
    pub latency_delta_us: i64,
    pub error_rate_delta: f64,
    pub throughput_delta: i64,
}

impl EnvDiff {
    pub fn calculate(source: &MetricPoint, target: &MetricPoint, s_name: &str, t_name: &str) -> Self {
        // Handle u64 to i64 conversion safely to avoid overflow
        let latency_delta = if target.avg_latency_us >= source.avg_latency_us {
            let diff = target.avg_latency_us - source.avg_latency_us;
            if diff > i64::MAX as u64 {
                i64::MAX
            } else {
                diff as i64
            }
        } else {
            let diff = source.avg_latency_us - target.avg_latency_us;
            if diff > i64::MAX as u64 {
                i64::MIN
            } else {
                -(diff as i64)
            }
        };
        
        Self {
            source_env: s_name.to_string(),
            target_env: t_name.to_string(),
            latency_delta_us: latency_delta,
            error_rate_delta: target.error_rate - source.error_rate,
            throughput_delta: (target.throughput_rps as i64) - (source.throughput_rps as i64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 41: MetricPoint default
    #[test]
    fn test_metric_point_default() {
        let mp: MetricPoint = Default::default();
        assert_eq!(mp.avg_latency_us, 0);
        assert_eq!(mp.error_rate, 0.0);
        assert_eq!(mp.throughput_rps, 0);
    }

    // Test 42: MetricPoint creation
    #[test]
    fn test_metric_point_creation() {
        let mp = MetricPoint {
            avg_latency_us: 1000,
            error_rate: 0.05,
            throughput_rps: 500,
        };
        assert_eq!(mp.avg_latency_us, 1000);
        assert_eq!(mp.error_rate, 0.05);
        assert_eq!(mp.throughput_rps, 500);
    }

    // Test 43: EnvDiff calculation - positive latency delta
    #[test]
    fn test_env_diff_positive_latency() {
        let source = MetricPoint { avg_latency_us: 1000, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 2000, error_rate: 0.01, throughput_rps: 100 };
        let diff = EnvDiff::calculate(&source, &target, "prod", "staging");
        assert_eq!(diff.latency_delta_us, 1000);
    }

    // Test 44: EnvDiff calculation - negative latency delta
    #[test]
    fn test_env_diff_negative_latency() {
        let source = MetricPoint { avg_latency_us: 2000, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 1000, error_rate: 0.01, throughput_rps: 100 };
        let diff = EnvDiff::calculate(&source, &target, "prod", "staging");
        assert_eq!(diff.latency_delta_us, -1000);
    }

    // Test 45: EnvDiff calculation - zero latency delta
    #[test]
    fn test_env_diff_zero_latency() {
        let source = MetricPoint { avg_latency_us: 1000, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 1000, error_rate: 0.01, throughput_rps: 100 };
        let diff = EnvDiff::calculate(&source, &target, "prod", "staging");
        assert_eq!(diff.latency_delta_us, 0);
    }

    // Test 46: EnvDiff calculation - error rate delta
    #[test]
    fn test_env_diff_error_rate() {
        let source = MetricPoint { avg_latency_us: 1000, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 1000, error_rate: 0.05, throughput_rps: 100 };
        let diff = EnvDiff::calculate(&source, &target, "prod", "staging");
        assert_eq!(diff.error_rate_delta, 0.04);
    }

    // Test 47: EnvDiff calculation - throughput delta
    #[test]
    fn test_env_diff_throughput() {
        let source = MetricPoint { avg_latency_us: 1000, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 1000, error_rate: 0.01, throughput_rps: 200 };
        let diff = EnvDiff::calculate(&source, &target, "prod", "staging");
        assert_eq!(diff.throughput_delta, 100);
    }

    // Test 48: EnvDiff serialization
    #[test]
    fn test_env_diff_serialization() {
        let source = MetricPoint { avg_latency_us: 1000, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 2000, error_rate: 0.02, throughput_rps: 150 };
        let diff = EnvDiff::calculate(&source, &target, "prod", "staging");
        let json = serde_json::to_string(&diff).unwrap();
        assert!(json.contains("prod"));
        assert!(json.contains("staging"));
    }

    // Test 49: MetricPoint serialization
    #[test]
    fn test_metric_point_serialization() {
        let mp = MetricPoint { avg_latency_us: 1000, error_rate: 0.05, throughput_rps: 500 };
        let json = serde_json::to_string(&mp).unwrap();
        assert!(json.contains("1000"));
        assert!(json.contains("0.05"));
    }

    // Test 50: MetricPoint deserialization
    #[test]
    fn test_metric_point_deserialization() {
        let json = r#"{"avg_latency_us":1000,"error_rate":0.05,"throughput_rps":500}"#;
        let mp: MetricPoint = serde_json::from_str(json).unwrap();
        assert_eq!(mp.avg_latency_us, 1000);
        assert_eq!(mp.error_rate, 0.05);
        assert_eq!(mp.throughput_rps, 500);
    }

    // Test 341: MetricPoint with zero values
    #[test]
    fn test_metric_point_zero_values() {
        let mp = MetricPoint {
            avg_latency_us: 0,
            error_rate: 0.0,
            throughput_rps: 0,
        };
        assert_eq!(mp.avg_latency_us, 0);
        assert_eq!(mp.error_rate, 0.0);
        assert_eq!(mp.throughput_rps, 0);
    }

    // Test 342: MetricPoint with max values
    #[test]
    fn test_metric_point_max_values() {
        let mp = MetricPoint {
            avg_latency_us: u64::MAX,
            error_rate: f64::MAX,
            throughput_rps: u64::MAX,
        };
        assert_eq!(mp.avg_latency_us, u64::MAX);
        assert_eq!(mp.throughput_rps, u64::MAX);
    }

    // Test 343: MetricPoint clone
    #[test]
    fn test_metric_point_clone() {
        let mp = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 50 };
        let cloned = mp.clone();
        assert_eq!(mp.avg_latency_us, cloned.avg_latency_us);
        assert_eq!(mp.error_rate, cloned.error_rate);
    }

    // Test 344: MetricPoint debug format
    #[test]
    fn test_metric_point_debug() {
        let mp = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 50 };
        let debug_str = format!("{:?}", mp);
        assert!(debug_str.contains("100"));
    }

    // Test 345: MetricPoint round-trip
    #[test]
    fn test_metric_point_round_trip() {
        let original = MetricPoint { avg_latency_us: 1000, error_rate: 0.05, throughput_rps: 500 };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: MetricPoint = serde_json::from_str(&json).unwrap();
        assert_eq!(original.avg_latency_us, deserialized.avg_latency_us);
        assert!((original.error_rate - deserialized.error_rate).abs() < f64::EPSILON);
    }

    // Test 346: EnvDiff with positive deltas
    #[test]
    fn test_env_diff_positive_deltas() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 200, error_rate: 0.05, throughput_rps: 150 };
        let diff = EnvDiff::calculate(&source, &target, "prod", "staging");
        assert_eq!(diff.latency_delta_us, 100);
        assert!((diff.error_rate_delta - 0.04).abs() < f64::EPSILON);
        assert_eq!(diff.throughput_delta, 50);
    }

    // Test 347: EnvDiff with negative deltas
    #[test]
    fn test_env_diff_negative_deltas() {
        let source = MetricPoint { avg_latency_us: 200, error_rate: 0.05, throughput_rps: 150 };
        let target = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let diff = EnvDiff::calculate(&source, &target, "prod", "staging");
        assert_eq!(diff.latency_delta_us, -100);
        assert!((diff.error_rate_delta + 0.04).abs() < f64::EPSILON);
        assert_eq!(diff.throughput_delta, -50);
    }

    // Test 348: EnvDiff with zero deltas
    #[test]
    fn test_env_diff_zero_deltas() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let diff = EnvDiff::calculate(&source, &target, "a", "b");
        assert_eq!(diff.latency_delta_us, 0);
        assert!(diff.error_rate_delta.abs() < f64::EPSILON);
        assert_eq!(diff.throughput_delta, 0);
    }

    // Test 349: EnvDiff environment names
    #[test]
    fn test_env_diff_names() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let diff = EnvDiff::calculate(&source, &target, "production", "development");
        assert_eq!(diff.source_env, "production");
        assert_eq!(diff.target_env, "development");
    }

    // Test 350: EnvDiff clone
    #[test]
    fn test_env_diff_clone() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 200, error_rate: 0.02, throughput_rps: 150 };
        let diff = EnvDiff::calculate(&source, &target, "a", "b");
        let cloned = diff.clone();
        assert_eq!(diff.latency_delta_us, cloned.latency_delta_us);
        assert_eq!(diff.source_env, cloned.source_env);
    }

    // Test 351: EnvDiff debug format
    #[test]
    fn test_env_diff_debug() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 200, error_rate: 0.02, throughput_rps: 150 };
        let diff = EnvDiff::calculate(&source, &target, "a", "b");
        let debug_str = format!("{:?}", diff);
        assert!(debug_str.contains("EnvDiff"));
    }

    // Test 352: EnvDiff round-trip
    #[test]
    fn test_env_diff_round_trip() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 200, error_rate: 0.02, throughput_rps: 150 };
        let original = EnvDiff::calculate(&source, &target, "source", "target");
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: EnvDiff = serde_json::from_str(&json).unwrap();
        assert_eq!(original.latency_delta_us, deserialized.latency_delta_us);
        assert_eq!(original.source_env, deserialized.source_env);
    }

    // Test 353: EnvDiff with large values
    #[test]
    fn test_env_diff_large_values() {
        let source = MetricPoint { avg_latency_us: 0, error_rate: 0.0, throughput_rps: 0 };
        let target = MetricPoint { avg_latency_us: u64::MAX, error_rate: 1.0, throughput_rps: u64::MAX };
        let diff = EnvDiff::calculate(&source, &target, "a", "b");
        // When u64::MAX would overflow i64, we clamp to i64::MAX
        assert_eq!(diff.latency_delta_us, i64::MAX);
        assert_eq!(diff.throughput_delta, u64::MAX as i64);
    }

    // Test 354: EnvDiff with negative latency delta renamed
    #[test]
    fn test_env_diff_negative_latency_delta_renamed() {
        let source = MetricPoint { avg_latency_us: 1000, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 0, error_rate: 0.01, throughput_rps: 100 };
        let diff = EnvDiff::calculate(&source, &target, "a", "b");
        assert_eq!(diff.latency_delta_us, -1000);
    }

    // Test 355: MetricPoint with floating point precision
    #[test]
    fn test_metric_point_float_precision() {
        let mp = MetricPoint { avg_latency_us: 100, error_rate: 0.123456789, throughput_rps: 50 };
        let json = serde_json::to_string(&mp).unwrap();
        assert!(json.contains("0.123456789"));
    }

    // Test 356: EnvDiff deserialization
    #[test]
    fn test_env_diff_deserialization() {
        let json = r#"{"source_env":"prod","target_env":"dev","latency_delta_us":-500,"error_rate_delta":0.05,"throughput_delta":100}"#;
        let diff: EnvDiff = serde_json::from_str(json).unwrap();
        assert_eq!(diff.source_env, "prod");
        assert_eq!(diff.target_env, "dev");
        assert_eq!(diff.latency_delta_us, -500);
    }

    // Test 357: MetricPoint with high throughput
    #[test]
    fn test_metric_point_high_throughput() {
        let mp = MetricPoint { avg_latency_us: 1, error_rate: 0.0, throughput_rps: 1000000 };
        assert_eq!(mp.throughput_rps, 1000000);
    }

    // Test 358: MetricPoint with high latency
    #[test]
    fn test_metric_point_high_latency() {
        let mp = MetricPoint { avg_latency_us: 3600000000, error_rate: 0.0, throughput_rps: 1 };
        assert_eq!(mp.avg_latency_us, 3600000000);
    }

    // Test 359: EnvDiff with empty env names
    #[test]
    fn test_env_diff_empty_names() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 200, error_rate: 0.02, throughput_rps: 150 };
        let diff = EnvDiff::calculate(&source, &target, "", "");
        assert!(diff.source_env.is_empty());
        assert!(diff.target_env.is_empty());
    }

    // Test 360: EnvDiff with unicode names
    #[test]
    fn test_env_diff_unicode_names() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 200, error_rate: 0.02, throughput_rps: 150 };
        let diff = EnvDiff::calculate(&source, &target, "生产环境", "开发环境");
        assert_eq!(diff.source_env, "生产环境");
        assert_eq!(diff.target_env, "开发环境");
    }

    // Test 361: MetricPoint default is all zeros
    #[test]
    fn test_metric_point_default_all_zeros() {
        let mp: MetricPoint = Default::default();
        assert_eq!(mp.avg_latency_us, 0);
        assert!((mp.error_rate - 0.0).abs() < f64::EPSILON);
        assert_eq!(mp.throughput_rps, 0);
    }

    // Test 362: EnvDiff error rate calculation
    #[test]
    fn test_env_diff_error_rate_calculation() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.5, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 100, error_rate: 0.25, throughput_rps: 100 };
        let diff = EnvDiff::calculate(&source, &target, "a", "b");
        assert!((diff.error_rate_delta + 0.25).abs() < f64::EPSILON);
    }

    // Test 363: MetricPoint partial equality
    #[test]
    fn test_metric_point_partial_eq() {
        let mp1 = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 50 };
        let mp2 = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 50 };
        assert_eq!(mp1.avg_latency_us, mp2.avg_latency_us);
    }

    // Test 364: MetricPoint with error rate 1.0
    #[test]
    fn test_metric_point_max_error_rate() {
        let mp = MetricPoint { avg_latency_us: 100, error_rate: 1.0, throughput_rps: 50 };
        assert!((mp.error_rate - 1.0).abs() < f64::EPSILON);
    }

    // Test 365: MetricPoint with very small error rate
    #[test]
    fn test_metric_point_min_error_rate() {
        let mp = MetricPoint { avg_latency_us: 100, error_rate: 0.0000001, throughput_rps: 50 };
        assert!(mp.error_rate > 0.0);
    }

    // Test 366: EnvDiff throughput negative overflow
    #[test]
    fn test_env_diff_throughput_negative() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 1000 };
        let target = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let diff = EnvDiff::calculate(&source, &target, "a", "b");
        assert_eq!(diff.throughput_delta, -900);
    }

    // Test 367: MetricPoint serialization structure
    #[test]
    fn test_metric_point_json_structure() {
        let mp = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 50 };
        let json = serde_json::to_string(&mp).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("avg_latency_us").is_some());
        assert!(parsed.get("error_rate").is_some());
        assert!(parsed.get("throughput_rps").is_some());
    }

    // Test 368: EnvDiff serialization structure
    #[test]
    fn test_env_diff_json_structure() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 200, error_rate: 0.02, throughput_rps: 150 };
        let diff = EnvDiff::calculate(&source, &target, "a", "b");
        let json = serde_json::to_string(&diff).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("source_env").is_some());
        assert!(parsed.get("latency_delta_us").is_some());
    }

    // Test 369: MetricPoint is Send + Sync
    #[test]
    fn test_metric_point_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MetricPoint>();
    }

    // Test 370: EnvDiff is Send + Sync
    #[test]
    fn test_env_diff_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EnvDiff>();
    }

    // Test 371: MetricPoint deserialization with missing fields
    #[test]
    fn test_metric_point_deserialization_missing_fields() {
        let json = r#"{"avg_latency_us":100,"throughput_rps":50}"#;
        let result: Result<MetricPoint, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    // Test 372: EnvDiff with identical environments
    #[test]
    fn test_env_diff_identical() {
        let mp = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let diff = EnvDiff::calculate(&mp, &mp, "same", "same");
        assert_eq!(diff.latency_delta_us, 0);
        assert_eq!(diff.source_env, "same");
        assert_eq!(diff.target_env, "same");
    }

    // Test 373: MetricPoint with very high error rate
    #[test]
    fn test_metric_point_very_high_error_rate() {
        let mp = MetricPoint { avg_latency_us: 100, error_rate: 0.999999, throughput_rps: 50 };
        assert!(mp.error_rate < 1.0);
    }

    // Test 374: EnvDiff with special chars in names
    #[test]
    fn test_env_diff_special_chars_names() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 200, error_rate: 0.02, throughput_rps: 150 };
        let diff = EnvDiff::calculate(&source, &target, "env-1.0", "env-2.0!");
        assert_eq!(diff.source_env, "env-1.0");
    }

    // Test 375: MetricPoint serialization with very small float
    #[test]
    fn test_metric_point_small_float() {
        let mp = MetricPoint { avg_latency_us: 100, error_rate: 1e-10, throughput_rps: 50 };
        let json = serde_json::to_string(&mp).unwrap();
        assert!(json.contains("error_rate"));
    }

    // Test 376: EnvDiff latency delta overflow handling
    #[test]
    fn test_env_diff_latency_overflow() {
        let source = MetricPoint { avg_latency_us: 0, error_rate: 0.0, throughput_rps: 0 };
        let target = MetricPoint { avg_latency_us: u64::MAX, error_rate: 0.0, throughput_rps: 0 };
        let diff = EnvDiff::calculate(&source, &target, "a", "b");
        assert!(diff.latency_delta_us > 0);
    }

    // Test 377: MetricPoint clone independence
    #[test]
    fn test_metric_point_clone_independence() {
        let mp1 = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 50 };
        let mut mp2 = mp1.clone();
        mp2.avg_latency_us = 200;
        assert_ne!(mp1.avg_latency_us, mp2.avg_latency_us);
    }

    // Test 378: EnvDiff with long names
    #[test]
    fn test_env_diff_long_names() {
        let long = "x".repeat(1000);
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 200, error_rate: 0.02, throughput_rps: 150 };
        let diff = EnvDiff::calculate(&source, &target, &long, &long);
        assert_eq!(diff.source_env.len(), 1000);
    }

    // Test 379: MetricPoint default clone
    #[test]
    fn test_metric_point_default_clone() {
        let mp1: MetricPoint = Default::default();
        let mp2 = mp1.clone();
        assert_eq!(mp1.avg_latency_us, mp2.avg_latency_us);
    }

    // Test 380: EnvDiff all fields present
    #[test]
    fn test_env_diff_all_fields() {
        let source = MetricPoint { avg_latency_us: 100, error_rate: 0.01, throughput_rps: 100 };
        let target = MetricPoint { avg_latency_us: 200, error_rate: 0.05, throughput_rps: 150 };
        let diff = EnvDiff::calculate(&source, &target, "src", "tgt");
        assert!(!diff.source_env.is_empty());
        assert!(!diff.target_env.is_empty());
    }
}
