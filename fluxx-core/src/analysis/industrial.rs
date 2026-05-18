use crate::models::event::{Event, ProtocolType};
use crate::analysis::insights::{Insight, InsightLevel};
use crate::analysis::{Analyzer, AnalysisUtils};

/// IoT/Sensor Intelligence: Detects stale sensor data or telemetry drift.
#[derive(Debug, Default)]
pub struct SensorAnalyzer;

impl Analyzer for SensorAnalyzer {
    fn analyze(&self, event: &Event) -> Vec<Insight> {
        let mut insights = Vec::new();
        if let Some(drift) = event.metadata.extra.get("drift_factor") {
            if let Ok(f) = drift.parse::<f64>() {
                if f > 0.15 {
                    insights.push(Insight::new(self.protocol(), InsightLevel::Warning, "IoT: Potential sensor drift detected in edge stream."));
                }
            }
        }
        insights
    }
    fn protocol(&self) -> ProtocolType { ProtocolType::Other } // Future: Map to Mqtt
}

/// Factory Automation Intelligence: Tracks actuator response and cycle times.
#[derive(Debug, Default)]
pub struct ActuatorAnalyzer;

impl Analyzer for ActuatorAnalyzer {
    fn analyze(&self, event: &Event) -> Vec<Insight> {
        let mut insights = Vec::new();
        if AnalysisUtils::is_high_latency(event.latency_us, 500_000) {
            insights.push(Insight::new(self.protocol(), InsightLevel::Critical, "Factory: PLC Actuator response breach (latency > 500ms)."));
        }
        insights
    }
    fn protocol(&self) -> ProtocolType { ProtocolType::Modbus }
}

/// Automobile Intelligence: Detects unusual CAN/Ethernet traffic patterns.
#[derive(Debug, Default)]
pub struct VehicleAnalyzer;

impl Analyzer for VehicleAnalyzer {
    fn analyze(&self, event: &Event) -> Vec<Insight> {
        let mut insights = Vec::new();
        if let Some(__restarts) = event.metadata.extra.get("ecu_resets") {
             insights.push(Insight::new(self.protocol(), InsightLevel::Critical, "Vehicle: ECU reset detected on CAN/SomeIP bus."));
        }
        insights
    }
    fn protocol(&self) -> ProtocolType { ProtocolType::Ebpf }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 501: SensorAnalyzer default
    #[test]
    fn test_sensor_analyzer_default() {
        let analyzer = SensorAnalyzer::default();
        assert_eq!(analyzer.protocol(), ProtocolType::Other);
    }

    // Test 502: SensorAnalyzer no drift detected
    #[test]
    fn test_sensor_analyzer_no_drift() {
        let analyzer = SensorAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Other);
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 503: SensorAnalyzer drift below threshold
    #[test]
    fn test_sensor_analyzer_drift_below() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "0.10");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 504: SensorAnalyzer drift exactly at threshold
    #[test]
    fn test_sensor_analyzer_drift_at_threshold() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "0.15");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 505: SensorAnalyzer drift just above threshold
    #[test]
    fn test_sensor_analyzer_drift_above() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "0.1500001");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
        assert_eq!(insights[0].level, InsightLevel::Warning);
    }

    // Test 506: SensorAnalyzer with high drift
    #[test]
    fn test_sensor_analyzer_high_drift() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "0.5");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 507: SensorAnalyzer with extreme drift
    #[test]
    fn test_sensor_analyzer_extreme_drift() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "999.99");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 508: SensorAnalyzer with invalid drift
    #[test]
    fn test_sensor_analyzer_invalid_drift() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "invalid");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 509: SensorAnalyzer with negative drift
    #[test]
    fn test_sensor_analyzer_negative_drift() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "-0.5");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 510: SensorAnalyzer with zero drift
    #[test]
    fn test_sensor_analyzer_zero_drift() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "0.0");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 511: SensorAnalyzer insight message content
    #[test]
    fn test_sensor_analyzer_insight_message() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "0.2");
        let insights = analyzer.analyze(&event);
        assert!(insights[0].message.contains("IoT"));
        assert!(insights[0].message.contains("drift"));
    }

    // Test 512: SensorAnalyzer insight protocol
    #[test]
    fn test_sensor_analyzer_insight_protocol() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "0.2");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights[0].protocol, ProtocolType::Other);
    }

    // Test 513: ActuatorAnalyzer default
    #[test]
    fn test_actuator_analyzer_default() {
        let analyzer = ActuatorAnalyzer::default();
        assert_eq!(analyzer.protocol(), ProtocolType::Modbus);
    }

    // Test 514: ActuatorAnalyzer normal latency
    #[test]
    fn test_actuator_analyzer_normal() {
        let analyzer = ActuatorAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 515: ActuatorAnalyzer latency below threshold
    #[test]
    fn test_actuator_analyzer_below_threshold() {
        let analyzer = ActuatorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        event.latency_us = 400_000;
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 516: ActuatorAnalyzer latency exactly at threshold
    #[test]
    fn test_actuator_analyzer_at_threshold() {
        let analyzer = ActuatorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        event.latency_us = 500_000;
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 517: ActuatorAnalyzer latency just above threshold
    #[test]
    fn test_actuator_analyzer_above_threshold() {
        let analyzer = ActuatorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        event.latency_us = 500_001;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
        assert_eq!(insights[0].level, InsightLevel::Critical);
    }

    // Test 518: ActuatorAnalyzer high latency
    #[test]
    fn test_actuator_analyzer_high_latency() {
        let analyzer = ActuatorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        event.latency_us = 1_000_000;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 519: ActuatorAnalyzer extreme latency
    #[test]
    fn test_actuator_analyzer_extreme_latency() {
        let analyzer = ActuatorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        event.latency_us = u64::MAX;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 520: ActuatorAnalyzer zero latency
    #[test]
    fn test_actuator_analyzer_zero_latency() {
        let analyzer = ActuatorAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 521: ActuatorAnalyzer insight message
    #[test]
    fn test_actuator_analyzer_insight_message() {
        let analyzer = ActuatorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        event.latency_us = 600_000;
        let insights = analyzer.analyze(&event);
        assert!(insights[0].message.contains("Factory"));
        assert!(insights[0].message.contains("PLC"));
        assert!(insights[0].message.contains("Actuator"));
    }

    // Test 522: ActuatorAnalyzer insight protocol
    #[test]
    fn test_actuator_analyzer_insight_protocol() {
        let analyzer = ActuatorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        event.latency_us = 600_000;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights[0].protocol, ProtocolType::Modbus);
    }

    // Test 523: VehicleAnalyzer default
    #[test]
    fn test_vehicle_analyzer_default() {
        let analyzer = VehicleAnalyzer::default();
        assert_eq!(analyzer.protocol(), ProtocolType::Ebpf);
    }

    // Test 524: VehicleAnalyzer no ecu resets
    #[test]
    fn test_vehicle_analyzer_no_resets() {
        let analyzer = VehicleAnalyzer::default();
        let event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 525: VehicleAnalyzer with ecu_resets present
    #[test]
    fn test_vehicle_analyzer_with_resets() {
        let analyzer = VehicleAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("ecu_resets", "1");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
        assert_eq!(insights[0].level, InsightLevel::Critical);
    }

    // Test 526: VehicleAnalyzer with multiple ecu resets
    #[test]
    fn test_vehicle_analyzer_multiple_resets() {
        let analyzer = VehicleAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("ecu_resets", "5");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 527: VehicleAnalyzer with zero ecu resets
    #[test]
    fn test_vehicle_analyzer_zero_resets() {
        let analyzer = VehicleAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("ecu_resets", "0");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1); // Key presence triggers insight
    }

    // Test 528: VehicleAnalyzer with empty ecu_resets
    #[test]
    fn test_vehicle_analyzer_empty_resets() {
        let analyzer = VehicleAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("ecu_resets", "");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1); // Key presence triggers insight
    }

    // Test 529: VehicleAnalyzer insight message
    #[test]
    fn test_vehicle_analyzer_insight_message() {
        let analyzer = VehicleAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("ecu_resets", "1");
        let insights = analyzer.analyze(&event);
        assert!(insights[0].message.contains("Vehicle"));
        assert!(insights[0].message.contains("ECU"));
        assert!(insights[0].message.contains("CAN"));
    }

    // Test 530: VehicleAnalyzer insight protocol
    #[test]
    fn test_vehicle_analyzer_insight_protocol() {
        let analyzer = VehicleAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("ecu_resets", "1");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights[0].protocol, ProtocolType::Ebpf);
    }

    // Test 531: SensorAnalyzer drift parsing edge cases
    #[test]
    fn test_sensor_analyzer_drift_parsing() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        
        event.metadata.add("drift_factor", "0.15");
        assert!(analyzer.analyze(&event).is_empty());
        
        event.metadata.add("drift_factor", "0.1500000001");
        assert_eq!(analyzer.analyze(&event).len(), 1);
        
        event.metadata.add("drift_factor", "not_a_number");
        assert!(analyzer.analyze(&event).is_empty());
        
        event.metadata.add("drift_factor", "inf");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1); // inf > 0.15
    }

    // Test 532: All industrial analyzers implement Send
    #[test]
    fn test_industrial_analyzers_send() {
        fn assert_send<T: Send>() {}
        assert_send::<SensorAnalyzer>();
        assert_send::<ActuatorAnalyzer>();
        assert_send::<VehicleAnalyzer>();
    }

    // Test 533: All industrial analyzers implement Sync
    #[test]
    fn test_industrial_analyzers_sync() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<SensorAnalyzer>();
        assert_sync::<ActuatorAnalyzer>();
        assert_sync::<VehicleAnalyzer>();
    }

    // Test 534: All industrial analyzers implement Debug
    #[test]
    fn test_industrial_analyzers_debug() {
        let sensor = SensorAnalyzer::default();
        let actuator = ActuatorAnalyzer::default();
        let vehicle = VehicleAnalyzer::default();
        
        assert!(format!("{:?}", sensor).contains("SensorAnalyzer"));
        assert!(format!("{:?}", actuator).contains("ActuatorAnalyzer"));
        assert!(format!("{:?}", vehicle).contains("VehicleAnalyzer"));
    }

    // Test 535: SensorAnalyzer with very small drift
    #[test]
    fn test_sensor_analyzer_very_small_drift() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "0.000001");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 536: SensorAnalyzer with boundary drift values
    #[test]
    fn test_sensor_analyzer_boundary_drift() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        
        // Just below threshold
        event.metadata.add("drift_factor", "0.1499999");
        assert!(analyzer.analyze(&event).is_empty());
        
        // Just above threshold
        event.metadata.add("drift_factor", "0.1500001");
        assert_eq!(analyzer.analyze(&event).len(), 1);
    }

    // Test 537: ActuatorAnalyzer boundary latency values
    #[test]
    fn test_actuator_analyzer_boundary_latency() {
        let analyzer = ActuatorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        
        event.latency_us = 499_999;
        assert!(analyzer.analyze(&event).is_empty());
        
        event.latency_us = 500_001;
        assert_eq!(analyzer.analyze(&event).len(), 1);
    }

    // Test 538: VehicleAnalyzer with different ecu_resets values
    #[test]
    fn test_vehicle_analyzer_various_reset_values() {
        let analyzer = VehicleAnalyzer::default();
        let values = vec!["1", "10", "100", "abc", ""];
        
        for val in values {
            let mut event = Event::new(format!("evt-{}", val), ProtocolType::Ebpf);
            event.metadata.add("ecu_resets", val);
            let insights = analyzer.analyze(&event);
            assert_eq!(insights.len(), 1, "Value '{}' should trigger insight", val);
        }
    }

    // Test 539: SensorAnalyzer without drift_factor metadata
    #[test]
    fn test_sensor_analyzer_no_drift_metadata() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("other_key", "other_value");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 540: ActuatorAnalyzer with other metadata
    #[test]
    fn test_actuator_analyzer_with_other_metadata() {
        let analyzer = ActuatorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        event.metadata.add("cycle_time", "100");
        event.latency_us = 600_000;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 541: VehicleAnalyzer with other metadata
    #[test]
    fn test_vehicle_analyzer_with_other_metadata() {
        let analyzer = VehicleAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("vehicle_id", "VIN123");
        event.metadata.add("ecu_resets", "1");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 542: SensorAnalyzer with whitespace in drift_factor
    #[test]
    fn test_sensor_analyzer_whitespace_drift() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", " 0.2 ");
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty()); // Whitespace causes parse failure
    }

    // Test 543: SensorAnalyzer with scientific notation drift
    #[test]
    fn test_sensor_analyzer_scientific_drift() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "2e-1"); // 0.2
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 544: SensorAnalyzer with large drift value
    #[test]
    fn test_sensor_analyzer_large_drift() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "1000000.0");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights.len(), 1);
    }

    // Test 545: ActuatorAnalyzer with very low latency
    #[test]
    fn test_actuator_analyzer_very_low_latency() {
        let analyzer = ActuatorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        event.latency_us = 1;
        let insights = analyzer.analyze(&event);
        assert!(insights.is_empty());
    }

    // Test 546: ActuatorAnalyzer insight level
    #[test]
    fn test_actuator_analyzer_insight_level() {
        let analyzer = ActuatorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Modbus);
        event.latency_us = 600_000;
        let insights = analyzer.analyze(&event);
        assert_eq!(insights[0].level, InsightLevel::Critical);
    }

    // Test 547: VehicleAnalyzer insight level
    #[test]
    fn test_vehicle_analyzer_insight_level() {
        let analyzer = VehicleAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Ebpf);
        event.metadata.add("ecu_resets", "5");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights[0].level, InsightLevel::Critical);
    }

    // Test 548: SensorAnalyzer insight level
    #[test]
    fn test_sensor_analyzer_insight_level() {
        let analyzer = SensorAnalyzer::default();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "0.2");
        let insights = analyzer.analyze(&event);
        assert_eq!(insights[0].level, InsightLevel::Warning);
    }

    // Test 549: Industrial analyzers trait object compatibility
    #[test]
    fn test_industrial_analyzer_trait_object() {
        let analyzers: Vec<Box<dyn Analyzer>> = vec![
            Box::new(SensorAnalyzer::default()),
            Box::new(ActuatorAnalyzer::default()),
            Box::new(VehicleAnalyzer::default()),
        ];
        assert_eq!(analyzers.len(), 3);
        assert_eq!(analyzers[0].protocol(), ProtocolType::Other);
        assert_eq!(analyzers[1].protocol(), ProtocolType::Modbus);
        assert_eq!(analyzers[2].protocol(), ProtocolType::Ebpf);
    }

    // Test 550: All industrial analyzers with empty event ID
    #[test]
    fn test_industrial_analyzers_empty_event_id() {
        let sensor = SensorAnalyzer::default();
        let actuator = ActuatorAnalyzer::default();
        let vehicle = VehicleAnalyzer::default();
        
        let mut event = Event::new("".to_string(), ProtocolType::Other);
        event.metadata.add("drift_factor", "0.2");
        assert_eq!(sensor.analyze(&event).len(), 1);
        
        let mut event = Event::new("".to_string(), ProtocolType::Modbus);
        event.latency_us = 600_000;
        assert_eq!(actuator.analyze(&event).len(), 1);
        
        let mut event = Event::new("".to_string(), ProtocolType::Ebpf);
        event.metadata.add("ecu_resets", "1");
        assert_eq!(vehicle.analyze(&event).len(), 1);
    }
}
