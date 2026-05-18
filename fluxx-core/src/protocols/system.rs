//! System Resource Capturer for Host Observability
//! Collects CPU, memory, and disk telemetry from the standalone server.

use crate::core::traits::Capturer;
use crate::models::event::{Event, EventSeverity, ProtocolType, Metadata};
use crate::core::error::Result;
use async_trait::async_trait;
use sysinfo::{System, CpuRefreshKind, RefreshKind, MemoryRefreshKind};
use tracing::debug;
use std::time::SystemTime;
use parking_lot::Mutex;

/// SystemCapturer: High-performance cross-platform system resource monitor.
#[derive(Debug)]
pub struct SystemCapturer {
    name: String,
    interval_secs: u64,
    sys: Mutex<System>,
}

impl SystemCapturer {
    pub fn new(name: &str, interval_secs: u64) -> Self {
        Self {
            name: name.to_string(),
            interval_secs,
            sys: Mutex::new(System::new_with_specifics(
                RefreshKind::new()
                    .with_cpu(CpuRefreshKind::everything())
                    .with_memory(MemoryRefreshKind::everything())
            )),
        }
    }
}

#[async_trait]
impl Capturer for SystemCapturer {
    fn name(&self) -> &str {
        &self.name
    }

    async fn capture(&self) -> Result<Event> {
        tokio::time::sleep(tokio::time::Duration::from_secs(self.interval_secs)).await;
        
        let agent_name = whoami::devicename(); // More stable on macOS than hostname()
        let (cpu_usage, mem_pct, total_mem, used_mem) = {
            let mut sys = self.sys.lock();
            sys.refresh_all();
            
            let cpu_usage = sys.global_cpu_info().cpu_usage();
            let total_mem = sys.total_memory();
            let used_mem = sys.used_memory();
            let mem_pct = (used_mem as f32 / total_mem as f32) * 100.0;
            (cpu_usage, mem_pct, total_mem, used_mem)
        };

        debug!(cpu = %cpu_usage, mem_pct = %mem_pct, "System metrics collected");

        let mut metadata = Metadata::new();
        metadata.add("cpu_usage_pct", &format!("{:.2}", cpu_usage));
        metadata.add("memory_used_bytes", &used_mem.to_string());
        metadata.add("memory_total_bytes", &total_mem.to_string());
        metadata.add("memory_usage_pct", &format!("{:.2}", mem_pct));
        metadata.add("load_avg_1m", &System::load_average().one.to_string());
        metadata.add("uptime_h", &format!("{:.2}", System::uptime() as f64 / 3600.0));
        metadata.user_agent = Some(format!("fluxx-agent/{}", agent_name));

        let severity = if cpu_usage > 90.0 || mem_pct > 90.0 {
            EventSeverity::Critical
        } else if cpu_usage > 70.0 || mem_pct > 70.0 {
            EventSeverity::High
        } else {
            EventSeverity::Info
        };

        let timestamp_us = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        Ok(Event {
            id: uuid::Uuid::new_v4().to_string(),
            session_id: Some("default_system_session".to_string()),
            project_id: Some("standalone".to_string()),
            environment_id: Some("production".to_string()),
            service_id: Some("host-health".to_string()),
            timestamp_us,
            protocol: ProtocolType::System,
            latency_us: 0,
            severity,
            payload: Some(format!("System Monitor: CPU {:.1}% | RAM {:.1}%", cpu_usage, mem_pct)),
            metadata,
        })
    }
}
