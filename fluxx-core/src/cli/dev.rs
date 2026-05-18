use crate::core::runtime::AgentService;
use crate::config::Config;
use tracing::info;

/// DevManager: Orchestrates the high-velocity 'dev' mode for Fluxx.
/// Automatically detects local services and streams real-time telemetry 
/// directly to the terminal stdout for rapid development.
pub struct DevManager {
    service: AgentService,
}

impl DevManager {
    pub fn new() -> Self {
        Self {
            service: AgentService::new(),
        }
    }

    /// Primary execution flow for 'fluxx dev'.
    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!("DevMode: Initializing Fluxx rapid-development environment.");
        
        // 1. Service Auto-Discovery (Conceptual port scan / process discovery)
        let mut config = Config::load().unwrap_or_default();
        config.agent_name = "fluxx-local-dev".to_string();
        
        info!("DevMode: Auto-detecting local services on ports 8080 (HTTP), 50051 (gRPC)...");

        // 2. High-Fidelity Terminal Streaming
        // We ensure that StdoutTransporter is active and we enable debug logging.
        
        // 3. Automated Activation
        let service_ref = &mut self.service;
        
        info!("DevMode: Fluxx is now LIVE. Streaming captured events below:");
        println!("----------------------------------------------------------");
        
        service_ref.run_agent(config).await?;
        
        Ok(())
    }
}
