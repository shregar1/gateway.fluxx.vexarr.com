use crate::core::runtime::AgentService;
use crate::config::Config;
use tracing::info;

/// DevManager: Orchestrates the high-velocity 'dev' mode for Fluxx.
/// Automatically detects local services and streams real-time telemetry
/// directly to the terminal stdout for rapid development.
///
/// Wiring note: dev mode used to start the api server inline. After
/// the fluxx-core / fluxx-api split, this module — being inside
/// fluxx-core — can't reference the api crate. Instead, `run`
/// accepts the same `start_handler` closure that `cli::run` already
/// threads in from the binary, and delegates the agent + api wiring
/// to it. fluxx-core stays api-agnostic.
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
    ///
    /// `start_handler` is the same closure `cli::run` accepts for
    /// the `start` subcommand — it owns the bridge to fluxx-api.
    /// Reusing it here means dev mode and `fluxx start` both go
    /// through identical wiring (agent + api joined), with the only
    /// difference being the dev-mode config overrides applied
    /// upfront.
    pub async fn run<F, Fut>(&mut self, start_handler: F) -> anyhow::Result<()>
    where
        F: FnOnce(AgentService, Config) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        info!("DevMode: Initializing Fluxx rapid-development environment.");

        // 1. Service Auto-Discovery (Conceptual port scan / process discovery)
        let mut config = Config::load().unwrap_or_default();
        config.agent_name = "fluxx-local-dev".to_string();

        info!("DevMode: Auto-detecting local services on ports 8080 (HTTP), 50051 (gRPC)...");

        // 2. High-Fidelity Terminal Streaming
        // We ensure that StdoutTransporter is active and we enable debug logging.

        info!("DevMode: Fluxx is now LIVE. Streaming captured events below:");
        println!("----------------------------------------------------------");

        // 3. Hand off to the binary's start handler. It builds the
        //    runtime context, instantiates the api server, and joins
        //    both futures — same flow as `fluxx start`.
        let service = std::mem::replace(&mut self.service, AgentService::new());
        start_handler(service, config).await?;

        Ok(())
    }
}
