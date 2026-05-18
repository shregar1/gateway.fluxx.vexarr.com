pub mod plugin;

use crate::plugins::plugin::Plugin;
use crate::core::traits::Processor;
use crate::analysis::Analyzer;
use crate::core::error::Result;
use std::sync::Arc;
use tracing::{info, warn};

/// PluginManager: The central registry for all active Fluxx plugins.
/// Coordinates the discovery and activation of third-party extensions.
#[derive(Debug, Default)]
pub struct PluginManager {
    plugins: Vec<Arc<dyn Plugin>>,
}

impl PluginManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers and initializes a new plugin into the Fluxx engine.
    pub fn register(&mut self, plugin: Arc<dyn Plugin>) -> Result<()> {
        info!("Plugins: Registering extension '{}'...", plugin.name());
        
        // Trigger the initialization hook
        plugin.on_load()?;
        
        self.plugins.push(plugin);
        Ok(())
    }

    /// Aggregates all processors provided by the active plugin collection.
    /// This is the bridge between the plugin system and the main pipeline.
    pub fn aggregate_processors(&self) -> Vec<Arc<dyn Processor>> {
        self.plugins.iter()
            .flat_map(|p| p.processors())
            .collect()
    }

    /// Aggregates all specialized analyzers provided by the active plugin collection.
    /// This is the bridge between the plugin system and the intelligence layer.
    pub fn aggregate_analyzers(&self) -> Vec<Arc<dyn Analyzer>> {
        self.plugins.iter()
            .flat_map(|p| p.analyzers())
            .collect()
    }

    /// Triggers the global shutdown sequence for all enabled extensions.
    pub fn shutdown_all(&self) {
        for plugin in &self.plugins {
            info!("Plugins: Unloading extension '{}'...", plugin.name());
            if let Err(e) = plugin.on_unload() {
                warn!("Plugins: Plugin '{}' shutdown error: {}", plugin.name(), e);
            }
        }
    }
}
