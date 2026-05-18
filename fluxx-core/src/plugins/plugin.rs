use crate::core::traits::Processor;
use crate::analysis::Analyzer;
use std::sync::Arc;

/// Plugin: The core interface for extending Fluxx functionality.
/// Plugins can contribute new data processors, protocol analyzers, and logic.
pub trait Plugin: Send + Sync + std::fmt::Debug {
    /// Returns the unique human-readable name of the plugin.
    fn name(&self) -> &str;

    /// Returns a list of new processors provided by this plugin.
    fn processors(&self) -> Vec<Arc<dyn Processor>>;

    /// Returns a list of protocol-specific analyzers provided by this plugin.
    fn analyzers(&self) -> Vec<Arc<dyn Analyzer>>;

    /// Lifecycle hook: Called during plugin initialization.
    fn on_load(&self) -> crate::core::error::Result<()> {
        Ok(())
    }

    /// Lifecycle hook: Called during plugin shutdown.
    fn on_unload(&self) -> crate::core::error::Result<()> {
        Ok(())
    }
}
