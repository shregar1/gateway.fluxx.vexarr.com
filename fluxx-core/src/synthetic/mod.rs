//! Synthetic Monitoring
//!
//! HTTP uptime checks, multi-step API tests, and SSL certificate expiry monitoring.
//! Runs checks on configurable intervals and reports results as metrics + events.

pub mod checks;
pub mod runner;
pub mod ssl;
