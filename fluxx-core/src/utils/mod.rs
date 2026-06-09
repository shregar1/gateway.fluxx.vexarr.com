//! Initialise the global tracing subscriber.
//!
//! The implementation lives in `utilx::logging::init_default` (operator-
//! friendly defaults: level `info` overridable by `RUST_LOG`, target column
//! hidden, thread ids + line numbers shown). This file is a thin wrapper so
//! existing fluxx call sites continue to use `crate::utils::init_logging`.

/// Initialise the global tracing subscriber with operator-friendly defaults.
/// See `utilx::logging::init_default` for the exact configuration.
pub fn init_logging() {
    utilx::logging::init_default();
}
