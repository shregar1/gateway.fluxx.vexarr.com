//! Cache Integrations
//!
//! Monitor Memcached, Varnish, and other caching systems.

pub mod memcached;
pub mod varnish;

pub use memcached::MemcachedIntegration;
pub use varnish::VarnishIntegration;
