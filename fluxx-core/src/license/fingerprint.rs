//! Machine fingerprint.
//!
//! The implementation lives in `utilx::fingerprint`; this file is a thin
//! wrapper that pins the fluxx-specific version prefix (`fluxx-fp-v1`) so
//! existing on-disk caches and control-plane bindings continue to validate.
//! The cached call (`current()`) keeps the lock-free hot path that the rest
//! of the gateway relies on.
//!
//! See `utilx::fingerprint` for the input mix, fallback chain across OSes,
//! and the rationale for excluding unstable inputs like PID / wall-clock.

use std::sync::OnceLock;

/// Version prefix mixed into the fingerprint hash. Bumping this invalidates
/// every existing fluxx-issued grant, so don't touch unless the algorithm
/// changes.
const FLUXX_FP_PREFIX: &str = "fluxx-fp-v1";

static CACHED: OnceLock<String> = OnceLock::new();

/// The sha256-hex fingerprint for the current host. Computed once and cached
/// for the process lifetime.
pub fn current() -> &'static str {
    CACHED.get_or_init(|| utilx::fingerprint::compute_with_prefix(FLUXX_FP_PREFIX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_64_hex_chars() {
        let fp = current();
        assert_eq!(fp.len(), 64);
        assert!(fp.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn fingerprint_is_stable_across_calls() {
        let a = current();
        let b = current();
        assert_eq!(a, b);
    }

    #[test]
    fn fluxx_prefix_distinct_from_utilx_default() {
        // Sanity guard: the fluxx-flavoured fingerprint must not collide with
        // the utilx-flavoured default, even on the same host. If this fails,
        // a refactor accidentally let the prefix leak.
        let fluxx_fp = current();
        let utilx_fp = utilx::fingerprint::current();
        assert_ne!(fluxx_fp, utilx_fp);
    }
}
