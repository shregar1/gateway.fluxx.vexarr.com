//! TLS certificate pinning.
//!
//! The implementation lives in `utilx::tls_pin`; this file is a thin
//! re-export so existing fluxx call sites continue to use
//! `crate::license::cert_pin::{pinned_client, PinError}`.
//!
//! ## Operator instructions
//!
//! Optional: when `FLUXX_API_TLS_PIN` is set to a base64-encoded SHA-256 of
//! the expected SubjectPublicKeyInfo, the HTTP client built with
//! [`pinned_client`] will reject any TLS connection where the leaf
//! certificate's SPKI doesn't match. When unset, the client uses the system
//! trust store as usual.
//!
//! Capture the production pin once with:
//!
//! ```sh
//! openssl s_client -connect api.fluxx.dev:443 -servername api.fluxx.dev </dev/null 2>/dev/null \
//!   | openssl x509 -pubkey -noout \
//!   | openssl pkey -pubin -outform der \
//!   | openssl dgst -sha256 -binary \
//!   | base64
//! ```
//!
//! Bake the result into the agent build (or distribute via env) and ship.

pub use utilx::tls_pin::{PinError, pinned_client};
