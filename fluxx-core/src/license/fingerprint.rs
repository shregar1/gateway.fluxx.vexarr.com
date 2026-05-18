//! Machine fingerprint.
//!
//! sha256 over a small set of **stable** machine identifiers. The hash is
//! sent to the control plane on every grant mint and refresh. The raw inputs
//! never leave the host — only the hash.
//!
//! Stable inputs (chosen for low churn under normal ops):
//!   * machine UUID — Linux `/etc/machine-id`, macOS `IOPlatformUUID`,
//!     Windows registry. Survives reboots, dies on reinstall.
//!   * primary MAC address — first non-loopback MAC. Drifts on VM resize
//!     or new NIC, which is acceptable: that's a real "new device".
//!   * hostname.
//!   * CPU brand string.
//!
//! Deliberately UNSTABLE inputs we do NOT use:
//!   * current PID, process ID
//!   * wall-clock time
//!   * any environment variable (would let a tampered binary trivially
//!     forge a different machine)
//!
//! The fingerprint is computed once at startup and cached in memory for the
//! lifetime of the process. Use `current()` from anywhere — it's lock-free
//! after the first call.

use sha2::{Digest, Sha256};
use std::sync::OnceLock;

static CACHED: OnceLock<String> = OnceLock::new();

/// The sha256-hex fingerprint for the current host. Computed once.
pub fn current() -> &'static str {
    CACHED.get_or_init(compute)
}

fn compute() -> String {
    let mut h = Sha256::new();
    h.update(b"fluxx-fp-v1\0");
    h.update(read_machine_id().as_bytes());
    h.update(b"\0");
    h.update(read_primary_mac().as_bytes());
    h.update(b"\0");
    h.update(read_hostname().as_bytes());
    h.update(b"\0");
    h.update(read_cpu_brand().as_bytes());
    hex::encode(h.finalize())
}

fn read_machine_id() -> String {
    // Linux first.
    if let Ok(s) = std::fs::read_to_string("/etc/machine-id") {
        let id = s.trim();
        if !id.is_empty() {
            return id.to_string();
        }
    }
    // /var/lib/dbus path (some distros).
    if let Ok(s) = std::fs::read_to_string("/var/lib/dbus/machine-id") {
        let id = s.trim();
        if !id.is_empty() {
            return id.to_string();
        }
    }
    // macOS fallback via ioreg if available.
    if let Ok(out) = std::process::Command::new("ioreg")
        .args(["-rd1", "-c", "IOPlatformExpertDevice"])
        .output()
    {
        if let Ok(s) = std::str::from_utf8(&out.stdout) {
            for line in s.lines() {
                if let Some(eq) = line.find("IOPlatformUUID") {
                    if let Some(start) = line[eq..].find('"') {
                        let after = &line[eq + start + 1..];
                        if let Some(end) = after.find('"') {
                            return after[..end].to_string();
                        }
                    }
                }
            }
        }
    }
    // Windows fallback via WMI's `wmic csproduct get uuid`.
    if cfg!(windows) {
        if let Ok(out) = std::process::Command::new("wmic")
            .args(["csproduct", "get", "uuid"])
            .output()
        {
            if let Ok(s) = std::str::from_utf8(&out.stdout) {
                for line in s.lines() {
                    let trimmed = line.trim();
                    // First non-header line that looks like a UUID.
                    if trimmed.len() == 36 && trimmed.matches('-').count() == 4 {
                        return trimmed.to_string();
                    }
                }
            }
        }
    }
    // Last-ditch: stable across the process lifetime even if we can't read
    // a real machine id. This is rare in practice (no /etc/machine-id, no
    // ioreg, no wmic) and the grant verification will simply rebind to
    // whatever we report.
    "unknown-machine".to_string()
}

fn read_primary_mac() -> String {
    match mac_address::get_mac_address() {
        Ok(Some(addr)) => addr.to_string(),
        _ => "00:00:00:00:00:00".to_string(),
    }
}

fn read_hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|os| os.into_string().ok())
        .unwrap_or_else(|| "unknown-host".to_string())
}

fn read_cpu_brand() -> String {
    // sysinfo 0.30 removed the SystemExt/CpuExt traits — methods are now
    // directly on the structs.
    use sysinfo::System;
    let mut sys = System::new();
    sys.refresh_cpu();
    sys.cpus()
        .first()
        .map(|c| c.brand().to_string())
        .unwrap_or_else(|| "unknown-cpu".to_string())
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
    fn compute_is_deterministic_for_the_same_machine() {
        // Compute twice in a row; should match within the same process.
        let a = compute();
        let b = compute();
        assert_eq!(a, b);
    }
}
