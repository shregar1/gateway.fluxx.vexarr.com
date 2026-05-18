// Core network protocols and capture layer.
pub mod common;
pub mod http;
pub mod websocket;
pub mod quic;
pub mod grpc;
// pub mod ebpf;  // disabled: stale source, missing imports
pub mod modbus;
// pub mod pcap;  // disabled: shadowed by local module name
pub mod server;
pub mod system;

// Telemetry ingestion protocols
pub mod otlp;
pub mod statsd;
// Prometheus remote_write moved to fluxx-api (the HTTP handler lives
// there). No public submodule remains under `protocols::` for it; the
// route is now wired as `crate::prometheus::remote_write` from inside
// fluxx-api's `server.rs`.
pub mod tracecontext;
