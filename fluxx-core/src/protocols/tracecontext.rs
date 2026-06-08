//! W3C TraceContext (traceparent / tracestate) parsing and generation.
//!
//! The implementation now lives in `utilx::tracecontext`. This file is a
//! thin re-export so existing call sites in fluxx continue to compile
//! against `crate::protocols::tracecontext::{TraceContext, SpanTiming}`.
//!
//! See [`utilx::tracecontext`] for the W3C Trace Context spec
//! (<https://www.w3.org/TR/trace-context/>) implementation, including
//! `tracestate` propagation, header injection/extraction helpers, and
//! the `child` / `new_root` constructors.

pub use utilx::tracecontext::{SpanTiming, TraceContext};
