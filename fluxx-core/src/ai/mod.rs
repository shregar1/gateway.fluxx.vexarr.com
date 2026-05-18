//! AI Assistant Integrations
//!
//! This module provides integrations with AI coding assistants like Claude,
//! GitHub Copilot, Cursor, Continue.dev, and others.

pub mod diagnose_remote;
pub mod mcp_server;
pub mod nlp_query;

pub use mcp_server::{FluxxMcpServer, McpConfig, start_mcp_server_stdio};
pub use nlp_query::NlpQueryEngine;
