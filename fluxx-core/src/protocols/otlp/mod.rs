pub mod convert;
pub mod grpc_server;
// `http_handlers` was moved into the sibling `fluxx-api` crate as
// `fluxx_api::otlp_http_handlers` — those axum handlers need
// `ApiState`, which lives in fluxx-api, so keeping them here would
// reintroduce the api↔core dependency cycle.
pub mod http_server;
