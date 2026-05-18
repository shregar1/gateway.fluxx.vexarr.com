use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Capture failed: {0}")]
    Capture(String),

    #[error("Processing failed: {0}")]
    Processing(String),

    #[error("Transport failed: {0}")]
    Transport(String),

    #[error("Protocol logic error ('{0}'): {1}")]
    Protocol(String, String),

    #[error("Configuration fault: {0}")]
    Config(String),

    #[error("Batch processing error: {0}")]
    Batch(String),

    #[error("Internal I/O failure: {0}")]
    Io(#[from] std::io::Error),

    #[error("Network runtime failure: {0}")]
    Network(#[from] reqwest::Error),

    #[error("Internal structural error: {0}")]
    Internal(String),

    #[error("Unexpected event loop error")]
    Unknown,
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    // Test 51: Capture error display
    #[test]
    fn test_error_capture_display() {
        let err = Error::Capture("test capture".to_string());
        assert_eq!(format!("{}", err), "Capture failed: test capture");
    }

    // Test 52: Processing error display
    #[test]
    fn test_error_processing_display() {
        let err = Error::Processing("test processing".to_string());
        assert_eq!(format!("{}", err), "Processing failed: test processing");
    }

    // Test 53: Transport error display
    #[test]
    fn test_error_transport_display() {
        let err = Error::Transport("test transport".to_string());
        assert_eq!(format!("{}", err), "Transport failed: test transport");
    }

    // Test 54: Protocol error display
    #[test]
    fn test_error_protocol_display() {
        let err = Error::Protocol("http".to_string(), "invalid header".to_string());
        assert_eq!(format!("{}", err), "Protocol logic error ('http'): invalid header");
    }

    // Test 55: Config error display
    #[test]
    fn test_error_config_display() {
        let err = Error::Config("missing file".to_string());
        assert_eq!(format!("{}", err), "Configuration fault: missing file");
    }

    // Test 56: Batch error display
    #[test]
    fn test_error_batch_display() {
        let err = Error::Batch("timeout".to_string());
        assert_eq!(format!("{}", err), "Batch processing error: timeout");
    }

    // Test 57: Internal error display
    #[test]
    fn test_error_internal_display() {
        let err = Error::Internal("structural".to_string());
        assert_eq!(format!("{}", err), "Internal structural error: structural");
    }

    // Test 58: Unknown error display
    #[test]
    fn test_error_unknown_display() {
        let err = Error::Unknown;
        assert_eq!(format!("{}", err), "Unexpected event loop error");
    }

    // Test 59: Io error from std::io::Error
    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    // Test 60: Error debug format
    #[test]
    fn test_error_debug() {
        let err = Error::Capture("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("Capture"));
    }
}
