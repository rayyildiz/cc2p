use thiserror::Error;

/// Custom error types for the cc2p library.
#[derive(Error, Debug)]
pub enum Cc2pError {
    /// Error that occurs when a file operation fails.
    #[error("File operation failed: {0}")]
    FileError(#[from] std::io::Error),

    /// Error that occurs when CSV parsing fails.
    #[error("CSV parsing error: {0}")]
    CsvError(String),

    /// Error that occurs when Parquet writing fails.
    #[error("Parquet writing error: {0}")]
    ParquetError(String),

    /// Error that occurs when schema inference fails.
    #[error("Schema inference error: {0}")]
    SchemaError(String),

    /// Error that occurs when a pattern matching operation fails.
    #[error("Pattern matching error: {0}")]
    PatternError(String),

    /// A generic error type for other errors.
    #[error("Other error: {0}")]
    Other(String),
}

/// Result type alias for cc2p operations.
pub type Result<T> = std::result::Result<T, Cc2pError>;

/// Convert any error type to a Cc2pError::Other.
pub fn to_cc2p_error<E: std::error::Error>(err: E) -> Cc2pError {
    Cc2pError::Other(err.to_string())
}

// Arrow CSV errors are typically returned as Box<dyn Error>
// We'll handle them in the conversion code directly

impl From<parquet::errors::ParquetError> for Cc2pError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        Cc2pError::ParquetError(err.to_string())
    }
}

impl From<Box<dyn std::error::Error>> for Cc2pError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        Cc2pError::Other(err.to_string())
    }
}
