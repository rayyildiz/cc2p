//! # CC2P - Convert CSV to Parquet
//!
//! A library for converting CSV files to Parquet format with support for
//! asynchronous operations, custom delimiters, and schema inference.
//!
//! ## Features
//!
//! - Asynchronous file operations
//! - Custom delimiters
//! - Schema inference
//! - Header detection
//! - Duplicate column handling
//! - Parallel processing
//!
//! ## Example
//!
//! ```rust
//! use std::path::PathBuf;
//! use cc2p::conversion::convert_to_parquet;
//!
//! #[tokio::main]
//! async fn main() -> cc2p::error::Result<()> {
//!     let file_path = PathBuf::from("testdata/sample.csv");
//!     let delimiter = ',';
//!     let has_header = true;
//!     let sampling_size = 10;
//!
//!     convert_to_parquet(&file_path, delimiter, has_header, sampling_size).await?;
//!
//!     Ok(())
//! }
//! ```

pub mod conversion;
pub mod error;
pub mod tui;
pub mod utils;

// Re-export commonly used items
pub use conversion::convert_to_parquet;
pub use conversion::convert_to_parquet_with_columns;
pub use conversion::infer_schema;
pub use conversion::remove_deduplicate_columns;
pub use utils::clean_column_name;
pub use utils::find_files;
