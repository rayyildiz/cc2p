use crate::error::{Cc2pError, Result};
use glob::{MatchOptions, glob_with};
use once_cell::sync::Lazy;
use regex::Regex;
use std::path::PathBuf;

/// Static regex pattern for cleaning column names
static COLUMN_NAME_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"[^a-zA-Z0-9_\-\s]").unwrap());

/// Cleans a given string by removing any characters that are not alphanumeric or whitespace.
///
/// # Arguments
///
/// * `column_name` - The string to be cleaned.
///
/// # Examples
///
/// ```rust
/// use cc2p::utils::clean_column_name;
///
/// let name = clean_column_name("John!Doe");
/// assert_eq!(name, "JohnDoe");
///
/// let name = clean_column_name("Welcome, User 123!");
/// assert_eq!(name, "Welcome User 123");
/// ```
///
/// # Returns
///
/// A `String` containing the cleaned string, with all non-alphanumeric characters removed.
pub fn clean_column_name(column_name: &str) -> String {
    COLUMN_NAME_REGEX.replace_all(column_name, "").to_string()
}

/// Searches for files matching the given pattern.
///
/// # Arguments
///
/// * `pattern` - A string slice representing the search pattern.
///
/// # Returns
///
/// A Result containing a vector of `PathBuf` representing the paths of the matching files.
///
/// # Examples
///
/// ```rust
/// use std::path::PathBuf;
/// use cc2p::utils::find_files;
///
/// #[tokio::main]
/// async fn main() -> cc2p::error::Result<()> {
///     let pattern = "testdata/sample*.csv";
///     let files = find_files(pattern)?;
///
///     for file in files {
///         println!("{:?}", file);
///     }
///     Ok(())
/// }
/// ```
pub fn find_files(pattern: &str) -> Result<Vec<PathBuf>> {
    let mut files = vec![];
    let options = MatchOptions {
        case_sensitive: false,
        require_literal_separator: false,
        require_literal_leading_dot: false,
    };

    for entry in glob_with(pattern, options).map_err(|e| Cc2pError::PatternError(e.to_string()))? {
        match entry {
            Ok(p) => {
                if p.is_file() {
                    if let Some(ext) = p.extension() {
                        if ext == "csv" {
                            files.push(p);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("{:?}", e);
                // Continue processing other files even if one fails
            }
        }
    }

    Ok(files)
}

/// Deletes a file if it exists.
///
/// # Arguments
///
/// * `filename` - The name of the file to delete.
///
/// # Returns
///
/// Returns `Ok(())` if the file was deleted or didn't exist, otherwise returns an error.
///
/// # Examples
///
/// ```rust
/// use cc2p::utils::delete_if_exist;
///
/// #[tokio::main]
/// async fn main() -> cc2p::error::Result<()> {
///     delete_if_exist("temp_file.txt").await?;
///     Ok(())
/// }
/// ```
pub async fn delete_if_exist(filename: &str) -> Result<()> {
    if tokio::fs::metadata(filename).await.is_ok() {
        tokio::fs::remove_file(filename).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::fs;

    #[test]
    fn test_clean_column_names() {
        assert_eq!(clean_column_name("abc"), "abc");
        assert_eq!(clean_column_name("ab c"), "ab c");
        assert_eq!(clean_column_name("ab.c"), "abc");
        assert_eq!(clean_column_name("ab-_c"), "ab-_c");
        assert_eq!(clean_column_name("Abc"), "Abc");
        assert_eq!(clean_column_name("a8A"), "a8A");
        assert_eq!(clean_column_name("a@bc"), "abc");
        assert_eq!(clean_column_name("abc#"), "abc");
        assert_eq!(clean_column_name("ab}}[}c"), "abc");
        assert_eq!(clean_column_name("ab c "), "ab c ");
    }

    #[tokio::test]
    async fn test_find_files() {
        assert_eq!(find_files("testdata/sample.csv").unwrap().len(), 1);
        assert_eq!(find_files("testdata/*.csv").unwrap().len(), 4);
        assert_eq!(find_files("not-exist/*.csv").unwrap().len(), 0);
        assert_eq!(find_files("testdata/*delimi*.csv").unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_delete_if_exist() {
        // Create a temporary file
        let temp_file = "testdata/temp_test_file.txt";
        fs::write(temp_file, "test content").await.unwrap();

        // Verify file exists
        assert!(fs::metadata(temp_file).await.is_ok());

        // Delete the file
        let result = delete_if_exist(temp_file).await;
        assert!(result.is_ok());

        // Verify file no longer exists
        assert!(fs::metadata(temp_file).await.is_err());

        // Test deleting a non-existent file (should not error)
        let result = delete_if_exist("testdata/non_existent_file.txt").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_find_files_with_different_extensions() {
        // Create a temporary file with a non-csv extension
        let temp_file = "testdata/temp_test_file.txt";
        fs::write(temp_file, "test content").await.unwrap();

        // Test that find_files only returns csv files
        let files = find_files("testdata/*").unwrap();
        for file in &files {
            assert_eq!(file.extension().unwrap(), "csv");
        }

        // Clean up
        let _ = delete_if_exist(temp_file).await;
    }

    #[test]
    fn test_clean_column_name_edge_cases() {
        // Test with empty string
        assert_eq!(clean_column_name(""), "");

        // Test with only special characters
        assert_eq!(clean_column_name("@#$%^&*()"), "");

        // Test with mixed characters and whitespace
        assert_eq!(clean_column_name(" a@b c# "), " ab c ");

        // Test with numbers
        assert_eq!(clean_column_name("123"), "123");

        // Test with very long string
        let long_string = "a".repeat(1000) + "@#$";
        let expected = "a".repeat(1000);
        assert_eq!(clean_column_name(&long_string), expected);
    }
}
