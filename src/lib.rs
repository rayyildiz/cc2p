use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::{PathBuf};
use std::sync::Arc;
use glob::{glob_with, MatchOptions};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;


/// The maximum number of records that can be read to determine schema.
const READ_MAX_RECORDS: usize = 10;

/// Convert a CSV file to Parquet format.
///
/// # Arguments
///
/// * `file_path` - The path to the CSV file.
/// * `delimiter` - The delimiter used in the CSV file.
/// * `has_header` - Indicates whether the CSV file has a header row.
///
/// # Errors
///
/// Returns an error if there was a problem reading the file or converting it to Parquet format.
///
/// # Example
///
/// ```
/// use std::path::PathBuf;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     use cc2p::convert_to_parquet;
///
///     let file_path = PathBuf::from("testdata/sample.csv");
///     let delimiter = ',';
///     let has_header = true;
///
///     convert_to_parquet(&file_path, delimiter, has_header)?;
///
///     Ok(())
/// }
/// ```
pub fn convert_to_parquet(
    file_path: &PathBuf,
    delimiter: char,
    has_header: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;

    let (csv_schema, _) = arrow_csv::reader::Format::default()
        .with_header(has_header)
        .with_delimiter(delimiter as u8)
        .infer_schema(file, Some(READ_MAX_RECORDS))?;

    let schema_ref = remove_deduplicate_columns(csv_schema);

    let file = File::open(file_path)?;
    let mut csv = arrow_csv::ReaderBuilder::new(schema_ref.clone())
        .with_delimiter(delimiter as u8)
        .with_header(has_header)
        .build(file)?;

    let target_file = file_path.with_extension("parquet");

    // delete if exist
    delete_if_exist(target_file.to_str().unwrap())?;

    let mut file = File::create(target_file).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by("cc2p".to_string())
        .build();

    let mut parquet_writer = parquet::arrow::ArrowWriter::try_new(&mut file, schema_ref, Some(props))?;

    for batch in csv.by_ref() {
        match batch {
            Ok(batch) => parquet_writer.write(&batch)?,
            Err(_error) => {
                return Err(Box::new(_error));
            }
        }
    }


    parquet_writer.close()?;

    Ok(())
}

/// Deletes a file if it exists.
///
/// # Arguments
///
/// * `filename` - The name of the file to delete.
///
/// # Errors
///
/// Returns `Err` if there is an error accessing the file or deleting it.
///
pub fn delete_if_exist(filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    if fs::metadata(filename).is_ok() {
        fs::remove_file(filename)?;
    }

    Ok(())
}

struct Empty {}

/// Removes duplicate columns from a given Arrow schema, and returns a new schema with deduplicated columns.
///
/// # Arguments
///
/// * `sc` - The input Arrow schema.
///
/// # Returns
///
/// Returns an `Arc` containing the deduplicated schema.
pub fn remove_deduplicate_columns(sc: arrow_schema::Schema) -> Arc<arrow_schema::Schema> {
    let mut index = 1;
    let mut deduplicated_fields = Vec::new();
    let mut names = HashMap::new();
    for field in sc.fields() {
        let field_name = field.name().as_str();
        let field_name = clean_column_name(field_name);

        if let std::collections::hash_map::Entry::Vacant(e) = names.entry(field_name.clone()) {
            e.insert(Empty {});

            if field.name().is_empty() {
                let name = format!("column_{}", index);
                index += 1;
                let new_field = <arrow_schema::Field as Clone>::clone(&(*field).clone()).with_name(name);
                deduplicated_fields.push(Arc::new(new_field));
            } else {
                deduplicated_fields.push(field.clone());
            }
        } else {
            let name = format!("{}_{}", field_name, index);
            index += 1;
            let new_field = <arrow_schema::Field as Clone>::clone(&(*field).clone()).with_name(name);
            deduplicated_fields.push(Arc::new(new_field));
        }
    }

    let list_fields: Vec<_> = deduplicated_fields.into_iter().collect();

    let deduplicated_schema = arrow_schema::Schema::new_with_metadata(list_fields, sc.metadata);

    Arc::new(deduplicated_schema)
}

/// Searches for files matching the given pattern.
///
/// # Arguments
///
/// * `pattern` - A string slice representing the search pattern.
///
/// # Returns
///
/// A vector of `PathBuf` representing the paths of the matching files.
///
/// # Panics
///
/// This function will panic if it fails to read the file search pattern.
///
/// # Examples
///
/// ```rust
/// use std::path::PathBuf;
/// use cc2p::find_files;
///
/// let pattern = "testdata/sample*.csv";
/// let files = find_files(pattern);
///
/// for file in files {
///     println!("{:?}", file);
/// }
/// ```
pub fn find_files(pattern: &str) -> Vec<PathBuf> {
    let mut files = vec![];
    let options = MatchOptions {
        case_sensitive: false,
        require_literal_separator: false,
        require_literal_leading_dot: false,
    };

    for entry in glob_with(pattern, options).expect("failed to read file search pattern") {
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
            Err(e) => eprintln!("{:?}", e),
        }
    }

    files
}

/// Cleans a given string by removing any characters that are not alphanumeric or whitespace.
///
/// # Arguments
///
/// * `column_name` - The string to be cleaned.
///
/// # Examples
///
/// ```rust
/// use cc2p::*;
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
    let cleaned = regex::Regex::new(r"[^a-zA-Z0-9_\-\s]").unwrap().replace_all(column_name, "");

    cleaned.to_string()
}

#[cfg(test)]
mod tests {
    use arrow_schema::{Field};
    use super::*;

    #[test]
    fn test_convert_to_parquet() {
        let mut source_file = std::env::current_dir().unwrap();
        source_file.push("testdata");
        source_file.push("sample_empty_header.csv");

        let result = convert_to_parquet(&source_file, ',', true);

        // Check that the function completed successfully
        assert!(result.is_ok());

        let parquet_file = PathBuf::from("testdata/sample_empty_header.parquet");
        // Verify the parquet file was created
        assert!(parquet_file.exists());

        // Optionally, clean up the parquet file
        fs::remove_file(parquet_file).unwrap();
    }

    #[test]
    fn test_convert_to_parquet_delimiter() {
        let mut source_file = std::env::current_dir().unwrap();
        source_file.push("testdata");
        source_file.push("sample_delimiter.csv");

        let result = convert_to_parquet(&source_file, ';', true);

        // Check that the function completed successfully
        assert!(result.is_ok());

        // Verify the parquet file was created
        let parquet_file = PathBuf::from("testdata/sample_delimiter.parquet");
        assert!(parquet_file.exists());

        // Optionally, clean up the parquet file
        std::fs::remove_file(parquet_file).unwrap();
    }

    #[test]
    fn test_convert_to_parquet_no_header() {
        let mut source_file = std::env::current_dir().unwrap();
        source_file.push("testdata");
        source_file.push("sample_no_header.csv");

        let result = convert_to_parquet(&source_file, ',', false);

        // Check that the function completed successfully
        assert!(result.is_ok());

        // Verify the parquet file was created
        let parquet_file = PathBuf::from("testdata/sample_no_header.parquet");
        assert!(parquet_file.exists());

        // Optionally, clean up the parquet file
        std::fs::remove_file(parquet_file).unwrap();
    }

    #[test]
    fn test_remove_deduplicate_columns() {
        let schema = arrow_schema::Schema::new(vec![
            Field::new("name", arrow_schema::DataType::Utf8, false),
            Field::new("", arrow_schema::DataType::Utf8, false),
            Field::new("age", arrow_schema::DataType::Int32, false),
            Field::new("age", arrow_schema::DataType::Int64, false),
        ]);
        let deduplicated_schema = remove_deduplicate_columns(schema);
        dbg!(&deduplicated_schema.fields);
        assert_eq!(deduplicated_schema.fields().len(), 4);
    }

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

    #[test]
    fn test_find_files() {
        assert_eq!(find_files("testdata/sample.csv").len(), 1);
        assert_eq!(find_files("testdata/*.csv").len(), 4);
        assert_eq!(find_files("not-exist/*.csv").len(), 0);
        assert_eq!(find_files("testdata/*delimi*.csv").len(), 1);
    }
}
