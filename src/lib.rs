use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;


/// The maximum number of records that can be read to determine schema.
const READ_MAX_RECORDS: usize = 10;

/// Process a CSV file and convert it to a Parquet file.
///
/// # Arguments
///
/// * `base` - The base directory where the file resides.
/// * `delimiter` - The delimiter character used in the CSV file.
/// * `has_header` - Whether the CSV file has a header row or not.
/// * `file_name` - The name of the CSV file to process.
///
/// # Errors
///
/// This function returns an `Err` value if it encounters any errors during processing.
/// The error type is a boxed trait object that implements the `std::error::Error` trait.
///
/// # Example
///
/// ```rust
/// use std::path::PathBuf;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     use cc2p::convert_to_parquet;
/// let base = PathBuf::from("testdata");
///     let delimiter = ',';
///     let has_header = true;
///     let file_name = "sample2.csv";
///
///     convert_to_parquet(&base, delimiter, has_header, file_name)?;
///
///     Ok(())
/// }
/// ```
pub fn convert_to_parquet(
    base: &Path,
    delimiter: char,
    has_header: bool,
    file_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_path = base.join(file_name);

    let file = File::open(&file_path)?;

    let (csv_schema, _) = arrow_csv::reader::Format::default()
        .with_header(has_header)
        .with_delimiter(delimiter as u8)
        .infer_schema(file, Some(READ_MAX_RECORDS))?;

    let schema_ref = remove_deduplicate_columns(csv_schema);// Arc::new(csv_schema);

    let file = File::open(&file_path)?;

    let mut csv = arrow_csv::ReaderBuilder::new(schema_ref.clone())
        .with_delimiter(delimiter as u8)
        .with_header(has_header)
        .build(file)?;

    let target_file = file_path.with_extension("parquet");
    let mut file = File::create(target_file).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by("cc2p".to_string())
        .build();

    let mut parquet_writer = parquet::arrow::ArrowWriter::try_new(&mut file, schema_ref, Some(props))?;


    for batch in csv.by_ref() {
        match batch {
            Ok(batch) => parquet_writer.write(&batch)?,
            Err(error) => eprintln!("error : {:?}", error)
        }
    }

    parquet_writer.close()?;

    Ok(())
}


fn remove_deduplicate_columns(sc: arrow_schema::Schema) -> Arc<arrow_schema::Schema> {
    let mut index = 1;
    let mut deduplicated_fields = Vec::new();
    let mut names = HashMap::new();
    for field in sc.fields() {
        let field_name = field.name().as_str();
        let field_name = clean_name(field_name);

        if names.contains_key(&field_name) {
            let name = format!("{} {}", field_name, index);
            index += 1;
            let new_field = <arrow_schema::Field as Clone>::clone(&(*field).clone()).with_name(name);
            deduplicated_fields.push(Arc::new(new_field));
        } else {
            names.insert(field_name, 1u8);

            if field.name().len() == 0 {
                let name = format!("unknown {}", index);
                index += 1;
                let new_field = <arrow_schema::Field as Clone>::clone(&(*field).clone()).with_name(name);
                deduplicated_fields.push(Arc::new(new_field));
            } else {
                deduplicated_fields.push(field.clone());
            }
        }
    }

    let list_fields: Vec<_> = deduplicated_fields.into_iter().collect();

    let deduplicated_schema = arrow_schema::Schema::new_with_metadata(list_fields, sc.metadata.clone());

    Arc::new(deduplicated_schema)
}

/// Cleans a given string by removing any characters that are not alphanumeric or whitespace.
///
/// # Arguments
///
/// * `s` - The string to be cleaned.
///
/// # Examples
///
/// ```rust
/// use cc2p::*;
///
/// let name = clean_name("John!Doe");
/// assert_eq!(name, "JohnDoe");
///
/// let name = clean_name("Welcome, User 123!");
/// assert_eq!(name, "Welcome User 123");
///
/// ```
///
/// # Returns
///
/// A `String` containing the cleaned string, with all non-alphanumeric characters removed.
pub fn clean_name(s: &str) -> String {
    let cleaned = regex::Regex::new(r"[^a-zA-Z0-9\s]").unwrap().replace_all(s, "");

    cleaned.to_string()
}

#[cfg(test)]
mod tests {
    use arrow_schema::{Field};
    use super::*;

    #[test]
    fn test_convert_to_parquet() {
        let mut base = std::env::current_dir().unwrap();
        base.push("testdata");

        let file_name = "sample.csv";

        let result = convert_to_parquet(&base, ',', true, file_name);

        // Check that the function completed successfully
        assert!(result.is_ok());

        // Verify the parquet file was created
        let parquet_file = base.join("sample.parquet");
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
    fn test_clean_names() {
        assert_eq!(clean_name("abc"), "abc");
        assert_eq!(clean_name("ab c"), "ab c");
        assert_eq!(clean_name("ab.c"), "abc");
        assert_eq!(clean_name("ab-_c"), "abc");
        assert_eq!(clean_name("Abc"), "Abc");
        assert_eq!(clean_name("a8A"), "a8A");
        assert_eq!(clean_name("a@bc"), "abc");
        assert_eq!(clean_name("abc#"), "abc");
        assert_eq!(clean_name("ab}}[}c"), "abc");
    }
}
