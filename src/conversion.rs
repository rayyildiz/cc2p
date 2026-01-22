use crate::error::{Cc2pError, Result};
use crate::utils::{clean_column_name, delete_if_exist};
use arrow_schema::Schema;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

struct Empty {}

/// Removes duplicate columns from a given Arrow schema and returns a new schema with deduplicated columns.
///
/// # Arguments
///
/// * `sc` - The input Arrow schema.
///
/// # Returns
///
/// Returns an `Arc` containing the deduplicated schema.
pub fn remove_deduplicate_columns(sc: Schema) -> Arc<Schema> {
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
    let deduplicated_schema = Schema::new_with_metadata(list_fields, sc.metadata);

    Arc::new(deduplicated_schema)
}

/// Infers the schema of a CSV file.
///
/// # Arguments
///
/// * `file_path` - The path of the CSV file.
/// * `delimiter` - The delimiter character used in the CSV file.
/// * `has_header` - Indicates whether the CSV file has a header row.
/// * `sampling_size` - The number of rows to sample for inferring the schema.
///
/// # Returns
///
/// Returns the inferred schema if successful, otherwise returns an error.
pub fn infer_schema(file_path: &Path, delimiter: char, has_header: bool, sampling_size: u16) -> Result<Schema> {
    let file = std::fs::File::open(file_path).map_err(Cc2pError::FileError)?;
    let (csv_schema, _) = arrow_csv::reader::Format::default()
        .with_header(has_header)
        .with_delimiter(delimiter as u8)
        .infer_schema(file, Some(sampling_size as usize))
        .map_err(|e| Cc2pError::SchemaError(e.to_string()))?;

    Ok(csv_schema)
}

/// Converts a CSV file to Parquet format asynchronously.
///
/// # Arguments
///
/// * `file_path` - The path of the CSV file to be converted.
/// * `delimiter` - The delimiter character used in the CSV file.
/// * `has_header` - Indicates whether the CSV file has a header row.
/// * `sampling_size` - The number of rows to sample for inferring the schema.
///
/// # Returns
///
/// Returns `Ok(())` if the conversion is successful, otherwise returns an error.
///
/// # Example
///
/// ```
/// use std::path::PathBuf;
/// use cc2p::conversion::convert_to_parquet;
///
/// #[tokio::main]
/// async fn main() -> cc2p::error::Result<()> {
///     let file_path = PathBuf::from("testdata/sample.csv");
///     let delimiter = ',';
///     let has_header = true;
///
///     convert_to_parquet(&file_path, delimiter, has_header, 10).await?;
///
///     Ok(())
/// }
/// ```
pub async fn convert_to_parquet(file_path: &Path, delimiter: char, has_header: bool, sampling_size: u16) -> Result<()> {
    // Compute the target path and delete if exists using async FS to avoid blocking
    let target_file = file_path.with_extension("parquet");
    let target_path = target_file
        .to_str()
        .ok_or_else(|| Cc2pError::Other("Failed to convert a path to string".to_string()))?;

    // Delete a target file if it exists (async I/O)
    delete_if_exist(target_path).await?;

    // Offload blocking Arrow/Parquet work to a dedicated blocking thread
    let file_path = file_path.to_path_buf();
    let delimiter_u8 = delimiter as u8;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let csv_schema = infer_schema(&file_path, delimiter, has_header, sampling_size)?;
        let schema_ref = remove_deduplicate_columns(csv_schema);

        // Reopen the file for reading the actual data
        let file = std::fs::File::open(&file_path).map_err(Cc2pError::FileError)?;

        let mut csv = arrow_csv::ReaderBuilder::new(schema_ref.clone())
            .with_delimiter(delimiter_u8)
            .with_header(has_header)
            .build(file)
            .map_err(|e| Cc2pError::CsvError(e.to_string()))?;

        // Create the target file
        let file = std::fs::File::create(&target_file).map_err(Cc2pError::FileError)?;

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_created_by("cc2p".to_string())
            .build();

        let mut parquet_writer =
            parquet::arrow::ArrowWriter::try_new(file, schema_ref, Some(props)).map_err(|e| Cc2pError::ParquetError(e.to_string()))?;

        // Process batches
        for batch in csv.by_ref() {
            match batch {
                Ok(batch) => parquet_writer.write(&batch).map_err(|e| Cc2pError::ParquetError(e.to_string()))?,
                Err(e) => return Err(Cc2pError::CsvError(e.to_string())),
            }
        }

        // Close the writer
        parquet_writer.close().map_err(|e| Cc2pError::ParquetError(e.to_string()))?;

        Ok(())
    })
    .await
    .map_err(|e| Cc2pError::Other(format!("Blocking task join error: {}", e)))??;

    Ok(())
}

/// Converts a CSV file to Parquet format asynchronously with selected columns.
///
/// # Arguments
///
/// * `file_path` - The path of the CSV file to be converted.
/// * `delimiter` - The delimiter character used in the CSV file.
/// * `has_header` - Indicates whether the CSV file has a header row.
/// * `sampling_size` - The number of rows to sample for inferring the schema.
/// * `selected_columns` - The names of the columns to be included in the Parquet file.
///
/// # Returns
///
/// Returns `Ok(())` if the conversion is successful, otherwise returns an error.
pub async fn convert_to_parquet_with_columns(
    file_path: &Path,
    delimiter: char,
    has_header: bool,
    sampling_size: u16,
    selected_columns: Vec<String>,
) -> Result<()> {
    // Compute the target path and delete if exists using async FS to avoid blocking
    let target_file = file_path.with_extension("parquet");
    let target_path = target_file
        .to_str()
        .ok_or_else(|| Cc2pError::Other("Failed to convert a path to string".to_string()))?;

    // Delete a target file if it exists (async I/O)
    delete_if_exist(target_path).await?;

    // Offload blocking Arrow/Parquet work to a dedicated blocking thread
    let file_path = file_path.to_path_buf();
    let delimiter_u8 = delimiter as u8;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let csv_schema = infer_schema(&file_path, delimiter, has_header, sampling_size)?;
        let full_schema = remove_deduplicate_columns(csv_schema);

        let mut projection_indices = Vec::new();
        let mut projected_fields = Vec::new();

        for (i, field) in full_schema.fields().iter().enumerate() {
            if selected_columns.contains(field.name()) {
                projection_indices.push(i);
                projected_fields.push(field.clone());
            }
        }

        if projection_indices.is_empty() {
            return Err(Cc2pError::Other("No columns selected for export".to_string()));
        }

        let projected_schema = Arc::new(Schema::new_with_metadata(projected_fields, full_schema.metadata().clone()));

        // Reopen the file for reading the actual data
        let file = std::fs::File::open(&file_path).map_err(Cc2pError::FileError)?;

        let mut csv = arrow_csv::ReaderBuilder::new(full_schema)
            .with_delimiter(delimiter_u8)
            .with_header(has_header)
            .with_projection(projection_indices)
            .build(file)
            .map_err(|e| Cc2pError::CsvError(e.to_string()))?;

        // Create the target file
        let file = std::fs::File::create(&target_file).map_err(Cc2pError::FileError)?;

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_created_by("cc2p".to_string())
            .build();

        let mut parquet_writer = parquet::arrow::ArrowWriter::try_new(file, projected_schema, Some(props))
            .map_err(|e| Cc2pError::ParquetError(e.to_string()))?;

        // Process batches
        for batch in csv.by_ref() {
            match batch {
                Ok(batch) => parquet_writer.write(&batch).map_err(|e| Cc2pError::ParquetError(e.to_string()))?,
                Err(e) => return Err(Cc2pError::CsvError(e.to_string())),
            }
        }

        // Close the writer
        parquet_writer.close().map_err(|e| Cc2pError::ParquetError(e.to_string()))?;

        Ok(())
    })
    .await
    .map_err(|e| Cc2pError::Other(format!("Blocking task join error: {}", e)))??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use parquet::file::reader::FileReader;
    use std::fs;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_convert_to_parquet() {
        let mut source_file = std::env::current_dir().unwrap();
        source_file.push("testdata");
        source_file.push("sample_empty_header.csv");

        let result = convert_to_parquet(&source_file, ',', true, 10).await;

        // Check that the function completed successfully
        assert!(result.is_ok());

        let parquet_file = PathBuf::from("testdata/sample_empty_header.parquet");
        // Verify the parquet file was created
        assert!(parquet_file.exists());

        // Clean up the parquet file
        fs::remove_file(parquet_file).unwrap();
    }

    #[tokio::test]
    async fn test_convert_to_parquet_delimiter() {
        let mut source_file = std::env::current_dir().unwrap();
        source_file.push("testdata");
        source_file.push("sample_delimiter.csv");

        let result = convert_to_parquet(&source_file, ';', true, 10).await;

        // Check that the function completed successfully
        assert!(result.is_ok());

        // Verify the parquet file was created
        let parquet_file = PathBuf::from("testdata/sample_delimiter.parquet");
        assert!(parquet_file.exists());

        // Clean up the parquet file
        fs::remove_file(parquet_file).unwrap();
    }

    #[tokio::test]
    async fn test_convert_to_parquet_no_header() {
        let mut source_file = std::env::current_dir().unwrap();
        source_file.push("testdata");
        source_file.push("sample_no_header.csv");

        let result = convert_to_parquet(&source_file, ',', false, 10).await;

        // Check that the function completed successfully
        assert!(result.is_ok());

        // Verify the parquet file was created
        let parquet_file = PathBuf::from("testdata/sample_no_header.parquet");
        assert!(parquet_file.exists());

        // Clean up the parquet file
        fs::remove_file(parquet_file).unwrap();
    }

    #[test]
    fn test_remove_deduplicate_columns() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("age", DataType::Int64, false),
        ]);
        let deduplicated_schema = remove_deduplicate_columns(schema);

        assert_eq!(deduplicated_schema.fields().len(), 4);
        assert_eq!(deduplicated_schema.fields.first().unwrap().name(), "name");
        assert_eq!(deduplicated_schema.fields.get(1).unwrap().name(), "column_1");
        assert_eq!(deduplicated_schema.fields.get(2).unwrap().name(), "age");
        assert_eq!(deduplicated_schema.fields.get(3).unwrap().name(), "age_2");
    }

    #[test]
    fn test_remove_deduplicate_columns_empty_schema() {
        // Test with empty schema
        let empty_schema = Schema::new(Vec::<arrow_schema::Field>::new());
        let deduplicated_schema = remove_deduplicate_columns(empty_schema);
        assert_eq!(deduplicated_schema.fields().len(), 0);
    }

    #[test]
    fn test_remove_deduplicate_columns_all_duplicates() {
        // Test with schema where all fields have the same name
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("name", DataType::Int32, false),
        ]);
        let deduplicated_schema = remove_deduplicate_columns(schema);
        assert_eq!(deduplicated_schema.fields().len(), 3);
        assert_eq!(deduplicated_schema.fields.first().unwrap().name(), "name");
        assert_eq!(deduplicated_schema.fields.get(1).unwrap().name(), "name_1");
        assert_eq!(deduplicated_schema.fields.get(2).unwrap().name(), "name_2");
    }

    #[tokio::test]
    async fn test_convert_to_parquet_error_handling() {
        // Test with non-existent file
        let non_existent_file = PathBuf::from("testdata/non_existent.csv");
        let result = convert_to_parquet(&non_existent_file, ',', true, 10).await;
        assert!(result.is_err());

        if let Err(e) = result {
            match e {
                Cc2pError::FileError(_) => {} // Expected error type
                _ => panic!("Unexpected error type: {:?}", e),
            }
        }
    }

    #[tokio::test]
    async fn test_convert_to_parquet_with_metadata() {
        let mut source_file = std::env::current_dir().unwrap();
        source_file.push("testdata");
        source_file.push("sample.csv");

        // Test with different sampling size
        let result = convert_to_parquet(&source_file, ',', true, 5).await;
        assert!(result.is_ok());

        // Verify the parquet file was created
        let parquet_file = PathBuf::from("testdata/sample.parquet");
        assert!(parquet_file.exists());

        // Clean up
        fs::remove_file(parquet_file).unwrap();
    }

    #[tokio::test]
    async fn test_convert_to_parquet_with_columns() {
        let mut source_file = std::env::current_dir().unwrap();
        source_file.push("testdata");
        source_file.push("sample.csv");

        // Copy sample.csv to a temp file to avoid race conditions and test interference
        let mut temp_csv = std::env::temp_dir();
        temp_csv.push("temp_sample_for_test.csv");
        fs::copy(&source_file, &temp_csv).unwrap();

        // Assuming sample.csv has columns: "name", "age", "job"
        let schema = infer_schema(&temp_csv, ',', true, 10).unwrap();
        let full_schema = remove_deduplicate_columns(schema);
        let all_columns: Vec<String> = full_schema.fields().iter().map(|f| f.name().clone()).collect();

        if all_columns.is_empty() {
            fs::remove_file(temp_csv).unwrap();
            return;
        }

        let selected_columns = vec![all_columns[0].clone()];

        let result = convert_to_parquet_with_columns(&temp_csv, ',', true, 10, selected_columns.clone()).await;

        assert!(result.is_ok());

        let parquet_file = temp_csv.with_extension("parquet");
        assert!(parquet_file.exists());

        // Verify that the parquet file only has the selected column
        let file = std::fs::File::open(&parquet_file).unwrap();
        let reader = parquet::file::reader::SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        let schema_desc = metadata.file_metadata().schema_descr();

        // schema_desc has a root node + columns
        assert_eq!(schema_desc.num_columns(), selected_columns.len());
        assert_eq!(schema_desc.column(0).name(), &selected_columns[0]);

        // Clean up
        let _ = fs::remove_file(temp_csv);
        let _ = fs::remove_file(parquet_file);
    }
}
