extern crate core;

use std::ffi::OsString;
use clap::{arg, Parser};
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;
use tokio::runtime;
use glob::glob;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Represents the folder path for CSV search.
    #[arg(short, long, default_value_t = String::from("*.csv"))]
    path: String,

    /// Represents the delimiter used in CSV files.
    #[arg(short, long, default_value_t = String::from(","))]
    delimiter: String,

    /// Number of worker threads to use for performing the task.
    #[arg(short, long, default_value_t = 4)]
    worker: u8,
}

// New struct for storing file path and error data
struct ErrorData {
    file_path: String,
    error: String,
}

struct FileInfo {
    dir: PathBuf,
    file_name: OsString,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let start = Instant::now();
    let path = args.path;
    let delimiter = args.delimiter.as_str().chars().next().unwrap_or(',');

    println!(
        "Program arguments\n path: {}\n delimiter: {}\n worker count: {}",
        path,
        delimiter,
        args.worker
    );
    let errors = Arc::new(Mutex::new(Vec::<ErrorData>::new()));

    let mut files = vec![];
    for entry in glob(path.as_str())? {
        match entry {
            Ok(path) => {
                if path.extension() == Some(std::ffi::OsStr::new("csv")) {
                    files.push(FileInfo {
                        dir: path.parent().unwrap().to_path_buf(),
                        file_name: path.file_name().unwrap().to_owned(),
                    });
                }
            }
            Err(e) => eprintln!("error file getting path, {:?}", e),
        }
    }

    let bar = ProgressBar::new(files.len().try_into().unwrap());

    bar.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.yellow/blue} {pos:>7}/{len:7} {msg}",
        )
            .unwrap(),
    );
    let bar = Arc::new(Mutex::new(bar));

    let runtime = runtime::Builder::new_multi_thread()
        .worker_threads(args.worker as usize)
        .enable_all()
        .build()?;

    runtime.block_on(async {
        let mut handles = vec![];

        for file in files {
            let path_clone = file.dir.clone();
            let bar = Arc::clone(&bar);
            let errors_clone = Arc::clone(&errors);
            let h = tokio::spawn(async move {
                if let Err(err) =
                    convert_to_parquet(path_clone, delimiter, file.file_name.to_str().unwrap())
                {
                    let mut errors = errors_clone.lock().unwrap();

                    errors.push(ErrorData {
                        file_path: file.file_name.to_str().unwrap().to_string(),
                        error: err.to_string(),
                    });
                }
                bar.lock().unwrap().inc(1);
            });

            handles.push(h);
        }

        for handle in handles {
            let _ = handle.await;
        }
    });

    bar.lock().unwrap().finish();

    let errors = errors.lock().unwrap();
    for err_data in &*errors {
        println!(
            "File: {}  Error: {:?}\n",
            err_data.file_path, err_data.error
        );
    }

    let elapsed = start.elapsed();
    println!("elapsed time {} ms", elapsed.as_millis());

    Ok(())
}

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
///     let base = PathBuf::from("/path/to/files");
///     let delimiter = ',';
///     let has_header = true;
///     let file_name = "data.csv";
///
///     convert_to_parquet(&base, delimiter, has_header, file_name)?;
///
///     Ok(())
/// }
/// ```
pub fn convert_to_parquet(
    base: PathBuf,
    delimiter: char,
    file_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_path = base.join(file_name);

    let file = File::open(&file_path)?;

    let mut df_posts = CsvReader::new(file)
        .has_header(true)
        .with_separator(delimiter as u8)
        .finish()?;

    let target_file = file_path.with_extension("parquet");
    let mut file = File::create(target_file).unwrap();

    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df_posts)
        .unwrap();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_to_parquet() {
        let mut base = std::env::current_dir().unwrap();
        base.push("testdata");

        let file_name = "sample.csv";

        let result = convert_to_parquet(base.clone(), ',',  file_name);

        // Check that the function completed successfully
        assert!(result.is_ok());

        // Verify the parquet file was created
        let parquet_file = base.join("sample.parquet");
        assert!(parquet_file.exists());

        // Optionally, clean up the parquet file
        std::fs::remove_file(parquet_file).unwrap();
    }
}
