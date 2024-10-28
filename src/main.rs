extern crate core;

use std::sync::{Arc, Mutex};
use std::time::Instant;

use clap::{arg, Parser};
use indicatif::{ProgressBar, ProgressStyle};
use tokio::runtime;

use cc2p::{convert_to_parquet, find_files};

/// A command line parser for processing CSV files with specified parameters.
///
/// This struct represents the possible command line arguments that can be supplied
/// when executing the program. Each field in the struct corresponds to an argument
/// and can be used to set various options such as the folder path for searching CSV files,
/// the delimiter used within the CSV files, and the number of worker threads to use.
///
/// # Arguments
///
/// * `path` - Represents the folder path for CSV search. Default value is "*.csv".
/// * `delimiter` - Represents the delimiter used in CSV files. Default value is ",".
/// * `no_header` - Represents whether to include the header in the CSV search column. Default value is `false`.
/// * `worker` - Number of worker threads to use for performing the task. Default value is 1.
/// * `sampling` - Number of rows to sample for inferring the schema. Default value is 100.
///
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Represents the folder path for CSV search.
    #[arg(default_value_t = String::from("*.csv"))]
    path: String,

    /// Represents the delimiter used in CSV files.
    #[arg(short, long, default_value_t = String::from(","))]
    delimiter: String,

    /// Represents whether to include the header in the CSV search column.
    #[arg(short, long, default_value_t = false)]
    no_header: bool,

    /// Number of worker threads to use for performing the task.
    #[arg(short, long, default_value_t = 1)]
    worker: u8,

    /// Number of rows to sample for inferring the schema.
    #[arg(short, long, default_value_t = 100)]
    sampling: u16,
}

/// A structure to hold error information related to CSV file processing.
///
/// This struct is designed to capture and store error details that occur during
/// the processing of CSV files within the application. It encapsulates the file path
/// where the error occurred and the corresponding error message.
///
/// # Fields
///
/// * `file_path` - The path of the CSV file where the error was encountered.
/// * `error` - A description of the error that occurred in the given file.
///
/// # Example
///
/// ```
/// let error_data = ErrorData {
///     file_path: String::from("data.csv"),
///     error: String::from("Failed to open the file."),
/// };
/// println!("Error in file {}: {}", error_data.file_path, error_data.error);
/// ```
struct ErrorData {
    /// The path of the CSV file where the error was encountered.
    file_path: String,

    /// A description of the error that occurred in the given file.
    error: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let start = Instant::now();
    let path = args.path.as_str();
    let sampling_size = args.sampling;
    let has_header = !args.no_header;
    let delimiter = args.delimiter.as_str().chars().next().unwrap_or(',');

    println!(
        "Program arguments\n path: {}\n delimiter: {}\n has header: {} \n worker count: {} \n sampling size {}",
        path,
        delimiter,
        has_header,
        args.worker,
        sampling_size
    );
    let errors = Arc::new(Mutex::new(Vec::<ErrorData>::new()));

    let files = find_files(path);

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
            let bar = Arc::clone(&bar);
            let errors_clone = Arc::clone(&errors);
            let h = tokio::spawn(async move {
                if let Err(err) = convert_to_parquet(&file, delimiter, has_header, sampling_size) {
                    let mut errors = errors_clone.lock().unwrap();

                    errors.push(ErrorData {
                        file_path: file.to_str().unwrap().to_string(),
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
    println!("Elapsed time {} ms", elapsed.as_millis());

    Ok(())
}
