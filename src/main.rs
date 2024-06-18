extern crate core;

use std::sync::{Arc, Mutex};
use std::time::Instant;

use clap::{arg, Parser};
use indicatif::{ProgressBar, ProgressStyle};
use tokio::runtime;

use cc2p::{convert_to_parquet, find_files};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Represents the folder path for CSV search.
    #[arg(default_value_t = String::from("*.csv"))]
    path: String,

    /// Represents the destination folder path for saving the files.
    #[arg(short, long, default_value_t = String::from("."))]
    dest: String,

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

/// The `ErrorData` struct represents data related to an error.
struct ErrorData {
    file_path: String,
    error: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let start = Instant::now();
    let path = args.path.as_str();
    let destination = args.dest;
    let sampling_size = args.sampling;
    let has_header = !args.no_header;
    let delimiter = args.delimiter.as_str().chars().next().unwrap_or(',');

    println!(
        "Program arguments\n path: {} destination path: {}\n delimiter: {}\n has header: {} \n worker count: {} \n sampling size {}",
        path,
        destination.as_str(),
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

    let destination = destination.clone().as_str();

    runtime.block_on(async move {
        let dest = destination.clone();
        let mut handles = vec![];

        for file in files {
            let bar = Arc::clone(&bar);
            let errors_clone = Arc::clone(&errors);
            let h = tokio::spawn(async move {
                if let Err(err) =
                    convert_to_parquet(&file, &dest, delimiter, has_header, sampling_size)
                {
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
