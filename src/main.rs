extern crate core;

use std::fs::File;
use std::time::Instant;
use clap::{arg, Parser};
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use std::path::PathBuf;
use std::sync::Mutex;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Represents the folder path for CSV search.
    #[arg(short, long, default_value_t = String::from("."))]
    path: String,

    /// Number of worker threads to use for performing the task.
    #[arg(short, long, default_value_t = 4)]
    worker: u8,
}


// New struct for storing file path and error data
struct ErrorData {
    file_path: String,
    error: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let start = Instant::now();
    let path = PathBuf::from(args.path);
    println!("reading  path: {}, worker count: {}", path.display(), args.worker);
    let errors = Arc::new(Mutex::new(Vec::<ErrorData>::new()));

    let d = std::fs::read_dir(&path)?;
    let mut files = vec![];
    for path_result in d {
        let path = path_result?.path();

        if path.extension() == Some(std::ffi::OsStr::new("csv")) {
            files.push(path.file_name().unwrap().to_owned());
        }
    }
    let bar = ProgressBar::new(files.len().try_into().unwrap());
    bar.set_style(ProgressStyle::with_template("[{elapsed_precise}] {bar:40.yellow/blue} {pos:>7}/{len:7} {msg}")
        .unwrap());
    let pool = rayon::ThreadPoolBuilder::default().num_threads(args.worker as usize).build().unwrap();

    for file in files {
        let path_clone = path.clone();
        bar.inc(1);
        let errors_clone = Arc::clone(&errors);
        pool.install(move || {
            let r = process_file(&path_clone, file.to_str().unwrap());
            if let Err(err) = r {
                let mut errors = errors_clone.lock().unwrap();

                errors.push(ErrorData { file_path: file.to_str().unwrap().to_string(), error:err.to_string() });
            }
        });
    }

    bar.finish();
    let errors = errors.lock().unwrap();
    for err_data in &*errors{
        println!("File: {}  Error: {:?}", err_data.file_path, err_data.error);
    }

    let elapsed = start.elapsed();
    println!("elapsed time {} ms", elapsed.as_millis());

    Ok(())
}

fn process_file(base: &PathBuf, file_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let file_path = base.join(file_name);

    let file = File::open(&file_path)?;

    let mut df_posts = CsvReader::new(file).has_header(true).finish()?;

    let target_file = file_path.with_extension("parquet");
    let mut file = File::create(target_file).unwrap();

    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df_posts)
        .unwrap();

    Ok(())
}
