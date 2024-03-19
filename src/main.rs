use std::fs::File;
use std::time::Instant;
use clap::{arg, Parser};
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use std::path::PathBuf;

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


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let start = Instant::now();
    let path = PathBuf::from(args.path);
    println!("reading  path: {}, worker count: {}", path.display(), args.worker);


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

        pool.install(move || {
            let r = process_file(&path_clone, file.to_str().unwrap());
            if r.is_err() {
                eprintln!("error occurred {:?}", r.err());
            }
        });
    }

    bar.finish();

    let elapsed = start.elapsed();
    println!("elapsed time {} ms", elapsed.as_millis());

    Ok(())
}


/// Process a file and convert it from CSV format to Parquet format.
///
/// # Arguments
///
/// * `base` - The base directory where the file is located.
/// * `file_name` - The name of the file to process.
///
/// # Errors
///
/// Returns an error if any of the following conditions are not met:
///
/// * The file could not be opened.
/// * The CSV reader encountered an error.
/// * The Parquet writer encountered an error.
///
/// # Example
///
/// ```rust
/// use std::path::PathBuf;
/// use std::fs::File;
/// use csv::ReaderBuilder;
/// use parquet::file::properties::WriterProperties;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// fn process_file(base: &PathBuf, file_name: &str) -> Result<(), Box<dyn std::error::Error>> {
///     let file_path = base.join(file_name);
///
///     let file = File::open(&file_path)?;
///
///     let mut reader = ReaderBuilder::new().from_reader(file);
///
///     let mut writer = WriterProperties::builder()
///         .build(file_path)?;
///
///     for record in reader.records() {
///         let record = record?;
///         writer.write_record(&record)?;
///     }
///
///     writer.close()?;
///
///     Ok(())
/// }
/// ```
fn process_file(base: &PathBuf, file_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let file_path = base.join(file_name);

    let file = File::open(&file_path)?;

    let mut df_posts = CsvReader::new(file).has_header(true).finish()?;

    let target_file = file_path.with_extension("parquet");
    let mut file = File::create(target_file).unwrap();

    ParquetWriter::new(&mut file).finish(&mut df_posts).unwrap();

    Ok(())
}