# Convert CSV To Parquet (CC2P)

[![Build](https://github.com/rayyildiz/cc2p/actions/workflows/build.yaml/badge.svg)](https://github.com/rayyildiz/cc2p/actions/workflows/build.yaml)
[![Publish](https://github.com/rayyildiz/cc2p/actions/workflows/publish.yaml/badge.svg)](https://github.com/rayyildiz/cc2p/actions/workflows/publish.yaml)
[![cc2p](https://snapcraft.io/cc2p/badge.svg)](https://snapcraft.io/cc2p)

**CC2P** (Convert CSV To Parquet) is a high-performance command-line tool written in Rust that efficiently converts CSV files to the Apache Parquet format. Parquet is a columnar storage file format that offers efficient data compression and encoding schemes, making it ideal for big data processing.

## Why Use CC2P?

- **Performance**: Leverages Rust's speed and multi-threading for fast conversions
- **Memory Efficiency**: Processes files with minimal memory footprint
- **Flexibility**: Supports various CSV formats with different delimiters and header options
- **Schema Inference**: Automatically detects column types from your data
- **Batch Processing**: Convert multiple CSV files in a single command

## Installation

### From Cargo (Recommended)

If you have Rust installed, you can install CC2P directly from crates.io:

```shell
cargo install cc2p
```

### From GitHub Releases

You can download pre-built binaries from the [GitHub Releases page](https://github.com/rayyildiz/cc2p/releases).

### From Source

To build from source:

```shell
# Clone the repository
git clone https://github.com/rayyildiz/cc2p.git
cd cc2p

# Build in release mode
cargo build --release

# The binary will be in target/release/cc2p
```

## Usage

Basic usage:

```shell
cc2p [OPTIONS] [PATH]
```

Where `PATH` is the path to a CSV file or a glob pattern (default: `*.csv`).

### Examples

Convert a single CSV file:
```shell
cc2p data.csv
```

Convert all CSV files in the current directory:
```shell
cc2p
```

Convert CSV files with semicolon delimiter:
```shell
cc2p --delimiter ";" *.csv
```

Convert CSV files without headers:
```shell
cc2p --no-header data_files/*.csv
```

Use 4 worker threads for faster processing:
```shell
cc2p --worker 4 large_data.csv
```

### Options

- **-d, --delimiter** : Delimiter character used in CSV files (default: `,`)
- **-n, --no-header**: Whether to include the header in the CSV search column (default: `false`)
- **-w, --worker**: Number of worker threads to use for performing the task (default: `1`)
- **-s, --sampling**: Number of rows to sample for inferring the schema (default: `2048`)

```shell
$ cc2p --help

Convert a CSV to parquet file format

Usage: cc2p [OPTIONS] [PATH]

Arguments:
  [PATH]  Represents the folder path for CSV search [default: *.csv]

Options:
  -d, --delimiter <DELIMITER>  Represents the delimiter used in CSV files [default: ,]
  -n, --no-header              Represents whether to include the header in the CSV search column
  -w, --worker <WORKER>        Number of worker threads to use for performing the task [default: 1]
  -s, --sampling <SAMPLING>    Number of rows to sample for inferring the schema. [default: 100]
  -h, --help                   Print help
  -V, --version                Print version
```

## Features

### Technical Features

- **Columnar Storage**: Parquet's columnar format provides better compression and faster query performance compared to row-based formats like CSV
- **Efficient Compression**: Uses Snappy compression for a good balance between compression ratio and speed
- **Schema Handling**: Automatically infers data types and handles duplicate column names
- **Parallel Processing**: Multi-threaded conversion using [Tokio](https://tokio.rs/) runtime
- **Progress Tracking**: Real-time progress indication with [indicatif](https://docs.rs/indicatif) progress bars
- **Error Handling**: Robust error handling with detailed error messages

### Performance Benefits

- **Reduced Storage**: Parquet files are typically much smaller than equivalent CSV files
- **Faster Analytics**: A columnar format allows for more efficient querying in data analysis tools
- **Schema Enforcement**: Parquet maintains schema information, unlike CSV which is schema-less
- **Selective Column Reading**: Analytics tools can read only the columns they need, improving performance

## Platform-Specific Notes

### macOS Users

**NOTE for macOS Users:** Our Apple signing/notarization is not entirely done yet,
thus you have to run the following command once to run the application. [Download the app](https://github.com/rayyildiz/cc2p/releases) and run this command:

```shell
xattr -c cc2p
```

### Linux Users

On Linux, you can also install CC2P via Snap:

[![Get it from the Snap Store](https://snapcraft.io/static/images/badges/en/snap-store-black.svg)](https://snapcraft.io/cc2p)

```shell
sudo snap install cc2p
```

## Technical Requirements

- **Rust Version**: 1.85.0 or later
- **Rust Edition**: 2024
- **Minimum Memory**: Depends on the size of CSV files being processed

## Contributing

If you wish to contribute, please feel free to fork the repository, make your changes, and submit a pull request. All contributions are welcome!

### Development Setup

1. Clone the repository
2. Install Rust (1.85.0 or later)
3. Run `cargo build` to build the project
4. Run `cargo test` to run the tests

## License

This project is licensed under MIT, see the [LICENSE](LICENSE) file for details.

## Contact

- Project Link: https://github.com/rayyildiz/cc2p
- Report Issues: https://github.com/rayyildiz/cc2p/issues
