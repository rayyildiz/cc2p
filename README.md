# Convert CSV To Parquet (CC2P)

[![Build](https://github.com/rayyildiz/cc2p/actions/workflows/build.yaml/badge.svg)](https://github.com/rayyildiz/cc2p/actions/workflows/build.yaml)
[![Publish](https://github.com/rayyildiz/cc2p/actions/workflows/publish.yaml/badge.svg)](https://github.com/rayyildiz/cc2p/actions/workflows/publish.yaml)

**(CC2P)** is a Rust-based project that converts CSV files in a selected folder into parquet format. This tool provides a simple and efficient way of handling and converting your CSV data files.


## Installation & Usage

### Prerequisites

- Rust 1.71.0

### Building

Provide instructions on how to build the project, for example, installing the Rust compiler and necessary crates.

Here is how to install the `cc2p` directly from the Git repository:

```shell
cargo install cc2p
```

### Running

Provide Instructions on how to run the scripts. For example, how to specify the input CSV file and the output Parquet file.

```shell
cc2p --path /path/to/csv/files  --worker 8
```

Other arguments:

- **path** : folder path for CSV search (default `.` - current folder)
- **delimiter** : delimiter char used in CSV files (default: `,`)
- **header** : whether to include the header in the CSV search column (default: `true`)
- **worker**: Number of worker threads to use for performing the task (default: `4`)

## Features

- Fast and reliable CSV to Parquet conversion.
- Multithreaded processing with the help of the [tokio](https://tokio.rs/) crate.
- Progress indication during conversion with the help of the [indicatif](https://docs.rs/indicatif) crate.

## Contributing

If you wish to contribute, please feel free to fork the repository, make your changes, and submit a pull request. All contributions are welcome!

## License

This project is licensed under MIT, see the [LICENSE](LICENSE) file for details.

## Contact

Project Link: https://github.com/rayyildiz/cc2p
