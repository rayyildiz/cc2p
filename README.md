# CSV To Parquet Converter (CC2P)

[![Build](https://github.com/rayyildiz/cc2p/actions/workflows/build.yaml/badge.svg)](https://github.com/rayyildiz/cc2p/actions/workflows/build.yaml)

**(CC2P)** is a Rust-based project that converts CSV files in a selected folder into parquet format. This tool provides a simple and efficient way of handling and converting your CSV data files.


## Installation & Usage

### Prerequisites

- Rust 1.76.0

### Building

Provide instructions on how to build the project for example installing the Rust compiler and necessary crates.

Here is how to install the `cc2p` directly from the Git repository:

```shell
cargo install --git https://github.com/rayyildiz/cc2p.git
```

### Running

Provide Instructions on how to run the scripts. For example, how to specify the input CSV file and the output Parquet file.

```shell
cc2p --path /path/to/csv/files  --worker 8
```

## Features

- Fast and reliable CSV to Parquet conversion.
- Multithreaded processing with the help of the Rayon crate.
- Progress indication during conversion with the help of the Indicatif crate.

## Contributing

If you wish to contribute, please feel free to fork the repository, make your changes, and submit a pull request. All contributions are welcome!

## License

This project is licensed under MIT, see the [LICENSE](LICENSE) file for details.

## Contact

Project Link: https://github.com/rayyildiz/cc2p
