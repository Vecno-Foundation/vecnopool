# Vecnopool

Vecnopool is a high performance mining pool written in Rust. It uses postgresql to store and update balances for miners. Use [Vecnopool Payment](https://github.com/Vecno-Foundation/vecnopool-payment "Vecnopool Payment App") for paying out rewards using WASM.

## Installation

- Install [Rust](http://rustup.rs)
- Checkout repository and `cd` to the folder
- Run `cargo build --release`
- The binary will be in `target/release/ `

## Usage

1. Create a pool wallet using the latest [WASM](https://github.com/Vecno-Foundation/vecnod/releases "Vecno WASM") release. Follow the instructions and make sure to save the keys.
2. Create a postgresql database.
3. Create a .env file and copy content of the .env.example. Enter your own configs.

To start the mining pool, simply run

```commandline
cargo run
```

This will start a stratum server at `127.0.0.1:6969`.
