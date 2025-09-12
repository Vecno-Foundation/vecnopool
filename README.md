# Vecno-Stratum-Pool

## Installation

- Install [Rust](http://rustup.rs)
- Checkout repository and `cd` to the folder
- Run `cargo build --release`
- The binary will be in `target/release/ `

**Install WebAssembly Dependencies**:

- Navigate to the `wasm` directory:
- Install dependencies listed in `package.json`:

```markdown
npm install
```

Run the wasm by:

```markdown
node run_wasm.js
```

## Usage

Configure create a .env file and copy content of the .env.example. Enter your own configs.

To start the mining pool, simply run

```commandline
cargo run
```

This will start a stratum server at `127.0.0.1:6969`.
