# mini-iguana

## Install Rust
See https://rustup.rs/

## Build
```
cargo build --release
```

## Run (Example)
```shell
target/release/mini-iguana http://localhost:9080/sparql queries.txt warmup
target/release/mini-iguana http://localhost:9080/update updates.txt update > results.csv
```
