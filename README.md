# mini-iguana

## Install Rust
See https://rustup.rs/

## Install
```
cargo install --git https://github.com/dice-group/mini-iguana
```

## Run (Example)
```shell
target/release/mini-iguana http://localhost:9080/sparql queries.txt warmup
target/release/mini-iguana http://localhost:9080/update updates.txt update > results.csv
```
