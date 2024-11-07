# mini-redis

Example taken from [mini-redis](https://github.com/tokio-rs/mini-redis/blob/e186482ca00f8d884ddcbe20417f3654d03315a4/README.md?plain=1) rewritten for educational purpose.

## How to run

```bash
RUST_LOG=debug cargo run --package mini-redis --bin mini-redis-server

RUST_LOG=debug cargo run --package mini-redis --bin mini-redis-cli ping