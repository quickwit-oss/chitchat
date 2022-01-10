# Scuttlebutt test

This project runs a simple scuttlebut server
to test the scuttlebutt crate.


## Example

```bash
# First server
cargo run -- -h localhost:10000
# Second server
cargo run -- -h localhost:10001 --seed localhost:10000
```
