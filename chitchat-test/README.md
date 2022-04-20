# Chitchat test

This project runs a simple chitchat server
to test the chitchat crate.


## Example

```bash
# First server
cargo run -- -h localhost:10000
# Second server
cargo run -- -h localhost:10001 --seed localhost:10000
```
