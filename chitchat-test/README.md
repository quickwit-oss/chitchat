# Chitchat test

This project runs a simple chitchat server
to test the chitchat crate.


## Example

```bash
# First server
cargo run -- --listen_addr 127.0.0.1:10000
# Second server
cargo run -- --listen_addr 127.0.0.1:10001 --seed 127.0.0.1:10000
```

## Startup Flags that are optional 

```bash 
--interval_ms <interval>
--node_id <node-id>
--public_addr <public-addr>
```
