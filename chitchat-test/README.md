# Chitchat test

This project runs a simple chitchat server
to test the chitchat crate.


## Example

```bash
# First server
cargo run -- --listen_addr localhost:10000
# Second server
cargo run -- --listen_addr localhost:10001 --seed localhost:10000
```

## Startup Flags that are optional 

```bash 
--interval_ms <interval>
--node_id <node-id>
--public_addr <public-addr>
```
