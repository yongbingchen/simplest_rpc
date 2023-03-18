# simplest_rpc
A simplest Rust RPC library

Run an UDP server at localhost: 
```sh
~$ ncat -l 8081 --keep-open --udp --exec "/bin/cat"
```
then run "cargo run" to see the result.

This version is to test async communication path only, not real RPC yet.
