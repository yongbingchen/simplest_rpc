# simplest_rpc
A simplest Rust RPC library

Run an UDP server at localhost: 
```sh
~$ ncat -l 8081 --keep-open --udp --exec "/bin/cat"
```
then run "cargo run" to see the result.

This version is to test async communication path only, not real RPC yet.


Install [Cap'np compiler](https://capnproto.org/install.html) before build this version
```sh
curl -O https://capnproto.org/capnproto-c++-0.10.3.tar.gz
tar zxf capnproto-c++-0.10.3.tar.gz
cd capnproto-c++-0.10.3
./configure
make -j6 check
sudo make install
```
