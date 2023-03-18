fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("src/lib") // This passed to "--src-prefix=<prefix>" of capnpc
        .file("src/lib/rpc_message.capnp")
        .output_path("") // This ask the compiler to put the generated file under crate root
        .run()
        .expect("Failed to run code generator");
}
