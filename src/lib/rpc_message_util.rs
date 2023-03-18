pub mod rpc_message_capnp {
    include!("../rpc_message_capnp.rs"); // The generated file is under crate root
}

use capnp::serialize_packed;
use std::io::Cursor;

pub fn write_message() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut builder = ::capnp::message::Builder::new_default();
    let mut message = builder.init_root::<rpc_message_capnp::rpc_message::Builder>();

    message.set_type(rpc_message_capnp::rpc_message::Type::Request);
    message.set_id(1234);
    message.set_request_ctx(5678);
    message.set_payload(b"hello, world");

    let mut serialized_message = Vec::new();
    serialize_packed::write_message(&mut serialized_message, &builder)?;

    Ok(serialized_message)
}

pub fn read_message(serialized_message: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    // Cursor implements the Read trait,  allow you read data from a buffer as if from a file or other input stream.
    let mut cursor = Cursor::new(serialized_message);
    let message_reader = serialize_packed::read_message(&mut cursor, Default::default())?;

    let rpc_message_reader = message_reader.get_root::<rpc_message_capnp::rpc_message::Reader>()?;

    println!("Type: {:?}", rpc_message_reader.get_type()?);
    println!("ID: {}", rpc_message_reader.get_id());
    println!("RequestCtx: {}", rpc_message_reader.get_request_ctx());
    println!("Payload: {:?}", rpc_message_reader.get_payload()?);

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Write a message
    let serialized_message = write_message()?;
    println!("Serialized message: {:?}", serialized_message);

    // Read the message back
    read_message(&serialized_message)?;

    Ok(())
}
