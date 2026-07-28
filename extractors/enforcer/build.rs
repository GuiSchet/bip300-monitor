use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("../../proto/upstream");
    let validator_proto = proto_root.join("cusf/mainchain/v1/validator.proto");

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&[validator_proto], &[proto_root])?;

    println!("cargo:rerun-if-changed=../../proto/upstream");
    Ok(())
}
