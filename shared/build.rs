use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("../proto");
    let event_proto = proto_root.join("event.proto");

    prost_build::Config::new()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(&[event_proto], &[proto_root])?;

    println!("cargo:rerun-if-changed=../proto/event.proto");
    println!("cargo:rerun-if-changed=../proto/enforcer_extractor.proto");
    Ok(())
}
