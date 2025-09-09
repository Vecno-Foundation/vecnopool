use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = [
        "proto/messages.proto",
        "proto/p2p.proto",
        "proto/rpc.proto",
    ];
    let proto_dir = Path::new("proto");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute("protowire.RpcBlockHeader", "#[derive(serde::Serialize)]")
        .type_attribute("protowire.RpcBlockLevelParents", "#[derive(serde::Serialize)]")
        .compile_protos(&proto_files, &[proto_dir])?;

    Ok(())
}