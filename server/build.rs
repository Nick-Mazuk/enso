use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(
        &[
            "../proto/google/rpc/code.proto",
            "../proto/google/rpc/status.proto",
            "../proto/protocol.proto",
        ],
        &["../proto/"],
    )?;
    Ok(())
}
