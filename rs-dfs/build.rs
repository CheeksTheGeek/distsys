use tonic_build;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/datanode.proto")?;
    tonic_build::compile_protos("proto/namenode.proto")?;
    Ok(())
}