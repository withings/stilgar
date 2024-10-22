/// Build script for Clickhouse: generates code from protobuf definitions
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(
            &["proto/clickhouse/clickhouse.proto"],
            &["proto/clickhouse"],
        )?;
    Ok(())
}
