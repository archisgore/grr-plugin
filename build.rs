// Build the VM's protobuf into a Rust server
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(false)
        .compile(
            &[
                "proto/grpc_broker.proto",
                "proto/grpc_controller.proto",
                "proto/grpc_stdio.proto",
            ],
            &["proto"],
        )?;

    Ok(())
}
