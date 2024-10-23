fn main() -> Result<(), Box<dyn std::error::Error>> {
    shadow_rs::new()?;
    //tonic_build::compile_protos("proto/hematite.proto")?;
    tonic_build::configure()
        .build_server(true)
        .compile(
            &[
                "proto/hematite/hematite.proto",
                "proto/io/cloudevents/v1/cloudevents.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}

