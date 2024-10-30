use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = fs::read_dir("./protobuf/spark-3.5/spark/connect/")?;

    let mut file_paths: Vec<String> = vec![];

    for file in files {
        let entry = file?.path();
        file_paths.push(entry.to_str().unwrap().to_string());
    }

    #[cfg(feature = "wasm")]
    let transport = false;
    #[cfg(not(feature = "wasm"))]
    let transport = true;

    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .build_server(false)
        .build_client(true)
        .build_transport(transport)
        .compile(file_paths.as_ref(), &["./protobuf/spark-3.5/"])?;

    Ok(())
}
