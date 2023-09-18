use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = fs::read_dir("./spark/connector/connect/common/src/main/protobuf/spark/connect")?;

    let mut file_paths: Vec<String> = vec![];

    for file in files {
        let entry = file?.path();
        file_paths.push(entry.to_str().unwrap().to_string());
    }

    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .build_server(false)
        .build_client(true)
        .compile(
            file_paths.as_ref(),
            &["./spark/connector/connect/common/src/main/protobuf"],
        )?;

    Ok(())
}
