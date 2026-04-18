fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("PROTOC").is_err() {
        let protoc = protoc_bin_vendored::protoc_bin_path().unwrap();
        std::env::set_var("PROTOC", protoc);
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/gen")
        .compile(
            &["../proto/compute.proto", "../proto/health.proto", "../proto/storage.proto"],
            &["../proto"],
        )?;
    Ok(())
}
