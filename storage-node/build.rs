fn main() -> Result<(), Box<dyn std::error::Error>> {
    // protoc 바이너리 자동 탐색 (시스템 protoc 없으면 vendored 사용)
    if std::env::var("PROTOC").is_err() {
        let protoc = protoc_bin_vendored::protoc_bin_path().unwrap();
        std::env::set_var("PROTOC", protoc);
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/gen")
        .compile(
            &[
                "../proto/storage.proto",
                "../proto/compute.proto",
                "../proto/health.proto",
            ],
            &["../proto"],
        )?;
    Ok(())
}
