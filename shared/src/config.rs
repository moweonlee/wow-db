/// Parse `--config <path>` or `--config=<path>` from CLI arguments.
/// Returns `None` if the flag is not present.
pub fn parse_config_path() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" {
            if i + 1 < args.len() {
                return Some(args[i + 1].clone());
            }
        } else if let Some(path) = args[i].strip_prefix("--config=") {
            return Some(path.to_string());
        }
        i += 1;
    }
    None
}
