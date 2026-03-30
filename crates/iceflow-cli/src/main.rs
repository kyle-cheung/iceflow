fn main() {
    if let Err(err) = iceflow_cli::run_env() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
