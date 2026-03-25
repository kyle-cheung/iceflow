fn main() {
    if let Err(err) = greytl_cli::run_env() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
