fn main() {
    dharma_cli::print_banner();
    if let Err(err) = dharma_cli::run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
