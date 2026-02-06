fn main() {
    dharma_cli::print_banner();
    if let Err(err) = dharma_cli::run() {
        tracing::error!(error = %err, "cli exited with error");
        std::process::exit(1);
    }
}
