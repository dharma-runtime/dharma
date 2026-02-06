fn main() {
    dharma_cli::print_banner();
    if let Err(err) = dharma_cli::run() {
        tracing::error!("cli exited with error");
        tracing::debug!(error = ?err, "cli error details");
        eprintln!("Error: command failed.");
        std::process::exit(1);
    }
}
