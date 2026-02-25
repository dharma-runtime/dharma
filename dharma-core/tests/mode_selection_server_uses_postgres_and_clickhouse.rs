use dharma_core::config::Config;
use dharma_core::store::spi::{BackendKind, BackendSelection, RuntimeMode};

#[test]
fn mode_selection_server_uses_postgres_and_clickhouse() {
    let mut config = Config::default();
    config.profile.mode = "server".to_string();
    let selection = BackendSelection::from_config(&config);
    assert_eq!(selection.mode, RuntimeMode::Server);
    assert_eq!(selection.commit, BackendKind::Postgres);
    assert_eq!(selection.read, BackendKind::Postgres);
    assert_eq!(selection.index, BackendKind::Postgres);
    assert_eq!(selection.query, BackendKind::ClickHouse);
}
