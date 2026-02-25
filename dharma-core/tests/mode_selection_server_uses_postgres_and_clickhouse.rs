use dharma_core::config::Config;
use dharma_core::store::spi::{
    BackendKind, BackendSelection, RuntimeMode, BACKEND_CAPABILITY_MATRIX,
};

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

#[test]
fn capability_matrix_marks_clickhouse_as_eventually_consistent_query_backend() {
    let clickhouse = BACKEND_CAPABILITY_MATRIX
        .iter()
        .find(|row| row.backend == BackendKind::ClickHouse)
        .expect("clickhouse backend row missing");
    assert!(clickhouse.query);
    assert!(!clickhouse.commit);
    assert!(!clickhouse.read);
    assert!(clickhouse.eventual_consistency);
}
