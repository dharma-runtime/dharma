use ciborium::value::Value;
use dharma_core::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
use dharma_core::config::Config;
use dharma_core::crypto;
use dharma_core::store::clickhouse::ClickHouseServerAnalyticsAdapter;
use dharma_core::store::consistency::compare_backends;
use dharma_core::store::postgres::PostgresServerAdapter;
use dharma_core::store::spi::StorageSpi;
use dharma_core::store::sqlite::SqliteEmbeddedAdapter;
use dharma_core::store::state;
use dharma_core::store::Store;
use dharma_core::types::{AssertionId, ContractId, IdentityKey, SchemaId, SubjectId};
use postgres::{Client as PostgresClient, NoTls};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::runtime::Builder;

struct HarnessContext {
    _temp: TempDir,
    store: Store,
    config: Config,
    postgres_url: String,
    postgres_schema: String,
    clickhouse_url: String,
    clickhouse_database: String,
    clickhouse_table_prefix: String,
}

impl Drop for HarnessContext {
    fn drop(&mut self) {
        let _ = cleanup_postgres(&self.postgres_url, &self.postgres_schema);
        let _ = cleanup_clickhouse(
            &self.clickhouse_url,
            &self.clickhouse_database,
            &self.clickhouse_table_prefix,
        );
    }
}

fn cleanup_postgres(url: &str, schema: &str) -> Result<(), postgres::Error> {
    let mut client = PostgresClient::connect(url, NoTls)?;
    client.batch_execute(&format!(
        "DROP SCHEMA IF EXISTS {} CASCADE",
        quote_postgres_ident(schema)
    ))?;
    Ok(())
}

fn cleanup_clickhouse(url: &str, database: &str, table_prefix: &str) -> Result<(), String> {
    for suffix in [
        "subject_assertions",
        "semantic_index",
        "cqrs_reverse",
        "watermark",
    ] {
        exec_clickhouse_sql(
            url,
            database,
            &format!(
                "DROP TABLE IF EXISTS `{}`",
                clickhouse_table_name(table_prefix, suffix)
            ),
        )?;
    }
    Ok(())
}

fn clickhouse_table_name(prefix: &str, suffix: &str) -> String {
    format!("{prefix}_{suffix}")
}

fn exec_clickhouse_sql(url: &str, database: &str, sql: &str) -> Result<(), String> {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("failed to build tokio runtime: {err}"))?;
    runtime
        .block_on(async {
            clickhouse::Client::default()
                .with_url(url.to_string())
                .with_database(database.to_string())
                .query(sql)
                .execute()
                .await
        })
        .map_err(|err| format!("clickhouse sql failed `{sql}`: {err}"))
}

fn quote_postgres_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn required_env(name: &str) -> Option<String> {
    let value = env::var(name).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn test_context(name: &str, postgres_url: String, clickhouse_url: String) -> HarnessContext {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let postgres_schema = format!(
        "dharma_consistency_{}_{}_{}",
        sanitize_name(name),
        std::process::id(),
        unique,
    );

    let clickhouse_table_prefix = format!(
        "dharma_consistency_{}_{}_{}",
        sanitize_name(name),
        std::process::id(),
        unique,
    );

    let temp = tempfile::tempdir().unwrap();
    let store = Store::from_root(temp.path());
    let mut config = Config::default();
    config.profile.mode = "server".to_string();
    config.storage.postgres.url = postgres_url.clone();
    config.storage.postgres.schema = postgres_schema.clone();
    config.storage.postgres.pool_max_size = 8;
    config.storage.postgres.connect_timeout_ms = 5_000;
    config.storage.postgres.acquire_timeout_ms = 5_000;
    config.storage.postgres.statement_timeout_ms = 5_000;
    config.storage.postgres.retry_max_attempts = 5;
    config.storage.postgres.retry_backoff_ms = 25;

    config.storage.clickhouse.enabled = true;
    config.storage.clickhouse.url = clickhouse_url.clone();
    config.storage.clickhouse.database = "default".to_string();
    config.storage.clickhouse.table_prefix = clickhouse_table_prefix.clone();
    config.storage.clickhouse.retry_max_attempts = 3;
    config.storage.clickhouse.retry_backoff_ms = 25;

    HarnessContext {
        _temp: temp,
        store,
        config,
        postgres_url,
        postgres_schema,
        clickhouse_url,
        clickhouse_database: "default".to_string(),
        clickhouse_table_prefix,
    }
}

fn signed_assertion(
    seed: u64,
    subject: SubjectId,
    seq: u64,
    typ: &str,
    prev: Option<AssertionId>,
    refs: Vec<AssertionId>,
) -> (AssertionId, dharma_core::EnvelopeId, Vec<u8>) {
    let mut rng = StdRng::seed_from_u64(seed);
    let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: typ.to_string(),
        auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
        seq,
        prev,
        refs,
        ts: None,
        schema: SchemaId::from_bytes([9u8; 32]),
        contract: ContractId::from_bytes([8u8; 32]),
        note: None,
        meta: None,
    };
    let body = Value::Map(vec![
        (
            Value::Text("seed".to_string()),
            Value::Integer((seed as i64).into()),
        ),
        (
            Value::Text("seq".to_string()),
            Value::Integer((seq as i64).into()),
        ),
    ]);
    let assertion = AssertionPlaintext::sign(header, body, &signing_key).unwrap();
    let bytes = assertion.to_cbor().unwrap();
    let assertion_id = assertion.assertion_id().unwrap();
    let envelope_id = crypto::envelope_id(&bytes);
    (assertion_id, envelope_id, bytes)
}

fn append_assertion(
    store: &Store,
    subject: SubjectId,
    seq: u64,
    typ: &str,
    prev: Option<AssertionId>,
    refs: Vec<AssertionId>,
    seed: u64,
) -> AssertionId {
    let (assertion_id, envelope_id, bytes) = signed_assertion(seed, subject, seq, typ, prev, refs);
    store.put_assertion(&subject, &envelope_id, &bytes).unwrap();
    store.record_semantic(&assertion_id, &envelope_id).unwrap();
    state::append_assertion(
        store.env(),
        &subject,
        seq,
        assertion_id,
        envelope_id,
        typ,
        &bytes,
    )
    .unwrap();
    assertion_id
}

fn seed_history(store: &Store) {
    let subject_a = SubjectId::from_bytes([41u8; 32]);
    let subject_b = SubjectId::from_bytes([42u8; 32]);

    let a1 = append_assertion(store, subject_a, 1, "note.text", None, vec![], 1001);
    let _a2 = append_assertion(store, subject_a, 2, "note.text", Some(a1), vec![], 1002);

    let b1 = append_assertion(store, subject_b, 1, "note.text", None, vec![], 2001);
    let b2 = append_assertion(store, subject_b, 2, "note.text", Some(b1), vec![], 2002);
    let b3 = append_assertion(store, subject_b, 2, "note.text", Some(b1), vec![], 2003);
    let _merge = append_assertion(
        store,
        subject_b,
        3,
        "core.merge",
        Some(b2),
        vec![b2, b3],
        2004,
    );
}

#[test]
fn cross_backend_parity_replay_hash_match() {
    let Some(postgres_url) = required_env("DHARMA_TEST_POSTGRES_URL") else {
        eprintln!(
            "skipping cross_backend_parity_replay_hash_match: DHARMA_TEST_POSTGRES_URL is not set"
        );
        return;
    };
    let Some(clickhouse_url) = required_env("DHARMA_TEST_CLICKHOUSE_URL") else {
        eprintln!(
            "skipping cross_backend_parity_replay_hash_match: DHARMA_TEST_CLICKHOUSE_URL is not set"
        );
        return;
    };

    let ctx = test_context("cross_backend_parity", postgres_url, clickhouse_url);
    seed_history(&ctx.store);

    let sqlite = SqliteEmbeddedAdapter::open(ctx.store.root(), ctx.store.clone()).unwrap();
    let postgres =
        PostgresServerAdapter::open(ctx.store.root(), ctx.store.clone(), &ctx.config).unwrap();
    let clickhouse =
        ClickHouseServerAnalyticsAdapter::open(ctx.store.root(), ctx.store.clone(), &ctx.config)
            .unwrap();

    let backends: [(&str, &dyn StorageSpi); 3] = [
        ("sqlite", &sqlite),
        ("postgres", &postgres),
        ("clickhouse", &clickhouse),
    ];
    let report = compare_backends(&backends).unwrap();

    assert!(
        report.issues.is_empty(),
        "unexpected issues: {:?}",
        report.issues
    );
    assert_eq!(report.snapshots.len(), 3);

    let replay_hashes: std::collections::HashSet<String> = report
        .snapshots
        .iter()
        .map(|s| s.replay_hash_hex.clone())
        .collect();
    let replay_pairs: Vec<(&str, &str)> = report
        .snapshots
        .iter()
        .map(|s| (s.backend.as_str(), s.replay_hash_hex.as_str()))
        .collect();
    assert_eq!(
        replay_hashes.len(),
        1,
        "replay hash mismatch: {:?}",
        replay_pairs
    );

    let frontier_hashes: std::collections::HashSet<String> = report
        .snapshots
        .iter()
        .map(|s| s.frontier_hash_hex.clone())
        .collect();
    let frontier_pairs: Vec<(&str, &str)> = report
        .snapshots
        .iter()
        .map(|s| (s.backend.as_str(), s.frontier_hash_hex.as_str()))
        .collect();
    assert_eq!(
        frontier_hashes.len(),
        1,
        "frontier hash mismatch: {:?}",
        frontier_pairs
    );

    let base = &report.snapshots[0];
    for snapshot in report.snapshots.iter().skip(1) {
        assert_eq!(snapshot.subjects, base.subjects);
        assert_eq!(snapshot.assertions, base.assertions);
        assert_eq!(snapshot.objects, base.objects);
    }
}
