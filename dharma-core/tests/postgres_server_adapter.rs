use ciborium::value::Value;
use dharma_core::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
use dharma_core::config::Config;
use dharma_core::crypto;
use dharma_core::store::postgres::PostgresServerAdapter;
use dharma_core::store::spi::{StorageCommit, StorageQuery, StorageRead};
use dharma_core::store::state;
use dharma_core::store::Store;
use dharma_core::types::{ContractId, EnvelopeId, IdentityKey, SchemaId, SubjectId};
use postgres::{Client, NoTls};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::env;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

const MIGRATION_ID: &str = "0001_init";

struct PostgresTestContext {
    _temp: TempDir,
    store: Store,
    config: Config,
    url: String,
    schema: String,
}

impl Drop for PostgresTestContext {
    fn drop(&mut self) {
        let cleanup = || -> Result<(), postgres::Error> {
            let mut client = Client::connect(&self.url, NoTls)?;
            client.batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_ident(&self.schema)
            ))?;
            Ok(())
        };

        if let Err(err) = cleanup() {
            if std::thread::panicking() {
                eprintln!(
                    "warning: failed to clean up postgres test schema {}: {}",
                    self.schema, err
                );
            } else {
                panic!(
                    "failed to clean up postgres test schema {}: {}",
                    self.schema, err
                );
            }
        }
    }
}

fn postgres_url() -> Option<String> {
    let value = match env::var("DHARMA_TEST_POSTGRES_URL") {
        Ok(v) => v,
        Err(_) => return None,
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

fn require_postgres_or_skip(test_name: &str) -> Option<String> {
    let url = postgres_url();
    if url.is_none() {
        eprintln!(
            "skipping {test_name}: DHARMA_TEST_POSTGRES_URL is not set in environment"
        );
    }
    url
}

fn test_context(name: &str, url: String) -> PostgresTestContext {

    let mut client = Client::connect(&url, NoTls)
        .unwrap_or_else(|err| panic!("unable to connect to DHARMA_TEST_POSTGRES_URL {url}: {err}"));

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!(
        "dharma_test_{}_{}_{}",
        sanitize_name(name),
        std::process::id(),
        unique
    );
    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&schema)
        ))
        .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let store = Store::from_root(temp.path());
    let mut config = Config::default();
    config.storage.postgres.url = url.clone();
    config.storage.postgres.schema = schema.clone();
    config.storage.postgres.pool_max_size = 8;
    config.storage.postgres.connect_timeout_ms = 5_000;
    config.storage.postgres.acquire_timeout_ms = 5_000;
    config.storage.postgres.statement_timeout_ms = 5_000;
    config.storage.postgres.retry_max_attempts = 5;
    config.storage.postgres.retry_backoff_ms = 25;

    PostgresTestContext {
        _temp: temp,
        store,
        config,
        url,
        schema,
    }
}

fn open_adapter(ctx: &PostgresTestContext) -> PostgresServerAdapter {
    PostgresServerAdapter::open(ctx.store.root(), ctx.store.clone(), &ctx.config).unwrap()
}

fn signed_assertion_bytes(
    subject: SubjectId,
    seq: u64,
    typ: &str,
    prev: Option<dharma_core::AssertionId>,
) -> (dharma_core::AssertionId, dharma_core::EnvelopeId, Vec<u8>) {
    let mut rng = StdRng::seed_from_u64(1000 + seq);
    let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: typ.to_string(),
        auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
        seq,
        prev,
        refs: prev.into_iter().collect(),
        ts: None,
        schema: SchemaId::from_bytes([2u8; 32]),
        contract: ContractId::from_bytes([3u8; 32]),
        note: None,
        meta: None,
    };
    let assertion =
        AssertionPlaintext::sign(header, Value::Text(format!("value-{seq}")), &signing_key)
            .unwrap();
    let bytes = assertion.to_cbor().unwrap();
    let assertion_id = assertion.assertion_id().unwrap();
    let envelope_id = crypto::envelope_id(&bytes);
    (assertion_id, envelope_id, bytes)
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

#[test]
fn postgres_server_adapter_applies_migrations_and_indexes() {
    let Some(url) = require_postgres_or_skip("postgres_server_adapter_applies_migrations_and_indexes") else {
        return;
    };
    let ctx = test_context("migrations", url);

    let adapter = open_adapter(&ctx);
    assert_eq!(adapter.schema(), ctx.schema.as_str());

    let mut client = Client::connect(&ctx.url, NoTls).unwrap();
    let rows = client
        .query(
            "SELECT tablename FROM pg_tables WHERE schemaname = $1",
            &[&ctx.schema],
        )
        .unwrap();
    let table_names: Vec<String> = rows.iter().map(|row| row.get(0)).collect();

    for table in [
        "schema_migrations",
        "objects",
        "semantic_index",
        "cqrs_reverse",
        "subject_assertions",
        "permission_summaries",
    ] {
        assert!(table_names.iter().any(|name| name == table));
    }

    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = $1",
            &[&ctx.schema],
        )
        .unwrap();
    let index_names: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
    for index in [
        "idx_semantic_assertion",
        "idx_cqrs_envelope",
        "idx_cqrs_assertion",
        "idx_subject_assertions_subject_seq",
    ] {
        assert!(index_names.iter().any(|name| name == index));
    }

    let _reopened = open_adapter(&ctx);
    let count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*) FROM {}.schema_migrations WHERE id = $1",
                quote_ident(&ctx.schema)
            ),
            &[&MIGRATION_ID],
        )
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
}

#[test]
fn postgres_server_adapter_concurrent_writes_are_consistent() {
    let Some(url) = require_postgres_or_skip("postgres_server_adapter_concurrent_writes_are_consistent") else {
        return;
    };
    let ctx = test_context("concurrency", url);

    let adapter = Arc::new(open_adapter(&ctx));
    let subject = SubjectId::from_bytes([11u8; 32]);

    let (_, shared_envelope, shared_bytes) = signed_assertion_bytes(subject, 1, "note.text", None);
    let mut handles = Vec::new();
    for _ in 0..12 {
        let adapter = Arc::clone(&adapter);
        let bytes = shared_bytes.clone();
        handles.push(thread::spawn(move || {
            adapter
                .put_object_if_absent(&shared_envelope, &bytes)
                .unwrap();
        }));
    }

    for seq in 2..22 {
        let adapter = Arc::clone(&adapter);
        handles.push(thread::spawn(move || {
            let subject = SubjectId::from_bytes([11u8; 32]);
            let (_, envelope_id, bytes) = signed_assertion_bytes(subject, seq, "note.text", None);
            adapter
                .put_assertion(&subject, &envelope_id, &bytes)
                .unwrap();
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let mut client = Client::connect(&ctx.url, NoTls).unwrap();
    let object_count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*) FROM {}.objects WHERE envelope_id = $1",
                quote_ident(&ctx.schema)
            ),
            &[&shared_envelope.as_bytes().as_slice()],
        )
        .unwrap()
        .get(0);
    assert_eq!(object_count, 1);

    let rows = client
        .query(
            &format!(
                "SELECT seq FROM {}.subject_assertions \
                 WHERE subject_id = $1 AND is_overlay = FALSE \
                 ORDER BY seq ASC, assertion_id ASC",
                quote_ident(&ctx.schema)
            ),
            &[&subject.as_bytes().as_slice()],
        )
        .unwrap();

    let seqs: Vec<i64> = rows.iter().map(|row| row.get(0)).collect();
    assert!(!seqs.is_empty());
    assert!(seqs.windows(2).all(|window| window[0] <= window[1]));
}

#[test]
fn postgres_server_adapter_retries_transient_failures() {
    let Some(url) = require_postgres_or_skip("postgres_server_adapter_retries_transient_failures") else {
        return;
    };
    let mut ctx = test_context("retry", url);

    ctx.config.storage.postgres.statement_timeout_ms = 40;
    ctx.config.storage.postgres.retry_backoff_ms = 25;
    ctx.config.storage.postgres.retry_max_attempts = 6;

    let adapter = open_adapter(&ctx);

    let lock_url = ctx.url.clone();
    let lock_schema = ctx.schema.clone();
    let lock_handle = thread::spawn(move || {
        let mut client = Client::connect(&lock_url, NoTls).unwrap();
        client.batch_execute("BEGIN").unwrap();
        client
            .batch_execute(&format!(
                "LOCK TABLE {}.objects IN ACCESS EXCLUSIVE MODE",
                quote_ident(&lock_schema)
            ))
            .unwrap();
        thread::sleep(Duration::from_millis(180));
        client.batch_execute("COMMIT").unwrap();
    });

    thread::sleep(Duration::from_millis(20));

    let envelope_id = EnvelopeId::from_bytes([44u8; 32]);
    let payload = vec![0x00, 0x61, 0x73, 0x6d, 0x01];
    adapter
        .put_object_if_absent(&envelope_id, &payload)
        .unwrap();

    lock_handle.join().unwrap();

    let mut client = Client::connect(&ctx.url, NoTls).unwrap();
    let count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*) FROM {}.objects WHERE envelope_id = $1",
                quote_ident(&ctx.schema)
            ),
            &[&envelope_id.as_bytes().as_slice()],
        )
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
}

#[test]
fn postgres_server_adapter_replay_state_derivation_matches_legacy() {
    let Some(url) = require_postgres_or_skip("postgres_server_adapter_replay_state_derivation_matches_legacy") else {
        return;
    };
    let ctx = test_context("parity", url);

    let subject = SubjectId::from_bytes([77u8; 32]);
    let (base_id, base_env, base_bytes) = signed_assertion_bytes(subject, 1, "note.text", None);
    ctx.store
        .put_assertion(&subject, &base_env, &base_bytes)
        .unwrap();
    ctx.store.record_semantic(&base_id, &base_env).unwrap();
    state::append_assertion(
        ctx.store.env(),
        &subject,
        1,
        base_id,
        base_env,
        "Init",
        &base_bytes,
    )
    .unwrap();

    let (overlay_id, overlay_env, overlay_bytes) =
        signed_assertion_bytes(subject, 2, "action.Touch", Some(base_id));
    ctx.store
        .put_assertion(&subject, &overlay_env, &overlay_bytes)
        .unwrap();
    ctx.store
        .record_semantic(&overlay_id, &overlay_env)
        .unwrap();
    state::append_overlay(
        ctx.store.env(),
        &subject,
        2,
        overlay_id,
        overlay_env,
        "Touch",
        &overlay_bytes,
    )
    .unwrap();

    let adapter = open_adapter(&ctx);

    assert_eq!(
        adapter.lookup_envelope(&base_id).unwrap(),
        ctx.store.lookup_envelope(&base_id).unwrap()
    );
    assert_eq!(
        adapter.lookup_cqrs_by_envelope(&overlay_env).unwrap(),
        ctx.store.lookup_cqrs_by_envelope(&overlay_env).unwrap()
    );
    assert_eq!(
        adapter.lookup_cqrs_by_assertion(&overlay_id).unwrap(),
        ctx.store.lookup_cqrs_by_assertion(&overlay_id).unwrap()
    );
    assert_eq!(
        adapter.scan_subject(&subject).unwrap(),
        ctx.store.scan_subject(&subject).unwrap()
    );
}
