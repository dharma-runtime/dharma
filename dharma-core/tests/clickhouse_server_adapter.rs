use ciborium::value::Value;
use dharma_core::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
use dharma_core::config::Config;
use dharma_core::crypto;
use dharma_core::store::clickhouse::ClickHouseServerAnalyticsAdapter;
use dharma_core::store::spi::StorageQuery;
use dharma_core::store::state;
use dharma_core::store::Store;
use dharma_core::types::{ContractId, IdentityKey, SchemaId, SubjectId};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::runtime::Builder;

struct ClickHouseTestContext {
    _temp: TempDir,
    store: Store,
    config: Config,
    url: String,
    database: String,
    table_prefix: String,
}

impl Drop for ClickHouseTestContext {
    fn drop(&mut self) {
        for suffix in [
            "subject_assertions",
            "semantic_index",
            "cqrs_reverse",
            "watermark",
        ] {
            let _ = exec_clickhouse_sql(
                &self.url,
                &self.database,
                &format!(
                    "DROP TABLE IF EXISTS `{}`",
                    table_name(&self.table_prefix, suffix)
                ),
            );
        }
    }
}

fn clickhouse_url() -> Option<String> {
    let value = env::var("DHARMA_TEST_CLICKHOUSE_URL").ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

fn require_clickhouse_or_skip(test_name: &str) -> Option<String> {
    let url = clickhouse_url();
    if url.is_none() {
        eprintln!("skipping {test_name}: DHARMA_TEST_CLICKHOUSE_URL is not set in environment");
    }
    url
}

fn table_name(prefix: &str, suffix: &str) -> String {
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

fn test_context(name: &str, url: String) -> ClickHouseTestContext {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::from_root(temp.path());
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table_prefix = format!(
        "dharma_ch_{}_{}_{}",
        sanitize_name(name),
        std::process::id(),
        unique
    );

    let mut config = Config::default();
    config.profile.mode = "server".to_string();
    config.storage.clickhouse.enabled = true;
    config.storage.clickhouse.url = url.clone();
    config.storage.clickhouse.database = "default".to_string();
    config.storage.clickhouse.table_prefix = table_prefix.clone();
    config.storage.clickhouse.retry_max_attempts = 3;
    config.storage.clickhouse.retry_backoff_ms = 25;

    ClickHouseTestContext {
        _temp: temp,
        store,
        config,
        url,
        database: "default".to_string(),
        table_prefix,
    }
}

fn open_adapter(ctx: &ClickHouseTestContext) -> ClickHouseServerAnalyticsAdapter {
    ClickHouseServerAnalyticsAdapter::open(ctx.store.root(), ctx.store.clone(), &ctx.config)
        .unwrap()
}

fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn signed_assertion_bytes(
    subject: SubjectId,
    seq: u64,
    typ: &str,
    prev: Option<dharma_core::AssertionId>,
) -> (dharma_core::AssertionId, dharma_core::EnvelopeId, Vec<u8>) {
    let mut rng = StdRng::seed_from_u64(2000 + seq);
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
        schema: SchemaId::from_bytes([9u8; 32]),
        contract: ContractId::from_bytes([8u8; 32]),
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

fn seed_subject_history(store: &Store, subject: SubjectId) {
    let mut prev = None;
    for seq in 1..=3_u64 {
        let (assertion_id, envelope_id, bytes) =
            signed_assertion_bytes(subject, seq, "note.text", prev);
        store.put_assertion(&subject, &envelope_id, &bytes).unwrap();
        store.record_semantic(&assertion_id, &envelope_id).unwrap();
        state::append_assertion(
            store.env(),
            &subject,
            seq,
            assertion_id,
            envelope_id,
            "note.text",
            &bytes,
        )
        .unwrap();
        prev = Some(assertion_id);
    }
}

#[test]
fn clickhouse_query_parity_matches_legacy() {
    let Some(url) = require_clickhouse_or_skip("clickhouse_query_parity_matches_legacy") else {
        return;
    };
    let ctx = test_context("parity", url);
    let subject = SubjectId::from_bytes([41u8; 32]);
    seed_subject_history(&ctx.store, subject);

    let adapter = open_adapter(&ctx);
    let clickhouse_scan = adapter.scan_subject_analytics(&subject).unwrap();
    let legacy_scan = ctx.store.scan_subject(&subject).unwrap();
    assert_eq!(clickhouse_scan, legacy_scan);

    let assertion_id = legacy_scan[0];
    assert_eq!(
        adapter.lookup_envelope(&assertion_id).unwrap(),
        ctx.store.lookup_envelope(&assertion_id).unwrap()
    );
}

#[test]
fn clickhouse_backfill_reconciles_partial_projection() {
    let Some(url) = require_clickhouse_or_skip("clickhouse_backfill_reconciles_partial_projection")
    else {
        return;
    };
    let ctx = test_context("backfill", url);
    let subject = SubjectId::from_bytes([42u8; 32]);
    seed_subject_history(&ctx.store, subject);

    let adapter = open_adapter(&ctx);
    let baseline = adapter.scan_subject_analytics(&subject).unwrap();
    assert_eq!(baseline.len(), 3);

    exec_clickhouse_sql(
        &ctx.url,
        &ctx.database,
        &format!(
            "TRUNCATE TABLE `{}`",
            table_name(&ctx.table_prefix, "subject_assertions")
        ),
    )
    .unwrap();

    let repaired = adapter.scan_subject_analytics(&subject).unwrap();
    let legacy = ctx.store.scan_subject(&subject).unwrap();
    assert_eq!(repaired, legacy);
}

#[test]
fn clickhouse_rebuild_is_deterministic() {
    let Some(url) = require_clickhouse_or_skip("clickhouse_rebuild_is_deterministic") else {
        return;
    };
    let ctx = test_context("rebuild", url);
    let subject = SubjectId::from_bytes([43u8; 32]);
    seed_subject_history(&ctx.store, subject);

    let adapter = open_adapter(&ctx);
    let first = adapter.rebuild_from_canonical().unwrap();
    let first_scan = adapter.scan_subject_analytics(&subject).unwrap();

    let second = adapter.rebuild_from_canonical().unwrap();
    let second_scan = adapter.scan_subject_analytics(&subject).unwrap();

    assert_eq!(first.watermark_seq, second.watermark_seq);
    assert_eq!(first.committed_seq, second.committed_seq);
    assert_eq!(first_scan, second_scan);
}
