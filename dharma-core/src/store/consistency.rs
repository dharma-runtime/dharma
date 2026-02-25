use crate::assertion::AssertionPlaintext;
use crate::config::Config;
use crate::error::DharmaError;
use crate::store::clickhouse::ClickHouseServerAnalyticsAdapter;
use crate::store::postgres::PostgresServerAdapter;
use crate::store::spi::StorageSpi;
use crate::store::sqlite::SqliteEmbeddedAdapter;
use crate::store::state::CqrsReverseEntry;
use crate::store::Store;
use crate::types::{AssertionId, EnvelopeId, SubjectId};
use clickhouse::{Client as ClickHouseClient, Row};
use postgres::{Client as PostgresClient, NoTls};
use rusqlite::Connection;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use tokio::runtime::Builder;

const POSTGRES_INIT_MIGRATION_ID: &str = "0001_init";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendSnapshot {
    pub backend: String,
    pub subjects: usize,
    pub assertions: usize,
    pub objects: usize,
    pub replay_hash_hex: String,
    pub frontier_hash_hex: String,
    pub issues: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendValidation {
    pub backend: String,
    pub subjects: usize,
    pub assertions: usize,
    pub objects: usize,
    pub replay_hash_hex: String,
    pub frontier_hash_hex: String,
    pub issues: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CrossBackendReport {
    pub snapshots: Vec<BackendSnapshot>,
    pub issues: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationValidationReport {
    pub validations: Vec<BackendValidation>,
    pub issues: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationBackend {
    Sqlite,
    Postgres,
    ClickHouse,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConsistencySubjectRow {
    pub subject: SubjectId,
    pub seq: u64,
    pub assertion_id: AssertionId,
    pub envelope_id: EnvelopeId,
    pub is_overlay: bool,
    pub bytes: Option<Vec<u8>>,
    pub inserted_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConsistencySemanticRow {
    pub assertion_id: AssertionId,
    pub envelope_id: EnvelopeId,
    pub inserted_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConsistencyCqrsRow {
    pub envelope_id: EnvelopeId,
    pub assertion_id: AssertionId,
    pub subject: SubjectId,
    pub is_overlay: bool,
    pub inserted_at: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct BackendConsistencyRows {
    pub subject_rows: Vec<ConsistencySubjectRow>,
    pub semantic_rows: Vec<ConsistencySemanticRow>,
    pub cqrs_rows: Vec<ConsistencyCqrsRow>,
}

#[derive(Clone)]
struct AssertionDigestRow {
    subject: SubjectId,
    seq: u64,
    ver: u64,
    assertion_id: AssertionId,
    envelope_id: EnvelopeId,
    prev: Option<AssertionId>,
    merge_refs: Vec<AssertionId>,
    bytes_hash: [u8; 32],
}

#[derive(Default)]
struct FrontierState {
    tips: BTreeMap<SubjectId, BTreeMap<u64, BTreeMap<AssertionId, u64>>>,
}

#[derive(Row, Deserialize)]
struct ClickHouseCountRow {
    count: u64,
}

#[derive(Row, Deserialize)]
struct ClickHouseWatermarkRow {
    watermark_seq: u64,
    committed_seq: u64,
}

#[derive(Row, Deserialize)]
struct ClickHouseSubjectAssertionRow {
    subject_id: String,
    seq: u64,
    assertion_id: String,
    envelope_id: String,
    is_overlay: u8,
    inserted_at: u64,
}

#[derive(Row, Deserialize)]
struct ClickHouseSemanticRow {
    assertion_id: String,
    envelope_id: String,
    inserted_at: u64,
}

#[derive(Row, Deserialize)]
struct ClickHouseCqrsRow {
    envelope_id: String,
    assertion_id: String,
    subject_id: String,
    is_overlay: u8,
    inserted_at: u64,
}

impl FrontierState {
    fn apply(
        &mut self,
        subject: SubjectId,
        ver: u64,
        seq: u64,
        assertion_id: AssertionId,
        prev: Option<AssertionId>,
        merge_refs: &[AssertionId],
    ) {
        let tips = self
            .tips
            .entry(subject)
            .or_default()
            .entry(ver)
            .or_default();
        if let Some(prev_id) = prev {
            tips.remove(&prev_id);
        }
        for ref_id in merge_refs {
            tips.remove(ref_id);
        }
        tips.insert(assertion_id, seq);
    }

    fn digest_hex(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        for (subject, by_ver) in &self.tips {
            for (ver, tips) in by_ver {
                for (assertion_id, seq) in tips {
                    hasher.update(b"frontier");
                    hasher.update(subject.as_bytes());
                    hasher.update(&ver.to_le_bytes());
                    hasher.update(assertion_id.as_bytes());
                    hasher.update(&seq.to_le_bytes());
                }
            }
        }
        hex::encode(hasher.finalize().as_bytes())
    }
}

pub fn compare_backends(
    backends: &[(&str, &dyn StorageSpi)],
) -> Result<CrossBackendReport, DharmaError> {
    if backends.is_empty() {
        return Err(DharmaError::Validation(
            "compare_backends requires at least one backend".to_string(),
        ));
    }

    let mut snapshots = Vec::with_capacity(backends.len());
    for (name, backend) in backends {
        if let Some(sqlite) = backend.as_any().downcast_ref::<SqliteEmbeddedAdapter>() {
            snapshots.push(capture_snapshot_from_rows(
                name,
                sqlite.consistency_rows()?,
            )?);
            continue;
        }
        if let Some(postgres) = backend.as_any().downcast_ref::<PostgresServerAdapter>() {
            snapshots.push(capture_snapshot_from_rows(
                name,
                postgres.consistency_rows()?,
            )?);
            continue;
        }
        if let Some(clickhouse) = backend
            .as_any()
            .downcast_ref::<ClickHouseServerAnalyticsAdapter>()
        {
            snapshots.push(capture_snapshot_from_rows(
                name,
                clickhouse.consistency_rows()?,
            )?);
            continue;
        }
        snapshots.push(capture_snapshot(name, *backend)?);
    }

    Ok(compare_snapshot_set(snapshots))
}

pub fn compare_configured_backends(
    root: &Path,
    config: &Config,
    store: &Store,
) -> Result<CrossBackendReport, DharmaError> {
    let data_root = config.storage_path(root);
    let sqlite_path = data_root.join("indexes").join("embedded.sqlite");

    let sqlite_rows = read_sqlite_consistency_rows(&sqlite_path)?;
    let postgres_rows = read_postgres_consistency_rows(config)?;
    let clickhouse_rows = read_clickhouse_consistency_rows(config, store)?;

    let snapshots = vec![
        capture_snapshot_from_rows("sqlite", sqlite_rows)?,
        capture_snapshot_from_rows("postgres", postgres_rows)?,
        capture_snapshot_from_rows("clickhouse", clickhouse_rows)?,
    ];
    Ok(compare_snapshot_set(snapshots))
}

pub fn validate_migrations(
    root: &Path,
    config: &Config,
    store: &Store,
) -> Result<MigrationValidationReport, DharmaError> {
    validate_migrations_for_backends(
        root,
        config,
        store,
        &[
            MigrationBackend::Sqlite,
            MigrationBackend::Postgres,
            MigrationBackend::ClickHouse,
        ],
    )
}

pub fn validate_migrations_for_backends(
    root: &Path,
    config: &Config,
    store: &Store,
    backends: &[MigrationBackend],
) -> Result<MigrationValidationReport, DharmaError> {
    let baseline = capture_snapshot("canonical", store)?;
    let data_root = config.storage_path(root);

    let mut validations = Vec::with_capacity(backends.len());
    for backend in backends {
        let validation = match backend {
            MigrationBackend::Sqlite => validate_sqlite_backend(&data_root, &baseline)?,
            MigrationBackend::Postgres => validate_postgres_backend(config, &baseline)?,
            MigrationBackend::ClickHouse => validate_clickhouse_backend(config, &baseline)?,
        };
        validations.push(validation);
    }
    let mut issues = Vec::new();
    for validation in &validations {
        for issue in &validation.issues {
            issues.push(format!("{}: {issue}", validation.backend));
        }
    }

    Ok(MigrationValidationReport {
        validations,
        issues,
    })
}

fn compare_snapshot_set(snapshots: Vec<BackendSnapshot>) -> CrossBackendReport {
    let mut issues = Vec::new();
    for snapshot in &snapshots {
        for issue in &snapshot.issues {
            issues.push(format!("{}: {issue}", snapshot.backend));
        }
    }

    let baseline = &snapshots[0];
    for snapshot in snapshots.iter().skip(1) {
        if snapshot.subjects != baseline.subjects {
            issues.push(format!(
                "subjects mismatch: {}={} {}={}",
                baseline.backend, baseline.subjects, snapshot.backend, snapshot.subjects
            ));
        }
        if snapshot.assertions != baseline.assertions {
            issues.push(format!(
                "assertions mismatch: {}={} {}={}",
                baseline.backend, baseline.assertions, snapshot.backend, snapshot.assertions
            ));
        }
        if snapshot.objects != baseline.objects {
            issues.push(format!(
                "objects mismatch: {}={} {}={}",
                baseline.backend, baseline.objects, snapshot.backend, snapshot.objects
            ));
        }
        if snapshot.replay_hash_hex != baseline.replay_hash_hex {
            issues.push(format!(
                "replay_hash_hex mismatch: {}={} {}={}",
                baseline.backend,
                baseline.replay_hash_hex,
                snapshot.backend,
                snapshot.replay_hash_hex
            ));
        }
        if snapshot.frontier_hash_hex != baseline.frontier_hash_hex {
            issues.push(format!(
                "frontier_hash_hex mismatch: {}={} {}={}",
                baseline.backend,
                baseline.frontier_hash_hex,
                snapshot.backend,
                snapshot.frontier_hash_hex
            ));
        }
    }

    CrossBackendReport { snapshots, issues }
}

fn read_sqlite_consistency_rows(db_path: &Path) -> Result<BackendConsistencyRows, DharmaError> {
    if !db_path.exists() {
        return Err(DharmaError::Validation(format!(
            "sqlite file missing: {}",
            db_path.display()
        )));
    }
    let conn = Connection::open(db_path)?;

    let mut subject_stmt = conn.prepare(
        "SELECT subject_id, seq, assertion_id, envelope_id, is_overlay, bytes, rowid
         FROM subject_assertions",
    )?;
    let mut subject_rows_sql = subject_stmt.query([])?;
    let mut subject_rows = Vec::new();
    while let Some(row) = subject_rows_sql.next()? {
        let subject_raw: Vec<u8> = row.get(0)?;
        let seq_i64: i64 = row.get(1)?;
        let assertion_raw: Vec<u8> = row.get(2)?;
        let envelope_raw: Vec<u8> = row.get(3)?;
        let is_overlay_i64: i64 = row.get(4)?;
        let bytes: Vec<u8> = row.get(5)?;
        let rowid_i64: i64 = row.get(6)?;
        let seq = u64::try_from(seq_i64)
            .map_err(|_| DharmaError::Validation("assertion seq overflow".to_string()))?;
        let inserted_at = u64::try_from(rowid_i64).map_err(|_| {
            DharmaError::Validation("subject_assertions rowid overflow".to_string())
        })?;
        subject_rows.push(ConsistencySubjectRow {
            subject: SubjectId::from_slice(&subject_raw)?,
            seq,
            assertion_id: AssertionId::from_slice(&assertion_raw)?,
            envelope_id: EnvelopeId::from_slice(&envelope_raw)?,
            is_overlay: is_overlay_i64 != 0,
            bytes: Some(bytes),
            inserted_at,
        });
    }

    let mut semantic_stmt =
        conn.prepare("SELECT assertion_id, envelope_id, inserted_at FROM semantic_index")?;
    let mut semantic_rows_sql = semantic_stmt.query([])?;
    let mut semantic_rows = Vec::new();
    while let Some(row) = semantic_rows_sql.next()? {
        let assertion_raw: Vec<u8> = row.get(0)?;
        let envelope_raw: Vec<u8> = row.get(1)?;
        let inserted_i64: i64 = row.get(2)?;
        let inserted_at = u64::try_from(inserted_i64).map_err(|_| {
            DharmaError::Validation("semantic_index inserted_at overflow".to_string())
        })?;
        semantic_rows.push(ConsistencySemanticRow {
            assertion_id: AssertionId::from_slice(&assertion_raw)?,
            envelope_id: EnvelopeId::from_slice(&envelope_raw)?,
            inserted_at,
        });
    }

    let mut cqrs_stmt = conn.prepare(
        "SELECT envelope_id, assertion_id, subject_id, is_overlay, inserted_at
         FROM cqrs_reverse",
    )?;
    let mut cqrs_rows_sql = cqrs_stmt.query([])?;
    let mut cqrs_rows = Vec::new();
    while let Some(row) = cqrs_rows_sql.next()? {
        let envelope_raw: Vec<u8> = row.get(0)?;
        let assertion_raw: Vec<u8> = row.get(1)?;
        let subject_raw: Vec<u8> = row.get(2)?;
        let is_overlay_i64: i64 = row.get(3)?;
        let inserted_i64: i64 = row.get(4)?;
        let inserted_at = u64::try_from(inserted_i64).map_err(|_| {
            DharmaError::Validation("cqrs_reverse inserted_at overflow".to_string())
        })?;
        cqrs_rows.push(ConsistencyCqrsRow {
            envelope_id: EnvelopeId::from_slice(&envelope_raw)?,
            assertion_id: AssertionId::from_slice(&assertion_raw)?,
            subject: SubjectId::from_slice(&subject_raw)?,
            is_overlay: is_overlay_i64 != 0,
            inserted_at,
        });
    }

    Ok(BackendConsistencyRows {
        subject_rows,
        semantic_rows,
        cqrs_rows,
    })
}

fn read_postgres_consistency_rows(config: &Config) -> Result<BackendConsistencyRows, DharmaError> {
    let url = config.storage.postgres.url.trim();
    if url.is_empty() {
        return Err(DharmaError::Validation(
            "storage.postgres.url must not be empty".to_string(),
        ));
    }
    let schema = normalize_postgres_schema(&config.storage.postgres.schema);
    let schema_ident = quote_postgres_ident(&schema);
    let mut client = PostgresClient::connect(url, NoTls)?;

    let subject_rows_sql = client.query(
        &format!(
            "SELECT subject_id, seq, assertion_id, envelope_id, is_overlay, bytes, inserted_at FROM {}.subject_assertions",
            schema_ident
        ),
        &[],
    )?;
    let mut subject_rows = Vec::with_capacity(subject_rows_sql.len());
    for row in subject_rows_sql {
        let subject_raw: Vec<u8> = row.get(0);
        let seq_i64: i64 = row.get(1);
        let assertion_raw: Vec<u8> = row.get(2);
        let envelope_raw: Vec<u8> = row.get(3);
        let is_overlay: bool = row.get(4);
        let bytes: Vec<u8> = row.get(5);
        let inserted_i64: i64 = row.get(6);
        let seq = u64::try_from(seq_i64)
            .map_err(|_| DharmaError::Validation("assertion seq overflow".to_string()))?;
        let inserted_at = u64::try_from(inserted_i64).map_err(|_| {
            DharmaError::Validation("subject_assertions inserted_at overflow".to_string())
        })?;
        subject_rows.push(ConsistencySubjectRow {
            subject: SubjectId::from_slice(&subject_raw)?,
            seq,
            assertion_id: AssertionId::from_slice(&assertion_raw)?,
            envelope_id: EnvelopeId::from_slice(&envelope_raw)?,
            is_overlay,
            bytes: Some(bytes),
            inserted_at,
        });
    }

    let semantic_rows_sql = client.query(
        &format!(
            "SELECT assertion_id, envelope_id, inserted_at FROM {}.semantic_index",
            schema_ident
        ),
        &[],
    )?;
    let mut semantic_rows = Vec::with_capacity(semantic_rows_sql.len());
    for row in semantic_rows_sql {
        let assertion_raw: Vec<u8> = row.get(0);
        let envelope_raw: Vec<u8> = row.get(1);
        let inserted_i64: i64 = row.get(2);
        let inserted_at = u64::try_from(inserted_i64).map_err(|_| {
            DharmaError::Validation("semantic_index inserted_at overflow".to_string())
        })?;
        semantic_rows.push(ConsistencySemanticRow {
            assertion_id: AssertionId::from_slice(&assertion_raw)?,
            envelope_id: EnvelopeId::from_slice(&envelope_raw)?,
            inserted_at,
        });
    }

    let cqrs_rows_sql = client.query(
        &format!(
            "SELECT envelope_id, assertion_id, subject_id, is_overlay, inserted_at FROM {}.cqrs_reverse",
            schema_ident
        ),
        &[],
    )?;
    let mut cqrs_rows = Vec::with_capacity(cqrs_rows_sql.len());
    for row in cqrs_rows_sql {
        let envelope_raw: Vec<u8> = row.get(0);
        let assertion_raw: Vec<u8> = row.get(1);
        let subject_raw: Vec<u8> = row.get(2);
        let is_overlay: bool = row.get(3);
        let inserted_i64: i64 = row.get(4);
        let inserted_at = u64::try_from(inserted_i64).map_err(|_| {
            DharmaError::Validation("cqrs_reverse inserted_at overflow".to_string())
        })?;
        cqrs_rows.push(ConsistencyCqrsRow {
            envelope_id: EnvelopeId::from_slice(&envelope_raw)?,
            assertion_id: AssertionId::from_slice(&assertion_raw)?,
            subject: SubjectId::from_slice(&subject_raw)?,
            is_overlay,
            inserted_at,
        });
    }

    Ok(BackendConsistencyRows {
        subject_rows,
        semantic_rows,
        cqrs_rows,
    })
}

fn read_clickhouse_consistency_rows(
    config: &Config,
    store: &Store,
) -> Result<BackendConsistencyRows, DharmaError> {
    if !config.storage.clickhouse.enabled {
        return Err(DharmaError::Validation(
            "storage.clickhouse.enabled is false".to_string(),
        ));
    }
    let url = config.storage.clickhouse.url.trim();
    if url.is_empty() {
        return Err(DharmaError::Validation(
            "storage.clickhouse.url must not be empty".to_string(),
        ));
    }

    let database = normalize_clickhouse_identifier(&config.storage.clickhouse.database, "default");
    let table_prefix = normalize_clickhouse_identifier(
        &config.storage.clickhouse.table_prefix,
        "dharma_analytics",
    );
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| DharmaError::Config(format!("unable to build clickhouse runtime: {err}")))?;
    let client = ClickHouseClient::default()
        .with_url(url.to_string())
        .with_database(database.clone());

    let subject_table = format!("{table_prefix}_subject_assertions");
    let subject_sql = format!(
        "SELECT subject_id, seq, assertion_id, envelope_id, is_overlay, inserted_at FROM `{}`",
        subject_table
    );
    let subject_records = runtime.block_on(async {
        client
            .query(&subject_sql)
            .fetch_all::<ClickHouseSubjectAssertionRow>()
            .await
    })?;
    let mut subject_rows = Vec::with_capacity(subject_records.len());
    for row in subject_records {
        let envelope_id = EnvelopeId::from_hex(&row.envelope_id)?;
        subject_rows.push(ConsistencySubjectRow {
            subject: SubjectId::from_hex(&row.subject_id)?,
            seq: row.seq,
            assertion_id: AssertionId::from_hex(&row.assertion_id)?,
            envelope_id,
            is_overlay: row.is_overlay != 0,
            bytes: store.get_object_any(&envelope_id)?,
            inserted_at: row.inserted_at,
        });
    }

    let semantic_table = format!("{table_prefix}_semantic_index");
    let semantic_sql = format!(
        "SELECT assertion_id, envelope_id, inserted_at FROM `{}`",
        semantic_table
    );
    let semantic_records = runtime.block_on(async {
        client
            .query(&semantic_sql)
            .fetch_all::<ClickHouseSemanticRow>()
            .await
    })?;
    let mut semantic_rows = Vec::with_capacity(semantic_records.len());
    for row in semantic_records {
        semantic_rows.push(ConsistencySemanticRow {
            assertion_id: AssertionId::from_hex(&row.assertion_id)?,
            envelope_id: EnvelopeId::from_hex(&row.envelope_id)?,
            inserted_at: row.inserted_at,
        });
    }

    let cqrs_table = format!("{table_prefix}_cqrs_reverse");
    let cqrs_sql = format!(
        "SELECT envelope_id, assertion_id, subject_id, is_overlay, inserted_at FROM `{}`",
        cqrs_table
    );
    let cqrs_records = runtime.block_on(async {
        client
            .query(&cqrs_sql)
            .fetch_all::<ClickHouseCqrsRow>()
            .await
    })?;
    let mut cqrs_rows = Vec::with_capacity(cqrs_records.len());
    for row in cqrs_records {
        cqrs_rows.push(ConsistencyCqrsRow {
            envelope_id: EnvelopeId::from_hex(&row.envelope_id)?,
            assertion_id: AssertionId::from_hex(&row.assertion_id)?,
            subject: SubjectId::from_hex(&row.subject_id)?,
            is_overlay: row.is_overlay != 0,
            inserted_at: row.inserted_at,
        });
    }

    Ok(BackendConsistencyRows {
        subject_rows,
        semantic_rows,
        cqrs_rows,
    })
}

fn capture_snapshot_from_rows(
    name: &str,
    rows: BackendConsistencyRows,
) -> Result<BackendSnapshot, DharmaError> {
    let mut issues = Vec::new();
    let mut replay_hasher = blake3::Hasher::new();
    let mut frontier_state = FrontierState::default();

    let mut subject_index: BTreeMap<(SubjectId, bool, u64, AssertionId), ConsistencySubjectRow> =
        BTreeMap::new();
    for row in rows.subject_rows {
        let key = (row.subject, row.is_overlay, row.seq, row.assertion_id);
        match subject_index.get(&key) {
            Some(existing) if existing.inserted_at > row.inserted_at => {}
            Some(existing)
                if existing.inserted_at == row.inserted_at
                    && existing.envelope_id.as_bytes() >= row.envelope_id.as_bytes() => {}
            _ => {
                subject_index.insert(key, row);
            }
        }
    }

    let mut semantic_by_assertion: BTreeMap<AssertionId, (EnvelopeId, u64)> = BTreeMap::new();
    for row in rows.semantic_rows {
        let entry = semantic_by_assertion
            .entry(row.assertion_id)
            .or_insert((row.envelope_id, row.inserted_at));
        if row.inserted_at > entry.1
            || (row.inserted_at == entry.1 && row.envelope_id.as_bytes() < entry.0.as_bytes())
        {
            *entry = (row.envelope_id, row.inserted_at);
        }
    }

    let mut cqrs_by_assertion: BTreeMap<AssertionId, (ConsistencyCqrsRow, u64)> = BTreeMap::new();
    let mut cqrs_by_envelope: BTreeMap<EnvelopeId, (ConsistencyCqrsRow, u64)> = BTreeMap::new();
    for row in rows.cqrs_rows {
        let assertion_entry = cqrs_by_assertion
            .entry(row.assertion_id)
            .or_insert((row.clone(), row.inserted_at));
        if row.inserted_at > assertion_entry.1
            || (row.inserted_at == assertion_entry.1
                && row.envelope_id.as_bytes() < assertion_entry.0.envelope_id.as_bytes())
        {
            *assertion_entry = (row.clone(), row.inserted_at);
        }

        let envelope_entry = cqrs_by_envelope
            .entry(row.envelope_id)
            .or_insert((row.clone(), row.inserted_at));
        if row.inserted_at > envelope_entry.1
            || (row.inserted_at == envelope_entry.1
                && row.assertion_id.as_bytes() < envelope_entry.0.assertion_id.as_bytes())
        {
            *envelope_entry = (row.clone(), row.inserted_at);
        }
    }

    let mut subjects = BTreeSet::new();
    let mut object_ids = BTreeSet::new();
    let mut assertion_count = 0usize;
    for row in subject_index.values() {
        if row.is_overlay {
            continue;
        }
        subjects.insert(row.subject);
        object_ids.insert(row.envelope_id);
    }
    for subject in &subjects {
        replay_hasher.update(b"subject");
        replay_hasher.update(subject.as_bytes());
    }

    for row in subject_index.values() {
        if row.is_overlay {
            continue;
        }
        assertion_count = assertion_count.saturating_add(1);

        let Some(assertion_bytes) = row.bytes.as_ref() else {
            issues.push(format!(
                "missing assertion bytes for envelope {}",
                row.envelope_id.to_hex()
            ));
            continue;
        };

        let bytes_hash = blake3::hash(assertion_bytes);
        let mut ver = 0u64;
        let mut seq = row.seq;
        let mut prev = None;
        let mut merge_refs = Vec::new();

        match AssertionPlaintext::from_cbor(assertion_bytes) {
            Ok(parsed) => {
                match parsed.assertion_id() {
                    Ok(parsed_id) if parsed_id != row.assertion_id => {
                        issues.push(format!(
                            "assertion id mismatch for subject {}: row={} parsed={}",
                            row.subject.to_hex(),
                            row.assertion_id.to_hex(),
                            parsed_id.to_hex()
                        ));
                    }
                    Ok(_) => {}
                    Err(err) => issues.push(format!(
                        "failed deriving assertion id for envelope {}: {err}",
                        row.envelope_id.to_hex()
                    )),
                }
                if parsed.header.sub != row.subject {
                    issues.push(format!(
                        "subject mismatch for assertion {}: row={} parsed={}",
                        row.assertion_id.to_hex(),
                        row.subject.to_hex(),
                        parsed.header.sub.to_hex()
                    ));
                }
                if parsed.header.seq != row.seq {
                    issues.push(format!(
                        "seq mismatch for assertion {}: row={} parsed={}",
                        row.assertion_id.to_hex(),
                        row.seq,
                        parsed.header.seq
                    ));
                }
                ver = parsed.header.ver;
                seq = parsed.header.seq;
                prev = parsed.header.prev;
                if parsed.header.typ == "core.merge" {
                    merge_refs = parsed.header.refs.clone();
                    merge_refs.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
                }
            }
            Err(err) => {
                issues.push(format!(
                    "invalid assertion cbor for assertion {}: {err}",
                    row.assertion_id.to_hex()
                ));
            }
        }

        replay_hasher.update(b"assertion");
        replay_hasher.update(row.subject.as_bytes());
        replay_hasher.update(&seq.to_le_bytes());
        replay_hasher.update(row.assertion_id.as_bytes());
        replay_hasher.update(row.envelope_id.as_bytes());
        replay_hasher.update(bytes_hash.as_bytes());
        if let Some(prev_id) = prev {
            replay_hasher.update(b"prev");
            replay_hasher.update(prev_id.as_bytes());
        }
        for ref_id in &merge_refs {
            replay_hasher.update(b"merge_ref");
            replay_hasher.update(ref_id.as_bytes());
        }

        match semantic_by_assertion.get(&row.assertion_id) {
            Some((mapped_envelope, _)) => {
                replay_hasher.update(b"semantic");
                replay_hasher.update(row.assertion_id.as_bytes());
                replay_hasher.update(mapped_envelope.as_bytes());
                if mapped_envelope != &row.envelope_id {
                    issues.push(format!(
                        "semantic mismatch for assertion {}: subject_assertions={} semantic_index={}",
                        row.assertion_id.to_hex(),
                        row.envelope_id.to_hex(),
                        mapped_envelope.to_hex()
                    ));
                }
            }
            None => {
                issues.push(format!(
                    "missing semantic mapping for assertion {}",
                    row.assertion_id.to_hex()
                ));
            }
        }

        hash_lookup_tuple_from_maps(
            &mut replay_hasher,
            &cqrs_by_assertion,
            &cqrs_by_envelope,
            &row.assertion_id,
            &row.envelope_id,
        );

        frontier_state.apply(row.subject, ver, seq, row.assertion_id, prev, &merge_refs);
    }

    for (assertion_id, (envelope_id, _)) in &semantic_by_assertion {
        replay_hasher.update(b"semantic_map");
        replay_hasher.update(assertion_id.as_bytes());
        replay_hasher.update(envelope_id.as_bytes());
    }
    for (assertion_id, (entry, _)) in &cqrs_by_assertion {
        replay_hasher.update(b"cqrs_assertion_map");
        replay_hasher.update(assertion_id.as_bytes());
        hash_cqrs_consistency_row(&mut replay_hasher, entry);
    }
    for (envelope_id, (entry, _)) in &cqrs_by_envelope {
        replay_hasher.update(b"cqrs_envelope_map");
        replay_hasher.update(envelope_id.as_bytes());
        hash_cqrs_consistency_row(&mut replay_hasher, entry);
    }

    Ok(BackendSnapshot {
        backend: name.to_string(),
        subjects: subjects.len(),
        assertions: assertion_count,
        objects: object_ids.len(),
        replay_hash_hex: hex::encode(replay_hasher.finalize().as_bytes()),
        frontier_hash_hex: frontier_state.digest_hex(),
        issues,
    })
}

fn hash_lookup_tuple_from_maps(
    hasher: &mut blake3::Hasher,
    by_assertion: &BTreeMap<AssertionId, (ConsistencyCqrsRow, u64)>,
    by_envelope: &BTreeMap<EnvelopeId, (ConsistencyCqrsRow, u64)>,
    assertion_id: &AssertionId,
    envelope_id: &EnvelopeId,
) {
    if let Some((entry, _)) = by_assertion.get(assertion_id) {
        hasher.update(b"cqrs_by_assertion");
        hash_cqrs_consistency_row(hasher, entry);
    } else {
        hasher.update(b"cqrs_by_assertion_none");
    }

    if let Some((entry, _)) = by_envelope.get(envelope_id) {
        hasher.update(b"cqrs_by_envelope");
        hash_cqrs_consistency_row(hasher, entry);
    } else {
        hasher.update(b"cqrs_by_envelope_none");
    }
}

fn hash_cqrs_consistency_row(hasher: &mut blake3::Hasher, row: &ConsistencyCqrsRow) {
    hasher.update(row.envelope_id.as_bytes());
    hasher.update(row.assertion_id.as_bytes());
    hasher.update(row.subject.as_bytes());
    hasher.update(&[u8::from(row.is_overlay)]);
}

fn capture_snapshot(name: &str, backend: &dyn StorageSpi) -> Result<BackendSnapshot, DharmaError> {
    let mut issues = Vec::new();

    let mut subjects = backend.list_subjects()?;
    subjects.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

    let mut objects = backend.list_objects()?;
    objects.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

    let mut replay_hasher = blake3::Hasher::new();
    let mut frontier_state = FrontierState::default();
    let mut assertion_rows = Vec::new();
    let mut assertion_count = 0usize;

    for subject in &subjects {
        replay_hasher.update(b"subject");
        replay_hasher.update(subject.as_bytes());

        let mut assertion_ids = backend.scan_subject(subject)?;
        assertion_ids.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

        for assertion_id in assertion_ids {
            assertion_count = assertion_count.saturating_add(1);

            let envelope_id = match backend.lookup_envelope(&assertion_id) {
                Ok(Some(id)) => id,
                Ok(None) => {
                    issues.push(format!(
                        "missing envelope mapping for assertion {}",
                        assertion_id.to_hex()
                    ));
                    continue;
                }
                Err(err) => {
                    issues.push(format!(
                        "lookup_envelope failed for assertion {}: {err}",
                        assertion_id.to_hex()
                    ));
                    continue;
                }
            };

            let assertion_bytes = match backend.get_assertion(subject, &envelope_id) {
                Ok(bytes) => bytes,
                Err(err) => {
                    issues.push(format!(
                        "get_assertion failed for subject {} assertion {}: {err}",
                        subject.to_hex(),
                        assertion_id.to_hex()
                    ));
                    continue;
                }
            };

            let mut seq = 0u64;
            let mut ver = 0u64;
            let mut prev = None;
            let mut merge_refs = Vec::new();

            match AssertionPlaintext::from_cbor(&assertion_bytes) {
                Ok(parsed) => {
                    seq = parsed.header.seq;
                    ver = parsed.header.ver;
                    prev = parsed.header.prev;
                    if parsed.header.typ == "core.merge" {
                        merge_refs = parsed.header.refs.clone();
                        merge_refs.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
                    }
                    replay_hasher.update(b"assertion_header");
                    replay_hasher.update(&parsed.header.ver.to_le_bytes());
                    replay_hasher.update(&parsed.header.seq.to_le_bytes());
                    replay_hasher.update(parsed.header.sub.as_bytes());
                    replay_hasher.update(parsed.header.typ.as_bytes());
                }
                Err(err) => {
                    issues.push(format!(
                        "invalid assertion cbor for assertion {}: {err}",
                        assertion_id.to_hex()
                    ));
                }
            }

            replay_hasher.update(b"assertion");
            replay_hasher.update(subject.as_bytes());
            replay_hasher.update(&seq.to_le_bytes());
            replay_hasher.update(assertion_id.as_bytes());
            replay_hasher.update(envelope_id.as_bytes());
            replay_hasher.update(blake3::hash(&assertion_bytes).as_bytes());

            if let Some(prev_id) = prev {
                replay_hasher.update(b"prev");
                replay_hasher.update(prev_id.as_bytes());
            }
            for ref_id in &merge_refs {
                replay_hasher.update(b"merge_ref");
                replay_hasher.update(ref_id.as_bytes());
            }

            hash_lookup_tuple(
                &mut replay_hasher,
                backend,
                &assertion_id,
                &envelope_id,
                &mut issues,
            );

            assertion_rows.push(AssertionDigestRow {
                subject: *subject,
                seq,
                ver,
                assertion_id,
                envelope_id,
                prev,
                merge_refs,
                bytes_hash: *blake3::hash(&assertion_bytes).as_bytes(),
            });
        }
    }

    assertion_rows.sort_by(|a, b| {
        a.subject
            .as_bytes()
            .cmp(b.subject.as_bytes())
            .then_with(|| a.seq.cmp(&b.seq))
            .then_with(|| a.assertion_id.as_bytes().cmp(b.assertion_id.as_bytes()))
    });

    for row in &assertion_rows {
        frontier_state.apply(
            row.subject,
            row.ver,
            row.seq,
            row.assertion_id,
            row.prev,
            row.merge_refs.as_slice(),
        );
        replay_hasher.update(b"row");
        replay_hasher.update(row.subject.as_bytes());
        replay_hasher.update(&row.ver.to_le_bytes());
        replay_hasher.update(&row.seq.to_le_bytes());
        replay_hasher.update(row.assertion_id.as_bytes());
        replay_hasher.update(row.envelope_id.as_bytes());
        replay_hasher.update(&row.bytes_hash);
        if let Some(prev_id) = row.prev {
            replay_hasher.update(prev_id.as_bytes());
        }
    }

    for envelope_id in &objects {
        replay_hasher.update(b"object");
        replay_hasher.update(envelope_id.as_bytes());
        match backend.get_object_any(envelope_id) {
            Ok(Some(bytes)) => {
                replay_hasher.update(blake3::hash(&bytes).as_bytes());
            }
            Ok(None) => issues.push(format!("object missing bytes for {}", envelope_id.to_hex())),
            Err(err) => issues.push(format!(
                "get_object_any failed for {}: {err}",
                envelope_id.to_hex()
            )),
        }
    }

    Ok(BackendSnapshot {
        backend: name.to_string(),
        subjects: subjects.len(),
        assertions: assertion_count,
        objects: objects.len(),
        replay_hash_hex: hex::encode(replay_hasher.finalize().as_bytes()),
        frontier_hash_hex: frontier_state.digest_hex(),
        issues,
    })
}

fn hash_lookup_tuple(
    hasher: &mut blake3::Hasher,
    backend: &dyn StorageSpi,
    assertion_id: &AssertionId,
    envelope_id: &EnvelopeId,
    issues: &mut Vec<String>,
) {
    match backend.lookup_cqrs_by_assertion(assertion_id) {
        Ok(Some(entry)) => hash_cqrs_entry(hasher, b"cqrs_by_assertion", &entry),
        Ok(None) => {
            hasher.update(b"cqrs_by_assertion_none");
        }
        Err(err) => issues.push(format!(
            "lookup_cqrs_by_assertion failed for {}: {err}",
            assertion_id.to_hex()
        )),
    }

    match backend.lookup_cqrs_by_envelope(envelope_id) {
        Ok(Some(entry)) => hash_cqrs_entry(hasher, b"cqrs_by_envelope", &entry),
        Ok(None) => {
            hasher.update(b"cqrs_by_envelope_none");
        }
        Err(err) => issues.push(format!(
            "lookup_cqrs_by_envelope failed for {}: {err}",
            envelope_id.to_hex()
        )),
    }
}

fn hash_cqrs_entry(hasher: &mut blake3::Hasher, tag: &[u8], entry: &CqrsReverseEntry) {
    hasher.update(tag);
    hasher.update(entry.envelope_id.as_bytes());
    hasher.update(entry.assertion_id.as_bytes());
    hasher.update(entry.subject.as_bytes());
    hasher.update(&[u8::from(entry.is_overlay)]);
}

fn validate_sqlite_backend(
    data_root: &Path,
    baseline: &BackendSnapshot,
) -> Result<BackendValidation, DharmaError> {
    let mut report = base_validation("sqlite", baseline);

    let db_path = data_root.join("indexes").join("embedded.sqlite");
    if !db_path.exists() {
        report
            .issues
            .push(format!("sqlite file missing: {}", db_path.display()));
        return Ok(report);
    }

    let conn = match Connection::open(&db_path) {
        Ok(conn) => conn,
        Err(err) => {
            report.issues.push(format!(
                "failed opening sqlite db {}: {err}",
                db_path.display()
            ));
            return Ok(report);
        }
    };

    let tables = sqlite_master_names(&conn, "table")?;
    let indexes = sqlite_master_names(&conn, "index")?;

    for table in [
        "objects",
        "semantic_index",
        "cqrs_reverse",
        "subject_assertions",
        "permission_summaries",
    ] {
        if !tables.contains(table) {
            report.issues.push(format!("missing table `{table}`"));
        }
    }

    for index in [
        "idx_semantic_assertion",
        "idx_cqrs_envelope",
        "idx_cqrs_assertion",
        "idx_subject_assertions_subject_seq",
    ] {
        if !indexes.contains(index) {
            report.issues.push(format!("missing index `{index}`"));
        }
    }

    Ok(report)
}

fn validate_postgres_backend(
    config: &Config,
    baseline: &BackendSnapshot,
) -> Result<BackendValidation, DharmaError> {
    let mut report = base_validation("postgres", baseline);

    let url = config.storage.postgres.url.trim();
    if url.is_empty() {
        report
            .issues
            .push("storage.postgres.url must not be empty".to_string());
        return Ok(report);
    }

    let schema = normalize_postgres_schema(&config.storage.postgres.schema);
    let mut client = match PostgresClient::connect(url, NoTls) {
        Ok(client) => client,
        Err(err) => {
            report
                .issues
                .push(format!("postgres connection failed: {err}"));
            return Ok(report);
        }
    };

    let table_rows = match client.query(
        "SELECT tablename FROM pg_tables WHERE schemaname = $1",
        &[&schema],
    ) {
        Ok(rows) => rows,
        Err(err) => {
            report.issues.push(format!(
                "failed listing postgres tables in schema `{schema}`: {err}"
            ));
            Vec::new()
        }
    };
    let table_names: HashSet<String> = table_rows.iter().map(|row| row.get(0)).collect();

    for table in [
        "schema_migrations",
        "objects",
        "semantic_index",
        "cqrs_reverse",
        "subject_assertions",
        "permission_summaries",
    ] {
        if !table_names.contains(table) {
            report.issues.push(format!("missing table `{table}`"));
        }
    }

    let index_rows = match client.query(
        "SELECT indexname FROM pg_indexes WHERE schemaname = $1",
        &[&schema],
    ) {
        Ok(rows) => rows,
        Err(err) => {
            report.issues.push(format!(
                "failed listing postgres indexes in schema `{schema}`: {err}"
            ));
            Vec::new()
        }
    };
    let index_names: HashSet<String> = index_rows.iter().map(|row| row.get(0)).collect();

    for index in [
        "idx_semantic_assertion",
        "idx_cqrs_envelope",
        "idx_cqrs_assertion",
        "idx_subject_assertions_subject_seq",
    ] {
        if !index_names.contains(index) {
            report.issues.push(format!("missing index `{index}`"));
        }
    }

    let migration_query = format!(
        "SELECT COUNT(*) FROM {}.schema_migrations WHERE id = $1",
        quote_postgres_ident(&schema)
    );
    match client.query_one(&migration_query, &[&POSTGRES_INIT_MIGRATION_ID]) {
        Ok(row) => {
            let count: i64 = row.get(0);
            if count != 1 {
                report.issues.push(format!(
                    "schema_migrations missing `{POSTGRES_INIT_MIGRATION_ID}`"
                ));
            }
        }
        Err(err) => report
            .issues
            .push(format!("failed validating schema_migrations entry: {err}")),
    }

    Ok(report)
}

fn validate_clickhouse_backend(
    config: &Config,
    baseline: &BackendSnapshot,
) -> Result<BackendValidation, DharmaError> {
    let mut report = base_validation("clickhouse", baseline);

    if !config.storage.clickhouse.enabled {
        report
            .issues
            .push("storage.clickhouse.enabled is false".to_string());
        return Ok(report);
    }

    let url = config.storage.clickhouse.url.trim();
    if url.is_empty() {
        report
            .issues
            .push("storage.clickhouse.url must not be empty".to_string());
        return Ok(report);
    }

    let database = normalize_clickhouse_identifier(&config.storage.clickhouse.database, "default");
    let table_prefix = normalize_clickhouse_identifier(
        &config.storage.clickhouse.table_prefix,
        "dharma_analytics",
    );

    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| DharmaError::Config(format!("unable to build clickhouse runtime: {err}")))?;

    let clickhouse = ClickHouseClient::default()
        .with_url(url.to_string())
        .with_database(database.clone());

    let required_tables = [
        format!("{table_prefix}_subject_assertions"),
        format!("{table_prefix}_semantic_index"),
        format!("{table_prefix}_cqrs_reverse"),
        format!("{table_prefix}_watermark"),
    ];

    let mut existing_tables = HashSet::new();
    for table in &required_tables {
        let sql = format!(
            "SELECT count() AS count FROM system.tables WHERE database = '{}' AND name = '{}'",
            escape_clickhouse_literal(&database),
            escape_clickhouse_literal(table),
        );
        let count_rows = runtime.block_on(async {
            clickhouse
                .query(&sql)
                .fetch_all::<ClickHouseCountRow>()
                .await
        });
        match count_rows {
            Ok(rows) => {
                let count = rows.first().map(|row| row.count).unwrap_or(0);
                if count == 0 {
                    report.issues.push(format!("missing table `{table}`"));
                } else {
                    existing_tables.insert(table.clone());
                }
            }
            Err(err) => report.issues.push(format!(
                "failed checking table `{table}` in `{database}`: {err}"
            )),
        }
    }

    let watermark_table = format!("{table_prefix}_watermark");
    if existing_tables.contains(&watermark_table) {
        let sql = format!(
            "SELECT watermark_seq, committed_seq FROM `{}` ORDER BY updated_ms DESC LIMIT 1",
            watermark_table
        );
        let rows = runtime.block_on(async {
            clickhouse
                .query(&sql)
                .fetch_all::<ClickHouseWatermarkRow>()
                .await
        });
        match rows {
            Ok(rows) => {
                if let Some(row) = rows.first() {
                    if row.watermark_seq > row.committed_seq {
                        report.issues.push(format!(
                            "watermark invariant violated: watermark_seq={} > committed_seq={}",
                            row.watermark_seq, row.committed_seq
                        ));
                    }
                } else {
                    report
                        .issues
                        .push("watermark row missing (id=1)".to_string());
                }
            }
            Err(err) => report.issues.push(format!(
                "failed reading watermark table `{watermark_table}`: {err}"
            )),
        }
    }

    Ok(report)
}

fn sqlite_master_names(conn: &Connection, kind: &str) -> Result<HashSet<String>, DharmaError> {
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type = ?1")?;
    let rows = stmt
        .query_map([kind], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows.into_iter().collect())
}

fn base_validation(backend: &str, baseline: &BackendSnapshot) -> BackendValidation {
    BackendValidation {
        backend: backend.to_string(),
        subjects: baseline.subjects,
        assertions: baseline.assertions,
        objects: baseline.objects,
        replay_hash_hex: baseline.replay_hash_hex.clone(),
        frontier_hash_hex: baseline.frontier_hash_hex.clone(),
        issues: Vec::new(),
    }
}

fn normalize_postgres_schema(schema: &str) -> String {
    let trimmed = schema.trim();
    if trimmed.is_empty() {
        "public".to_string()
    } else {
        trimmed.to_string()
    }
}

fn quote_postgres_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn normalize_clickhouse_identifier(value: &str, default: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return default.to_string();
    }
    let mut out = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        default.to_string()
    } else {
        out
    }
}

fn escape_clickhouse_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::crypto;
    use crate::keys::Keyring;
    use crate::store::spi::{StorageCommit, StorageIndex, StorageQuery, StorageRead};
    use crate::types::{ContractId, IdentityKey, SchemaId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[derive(Default)]
    struct MockBackend {
        subjects: Vec<SubjectId>,
        assertions: BTreeMap<SubjectId, Vec<AssertionId>>,
        assertion_env: BTreeMap<AssertionId, EnvelopeId>,
        assertion_bytes: BTreeMap<(SubjectId, EnvelopeId), Vec<u8>>,
        objects: Vec<EnvelopeId>,
        object_bytes: BTreeMap<EnvelopeId, Vec<u8>>,
        cqrs_assertion: BTreeMap<AssertionId, CqrsReverseEntry>,
        cqrs_envelope: BTreeMap<EnvelopeId, CqrsReverseEntry>,
    }

    impl StorageCommit for MockBackend {
        fn put_object_if_absent(
            &self,
            _envelope_id: &EnvelopeId,
            _bytes: &[u8],
        ) -> Result<(), DharmaError> {
            Ok(())
        }

        fn put_assertion(
            &self,
            _subject: &SubjectId,
            _envelope_id: &EnvelopeId,
            _bytes: &[u8],
        ) -> Result<(), DharmaError> {
            Ok(())
        }

        fn put_permission_summary(
            &self,
            _summary: &crate::contract::PermissionSummary,
        ) -> Result<(), DharmaError> {
            Ok(())
        }
    }

    impl StorageIndex for MockBackend {
        fn rebuild_subject_views(&self, _keys: &Keyring) -> Result<(), DharmaError> {
            Ok(())
        }

        fn record_semantic(
            &self,
            _assertion_id: &AssertionId,
            _envelope_id: &EnvelopeId,
        ) -> Result<(), DharmaError> {
            Ok(())
        }
    }

    impl StorageRead for MockBackend {
        fn get_object(&self, envelope_id: &EnvelopeId) -> Result<Vec<u8>, DharmaError> {
            self.get_object_any(envelope_id)?
                .ok_or_else(|| DharmaError::NotFound(envelope_id.to_hex()))
        }

        fn get_object_any(&self, envelope_id: &EnvelopeId) -> Result<Option<Vec<u8>>, DharmaError> {
            Ok(self.object_bytes.get(envelope_id).cloned())
        }

        fn get_assertion(
            &self,
            subject: &SubjectId,
            envelope_id: &EnvelopeId,
        ) -> Result<Vec<u8>, DharmaError> {
            self.assertion_bytes
                .get(&(*subject, *envelope_id))
                .cloned()
                .ok_or_else(|| DharmaError::NotFound(envelope_id.to_hex()))
        }

        fn scan_subject(&self, subject: &SubjectId) -> Result<Vec<AssertionId>, DharmaError> {
            Ok(self.assertions.get(subject).cloned().unwrap_or_default())
        }

        fn list_subjects(&self) -> Result<Vec<SubjectId>, DharmaError> {
            Ok(self.subjects.clone())
        }

        fn list_objects(&self) -> Result<Vec<EnvelopeId>, DharmaError> {
            Ok(self.objects.clone())
        }

        fn get_permission_summary(
            &self,
            _contract: &ContractId,
        ) -> Result<Option<crate::contract::PermissionSummary>, DharmaError> {
            Ok(None)
        }
    }

    impl StorageQuery for MockBackend {
        fn lookup_envelope(
            &self,
            assertion_id: &AssertionId,
        ) -> Result<Option<EnvelopeId>, DharmaError> {
            Ok(self.assertion_env.get(assertion_id).copied())
        }

        fn lookup_cqrs_by_envelope(
            &self,
            envelope_id: &EnvelopeId,
        ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
            Ok(self.cqrs_envelope.get(envelope_id).copied())
        }

        fn lookup_cqrs_by_assertion(
            &self,
            assertion_id: &AssertionId,
        ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
            Ok(self.cqrs_assertion.get(assertion_id).copied())
        }
    }

    fn signed_assertion(
        seed: u64,
        subject: SubjectId,
        seq: u64,
        typ: &str,
        prev: Option<AssertionId>,
        refs: Vec<AssertionId>,
    ) -> (AssertionId, EnvelopeId, Vec<u8>) {
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
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
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

    fn backend_from_records(subject_order: Vec<SubjectId>, reverse_scan: bool) -> MockBackend {
        let mut backend = MockBackend::default();

        let subject_a = SubjectId::from_bytes([7u8; 32]);
        let subject_b = SubjectId::from_bytes([8u8; 32]);

        let (a1, a1_env, a1_bytes) = signed_assertion(1, subject_a, 1, "note.text", None, vec![]);
        let (a2, a2_env, a2_bytes) =
            signed_assertion(2, subject_a, 2, "note.text", Some(a1), vec![]);
        let (b1, b1_env, b1_bytes) = signed_assertion(3, subject_b, 1, "note.text", None, vec![]);
        let (b2, b2_env, b2_bytes) =
            signed_assertion(4, subject_b, 2, "note.text", Some(b1), vec![]);
        let (b3, b3_env, b3_bytes) =
            signed_assertion(5, subject_b, 2, "note.text", Some(b1), vec![]);
        let (merge, merge_env, merge_bytes) =
            signed_assertion(6, subject_b, 3, "core.merge", Some(b2), vec![b2, b3]);

        backend.subjects = subject_order;

        let mut subject_a_scan = vec![a1, a2];
        let mut subject_b_scan = vec![b1, b2, b3, merge];
        if reverse_scan {
            subject_a_scan.reverse();
            subject_b_scan.reverse();
        }

        backend.assertions.insert(subject_a, subject_a_scan);
        backend.assertions.insert(subject_b, subject_b_scan);

        for (subject, assertion_id, envelope_id, bytes, is_overlay) in [
            (subject_a, a1, a1_env, a1_bytes, false),
            (subject_a, a2, a2_env, a2_bytes, false),
            (subject_b, b1, b1_env, b1_bytes, false),
            (subject_b, b2, b2_env, b2_bytes, false),
            (subject_b, b3, b3_env, b3_bytes, false),
            (subject_b, merge, merge_env, merge_bytes, false),
        ] {
            backend.assertion_env.insert(assertion_id, envelope_id);
            backend
                .assertion_bytes
                .insert((subject, envelope_id), bytes.clone());
            backend.object_bytes.insert(envelope_id, bytes);
            let cqrs = CqrsReverseEntry {
                envelope_id,
                assertion_id,
                subject,
                is_overlay,
            };
            backend.cqrs_assertion.insert(assertion_id, cqrs);
            backend.cqrs_envelope.insert(envelope_id, cqrs);
        }

        backend.objects = vec![merge_env, b3_env, a2_env, a1_env, b1_env, b2_env];
        backend
    }

    #[test]
    fn deterministic_hash_is_stable_across_order_variants() {
        let backend_a = backend_from_records(
            vec![
                SubjectId::from_bytes([8u8; 32]),
                SubjectId::from_bytes([7u8; 32]),
            ],
            false,
        );
        let backend_b = backend_from_records(
            vec![
                SubjectId::from_bytes([7u8; 32]),
                SubjectId::from_bytes([8u8; 32]),
            ],
            true,
        );

        let report = compare_backends(&[("a", &backend_a), ("b", &backend_b)]).unwrap();
        assert!(
            report.issues.is_empty(),
            "unexpected issues: {:?}",
            report.issues
        );
        assert_eq!(
            report.snapshots[0].replay_hash_hex,
            report.snapshots[1].replay_hash_hex
        );
        assert_eq!(
            report.snapshots[0].frontier_hash_hex,
            report.snapshots[1].frontier_hash_hex
        );
    }

    #[test]
    fn mismatch_detection_flags_replay_hash_drift() {
        let backend_a = backend_from_records(
            vec![
                SubjectId::from_bytes([7u8; 32]),
                SubjectId::from_bytes([8u8; 32]),
            ],
            false,
        );
        let mut backend_b = backend_from_records(
            vec![
                SubjectId::from_bytes([7u8; 32]),
                SubjectId::from_bytes([8u8; 32]),
            ],
            false,
        );

        let first_env = *backend_b.objects.first().unwrap();
        backend_b
            .object_bytes
            .insert(first_env, vec![0xff, 0x00, 0x01, 0x02]);

        let report = compare_backends(&[("a", &backend_a), ("b", &backend_b)]).unwrap();
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("replay_hash_hex mismatch")),
            "expected replay hash mismatch, got {:?}",
            report.issues
        );
    }
}
