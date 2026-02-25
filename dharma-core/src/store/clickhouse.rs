use crate::config::Config;
use crate::contract::PermissionSummary;
use crate::error::DharmaError;
use crate::keys::Keyring;
use crate::metrics;
use crate::store::spi::{
    BackendErrorClass, BackendErrorTaxonomy, BackendKind, StorageCommit, StorageIndex,
    StorageOperation, StorageQuery, StorageRead,
};
use crate::store::state::{self, CqrsReverseEntry};
use crate::store::Store;
use crate::types::{AssertionId, ContractId, EnvelopeId, SubjectId};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::{Builder, Runtime};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnalyticsWatermark {
    pub watermark_seq: u64,
    pub committed_seq: u64,
    pub lag_ms: u64,
    pub updated_ms: u64,
}

#[derive(Clone)]
pub struct ClickHouseServerAnalyticsAdapter {
    legacy: Store,
    bootstrap_client: Client,
    analytics_client: Client,
    runtime: Arc<Runtime>,
    database: String,
    table_prefix: String,
    retry_max_attempts: u32,
    retry_backoff_ms: u64,
}

#[derive(Row, Serialize, Deserialize)]
struct SubjectAssertionRecord {
    subject_id: String,
    seq: u64,
    assertion_id: String,
    envelope_id: String,
    is_overlay: u8,
    inserted_at: u64,
}

#[derive(Row, Serialize, Deserialize)]
struct SemanticIndexRecord {
    assertion_id: String,
    envelope_id: String,
    inserted_at: u64,
}

#[derive(Row, Serialize, Deserialize)]
struct CqrsReverseRecord {
    envelope_id: String,
    assertion_id: String,
    subject_id: String,
    is_overlay: u8,
    inserted_at: u64,
}

#[derive(Row, Serialize, Deserialize)]
struct WatermarkRecord {
    id: u8,
    watermark_seq: u64,
    committed_seq: u64,
    updated_ms: u64,
}

#[derive(Row, Deserialize)]
struct AssertionIdOnly {
    assertion_id: String,
}

#[derive(Row, Deserialize)]
struct SubjectIdOnly {
    subject_id: String,
}

#[derive(Row, Deserialize)]
struct EnvelopeIdOnly {
    envelope_id: String,
}

#[derive(Row, Deserialize)]
struct CqrsLookupRow {
    envelope_id: String,
    assertion_id: String,
    subject_id: String,
    is_overlay: u8,
}

#[derive(Default)]
struct CanonicalProjection {
    subject_assertions: Vec<SubjectAssertionRecord>,
    semantic_rows: Vec<SemanticIndexRecord>,
    cqrs_rows: Vec<CqrsReverseRecord>,
    committed_seq: u64,
}

impl ClickHouseServerAnalyticsAdapter {
    pub fn open(_root: &Path, legacy: Store, config: &Config) -> Result<Self, DharmaError> {
        let clickhouse_cfg = &config.storage.clickhouse;
        if !clickhouse_cfg.enabled {
            return Err(DharmaError::Config(
                "storage.clickhouse.enabled is false".to_string(),
            ));
        }
        if clickhouse_cfg.url.trim().is_empty() {
            return Err(DharmaError::Config(
                "storage.clickhouse.url must not be empty".to_string(),
            ));
        }

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| {
                DharmaError::Config(format!("unable to build clickhouse runtime: {err}"))
            })?;

        let bootstrap_client = Client::default().with_url(clickhouse_cfg.url.clone());
        let analytics_client = bootstrap_client
            .clone()
            .with_database(clickhouse_cfg.database.clone());
        let adapter = Self {
            legacy,
            bootstrap_client,
            analytics_client,
            runtime: Arc::new(runtime),
            database: normalize_identifier(&clickhouse_cfg.database, "default"),
            table_prefix: normalize_identifier(&clickhouse_cfg.table_prefix, "dharma_analytics"),
            retry_max_attempts: clickhouse_cfg.retry_max_attempts.max(1),
            retry_backoff_ms: clickhouse_cfg.retry_backoff_ms,
        };
        adapter.ensure_schema_bootstrap()?;
        let _ = adapter.sync_from_canonical()?;
        Ok(adapter)
    }

    pub fn sync_from_canonical(&self) -> Result<AnalyticsWatermark, DharmaError> {
        self.with_retry(StorageOperation::Query, || {
            let committed_seq = self.compute_committed_seq()?;
            let current = self.current_watermark()?.unwrap_or(AnalyticsWatermark {
                watermark_seq: 0,
                committed_seq: 0,
                lag_ms: committed_seq.saturating_mul(1000),
                updated_ms: now_millis(),
            });
            metrics::analytics_committed_seq_set(committed_seq);

            if current.watermark_seq >= committed_seq {
                let lag_ms = current
                    .committed_seq
                    .saturating_sub(current.watermark_seq)
                    .saturating_mul(1000);
                metrics::analytics_watermark_seq_set(current.watermark_seq);
                metrics::analytics_lag_ms_set(lag_ms);
                return Ok(AnalyticsWatermark {
                    lag_ms,
                    committed_seq,
                    ..current
                });
            }

            self.rebuild_from_canonical()
        })
    }

    pub fn rebuild_from_canonical(&self) -> Result<AnalyticsWatermark, DharmaError> {
        self.with_retry(StorageOperation::Index, || {
            let projection = self.build_projection_from_canonical()?;
            let CanonicalProjection {
                subject_assertions,
                semantic_rows,
                cqrs_rows,
                committed_seq,
            } = projection;
            self.truncate_analytics_tables()?;
            self.insert_subject_rows(subject_assertions)?;
            self.insert_semantic_rows(semantic_rows)?;
            self.insert_cqrs_rows(cqrs_rows)?;
            let updated_ms = now_millis();
            self.upsert_watermark(committed_seq, committed_seq, updated_ms)?;

            metrics::analytics_watermark_seq_set(committed_seq);
            metrics::analytics_committed_seq_set(committed_seq);
            metrics::analytics_lag_ms_set(0);

            Ok(AnalyticsWatermark {
                watermark_seq: committed_seq,
                committed_seq,
                lag_ms: 0,
                updated_ms,
            })
        })
    }

    pub fn scan_subject_analytics(
        &self,
        subject: &SubjectId,
    ) -> Result<Vec<AssertionId>, DharmaError> {
        self.sync_from_canonical()?;
        let cached = self.scan_subject_clickhouse(subject)?;
        let legacy = self.legacy.scan_subject(subject)?;
        if cached != legacy {
            self.rebuild_from_canonical()?;
            let rebuilt = self.scan_subject_clickhouse(subject)?;
            if rebuilt != legacy {
                return Err(DharmaError::ClickHouse {
                    code: Some("projection_mismatch".to_string()),
                    message: format!(
                        "scan_subject analytics mismatch after rebuild for subject {:?}: clickhouse={} legacy={}",
                        subject,
                        rebuilt.len(),
                        legacy.len()
                    ),
                });
            }
            return Ok(rebuilt);
        }
        Ok(cached)
    }

    pub fn list_subjects_analytics(&self) -> Result<Vec<SubjectId>, DharmaError> {
        self.sync_from_canonical()?;
        let cached = self.list_subjects_clickhouse()?;
        let legacy = self.legacy.list_subjects()?;
        let cached_set: HashSet<SubjectId> = cached.iter().copied().collect();
        let legacy_set: HashSet<SubjectId> = legacy.iter().copied().collect();
        if cached_set != legacy_set {
            self.rebuild_from_canonical()?;
            let rebuilt = self.list_subjects_clickhouse()?;
            let rebuilt_set: HashSet<SubjectId> = rebuilt.iter().copied().collect();
            if rebuilt_set != legacy_set {
                return Err(DharmaError::ClickHouse {
                    code: Some("projection_mismatch".to_string()),
                    message: format!(
                        "list_subjects analytics mismatch after rebuild: clickhouse={} legacy={}",
                        rebuilt_set.len(),
                        legacy_set.len()
                    ),
                });
            }
            return Ok(rebuilt);
        }
        Ok(cached)
    }

    fn subject_assertions_table(&self) -> String {
        format!("{}_subject_assertions", self.table_prefix)
    }

    fn semantic_index_table(&self) -> String {
        format!("{}_semantic_index", self.table_prefix)
    }

    fn cqrs_reverse_table(&self) -> String {
        format!("{}_cqrs_reverse", self.table_prefix)
    }

    fn watermark_table(&self) -> String {
        format!("{}_watermark", self.table_prefix)
    }

    fn ensure_schema_bootstrap(&self) -> Result<(), DharmaError> {
        let database = quote_ident(&self.database);
        self.exec_bootstrap(&format!("CREATE DATABASE IF NOT EXISTS {database}"))?;
        self.exec_analytics(&format!(
            "CREATE TABLE IF NOT EXISTS {} \
             (subject_id String, seq UInt64, assertion_id String, envelope_id String, is_overlay UInt8, inserted_at UInt64) \
             ENGINE = ReplacingMergeTree(inserted_at) \
             ORDER BY (subject_id, is_overlay, seq, assertion_id)",
            quote_ident(&self.subject_assertions_table())
        ))?;
        self.exec_analytics(&format!(
            "CREATE TABLE IF NOT EXISTS {} \
             (assertion_id String, envelope_id String, inserted_at UInt64) \
             ENGINE = ReplacingMergeTree(inserted_at) \
             ORDER BY (assertion_id, inserted_at)",
            quote_ident(&self.semantic_index_table())
        ))?;
        self.exec_analytics(&format!(
            "CREATE TABLE IF NOT EXISTS {} \
             (envelope_id String, assertion_id String, subject_id String, is_overlay UInt8, inserted_at UInt64) \
             ENGINE = ReplacingMergeTree(inserted_at) \
             ORDER BY (assertion_id, envelope_id, inserted_at)",
            quote_ident(&self.cqrs_reverse_table())
        ))?;
        self.exec_analytics(&format!(
            "CREATE TABLE IF NOT EXISTS {} \
             (id UInt8, watermark_seq UInt64, committed_seq UInt64, updated_ms UInt64) \
             ENGINE = ReplacingMergeTree(updated_ms) \
             ORDER BY (id)",
            quote_ident(&self.watermark_table())
        ))?;
        Ok(())
    }

    fn compute_committed_seq(&self) -> Result<u64, DharmaError> {
        let mut committed = 0u64;
        for subject in self.legacy.list_subjects()? {
            for record in state::list_assertions(self.legacy.env(), &subject)? {
                committed = committed.max(record.seq);
            }
            for record in state::list_overlays(self.legacy.env(), &subject)? {
                committed = committed.max(record.seq);
            }
        }
        Ok(committed)
    }

    fn build_projection_from_canonical(&self) -> Result<CanonicalProjection, DharmaError> {
        let mut projection = CanonicalProjection::default();
        let mut inserted_at = 0u64;

        for subject in self.legacy.list_subjects()? {
            let assertions = state::list_assertions(self.legacy.env(), &subject)?;
            let overlays = state::list_overlays(self.legacy.env(), &subject)?;

            for record in assertions {
                inserted_at = inserted_at.saturating_add(1);
                projection.committed_seq = projection.committed_seq.max(record.seq);
                let envelope = self
                    .legacy
                    .lookup_envelope(&record.assertion_id)?
                    .unwrap_or(record.envelope_id);
                projection.subject_assertions.push(SubjectAssertionRecord {
                    subject_id: subject.to_hex(),
                    seq: record.seq,
                    assertion_id: record.assertion_id.to_hex(),
                    envelope_id: envelope.to_hex(),
                    is_overlay: 0,
                    inserted_at,
                });
                projection.semantic_rows.push(SemanticIndexRecord {
                    assertion_id: record.assertion_id.to_hex(),
                    envelope_id: envelope.to_hex(),
                    inserted_at,
                });
            }

            for record in overlays {
                inserted_at = inserted_at.saturating_add(1);
                projection.committed_seq = projection.committed_seq.max(record.seq);
                let envelope = self
                    .legacy
                    .lookup_envelope(&record.assertion_id)?
                    .unwrap_or(record.envelope_id);
                projection.subject_assertions.push(SubjectAssertionRecord {
                    subject_id: subject.to_hex(),
                    seq: record.seq,
                    assertion_id: record.assertion_id.to_hex(),
                    envelope_id: envelope.to_hex(),
                    is_overlay: 1,
                    inserted_at,
                });
                projection.semantic_rows.push(SemanticIndexRecord {
                    assertion_id: record.assertion_id.to_hex(),
                    envelope_id: envelope.to_hex(),
                    inserted_at,
                });
            }
        }

        let mut reverse_entries = state::read_cqrs_reverse_entries(self.legacy.env())?;
        if reverse_entries.is_empty() {
            for row in &projection.subject_assertions {
                reverse_entries.push(CqrsReverseEntry {
                    envelope_id: EnvelopeId::from_hex(&row.envelope_id)?,
                    assertion_id: AssertionId::from_hex(&row.assertion_id)?,
                    subject: SubjectId::from_hex(&row.subject_id)?,
                    is_overlay: row.is_overlay == 1,
                });
            }
        }

        for entry in reverse_entries {
            inserted_at = inserted_at.saturating_add(1);
            projection.cqrs_rows.push(CqrsReverseRecord {
                envelope_id: entry.envelope_id.to_hex(),
                assertion_id: entry.assertion_id.to_hex(),
                subject_id: entry.subject.to_hex(),
                is_overlay: if entry.is_overlay { 1 } else { 0 },
                inserted_at,
            });
        }

        Ok(projection)
    }

    fn current_watermark(&self) -> Result<Option<AnalyticsWatermark>, DharmaError> {
        let sql = format!(
            "SELECT id, watermark_seq, committed_seq, updated_ms FROM {} \
             WHERE id = 1 ORDER BY updated_ms DESC LIMIT 1",
            quote_ident(&self.watermark_table())
        );
        let rows: Vec<WatermarkRecord> = self.query_rows(sql)?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        let lag_ms = row
            .committed_seq
            .saturating_sub(row.watermark_seq)
            .saturating_mul(1000);
        Ok(Some(AnalyticsWatermark {
            watermark_seq: row.watermark_seq,
            committed_seq: row.committed_seq,
            lag_ms,
            updated_ms: row.updated_ms,
        }))
    }

    fn upsert_watermark(
        &self,
        watermark_seq: u64,
        committed_seq: u64,
        updated_ms: u64,
    ) -> Result<(), DharmaError> {
        self.insert_rows(
            &self.watermark_table(),
            vec![WatermarkRecord {
                id: 1,
                watermark_seq,
                committed_seq,
                updated_ms,
            }],
        )
    }

    fn truncate_analytics_tables(&self) -> Result<(), DharmaError> {
        self.exec_analytics(&format!(
            "TRUNCATE TABLE {}",
            quote_ident(&self.subject_assertions_table())
        ))?;
        self.exec_analytics(&format!(
            "TRUNCATE TABLE {}",
            quote_ident(&self.semantic_index_table())
        ))?;
        self.exec_analytics(&format!(
            "TRUNCATE TABLE {}",
            quote_ident(&self.cqrs_reverse_table())
        ))?;
        self.exec_analytics(&format!(
            "TRUNCATE TABLE {}",
            quote_ident(&self.watermark_table())
        ))?;
        Ok(())
    }

    fn insert_subject_rows(&self, rows: Vec<SubjectAssertionRecord>) -> Result<(), DharmaError> {
        self.insert_rows(&self.subject_assertions_table(), rows)
    }

    fn insert_semantic_rows(&self, rows: Vec<SemanticIndexRecord>) -> Result<(), DharmaError> {
        self.insert_rows(&self.semantic_index_table(), rows)
    }

    fn insert_cqrs_rows(&self, rows: Vec<CqrsReverseRecord>) -> Result<(), DharmaError> {
        self.insert_rows(&self.cqrs_reverse_table(), rows)
    }

    fn insert_rows<T>(&self, table: &str, rows: Vec<T>) -> Result<(), DharmaError>
    where
        T: Row + Serialize + Send + Sync + 'static,
    {
        if rows.is_empty() {
            return Ok(());
        }
        self.run_async(async {
            let mut insert = self.analytics_client.insert(table)?;
            for row in rows {
                insert.write(&row).await?;
            }
            insert.end().await?;
            Ok(())
        })
    }

    fn scan_subject_clickhouse(
        &self,
        subject: &SubjectId,
    ) -> Result<Vec<AssertionId>, DharmaError> {
        let sql = format!(
            "SELECT assertion_id FROM {} \
             WHERE subject_id = '{}' AND is_overlay = 0 \
             ORDER BY seq ASC, assertion_id ASC",
            quote_ident(&self.subject_assertions_table()),
            subject.to_hex()
        );
        let rows: Vec<AssertionIdOnly> = self.query_rows(sql)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(AssertionId::from_hex(&row.assertion_id)?);
        }
        Ok(out)
    }

    fn list_subjects_clickhouse(&self) -> Result<Vec<SubjectId>, DharmaError> {
        let sql = format!(
            "SELECT DISTINCT subject_id FROM {} ORDER BY subject_id ASC",
            quote_ident(&self.subject_assertions_table())
        );
        let rows: Vec<SubjectIdOnly> = self.query_rows(sql)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(SubjectId::from_hex(&row.subject_id)?);
        }
        Ok(out)
    }

    fn lookup_envelope_clickhouse(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<EnvelopeId>, DharmaError> {
        let sql = format!(
            "SELECT envelope_id FROM {} \
             WHERE assertion_id = '{}' \
             ORDER BY inserted_at DESC LIMIT 1",
            quote_ident(&self.semantic_index_table()),
            assertion_id.to_hex()
        );
        let rows: Vec<EnvelopeIdOnly> = self.query_rows(sql)?;
        match rows.into_iter().next() {
            Some(row) => Ok(Some(EnvelopeId::from_hex(&row.envelope_id)?)),
            None => Ok(None),
        }
    }

    fn lookup_cqrs_by_envelope_clickhouse(
        &self,
        envelope_id: &EnvelopeId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        let sql = format!(
            "SELECT envelope_id, assertion_id, subject_id, is_overlay FROM {} \
             WHERE envelope_id = '{}' \
             ORDER BY inserted_at DESC LIMIT 1",
            quote_ident(&self.cqrs_reverse_table()),
            envelope_id.to_hex()
        );
        let rows: Vec<CqrsLookupRow> = self.query_rows(sql)?;
        rows.into_iter()
            .next()
            .map_or(Ok(None), decode_cqrs_lookup_row)
    }

    fn lookup_cqrs_by_assertion_clickhouse(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        let sql = format!(
            "SELECT envelope_id, assertion_id, subject_id, is_overlay FROM {} \
             WHERE assertion_id = '{}' \
             ORDER BY inserted_at DESC LIMIT 1",
            quote_ident(&self.cqrs_reverse_table()),
            assertion_id.to_hex()
        );
        let rows: Vec<CqrsLookupRow> = self.query_rows(sql)?;
        rows.into_iter()
            .next()
            .map_or(Ok(None), decode_cqrs_lookup_row)
    }

    fn query_rows<T>(&self, sql: String) -> Result<Vec<T>, DharmaError>
    where
        T: Row + for<'de> Deserialize<'de> + Send + 'static,
    {
        self.run_async(async {
            self.analytics_client
                .query(&sql)
                .fetch_all::<T>()
                .await
                .map_err(DharmaError::from)
        })
    }

    fn exec_bootstrap(&self, sql: &str) -> Result<(), DharmaError> {
        self.run_async(async {
            self.bootstrap_client
                .query(sql)
                .execute()
                .await
                .map_err(DharmaError::from)
        })
    }

    fn exec_analytics(&self, sql: &str) -> Result<(), DharmaError> {
        self.run_async(async {
            self.analytics_client
                .query(sql)
                .execute()
                .await
                .map_err(DharmaError::from)
        })
    }

    fn run_async<T>(
        &self,
        fut: impl Future<Output = Result<T, DharmaError>>,
    ) -> Result<T, DharmaError> {
        self.runtime.block_on(fut)
    }

    fn with_retry<T>(
        &self,
        operation: StorageOperation,
        mut op: impl FnMut() -> Result<T, DharmaError>,
    ) -> Result<T, DharmaError> {
        let max_attempts = self.retry_max_attempts.max(1);
        let mut attempt = 0;
        loop {
            attempt += 1;
            match op() {
                Ok(value) => return Ok(value),
                Err(err) => {
                    let taxonomy =
                        BackendErrorTaxonomy::classify(BackendKind::ClickHouse, operation, &err);
                    if taxonomy.class == BackendErrorClass::Retryable && attempt < max_attempts {
                        let sleep_ms = self.retry_backoff_ms.saturating_mul(u64::from(attempt));
                        if sleep_ms > 0 {
                            thread::sleep(Duration::from_millis(sleep_ms));
                        }
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }
}

impl StorageCommit for ClickHouseServerAnalyticsAdapter {
    fn put_object_if_absent(
        &self,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        self.legacy.put_object_if_absent(envelope_id, bytes)?;
        let _ = self.sync_from_canonical()?;
        Ok(())
    }

    fn put_assertion(
        &self,
        subject: &SubjectId,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        self.legacy.put_assertion(subject, envelope_id, bytes)?;
        let _ = self.sync_from_canonical()?;
        Ok(())
    }

    fn put_permission_summary(&self, summary: &PermissionSummary) -> Result<(), DharmaError> {
        self.legacy.put_permission_summary(summary)
    }
}

impl StorageRead for ClickHouseServerAnalyticsAdapter {
    fn get_object(&self, envelope_id: &EnvelopeId) -> Result<Vec<u8>, DharmaError> {
        self.legacy.get_object(envelope_id)
    }

    fn get_object_any(&self, envelope_id: &EnvelopeId) -> Result<Option<Vec<u8>>, DharmaError> {
        self.legacy.get_object_any(envelope_id)
    }

    fn get_assertion(
        &self,
        subject: &SubjectId,
        envelope_id: &EnvelopeId,
    ) -> Result<Vec<u8>, DharmaError> {
        self.legacy.get_assertion(subject, envelope_id)
    }

    fn scan_subject(&self, subject: &SubjectId) -> Result<Vec<AssertionId>, DharmaError> {
        self.scan_subject_analytics(subject)
    }

    fn list_subjects(&self) -> Result<Vec<SubjectId>, DharmaError> {
        self.list_subjects_analytics()
    }

    fn list_objects(&self) -> Result<Vec<EnvelopeId>, DharmaError> {
        self.legacy.list_objects()
    }

    fn get_permission_summary(
        &self,
        contract: &ContractId,
    ) -> Result<Option<PermissionSummary>, DharmaError> {
        self.legacy.get_permission_summary(contract)
    }
}

impl StorageIndex for ClickHouseServerAnalyticsAdapter {
    fn rebuild_subject_views(&self, keys: &Keyring) -> Result<(), DharmaError> {
        self.legacy.rebuild_subject_views(keys)?;
        let _ = self.rebuild_from_canonical()?;
        Ok(())
    }

    fn record_semantic(
        &self,
        assertion_id: &AssertionId,
        envelope_id: &EnvelopeId,
    ) -> Result<(), DharmaError> {
        self.legacy.record_semantic(assertion_id, envelope_id)?;
        let _ = self.sync_from_canonical()?;
        Ok(())
    }
}

impl StorageQuery for ClickHouseServerAnalyticsAdapter {
    fn lookup_envelope(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<EnvelopeId>, DharmaError> {
        self.sync_from_canonical()?;
        let cached = self.lookup_envelope_clickhouse(assertion_id)?;
        let legacy = self.legacy.lookup_envelope(assertion_id)?;
        if cached != legacy {
            self.rebuild_from_canonical()?;
            let rebuilt = self.lookup_envelope_clickhouse(assertion_id)?;
            if rebuilt != legacy {
                return Err(DharmaError::ClickHouse {
                    code: Some("projection_mismatch".to_string()),
                    message: format!(
                        "lookup_envelope analytics mismatch after rebuild for assertion {:?}",
                        assertion_id
                    ),
                });
            }
            return Ok(rebuilt);
        }
        Ok(cached)
    }

    fn lookup_cqrs_by_envelope(
        &self,
        envelope_id: &EnvelopeId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        self.sync_from_canonical()?;
        let cached = self.lookup_cqrs_by_envelope_clickhouse(envelope_id)?;
        let legacy = self.legacy.lookup_cqrs_by_envelope(envelope_id)?;
        if cached != legacy {
            self.rebuild_from_canonical()?;
            let rebuilt = self.lookup_cqrs_by_envelope_clickhouse(envelope_id)?;
            if rebuilt != legacy {
                return Err(DharmaError::ClickHouse {
                    code: Some("projection_mismatch".to_string()),
                    message: format!(
                        "lookup_cqrs_by_envelope analytics mismatch after rebuild for envelope {:?}",
                        envelope_id
                    ),
                });
            }
            return Ok(rebuilt);
        }
        Ok(cached)
    }

    fn lookup_cqrs_by_assertion(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        self.sync_from_canonical()?;
        let cached = self.lookup_cqrs_by_assertion_clickhouse(assertion_id)?;
        let legacy = self.legacy.lookup_cqrs_by_assertion(assertion_id)?;
        if cached != legacy {
            self.rebuild_from_canonical()?;
            let rebuilt = self.lookup_cqrs_by_assertion_clickhouse(assertion_id)?;
            if rebuilt != legacy {
                return Err(DharmaError::ClickHouse {
                    code: Some("projection_mismatch".to_string()),
                    message: format!(
                        "lookup_cqrs_by_assertion analytics mismatch after rebuild for assertion {:?}",
                        assertion_id
                    ),
                });
            }
            return Ok(rebuilt);
        }
        Ok(cached)
    }
}

fn decode_cqrs_lookup_row(row: CqrsLookupRow) -> Result<Option<CqrsReverseEntry>, DharmaError> {
    Ok(Some(CqrsReverseEntry {
        envelope_id: EnvelopeId::from_hex(&row.envelope_id)?,
        assertion_id: AssertionId::from_hex(&row.assertion_id)?,
        subject: SubjectId::from_hex(&row.subject_id)?,
        is_overlay: row.is_overlay != 0,
    }))
}

fn normalize_identifier(value: &str, default: &str) -> String {
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

fn quote_ident(value: &str) -> String {
    format!("`{}`", value.replace('`', ""))
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
