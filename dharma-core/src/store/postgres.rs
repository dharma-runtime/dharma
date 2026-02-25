use crate::assertion::{is_overlay, AssertionPlaintext};
use crate::config::Config;
use crate::contract::PermissionSummary;
use crate::error::DharmaError;
use crate::keys::Keyring;
use crate::store::spi::{
    BackendErrorClass, BackendErrorTaxonomy, BackendKind, StorageCommit, StorageIndex,
    StorageOperation, StorageQuery, StorageRead,
};
use crate::store::state::{self, CqrsReverseEntry};
use crate::store::Store;
use crate::types::{AssertionId, ContractId, EnvelopeId, SubjectId};
use postgres::NoTls;
use r2d2::{Pool, PooledConnection};
use r2d2_postgres::PostgresConnectionManager;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const INIT_MIGRATION_ID: &str = "0001_init";
const INIT_MIGRATION_SQL: &str = include_str!("postgres/migrations/0001_init.sql");

#[derive(Clone)]
pub struct PostgresServerAdapter {
    legacy: Store,
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    schema: String,
    statement_timeout_ms: u64,
    retry_max_attempts: u32,
    retry_backoff_ms: u64,
}

impl PostgresServerAdapter {
    pub fn open(_root: &Path, legacy: Store, config: &Config) -> Result<Self, DharmaError> {
        let postgres_cfg = &config.storage.postgres;
        let mut pg_config: postgres::Config = postgres_cfg
            .url
            .parse()
            .map_err(|err| DharmaError::Config(format!("invalid storage.postgres.url: {err}")))?;
        pg_config.connect_timeout(Duration::from_millis(postgres_cfg.connect_timeout_ms));

        let manager = PostgresConnectionManager::new(pg_config, NoTls);
        let pool = Pool::builder()
            .max_size(postgres_cfg.pool_max_size)
            .connection_timeout(Duration::from_millis(postgres_cfg.acquire_timeout_ms))
            .build(manager)?;

        let adapter = Self {
            legacy,
            pool: Arc::new(pool),
            schema: normalize_schema(&postgres_cfg.schema),
            statement_timeout_ms: postgres_cfg.statement_timeout_ms,
            retry_max_attempts: postgres_cfg.retry_max_attempts.max(1),
            retry_backoff_ms: postgres_cfg.retry_backoff_ms,
        };
        adapter.ensure_schema_migrated()?;
        Ok(adapter)
    }

    fn ensure_schema_migrated(&self) -> Result<(), DharmaError> {
        self.with_retry(StorageOperation::Commit, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;

            client.batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {};",
                quote_ident(&self.schema)
            ))?;
            client.batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS {}.schema_migrations (\
                    id TEXT PRIMARY KEY,\
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\
                )",
                quote_ident(&self.schema)
            ))?;

            let migration_exists = client
                .query_opt(
                    &format!(
                        "SELECT id FROM {}.schema_migrations WHERE id = $1",
                        quote_ident(&self.schema)
                    ),
                    &[&INIT_MIGRATION_ID],
                )?
                .is_some();
            if migration_exists {
                return Ok(());
            }

            let migration_sql =
                INIT_MIGRATION_SQL.replace("__DHARMA_SCHEMA__", quote_ident(&self.schema).as_str());

            let mut tx = client.transaction()?;
            tx.batch_execute(&format!(
                "SET LOCAL statement_timeout = {}",
                self.statement_timeout_ms
            ))?;
            tx.batch_execute(&migration_sql)?;
            tx.execute(
                &format!(
                    "INSERT INTO {}.schema_migrations(id) VALUES ($1) \
                     ON CONFLICT (id) DO NOTHING",
                    quote_ident(&self.schema)
                ),
                &[&INIT_MIGRATION_ID],
            )?;
            tx.commit()?;
            Ok(())
        })
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    fn table(&self, name: &str) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(name))
    }

    fn apply_statement_timeout(
        &self,
        client: &mut PooledConnection<PostgresConnectionManager<NoTls>>,
    ) -> Result<(), DharmaError> {
        client.batch_execute(&format!(
            "SET statement_timeout = {}",
            self.statement_timeout_ms
        ))?;
        Ok(())
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
                        BackendErrorTaxonomy::classify(BackendKind::Postgres, operation, &err);
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

    fn insert_object_postgres(
        &self,
        client: &mut PooledConnection<PostgresConnectionManager<NoTls>>,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        client.execute(
            &format!(
                "INSERT INTO {}(envelope_id, bytes) VALUES ($1, $2) \
                 ON CONFLICT (envelope_id) DO NOTHING",
                self.table("objects")
            ),
            &[&envelope_id.as_bytes().as_slice(), &bytes],
        )?;
        Ok(())
    }

    fn insert_semantic_postgres(
        &self,
        client: &mut PooledConnection<PostgresConnectionManager<NoTls>>,
        assertion_id: &AssertionId,
        envelope_id: &EnvelopeId,
    ) -> Result<(), DharmaError> {
        client.execute(
            &format!(
                "INSERT INTO {}(assertion_id, envelope_id) VALUES ($1, $2)",
                self.table("semantic_index")
            ),
            &[
                &assertion_id.as_bytes().as_slice(),
                &envelope_id.as_bytes().as_slice(),
            ],
        )?;
        Ok(())
    }

    fn insert_cqrs_postgres(
        &self,
        client: &mut PooledConnection<PostgresConnectionManager<NoTls>>,
        entry: &CqrsReverseEntry,
    ) -> Result<(), DharmaError> {
        client.execute(
            &format!(
                "INSERT INTO {}(envelope_id, assertion_id, subject_id, is_overlay) \
                 VALUES ($1, $2, $3, $4)",
                self.table("cqrs_reverse")
            ),
            &[
                &entry.envelope_id.as_bytes().as_slice(),
                &entry.assertion_id.as_bytes().as_slice(),
                &entry.subject.as_bytes().as_slice(),
                &entry.is_overlay,
            ],
        )?;
        Ok(())
    }

    fn upsert_permission_summary_postgres(
        &self,
        client: &mut PooledConnection<PostgresConnectionManager<NoTls>>,
        summary: &PermissionSummary,
    ) -> Result<(), DharmaError> {
        let bytes = summary.to_cbor()?;
        client.execute(
            &format!(
                "INSERT INTO {}(contract_id, bytes) VALUES ($1, $2) \
                 ON CONFLICT (contract_id) DO UPDATE SET bytes = EXCLUDED.bytes",
                self.table("permission_summaries")
            ),
            &[&summary.contract.as_bytes().as_slice(), &bytes],
        )?;
        Ok(())
    }

    fn load_object_postgres(
        &self,
        envelope_id: &EnvelopeId,
    ) -> Result<Option<Vec<u8>>, DharmaError> {
        self.with_retry(StorageOperation::Read, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;
            let row = client.query_opt(
                &format!(
                    "SELECT bytes FROM {} WHERE envelope_id = $1",
                    self.table("objects")
                ),
                &[&envelope_id.as_bytes().as_slice()],
            )?;
            Ok(row.map(|row| row.get::<_, Vec<u8>>(0)))
        })
    }

    fn load_subject_assertions_postgres(
        &self,
        subject: &SubjectId,
    ) -> Result<Vec<AssertionId>, DharmaError> {
        self.with_retry(StorageOperation::Read, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;
            let rows = client.query(
                &format!(
                    "SELECT assertion_id FROM {} \
                     WHERE subject_id = $1 AND is_overlay = FALSE \
                     ORDER BY seq ASC, assertion_id ASC",
                    self.table("subject_assertions")
                ),
                &[&subject.as_bytes().as_slice()],
            )?;
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let raw: Vec<u8> = row.get(0);
                out.push(AssertionId::from_slice(&raw)?);
            }
            Ok(out)
        })
    }

    fn backfill_subject_from_legacy(&self, subject: &SubjectId) -> Result<(), DharmaError> {
        let assertions = state::list_assertions(self.legacy.env(), subject)?;
        let overlays = state::list_overlays(self.legacy.env(), subject)?;
        self.with_retry(StorageOperation::Read, || {
            let mut client = self.pool.get()?;
            let mut tx = client.transaction()?;
            tx.batch_execute(&format!(
                "SET LOCAL statement_timeout = {}",
                self.statement_timeout_ms
            ))?;

            for record in &assertions {
                tx.execute(
                    &format!(
                        "INSERT INTO {}(envelope_id, bytes) VALUES ($1, $2) \
                         ON CONFLICT (envelope_id) DO NOTHING",
                        self.table("objects")
                    ),
                    &[&record.envelope_id.as_bytes().as_slice(), &&record.bytes[..]],
                )?;
                let seq_i64 = i64::try_from(record.seq)
                    .map_err(|_| DharmaError::Validation("assertion seq overflow".to_string()))?;
                tx.execute(
                    &format!(
                        "INSERT INTO {}(subject_id, seq, assertion_id, envelope_id, bytes, is_overlay) \
                         VALUES ($1, $2, $3, $4, $5, FALSE) \
                         ON CONFLICT (subject_id, is_overlay, seq, assertion_id) \
                         DO UPDATE SET envelope_id = EXCLUDED.envelope_id, bytes = EXCLUDED.bytes",
                        self.table("subject_assertions")
                    ),
                    &[
                        &subject.as_bytes().as_slice(),
                        &seq_i64,
                        &record.assertion_id.as_bytes().as_slice(),
                        &record.envelope_id.as_bytes().as_slice(),
                        &&record.bytes[..],
                    ],
                )?;
                tx.execute(
                    &format!(
                        "INSERT INTO {}(assertion_id, envelope_id) VALUES ($1, $2)",
                        self.table("semantic_index")
                    ),
                    &[
                        &record.assertion_id.as_bytes().as_slice(),
                        &record.envelope_id.as_bytes().as_slice(),
                    ],
                )?;
                tx.execute(
                    &format!(
                        "INSERT INTO {}(envelope_id, assertion_id, subject_id, is_overlay) \
                         VALUES ($1, $2, $3, FALSE)",
                        self.table("cqrs_reverse")
                    ),
                    &[
                        &record.envelope_id.as_bytes().as_slice(),
                        &record.assertion_id.as_bytes().as_slice(),
                        &subject.as_bytes().as_slice(),
                    ],
                )?;
            }

            for record in &overlays {
                tx.execute(
                    &format!(
                        "INSERT INTO {}(envelope_id, bytes) VALUES ($1, $2) \
                         ON CONFLICT (envelope_id) DO NOTHING",
                        self.table("objects")
                    ),
                    &[&record.envelope_id.as_bytes().as_slice(), &&record.bytes[..]],
                )?;
                let seq_i64 = i64::try_from(record.seq)
                    .map_err(|_| DharmaError::Validation("assertion seq overflow".to_string()))?;
                tx.execute(
                    &format!(
                        "INSERT INTO {}(subject_id, seq, assertion_id, envelope_id, bytes, is_overlay) \
                         VALUES ($1, $2, $3, $4, $5, TRUE) \
                         ON CONFLICT (subject_id, is_overlay, seq, assertion_id) \
                         DO UPDATE SET envelope_id = EXCLUDED.envelope_id, bytes = EXCLUDED.bytes",
                        self.table("subject_assertions")
                    ),
                    &[
                        &subject.as_bytes().as_slice(),
                        &seq_i64,
                        &record.assertion_id.as_bytes().as_slice(),
                        &record.envelope_id.as_bytes().as_slice(),
                        &&record.bytes[..],
                    ],
                )?;
                tx.execute(
                    &format!(
                        "INSERT INTO {}(assertion_id, envelope_id) VALUES ($1, $2)",
                        self.table("semantic_index")
                    ),
                    &[
                        &record.assertion_id.as_bytes().as_slice(),
                        &record.envelope_id.as_bytes().as_slice(),
                    ],
                )?;
                tx.execute(
                    &format!(
                        "INSERT INTO {}(envelope_id, assertion_id, subject_id, is_overlay) \
                         VALUES ($1, $2, $3, TRUE)",
                        self.table("cqrs_reverse")
                    ),
                    &[
                        &record.envelope_id.as_bytes().as_slice(),
                        &record.assertion_id.as_bytes().as_slice(),
                        &subject.as_bytes().as_slice(),
                    ],
                )?;
            }

            tx.commit()?;
            Ok(())
        })
    }

    fn backfill_cqrs_from_legacy(&self) -> Result<(), DharmaError> {
        let entries = state::read_cqrs_reverse_entries(self.legacy.env())?;
        self.with_retry(StorageOperation::Read, || {
            let mut client = self.pool.get()?;
            let mut tx = client.transaction()?;
            tx.batch_execute(&format!(
                "SET LOCAL statement_timeout = {}",
                self.statement_timeout_ms
            ))?;
            for entry in &entries {
                tx.execute(
                    &format!(
                        "INSERT INTO {}(envelope_id, assertion_id, subject_id, is_overlay) \
                         VALUES ($1, $2, $3, $4)",
                        self.table("cqrs_reverse")
                    ),
                    &[
                        &entry.envelope_id.as_bytes().as_slice(),
                        &entry.assertion_id.as_bytes().as_slice(),
                        &entry.subject.as_bytes().as_slice(),
                        &entry.is_overlay,
                    ],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
    }

    fn rebuild_indexes_from_legacy(&self) -> Result<(), DharmaError> {
        self.with_retry(StorageOperation::Index, || {
            let mut client = self.pool.get()?;
            let mut tx = client.transaction()?;
            tx.batch_execute(&format!(
                "SET LOCAL statement_timeout = {}",
                self.statement_timeout_ms
            ))?;
            tx.execute(
                &format!("DELETE FROM {}", self.table("semantic_index")),
                &[],
            )?;
            tx.execute(&format!("DELETE FROM {}", self.table("cqrs_reverse")), &[])?;
            tx.execute(
                &format!("DELETE FROM {}", self.table("subject_assertions")),
                &[],
            )?;
            tx.commit()?;
            Ok(())
        })?;

        let subjects = self.legacy.list_subjects()?;
        for subject in subjects {
            self.backfill_subject_from_legacy(&subject)?;
        }
        self.backfill_cqrs_from_legacy()?;
        Ok(())
    }

    fn lookup_envelope_postgres(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<EnvelopeId>, DharmaError> {
        self.with_retry(StorageOperation::Query, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;
            let row = client.query_opt(
                &format!(
                    "SELECT envelope_id FROM {} \
                     WHERE assertion_id = $1 \
                     ORDER BY inserted_at DESC \
                     LIMIT 1",
                    self.table("semantic_index")
                ),
                &[&assertion_id.as_bytes().as_slice()],
            )?;
            match row {
                Some(row) => {
                    let bytes: Vec<u8> = row.get(0);
                    Ok(Some(EnvelopeId::from_slice(&bytes)?))
                }
                None => Ok(None),
            }
        })
    }

    fn lookup_cqrs_by_envelope_postgres(
        &self,
        envelope_id: &EnvelopeId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        self.with_retry(StorageOperation::Query, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;
            let row = client.query_opt(
                &format!(
                    "SELECT envelope_id, assertion_id, subject_id, is_overlay \
                     FROM {} \
                     WHERE envelope_id = $1 \
                     ORDER BY inserted_at DESC \
                     LIMIT 1",
                    self.table("cqrs_reverse")
                ),
                &[&envelope_id.as_bytes().as_slice()],
            )?;
            match row {
                Some(row) => {
                    let env: Vec<u8> = row.get(0);
                    let assertion: Vec<u8> = row.get(1);
                    let subject: Vec<u8> = row.get(2);
                    let overlay: bool = row.get(3);
                    Ok(Some(CqrsReverseEntry {
                        envelope_id: EnvelopeId::from_slice(&env)?,
                        assertion_id: AssertionId::from_slice(&assertion)?,
                        subject: SubjectId::from_slice(&subject)?,
                        is_overlay: overlay,
                    }))
                }
                None => Ok(None),
            }
        })
    }

    fn lookup_cqrs_by_assertion_postgres(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        self.with_retry(StorageOperation::Query, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;
            let row = client.query_opt(
                &format!(
                    "SELECT envelope_id, assertion_id, subject_id, is_overlay \
                     FROM {} \
                     WHERE assertion_id = $1 \
                     ORDER BY inserted_at DESC \
                     LIMIT 1",
                    self.table("cqrs_reverse")
                ),
                &[&assertion_id.as_bytes().as_slice()],
            )?;
            match row {
                Some(row) => {
                    let env: Vec<u8> = row.get(0);
                    let assertion: Vec<u8> = row.get(1);
                    let subject: Vec<u8> = row.get(2);
                    let overlay: bool = row.get(3);
                    Ok(Some(CqrsReverseEntry {
                        envelope_id: EnvelopeId::from_slice(&env)?,
                        assertion_id: AssertionId::from_slice(&assertion)?,
                        subject: SubjectId::from_slice(&subject)?,
                        is_overlay: overlay,
                    }))
                }
                None => Ok(None),
            }
        })
    }
}

impl StorageCommit for PostgresServerAdapter {
    fn put_object_if_absent(
        &self,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        self.legacy.put_object(envelope_id, bytes)?;
        self.with_retry(StorageOperation::Commit, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;
            self.insert_object_postgres(&mut client, envelope_id, bytes)
        })
    }

    fn put_assertion(
        &self,
        subject: &SubjectId,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        let assertion = AssertionPlaintext::from_cbor(bytes)?;
        let assertion_id = assertion.assertion_id()?;
        let seq = assertion.header.seq;
        let overlay = is_overlay(&assertion.header);

        self.legacy.put_assertion(subject, envelope_id, bytes)?;

        self.with_retry(StorageOperation::Commit, || {
            let mut client = self.pool.get()?;
            let mut tx = client.transaction()?;
            tx.batch_execute(&format!(
                "SET LOCAL statement_timeout = {}",
                self.statement_timeout_ms
            ))?;

            tx.execute(
                &format!(
                    "INSERT INTO {}(envelope_id, bytes) VALUES ($1, $2) \
                     ON CONFLICT (envelope_id) DO NOTHING",
                    self.table("objects")
                ),
                &[&envelope_id.as_bytes().as_slice(), &bytes],
            )?;

            let seq_i64 = i64::try_from(seq)
                .map_err(|_| DharmaError::Validation("assertion seq overflow".to_string()))?;
            tx.execute(
                &format!(
                    "INSERT INTO {}(subject_id, seq, assertion_id, envelope_id, bytes, is_overlay) \
                     VALUES ($1, $2, $3, $4, $5, $6) \
                     ON CONFLICT (subject_id, is_overlay, seq, assertion_id) \
                     DO UPDATE SET envelope_id = EXCLUDED.envelope_id, bytes = EXCLUDED.bytes",
                    self.table("subject_assertions")
                ),
                &[
                    &subject.as_bytes().as_slice(),
                    &seq_i64,
                    &assertion_id.as_bytes().as_slice(),
                    &envelope_id.as_bytes().as_slice(),
                    &bytes,
                    &overlay,
                ],
            )?;

            tx.commit()?;
            Ok(())
        })
    }

    fn put_permission_summary(&self, summary: &PermissionSummary) -> Result<(), DharmaError> {
        self.legacy.put_permission_summary(summary)?;
        self.with_retry(StorageOperation::Commit, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;
            self.upsert_permission_summary_postgres(&mut client, summary)
        })
    }
}

impl StorageRead for PostgresServerAdapter {
    fn get_object(&self, envelope_id: &EnvelopeId) -> Result<Vec<u8>, DharmaError> {
        if let Some(bytes) = self.load_object_postgres(envelope_id)? {
            return Ok(bytes);
        }

        let bytes = self.legacy.get_object(envelope_id)?;
        self.with_retry(StorageOperation::Read, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;
            self.insert_object_postgres(&mut client, envelope_id, &bytes)
        })?;
        Ok(bytes)
    }

    fn get_object_any(&self, envelope_id: &EnvelopeId) -> Result<Option<Vec<u8>>, DharmaError> {
        if let Some(bytes) = self.load_object_postgres(envelope_id)? {
            return Ok(Some(bytes));
        }

        let value = self.legacy.get_object_any(envelope_id)?;
        if let Some(bytes) = value.as_ref() {
            self.with_retry(StorageOperation::Read, || {
                let mut client = self.pool.get()?;
                self.apply_statement_timeout(&mut client)?;
                self.insert_object_postgres(&mut client, envelope_id, bytes)
            })?;
        }
        Ok(value)
    }

    fn get_assertion(
        &self,
        _subject: &SubjectId,
        envelope_id: &EnvelopeId,
    ) -> Result<Vec<u8>, DharmaError> {
        self.get_object(envelope_id)
    }

    fn scan_subject(&self, subject: &SubjectId) -> Result<Vec<AssertionId>, DharmaError> {
        let cached = self.load_subject_assertions_postgres(subject)?;
        let ids = self.legacy.scan_subject(subject)?;

        let cached_set: HashSet<AssertionId> = cached.iter().copied().collect();
        let missing_legacy = ids.iter().any(|id| !cached_set.contains(id));
        if missing_legacy {
            self.backfill_subject_from_legacy(subject)?;
        }

        let mut out = ids;
        let mut seen: HashSet<AssertionId> = out.iter().copied().collect();
        for assertion_id in cached {
            if seen.insert(assertion_id) {
                out.push(assertion_id);
            }
        }
        Ok(out)
    }

    fn list_subjects(&self) -> Result<Vec<SubjectId>, DharmaError> {
        let cached = self.with_retry(StorageOperation::Read, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;
            let rows = client.query(
                &format!(
                    "SELECT DISTINCT subject_id FROM {}",
                    self.table("subject_assertions")
                ),
                &[],
            )?;
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let raw: Vec<u8> = row.get(0);
                out.push(SubjectId::from_slice(&raw)?);
            }
            Ok(out)
        })?;

        let legacy = self.legacy.list_subjects()?;
        let cached_set: HashSet<SubjectId> = cached.iter().copied().collect();
        for subject in &legacy {
            if !cached_set.contains(subject) {
                self.backfill_subject_from_legacy(subject)?;
            }
        }

        let mut out = legacy;
        let mut seen: HashSet<SubjectId> = out.iter().copied().collect();
        for subject in cached {
            if seen.insert(subject) {
                out.push(subject);
            }
        }
        Ok(out)
    }

    fn list_objects(&self) -> Result<Vec<EnvelopeId>, DharmaError> {
        let cached = self.with_retry(StorageOperation::Read, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;
            let rows = client.query(
                &format!("SELECT envelope_id FROM {}", self.table("objects")),
                &[],
            )?;
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let raw: Vec<u8> = row.get(0);
                out.push(EnvelopeId::from_slice(&raw)?);
            }
            Ok(out)
        })?;

        let legacy = self.legacy.list_objects()?;
        let cached_set: HashSet<EnvelopeId> = cached.iter().copied().collect();
        for envelope_id in &legacy {
            if !cached_set.contains(envelope_id) {
                if let Some(bytes) = self.legacy.get_object_any(envelope_id)? {
                    self.with_retry(StorageOperation::Read, || {
                        let mut client = self.pool.get()?;
                        self.apply_statement_timeout(&mut client)?;
                        self.insert_object_postgres(&mut client, envelope_id, &bytes)
                    })?;
                }
            }
        }

        let mut out = legacy;
        let mut seen: HashSet<EnvelopeId> = out.iter().copied().collect();
        for envelope_id in cached {
            if seen.insert(envelope_id) {
                out.push(envelope_id);
            }
        }
        Ok(out)
    }

    fn get_permission_summary(
        &self,
        contract: &ContractId,
    ) -> Result<Option<PermissionSummary>, DharmaError> {
        let cached = self.with_retry(StorageOperation::Read, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;
            let row = client.query_opt(
                &format!(
                    "SELECT bytes FROM {} WHERE contract_id = $1",
                    self.table("permission_summaries")
                ),
                &[&contract.as_bytes().as_slice()],
            )?;
            Ok(row.map(|row| row.get::<_, Vec<u8>>(0)))
        })?;

        if let Some(bytes) = cached {
            return Ok(Some(PermissionSummary::from_cbor(&bytes)?));
        }

        let summary = self.legacy.get_permission_summary(contract)?;
        if let Some(value) = summary.as_ref() {
            self.with_retry(StorageOperation::Read, || {
                let mut client = self.pool.get()?;
                self.apply_statement_timeout(&mut client)?;
                self.upsert_permission_summary_postgres(&mut client, value)
            })?;
        }
        Ok(summary)
    }
}

impl StorageIndex for PostgresServerAdapter {
    fn rebuild_subject_views(&self, keys: &Keyring) -> Result<(), DharmaError> {
        self.legacy.rebuild_subject_views(keys)?;
        self.rebuild_indexes_from_legacy()
    }

    fn record_semantic(
        &self,
        assertion_id: &AssertionId,
        envelope_id: &EnvelopeId,
    ) -> Result<(), DharmaError> {
        self.legacy.record_semantic(assertion_id, envelope_id)?;
        self.with_retry(StorageOperation::Index, || {
            let mut client = self.pool.get()?;
            self.apply_statement_timeout(&mut client)?;
            self.insert_semantic_postgres(&mut client, assertion_id, envelope_id)
        })
    }
}

impl StorageQuery for PostgresServerAdapter {
    fn lookup_envelope(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<EnvelopeId>, DharmaError> {
        if let Some(envelope_id) = self.lookup_envelope_postgres(assertion_id)? {
            return Ok(Some(envelope_id));
        }

        let value = self.legacy.lookup_envelope(assertion_id)?;
        if let Some(envelope_id) = value.as_ref() {
            self.with_retry(StorageOperation::Query, || {
                let mut client = self.pool.get()?;
                self.apply_statement_timeout(&mut client)?;
                self.insert_semantic_postgres(&mut client, assertion_id, envelope_id)
            })?;
        }
        Ok(value)
    }

    fn lookup_cqrs_by_envelope(
        &self,
        envelope_id: &EnvelopeId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        if let Some(entry) = self.lookup_cqrs_by_envelope_postgres(envelope_id)? {
            return Ok(Some(entry));
        }

        let value = self.legacy.lookup_cqrs_by_envelope(envelope_id)?;
        if let Some(entry) = value.as_ref() {
            self.with_retry(StorageOperation::Query, || {
                let mut client = self.pool.get()?;
                self.apply_statement_timeout(&mut client)?;
                self.insert_cqrs_postgres(&mut client, entry)
            })?;
        }
        Ok(value)
    }

    fn lookup_cqrs_by_assertion(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        if let Some(entry) = self.lookup_cqrs_by_assertion_postgres(assertion_id)? {
            return Ok(Some(entry));
        }

        let value = self.legacy.lookup_cqrs_by_assertion(assertion_id)?;
        if let Some(entry) = value.as_ref() {
            self.with_retry(StorageOperation::Query, || {
                let mut client = self.pool.get()?;
                self.apply_statement_timeout(&mut client)?;
                self.insert_cqrs_postgres(&mut client, entry)
            })?;
        }
        Ok(value)
    }
}

fn normalize_schema(schema: &str) -> String {
    let trimmed = schema.trim();
    if trimmed.is_empty() {
        "public".to_string()
    } else {
        trimmed.to_string()
    }
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
