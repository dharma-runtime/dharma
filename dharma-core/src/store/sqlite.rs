use crate::contract::PermissionSummary;
use crate::error::DharmaError;
use crate::keys::Keyring;
use crate::store::spi::{StorageCommit, StorageIndex, StorageQuery, StorageRead};
use crate::store::state::{self, CqrsReverseEntry};
use crate::store::Store;
use crate::types::{AssertionId, ContractId, EnvelopeId, SubjectId};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const EMBEDDED_SQLITE_FILENAME: &str = "embedded.sqlite";

const INIT_SQL: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS objects (
    envelope_id BLOB PRIMARY KEY,
    bytes BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS semantic_index (
    assertion_id BLOB NOT NULL,
    envelope_id BLOB NOT NULL,
    inserted_at INTEGER PRIMARY KEY AUTOINCREMENT
);

CREATE TABLE IF NOT EXISTS cqrs_reverse (
    envelope_id BLOB NOT NULL,
    assertion_id BLOB NOT NULL,
    subject_id BLOB NOT NULL,
    is_overlay INTEGER NOT NULL CHECK(is_overlay IN (0, 1)),
    inserted_at INTEGER PRIMARY KEY AUTOINCREMENT
);

CREATE TABLE IF NOT EXISTS subject_assertions (
    subject_id BLOB NOT NULL,
    seq INTEGER NOT NULL,
    assertion_id BLOB NOT NULL,
    envelope_id BLOB NOT NULL,
    bytes BLOB NOT NULL,
    is_overlay INTEGER NOT NULL DEFAULT 0 CHECK(is_overlay IN (0, 1)),
    PRIMARY KEY (subject_id, is_overlay, seq, assertion_id)
);

CREATE TABLE IF NOT EXISTS permission_summaries (
    contract_id BLOB PRIMARY KEY,
    bytes BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_semantic_assertion
    ON semantic_index(assertion_id, inserted_at DESC);
CREATE INDEX IF NOT EXISTS idx_cqrs_envelope
    ON cqrs_reverse(envelope_id, inserted_at DESC);
CREATE INDEX IF NOT EXISTS idx_cqrs_assertion
    ON cqrs_reverse(assertion_id, inserted_at DESC);
CREATE INDEX IF NOT EXISTS idx_subject_assertions_subject_seq
    ON subject_assertions(subject_id, seq DESC);
"#;

#[derive(Clone)]
pub struct SqliteEmbeddedAdapter {
    legacy: Store,
    conn: Arc<Mutex<Connection>>,
    db_path: PathBuf,
}

impl SqliteEmbeddedAdapter {
    pub fn open(root: &Path, legacy: Store) -> Result<Self, DharmaError> {
        let indexes_dir = root.join("indexes");
        std::fs::create_dir_all(&indexes_dir)?;
        let db_path = indexes_dir.join(EMBEDDED_SQLITE_FILENAME);
        let conn = Connection::open(&db_path)?;
        conn.busy_timeout(Duration::from_millis(5_000))?;
        conn.execute_batch(INIT_SQL)?;
        Ok(Self {
            legacy,
            conn: Arc::new(Mutex::new(conn)),
            db_path,
        })
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    fn with_conn<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, DharmaError>,
    ) -> Result<T, DharmaError> {
        let guard = self
            .conn
            .lock()
            .map_err(|_| DharmaError::Validation("sqlite connection lock poisoned".to_string()))?;
        f(&guard)
    }

    fn insert_object_sqlite(
        conn: &Connection,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        conn.execute(
            "INSERT OR IGNORE INTO objects(envelope_id, bytes) VALUES (?1, ?2)",
            params![envelope_id.as_bytes().as_slice(), bytes],
        )?;
        Ok(())
    }

    fn insert_semantic_sqlite(
        conn: &Connection,
        assertion_id: &AssertionId,
        envelope_id: &EnvelopeId,
    ) -> Result<(), DharmaError> {
        conn.execute(
            "INSERT INTO semantic_index(assertion_id, envelope_id) VALUES (?1, ?2)",
            params![
                assertion_id.as_bytes().as_slice(),
                envelope_id.as_bytes().as_slice()
            ],
        )?;
        Ok(())
    }

    fn insert_cqrs_sqlite(conn: &Connection, entry: &CqrsReverseEntry) -> Result<(), DharmaError> {
        conn.execute(
            "INSERT INTO cqrs_reverse(envelope_id, assertion_id, subject_id, is_overlay)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                entry.envelope_id.as_bytes().as_slice(),
                entry.assertion_id.as_bytes().as_slice(),
                entry.subject.as_bytes().as_slice(),
                if entry.is_overlay { 1_i64 } else { 0_i64 },
            ],
        )?;
        Ok(())
    }

    fn upsert_subject_assertion_sqlite(
        conn: &Connection,
        subject: &SubjectId,
        seq: u64,
        assertion_id: &AssertionId,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
        is_overlay: bool,
    ) -> Result<(), DharmaError> {
        let seq_i64 = i64::try_from(seq)
            .map_err(|_| DharmaError::Validation("assertion seq overflow".to_string()))?;
        conn.execute(
            "INSERT OR REPLACE INTO subject_assertions(
                subject_id, seq, assertion_id, envelope_id, bytes, is_overlay
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                subject.as_bytes().as_slice(),
                seq_i64,
                assertion_id.as_bytes().as_slice(),
                envelope_id.as_bytes().as_slice(),
                bytes,
                if is_overlay { 1_i64 } else { 0_i64 },
            ],
        )?;
        Ok(())
    }

    fn upsert_permission_summary_sqlite(
        conn: &Connection,
        summary: &PermissionSummary,
    ) -> Result<(), DharmaError> {
        let bytes = summary.to_cbor()?;
        conn.execute(
            "INSERT OR REPLACE INTO permission_summaries(contract_id, bytes) VALUES (?1, ?2)",
            params![summary.contract.as_bytes().as_slice(), bytes],
        )?;
        Ok(())
    }

    fn load_object_sqlite(&self, envelope_id: &EnvelopeId) -> Result<Option<Vec<u8>>, DharmaError> {
        self.with_conn(|conn| {
            let value = conn
                .query_row(
                    "SELECT bytes FROM objects WHERE envelope_id = ?1",
                    params![envelope_id.as_bytes().as_slice()],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?;
            Ok(value)
        })
    }

    fn load_subject_assertions_sqlite(
        &self,
        subject: &SubjectId,
    ) -> Result<Vec<AssertionId>, DharmaError> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT assertion_id FROM subject_assertions
                 WHERE subject_id = ?1 AND is_overlay = 0
                 ORDER BY seq ASC, rowid ASC",
            )?;
            let mut rows = stmt.query(params![subject.as_bytes().as_slice()])?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                let raw: Vec<u8> = row.get(0)?;
                out.push(AssertionId::from_slice(&raw)?);
            }
            Ok(out)
        })
    }

    fn backfill_subject_from_legacy(&self, subject: &SubjectId) -> Result<(), DharmaError> {
        let assertions = state::list_assertions(self.legacy.env(), subject)?;
        let overlays = state::list_overlays(self.legacy.env(), subject)?;
        self.with_conn(|conn| {
            for record in assertions {
                Self::upsert_subject_assertion_sqlite(
                    conn,
                    subject,
                    record.seq,
                    &record.assertion_id,
                    &record.envelope_id,
                    &record.bytes,
                    false,
                )?;
                Self::insert_object_sqlite(conn, &record.envelope_id, &record.bytes)?;
                Self::insert_semantic_sqlite(conn, &record.assertion_id, &record.envelope_id)?;
                Self::insert_cqrs_sqlite(
                    conn,
                    &CqrsReverseEntry {
                        envelope_id: record.envelope_id,
                        assertion_id: record.assertion_id,
                        subject: *subject,
                        is_overlay: false,
                    },
                )?;
            }
            for record in overlays {
                Self::upsert_subject_assertion_sqlite(
                    conn,
                    subject,
                    record.seq,
                    &record.assertion_id,
                    &record.envelope_id,
                    &record.bytes,
                    true,
                )?;
                Self::insert_object_sqlite(conn, &record.envelope_id, &record.bytes)?;
                Self::insert_semantic_sqlite(conn, &record.assertion_id, &record.envelope_id)?;
                Self::insert_cqrs_sqlite(
                    conn,
                    &CqrsReverseEntry {
                        envelope_id: record.envelope_id,
                        assertion_id: record.assertion_id,
                        subject: *subject,
                        is_overlay: true,
                    },
                )?;
            }
            Ok(())
        })
    }

    fn backfill_cqrs_from_legacy(&self) -> Result<(), DharmaError> {
        let entries = state::read_cqrs_reverse_entries(self.legacy.env())?;
        self.with_conn(|conn| {
            for entry in entries {
                Self::insert_cqrs_sqlite(conn, &entry)?;
            }
            Ok(())
        })
    }

    fn rebuild_indexes_from_legacy(&self) -> Result<(), DharmaError> {
        self.with_conn(|conn| {
            conn.execute("DELETE FROM semantic_index", [])?;
            conn.execute("DELETE FROM cqrs_reverse", [])?;
            conn.execute("DELETE FROM subject_assertions", [])?;
            Ok(())
        })?;

        let subjects = self.legacy.list_subjects()?;
        for subject in subjects {
            self.backfill_subject_from_legacy(&subject)?;
        }
        self.backfill_cqrs_from_legacy()?;
        Ok(())
    }

    fn lookup_envelope_sqlite(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<EnvelopeId>, DharmaError> {
        self.with_conn(|conn| {
            let raw = conn
                .query_row(
                    "SELECT envelope_id FROM semantic_index
                     WHERE assertion_id = ?1
                     ORDER BY inserted_at DESC
                     LIMIT 1",
                    params![assertion_id.as_bytes().as_slice()],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?;
            match raw {
                Some(bytes) => Ok(Some(EnvelopeId::from_slice(&bytes)?)),
                None => Ok(None),
            }
        })
    }

    fn lookup_cqrs_by_envelope_sqlite(
        &self,
        envelope_id: &EnvelopeId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        self.with_conn(|conn| {
            let row = conn
                .query_row(
                    "SELECT envelope_id, assertion_id, subject_id, is_overlay
                     FROM cqrs_reverse
                     WHERE envelope_id = ?1
                     ORDER BY inserted_at DESC
                     LIMIT 1",
                    params![envelope_id.as_bytes().as_slice()],
                    |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
                .optional()?;
            match row {
                Some((env, assertion, subject, overlay)) => Ok(Some(CqrsReverseEntry {
                    envelope_id: EnvelopeId::from_slice(&env)?,
                    assertion_id: AssertionId::from_slice(&assertion)?,
                    subject: SubjectId::from_slice(&subject)?,
                    is_overlay: overlay != 0,
                })),
                None => Ok(None),
            }
        })
    }

    fn lookup_cqrs_by_assertion_sqlite(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        self.with_conn(|conn| {
            let row = conn
                .query_row(
                    "SELECT envelope_id, assertion_id, subject_id, is_overlay
                     FROM cqrs_reverse
                     WHERE assertion_id = ?1
                     ORDER BY inserted_at DESC
                     LIMIT 1",
                    params![assertion_id.as_bytes().as_slice()],
                    |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
                .optional()?;
            match row {
                Some((env, assertion, subject, overlay)) => Ok(Some(CqrsReverseEntry {
                    envelope_id: EnvelopeId::from_slice(&env)?,
                    assertion_id: AssertionId::from_slice(&assertion)?,
                    subject: SubjectId::from_slice(&subject)?,
                    is_overlay: overlay != 0,
                })),
                None => Ok(None),
            }
        })
    }

    pub fn explain_query_plan(
        &self,
        query: &str,
        param: &[u8],
    ) -> Result<Vec<String>, DharmaError> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(query)?;
            let mut rows = stmt.query(params![param])?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                out.push(row.get::<_, String>(3)?);
            }
            Ok(out)
        })
    }
}

impl StorageCommit for SqliteEmbeddedAdapter {
    fn put_object_if_absent(
        &self,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        self.legacy.put_object_if_absent(envelope_id, bytes)?;
        self.with_conn(|conn| Self::insert_object_sqlite(conn, envelope_id, bytes))
    }

    fn put_assertion(
        &self,
        subject: &SubjectId,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        let assertion = crate::assertion::AssertionPlaintext::from_cbor(bytes)?;
        let assertion_id = assertion.assertion_id()?;
        let seq = assertion.header.seq;

        self.legacy.put_assertion(subject, envelope_id, bytes)?;
        self.with_conn(|conn| {
            Self::insert_object_sqlite(conn, envelope_id, bytes)?;
            Self::upsert_subject_assertion_sqlite(
                conn,
                subject,
                seq,
                &assertion_id,
                envelope_id,
                bytes,
                false,
            )?;
            Ok(())
        })
    }

    fn put_permission_summary(&self, summary: &PermissionSummary) -> Result<(), DharmaError> {
        self.legacy.put_permission_summary(summary)?;
        self.with_conn(|conn| Self::upsert_permission_summary_sqlite(conn, summary))
    }
}

impl StorageRead for SqliteEmbeddedAdapter {
    fn get_object(&self, envelope_id: &EnvelopeId) -> Result<Vec<u8>, DharmaError> {
        if let Some(bytes) = self.load_object_sqlite(envelope_id)? {
            return Ok(bytes);
        }
        let bytes = self.legacy.get_object(envelope_id)?;
        self.with_conn(|conn| Self::insert_object_sqlite(conn, envelope_id, &bytes))?;
        Ok(bytes)
    }

    fn get_object_any(&self, envelope_id: &EnvelopeId) -> Result<Option<Vec<u8>>, DharmaError> {
        if let Some(bytes) = self.load_object_sqlite(envelope_id)? {
            return Ok(Some(bytes));
        }
        let value = self.legacy.get_object_any(envelope_id)?;
        if let Some(bytes) = value.as_ref() {
            self.with_conn(|conn| Self::insert_object_sqlite(conn, envelope_id, bytes))?;
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
        let cached = self.load_subject_assertions_sqlite(subject)?;
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
        let cached = self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT DISTINCT subject_id FROM subject_assertions")?;
            let mut rows = stmt.query([])?;
            let mut subjects = Vec::new();
            while let Some(row) = rows.next()? {
                let raw: Vec<u8> = row.get(0)?;
                subjects.push(SubjectId::from_slice(&raw)?);
            }
            Ok(subjects)
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
        let cached = self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT envelope_id FROM objects")?;
            let mut rows = stmt.query([])?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                let raw: Vec<u8> = row.get(0)?;
                out.push(EnvelopeId::from_slice(&raw)?);
            }
            Ok(out)
        })?;

        let legacy = self.legacy.list_objects()?;
        let cached_set: HashSet<EnvelopeId> = cached.iter().copied().collect();
        for envelope_id in &legacy {
            if !cached_set.contains(envelope_id) {
                if let Some(bytes) = self.legacy.get_object_any(envelope_id)? {
                    self.with_conn(|conn| Self::insert_object_sqlite(conn, envelope_id, &bytes))?;
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
        let cached = self.with_conn(|conn| {
            let bytes = conn
                .query_row(
                    "SELECT bytes FROM permission_summaries WHERE contract_id = ?1",
                    params![contract.as_bytes().as_slice()],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?;
            Ok(bytes)
        })?;

        if let Some(bytes) = cached {
            return Ok(Some(PermissionSummary::from_cbor(&bytes)?));
        }

        let summary = self.legacy.get_permission_summary(contract)?;
        if let Some(value) = summary.as_ref() {
            self.with_conn(|conn| Self::upsert_permission_summary_sqlite(conn, value))?;
        }
        Ok(summary)
    }
}

impl StorageIndex for SqliteEmbeddedAdapter {
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
        self.with_conn(|conn| Self::insert_semantic_sqlite(conn, assertion_id, envelope_id))
    }
}

impl StorageQuery for SqliteEmbeddedAdapter {
    fn lookup_envelope(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<EnvelopeId>, DharmaError> {
        if let Some(envelope_id) = self.lookup_envelope_sqlite(assertion_id)? {
            return Ok(Some(envelope_id));
        }

        let value = self.legacy.lookup_envelope(assertion_id)?;
        if let Some(envelope_id) = value.as_ref() {
            self.with_conn(|conn| Self::insert_semantic_sqlite(conn, assertion_id, envelope_id))?;
        }
        Ok(value)
    }

    fn lookup_cqrs_by_envelope(
        &self,
        envelope_id: &EnvelopeId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        if let Some(entry) = self.lookup_cqrs_by_envelope_sqlite(envelope_id)? {
            return Ok(Some(entry));
        }

        let value = self.legacy.lookup_cqrs_by_envelope(envelope_id)?;
        if let Some(entry) = value.as_ref() {
            self.with_conn(|conn| Self::insert_cqrs_sqlite(conn, entry))?;
        }
        Ok(value)
    }

    fn lookup_cqrs_by_assertion(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        if let Some(entry) = self.lookup_cqrs_by_assertion_sqlite(assertion_id)? {
            return Ok(Some(entry));
        }

        let value = self.legacy.lookup_cqrs_by_assertion(assertion_id)?;
        if let Some(entry) = value.as_ref() {
            self.with_conn(|conn| Self::insert_cqrs_sqlite(conn, entry))?;
        }
        Ok(value)
    }
}
