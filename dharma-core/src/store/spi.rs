use crate::config::Config;
use crate::contract::PermissionSummary;
use crate::env::Env;
use crate::error::DharmaError;
use crate::keys::Keyring;
use crate::store::clickhouse::ClickHouseServerAnalyticsAdapter;
use crate::store::postgres::PostgresServerAdapter;
use crate::store::sqlite::SqliteEmbeddedAdapter;
use crate::store::state::CqrsReverseEntry;
use crate::store::Store;
use crate::types::{AssertionId, ContractId, EnvelopeId, SubjectId};
use std::any::Any;
use std::io::ErrorKind;
use std::path::Path;

pub trait StorageCommit {
    fn put_object_if_absent(
        &self,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError>;
    fn put_assertion(
        &self,
        subject: &SubjectId,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError>;
    fn put_permission_summary(&self, summary: &PermissionSummary) -> Result<(), DharmaError>;
}

pub trait StorageRead {
    fn get_object(&self, envelope_id: &EnvelopeId) -> Result<Vec<u8>, DharmaError>;
    fn get_object_any(&self, envelope_id: &EnvelopeId) -> Result<Option<Vec<u8>>, DharmaError>;
    fn get_assertion(
        &self,
        subject: &SubjectId,
        envelope_id: &EnvelopeId,
    ) -> Result<Vec<u8>, DharmaError>;
    fn scan_subject(&self, subject: &SubjectId) -> Result<Vec<AssertionId>, DharmaError>;
    fn list_subjects(&self) -> Result<Vec<SubjectId>, DharmaError>;
    fn list_objects(&self) -> Result<Vec<EnvelopeId>, DharmaError>;
    fn get_permission_summary(
        &self,
        contract: &ContractId,
    ) -> Result<Option<PermissionSummary>, DharmaError>;
}

pub trait StorageIndex {
    fn rebuild_subject_views(&self, keys: &Keyring) -> Result<(), DharmaError>;
    fn record_semantic(
        &self,
        assertion_id: &AssertionId,
        envelope_id: &EnvelopeId,
    ) -> Result<(), DharmaError>;
}

pub trait StorageQuery {
    fn lookup_envelope(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<EnvelopeId>, DharmaError>;
    fn lookup_cqrs_by_envelope(
        &self,
        envelope_id: &EnvelopeId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError>;
    fn lookup_cqrs_by_assertion(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError>;
}

pub trait StorageSpi: StorageCommit + StorageRead + StorageIndex + StorageQuery {
    fn as_any(&self) -> &dyn Any;
}

impl<T> StorageSpi for T
where
    T: StorageCommit + StorageRead + StorageIndex + StorageQuery + Any,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StorageCommit for Store {
    fn put_object_if_absent(
        &self,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        self.put_object(envelope_id, bytes)
    }

    fn put_assertion(
        &self,
        subject: &SubjectId,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        self.put_assertion(subject, envelope_id, bytes)
    }

    fn put_permission_summary(&self, summary: &PermissionSummary) -> Result<(), DharmaError> {
        self.put_permission_summary(summary)
    }
}

impl StorageRead for Store {
    fn get_object(&self, envelope_id: &EnvelopeId) -> Result<Vec<u8>, DharmaError> {
        self.get_object(envelope_id)
    }

    fn get_object_any(&self, envelope_id: &EnvelopeId) -> Result<Option<Vec<u8>>, DharmaError> {
        self.get_object_any(envelope_id)
    }

    fn get_assertion(
        &self,
        subject: &SubjectId,
        envelope_id: &EnvelopeId,
    ) -> Result<Vec<u8>, DharmaError> {
        self.get_assertion(subject, envelope_id)
    }

    fn scan_subject(&self, subject: &SubjectId) -> Result<Vec<AssertionId>, DharmaError> {
        self.scan_subject(subject)
    }

    fn list_subjects(&self) -> Result<Vec<SubjectId>, DharmaError> {
        self.list_subjects()
    }

    fn list_objects(&self) -> Result<Vec<EnvelopeId>, DharmaError> {
        self.list_objects()
    }

    fn get_permission_summary(
        &self,
        contract: &ContractId,
    ) -> Result<Option<PermissionSummary>, DharmaError> {
        self.get_permission_summary(contract)
    }
}

impl StorageIndex for Store {
    fn rebuild_subject_views(&self, keys: &Keyring) -> Result<(), DharmaError> {
        self.rebuild_subject_views(keys)
    }

    fn record_semantic(
        &self,
        assertion_id: &AssertionId,
        envelope_id: &EnvelopeId,
    ) -> Result<(), DharmaError> {
        self.record_semantic(assertion_id, envelope_id)
    }
}

impl StorageQuery for Store {
    fn lookup_envelope(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<EnvelopeId>, DharmaError> {
        self.lookup_envelope(assertion_id)
    }

    fn lookup_cqrs_by_envelope(
        &self,
        envelope_id: &EnvelopeId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        self.lookup_cqrs_by_envelope(envelope_id)
    }

    fn lookup_cqrs_by_assertion(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        self.lookup_cqrs_by_assertion(assertion_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeMode {
    Embedded,
    Server,
}

impl RuntimeMode {
    pub fn from_profile_mode(mode: &str) -> Self {
        match mode.trim().to_ascii_lowercase().as_str() {
            "server" => RuntimeMode::Server,
            _ => RuntimeMode::Embedded,
        }
    }

    pub fn from_config(config: &Config) -> Self {
        Self::from_profile_mode(&config.profile.mode)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            RuntimeMode::Embedded => "embedded",
            RuntimeMode::Server => "server",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BackendKind {
    Sqlite,
    Postgres,
    ClickHouse,
}

impl BackendKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            BackendKind::Sqlite => "sqlite",
            BackendKind::Postgres => "postgresql",
            BackendKind::ClickHouse => "clickhouse",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageOperation {
    Commit,
    Read,
    Index,
    Query,
}

impl StorageOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageOperation::Commit => "commit",
            StorageOperation::Read => "read",
            StorageOperation::Index => "index",
            StorageOperation::Query => "query",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackendSelection {
    pub mode: RuntimeMode,
    pub commit: BackendKind,
    pub read: BackendKind,
    pub index: BackendKind,
    pub query: BackendKind,
}

impl BackendSelection {
    pub fn for_mode(mode: RuntimeMode) -> Self {
        match mode {
            RuntimeMode::Embedded => BackendSelection {
                mode,
                commit: BackendKind::Sqlite,
                read: BackendKind::Sqlite,
                index: BackendKind::Sqlite,
                query: BackendKind::Sqlite,
            },
            RuntimeMode::Server => BackendSelection {
                mode,
                commit: BackendKind::Postgres,
                read: BackendKind::Postgres,
                index: BackendKind::Postgres,
                query: BackendKind::ClickHouse,
            },
        }
    }

    pub fn from_config(config: &Config) -> Self {
        Self::for_mode(RuntimeMode::from_config(config))
    }

    pub fn backend_for(&self, operation: StorageOperation) -> BackendKind {
        match operation {
            StorageOperation::Commit => self.commit,
            StorageOperation::Read => self.read,
            StorageOperation::Index => self.index,
            StorageOperation::Query => self.query,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackendCapabilityRow {
    pub backend: BackendKind,
    pub commit: bool,
    pub read: bool,
    pub index: bool,
    pub query: bool,
    pub strong_consistency: bool,
    pub eventual_consistency: bool,
}

pub const BACKEND_CAPABILITY_MATRIX: [BackendCapabilityRow; 3] = [
    BackendCapabilityRow {
        backend: BackendKind::Sqlite,
        commit: true,
        read: true,
        index: true,
        query: true,
        strong_consistency: true,
        eventual_consistency: false,
    },
    BackendCapabilityRow {
        backend: BackendKind::Postgres,
        commit: true,
        read: true,
        index: true,
        query: true,
        strong_consistency: true,
        eventual_consistency: false,
    },
    BackendCapabilityRow {
        backend: BackendKind::ClickHouse,
        commit: false,
        read: false,
        index: false,
        query: true,
        strong_consistency: false,
        eventual_consistency: true,
    },
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageCompatibilityContract {
    pub version: &'static str,
    pub preserves_dhl_syntax: bool,
    pub preserves_dharmaq_syntax: bool,
    pub preserves_command_surface: bool,
    pub preserves_query_surface: bool,
}

pub const STORAGE_COMPATIBILITY_CONTRACT: StorageCompatibilityContract =
    StorageCompatibilityContract {
        version: "dharma-storage-spi-v1",
        preserves_dhl_syntax: true,
        preserves_dharmaq_syntax: true,
        preserves_command_surface: true,
        preserves_query_surface: true,
    };

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendErrorClass {
    Retryable,
    Fatal,
}

impl BackendErrorClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            BackendErrorClass::Retryable => "retryable",
            BackendErrorClass::Fatal => "fatal",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendErrorTaxonomy {
    pub backend: BackendKind,
    pub operation: StorageOperation,
    pub class: BackendErrorClass,
    pub message: String,
}

impl BackendErrorTaxonomy {
    pub fn classify(
        backend: BackendKind,
        operation: StorageOperation,
        error: &DharmaError,
    ) -> Self {
        Self {
            backend,
            operation,
            class: classify_backend_error(error),
            message: error.to_string(),
        }
    }
}

pub fn classify_backend_error(error: &DharmaError) -> BackendErrorClass {
    match error {
        DharmaError::Network(_) | DharmaError::LockBusy => BackendErrorClass::Retryable,
        DharmaError::Sqlite { code, .. }
            if code.as_deref() == Some("DatabaseBusy")
                || code.as_deref() == Some("DatabaseLocked") =>
        {
            BackendErrorClass::Retryable
        }
        DharmaError::Postgres { code, message } => {
            if let Some(code) = code.as_deref() {
                if is_retryable_postgres_state(code) {
                    return BackendErrorClass::Retryable;
                }
            } else {
                let lowered = message.to_ascii_lowercase();
                if lowered.contains("timeout")
                    || lowered.contains("timed out")
                    || lowered.contains("connection")
                {
                    return BackendErrorClass::Retryable;
                }
            }
            BackendErrorClass::Fatal
        }
        DharmaError::ClickHouse { code: _, message } => {
            let lowered = message.to_ascii_lowercase();
            if lowered.contains("timeout")
                || lowered.contains("timed out")
                || lowered.contains("connection")
                || lowered.contains("too many simultaneous queries")
                || lowered.contains("temporarily unavailable")
            {
                BackendErrorClass::Retryable
            } else {
                BackendErrorClass::Fatal
            }
        }
        DharmaError::Io(err) => match err.kind() {
            ErrorKind::TimedOut
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
            | ErrorKind::NotConnected
            | ErrorKind::AddrInUse
            | ErrorKind::AddrNotAvailable
            | ErrorKind::ConnectionRefused
            | ErrorKind::WouldBlock
            | ErrorKind::Interrupted => BackendErrorClass::Retryable,
            _ => BackendErrorClass::Fatal,
        },
        _ => BackendErrorClass::Fatal,
    }
}

fn is_retryable_postgres_state(code: &str) -> bool {
    matches!(code, "40001" | "40P01" | "55P03" | "57014")
        || code.starts_with("08")
        || code.starts_with("53")
}

#[derive(Clone)]
enum EmbeddedAdapter {
    Ready(SqliteEmbeddedAdapter),
    Failed(DharmaError),
}

#[derive(Clone)]
enum ServerAdapter {
    Ready(PostgresServerAdapter),
    Failed(DharmaError),
}

#[derive(Clone)]
enum ClickHouseAdapter {
    Ready(ClickHouseServerAnalyticsAdapter),
    Failed(DharmaError),
}

#[derive(Clone)]
pub struct StorageFacade {
    store: Store,
    selection: BackendSelection,
    embedded: Option<EmbeddedAdapter>,
    server: Option<ServerAdapter>,
    clickhouse: Option<ClickHouseAdapter>,
}

impl StorageFacade {
    pub fn new(store: Store, mode: RuntimeMode) -> Self {
        Self::new_with_config(store, mode, &Config::default())
    }

    pub fn new_with_config(store: Store, mode: RuntimeMode, config: &Config) -> Self {
        let selection = BackendSelection::for_mode(mode);
        let embedded = if selection.mode == RuntimeMode::Embedded {
            Some(
                match SqliteEmbeddedAdapter::open(store.root(), store.clone()) {
                    Ok(adapter) => EmbeddedAdapter::Ready(adapter),
                    Err(err) => EmbeddedAdapter::Failed(err),
                },
            )
        } else {
            None
        };
        let server = if selection.mode == RuntimeMode::Server {
            Some(
                match PostgresServerAdapter::open(store.root(), store.clone(), config) {
                    Ok(adapter) => ServerAdapter::Ready(adapter),
                    Err(err) => ServerAdapter::Failed(err),
                },
            )
        } else {
            None
        };
        let clickhouse = if selection.mode == RuntimeMode::Server {
            Some(
                match ClickHouseServerAnalyticsAdapter::open(store.root(), store.clone(), config) {
                    Ok(adapter) => ClickHouseAdapter::Ready(adapter),
                    Err(err) => ClickHouseAdapter::Failed(err),
                },
            )
        } else {
            None
        };
        StorageFacade {
            store,
            selection,
            embedded,
            server,
            clickhouse,
        }
    }

    pub fn from_env_and_config<E>(env: &E, config: &Config) -> Self
    where
        E: Env + Clone + Send + Sync + 'static,
    {
        let store = Store::new(env);
        Self::new_with_config(store, RuntimeMode::from_config(config), config)
    }

    pub fn from_root_and_config<P: AsRef<Path>>(root: P, config: &Config) -> Self {
        let store = Store::from_root(root.as_ref());
        Self::new_with_config(store, RuntimeMode::from_config(config), config)
    }

    pub fn store(&self) -> Store {
        self.store.clone()
    }

    pub fn selection(&self) -> BackendSelection {
        self.selection
    }

    pub fn backend_for_operation(&self, operation: StorageOperation) -> BackendKind {
        self.selection.backend_for(operation)
    }

    pub fn capability_matrix(&self) -> &'static [BackendCapabilityRow] {
        &BACKEND_CAPABILITY_MATRIX
    }

    pub fn compatibility_contract(&self) -> StorageCompatibilityContract {
        STORAGE_COMPATIBILITY_CONTRACT
    }

    pub fn classify_error(
        &self,
        operation: StorageOperation,
        error: &DharmaError,
    ) -> BackendErrorTaxonomy {
        BackendErrorTaxonomy::classify(self.backend_for_operation(operation), operation, error)
    }

    fn sqlite_for_operation(
        &self,
        operation: StorageOperation,
    ) -> Result<Option<&SqliteEmbeddedAdapter>, DharmaError> {
        if self.backend_for_operation(operation) != BackendKind::Sqlite {
            return Ok(None);
        }
        match self.embedded.as_ref() {
            Some(EmbeddedAdapter::Ready(adapter)) => Ok(Some(adapter)),
            Some(EmbeddedAdapter::Failed(err)) => Err(err.clone()),
            None => Err(DharmaError::Config(
                "embedded sqlite backend unavailable".to_string(),
            )),
        }
    }

    fn postgres_for_operation(
        &self,
        operation: StorageOperation,
    ) -> Result<Option<&PostgresServerAdapter>, DharmaError> {
        let wants_postgres = match operation {
            StorageOperation::Commit | StorageOperation::Read | StorageOperation::Index => {
                self.backend_for_operation(operation) == BackendKind::Postgres
            }
            StorageOperation::Query => self.selection.mode == RuntimeMode::Server,
        };
        if !wants_postgres {
            return Ok(None);
        }

        match self.server.as_ref() {
            Some(ServerAdapter::Ready(adapter)) => Ok(Some(adapter)),
            Some(ServerAdapter::Failed(err)) => {
                if operation == StorageOperation::Query {
                    Ok(None)
                } else {
                    Err(err.clone())
                }
            }
            None => {
                if operation == StorageOperation::Query {
                    Ok(None)
                } else {
                    Err(DharmaError::Config(
                        "server postgres backend unavailable".to_string(),
                    ))
                }
            }
        }
    }

    fn clickhouse_for_operation(
        &self,
        operation: StorageOperation,
    ) -> Result<Option<&ClickHouseServerAnalyticsAdapter>, DharmaError> {
        if operation != StorageOperation::Query {
            return Ok(None);
        }
        if self.backend_for_operation(operation) != BackendKind::ClickHouse {
            return Ok(None);
        }
        match self.clickhouse.as_ref() {
            Some(ClickHouseAdapter::Ready(adapter)) => Ok(Some(adapter)),
            Some(ClickHouseAdapter::Failed(err)) => Err(err.clone()),
            None => Err(DharmaError::Config(
                "server clickhouse backend unavailable".to_string(),
            )),
        }
    }

    pub fn clickhouse_server_adapter(
        &self,
    ) -> Result<Option<ClickHouseServerAnalyticsAdapter>, DharmaError> {
        match self.clickhouse.as_ref() {
            Some(ClickHouseAdapter::Ready(adapter)) => Ok(Some(adapter.clone())),
            Some(ClickHouseAdapter::Failed(err)) => Err(err.clone()),
            None => Ok(None),
        }
    }
}

impl StorageCommit for StorageFacade {
    fn put_object_if_absent(
        &self,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Commit)? {
            return adapter.put_object_if_absent(envelope_id, bytes);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Commit)? {
            return adapter.put_object_if_absent(envelope_id, bytes);
        }
        self.store.put_object_if_absent(envelope_id, bytes)
    }

    fn put_assertion(
        &self,
        subject: &SubjectId,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Commit)? {
            return adapter.put_assertion(subject, envelope_id, bytes);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Commit)? {
            return adapter.put_assertion(subject, envelope_id, bytes);
        }
        self.store.put_assertion(subject, envelope_id, bytes)
    }

    fn put_permission_summary(&self, summary: &PermissionSummary) -> Result<(), DharmaError> {
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Commit)? {
            return adapter.put_permission_summary(summary);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Commit)? {
            return adapter.put_permission_summary(summary);
        }
        self.store.put_permission_summary(summary)
    }
}

impl StorageRead for StorageFacade {
    fn get_object(&self, envelope_id: &EnvelopeId) -> Result<Vec<u8>, DharmaError> {
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Read)? {
            return adapter.get_object(envelope_id);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Read)? {
            return adapter.get_object(envelope_id);
        }
        self.store.get_object(envelope_id)
    }

    fn get_object_any(&self, envelope_id: &EnvelopeId) -> Result<Option<Vec<u8>>, DharmaError> {
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Read)? {
            return adapter.get_object_any(envelope_id);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Read)? {
            return adapter.get_object_any(envelope_id);
        }
        self.store.get_object_any(envelope_id)
    }

    fn get_assertion(
        &self,
        subject: &SubjectId,
        envelope_id: &EnvelopeId,
    ) -> Result<Vec<u8>, DharmaError> {
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Read)? {
            return adapter.get_assertion(subject, envelope_id);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Read)? {
            return adapter.get_assertion(subject, envelope_id);
        }
        self.store.get_assertion(subject, envelope_id)
    }

    fn scan_subject(&self, subject: &SubjectId) -> Result<Vec<AssertionId>, DharmaError> {
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Read)? {
            return adapter.scan_subject(subject);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Read)? {
            return adapter.scan_subject(subject);
        }
        self.store.scan_subject(subject)
    }

    fn list_subjects(&self) -> Result<Vec<SubjectId>, DharmaError> {
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Read)? {
            return adapter.list_subjects();
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Read)? {
            return adapter.list_subjects();
        }
        self.store.list_subjects()
    }

    fn list_objects(&self) -> Result<Vec<EnvelopeId>, DharmaError> {
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Read)? {
            return adapter.list_objects();
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Read)? {
            return adapter.list_objects();
        }
        self.store.list_objects()
    }

    fn get_permission_summary(
        &self,
        contract: &ContractId,
    ) -> Result<Option<PermissionSummary>, DharmaError> {
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Read)? {
            return adapter.get_permission_summary(contract);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Read)? {
            return adapter.get_permission_summary(contract);
        }
        self.store.get_permission_summary(contract)
    }
}

impl StorageIndex for StorageFacade {
    fn rebuild_subject_views(&self, keys: &Keyring) -> Result<(), DharmaError> {
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Index)? {
            return adapter.rebuild_subject_views(keys);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Index)? {
            return adapter.rebuild_subject_views(keys);
        }
        self.store.rebuild_subject_views(keys)
    }

    fn record_semantic(
        &self,
        assertion_id: &AssertionId,
        envelope_id: &EnvelopeId,
    ) -> Result<(), DharmaError> {
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Index)? {
            return adapter.record_semantic(assertion_id, envelope_id);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Index)? {
            return adapter.record_semantic(assertion_id, envelope_id);
        }
        self.store.record_semantic(assertion_id, envelope_id)
    }
}

impl StorageQuery for StorageFacade {
    fn lookup_envelope(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<EnvelopeId>, DharmaError> {
        if let Some(adapter) = self.clickhouse_for_operation(StorageOperation::Query)? {
            return adapter.lookup_envelope(assertion_id);
        }
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Query)? {
            return adapter.lookup_envelope(assertion_id);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Query)? {
            return adapter.lookup_envelope(assertion_id);
        }
        self.store.lookup_envelope(assertion_id)
    }

    fn lookup_cqrs_by_envelope(
        &self,
        envelope_id: &EnvelopeId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        if let Some(adapter) = self.clickhouse_for_operation(StorageOperation::Query)? {
            return adapter.lookup_cqrs_by_envelope(envelope_id);
        }
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Query)? {
            return adapter.lookup_cqrs_by_envelope(envelope_id);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Query)? {
            return adapter.lookup_cqrs_by_envelope(envelope_id);
        }
        self.store.lookup_cqrs_by_envelope(envelope_id)
    }

    fn lookup_cqrs_by_assertion(
        &self,
        assertion_id: &AssertionId,
    ) -> Result<Option<CqrsReverseEntry>, DharmaError> {
        if let Some(adapter) = self.clickhouse_for_operation(StorageOperation::Query)? {
            return adapter.lookup_cqrs_by_assertion(assertion_id);
        }
        if let Some(adapter) = self.sqlite_for_operation(StorageOperation::Query)? {
            return adapter.lookup_cqrs_by_assertion(assertion_id);
        }
        if let Some(adapter) = self.postgres_for_operation(StorageOperation::Query)? {
            return adapter.lookup_cqrs_by_assertion(assertion_id);
        }
        self.store.lookup_cqrs_by_assertion(assertion_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn mode_selection_defaults_to_embedded_sqlite() {
        let mut config = Config::default();
        config.profile.mode = "embedded".to_string();
        let selection = BackendSelection::from_config(&config);
        assert_eq!(selection.mode, RuntimeMode::Embedded);
        assert_eq!(selection.commit, BackendKind::Sqlite);
        assert_eq!(selection.read, BackendKind::Sqlite);
        assert_eq!(selection.index, BackendKind::Sqlite);
        assert_eq!(selection.query, BackendKind::Sqlite);
    }

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
    fn capability_matrix_covers_expected_backends() {
        let sqlite = BACKEND_CAPABILITY_MATRIX
            .iter()
            .find(|row| row.backend == BackendKind::Sqlite)
            .unwrap();
        assert!(sqlite.commit);
        assert!(sqlite.read);
        assert!(sqlite.index);
        assert!(sqlite.query);

        let clickhouse = BACKEND_CAPABILITY_MATRIX
            .iter()
            .find(|row| row.backend == BackendKind::ClickHouse)
            .unwrap();
        assert!(clickhouse.query);
        assert!(!clickhouse.commit);
        assert!(!clickhouse.strong_consistency);
        assert!(clickhouse.eventual_consistency);
    }

    #[test]
    fn backend_error_taxonomy_marks_transient_failures_as_retryable() {
        let network = DharmaError::Network("timeout".to_string());
        assert_eq!(
            classify_backend_error(&network),
            BackendErrorClass::Retryable
        );

        let io_err = DharmaError::Io(io::Error::new(ErrorKind::WouldBlock, "busy"));
        assert_eq!(
            classify_backend_error(&io_err),
            BackendErrorClass::Retryable
        );

        let sqlite_busy = DharmaError::Sqlite {
            code: Some("DatabaseBusy".to_string()),
            message: "database is busy".to_string(),
        };
        assert_eq!(
            classify_backend_error(&sqlite_busy),
            BackendErrorClass::Retryable
        );

        let postgres_retryable = DharmaError::Postgres {
            code: Some("40001".to_string()),
            message: "serialization failure".to_string(),
        };
        assert_eq!(
            classify_backend_error(&postgres_retryable),
            BackendErrorClass::Retryable
        );
    }

    #[test]
    fn backend_error_taxonomy_marks_validation_as_fatal() {
        let err = DharmaError::Validation("invalid signature".to_string());
        assert_eq!(classify_backend_error(&err), BackendErrorClass::Fatal);
    }

    #[test]
    fn facade_preserves_store_roundtrip_behavior() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let facade = StorageFacade::new(store, RuntimeMode::Embedded);
        assert!(temp.path().join("indexes").join("embedded.sqlite").exists());
        let envelope_id = EnvelopeId::from_bytes([7u8; 32]);
        let wasm_bytes = [0x00, 0x61, 0x73, 0x6d, 0x01];

        facade
            .put_object_if_absent(&envelope_id, &wasm_bytes)
            .unwrap();
        let loaded = facade.get_object(&envelope_id).unwrap();
        assert_eq!(loaded, wasm_bytes);
    }

    #[test]
    fn facade_embedded_init_failure_preserves_original_error_type() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("indexes"), b"not-a-directory").unwrap();

        let store = Store::from_root(temp.path());
        let facade = StorageFacade::new(store, RuntimeMode::Embedded);
        let envelope_id = EnvelopeId::from_bytes([3u8; 32]);

        let err = facade.get_object_any(&envelope_id).unwrap_err();
        match err {
            DharmaError::Io(io_err) => assert_eq!(io_err.kind(), ErrorKind::AlreadyExists),
            other => panic!("unexpected error type: {other:?}"),
        }
    }

    #[test]
    fn facade_server_query_returns_clickhouse_error_when_backend_unavailable() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut config = Config::default();
        config.profile.mode = "server".to_string();
        config.storage.postgres.url = "postgres://127.0.0.1:1/does_not_exist".to_string();
        config.storage.postgres.connect_timeout_ms = 1;
        config.storage.postgres.acquire_timeout_ms = 1;
        config.storage.clickhouse.url = "http://127.0.0.1:1".to_string();
        config.storage.clickhouse.retry_max_attempts = 1;
        config.storage.clickhouse.retry_backoff_ms = 0;

        let facade = StorageFacade::new_with_config(store, RuntimeMode::Server, &config);
        let assertion_id = AssertionId::from_bytes([9u8; 32]);
        let err = facade.lookup_envelope(&assertion_id).unwrap_err();
        assert!(matches!(
            err,
            DharmaError::ClickHouse { .. } | DharmaError::Network(_) | DharmaError::Config(_)
        ));

        let envelope_id = EnvelopeId::from_bytes([10u8; 32]);
        let err = facade.get_object_any(&envelope_id).unwrap_err();
        assert!(matches!(
            err,
            DharmaError::Network(_) | DharmaError::Postgres { .. } | DharmaError::Config(_)
        ));
    }
}
