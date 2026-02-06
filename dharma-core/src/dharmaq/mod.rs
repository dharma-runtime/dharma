use crate::assertion::AssertionPlaintext;
use crate::env::{Env, MappedBytes, StdEnv};
use crate::error::DharmaError;
use crate::lock::LockHandle;
use crate::pdl::schema::{CqrsSchema, TypeSpec as CqrsTypeSpec, Visibility};
use crate::store::state::{list_assertions, list_overlays, read_manifest};
use crate::store::Store;
use crate::types::{AssertionId, ContractId, EnvelopeId, SchemaId, SubjectId};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use ciborium::value::Value;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};

mod bitset;
use bitset::{
    BitSet, filter_eq_bytes32, filter_eq_i64_bytes, filter_eq_u32, filter_eq_u64, filter_eq_u8,
    filter_gte_i64_bytes, filter_gte_u64, filter_gt_i64_bytes, filter_gt_u64, filter_lte_i64_bytes,
    filter_lte_u64, filter_lt_i64_bytes, filter_lt_u64,
};

const ROOT_DIR: &str = "dharmaq";
const TABLE_ASSERTIONS: &str = "assertions";
const TABLE_META_FILE: &str = "table.meta";
const TABLE_KIND_CONTRACT_STATE: &str = "contract_state";
const TABLE_KIND_CONTRACT_ASSERTIONS: &str = "contract_assertions";

fn read_to_string(env: &dyn Env, path: &Path) -> Result<String, DharmaError> {
    let bytes = env.read(path)?;
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

fn file_len(env: &dyn Env, path: &Path) -> Result<u64, DharmaError> {
    if !env.exists(path) {
        return Ok(0);
    }
    env.file_len(path)
}

fn read_mapped(env: &dyn Env, path: &Path) -> Result<Option<MappedBytes>, DharmaError> {
    if !env.exists(path) {
        return Ok(None);
    }
    let bytes = env.read_mmap(path)?;
    if bytes.as_ref().is_empty() {
        return Ok(None);
    }
    Ok(Some(bytes))
}

#[derive(Clone, Debug)]
pub struct QueryPlan {
    pub table: String,
    pub filter: Option<Filter>,
    pub limit: usize,
}

#[derive(Clone, Debug)]
pub struct ContractTableSpec {
    pub name: String,
    pub lens: u64,
    pub schema_id: SchemaId,
    pub contract_id: ContractId,
    pub include_private: bool,
    pub kind: ContractTableKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContractTableKind {
    State,
    Assertions,
}

impl ContractTableKind {
    fn as_str(&self) -> &'static str {
        match self {
            ContractTableKind::State => TABLE_KIND_CONTRACT_STATE,
            ContractTableKind::Assertions => TABLE_KIND_CONTRACT_ASSERTIONS,
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            TABLE_KIND_CONTRACT_STATE => Some(ContractTableKind::State),
            TABLE_KIND_CONTRACT_ASSERTIONS => Some(ContractTableKind::Assertions),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
struct TableMeta {
    kind: String,
    contract: String,
    lens: u64,
    schema_id: SchemaId,
    contract_id: ContractId,
    manifest_len: u64,
    include_private: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Clone, Debug)]
pub enum Filter {
    Leaf(Predicate),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

#[derive(Clone, Debug)]
pub enum Predicate {
    Seq { op: CmpOp, value: u64 },
    TypEq(String),
    SubjectEq(SubjectId),
    TextSearch(String),
    DynI64 {
        col: String,
        op: CmpOp,
        value: i64,
    },
    DynBool {
        col: String,
        value: bool,
    },
    DynSymbol {
        col: String,
        value: String,
    },
    DynBytes32 {
        col: String,
        value: [u8; 32],
    },
}

#[derive(Clone, Debug)]
pub struct QueryRow {
    pub assertion_id: AssertionId,
    pub subject: SubjectId,
    pub seq: u64,
    pub typ: String,
    pub snippet: Option<String>,
    pub score: u32,
}

#[derive(Default)]
struct ColumnCache {
    partitions: HashMap<String, HashMap<String, ColumnInfo>>,
}

#[derive(Clone, Debug)]
struct ColumnInfo {
    bytes_per_row: usize,
}

struct SchemaCache {
    store: Store,
    cache: HashMap<SchemaId, Option<CqrsSchema>>,
}

impl SchemaCache {
    fn new(store: &Store) -> Self {
        Self {
            store: store.clone(),
            cache: HashMap::new(),
        }
    }

    fn load(&mut self, schema_id: &SchemaId) -> Result<Option<CqrsSchema>, DharmaError> {
        if let Some(entry) = self.cache.get(schema_id) {
            return Ok(entry.clone());
        }
        let envelope_id = EnvelopeId::from_bytes(*schema_id.as_bytes());
        let bytes = match self.store.get_object(&envelope_id) {
            Ok(bytes) => bytes,
            Err(_) => {
                self.cache.insert(*schema_id, None);
                return Ok(None);
            }
        };
        let schema = CqrsSchema::from_cbor(&bytes).ok();
        self.cache.insert(*schema_id, schema.clone());
        Ok(schema)
    }
}

pub fn execute(root: &Path, plan: &QueryPlan) -> Result<Vec<QueryRow>, DharmaError> {
    let env = StdEnv::new(root);
    execute_env(&env, plan)
}

pub fn execute_env<E>(env: &E, plan: &QueryPlan) -> Result<Vec<QueryRow>, DharmaError>
where
    E: Env + Clone + Send + Sync + 'static,
{
    execute_plan(env, plan)
}

pub fn search(root: &Path, query: &str, limit: usize) -> Result<Vec<QueryRow>, DharmaError> {
    let env = StdEnv::new(root);
    search_env(&env, query, limit)
}

pub fn search_env<E>(env: &E, query: &str, limit: usize) -> Result<Vec<QueryRow>, DharmaError>
where
    E: Env + Clone + Send + Sync + 'static,
{
    let plan = QueryPlan {
        table: TABLE_ASSERTIONS.to_string(),
        filter: Some(Filter::Leaf(Predicate::TextSearch(query.to_string()))),
        limit,
    };
    execute_plan(env, &plan)
}

fn execute_plan<E>(env: &E, plan: &QueryPlan) -> Result<Vec<QueryRow>, DharmaError>
where
    E: Env + Clone + Send + Sync + 'static,
{
    let base = dharmaq_root(env.root());
    let table = plan.table.as_str();
    if table == TABLE_ASSERTIONS {
        if !env.exists(&base) {
            let rebuilt = with_dharma_lock(env.root(), || rebuild_env(env))?;
            if rebuilt.is_none() {
                return Ok(Vec::new());
            }
        } else {
            let _ = with_dharma_lock(env.root(), || recover_wal(env))?;
        }
    } else {
        ensure_contract_table_from_meta(env, table)?;
    }
    let mut results = Vec::new();
    for partition in list_partitions(env, &base, table)? {
        let mut partition_results = execute_partition(env, &base, table, &partition, plan)?;
        results.append(&mut partition_results);
    }
    let has_text = plan
        .filter
        .as_ref()
        .map(filter_has_text_query)
        .unwrap_or(false);
    if has_text {
        results.sort_by(|a, b| b.score.cmp(&a.score));
    } else {
        results.sort_by(|a, b| b.seq.cmp(&a.seq));
    }
    results.truncate(plan.limit);
    Ok(results)
}

pub fn rebuild(root: &Path) -> Result<(), DharmaError> {
    let env = StdEnv::new(root);
    rebuild_env(&env)
}

pub fn rebuild_env<E>(env: &E) -> Result<(), DharmaError>
where
    E: Env + Clone + Send + Sync + 'static,
{
    let base = dharmaq_root(env.root());
    if !env.exists(&base) {
        env.create_dir_all(&base)?;
    }
    let table_root = table_root(&base, TABLE_ASSERTIONS);
    if env.exists(&table_root) {
        env.remove_dir_all(&table_root)?;
    }
    env.create_dir_all(&table_root.join("partitions"))?;
    env.create_dir_all(&table_root.join("sym"))?;
    env.create_dir_all(&table_root.join("hot"))?;

    let store = Store::new(env);
    let mut dict = SymDict::load(env, &table_root)?;
    let mut schema_cache = SchemaCache::new(&store);
    let mut columns = ColumnCache::default();
    for subject in store.list_subjects()? {
        ingest_subject(
            env,
            &base,
            TABLE_ASSERTIONS,
            &mut dict,
            &mut schema_cache,
            &mut columns,
            &subject,
        )?;
    }
    dict.persist(env)?;
    clear_wal(env, &table_root)?;
    Ok(())
}

pub fn ensure_contract_table(root: &Path, spec: &ContractTableSpec) -> Result<String, DharmaError> {
    let env = StdEnv::new(root);
    ensure_contract_table_env(&env, spec)
}

pub fn ensure_contract_table_env<E>(env: &E, spec: &ContractTableSpec) -> Result<String, DharmaError>
where
    E: Env + Clone + Send + Sync + 'static,
{
    let table_name = table_name_for_contract_kind(&spec.name, spec.lens, spec.kind.clone());
    let base = dharmaq_root(env.root());
    if !env.exists(&base) {
        env.create_dir_all(&base)?;
    }
    let table_root = table_root(&base, &table_name);
    let manifest_len = manifest_len(env)?;
    let needs_rebuild = match read_table_meta(env, &table_root)? {
        Some(meta) => {
            meta.kind != spec.kind.as_str()
                || meta.contract != spec.name
                || meta.lens != spec.lens
                || meta.schema_id != spec.schema_id
                || meta.contract_id != spec.contract_id
                || meta.manifest_len != manifest_len
                || meta.include_private != spec.include_private
        }
        None => true,
    };
    if needs_rebuild {
        rebuild_contract_table_env(env, spec, manifest_len)?;
    }
    Ok(table_name)
}

fn ensure_contract_table_from_meta<E>(env: &E, table: &str) -> Result<(), DharmaError>
where
    E: Env + Clone + Send + Sync + 'static,
{
    let base = dharmaq_root(env.root());
    let table_root = table_root(&base, table);
    let Some(meta) = read_table_meta(env, &table_root)? else {
        return Err(DharmaError::Validation("unknown table".to_string()));
    };
    if ContractTableKind::from_str(meta.kind.as_str()).is_none() {
        return Err(DharmaError::Validation("unknown table".to_string()));
    }
    let manifest_len = manifest_len(env)?;
    if meta.manifest_len == manifest_len {
        return Ok(());
    }
    let Some(kind) = ContractTableKind::from_str(meta.kind.as_str()) else {
        return Err(DharmaError::Validation("unknown table".to_string()));
    };
    let spec = ContractTableSpec {
        name: meta.contract,
        lens: meta.lens,
        schema_id: meta.schema_id,
        contract_id: meta.contract_id,
        include_private: meta.include_private,
        kind,
    };
    rebuild_contract_table_env(env, &spec, manifest_len)?;
    Ok(())
}

fn rebuild_contract_table_env<E>(
    env: &E,
    spec: &ContractTableSpec,
    manifest_len: u64,
) -> Result<(), DharmaError>
where
    E: Env + Clone + Send + Sync + 'static,
{
    let base = dharmaq_root(env.root());
    if !env.exists(&base) {
        env.create_dir_all(&base)?;
    }
    let table_name = table_name_for_contract_kind(&spec.name, spec.lens, spec.kind.clone());
    let table_root = table_root(&base, &table_name);
    if env.exists(&table_root) {
        env.remove_dir_all(&table_root)?;
    }
    env.create_dir_all(&table_root.join("partitions"))?;
    env.create_dir_all(&table_root.join("sym"))?;
    env.create_dir_all(&table_root.join("hot"))?;

    let store = Store::new(env);
    let schema = load_schema(&store, &spec.schema_id)?;
    let contract = load_contract(&store, &spec.contract_id)?;
    let mut dict = SymDict::load(env, &table_root)?;
    let mut columns = ColumnCache::default();

    let subjects = subjects_for_schema(&store, &spec.schema_id, spec.lens)?;
    match spec.kind {
        ContractTableKind::State => {
            for subject in subjects {
                if let Some(state_row) = build_state_row(
                    env,
                    &store,
                    &schema,
                    &contract,
                    spec.lens,
                    &subject,
                    spec.include_private,
                )? {
                    append_state_row(
                        env,
                        &base,
                        &table_name,
                        &mut dict,
                        &mut columns,
                        &schema,
                        &state_row,
                    )?;
                }
            }
        }
        ContractTableKind::Assertions => {
            for subject in subjects {
                ingest_contract_assertions(
                    env,
                    &base,
                    &table_name,
                    &schema,
                    &spec.schema_id,
                    spec.lens,
                    &subject,
                    spec.include_private,
                    &mut dict,
                    &mut columns,
                )?;
            }
        }
    }
    dict.persist(env)?;
    let meta = TableMeta {
        kind: spec.kind.as_str().to_string(),
        contract: spec.name.clone(),
        lens: spec.lens,
        schema_id: spec.schema_id,
        contract_id: spec.contract_id,
        manifest_len,
        include_private: spec.include_private,
    };
    write_table_meta(env, &table_root, &meta)?;
    Ok(())
}

#[derive(Clone)]
struct StateRow {
    subject: SubjectId,
    seq: u64,
    assertion_id: AssertionId,
    ts: u64,
    typ: String,
    value: Value,
}

fn load_schema(store: &Store, schema_id: &SchemaId) -> Result<CqrsSchema, DharmaError> {
    let envelope_id = EnvelopeId::from_bytes(*schema_id.as_bytes());
    let bytes = store.get_object(&envelope_id)?;
    CqrsSchema::from_cbor(&bytes)
}

fn load_contract(store: &Store, contract_id: &ContractId) -> Result<Vec<u8>, DharmaError> {
    let envelope_id = EnvelopeId::from_bytes(*contract_id.as_bytes());
    let Some(bytes) = store.get_verified_contract(&envelope_id)? else {
        return Err(DharmaError::Validation("missing contract".to_string()));
    };
    Ok(bytes)
}

fn subjects_for_schema(
    store: &Store,
    schema_id: &SchemaId,
    lens: u64,
) -> Result<Vec<SubjectId>, DharmaError> {
    let env = store.env();
    let mut out = Vec::new();
    for subject in store.list_subjects()? {
        let records = list_assertions(env, &subject)?;
        if records.iter().any(|record| {
            if let Ok(assertion) = AssertionPlaintext::from_cbor(&record.bytes) {
                assertion.header.schema == *schema_id && assertion.header.ver == lens
            } else {
                false
            }
        }) {
            out.push(subject);
        }
    }
    Ok(out)
}

fn lookup_assertion_ts(store: &Store, assertion_id: &AssertionId) -> Option<u64> {
    let env = store
        .lookup_envelope(assertion_id)
        .ok()
        .flatten()?;
    let bytes = store.get_object(&env).ok()?;
    let assertion = AssertionPlaintext::from_cbor(&bytes).ok()?;
    assertion.header.ts.map(|ts| ts.max(0) as u64)
}

fn build_state_row(
    env: &dyn Env,
    store: &Store,
    schema: &CqrsSchema,
    contract: &[u8],
    lens: u64,
    subject: &SubjectId,
    include_private: bool,
) -> Result<Option<StateRow>, DharmaError> {
    let state = crate::runtime::cqrs::load_state(env, subject, schema, contract, lens)?;
    let base_seq = state.last_seq;
    let overlay_seq = state.last_overlay_seq;
    if base_seq == 0 && overlay_seq == 0 {
        return Ok(None);
    }
    let (seq, assertion_id) = if overlay_seq > base_seq {
        (
            overlay_seq,
            state
                .last_overlay_object
                .unwrap_or_else(|| AssertionId::from_bytes([0u8; 32])),
        )
    } else {
        (
            base_seq,
            state
                .last_object
                .unwrap_or_else(|| AssertionId::from_bytes([0u8; 32])),
        )
    };
    let ts = lookup_assertion_ts(store, &assertion_id).unwrap_or(0);
    let mut value = crate::runtime::cqrs::decode_state(&state.memory, schema)?;
    if !include_private {
        value = filter_private_fields(schema, &value)?;
    }
    Ok(Some(StateRow {
        subject: *subject,
        seq,
        assertion_id,
        ts,
        typ: schema.namespace.clone(),
        value,
    }))
}

fn ingest_subject(
    env: &dyn Env,
    base: &Path,
    table: &str,
    dict: &mut SymDict,
    schema_cache: &mut SchemaCache,
    columns: &mut ColumnCache,
    subject: &SubjectId,
) -> Result<(), DharmaError> {
    for record in list_assertions(env, subject)? {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        let schema = schema_cache.load(&assertion.header.schema)?;
        append_row(
            env,
            base,
            table,
            dict,
            columns,
            &assertion,
            schema.as_ref(),
            &record.assertion_id,
            true,
        )?;
    }
    for record in list_overlays(env, subject)? {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        let schema = schema_cache.load(&assertion.header.schema)?;
        append_row(
            env,
            base,
            table,
            dict,
            columns,
            &assertion,
            schema.as_ref(),
            &record.assertion_id,
            true,
        )?;
    }
    Ok(())
}

fn ingest_contract_assertions(
    env: &dyn Env,
    base: &Path,
    table: &str,
    schema: &CqrsSchema,
    schema_id: &SchemaId,
    lens: u64,
    subject: &SubjectId,
    include_private: bool,
    dict: &mut SymDict,
    columns: &mut ColumnCache,
) -> Result<(), DharmaError> {
    for record in list_assertions(env, subject)? {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        if assertion.header.schema != *schema_id || assertion.header.ver != lens {
            continue;
        }
        append_row(
            env,
            base,
            table,
            dict,
            columns,
            &assertion,
            Some(schema),
            &record.assertion_id,
            include_private,
        )?;
    }
    for record in list_overlays(env, subject)? {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        if assertion.header.schema != *schema_id || assertion.header.ver != lens {
            continue;
        }
        append_row(
            env,
            base,
            table,
            dict,
            columns,
            &assertion,
            Some(schema),
            &record.assertion_id,
            include_private,
        )?;
    }
    Ok(())
}

fn append_row(
    env: &dyn Env,
    base: &Path,
    table: &str,
    dict: &mut SymDict,
    columns: &mut ColumnCache,
    assertion: &AssertionPlaintext,
    schema: Option<&CqrsSchema>,
    assertion_id: &AssertionId,
    include_private: bool,
) -> Result<(), DharmaError> {
    let ts = assertion.header.ts.unwrap_or(0);
    let ts_u64 = ts.max(0) as u64;
    let partition = partition_for(ts_u64);
    let partition_dir = partition_dir(base, table, &partition);
    env.create_dir_all(&partition_dir.join("cols"))?;
    env.create_dir_all(&partition_dir.join("text"))?;

    let row_id = row_count(env, &partition_dir)?;
    let text = extract_text(&assertion.body);
    let normalized = normalize_text(&text);
    let entry = WalEntry {
        partition,
        row_id,
        assertion_id: *assertion_id,
        subject: assertion.header.sub,
        seq: assertion.header.seq,
        typ: assertion.header.typ.clone(),
        text: normalized,
        ts: ts_u64,
    };
    wal_append(env, &table_root(base, table), &entry)?;
    apply_row(
        env,
        base,
        table,
        dict,
        columns,
        &entry,
        Some(assertion),
        schema,
        include_private,
    )?;
    Ok(())
}

fn apply_row(
    env: &dyn Env,
    base: &Path,
    table: &str,
    dict: &mut SymDict,
    columns: &mut ColumnCache,
    entry: &WalEntry,
    assertion: Option<&AssertionPlaintext>,
    schema: Option<&CqrsSchema>,
    include_private: bool,
) -> Result<(), DharmaError> {
    let partition_dir = partition_dir(base, table, &entry.partition);
    env.create_dir_all(&partition_dir.join("cols"))?;
    env.create_dir_all(&partition_dir.join("text"))?;

    let expected_row = row_count(env, &partition_dir)?;
    if entry.row_id < expected_row {
        return Ok(());
    }
    if entry.row_id > expected_row {
        return Err(DharmaError::Validation("wal row_id gap".to_string()));
    }

    append_fixed(env, &partition_dir, "object_id.bin", entry.assertion_id.as_bytes())?;
    append_fixed(env, &partition_dir, "subject_id.bin", entry.subject.as_bytes())?;
    append_u64(env, &partition_dir, "seq.bin", entry.seq)?;

    let typ_id = dict.intern(&entry.typ)?;
    append_u32(env, &partition_dir, "typ.bin", typ_id)?;

    append_text(env, &partition_dir, &entry.text)?;
    let trigs = trigrams(&entry.text);
    append_trigram_count(env, &partition_dir, trigs.len() as u16)?;
    append_trigram_index(env, &partition_dir, entry.row_id, &trigs)?;
    apply_dynamic_columns(
        env,
        &partition_dir,
        entry.row_id,
        dict,
        columns,
        assertion,
        schema,
        include_private,
    )?;
    Ok(())
}

fn append_state_row(
    env: &dyn Env,
    base: &Path,
    table: &str,
    dict: &mut SymDict,
    columns: &mut ColumnCache,
    schema: &CqrsSchema,
    row: &StateRow,
) -> Result<(), DharmaError> {
    let partition = partition_for(row.ts);
    let partition_dir = partition_dir(base, table, &partition);
    env.create_dir_all(&partition_dir.join("cols"))?;
    env.create_dir_all(&partition_dir.join("text"))?;

    let row_id = row_count(env, &partition_dir)?;
    append_fixed(
        env,
        &partition_dir,
        "object_id.bin",
        row.assertion_id.as_bytes(),
    )?;
    append_fixed(env, &partition_dir, "subject_id.bin", row.subject.as_bytes())?;
    append_u64(env, &partition_dir, "seq.bin", row.seq)?;
    let typ_id = dict.intern(&row.typ)? as u32;
    append_u32(env, &partition_dir, "typ.bin", typ_id)?;

    let text = extract_text(&row.value);
    let normalized = normalize_text(&text);
    append_text(env, &partition_dir, &normalized)?;
    let trigs = trigrams(&normalized);
    append_trigram_count(env, &partition_dir, trigs.len() as u16)?;
    append_trigram_index(env, &partition_dir, row_id, &trigs)?;
    apply_state_columns(env, &partition_dir, row_id, dict, columns, schema, &row.value)?;
    Ok(())
}

struct ColumnWrite {
    name: String,
    bytes: Vec<u8>,
    valid: bool,
    bytes_per_row: usize,
}

fn apply_dynamic_columns(
    env: &dyn Env,
    partition_dir: &Path,
    row_id: u64,
    dict: &mut SymDict,
    columns: &mut ColumnCache,
    assertion: Option<&AssertionPlaintext>,
    schema: Option<&CqrsSchema>,
    include_private: bool,
) -> Result<(), DharmaError> {
    let partition = partition_dir
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    if !columns.partitions.contains_key(&partition) {
        let scanned = scan_dynamic_columns(env, partition_dir, row_id)?;
        columns.partitions.insert(partition.clone(), scanned);
    }
    let col_map = columns
        .partitions
        .get_mut(&partition)
        .ok_or_else(|| DharmaError::Validation("missing column cache".to_string()))?;

    let writes = build_dynamic_writes(dict, assertion, schema, include_private)?;
    let mut seen = HashSet::new();
    for write in &writes {
        seen.insert(write.name.clone());
        let info = col_map
            .entry(write.name.clone())
            .or_insert(ColumnInfo { bytes_per_row: 0 });
        if info.bytes_per_row == 0 {
            info.bytes_per_row = write.bytes_per_row;
        }
        if info.bytes_per_row != write.bytes_per_row {
            return Err(DharmaError::Validation("column size mismatch".to_string()));
        }
        ensure_bin_padding(env, partition_dir, &write.name, info.bytes_per_row, row_id)?;
        append_bin_value(env, partition_dir, &write.name, &write.bytes)?;
        append_valid_bit(env, partition_dir, &write.name, row_id, write.valid)?;
    }

    for (name, info) in col_map.iter() {
        if seen.contains(name) {
            continue;
        }
        if info.bytes_per_row == 0 {
            continue;
        }
        ensure_bin_padding(env, partition_dir, name, info.bytes_per_row, row_id)?;
        append_zero_bytes(env, partition_dir, name, info.bytes_per_row)?;
        append_valid_bit(env, partition_dir, name, row_id, false)?;
    }
    Ok(())
}

fn apply_state_columns(
    env: &dyn Env,
    partition_dir: &Path,
    row_id: u64,
    dict: &mut SymDict,
    columns: &mut ColumnCache,
    schema: &CqrsSchema,
    value: &Value,
) -> Result<(), DharmaError> {
    let partition = partition_dir
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    if !columns.partitions.contains_key(&partition) {
        let scanned = scan_dynamic_columns(env, partition_dir, row_id)?;
        columns.partitions.insert(partition.clone(), scanned);
    }
    let col_map = columns
        .partitions
        .get_mut(&partition)
        .ok_or_else(|| DharmaError::Validation("missing column cache".to_string()))?;

    let writes = build_state_writes(dict, schema, value)?;
    let mut seen = HashSet::new();
    for write in &writes {
        seen.insert(write.name.clone());
        let info = col_map
            .entry(write.name.clone())
            .or_insert(ColumnInfo { bytes_per_row: 0 });
        if info.bytes_per_row == 0 {
            info.bytes_per_row = write.bytes_per_row;
        }
        if info.bytes_per_row != write.bytes_per_row {
            return Err(DharmaError::Validation("column size mismatch".to_string()));
        }
        ensure_bin_padding(env, partition_dir, &write.name, info.bytes_per_row, row_id)?;
        append_bin_value(env, partition_dir, &write.name, &write.bytes)?;
        append_valid_bit(env, partition_dir, &write.name, row_id, write.valid)?;
    }

    for (name, info) in col_map.iter() {
        if seen.contains(name) {
            continue;
        }
        if info.bytes_per_row == 0 {
            continue;
        }
        ensure_bin_padding(env, partition_dir, name, info.bytes_per_row, row_id)?;
        append_zero_bytes(env, partition_dir, name, info.bytes_per_row)?;
        append_valid_bit(env, partition_dir, name, row_id, false)?;
    }
    Ok(())
}

fn build_dynamic_writes(
    dict: &mut SymDict,
    assertion: Option<&AssertionPlaintext>,
    schema: Option<&CqrsSchema>,
    include_private: bool,
) -> Result<Vec<ColumnWrite>, DharmaError> {
    let assertion = match assertion {
        Some(assertion) => assertion,
        None => return Ok(Vec::new()),
    };
    let schema = match schema {
        Some(schema) => schema,
        None => return Ok(Vec::new()),
    };
    let action_name = assertion
        .header
        .typ
        .strip_prefix("action.")
        .unwrap_or(&assertion.header.typ);
    let action_schema = match schema.action(action_name) {
        Some(schema) => schema,
        None => return Ok(Vec::new()),
    };
    let body_map = extract_body_map(&assertion.body);
    let mut writes = Vec::new();
    for (name, typ) in &action_schema.args {
        if !include_private {
            if let Some(vis) = action_schema.arg_vis.get(name) {
                if *vis == Visibility::Private {
                    continue;
                }
            }
        }
        let value = body_map.get(name);
        append_field_writes(dict, name, typ, value, &mut writes)?;
    }
    Ok(writes)
}

fn build_state_writes(
    dict: &mut SymDict,
    schema: &CqrsSchema,
    value: &Value,
) -> Result<Vec<ColumnWrite>, DharmaError> {
    let state_map = extract_state_map(value);
    let mut writes = Vec::new();
    for (name, field) in &schema.fields {
        let value = state_map.get(name);
        append_state_field_writes(dict, name, &field.typ, value, &mut writes)?;
    }
    Ok(writes)
}

fn extract_body_map(value: &Value) -> HashMap<String, Value> {
    let mut out = HashMap::new();
    if let Value::Map(entries) = value {
        for (k, v) in entries {
            if let Value::Text(name) = k {
                out.insert(name.clone(), v.clone());
            }
        }
    }
    out
}

fn extract_state_map(value: &Value) -> HashMap<String, Value> {
    extract_body_map(value)
}

fn filter_private_fields(schema: &CqrsSchema, value: &Value) -> Result<Value, DharmaError> {
    let map = crate::value::expect_map(value)?;
    let mut out = Vec::new();
    for (k, v) in map {
        if let Value::Text(name) = k {
            if let Some(field) = schema.fields.get(name) {
                if field.visibility == Visibility::Private {
                    continue;
                }
            }
        }
        out.push((k.clone(), v.clone()));
    }
    Ok(Value::Map(out))
}

fn append_state_field_writes(
    dict: &mut SymDict,
    name: &str,
    typ: &CqrsTypeSpec,
    value: Option<&Value>,
    writes: &mut Vec<ColumnWrite>,
) -> Result<(), DharmaError> {
    if let Some(Value::Null) = value {
        return append_state_field_writes(dict, name, typ, None, writes);
    }
    match typ {
        CqrsTypeSpec::Optional(inner) => {
            return append_state_field_writes(dict, name, inner, value, writes);
        }
        CqrsTypeSpec::Ratio | CqrsTypeSpec::List(_) | CqrsTypeSpec::Map(_, _) => {
            return Ok(());
        }
        _ => {}
    }
    append_field_writes(dict, name, typ, value, writes)
}

fn append_field_writes(
    dict: &mut SymDict,
    name: &str,
    typ: &CqrsTypeSpec,
    value: Option<&Value>,
    writes: &mut Vec<ColumnWrite>,
) -> Result<(), DharmaError> {
    if let Some(Value::Null) = value {
        return append_field_writes(dict, name, typ, None, writes);
    }
    match typ {
        CqrsTypeSpec::Optional(inner) => {
            return append_field_writes(dict, name, inner, value, writes);
        }
        CqrsTypeSpec::Int
        | CqrsTypeSpec::Decimal(_)
        | CqrsTypeSpec::Duration
        | CqrsTypeSpec::Timestamp => {
            let (bytes, valid) = match value {
                Some(Value::Integer(int)) => {
                    let parsed: i64 = (*int).try_into().unwrap_or(0i64);
                    (parsed.to_le_bytes().to_vec(), true)
                }
                _ => (vec![0u8; 8], false),
            };
            writes.push(ColumnWrite {
                name: name.to_string(),
                bytes,
                valid,
                bytes_per_row: 8,
            });
        }
        CqrsTypeSpec::Bool => {
            let (bytes, valid) = match value {
                Some(Value::Bool(b)) => (vec![if *b { 1 } else { 0 }], true),
                _ => (vec![0u8; 1], false),
            };
            writes.push(ColumnWrite {
                name: name.to_string(),
                bytes,
                valid,
                bytes_per_row: 1,
            });
        }
        CqrsTypeSpec::Enum(_) | CqrsTypeSpec::Currency | CqrsTypeSpec::Text(_) => {
            let (bytes, valid) = match value {
                Some(Value::Text(text)) => {
                    let id = dict.intern(text)? as u32;
                    (id.to_le_bytes().to_vec(), true)
                }
                _ => (vec![0u8; 4], false),
            };
            writes.push(ColumnWrite {
                name: name.to_string(),
                bytes,
                valid,
                bytes_per_row: 4,
            });
        }
        CqrsTypeSpec::Identity | CqrsTypeSpec::Ref(_) => {
            let (bytes, valid) = match value {
                Some(Value::Bytes(bytes)) if bytes.len() == 32 => (bytes.clone(), true),
                Some(Value::Text(text)) => match parse_hex_bytes32(text) {
                    Some(bytes) => (bytes.to_vec(), true),
                    None => (vec![0u8; 32], false),
                },
                _ => (vec![0u8; 32], false),
            };
            writes.push(ColumnWrite {
                name: name.to_string(),
                bytes,
                valid,
                bytes_per_row: 32,
            });
        }
        CqrsTypeSpec::SubjectRef(_) => {
            let (bytes, valid) = match value {
                Some(Value::Map(entries)) => {
                    let mut id: Option<Vec<u8>> = None;
                    let mut seq: Option<u64> = None;
                    for (k, v) in entries {
                        if let Value::Text(key) = k {
                            if key == "id" {
                                if let Value::Bytes(bytes) = v {
                                    if bytes.len() == 32 {
                                        id = Some(bytes.clone());
                                    }
                                }
                            } else if key == "seq" {
                                if let Value::Integer(int) = v {
                                    seq = (*int).try_into().ok();
                                }
                            }
                        }
                    }
                    if let (Some(id), Some(seq)) = (id, seq) {
                        let mut out = vec![0u8; 40];
                        out[..32].copy_from_slice(&id);
                        out[32..40].copy_from_slice(&seq.to_le_bytes());
                        (out, true)
                    } else {
                        (vec![0u8; 40], false)
                    }
                }
                _ => (vec![0u8; 40], false),
            };
            writes.push(ColumnWrite {
                name: name.to_string(),
                bytes,
                valid,
                bytes_per_row: 40,
            });
        }
        CqrsTypeSpec::GeoPoint => {
            let (lat, lon, valid) = parse_geo_point(value);
            writes.push(ColumnWrite {
                name: format!("{name}_lat"),
                bytes: (lat as i32).to_le_bytes().to_vec(),
                valid,
                bytes_per_row: 4,
            });
            writes.push(ColumnWrite {
                name: format!("{name}_lon"),
                bytes: (lon as i32).to_le_bytes().to_vec(),
                valid,
                bytes_per_row: 4,
            });
        }
        CqrsTypeSpec::Ratio => {
            return Err(DharmaError::Validation("ratio unsupported in dharma-q".to_string()));
        }
        CqrsTypeSpec::Struct(_) => {
            return Err(DharmaError::Validation("struct unsupported in dharma-q".to_string()));
        }
        CqrsTypeSpec::List(_) | CqrsTypeSpec::Map(_, _) => {
            return Err(DharmaError::Validation("collection unsupported in dharma-q".to_string()));
        }
    }
    Ok(())
}

fn parse_geo_point(value: Option<&Value>) -> (i32, i32, bool) {
    match value {
        Some(Value::Map(entries)) => {
            let mut lat: Option<i32> = None;
            let mut lon: Option<i32> = None;
            for (k, v) in entries {
                if let Value::Text(name) = k {
                    if name == "lat" {
                        if let Value::Integer(int) = v {
                            lat = (*int).try_into().ok();
                        }
                    } else if name == "lon" {
                        if let Value::Integer(int) = v {
                            lon = (*int).try_into().ok();
                        }
                    }
                }
            }
            if let (Some(lat), Some(lon)) = (lat, lon) {
                return (lat, lon, true);
            }
            (0, 0, false)
        }
        Some(Value::Array(items)) => {
            if items.len() == 2 {
                if let (Value::Integer(lat), Value::Integer(lon)) = (&items[0], &items[1]) {
                    if let (Ok(lat), Ok(lon)) = ((*lat).try_into(), (*lon).try_into()) {
                        return (lat, lon, true);
                    }
                }
            }
            (0, 0, false)
        }
        _ => (0, 0, false),
    }
}

fn parse_hex_bytes32(text: &str) -> Option<[u8; 32]> {
    let hex = text.trim_start_matches("0x");
    if hex.len() != 64 {
        return None;
    }
    let bytes = crate::types::hex_decode(hex).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Some(out)
}

fn scan_dynamic_columns(
    env: &dyn Env,
    partition_dir: &Path,
    row_id: u64,
) -> Result<HashMap<String, ColumnInfo>, DharmaError> {
    let mut out = HashMap::new();
    let cols_dir = partition_dir.join("cols");
    if !env.exists(&cols_dir) {
        return Ok(out);
    }
    let base_cols = [
        "object_id.bin",
        "subject_id.bin",
        "seq.bin",
        "typ.bin",
        "text.bin",
        "text.off",
    ];
    let base_set: HashSet<&str> = base_cols.iter().copied().collect();
    for path in env.list_dir(&cols_dir)? {
        if !env.is_file(&path) {
            continue;
        }
        let Some(name) = path.file_name() else { continue };
        let name = name.to_string_lossy().to_string();
        if base_set.contains(name.as_str()) || !name.ends_with(".bin") {
            continue;
        }
        let col_name = name.trim_end_matches(".bin").to_string();
        let len = file_len(env, &path)?;
        let bytes_per_row = if row_id == 0 {
            if len == 0 {
                0
            } else {
                return Err(DharmaError::Validation("column length mismatch".to_string()));
            }
        } else {
        let row_id = row_id as u64;
            if len % row_id != 0 {
                return Err(DharmaError::Validation("column length mismatch".to_string()));
            }
            (len / row_id) as usize
        };
        out.insert(col_name, ColumnInfo { bytes_per_row });
    }
    Ok(out)
}

fn ensure_bin_padding(
    env: &dyn Env,
    partition_dir: &Path,
    name: &str,
    bytes_per_row: usize,
    row_id: u64,
) -> Result<(), DharmaError> {
    let path = partition_dir.join("cols").join(format!("{name}.bin"));
    let target = row_id
        .checked_mul(bytes_per_row as u64)
        .ok_or_else(|| DharmaError::Validation("column length overflow".to_string()))?;
    let current = file_len(env, &path)?;
    if current == target {
        return Ok(());
    }
    if current > target {
        return Err(DharmaError::Validation("column length mismatch".to_string()));
    }
    let pad = (target - current) as usize;
    if pad > 0 {
        env.append(&path, &vec![0u8; pad])?;
    }
    Ok(())
}

fn append_bin_value(env: &dyn Env, partition_dir: &Path, name: &str, bytes: &[u8]) -> Result<(), DharmaError> {
    let path = partition_dir.join("cols").join(format!("{name}.bin"));
    env.append(&path, bytes)?;
    Ok(())
}

fn append_zero_bytes(
    env: &dyn Env,
    partition_dir: &Path,
    name: &str,
    bytes_per_row: usize,
) -> Result<(), DharmaError> {
    let zeros = vec![0u8; bytes_per_row];
    append_bin_value(env, partition_dir, name, &zeros)
}

fn append_valid_bit(
    env: &dyn Env,
    partition_dir: &Path,
    name: &str,
    row_id: u64,
    valid: bool,
) -> Result<(), DharmaError> {
    let path = partition_dir.join("cols").join(format!("{name}.valid"));
    let byte_index = row_id / 8;
    let bit_index = (row_id % 8) as u8;
    let mut buf = if env.exists(&path) {
        env.read(&path)?
    } else {
        Vec::new()
    };
    let target_len = (byte_index + 1) as usize;
    if buf.len() < target_len {
        buf.resize(target_len, 0);
    }
    if valid {
        let byte = &mut buf[byte_index as usize];
        *byte |= 1u8 << bit_index;
    }
    env.write(&path, &buf)?;
    Ok(())
}

fn dharmaq_root(root: &Path) -> PathBuf {
    root.join(ROOT_DIR)
}

fn table_root(base: &Path, table: &str) -> PathBuf {
    base.join("tables").join(table)
}

fn partition_dir(base: &Path, table: &str, partition: &str) -> PathBuf {
    table_root(base, table).join("partitions").join(partition)
}

fn list_partitions(env: &dyn Env, base: &Path, table: &str) -> Result<Vec<String>, DharmaError> {
    let dir = table_root(base, table).join("partitions");
    let mut out = Vec::new();
    if !env.exists(&dir) {
        return Ok(out);
    }
    for path in env.list_dir(&dir)? {
        if env.is_dir(&path) {
            if let Some(name) = path.file_name() {
                out.push(name.to_string_lossy().to_string());
            }
        }
    }
    Ok(out)
}

fn table_meta_path(table_root: &Path) -> PathBuf {
    table_root.join(TABLE_META_FILE)
}

fn table_name_for_contract(name: &str, lens: u64) -> String {
    format!("{name}@v{lens}")
}

fn table_name_for_contract_kind(name: &str, lens: u64, kind: ContractTableKind) -> String {
    match kind {
        ContractTableKind::State => table_name_for_contract(name, lens),
        ContractTableKind::Assertions => format!("{name}@v{lens}.assertions"),
    }
}

fn manifest_len(env: &dyn Env) -> Result<u64, DharmaError> {
    let entries = read_manifest(env)?;
    Ok(entries.len() as u64)
}

fn read_table_meta(env: &dyn Env, table_root: &Path) -> Result<Option<TableMeta>, DharmaError> {
    let path = table_meta_path(table_root);
    if !env.exists(&path) {
        return Ok(None);
    }
    let contents = read_to_string(env, &path)?;
    let mut kind = None;
    let mut contract = None;
    let mut lens = None;
    let mut schema_id = None;
    let mut contract_id = None;
    let mut manifest_len = None;
    let mut include_private = None;
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let mut value = value.trim();
        if let Some(stripped) = value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) {
            value = stripped;
        }
        match key {
            "kind" => kind = Some(value.to_string()),
            "contract" => contract = Some(value.to_string()),
            "lens" => lens = value.parse::<u64>().ok(),
            "schema" => schema_id = SchemaId::from_hex(value).ok(),
            "contract_id" => contract_id = ContractId::from_hex(value).ok(),
            "manifest_len" => manifest_len = value.parse::<u64>().ok(),
            "include_private" => include_private = Some(value == "true"),
            _ => {}
        }
    }
    let Some(kind) = kind else { return Ok(None); };
    let Some(contract) = contract else { return Ok(None); };
    let Some(lens) = lens else { return Ok(None); };
    let Some(schema_id) = schema_id else { return Ok(None); };
    let Some(contract_id) = contract_id else { return Ok(None); };
    let manifest_len = manifest_len.unwrap_or(0);
    let include_private = include_private.unwrap_or(true);
    Ok(Some(TableMeta {
        kind,
        contract,
        lens,
        schema_id,
        contract_id,
        manifest_len,
        include_private,
    }))
}

fn write_table_meta(env: &dyn Env, table_root: &Path, meta: &TableMeta) -> Result<(), DharmaError> {
    let mut lines = Vec::new();
    lines.push(format!("kind = \"{}\"", meta.kind));
    lines.push(format!("contract = \"{}\"", meta.contract));
    lines.push(format!("lens = {}", meta.lens));
    lines.push(format!("schema = \"{}\"", meta.schema_id.to_hex()));
    lines.push(format!("contract_id = \"{}\"", meta.contract_id.to_hex()));
    lines.push(format!("manifest_len = {}", meta.manifest_len));
    lines.push(format!("include_private = {}", meta.include_private));
    let contents = lines.join("\n") + "\n";
    env.write(&table_meta_path(table_root), contents.as_bytes())?;
    Ok(())
}

fn partition_for(ts: u64) -> String {
    let days = ts / 86_400;
    let (y, m, d) = civil_from_days(days as i64);
    format!("p={:04}.{:02}.{:02}", y, m, d)
}

fn civil_from_days(days: i64) -> (i32, u8, u8) {
    let z = days + 719468;
    let era = if z >= 0 { z / 146097 } else { (z - 146096) / 146097 };
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year as i32, m as u8, d as u8)
}

fn row_count(env: &dyn Env, partition_dir: &Path) -> Result<u64, DharmaError> {
    let path = partition_dir.join("cols").join("seq.bin");
    if !env.exists(&path) {
        return Ok(0);
    }
    let len = file_len(env, &path)?;
    Ok(len / 8)
}

fn append_fixed(env: &dyn Env, partition_dir: &Path, name: &str, bytes: &[u8]) -> Result<(), DharmaError> {
    let path = partition_dir.join("cols").join(name);
    env.append(&path, bytes)?;
    Ok(())
}

fn append_u64(env: &dyn Env, partition_dir: &Path, name: &str, value: u64) -> Result<(), DharmaError> {
    let path = partition_dir.join("cols").join(name);
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<LittleEndian>(value)?;
    env.append(&path, &buf)?;
    Ok(())
}

fn append_u32(env: &dyn Env, partition_dir: &Path, name: &str, value: u32) -> Result<(), DharmaError> {
    let path = partition_dir.join("cols").join(name);
    let mut buf = Vec::with_capacity(4);
    buf.write_u32::<LittleEndian>(value)?;
    env.append(&path, &buf)?;
    Ok(())
}

fn append_text(env: &dyn Env, partition_dir: &Path, text: &str) -> Result<(), DharmaError> {
    let text_path = partition_dir.join("cols").join("text.bin");
    let off_path = partition_dir.join("cols").join("text.off");
    let offset = file_len(env, &text_path)?;
    env.append(&text_path, text.as_bytes())?;
    let mut off_buf = Vec::with_capacity(12);
    off_buf.write_u64::<LittleEndian>(offset)?;
    off_buf.write_u32::<LittleEndian>(text.len() as u32)?;
    env.append(&off_path, &off_buf)?;
    Ok(())
}

fn append_trigram_count(env: &dyn Env, partition_dir: &Path, count: u16) -> Result<(), DharmaError> {
    let path = partition_dir.join("text").join("tcnt.bin");
    let mut buf = Vec::with_capacity(2);
    buf.write_u16::<LittleEndian>(count)?;
    env.append(&path, &buf)?;
    Ok(())
}

fn append_trigram_index(
    env: &dyn Env,
    partition_dir: &Path,
    row_id: u64,
    trigs: &[u32],
) -> Result<(), DharmaError> {
    if trigs.is_empty() {
        return Ok(());
    }
    let path = partition_dir.join("text").join("trigrams.bin");
    let mut buf = Vec::with_capacity(trigs.len() * 12);
    for trigram in trigs {
        buf.write_u32::<LittleEndian>(*trigram)?;
        buf.write_u64::<LittleEndian>(row_id)?;
    }
    env.append(&path, &buf)?;
    Ok(())
}

fn execute_partition(
    env: &dyn Env,
    base: &Path,
    table: &str,
    partition: &str,
    plan: &QueryPlan,
) -> Result<Vec<QueryRow>, DharmaError> {
    let partition_dir = partition_dir(base, table, partition);
    let table_root = table_root(base, table);
    let maps = match load_partition_maps(env, &table_root, &partition_dir)? {
        Some(maps) => maps,
        None => return Ok(Vec::new()),
    };
    let row_count = row_count(env, &partition_dir)? as usize;
    if row_count == 0 {
        return Ok(Vec::new());
    }
    let mut results = Vec::new();
    let snippet_query = plan
        .filter
        .as_ref()
        .and_then(first_text_query)
        .unwrap_or_default();
    let (mask, scores) = if let Some(filter) = &plan.filter {
        mask_for_filter(env, filter, &maps, &partition_dir, row_count)?
    } else {
        (BitSet::filled(row_count), None)
    };
    if mask.is_empty() {
        return Ok(Vec::new());
    }
    let scores = scores.unwrap_or_else(|| vec![0u32; row_count]);
    for idx in mask.iter_ones() {
        let score = scores.get(idx).copied().unwrap_or(0);
        if let Some(row) = read_row(&maps, idx as u64, &snippet_query, score) {
            results.push(row);
        }
    }
    Ok(results)
}

fn text_candidates(
    env: &dyn Env,
    partition_dir: &Path,
    row_count: usize,
    query_trigs: &[u32],
) -> Result<(BitSet, Vec<u32>), DharmaError> {
    let tri_path = partition_dir.join("text").join("trigrams.bin");
    let tri_map = match read_mapped(env, &tri_path)? {
        Some(bytes) => bytes,
        None => return Ok((BitSet::new(row_count), vec![0u32; row_count])),
    };
    let tcnt_path = partition_dir.join("text").join("tcnt.bin");
    let tcnt_map = match read_mapped(env, &tcnt_path)? {
        Some(bytes) => bytes,
        None => return Ok((BitSet::new(row_count), vec![0u32; row_count])),
    };
    let query_set: HashSet<u32> = query_trigs.iter().copied().collect();
    let mut hits = BitSet::new(row_count);
    let mut overlaps = vec![0u16; row_count];
    for chunk in tri_map.as_ref().chunks_exact(12) {
        let trigram = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        if !query_set.contains(&trigram) {
            continue;
        }
        let row_id = u64::from_le_bytes([
            chunk[4], chunk[5], chunk[6], chunk[7], chunk[8], chunk[9], chunk[10], chunk[11],
        ]);
        let idx = match usize::try_from(row_id) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if idx >= row_count {
            continue;
        }
        hits.set(idx);
        overlaps[idx] = overlaps[idx].saturating_add(1);
    }
    let mut scores = vec![0u32; row_count];
    for idx in hits.iter_ones() {
        let overlap = overlaps[idx] as i64;
        if overlap == 0 {
            continue;
        }
        let offset = idx * 2;
        if offset + 2 > tcnt_map.as_ref().len() {
            continue;
        }
        let tcnt_bytes = tcnt_map.as_ref();
        let row_trigs = u16::from_le_bytes([tcnt_bytes[offset], tcnt_bytes[offset + 1]]);
        let denom = (query_trigs.len() as i64 + row_trigs as i64 - overlap).max(1);
        let score = (overlap * 1000 / denom) as u32;
        if score == 0 {
            continue;
        }
        scores[idx] = score;
    }
    Ok((hits, scores))
}

fn load_valid_mask(
    env: &dyn Env,
    partition_dir: &Path,
    name: &str,
    row_count: usize,
) -> Result<BitSet, DharmaError> {
    let path = partition_dir.join("cols").join(format!("{name}.valid"));
    let Some(map) = read_mapped(env, &path)? else {
        return Ok(BitSet::new(row_count));
    };
    Ok(bitset_from_valid(map.as_ref(), row_count))
}

fn bitset_from_valid(map: &[u8], row_count: usize) -> BitSet {
    let mut mask = BitSet::new(row_count);
    for (byte_idx, byte) in map.iter().enumerate() {
        let base = byte_idx * 8;
        if base >= row_count {
            break;
        }
        let mut bits = *byte;
        for bit in 0..8 {
            if base + bit >= row_count {
                break;
            }
            if bits & 1 == 1 {
                mask.set(base + bit);
            }
            bits >>= 1;
        }
    }
    mask
}

fn mask_for_filter(
    env: &dyn Env,
    filter: &Filter,
    maps: &PartitionMaps,
    partition_dir: &Path,
    row_count: usize,
) -> Result<(BitSet, Option<Vec<u32>>), DharmaError> {
    match filter {
        Filter::Leaf(pred) => mask_for_predicate(env, pred, maps, partition_dir, row_count),
        Filter::Not(inner) => {
            let (mut mask, _) = mask_for_filter(env, inner, maps, partition_dir, row_count)?;
            mask.invert_inplace();
            Ok((mask, None))
        }
        Filter::And(items) => {
            if items.is_empty() {
                return Ok((BitSet::filled(row_count), None));
            }
            let mut mask = BitSet::filled(row_count);
            let mut scores: Option<Vec<u32>> = None;
            for item in items {
                let (next_mask, next_scores) =
                    mask_for_filter(env, item, maps, partition_dir, row_count)?;
                mask.and_inplace(&next_mask);
                scores = merge_scores(scores, next_scores, row_count);
            }
            Ok((mask, scores))
        }
        Filter::Or(items) => {
            if items.is_empty() {
                return Ok((BitSet::new(row_count), None));
            }
            let mut mask = BitSet::new(row_count);
            let mut scores: Option<Vec<u32>> = None;
            for item in items {
                let (next_mask, next_scores) =
                    mask_for_filter(env, item, maps, partition_dir, row_count)?;
                mask.or_inplace(&next_mask);
                scores = merge_scores(scores, next_scores, row_count);
            }
            Ok((mask, scores))
        }
    }
}

fn mask_for_predicate(
    env: &dyn Env,
    pred: &Predicate,
    maps: &PartitionMaps,
    partition_dir: &Path,
    row_count: usize,
) -> Result<(BitSet, Option<Vec<u32>>), DharmaError> {
    match pred {
        Predicate::TextSearch(query) => {
            let normalized = normalize_text(query);
            let trigs = trigrams(&normalized);
            if trigs.is_empty() {
                return Ok((BitSet::new(row_count), Some(vec![0u32; row_count])));
            }
            let (text_mask, text_scores) = text_candidates(env, partition_dir, row_count, &trigs)?;
            Ok((text_mask, Some(text_scores)))
        }
        Predicate::Seq { op, value } => {
            let mut temp = BitSet::new(row_count);
            let seq_bytes = maps.seq.as_ref();
            match op {
                CmpOp::Eq => filter_eq_u64(seq_bytes, *value, &mut temp),
                CmpOp::Gt => filter_gt_u64(seq_bytes, *value, &mut temp),
                CmpOp::Gte => filter_gte_u64(seq_bytes, *value, &mut temp),
                CmpOp::Lt => filter_lt_u64(seq_bytes, *value, &mut temp),
                CmpOp::Lte => filter_lte_u64(seq_bytes, *value, &mut temp),
            }
            Ok((temp, None))
        }
        Predicate::TypEq(value) => {
            let typ_id = maps
                .symbols
                .iter()
                .position(|s| s == value)
                .map(|idx| idx as u32);
            let Some(typ_id) = typ_id else {
                return Ok((BitSet::new(row_count), None));
            };
            let mut temp = BitSet::new(row_count);
            filter_eq_u32(maps.typ.as_ref(), typ_id, &mut temp);
            Ok((temp, None))
        }
        Predicate::SubjectEq(subject) => {
            let mut temp = BitSet::new(row_count);
            filter_eq_bytes32(maps.subject_id.as_ref(), subject.as_bytes(), &mut temp);
            Ok((temp, None))
        }
        Predicate::DynI64 { col, op, value } => {
            let Some(col_map) =
                read_mapped(env, &partition_dir.join("cols").join(format!("{col}.bin")))? else {
                    return Ok((BitSet::new(row_count), None));
                };
            let valid = load_valid_mask(env, partition_dir, col, row_count)?;
            let mut temp = BitSet::new(row_count);
            let col_bytes = col_map.as_ref();
            match op {
                CmpOp::Eq => filter_eq_i64_bytes(col_bytes, *value, &mut temp),
                CmpOp::Gt => filter_gt_i64_bytes(col_bytes, *value, &mut temp),
                CmpOp::Gte => filter_gte_i64_bytes(col_bytes, *value, &mut temp),
                CmpOp::Lt => filter_lt_i64_bytes(col_bytes, *value, &mut temp),
                CmpOp::Lte => filter_lte_i64_bytes(col_bytes, *value, &mut temp),
            }
            temp.and_inplace(&valid);
            Ok((temp, None))
        }
        Predicate::DynBool { col, value } => {
            let Some(col_map) =
                read_mapped(env, &partition_dir.join("cols").join(format!("{col}.bin")))? else {
                    return Ok((BitSet::new(row_count), None));
                };
            let valid = load_valid_mask(env, partition_dir, col, row_count)?;
            let mut temp = BitSet::new(row_count);
            filter_eq_u8(col_map.as_ref(), if *value { 1 } else { 0 }, &mut temp);
            temp.and_inplace(&valid);
            Ok((temp, None))
        }
        Predicate::DynSymbol { col, value } => {
            let typ_id = maps
                .symbols
                .iter()
                .position(|s| s == value)
                .map(|idx| idx as u32);
            let Some(typ_id) = typ_id else {
                return Ok((BitSet::new(row_count), None));
            };
            let Some(col_map) =
                read_mapped(env, &partition_dir.join("cols").join(format!("{col}.bin")))? else {
                    return Ok((BitSet::new(row_count), None));
                };
            let valid = load_valid_mask(env, partition_dir, col, row_count)?;
            let mut temp = BitSet::new(row_count);
            filter_eq_u32(col_map.as_ref(), typ_id, &mut temp);
            temp.and_inplace(&valid);
            Ok((temp, None))
        }
        Predicate::DynBytes32 { col, value } => {
            let Some(col_map) =
                read_mapped(env, &partition_dir.join("cols").join(format!("{col}.bin")))? else {
                    return Ok((BitSet::new(row_count), None));
                };
            let valid = load_valid_mask(env, partition_dir, col, row_count)?;
            let mut temp = BitSet::new(row_count);
            filter_eq_bytes32(col_map.as_ref(), value, &mut temp);
            temp.and_inplace(&valid);
            Ok((temp, None))
        }
    }
}

fn merge_scores(
    current: Option<Vec<u32>>,
    next: Option<Vec<u32>>,
    row_count: usize,
) -> Option<Vec<u32>> {
    match (current, next) {
        (None, None) => None,
        (Some(scores), None) | (None, Some(scores)) => Some(scores),
        (Some(mut left), Some(right)) => {
            if left.len() < row_count {
                left.resize(row_count, 0);
            }
            for (idx, value) in right.iter().enumerate() {
                if *value > left[idx] {
                    left[idx] = *value;
                }
            }
            Some(left)
        }
    }
}

struct PartitionMaps {
    object_id: MappedBytes,
    subject_id: MappedBytes,
    seq: MappedBytes,
    typ: MappedBytes,
    text_off: MappedBytes,
    text: MappedBytes,
    symbols: Vec<String>,
}

fn load_partition_maps(
    env: &dyn Env,
    table_root: &Path,
    partition_dir: &Path,
) -> Result<Option<PartitionMaps>, DharmaError> {
    let cols = partition_dir.join("cols");
    let object_id = match read_mapped(env, &cols.join("object_id.bin"))? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    let subject_id = match read_mapped(env, &cols.join("subject_id.bin"))? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    let seq = match read_mapped(env, &cols.join("seq.bin"))? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    let typ = match read_mapped(env, &cols.join("typ.bin"))? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    let text_off = match read_mapped(env, &cols.join("text.off"))? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    let text = match read_mapped(env, &cols.join("text.bin"))? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    let symbols = load_symbols(env, table_root)?;
    Ok(Some(PartitionMaps {
        object_id,
        subject_id,
        seq,
        typ,
        text_off,
        text,
        symbols,
    }))
}

fn read_row(maps: &PartitionMaps, row_id: u64, query: &str, score: u32) -> Option<QueryRow> {
    let idx = row_id as usize;
    let object_bytes = maps.object_id.as_ref();
    let subject_bytes = maps.subject_id.as_ref();
    let seq_bytes = maps.seq.as_ref();
    let typ_bytes = maps.typ.as_ref();
    let obj_offset = idx * 32;
    if obj_offset + 32 > object_bytes.len() || obj_offset + 32 > subject_bytes.len() {
        return None;
    }
    let mut obj = [0u8; 32];
    obj.copy_from_slice(&object_bytes[obj_offset..obj_offset + 32]);
    let mut sub = [0u8; 32];
    sub.copy_from_slice(&subject_bytes[obj_offset..obj_offset + 32]);
    let seq_offset = idx * 8;
    if seq_offset + 8 > seq_bytes.len() {
        return None;
    }
    let seq = u64::from_le_bytes([
        seq_bytes[seq_offset],
        seq_bytes[seq_offset + 1],
        seq_bytes[seq_offset + 2],
        seq_bytes[seq_offset + 3],
        seq_bytes[seq_offset + 4],
        seq_bytes[seq_offset + 5],
        seq_bytes[seq_offset + 6],
        seq_bytes[seq_offset + 7],
    ]);
    let typ_offset = idx * 4;
    if typ_offset + 4 > typ_bytes.len() {
        return None;
    }
    let typ_id = u32::from_le_bytes([
        typ_bytes[typ_offset],
        typ_bytes[typ_offset + 1],
        typ_bytes[typ_offset + 2],
        typ_bytes[typ_offset + 3],
    ]);
    let typ = maps.symbols.get(typ_id as usize).cloned().unwrap_or_else(|| "unknown".to_string());
    let text = read_text_from_maps(maps, idx)?;
    let snippet = snippet_for(&text, query);
    Some(QueryRow {
        assertion_id: AssertionId::from_bytes(obj),
        subject: SubjectId::from_bytes(sub),
        seq,
        typ,
        snippet,
        score,
    })
}

fn read_text_from_maps(maps: &PartitionMaps, row_id: usize) -> Option<String> {
    let text_off_bytes = maps.text_off.as_ref();
    let text_bytes = maps.text.as_ref();
    let off_offset = row_id * 12;
    if off_offset + 12 > text_off_bytes.len() {
        return None;
    }
    let offset = u64::from_le_bytes([
        text_off_bytes[off_offset],
        text_off_bytes[off_offset + 1],
        text_off_bytes[off_offset + 2],
        text_off_bytes[off_offset + 3],
        text_off_bytes[off_offset + 4],
        text_off_bytes[off_offset + 5],
        text_off_bytes[off_offset + 6],
        text_off_bytes[off_offset + 7],
    ]);
    let len = u32::from_le_bytes([
        text_off_bytes[off_offset + 8],
        text_off_bytes[off_offset + 9],
        text_off_bytes[off_offset + 10],
        text_off_bytes[off_offset + 11],
    ]) as usize;
    let start = offset as usize;
    let end = start.saturating_add(len);
    if end > text_bytes.len() {
        return None;
    }
    let slice = &text_bytes[start..end];
    Some(String::from_utf8_lossy(slice).to_string())
}

fn extract_text(value: &Value) -> String {
    let mut out = Vec::new();
    collect_text(value, &mut out);
    out.join(" ")
}

fn collect_text(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Text(text) => out.push(text.clone()),
        Value::Map(entries) => {
            for (k, v) in entries {
                collect_text(k, out);
                collect_text(v, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_text(item, out);
            }
        }
        _ => {}
    }
}

fn normalize_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() || ch == ' ' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(' ');
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn trigrams(text: &str) -> Vec<u32> {
    let mut bytes = text.as_bytes().to_vec();
    if bytes.is_empty() {
        return Vec::new();
    }
    if bytes.len() < 3 {
        while bytes.len() < 3 {
            bytes.push(b' ');
        }
    }
    let mut set = HashSet::new();
    for i in 0..=bytes.len().saturating_sub(3) {
        let trigram = u32::from_le_bytes([bytes[i], bytes[i + 1], bytes[i + 2], 0]);
        set.insert(trigram);
    }
    set.into_iter().collect()
}

fn with_dharma_lock<F, T>(root: &Path, f: F) -> Result<Option<T>, DharmaError>
where
    F: FnOnce() -> Result<T, DharmaError>,
{
    match LockHandle::acquire(&root.join("dharma.lock")) {
        Ok(_lock) => Ok(Some(f()?)),
        Err(DharmaError::LockBusy) => Ok(None),
        Err(err) => Err(err),
    }
}

fn snippet_for(text: &str, query: &str) -> Option<String> {
    if text.is_empty() {
        return None;
    }
    let query = query.trim();
    if query.is_empty() {
        return Some(text.chars().take(80).collect());
    }
    let lower_text = text.to_lowercase();
    let lower_query = query.to_lowercase();
    if let Some(pos) = lower_text.find(&lower_query) {
        let start = pos.saturating_sub(20);
        let end = (pos + lower_query.len() + 40).min(text.len());
        return Some(text.chars().skip(start).take(end - start).collect());
    }
    Some(text.chars().take(80).collect())
}

fn first_text_query(filter: &Filter) -> Option<String> {
    match filter {
        Filter::Leaf(Predicate::TextSearch(query)) => Some(query.clone()),
        Filter::And(items) | Filter::Or(items) => {
            for item in items {
                if let Some(found) = first_text_query(item) {
                    return Some(found);
                }
            }
            None
        }
        Filter::Not(inner) => first_text_query(inner),
        _ => None,
    }
}

fn filter_has_text_query(filter: &Filter) -> bool {
    match filter {
        Filter::Leaf(Predicate::TextSearch(_)) => true,
        Filter::And(items) | Filter::Or(items) => {
            items.iter().any(filter_has_text_query)
        }
        Filter::Not(inner) => filter_has_text_query(inner),
        _ => false,
    }
}

fn load_symbols(env: &dyn Env, table_root: &Path) -> Result<Vec<String>, DharmaError> {
    let path = table_root.join("sym").join("sym.dict");
    if !env.exists(&path) {
        return Ok(Vec::new());
    }
    let contents = read_to_string(env, &path)?;
    let mut out = Vec::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        out.push(line.to_string());
    }
    Ok(out)
}

#[derive(Clone, Debug)]
struct WalEntry {
    partition: String,
    row_id: u64,
    assertion_id: AssertionId,
    subject: SubjectId,
    seq: u64,
    typ: String,
    text: String,
    ts: u64,
}

fn wal_path(table_root: &Path) -> PathBuf {
    table_root.join("hot").join("wal.bin")
}

fn wal_append(env: &dyn Env, table_root: &Path, entry: &WalEntry) -> Result<(), DharmaError> {
    let bytes = wal_encode(entry);
    let mut hasher = Hasher::new();
    hasher.update(&bytes);
    let crc = hasher.finalize();
    let mut buf = Vec::with_capacity(8 + bytes.len());
    buf.write_u32::<LittleEndian>(bytes.len() as u32)?;
    buf.write_u32::<LittleEndian>(crc)?;
    buf.extend_from_slice(&bytes);
    env.append(&wal_path(table_root), &buf)?;
    Ok(())
}

fn recover_wal<E>(env: &E) -> Result<(), DharmaError>
where
    E: Env + Clone + Send + Sync + 'static,
{
    let base = dharmaq_root(env.root());
    let table_root = table_root(&base, TABLE_ASSERTIONS);
    let path = wal_path(&table_root);
    if !env.exists(&path) {
        return Ok(());
    }
    let buf = env.read(&path)?;
    let mut cursor = std::io::Cursor::new(&buf);
    let mut dict = SymDict::load(env, &table_root)?;
    let store = Store::new(env);
    let mut schema_cache = SchemaCache::new(&store);
    let mut columns = ColumnCache::default();
    let mut last_good = 0u64;
    loop {
        let start = cursor.position();
        let len = match cursor.read_u32::<LittleEndian>() {
            Ok(v) => v as usize,
            Err(_) => break,
        };
        let crc = match cursor.read_u32::<LittleEndian>() {
            Ok(v) => v,
            Err(_) => break,
        };
        let mut buf = vec![0u8; len];
        if let Err(_) = cursor.read_exact(&mut buf) {
            break;
        }
        let mut hasher = Hasher::new();
        hasher.update(&buf);
        let actual = hasher.finalize();
        if actual != crc {
            break;
        }
        let entry = wal_decode(&buf)?;
        let assertion = match store.lookup_envelope(&entry.assertion_id)? {
            Some(env) => {
                let bytes = store.get_object(&env)?;
                AssertionPlaintext::from_cbor(&bytes).ok()
            }
            None => None,
        };
        let schema = match assertion.as_ref() {
            Some(assertion) => schema_cache.load(&assertion.header.schema)?,
            None => None,
        };
        apply_row(
            env,
            &base,
            TABLE_ASSERTIONS,
            &mut dict,
            &mut columns,
            &entry,
            assertion.as_ref(),
            schema.as_ref(),
            true,
        )?;
        last_good = cursor.position();
        if start == last_good {
            break;
        }
    }
    dict.persist(env)?;
    truncate_wal(env, &path, last_good)?;
    Ok(())
}

fn clear_wal(env: &dyn Env, table_root: &Path) -> Result<(), DharmaError> {
    let path = wal_path(table_root);
    truncate_wal(env, &path, 0)
}

fn truncate_wal(env: &dyn Env, path: &Path, size: u64) -> Result<(), DharmaError> {
    if size == 0 {
        if env.exists(path) {
            env.remove_file(path)?;
        }
        return Ok(());
    }
    let buf = env.read(path)?;
    let size = size as usize;
    let truncated = if buf.len() >= size { &buf[..size] } else { &buf[..] };
    env.write(path, truncated)?;
    Ok(())
}

fn wal_encode(entry: &WalEntry) -> Vec<u8> {
    let mut buf = Vec::new();
    write_string(&mut buf, &entry.partition);
    buf.write_u64::<LittleEndian>(entry.row_id).unwrap();
    buf.extend_from_slice(entry.assertion_id.as_bytes());
    buf.extend_from_slice(entry.subject.as_bytes());
    buf.write_u64::<LittleEndian>(entry.seq).unwrap();
    write_string(&mut buf, &entry.typ);
    write_string_u32(&mut buf, &entry.text);
    buf.write_u64::<LittleEndian>(entry.ts).unwrap();
    buf
}

fn wal_decode(bytes: &[u8]) -> Result<WalEntry, DharmaError> {
    let mut cursor = std::io::Cursor::new(bytes);
    let partition = read_string(&mut cursor)?;
    let row_id = cursor.read_u64::<LittleEndian>()?;
    let mut assertion_buf = [0u8; 32];
    cursor.read_exact(&mut assertion_buf)?;
    let mut subject_buf = [0u8; 32];
    cursor.read_exact(&mut subject_buf)?;
    let seq = cursor.read_u64::<LittleEndian>()?;
    let typ = read_string(&mut cursor)?;
    let text = read_string_u32(&mut cursor)?;
    let ts = cursor.read_u64::<LittleEndian>()?;
    Ok(WalEntry {
        partition,
        row_id,
        assertion_id: AssertionId::from_bytes(assertion_buf),
        subject: SubjectId::from_bytes(subject_buf),
        seq,
        typ,
        text,
        ts,
    })
}

fn write_string(buf: &mut Vec<u8>, value: &str) {
    let len = value.len().min(u16::MAX as usize) as u16;
    buf.write_u16::<LittleEndian>(len).unwrap();
    buf.extend_from_slice(&value.as_bytes()[..len as usize]);
}

fn write_string_u32(buf: &mut Vec<u8>, value: &str) {
    let len = value.len() as u32;
    buf.write_u32::<LittleEndian>(len).unwrap();
    buf.extend_from_slice(value.as_bytes());
}

fn read_string<R: Read>(reader: &mut R) -> Result<String, DharmaError> {
    let len = reader.read_u16::<LittleEndian>()? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(String::from_utf8_lossy(&buf).to_string())
}

fn read_string_u32<R: Read>(reader: &mut R) -> Result<String, DharmaError> {
    let len = reader.read_u32::<LittleEndian>()? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(String::from_utf8_lossy(&buf).to_string())
}

struct SymDict {
    root: PathBuf,
    entries: Vec<String>,
    index: HashMap<String, u32>,
}

impl SymDict {
    fn load(env: &dyn Env, root: &Path) -> Result<Self, DharmaError> {
        let path = root.join("sym").join("sym.dict");
        let mut entries = Vec::new();
        let mut index = HashMap::new();
        if env.exists(&path) {
            let contents = read_to_string(env, &path)?;
            for (idx, line) in contents.lines().enumerate() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                entries.push(line.to_string());
                index.insert(line.to_string(), idx as u32);
            }
        }
        Ok(SymDict {
            root: root.to_path_buf(),
            entries,
            index,
        })
    }

    fn intern(&mut self, value: &str) -> Result<u32, DharmaError> {
        if let Some(id) = self.index.get(value) {
            return Ok(*id);
        }
        let id = self.entries.len() as u32;
        self.entries.push(value.to_string());
        self.index.insert(value.to_string(), id);
        Ok(id)
    }

    fn persist(&self, env: &dyn Env) -> Result<(), DharmaError> {
        let path = self.root.join("sym").join("sym.dict");
        if self.entries.is_empty() {
            if env.exists(&path) {
                env.remove_file(&path)?;
            }
            return Ok(());
        }
        let contents = self.entries.join("\n") + "\n";
        env.write(&path, contents.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::crypto;
    use crate::env::Fs;
    use crate::pdl::schema::{ActionSchema, FieldSchema, Visibility};
    use crate::store::state::append_assertion;
    use crate::types::{ContractId, IdentityKey, SchemaId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    #[test]
    fn trigram_search_finds_text() {
        let temp = tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(7);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([9u8; 32]);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: Some(1_700_000_000),
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![(
            Value::Text("text".to_string()),
            Value::Text("Invoice #44 paid".to_string()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &signing_key).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        let envelope_id = crypto::envelope_id(&bytes);
        append_assertion(
            &env,
            &subject,
            1,
            assertion_id,
            envelope_id,
            "note.text",
            &bytes,
        )
        .unwrap();

        rebuild(temp.path()).unwrap();
        let results = search(temp.path(), "invoice 44", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].assertion_id, assertion_id);
    }

    #[test]
    fn wal_recovery_applies_rows() {
        let temp = tempdir().unwrap();
        let env = StdEnv::new(temp.path());
        let base = dharmaq_root(temp.path());
        let table = table_root(&base, TABLE_ASSERTIONS);
        env.create_dir_all(&table.join("hot")).unwrap();
        env.create_dir_all(&table.join("sym")).unwrap();

        let entry = WalEntry {
            partition: "p=2024.01.01".to_string(),
            row_id: 0,
            assertion_id: AssertionId::from_bytes([1u8; 32]),
            subject: SubjectId::from_bytes([2u8; 32]),
            seq: 7,
            typ: "note.text".to_string(),
            text: "hello world".to_string(),
            ts: 1_700_000_000,
        };
        wal_append(&env, &table, &entry).unwrap();
        recover_wal(&env).unwrap();
        let part_dir = partition_dir(&base, TABLE_ASSERTIONS, &entry.partition);
        assert_eq!(row_count(&env, &part_dir).unwrap(), 1);
        let dict_path = table.join("sym").join("sym.dict");
        assert!(dict_path.exists());
    }

    #[test]
    fn dynamic_columns_filter() {
        let temp = tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut rng = StdRng::seed_from_u64(11);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([7u8; 32]);

        let mut fields = BTreeMap::new();
        fields.insert(
            "status".to_string(),
            FieldSchema {
                typ: CqrsTypeSpec::Enum(vec!["Open".to_string(), "Paid".to_string()]),
                default: None,
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("amount".to_string(), CqrsTypeSpec::Int);
        args.insert(
            "status".to_string(),
            CqrsTypeSpec::Enum(vec!["Open".to_string(), "Paid".to_string()]),
        );
        args.insert("assignee".to_string(), CqrsTypeSpec::Identity);
        args.insert("active".to_string(), CqrsTypeSpec::Bool);
        actions.insert(
            "Create".to_string(),
            ActionSchema {
                args,
                arg_vis: BTreeMap::new(),
                doc: None,
            },
        );
        let schema = CqrsSchema {
            namespace: "com.test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Ticket".to_string(),
            extends: None,
            implements: Vec::new(),
            structs: BTreeMap::new(),
            fields,
            actions,
            queries: BTreeMap::new(),
        projections: BTreeMap::new(),
            concurrency: crate::pdl::schema::ConcurrencyMode::Strict,
        };
        let schema_bytes = schema.to_cbor().unwrap();
        let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
        let schema_obj = EnvelopeId::from_bytes(*schema_id.as_bytes());
        store.put_object(&schema_obj, &schema_bytes).unwrap();

        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "action.Create".to_string(),
            auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: Some(1_700_000_001),
            schema: schema_id,
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let assignee = Value::Bytes(vec![9u8; 32]);
        let body = Value::Map(vec![
            (Value::Text("amount".to_string()), Value::Integer(120.into())),
            (Value::Text("status".to_string()), Value::Text("Open".to_string())),
            (Value::Text("assignee".to_string()), assignee),
            (Value::Text("active".to_string()), Value::Bool(true)),
        ]);
        let assertion = AssertionPlaintext::sign(header, body, &signing_key).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        let envelope_id = crypto::envelope_id(&bytes);
        let env = crate::env::StdEnv::new(temp.path());
        append_assertion(&env, &subject, 1, assertion_id, envelope_id, "Create", &bytes).unwrap();

        rebuild(temp.path()).unwrap();

        let plan = QueryPlan {
            table: TABLE_ASSERTIONS.to_string(),
            filter: Some(Filter::Leaf(Predicate::DynI64 {
                col: "amount".to_string(),
                op: CmpOp::Gt,
                value: 100,
            })),
            limit: 10,
        };
        let results = execute(temp.path(), &plan).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].assertion_id, assertion_id);

        let plan = QueryPlan {
            table: TABLE_ASSERTIONS.to_string(),
            filter: Some(Filter::Leaf(Predicate::DynSymbol {
                col: "status".to_string(),
                value: "Open".to_string(),
            })),
            limit: 10,
        };
        let results = execute(temp.path(), &plan).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].assertion_id, assertion_id);

        let plan = QueryPlan {
            table: TABLE_ASSERTIONS.to_string(),
            filter: Some(Filter::Leaf(Predicate::DynBool {
                col: "active".to_string(),
                value: true,
            })),
            limit: 10,
        };
        let results = execute(temp.path(), &plan).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].assertion_id, assertion_id);
    }
}
