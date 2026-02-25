use crate::assertion::AssertionPlaintext;
use crate::cbor;
use crate::config::Config;
use crate::env::StdEnv;
use crate::error::DharmaError;
use crate::pdl::schema::CqrsSchema;
use crate::reactor::{Expr, Op};
use crate::runtime::cqrs::{load_state, read_value_at_path};
use crate::store::spi::{RuntimeMode, StorageFacade, StorageQuery, StorageRead};
use crate::store::Store;
use crate::types::{AssertionId, ContractId, EnvelopeId, SchemaId, SubjectId};
use crate::value::{expect_array, expect_int, expect_map, expect_text, map_get};
use ciborium::value::Value;
use std::collections::HashMap;
use std::path::Path;

#[derive(Clone, Debug, PartialEq)]
pub struct QueryPlan {
    pub version: u8,
    pub source: QuerySource,
    pub ops: Vec<QueryOp>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum QuerySource {
    Table(String),
    Search(SearchSpec),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchSpec {
    pub query: Expr,
    pub fields: Vec<String>,
    pub fuzz: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryOp {
    Where(Expr),
    Sort(Vec<SortKey>),
    Drop(Expr),
    Take(Expr),
    Select(Vec<SelectItem>),
    Join(JoinSpec),
    Explode(ExplodeSpec),
    Bucket(BucketSpec),
    GroupBy(Vec<String>),
    Agg(Vec<AggSpec>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SortKey {
    pub path: String,
    pub desc: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SelectItem {
    pub path: String,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinSpec {
    pub table: String,
    pub left: String,
    pub right: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BucketSpec {
    pub path: String,
    pub size_secs: u64,
    pub label: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExplodeSpec {
    pub path: String,
    pub key: Option<String>,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggSpec {
    pub func: AggFunc,
    pub path: Option<String>,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl QueryPlan {
    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.to_value())
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        Self::from_value(&value)
    }

    fn to_value(&self) -> Value {
        let mut ops = Vec::new();
        for op in &self.ops {
            ops.push(op.to_value());
        }
        Value::Map(vec![
            (
                Value::Text("v".to_string()),
                Value::Integer((self.version as u64).into()),
            ),
            (Value::Text("source".to_string()), self.source.to_value()),
            (Value::Text("ops".to_string()), Value::Array(ops)),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let version = map_get(map, "v").map(expect_int).transpose()?.unwrap_or(1) as u8;
        let source_val = map_get(map, "source")
            .ok_or_else(|| DharmaError::Validation("missing query source".to_string()))?;
        let source = QuerySource::from_value(source_val)?;
        let empty_ops = Value::Array(Vec::new());
        let ops_val = map_get(map, "ops").unwrap_or(&empty_ops);
        let mut ops = Vec::new();
        for item in expect_array(ops_val)? {
            ops.push(QueryOp::from_value(item)?);
        }
        Ok(Self {
            version,
            source,
            ops,
        })
    }
}

impl QuerySource {
    fn to_value(&self) -> Value {
        match self {
            QuerySource::Table(name) => Value::Map(vec![(
                Value::Text("table".to_string()),
                Value::Text(name.clone()),
            )]),
            QuerySource::Search(spec) => {
                Value::Map(vec![(Value::Text("search".to_string()), spec.to_value())])
            }
        }
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        if let Some(table) = map_get(map, "table") {
            return Ok(QuerySource::Table(expect_text(table)?));
        }
        if let Some(search) = map_get(map, "search") {
            return Ok(QuerySource::Search(SearchSpec::from_value(search)?));
        }
        Err(DharmaError::Validation("invalid query source".to_string()))
    }
}

impl SearchSpec {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (Value::Text("query".to_string()), expr_to_value(&self.query)),
            (
                Value::Text("fields".to_string()),
                Value::Array(self.fields.iter().map(|f| Value::Text(f.clone())).collect()),
            ),
            (
                Value::Text("fuzz".to_string()),
                Value::Integer((self.fuzz as u64).into()),
            ),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let query_val = map_get(map, "query")
            .ok_or_else(|| DharmaError::Validation("missing search query".to_string()))?;
        let empty_fields = Value::Array(Vec::new());
        let fields_val = map_get(map, "fields").unwrap_or(&empty_fields);
        let mut fields = Vec::new();
        for item in expect_array(fields_val)? {
            fields.push(expect_text(item)?);
        }
        let fuzz = map_get(map, "fuzz")
            .map(expect_int)
            .transpose()?
            .unwrap_or(0) as u8;
        Ok(Self {
            query: expr_from_value(query_val)?,
            fields,
            fuzz,
        })
    }
}

impl QueryOp {
    fn to_value(&self) -> Value {
        match self {
            QueryOp::Where(expr) => Value::Map(vec![(
                Value::Text("where".to_string()),
                expr_to_value(expr),
            )]),
            QueryOp::Sort(keys) => {
                let items = keys.iter().map(|k| k.to_value()).collect();
                Value::Map(vec![(Value::Text("sort".to_string()), Value::Array(items))])
            }
            QueryOp::Drop(expr) => {
                Value::Map(vec![(Value::Text("drop".to_string()), expr_to_value(expr))])
            }
            QueryOp::Take(expr) => {
                Value::Map(vec![(Value::Text("take".to_string()), expr_to_value(expr))])
            }
            QueryOp::Select(items) => {
                let list = items.iter().map(|i| i.to_value()).collect();
                Value::Map(vec![(
                    Value::Text("select".to_string()),
                    Value::Array(list),
                )])
            }
            QueryOp::Join(join) => {
                Value::Map(vec![(Value::Text("join".to_string()), join.to_value())])
            }
            QueryOp::Explode(spec) => {
                Value::Map(vec![(Value::Text("explode".to_string()), spec.to_value())])
            }
            QueryOp::Bucket(bucket) => {
                Value::Map(vec![(Value::Text("bucket".to_string()), bucket.to_value())])
            }
            QueryOp::GroupBy(keys) => Value::Map(vec![(
                Value::Text("group_by".to_string()),
                Value::Array(keys.iter().map(|k| Value::Text(k.clone())).collect()),
            )]),
            QueryOp::Agg(specs) => {
                let list = specs.iter().map(|s| s.to_value()).collect();
                Value::Map(vec![(Value::Text("agg".to_string()), Value::Array(list))])
            }
        }
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        if let Some(expr) = map_get(map, "where") {
            return Ok(QueryOp::Where(expr_from_value(expr)?));
        }
        if let Some(items) = map_get(map, "sort") {
            let mut keys = Vec::new();
            for item in expect_array(items)? {
                keys.push(SortKey::from_value(item)?);
            }
            return Ok(QueryOp::Sort(keys));
        }
        if let Some(expr) = map_get(map, "drop") {
            return Ok(QueryOp::Drop(expr_from_value(expr)?));
        }
        if let Some(expr) = map_get(map, "take") {
            return Ok(QueryOp::Take(expr_from_value(expr)?));
        }
        if let Some(items) = map_get(map, "select") {
            let mut list = Vec::new();
            for item in expect_array(items)? {
                list.push(SelectItem::from_value(item)?);
            }
            return Ok(QueryOp::Select(list));
        }
        if let Some(join) = map_get(map, "join") {
            return Ok(QueryOp::Join(JoinSpec::from_value(join)?));
        }
        if let Some(spec) = map_get(map, "explode") {
            return Ok(QueryOp::Explode(ExplodeSpec::from_value(spec)?));
        }
        if let Some(bucket) = map_get(map, "bucket") {
            return Ok(QueryOp::Bucket(BucketSpec::from_value(bucket)?));
        }
        if let Some(items) = map_get(map, "group_by") {
            let mut list = Vec::new();
            for item in expect_array(items)? {
                list.push(expect_text(item)?);
            }
            return Ok(QueryOp::GroupBy(list));
        }
        if let Some(items) = map_get(map, "agg") {
            let mut list = Vec::new();
            for item in expect_array(items)? {
                list.push(AggSpec::from_value(item)?);
            }
            return Ok(QueryOp::Agg(list));
        }
        Err(DharmaError::Validation("invalid query op".to_string()))
    }
}

impl SortKey {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (
                Value::Text("path".to_string()),
                Value::Text(self.path.clone()),
            ),
            (Value::Text("desc".to_string()), Value::Bool(self.desc)),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let path = map_get(map, "path")
            .map(expect_text)
            .transpose()?
            .unwrap_or_default();
        let desc = map_get(map, "desc")
            .map(|v| match v {
                Value::Bool(b) => Ok(*b),
                _ => Err(DharmaError::Validation("invalid sort flag".to_string())),
            })
            .transpose()?
            .unwrap_or(false);
        Ok(Self { path, desc })
    }
}

impl SelectItem {
    fn to_value(&self) -> Value {
        let mut entries = vec![(
            Value::Text("path".to_string()),
            Value::Text(self.path.clone()),
        )];
        if let Some(alias) = &self.alias {
            entries.push((Value::Text("as".to_string()), Value::Text(alias.clone())));
        }
        Value::Map(entries)
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let path = map_get(map, "path")
            .map(expect_text)
            .transpose()?
            .unwrap_or_default();
        let alias = map_get(map, "as").map(expect_text).transpose()?;
        Ok(Self { path, alias })
    }
}

impl JoinSpec {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (
                Value::Text("table".to_string()),
                Value::Text(self.table.clone()),
            ),
            (
                Value::Text("left".to_string()),
                Value::Text(self.left.clone()),
            ),
            (
                Value::Text("right".to_string()),
                Value::Text(self.right.clone()),
            ),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let table = map_get(map, "table")
            .map(expect_text)
            .transpose()?
            .unwrap_or_default();
        let left = map_get(map, "left")
            .map(expect_text)
            .transpose()?
            .unwrap_or_default();
        let right = map_get(map, "right")
            .map(expect_text)
            .transpose()?
            .unwrap_or_default();
        Ok(Self { table, left, right })
    }
}

impl BucketSpec {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (
                Value::Text("path".to_string()),
                Value::Text(self.path.clone()),
            ),
            (
                Value::Text("size".to_string()),
                Value::Integer((self.size_secs as u64).into()),
            ),
            (
                Value::Text("label".to_string()),
                Value::Text(self.label.clone()),
            ),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let path = map_get(map, "path")
            .map(expect_text)
            .transpose()?
            .unwrap_or_default();
        let size_secs = map_get(map, "size")
            .map(expect_int)
            .transpose()?
            .unwrap_or(0) as u64;
        let label = map_get(map, "label")
            .map(expect_text)
            .transpose()?
            .unwrap_or_else(|| "bucket".to_string());
        Ok(Self {
            path,
            size_secs,
            label,
        })
    }
}

impl ExplodeSpec {
    fn to_value(&self) -> Value {
        let mut entries = vec![
            (
                Value::Text("path".to_string()),
                Value::Text(self.path.clone()),
            ),
            (
                Value::Text("value".to_string()),
                Value::Text(self.value.clone()),
            ),
        ];
        if let Some(key) = &self.key {
            entries.push((Value::Text("key".to_string()), Value::Text(key.clone())));
        }
        Value::Map(entries)
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let path = map_get(map, "path")
            .map(expect_text)
            .transpose()?
            .unwrap_or_default();
        let value_name = map_get(map, "value")
            .map(expect_text)
            .transpose()?
            .unwrap_or_default();
        let key = map_get(map, "key").map(expect_text).transpose()?;
        Ok(Self {
            path,
            key,
            value: value_name,
        })
    }
}

impl AggSpec {
    fn to_value(&self) -> Value {
        let mut entries = vec![(
            Value::Text("func".to_string()),
            Value::Text(self.func.as_str().to_string()),
        )];
        if let Some(path) = &self.path {
            entries.push((Value::Text("path".to_string()), Value::Text(path.clone())));
        }
        if let Some(alias) = &self.alias {
            entries.push((Value::Text("as".to_string()), Value::Text(alias.clone())));
        }
        Value::Map(entries)
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let func = map_get(map, "func")
            .map(expect_text)
            .transpose()?
            .ok_or_else(|| DharmaError::Validation("missing agg func".to_string()))?;
        let func = AggFunc::from_str(&func)?;
        let path = map_get(map, "path").map(expect_text).transpose()?;
        let alias = map_get(map, "as").map(expect_text).transpose()?;
        Ok(Self { func, path, alias })
    }
}

impl AggFunc {
    fn as_str(&self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
        }
    }

    pub fn from_str(value: &str) -> Result<Self, DharmaError> {
        match value {
            "count" => Ok(AggFunc::Count),
            "sum" => Ok(AggFunc::Sum),
            "avg" => Ok(AggFunc::Avg),
            "min" => Ok(AggFunc::Min),
            "max" => Ok(AggFunc::Max),
            _ => Err(DharmaError::Validation("invalid agg func".to_string())),
        }
    }
}

pub fn execute(root: &Path, plan: &QueryPlan, params: &Value) -> Result<Vec<Value>, DharmaError> {
    let env = StdEnv::new(root);
    let store = Store::new(&env);
    let config = Config::load(root)?;

    if RuntimeMode::from_config(&config) == RuntimeMode::Server {
        let facade = StorageFacade::new_with_config(store.clone(), RuntimeMode::Server, &config);
        let adapter = facade.clickhouse_server_adapter()?.ok_or_else(|| {
            DharmaError::Config("server clickhouse backend unavailable".to_string())
        })?;
        adapter.sync_from_canonical()?;
        return execute_with_backend(&adapter, &store, plan, params);
    }

    execute_with_backend(&store, &store, plan, params)
}

fn execute_with_backend<B>(
    backend: &B,
    state_store: &Store,
    plan: &QueryPlan,
    params: &Value,
) -> Result<Vec<Value>, DharmaError>
where
    B: StorageRead + StorageQuery,
{
    let mut cache = RuntimeCache::new();
    let mut rows = match &plan.source {
        QuerySource::Table(name) => {
            load_rows_for_table(backend, state_store, &mut cache, name, params, None)?
        }
        QuerySource::Search(spec) => {
            load_rows_for_search(backend, state_store, &mut cache, spec, params)?
        }
    };

    let mut values: Option<Vec<Value>> = None;
    for op in &plan.ops {
        match op {
            QueryOp::Where(expr) => {
                rows = rows
                    .into_iter()
                    .filter(|row| eval_bool(row, params, expr).unwrap_or(false))
                    .collect();
            }
            QueryOp::Sort(keys) => {
                if let Some(ref mut out) = values {
                    sort_values(out, keys)?;
                } else {
                    sort_rows(&mut rows, keys, params)?;
                }
            }
            QueryOp::Drop(expr) => {
                let count = eval_usize(params, expr)?;
                if let Some(ref mut out) = values {
                    if count < out.len() {
                        out.drain(0..count);
                    } else {
                        out.clear();
                    }
                } else if count < rows.len() {
                    rows.drain(0..count);
                } else {
                    rows.clear();
                }
            }
            QueryOp::Take(expr) => {
                let count = eval_usize(params, expr)?;
                if let Some(ref mut out) = values {
                    out.truncate(count);
                } else {
                    rows.truncate(count);
                }
            }
            QueryOp::Select(items) => {
                let mut out = Vec::new();
                for row in &rows {
                    out.push(select_row(row, params, items)?);
                }
                values = Some(out);
            }
            QueryOp::Join(spec) => {
                if values.is_some() {
                    return Err(DharmaError::Validation(
                        "join after projection not supported".to_string(),
                    ));
                }
                rows = join_rows(backend, state_store, &mut cache, rows, spec, params)?;
            }
            QueryOp::Explode(spec) => {
                if values.is_some() {
                    return Err(DharmaError::Validation(
                        "explode after projection not supported".to_string(),
                    ));
                }
                rows = explode_rows(rows, params, spec)?;
            }
            QueryOp::Bucket(spec) => {
                for row in &mut rows {
                    apply_bucket(row, params, spec)?;
                }
            }
            QueryOp::GroupBy(keys) => {
                let grouped = group_rows(&rows, params, keys)?;
                values = Some(grouped);
            }
            QueryOp::Agg(specs) => {
                let agg = aggregate_rows(values.take(), &rows, params, specs)?;
                values = Some(agg);
            }
        }
    }

    Ok(values.unwrap_or_else(|| rows.into_iter().map(|row| row.to_value()).collect()))
}

struct RuntimeCache {
    schemas: HashMap<SchemaId, CqrsSchema>,
    contracts: HashMap<ContractId, Vec<u8>>,
}

impl RuntimeCache {
    fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            contracts: HashMap::new(),
        }
    }

    fn schema(
        &mut self,
        storage: &impl StorageRead,
        schema_id: &SchemaId,
    ) -> Result<CqrsSchema, DharmaError> {
        if let Some(schema) = self.schemas.get(schema_id) {
            return Ok(schema.clone());
        }
        let bytes = storage.get_object(&EnvelopeId::from_bytes(*schema_id.as_bytes()))?;
        let schema = CqrsSchema::from_cbor(&bytes)?;
        self.schemas.insert(*schema_id, schema.clone());
        Ok(schema)
    }

    fn contract(
        &mut self,
        storage: &impl StorageRead,
        contract_id: &ContractId,
    ) -> Result<Vec<u8>, DharmaError> {
        if let Some(bytes) = self.contracts.get(contract_id) {
            return Ok(bytes.clone());
        }
        let bytes = storage.get_object(&EnvelopeId::from_bytes(*contract_id.as_bytes()))?;
        self.contracts.insert(*contract_id, bytes.clone());
        Ok(bytes)
    }
}

#[derive(Clone)]
struct RowSource {
    table: String,
    subject: SubjectId,
    oid: Option<AssertionId>,
    seq: u64,
    schema: CqrsSchema,
    memory: Vec<u8>,
}

#[derive(Clone)]
struct QueryRow {
    base_table: String,
    sources: HashMap<String, RowSource>,
    score: u32,
    derived: HashMap<String, Value>,
}

impl QueryRow {
    fn new(base: RowSource, score: u32) -> Self {
        let base_table = base.table.clone();
        let mut sources = HashMap::new();
        sources.insert(base.table.clone(), base);
        Self {
            base_table,
            sources,
            score,
            derived: HashMap::new(),
        }
    }

    fn to_value(&self) -> Value {
        let base = self.sources.get(&self.base_table);
        let mut entries = Vec::new();
        if let Some(base) = base {
            entries.push((
                Value::Text("subject".to_string()),
                Value::Bytes(base.subject.as_bytes().to_vec()),
            ));
            if let Some(oid) = base.oid {
                entries.push((
                    Value::Text("oid".to_string()),
                    Value::Bytes(oid.as_bytes().to_vec()),
                ));
            }
            entries.push((
                Value::Text("seq".to_string()),
                Value::Integer((base.seq as u64).into()),
            ));
        }
        if self.score > 0 {
            entries.push((
                Value::Text("score".to_string()),
                Value::Integer((self.score as u64).into()),
            ));
        }
        Value::Map(entries)
    }
}

fn load_rows_for_table<B>(
    backend: &B,
    state_store: &Store,
    cache: &mut RuntimeCache,
    table: &str,
    params: &Value,
    score: Option<u32>,
) -> Result<Vec<QueryRow>, DharmaError>
where
    B: StorageRead + StorageQuery,
{
    let mut rows = Vec::new();
    for subject in backend.list_subjects()? {
        let Some(head) = latest_subject_head(backend, &subject)? else {
            continue;
        };
        let schema = match cache.schema(backend, &head.schema_id) {
            Ok(schema) => schema,
            Err(DharmaError::Schema(_)) => continue,
            Err(err) => return Err(err),
        };
        if schema.namespace != table {
            continue;
        }
        let contract_bytes = cache.contract(backend, &head.contract_id)?;
        let state = load_state(
            state_store.env(),
            &subject,
            &schema,
            &contract_bytes,
            head.ver,
        )?;
        let source = RowSource {
            table: table.to_string(),
            subject,
            oid: head.oid,
            seq: state.last_seq,
            schema,
            memory: state.memory,
        };
        rows.push(QueryRow::new(source, score.unwrap_or(0)));
    }
    let _ = params;
    Ok(rows)
}

fn load_rows_for_search<B>(
    backend: &B,
    state_store: &Store,
    cache: &mut RuntimeCache,
    spec: &SearchSpec,
    params: &Value,
) -> Result<Vec<QueryRow>, DharmaError>
where
    B: StorageRead + StorageQuery,
{
    let query_val = eval_expr_value_raw(params, &spec.query)?;
    let Some(query_text) = value_to_text(&query_val) else {
        return Ok(Vec::new());
    };
    let table = match spec.fields.first() {
        Some(path) => path.split('.').take(4).collect::<Vec<_>>().join("."),
        None => return Ok(Vec::new()),
    };
    let rows = load_rows_for_table(backend, state_store, cache, &table, params, None)?;
    let mut filtered = Vec::new();
    for mut row in rows {
        let mut best: Option<u32> = None;
        for field in &spec.fields {
            if let Ok(value) = row_value(&row, params, field) {
                if let Some(text) = value_to_text(&value) {
                    if let Some(score) = search_score(&query_text, &text, spec.fuzz) {
                        best = Some(best.map(|b| b.max(score)).unwrap_or(score));
                    }
                }
            }
        }
        if let Some(score) = best {
            row.score = score;
            filtered.push(row);
        }
    }
    Ok(filtered)
}

fn latest_subject_head(
    backend: &(impl StorageRead + StorageQuery),
    subject: &SubjectId,
) -> Result<Option<SubjectHead>, DharmaError> {
    let mut best: Option<SubjectHead> = None;
    for assertion_id in backend.scan_subject(subject)? {
        let Some(envelope_id) = backend.lookup_envelope(&assertion_id)? else {
            continue;
        };
        let bytes = backend.get_assertion(subject, &envelope_id)?;
        let assertion = match AssertionPlaintext::from_cbor(&bytes) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if assertion.header.sub != *subject {
            continue;
        }
        if best
            .as_ref()
            .map(|h| h.seq >= assertion.header.seq)
            .unwrap_or(false)
        {
            continue;
        }
        best = Some(SubjectHead {
            schema_id: assertion.header.schema,
            contract_id: assertion.header.contract,
            ver: assertion.header.ver,
            seq: assertion.header.seq,
            oid: Some(assertion_id),
        });
    }
    Ok(best)
}

#[derive(Clone)]
struct SubjectHead {
    schema_id: SchemaId,
    contract_id: ContractId,
    ver: u64,
    seq: u64,
    oid: Option<AssertionId>,
}

fn eval_bool(row: &QueryRow, params: &Value, expr: &Expr) -> Result<bool, DharmaError> {
    let value = eval_expr(row, params, expr)?;
    Ok(value == Value::Bool(true))
}

fn eval_usize(params: &Value, expr: &Expr) -> Result<usize, DharmaError> {
    let value = eval_expr_value_raw(params, expr)?;
    if let Value::Integer(val) = value {
        return Ok(val.try_into().unwrap_or(0));
    }
    Ok(0)
}

fn eval_expr(row: &QueryRow, params: &Value, expr: &Expr) -> Result<Value, DharmaError> {
    match expr {
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Path(parts) => {
            let path = parts.join(".");
            row_value(row, params, &path)
        }
        Expr::Unary(op, inner) => {
            let val = eval_expr(row, params, inner)?;
            match op {
                Op::Not => Ok(Value::Bool(!is_truthy(&val))),
                Op::Neg => match val {
                    Value::Integer(num) => {
                        let n = i64::try_from(num).unwrap_or(0);
                        Ok(Value::Integer((-n).into()))
                    }
                    _ => Ok(Value::Null),
                },
                _ => Ok(Value::Null),
            }
        }
        Expr::Binary(op, left, right) => {
            let lval = eval_expr(row, params, left)?;
            let rval = eval_expr(row, params, right)?;
            eval_binary(op, &lval, &rval)
        }
        Expr::Call(name, args) => match name.as_str() {
            "param" => {
                if let Some(Expr::Literal(Value::Integer(idx))) = args.get(0) {
                    return Ok(param_value(params, int_as_usize(idx)));
                }
                if let Some(Expr::Literal(Value::Text(name))) = args.get(0) {
                    return Ok(param_value_named(params, name));
                }
                Ok(Value::Null)
            }
            "len" => {
                let val = args
                    .get(0)
                    .map(|expr| eval_expr(row, params, expr))
                    .transpose()?
                    .unwrap_or(Value::Null);
                Ok(Value::Integer((value_len(&val) as u64).into()))
            }
            "between" => {
                if args.len() < 3 {
                    return Ok(Value::Bool(false));
                }
                let val = eval_expr(row, params, &args[0])?;
                let low = eval_expr(row, params, &args[1])?;
                let high = eval_expr(row, params, &args[2])?;
                Ok(Value::Bool(compare_between(&val, &low, &high)))
            }
            _ => Ok(Value::Null),
        },
    }
}

fn eval_expr_value_raw(params: &Value, expr: &Expr) -> Result<Value, DharmaError> {
    match expr {
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Call(name, args) if name == "param" => {
            if let Some(Expr::Literal(Value::Integer(idx))) = args.get(0) {
                return Ok(param_value(params, int_as_usize(idx)));
            }
            if let Some(Expr::Literal(Value::Text(name))) = args.get(0) {
                return Ok(param_value_named(params, name));
            }
            Ok(Value::Null)
        }
        _ => Ok(Value::Null),
    }
}

fn eval_binary(op: &Op, left: &Value, right: &Value) -> Result<Value, DharmaError> {
    match op {
        Op::And => Ok(Value::Bool(is_truthy(left) && is_truthy(right))),
        Op::Or => Ok(Value::Bool(is_truthy(left) || is_truthy(right))),
        Op::Eq => Ok(Value::Bool(left == right)),
        Op::Neq => Ok(Value::Bool(left != right)),
        Op::Gt => Ok(Value::Bool(
            compare_values(left, right) == Some(std::cmp::Ordering::Greater),
        )),
        Op::Gte => Ok(Value::Bool(matches!(
            compare_values(left, right),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ))),
        Op::Lt => Ok(Value::Bool(
            compare_values(left, right) == Some(std::cmp::Ordering::Less),
        )),
        Op::Lte => Ok(Value::Bool(matches!(
            compare_values(left, right),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ))),
        Op::In => Ok(Value::Bool(value_in_list(left, right))),
        Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Mod => {
            let l = value_as_i64(left);
            let r = value_as_i64(right);
            let out = match op {
                Op::Add => l.saturating_add(r),
                Op::Sub => l.saturating_sub(r),
                Op::Mul => l.saturating_mul(r),
                Op::Div => {
                    if r == 0 {
                        0
                    } else {
                        l / r
                    }
                }
                Op::Mod => {
                    if r == 0 {
                        0
                    } else {
                        l % r
                    }
                }
                _ => 0,
            };
            Ok(Value::Integer(out.into()))
        }
        _ => Ok(Value::Null),
    }
}

fn row_value(row: &QueryRow, _params: &Value, path: &str) -> Result<Value, DharmaError> {
    if let Some(val) = row.derived.get(path) {
        return Ok(val.clone());
    }
    let parts: Vec<&str> = path.split('.').filter(|p| !p.is_empty()).collect();
    if parts.is_empty() {
        return Ok(Value::Null);
    }
    if let Some(val) = row_meta_value(row, &parts) {
        return Ok(val);
    }
    let (source_key, field_path) = resolve_source(row, &parts);
    let Some(source) = row.sources.get(&source_key) else {
        return Ok(Value::Null);
    };
    if field_path.is_empty() {
        return Ok(Value::Null);
    }
    let field = field_path.join(".");
    let (_, value) = read_value_at_path(&source.memory, &source.schema, &field)?;
    Ok(value)
}

fn row_meta_value(row: &QueryRow, parts: &[&str]) -> Option<Value> {
    if parts.len() == 1 {
        let key = parts[0];
        if key == "score" {
            return Some(Value::Integer((row.score as u64).into()));
        }
        if let Some(base) = row.sources.get(&row.base_table) {
            match key {
                "subject" => return Some(Value::Bytes(base.subject.as_bytes().to_vec())),
                "oid" => return base.oid.map(|oid| Value::Bytes(oid.as_bytes().to_vec())),
                "seq" => return Some(Value::Integer((base.seq as u64).into())),
                _ => {}
            }
        }
    }
    if parts.len() >= 2 {
        let prefix = parts[..parts.len() - 1].join(".");
        if let Some(source) = row.sources.get(&prefix) {
            let key = parts[parts.len() - 1];
            match key {
                "subject" => return Some(Value::Bytes(source.subject.as_bytes().to_vec())),
                "oid" => return source.oid.map(|oid| Value::Bytes(oid.as_bytes().to_vec())),
                "seq" => return Some(Value::Integer((source.seq as u64).into())),
                _ => {}
            }
        }
    }
    None
}

fn resolve_source(row: &QueryRow, parts: &[&str]) -> (String, Vec<String>) {
    let mut best = None;
    for idx in (1..=parts.len()).rev() {
        let prefix = parts[..idx].join(".");
        if row.sources.contains_key(&prefix) {
            best = Some((prefix, parts[idx..].iter().map(|s| s.to_string()).collect()));
            break;
        }
    }
    best.unwrap_or_else(|| {
        (
            row.base_table.clone(),
            parts.iter().map(|s| s.to_string()).collect(),
        )
    })
}

fn value_as_i64(value: &Value) -> i64 {
    match value {
        Value::Integer(v) => i64::try_from(*v).unwrap_or(0),
        Value::Text(text) => text.parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => Some(
            i64::try_from(*l)
                .unwrap_or(0)
                .cmp(&i64::try_from(*r).unwrap_or(0)),
        ),
        (Value::Text(l), Value::Text(r)) => Some(l.cmp(r)),
        (Value::Bool(l), Value::Bool(r)) => Some(l.cmp(r)),
        _ => None,
    }
}

fn compare_between(value: &Value, low: &Value, high: &Value) -> bool {
    let Some(ord_low) = compare_values(value, low) else {
        return false;
    };
    let Some(ord_high) = compare_values(value, high) else {
        return false;
    };
    matches!(
        ord_low,
        std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
    ) && matches!(
        ord_high,
        std::cmp::Ordering::Less | std::cmp::Ordering::Equal
    )
}

fn is_truthy(value: &Value) -> bool {
    matches!(value, Value::Bool(true))
}

fn value_in_list(item: &Value, list: &Value) -> bool {
    match list {
        Value::Array(items) => items.iter().any(|v| v == item),
        _ => false,
    }
}

fn int_as_usize(value: &ciborium::value::Integer) -> usize {
    i64::try_from(*value)
        .ok()
        .and_then(|v| if v >= 0 { Some(v as usize) } else { None })
        .unwrap_or(0)
}

fn value_to_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Integer(v) => Some(i64::try_from(*v).unwrap_or(0).to_string()),
        _ => None,
    }
}

fn value_len(value: &Value) -> usize {
    match value {
        Value::Text(text) => text.len(),
        Value::Array(items) => items.len(),
        Value::Map(items) => items.len(),
        _ => 0,
    }
}

fn param_value(params: &Value, idx: usize) -> Value {
    match params {
        Value::Array(items) => items
            .get(idx.saturating_sub(1))
            .cloned()
            .unwrap_or(Value::Null),
        Value::Map(items) => {
            let key = Value::Text(idx.to_string());
            for (k, v) in items {
                if *k == key {
                    return v.clone();
                }
            }
            Value::Null
        }
        _ => Value::Null,
    }
}

fn param_value_named(params: &Value, name: &str) -> Value {
    match params {
        Value::Map(items) => {
            let key = Value::Text(name.to_string());
            for (k, v) in items {
                if *k == key {
                    return v.clone();
                }
            }
            Value::Null
        }
        _ => Value::Null,
    }
}

fn apply_bucket(row: &mut QueryRow, params: &Value, spec: &BucketSpec) -> Result<(), DharmaError> {
    let value = row_value(row, params, &spec.path)?;
    let ts = value_as_i64(&value);
    if spec.size_secs > 0 {
        let size = spec.size_secs as i64;
        let bucket = (ts / size) * size;
        row.derived
            .insert(spec.label.clone(), Value::Integer(bucket.into()));
    }
    Ok(())
}

fn explode_rows(
    rows: Vec<QueryRow>,
    params: &Value,
    spec: &ExplodeSpec,
) -> Result<Vec<QueryRow>, DharmaError> {
    let mut out = Vec::new();
    for row in rows {
        let value = row_value(&row, params, &spec.path)?;
        match value {
            Value::Array(items) => {
                for (idx, item) in items.into_iter().enumerate() {
                    let mut next = row.clone();
                    if let Some(key_name) = &spec.key {
                        next.derived
                            .insert(key_name.clone(), Value::Integer((idx as u64).into()));
                    }
                    next.derived.insert(spec.value.clone(), item);
                    out.push(next);
                }
            }
            Value::Map(items) => {
                for (key, val) in items.into_iter() {
                    let mut next = row.clone();
                    if let Some(key_name) = &spec.key {
                        next.derived.insert(key_name.clone(), key);
                    }
                    next.derived.insert(spec.value.clone(), val);
                    out.push(next);
                }
            }
            _ => {}
        }
    }
    Ok(out)
}

fn group_rows(
    rows: &[QueryRow],
    params: &Value,
    keys: &[String],
) -> Result<Vec<Value>, DharmaError> {
    let mut out = Vec::new();
    for row in rows {
        let mut entry = Vec::new();
        for key in keys {
            let val = row_value(row, params, key)?;
            entry.push((Value::Text(key.clone()), val));
        }
        out.push(Value::Map(entry));
    }
    Ok(out)
}

fn aggregate_rows(
    values: Option<Vec<Value>>,
    rows: &[QueryRow],
    params: &Value,
    specs: &[AggSpec],
) -> Result<Vec<Value>, DharmaError> {
    let base: Vec<Value> =
        values.unwrap_or_else(|| rows.iter().map(|row| row.to_value()).collect());
    if base.is_empty() {
        return Ok(Vec::new());
    }
    let mut totals = Vec::new();
    for spec in specs {
        let value = match spec.func {
            AggFunc::Count => Value::Integer((base.len() as u64).into()),
            AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {
                let mut vals = Vec::new();
                if let Some(path) = &spec.path {
                    for row in rows {
                        if let Ok(val) = row_value(row, params, path) {
                            vals.push(val);
                        }
                    }
                }
                aggregate_values(&spec.func, &vals)
            }
        };
        let alias = spec
            .alias
            .clone()
            .unwrap_or_else(|| spec.func.as_str().to_string());
        totals.push((Value::Text(alias), value));
    }
    Ok(vec![Value::Map(totals)])
}

fn aggregate_values(func: &AggFunc, values: &[Value]) -> Value {
    match func {
        AggFunc::Sum => {
            let sum: i64 = values.iter().map(value_as_i64).sum();
            Value::Integer(sum.into())
        }
        AggFunc::Avg => {
            if values.is_empty() {
                return Value::Integer(0.into());
            }
            let sum: i64 = values.iter().map(value_as_i64).sum();
            Value::Integer((sum / values.len() as i64).into())
        }
        AggFunc::Min => {
            let mut out: Option<Value> = None;
            for value in values {
                out = match out {
                    None => Some(value.clone()),
                    Some(ref existing) => {
                        if compare_values(value, existing) == Some(std::cmp::Ordering::Less) {
                            Some(value.clone())
                        } else {
                            Some(existing.clone())
                        }
                    }
                };
            }
            out.unwrap_or(Value::Null)
        }
        AggFunc::Max => {
            let mut out: Option<Value> = None;
            for value in values {
                out = match out {
                    None => Some(value.clone()),
                    Some(ref existing) => {
                        if compare_values(value, existing) == Some(std::cmp::Ordering::Greater) {
                            Some(value.clone())
                        } else {
                            Some(existing.clone())
                        }
                    }
                };
            }
            out.unwrap_or(Value::Null)
        }
        AggFunc::Count => Value::Integer((values.len() as u64).into()),
    }
}

fn sort_rows(
    rows: &mut Vec<QueryRow>,
    keys: &[SortKey],
    params: &Value,
) -> Result<(), DharmaError> {
    rows.sort_by(|a, b| compare_row_keys(a, b, keys, params).unwrap_or(std::cmp::Ordering::Equal));
    Ok(())
}

fn compare_row_keys(
    a: &QueryRow,
    b: &QueryRow,
    keys: &[SortKey],
    params: &Value,
) -> Result<std::cmp::Ordering, DharmaError> {
    for key in keys {
        let av = row_value(a, params, &key.path)?;
        let bv = row_value(b, params, &key.path)?;
        let ord = compare_values(&av, &bv).unwrap_or(std::cmp::Ordering::Equal);
        if ord != std::cmp::Ordering::Equal {
            return Ok(if key.desc { ord.reverse() } else { ord });
        }
    }
    Ok(std::cmp::Ordering::Equal)
}

fn sort_values(values: &mut Vec<Value>, keys: &[SortKey]) -> Result<(), DharmaError> {
    values.sort_by(|a, b| compare_value_rows(a, b, keys).unwrap_or(std::cmp::Ordering::Equal));
    Ok(())
}

fn compare_value_rows(
    a: &Value,
    b: &Value,
    keys: &[SortKey],
) -> Result<std::cmp::Ordering, DharmaError> {
    let amap = expect_map(a)?;
    let bmap = expect_map(b)?;
    for key in keys {
        let key_name = key.path.split('.').last().unwrap_or(&key.path);
        let av = map_get(amap, key_name).cloned().unwrap_or(Value::Null);
        let bv = map_get(bmap, key_name).cloned().unwrap_or(Value::Null);
        let ord = compare_values(&av, &bv).unwrap_or(std::cmp::Ordering::Equal);
        if ord != std::cmp::Ordering::Equal {
            return Ok(if key.desc { ord.reverse() } else { ord });
        }
    }
    Ok(std::cmp::Ordering::Equal)
}

fn select_row(row: &QueryRow, params: &Value, items: &[SelectItem]) -> Result<Value, DharmaError> {
    let mut out = Vec::new();
    for item in items {
        let val = row_value(row, params, &item.path)?;
        let key = item.alias.clone().unwrap_or_else(|| {
            item.path
                .split('.')
                .last()
                .unwrap_or(&item.path)
                .to_string()
        });
        out.push((Value::Text(key), val));
    }
    Ok(Value::Map(out))
}

fn join_rows<B>(
    backend: &B,
    state_store: &Store,
    cache: &mut RuntimeCache,
    rows: Vec<QueryRow>,
    spec: &JoinSpec,
    params: &Value,
) -> Result<Vec<QueryRow>, DharmaError>
where
    B: StorageRead + StorageQuery,
{
    let right_rows = load_rows_for_table(backend, state_store, cache, &spec.table, params, None)?;
    let mut index: HashMap<KeyValue, RowSource> = HashMap::new();
    for row in right_rows {
        if let Some(source) = row.sources.get(&spec.table) {
            let key_val = row_value(&row, params, &spec.right).ok();
            if let Some(value) = key_val {
                if let Some(key) = KeyValue::from_value(&value) {
                    index.insert(key, source.clone());
                }
            }
        }
    }
    let mut out = Vec::new();
    for mut row in rows {
        let key_val = row_value(&row, params, &spec.left).ok();
        if let Some(value) = key_val {
            if let Some(key) = KeyValue::from_value(&value) {
                if let Some(source) = index.get(&key) {
                    row.sources.insert(spec.table.clone(), source.clone());
                }
            }
        }
        out.push(row);
    }
    Ok(out)
}

#[derive(Hash, PartialEq, Eq)]
enum KeyValue {
    Int(i64),
    Text(String),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl KeyValue {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Integer(v) => Some(KeyValue::Int(i64::try_from(*v).unwrap_or(0))),
            Value::Text(text) => Some(KeyValue::Text(text.clone())),
            Value::Bool(b) => Some(KeyValue::Bool(*b)),
            Value::Bytes(bytes) => Some(KeyValue::Bytes(bytes.clone())),
            _ => None,
        }
    }
}

pub(crate) fn expr_to_value(expr: &Expr) -> Value {
    match expr {
        Expr::Literal(value) => Value::Map(vec![(Value::Text("lit".to_string()), value.clone())]),
        Expr::Path(parts) => Value::Map(vec![(
            Value::Text("path".to_string()),
            Value::Array(parts.iter().map(|p| Value::Text(p.clone())).collect()),
        )]),
        Expr::Unary(op, inner) => Value::Map(vec![
            (
                Value::Text("unary".to_string()),
                Value::Text(op_to_str(*op).to_string()),
            ),
            (Value::Text("expr".to_string()), expr_to_value(inner)),
        ]),
        Expr::Binary(op, left, right) => Value::Map(vec![
            (
                Value::Text("binary".to_string()),
                Value::Text(op_to_str(*op).to_string()),
            ),
            (Value::Text("left".to_string()), expr_to_value(left)),
            (Value::Text("right".to_string()), expr_to_value(right)),
        ]),
        Expr::Call(name, args) => Value::Map(vec![
            (Value::Text("call".to_string()), Value::Text(name.clone())),
            (
                Value::Text("args".to_string()),
                Value::Array(args.iter().map(expr_to_value).collect()),
            ),
        ]),
    }
}

pub(crate) fn expr_from_value(value: &Value) -> Result<Expr, DharmaError> {
    let map = expect_map(value)?;
    if let Some(lit) = map_get(map, "lit") {
        return Ok(Expr::Literal(lit.clone()));
    }
    if let Some(path) = map_get(map, "path") {
        let mut parts = Vec::new();
        for item in expect_array(path)? {
            parts.push(expect_text(item)?);
        }
        return Ok(Expr::Path(parts));
    }
    if let Some(call) = map_get(map, "call") {
        let name = expect_text(call)?;
        let empty_args = Value::Array(Vec::new());
        let args_val = map_get(map, "args").unwrap_or(&empty_args);
        let mut args = Vec::new();
        for item in expect_array(args_val)? {
            args.push(expr_from_value(item)?);
        }
        return Ok(Expr::Call(name, args));
    }
    if let Some(op_val) = map_get(map, "unary") {
        let op = op_from_str(&expect_text(op_val)?)?;
        let expr = map_get(map, "expr")
            .ok_or_else(|| DharmaError::Validation("invalid unary expr".to_string()))?;
        return Ok(Expr::Unary(op, Box::new(expr_from_value(expr)?)));
    }
    if let Some(op_val) = map_get(map, "binary") {
        let op = op_from_str(&expect_text(op_val)?)?;
        let left = map_get(map, "left")
            .ok_or_else(|| DharmaError::Validation("invalid binary expr".to_string()))?;
        let right = map_get(map, "right")
            .ok_or_else(|| DharmaError::Validation("invalid binary expr".to_string()))?;
        return Ok(Expr::Binary(
            op,
            Box::new(expr_from_value(left)?),
            Box::new(expr_from_value(right)?),
        ));
    }
    Err(DharmaError::Validation("invalid expr".to_string()))
}

fn op_to_str(op: Op) -> &'static str {
    match op {
        Op::Add => "+",
        Op::Sub => "-",
        Op::Mul => "*",
        Op::Div => "/",
        Op::Mod => "%",
        Op::In => "in",
        Op::Eq => "==",
        Op::Neq => "!=",
        Op::Gt => ">",
        Op::Lt => "<",
        Op::Gte => ">=",
        Op::Lte => "<=",
        Op::And => "and",
        Op::Or => "or",
        Op::Not => "not",
        Op::Neg => "neg",
    }
}

fn op_from_str(value: &str) -> Result<Op, DharmaError> {
    Ok(match value {
        "+" => Op::Add,
        "-" => Op::Sub,
        "*" => Op::Mul,
        "/" => Op::Div,
        "%" => Op::Mod,
        "in" => Op::In,
        "==" => Op::Eq,
        "!=" => Op::Neq,
        ">" => Op::Gt,
        "<" => Op::Lt,
        ">=" => Op::Gte,
        "<=" => Op::Lte,
        "and" => Op::And,
        "or" => Op::Or,
        "not" => Op::Not,
        "neg" => Op::Neg,
        _ => return Err(DharmaError::Validation("invalid op".to_string())),
    })
}

fn search_score(query: &str, text: &str, fuzz: u8) -> Option<u32> {
    let q = query.to_lowercase();
    let t = text.to_lowercase();
    if t.contains(&q) {
        return Some(100);
    }
    if fuzz == 0 {
        return None;
    }
    let dist = levenshtein(&q, &t);
    if dist <= fuzz as usize {
        Some(90 - dist as u32)
    } else {
        None
    }
}

fn levenshtein(a: &str, b: &str) -> usize {
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }
    let mut costs: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut last = i;
        costs[0] = i + 1;
        for (j, cb) in b.chars().enumerate() {
            let new = if ca == cb { last } else { last + 1 };
            last = costs[j + 1];
            costs[j + 1] = std::cmp::min(std::cmp::min(costs[j] + 1, costs[j + 1] + 1), new);
        }
    }
    costs[b.len()]
}
