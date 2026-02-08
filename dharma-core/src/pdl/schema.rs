use crate::cbor;
use crate::error::DharmaError;
use crate::value::{expect_array, expect_map, expect_text, map_get};
use ciborium::value::Value;
use std::collections::BTreeMap;

pub const DEFAULT_TEXT_LEN: usize = 64;
pub const DEFAULT_COLLECTION_BYTES: usize = 128;

#[derive(Clone, Debug, PartialEq)]
pub struct LayoutEntry {
    pub name: String,
    pub offset: usize,
    pub typ: TypeSpec,
    pub size: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CqrsSchema {
    pub namespace: String,
    pub version: String,
    pub aggregate: String,
    pub extends: Option<String>,
    pub implements: Vec<String>,
    pub structs: BTreeMap<String, StructSchema>,
    pub fields: BTreeMap<String, FieldSchema>,
    pub actions: BTreeMap<String, ActionSchema>,
    pub queries: BTreeMap<String, QuerySchema>,
    pub projections: BTreeMap<String, ProjectionSchema>,
    pub concurrency: ConcurrencyMode,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FieldSchema {
    pub typ: TypeSpec,
    pub default: Option<Value>,
    pub visibility: Visibility,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActionSchema {
    pub args: BTreeMap<String, TypeSpec>,
    pub arg_vis: BTreeMap<String, Visibility>,
    pub doc: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QuerySchema {
    pub args: BTreeMap<String, TypeSpec>,
    pub visibility: Visibility,
    pub query: String,
    pub plan: Option<Vec<u8>>,
    pub doc: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectionSchema {
    pub dsl: String,
    pub plan: Vec<u8>,
    pub doc: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StructSchema {
    pub fields: BTreeMap<String, FieldSchema>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Visibility {
    Public,
    Private,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConcurrencyMode {
    Strict,
    Allow,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TypeSpec {
    Int,
    Decimal(Option<u32>),
    Ratio,
    Duration,
    Timestamp,
    Currency,
    Text(Option<usize>),
    Bool,
    Enum(Vec<String>),
    Identity,
    SubjectRef(Option<String>),
    Ref(String),
    Struct(String),
    GeoPoint,
    List(Box<TypeSpec>),
    Map(Box<TypeSpec>, Box<TypeSpec>),
    Optional(Box<TypeSpec>),
}

impl CqrsSchema {
    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.to_value())
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        Self::from_value(&value)
    }

    pub fn to_value(&self) -> Value {
        let mut fields = Vec::new();
        for (name, field) in &self.fields {
            let mut entry = vec![
                (Value::Text("type".to_string()), field.typ.to_value()),
                (
                    Value::Text("vis".to_string()),
                    Value::Text(field.visibility.as_str().to_string()),
                ),
            ];
            if let Some(default) = &field.default {
                entry.push((Value::Text("default".to_string()), default.clone()));
            }
            fields.push((Value::Text(name.clone()), Value::Map(entry)));
        }

        let mut structs = Vec::new();
        for (name, struct_def) in &self.structs {
            let mut struct_fields = Vec::new();
            for (field_name, field) in &struct_def.fields {
                let mut entry = vec![(Value::Text("type".to_string()), field.typ.to_value())];
                if let Some(default) = &field.default {
                    entry.push((Value::Text("default".to_string()), default.clone()));
                }
                struct_fields.push((Value::Text(field_name.clone()), Value::Map(entry)));
            }
            structs.push((Value::Text(name.clone()), Value::Map(struct_fields)));
        }

        let mut actions = Vec::new();
        for (name, action) in &self.actions {
            let mut args_map = Vec::new();
            for (arg, typ) in &action.args {
                args_map.push((Value::Text(arg.clone()), typ.to_value()));
            }
            let mut action_entries = vec![(Value::Text("args".to_string()), Value::Map(args_map))];
            if !action.arg_vis.is_empty() {
                let mut vis_map = Vec::new();
                for (arg, vis) in &action.arg_vis {
                    vis_map.push((
                        Value::Text(arg.clone()),
                        Value::Text(vis.as_str().to_string()),
                    ));
                }
                action_entries.push((Value::Text("arg_vis".to_string()), Value::Map(vis_map)));
            }
            if let Some(doc) = &action.doc {
                action_entries.push((Value::Text("doc".to_string()), Value::Text(doc.clone())));
            }
            actions.push((Value::Text(name.clone()), Value::Map(action_entries)));
        }

        let mut queries = Vec::new();
        for (name, query) in &self.queries {
            let mut args_map = Vec::new();
            for (arg, typ) in &query.args {
                args_map.push((Value::Text(arg.clone()), typ.to_value()));
            }
            let mut query_entries = vec![
                (Value::Text("args".to_string()), Value::Map(args_map)),
                (
                    Value::Text("vis".to_string()),
                    Value::Text(query.visibility.as_str().to_string()),
                ),
                (
                    Value::Text("query".to_string()),
                    Value::Text(query.query.clone()),
                ),
            ];
            if let Some(plan) = &query.plan {
                query_entries.push((Value::Text("plan".to_string()), Value::Bytes(plan.clone())));
            }
            if let Some(doc) = &query.doc {
                query_entries.push((Value::Text("doc".to_string()), Value::Text(doc.clone())));
            }
            queries.push((Value::Text(name.clone()), Value::Map(query_entries)));
        }

        let mut projections = Vec::new();
        for (name, proj) in &self.projections {
            let mut proj_entries = vec![
                (
                    Value::Text("dsl".to_string()),
                    Value::Text(proj.dsl.clone()),
                ),
                (
                    Value::Text("plan".to_string()),
                    Value::Bytes(proj.plan.clone()),
                ),
            ];
            if let Some(doc) = &proj.doc {
                proj_entries.push((Value::Text("doc".to_string()), Value::Text(doc.clone())));
            }
            projections.push((Value::Text(name.clone()), Value::Map(proj_entries)));
        }

        let mut entries = vec![
            (
                Value::Text("type".to_string()),
                Value::Text("core.schema.cqrs".to_string()),
            ),
            (
                Value::Text("namespace".to_string()),
                Value::Text(self.namespace.clone()),
            ),
            (
                Value::Text("version".to_string()),
                Value::Text(self.version.clone()),
            ),
            (
                Value::Text("aggregate".to_string()),
                Value::Text(self.aggregate.clone()),
            ),
            (
                Value::Text("extends".to_string()),
                match &self.extends {
                    Some(ext) => Value::Text(ext.clone()),
                    None => Value::Null,
                },
            ),
            (
                Value::Text("concurrency".to_string()),
                self.concurrency.to_value(),
            ),
            (Value::Text("fields".to_string()), Value::Map(fields)),
            (Value::Text("actions".to_string()), Value::Map(actions)),
            (Value::Text("queries".to_string()), Value::Map(queries)),
            (
                Value::Text("projections".to_string()),
                Value::Map(projections),
            ),
        ];
        if !structs.is_empty() {
            entries.push((Value::Text("structs".to_string()), Value::Map(structs)));
        }
        if !self.implements.is_empty() {
            let list = self
                .implements
                .iter()
                .map(|name| Value::Text(name.clone()))
                .collect();
            entries.push((Value::Text("implements".to_string()), Value::Array(list)));
        }
        Value::Map(entries)
    }

    pub fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let schema_type = expect_text(
            map_get(map, "type").ok_or_else(|| DharmaError::Schema("missing type".to_string()))?,
        )?;
        if schema_type != "core.schema.cqrs" {
            return Err(DharmaError::Schema("unexpected schema type".to_string()));
        }
        let namespace = expect_text(
            map_get(map, "namespace")
                .ok_or_else(|| DharmaError::Schema("missing namespace".to_string()))?,
        )?;
        let version = expect_text(
            map_get(map, "version")
                .ok_or_else(|| DharmaError::Schema("missing version".to_string()))?,
        )?;
        let aggregate = expect_text(
            map_get(map, "aggregate")
                .ok_or_else(|| DharmaError::Schema("missing aggregate".to_string()))?,
        )?;
        let extends = map_get(map, "extends").and_then(|v| match v {
            Value::Text(text) => Some(text.clone()),
            _ => None,
        });
        let concurrency = match map_get(map, "concurrency") {
            Some(value) => ConcurrencyMode::from_value(value)?,
            None => ConcurrencyMode::Strict,
        };
        let mut implements = Vec::new();
        if let Some(value) = map_get(map, "implements") {
            let list = expect_array(value)?;
            for item in list {
                implements.push(expect_text(item)?);
            }
        }
        let fields_val = map_get(map, "fields")
            .ok_or_else(|| DharmaError::Schema("missing fields".to_string()))?;
        let actions_val = map_get(map, "actions")
            .ok_or_else(|| DharmaError::Schema("missing actions".to_string()))?;
        let structs_val = map_get(map, "structs");

        let mut fields = BTreeMap::new();
        for (k, v) in expect_map(fields_val)? {
            let name = expect_text(k)?;
            let field_map = expect_map(v)?;
            let typ_val = map_get(field_map, "type")
                .ok_or_else(|| DharmaError::Schema("missing field type".to_string()))?;
            let typ = TypeSpec::from_value(typ_val)?;
            let default = map_get(field_map, "default").cloned();
            let visibility = map_get(field_map, "vis")
                .map(|v| Visibility::from_value(v))
                .transpose()?
                .unwrap_or(Visibility::Public);
            fields.insert(
                name,
                FieldSchema {
                    typ,
                    default,
                    visibility,
                },
            );
        }

        let mut structs = BTreeMap::new();
        if let Some(value) = structs_val {
            for (k, v) in expect_map(value)? {
                let struct_name = expect_text(k)?;
                let mut struct_fields = BTreeMap::new();
                for (field_k, field_v) in expect_map(v)? {
                    let field_name = expect_text(field_k)?;
                    let field_map = expect_map(field_v)?;
                    let typ_val = map_get(field_map, "type").ok_or_else(|| {
                        DharmaError::Schema("missing struct field type".to_string())
                    })?;
                    let typ = TypeSpec::from_value(typ_val)?;
                    let default = map_get(field_map, "default").cloned();
                    struct_fields.insert(
                        field_name,
                        FieldSchema {
                            typ,
                            default,
                            visibility: Visibility::Public,
                        },
                    );
                }
                structs.insert(
                    struct_name,
                    StructSchema {
                        fields: struct_fields,
                    },
                );
            }
        }

        let mut actions = BTreeMap::new();
        for (k, v) in expect_map(actions_val)? {
            let name = expect_text(k)?;
            let action_map = expect_map(v)?;
            let args_val = map_get(action_map, "args")
                .ok_or_else(|| DharmaError::Schema("missing args".to_string()))?;
            let mut args = BTreeMap::new();
            for (arg_k, arg_v) in expect_map(args_val)? {
                let arg_name = expect_text(arg_k)?;
                let arg_typ = TypeSpec::from_value(arg_v)?;
                args.insert(arg_name, arg_typ);
            }
            let mut arg_vis = BTreeMap::new();
            if let Some(vis_val) = map_get(action_map, "arg_vis") {
                for (vis_k, vis_v) in expect_map(vis_val)? {
                    let arg_name = expect_text(vis_k)?;
                    let vis = Visibility::from_value(vis_v)?;
                    arg_vis.insert(arg_name, vis);
                }
            }
            for arg_name in args.keys() {
                arg_vis
                    .entry(arg_name.clone())
                    .or_insert(Visibility::Public);
            }
            let doc = map_get(action_map, "doc")
                .map(|v| expect_text(v))
                .transpose()?;
            actions.insert(name, ActionSchema { args, arg_vis, doc });
        }

        let mut queries = BTreeMap::new();
        if let Some(queries_val) = map_get(map, "queries") {
            if let Ok(entries) = expect_map(queries_val) {
                for (k, v) in entries {
                    let name = expect_text(k)?;
                    let query_map = expect_map(v)?;
                    let args_val = map_get(query_map, "args")
                        .ok_or_else(|| DharmaError::Schema("missing query args".to_string()))?;
                    let mut args = BTreeMap::new();
                    for (arg_k, arg_v) in expect_map(args_val)? {
                        let arg_name = expect_text(arg_k)?;
                        let arg_typ = TypeSpec::from_value(arg_v)?;
                        args.insert(arg_name, arg_typ);
                    }
                    let visibility = map_get(query_map, "vis")
                        .map(|v| Visibility::from_value(v))
                        .transpose()?
                        .unwrap_or(Visibility::Private);
                    let query = map_get(query_map, "query")
                        .map(|v| expect_text(v))
                        .transpose()?
                        .unwrap_or_default();
                    let plan = map_get(query_map, "plan")
                        .map(|v| crate::value::expect_bytes(v))
                        .transpose()?;
                    let doc = map_get(query_map, "doc")
                        .map(|v| expect_text(v))
                        .transpose()?;
                    queries.insert(
                        name,
                        QuerySchema {
                            args,
                            visibility,
                            query,
                            plan,
                            doc,
                        },
                    );
                }
            }
        }

        let mut projections = BTreeMap::new();
        if let Some(proj_val) = map_get(map, "projections") {
            if let Ok(entries) = expect_map(proj_val) {
                for (k, v) in entries {
                    let name = expect_text(k)?;
                    let proj_map = expect_map(v)?;
                    let dsl = map_get(proj_map, "dsl")
                        .map(|v| expect_text(v))
                        .transpose()?
                        .unwrap_or_default();
                    let plan = map_get(proj_map, "plan")
                        .map(|v| crate::value::expect_bytes(v))
                        .transpose()?
                        .unwrap_or_default();
                    let doc = map_get(proj_map, "doc")
                        .map(|v| expect_text(v))
                        .transpose()?;
                    projections.insert(name, ProjectionSchema { dsl, plan, doc });
                }
            }
        }

        Ok(CqrsSchema {
            namespace,
            version,
            aggregate,
            extends,
            implements,
            structs,
            fields,
            actions,
            queries,
            projections,
            concurrency,
        })
    }

    pub fn action(&self, name: &str) -> Option<&ActionSchema> {
        self.actions.get(name)
    }
}

impl TypeSpec {
    pub fn to_value(&self) -> Value {
        match self {
            TypeSpec::Int => Value::Text("int".to_string()),
            TypeSpec::Decimal(scale) => match scale {
                Some(scale) => Value::Map(vec![(
                    Value::Text("decimal".to_string()),
                    Value::Integer((*scale as u64).into()),
                )]),
                None => Value::Text("decimal".to_string()),
            },
            TypeSpec::Ratio => Value::Text("ratio".to_string()),
            TypeSpec::Duration => Value::Text("duration".to_string()),
            TypeSpec::Timestamp => Value::Text("timestamp".to_string()),
            TypeSpec::Currency => Value::Text("currency".to_string()),
            TypeSpec::Bool => Value::Text("bool".to_string()),
            TypeSpec::Identity => Value::Text("identity".to_string()),
            TypeSpec::SubjectRef(name) => match name {
                Some(name) => Value::Map(vec![(
                    Value::Text("subject_ref".to_string()),
                    Value::Text(name.clone()),
                )]),
                None => Value::Text("subject_ref".to_string()),
            },
            TypeSpec::Text(len) => match len {
                Some(len) => Value::Map(vec![(
                    Value::Text("text".to_string()),
                    Value::Integer((*len as u64).into()),
                )]),
                None => Value::Text("text".to_string()),
            },
            TypeSpec::Enum(variants) => Value::Map(vec![(
                Value::Text("enum".to_string()),
                Value::Array(variants.iter().map(|v| Value::Text(v.clone())).collect()),
            )]),
            TypeSpec::Ref(name) => Value::Map(vec![(
                Value::Text("ref".to_string()),
                Value::Text(name.clone()),
            )]),
            TypeSpec::Struct(name) => Value::Map(vec![(
                Value::Text("struct".to_string()),
                Value::Text(name.clone()),
            )]),
            TypeSpec::GeoPoint => Value::Text("geopoint".to_string()),
            TypeSpec::List(inner) => {
                Value::Map(vec![(Value::Text("list".to_string()), inner.to_value())])
            }
            TypeSpec::Map(key, value) => Value::Map(vec![(
                Value::Text("map".to_string()),
                Value::Array(vec![key.to_value(), value.to_value()]),
            )]),
            TypeSpec::Optional(inner) => Value::Map(vec![(
                Value::Text("optional".to_string()),
                inner.to_value(),
            )]),
        }
    }

    pub fn from_value(value: &Value) -> Result<Self, DharmaError> {
        match value {
            Value::Text(text) => match text.as_str() {
                "int" => Ok(TypeSpec::Int),
                "decimal" => Ok(TypeSpec::Decimal(None)),
                "ratio" => Ok(TypeSpec::Ratio),
                "duration" => Ok(TypeSpec::Duration),
                "timestamp" => Ok(TypeSpec::Timestamp),
                "currency" => Ok(TypeSpec::Currency),
                "bool" => Ok(TypeSpec::Bool),
                "text" => Ok(TypeSpec::Text(None)),
                "identity" => Ok(TypeSpec::Identity),
                "subject_ref" => Ok(TypeSpec::SubjectRef(None)),
                "geopoint" => Ok(TypeSpec::GeoPoint),
                _ => Err(DharmaError::Schema("unknown type".to_string())),
            },
            Value::Map(map) => {
                if let Some(text_val) = map_get(map, "text") {
                    let len = match text_val {
                        Value::Integer(int) => (*int).try_into().ok(),
                        _ => None,
                    };
                    return Ok(TypeSpec::Text(len));
                }
                if let Some(dec_val) = map_get(map, "decimal") {
                    let scale = match dec_val {
                        Value::Integer(int) => (*int).try_into().ok(),
                        _ => None,
                    };
                    return Ok(TypeSpec::Decimal(scale));
                }
                if let Some(enum_val) = map_get(map, "enum") {
                    let items = expect_array(enum_val)?;
                    let mut variants = Vec::new();
                    for item in items {
                        variants.push(expect_text(item)?);
                    }
                    return Ok(TypeSpec::Enum(variants));
                }
                if let Some(ref_val) = map_get(map, "ref") {
                    return Ok(TypeSpec::Ref(expect_text(ref_val)?));
                }
                if let Some(ref_val) = map_get(map, "subject_ref") {
                    return Ok(TypeSpec::SubjectRef(Some(expect_text(ref_val)?)));
                }
                if let Some(struct_val) = map_get(map, "struct") {
                    return Ok(TypeSpec::Struct(expect_text(struct_val)?));
                }
                if let Some(list_val) = map_get(map, "list") {
                    let inner = TypeSpec::from_value(list_val)?;
                    return Ok(TypeSpec::List(Box::new(inner)));
                }
                if let Some(map_val) = map_get(map, "map") {
                    let items = expect_array(map_val)?;
                    if items.len() != 2 {
                        return Err(DharmaError::Schema("invalid map type".to_string()));
                    }
                    let key = TypeSpec::from_value(&items[0])?;
                    let value = TypeSpec::from_value(&items[1])?;
                    return Ok(TypeSpec::Map(Box::new(key), Box::new(value)));
                }
                if let Some(opt_val) = map_get(map, "optional") {
                    let inner = TypeSpec::from_value(opt_val)?;
                    return Ok(TypeSpec::Optional(Box::new(inner)));
                }
                Err(DharmaError::Schema("invalid type descriptor".to_string()))
            }
            _ => Err(DharmaError::Schema("invalid type descriptor".to_string())),
        }
    }
}

impl Visibility {
    pub fn as_str(&self) -> &'static str {
        match self {
            Visibility::Public => "public",
            Visibility::Private => "private",
        }
    }

    pub fn from_value(value: &Value) -> Result<Self, DharmaError> {
        match value {
            Value::Text(text) => match text.as_str() {
                "public" => Ok(Visibility::Public),
                "private" => Ok(Visibility::Private),
                _ => Err(DharmaError::Schema("invalid visibility".to_string())),
            },
            _ => Err(DharmaError::Schema("invalid visibility".to_string())),
        }
    }
}

impl ConcurrencyMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConcurrencyMode::Strict => "strict",
            ConcurrencyMode::Allow => "allow",
        }
    }

    pub fn to_value(&self) -> Value {
        Value::Text(self.as_str().to_string())
    }

    pub fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let text = expect_text(value)?;
        Self::from_str(&text)
    }

    pub fn from_str(value: &str) -> Result<Self, DharmaError> {
        match value {
            "strict" => Ok(ConcurrencyMode::Strict),
            "allow" => Ok(ConcurrencyMode::Allow),
            _ => Err(DharmaError::Schema("invalid concurrency".to_string())),
        }
    }
}

pub fn type_size(typ: &TypeSpec, structs: &BTreeMap<String, StructSchema>) -> usize {
    match typ {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => 8,
        TypeSpec::Ratio => 16,
        TypeSpec::Bool => 1,
        TypeSpec::Enum(_) => 4,
        TypeSpec::Identity | TypeSpec::Ref(_) => 32,
        TypeSpec::SubjectRef(_) => 40,
        TypeSpec::Struct(name) => structs
            .get(name)
            .map(|def| {
                def.fields
                    .values()
                    .map(|field| type_size(&field.typ, structs))
                    .sum()
            })
            .unwrap_or(0),
        TypeSpec::Text(len) => 4 + len.unwrap_or(DEFAULT_TEXT_LEN),
        TypeSpec::Currency => 4 + DEFAULT_TEXT_LEN,
        TypeSpec::GeoPoint => 8,
        TypeSpec::List(inner) => list_storage_size(inner, structs),
        TypeSpec::Map(key, value) => map_storage_size(key, value, structs),
        TypeSpec::Optional(inner) => 1 + type_size(inner, structs),
    }
}

pub fn list_capacity(inner: &TypeSpec, structs: &BTreeMap<String, StructSchema>) -> usize {
    let item_size = type_size(inner, structs);
    collection_capacity(item_size)
}

pub fn map_capacity(
    key: &TypeSpec,
    value: &TypeSpec,
    structs: &BTreeMap<String, StructSchema>,
) -> usize {
    let entry_size = type_size(key, structs) + type_size(value, structs);
    collection_capacity(entry_size)
}

pub fn list_storage_size(inner: &TypeSpec, structs: &BTreeMap<String, StructSchema>) -> usize {
    let item_size = type_size(inner, structs);
    let cap = collection_capacity(item_size);
    4 + cap * item_size
}

pub fn map_storage_size(
    key: &TypeSpec,
    value: &TypeSpec,
    structs: &BTreeMap<String, StructSchema>,
) -> usize {
    let entry_size = type_size(key, structs) + type_size(value, structs);
    let cap = collection_capacity(entry_size);
    4 + cap * entry_size
}

fn collection_capacity(item_size: usize) -> usize {
    if item_size == 0 {
        return 1;
    }
    let cap = DEFAULT_COLLECTION_BYTES / item_size;
    if cap == 0 {
        1
    } else {
        cap
    }
}

pub fn layout_state(schema: &CqrsSchema) -> Vec<LayoutEntry> {
    let mut offset = 0usize;
    let mut out = Vec::new();
    for (name, field) in &schema.fields {
        let size = type_size(&field.typ, &schema.structs);
        out.push(LayoutEntry {
            name: name.clone(),
            offset,
            typ: field.typ.clone(),
            size,
        });
        offset += size;
    }
    out
}

pub fn layout_public(schema: &CqrsSchema) -> Vec<LayoutEntry> {
    layout_by_visibility(schema, Visibility::Public)
}

pub fn layout_private(schema: &CqrsSchema) -> Vec<LayoutEntry> {
    layout_by_visibility(schema, Visibility::Private)
}

fn layout_by_visibility(schema: &CqrsSchema, visibility: Visibility) -> Vec<LayoutEntry> {
    let mut offset = 0usize;
    let mut out = Vec::new();
    for (name, field) in &schema.fields {
        if field.visibility != visibility {
            continue;
        }
        let size = type_size(&field.typ, &schema.structs);
        out.push(LayoutEntry {
            name: name.clone(),
            offset,
            typ: field.typ.clone(),
            size,
        });
        offset += size;
    }
    out
}

pub fn layout_action(
    action: &ActionSchema,
    structs: &BTreeMap<String, StructSchema>,
) -> Vec<LayoutEntry> {
    let mut offset = 4usize;
    let mut out = Vec::new();
    for (name, typ) in &action.args {
        let size = type_size(typ, structs);
        out.push(LayoutEntry {
            name: name.clone(),
            offset,
            typ: typ.clone(),
            size,
        });
        offset += size;
    }
    out
}

pub fn validate_args(action: &ActionSchema, value: &Value) -> Result<(), DharmaError> {
    let map = expect_map(value)?;
    let mut seen = BTreeMap::new();
    for (k, v) in map {
        let name = expect_text(k)?;
        let typ = action
            .args
            .get(&name)
            .ok_or_else(|| DharmaError::Schema("unexpected arg".to_string()))?;
        validate_type(typ, v)?;
        seen.insert(name, true);
    }
    for key in action.args.keys() {
        if !seen.contains_key(key) {
            if let Some(TypeSpec::Optional(_)) = action.args.get(key) {
                continue;
            }
            return Err(DharmaError::Schema("missing arg".to_string()));
        }
    }
    Ok(())
}

fn validate_type(typ: &TypeSpec, value: &Value) -> Result<(), DharmaError> {
    match typ {
        TypeSpec::Optional(inner) => match value {
            Value::Null => Ok(()),
            _ => validate_type(inner, value),
        },
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            match value {
                Value::Integer(_) => Ok(()),
                _ => Err(DharmaError::Schema("expected int".to_string())),
            }
        }
        TypeSpec::Bool => match value {
            Value::Bool(_) => Ok(()),
            _ => Err(DharmaError::Schema("expected bool".to_string())),
        },
        TypeSpec::Text(max) => match value {
            Value::Text(text) => {
                if let Some(max) = max {
                    if text.len() > *max {
                        return Err(DharmaError::Schema("text too long".to_string()));
                    }
                }
                Ok(())
            }
            _ => Err(DharmaError::Schema("expected text".to_string())),
        },
        TypeSpec::Currency => match value {
            Value::Text(text) => {
                if text.len() > DEFAULT_TEXT_LEN {
                    return Err(DharmaError::Schema("text too long".to_string()));
                }
                Ok(())
            }
            _ => Err(DharmaError::Schema("expected text".to_string())),
        },
        TypeSpec::Enum(variants) => match value {
            Value::Text(text) if variants.contains(text) => Ok(()),
            _ => Err(DharmaError::Schema("expected enum".to_string())),
        },
        TypeSpec::Identity | TypeSpec::Ref(_) => match value {
            Value::Bytes(bytes) if bytes.len() == 32 => Ok(()),
            _ => Err(DharmaError::Schema("expected 32-byte".to_string())),
        },
        TypeSpec::SubjectRef(_) => match value {
            Value::Map(entries) => {
                let mut id_ok = false;
                let mut seq_ok = false;
                for (k, v) in entries {
                    if let Value::Text(name) = k {
                        if name == "id" {
                            if let Value::Bytes(bytes) = v {
                                if bytes.len() == 32 {
                                    id_ok = true;
                                }
                            }
                        } else if name == "seq" {
                            if let Value::Integer(int) = v {
                                if u64::try_from(*int).is_ok() {
                                    seq_ok = true;
                                }
                            }
                        }
                    }
                }
                if id_ok && seq_ok {
                    Ok(())
                } else {
                    Err(DharmaError::Schema("expected subject_ref".to_string()))
                }
            }
            _ => Err(DharmaError::Schema("expected subject_ref".to_string())),
        },
        TypeSpec::Struct(_) => match value {
            Value::Map(_) => Ok(()),
            _ => Err(DharmaError::Schema("expected struct".to_string())),
        },
        TypeSpec::GeoPoint => match value {
            Value::Map(entries) => {
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
                if lat.is_some() && lon.is_some() {
                    Ok(())
                } else {
                    Err(DharmaError::Schema("expected geopoint".to_string()))
                }
            }
            Value::Array(items) => {
                if items.len() == 2 {
                    if let (Value::Integer(lat), Value::Integer(lon)) = (&items[0], &items[1]) {
                        if i32::try_from(*lat).is_ok() && i32::try_from(*lon).is_ok() {
                            return Ok(());
                        }
                    }
                }
                Err(DharmaError::Schema("expected geopoint".to_string()))
            }
            _ => Err(DharmaError::Schema("expected geopoint".to_string())),
        },
        TypeSpec::Ratio => match value {
            Value::Map(entries) => {
                let mut num: Option<i64> = None;
                let mut den: Option<i64> = None;
                for (k, v) in entries {
                    if let Value::Text(name) = k {
                        if name == "num" {
                            if let Value::Integer(int) = v {
                                num = (*int).try_into().ok();
                            }
                        } else if name == "den" {
                            if let Value::Integer(int) = v {
                                den = (*int).try_into().ok();
                            }
                        }
                    }
                }
                if num.is_some() && den.is_some() {
                    Ok(())
                } else {
                    Err(DharmaError::Schema("expected ratio".to_string()))
                }
            }
            Value::Array(items) => {
                if items.len() == 2 {
                    if let (Value::Integer(num), Value::Integer(den)) = (&items[0], &items[1]) {
                        if i64::try_from(*num).is_ok() && i64::try_from(*den).is_ok() {
                            return Ok(());
                        }
                    }
                }
                Err(DharmaError::Schema("expected ratio".to_string()))
            }
            _ => Err(DharmaError::Schema("expected ratio".to_string())),
        },
        TypeSpec::List(inner) => match value {
            Value::Array(items) => {
                for item in items {
                    validate_type(inner, item)?;
                }
                Ok(())
            }
            _ => Err(DharmaError::Schema("expected list".to_string())),
        },
        TypeSpec::Map(key, val) => match value {
            Value::Map(entries) => {
                for (k, v) in entries {
                    validate_type(key, k)?;
                    validate_type(val, v)?;
                }
                Ok(())
            }
            _ => Err(DharmaError::Schema("expected map".to_string())),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_roundtrip() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "status".to_string(),
            FieldSchema {
                typ: TypeSpec::Enum(vec!["Open".to_string(), "Closed".to_string()]),
                default: Some(Value::Text("Open".to_string())),
                visibility: Visibility::Public,
            },
        );
        fields.insert(
            "price".to_string(),
            FieldSchema {
                typ: TypeSpec::Decimal(Some(2)),
                default: Some(Value::Integer(1234.into())),
                visibility: Visibility::Public,
            },
        );
        fields.insert(
            "ratio".to_string(),
            FieldSchema {
                typ: TypeSpec::Ratio,
                default: Some(Value::Map(vec![
                    (Value::Text("num".to_string()), Value::Integer(1.into())),
                    (Value::Text("den".to_string()), Value::Integer(2.into())),
                ])),
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("reason".to_string(), TypeSpec::Text(Some(64)));
        actions.insert(
            "Close".to_string(),
            ActionSchema {
                args,
                arg_vis: BTreeMap::new(),
                doc: Some("doc".to_string()),
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
            concurrency: ConcurrencyMode::Strict,
        };
        let bytes = schema.to_cbor().unwrap();
        let parsed = CqrsSchema::from_cbor(&bytes).unwrap();
        assert_eq!(schema.aggregate, parsed.aggregate);
        assert!(parsed.actions.contains_key("Close"));
        assert_eq!(
            parsed.fields.get("price").unwrap().typ,
            TypeSpec::Decimal(Some(2))
        );
        assert_eq!(parsed.fields.get("ratio").unwrap().typ, TypeSpec::Ratio);
    }

    #[test]
    fn validate_args_rejects_missing() {
        let mut args = BTreeMap::new();
        args.insert("name".to_string(), TypeSpec::Text(None));
        let action = ActionSchema {
            args,
            arg_vis: BTreeMap::new(),
            doc: None,
        };
        let value = Value::Map(vec![]);
        assert!(validate_args(&action, &value).is_err());
    }
}
