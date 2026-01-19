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
    pub fields: BTreeMap<String, FieldSchema>,
    pub actions: BTreeMap<String, ActionSchema>,
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
    Ref(String),
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

        let mut actions = Vec::new();
        for (name, action) in &self.actions {
            let mut args_map = Vec::new();
            for (arg, typ) in &action.args {
                args_map.push((Value::Text(arg.clone()), typ.to_value()));
            }
            let mut action_entries = vec![(
                Value::Text("args".to_string()),
                Value::Map(args_map),
            )];
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

        Value::Map(vec![
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
            (
                Value::Text("actions".to_string()),
                Value::Map(actions),
            ),
        ])
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
            map_get(map, "version").ok_or_else(|| DharmaError::Schema("missing version".to_string()))?,
        )?;
        let aggregate = expect_text(
            map_get(map, "aggregate")
                .ok_or_else(|| DharmaError::Schema("missing aggregate".to_string()))?,
        )?;
        let extends = map_get(map, "extends")
            .and_then(|v| match v {
                Value::Text(text) => Some(text.clone()),
                _ => None,
            });
        let concurrency = match map_get(map, "concurrency") {
            Some(value) => ConcurrencyMode::from_value(value)?,
            None => ConcurrencyMode::Strict,
        };
        let fields_val = map_get(map, "fields").ok_or_else(|| DharmaError::Schema("missing fields".to_string()))?;
        let actions_val =
            map_get(map, "actions").ok_or_else(|| DharmaError::Schema("missing actions".to_string()))?;

        let mut fields = BTreeMap::new();
        for (k, v) in expect_map(fields_val)? {
            let name = expect_text(k)?;
            let field_map = expect_map(v)?;
            let typ_val = map_get(field_map, "type").ok_or_else(|| DharmaError::Schema("missing field type".to_string()))?;
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

        let mut actions = BTreeMap::new();
        for (k, v) in expect_map(actions_val)? {
            let name = expect_text(k)?;
            let action_map = expect_map(v)?;
            let args_val = map_get(action_map, "args").ok_or_else(|| DharmaError::Schema("missing args".to_string()))?;
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
                arg_vis.entry(arg_name.clone()).or_insert(Visibility::Public);
            }
            let doc = map_get(action_map, "doc").map(|v| expect_text(v)).transpose()?;
            actions.insert(
                name,
                ActionSchema {
                    args,
                    arg_vis,
                    doc,
                },
            );
        }

        Ok(CqrsSchema {
            namespace,
            version,
            aggregate,
            extends,
            fields,
            actions,
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
            TypeSpec::GeoPoint => Value::Text("geopoint".to_string()),
            TypeSpec::List(inner) => {
                Value::Map(vec![(Value::Text("list".to_string()), inner.to_value())])
            }
            TypeSpec::Map(key, value) => Value::Map(vec![(
                Value::Text("map".to_string()),
                Value::Array(vec![key.to_value(), value.to_value()]),
            )]),
            TypeSpec::Optional(inner) => {
                Value::Map(vec![(Value::Text("optional".to_string()), inner.to_value())])
            }
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

pub fn type_size(typ: &TypeSpec) -> usize {
    match typ {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => 8,
        TypeSpec::Ratio => 16,
        TypeSpec::Bool => 1,
        TypeSpec::Enum(_) => 4,
        TypeSpec::Identity | TypeSpec::Ref(_) => 32,
        TypeSpec::Text(len) => 4 + len.unwrap_or(DEFAULT_TEXT_LEN),
        TypeSpec::Currency => 4 + DEFAULT_TEXT_LEN,
        TypeSpec::GeoPoint => 8,
        TypeSpec::List(inner) => list_storage_size(inner),
        TypeSpec::Map(key, value) => map_storage_size(key, value),
        TypeSpec::Optional(inner) => 1 + type_size(inner),
    }
}

pub fn list_capacity(inner: &TypeSpec) -> usize {
    let item_size = type_size(inner);
    collection_capacity(item_size)
}

pub fn map_capacity(key: &TypeSpec, value: &TypeSpec) -> usize {
    let entry_size = type_size(key) + type_size(value);
    collection_capacity(entry_size)
}

pub fn list_storage_size(inner: &TypeSpec) -> usize {
    let item_size = type_size(inner);
    let cap = collection_capacity(item_size);
    4 + cap * item_size
}

pub fn map_storage_size(key: &TypeSpec, value: &TypeSpec) -> usize {
    let entry_size = type_size(key) + type_size(value);
    let cap = collection_capacity(entry_size);
    4 + cap * entry_size
}

fn collection_capacity(item_size: usize) -> usize {
    if item_size == 0 {
        return 1;
    }
    let cap = DEFAULT_COLLECTION_BYTES / item_size;
    if cap == 0 { 1 } else { cap }
}

pub fn layout_state(schema: &CqrsSchema) -> Vec<LayoutEntry> {
    let mut offset = 0usize;
    let mut out = Vec::new();
    for (name, field) in &schema.fields {
        let size = type_size(&field.typ);
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
        let size = type_size(&field.typ);
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

pub fn layout_action(action: &ActionSchema) -> Vec<LayoutEntry> {
    let mut offset = 4usize;
    let mut out = Vec::new();
    for (name, typ) in &action.args {
        let size = type_size(typ);
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
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => match value {
            Value::Integer(_) => Ok(()),
            _ => Err(DharmaError::Schema("expected int".to_string())),
        },
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
            fields,
            actions,
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
