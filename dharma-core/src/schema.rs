use crate::cbor;
use crate::error::DharmaError;
use crate::value::{expect_array, expect_bool, expect_map, expect_text, map_get};
use ciborium::value::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaManifest {
    pub v: u64,
    pub name: String,
    pub implements: Vec<String>,
    pub types: BTreeMap<String, SchemaType>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaType {
    pub body: BTreeMap<String, TypeDesc>,
    pub required: BTreeSet<String>,
    pub allow_extra: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeDesc {
    Bool,
    Int,
    Bytes,
    Text,
    Id32,
    PubKey32,
    List(Box<TypeDesc>),
    Map(Box<TypeDesc>, Box<TypeDesc>),
    Enum(Vec<String>),
}

impl SchemaManifest {
    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.to_value())
    }

    pub fn to_value(&self) -> Value {
        let mut types = Vec::new();
        for (name, schema_type) in &self.types {
            types.push((Value::Text(name.clone()), schema_type.to_value()));
        }
        let mut entries = vec![
            (Value::Text("v".to_string()), Value::Integer(self.v.into())),
            (
                Value::Text("name".to_string()),
                Value::Text(self.name.clone()),
            ),
            (Value::Text("types".to_string()), Value::Map(types)),
        ];
        if !self.implements.is_empty() {
            let list = self
                .implements
                .iter()
                .cloned()
                .map(Value::Text)
                .collect::<Vec<_>>();
            entries.push((Value::Text("implements".to_string()), Value::Array(list)));
        }
        Value::Map(entries)
    }
}

impl SchemaType {
    pub fn to_value(&self) -> Value {
        let mut body = Vec::new();
        for (name, desc) in &self.body {
            body.push((Value::Text(name.clone()), desc.to_value()));
        }
        let required = self
            .required
            .iter()
            .cloned()
            .map(Value::Text)
            .collect::<Vec<_>>();
        Value::Map(vec![
            (Value::Text("body".to_string()), Value::Map(body)),
            (Value::Text("required".to_string()), Value::Array(required)),
            (
                Value::Text("allow_extra".to_string()),
                Value::Bool(self.allow_extra),
            ),
        ])
    }
}

impl TypeDesc {
    pub fn to_value(&self) -> Value {
        match self {
            TypeDesc::Bool => Value::Text("bool".to_string()),
            TypeDesc::Int => Value::Text("int".to_string()),
            TypeDesc::Bytes => Value::Text("bytes".to_string()),
            TypeDesc::Text => Value::Text("text".to_string()),
            TypeDesc::Id32 => Value::Text("id32".to_string()),
            TypeDesc::PubKey32 => Value::Text("pubkey32".to_string()),
            TypeDesc::List(inner) => {
                Value::Map(vec![(Value::Text("list".to_string()), inner.to_value())])
            }
            TypeDesc::Map(key, value) => Value::Map(vec![(
                Value::Text("map".to_string()),
                Value::Array(vec![key.to_value(), value.to_value()]),
            )]),
            TypeDesc::Enum(items) => Value::Map(vec![(
                Value::Text("enum".to_string()),
                Value::Array(items.iter().cloned().map(Value::Text).collect()),
            )]),
        }
    }
}

pub fn parse_schema(bytes: &[u8]) -> Result<SchemaManifest, DharmaError> {
    let value = cbor::ensure_canonical(bytes)?;
    parse_schema_value(&value)
}

pub fn parse_schema_value(value: &Value) -> Result<SchemaManifest, DharmaError> {
    let map = expect_map(value)?;
    let v = map_get(map, "v").ok_or_else(|| DharmaError::Schema("missing v".to_string()))?;
    let name =
        map_get(map, "name").ok_or_else(|| DharmaError::Schema("missing name".to_string()))?;
    let types_val =
        map_get(map, "types").ok_or_else(|| DharmaError::Schema("missing types".to_string()))?;
    let implements_val = map_get(map, "implements");

    let version = match v {
        Value::Integer(int) => (*int)
            .try_into()
            .map_err(|_| DharmaError::Schema("invalid v".to_string()))?,
        _ => return Err(DharmaError::Schema("invalid v".to_string())),
    };
    let name = expect_text(name)?;
    let types_map = expect_map(types_val)?;
    let mut types = BTreeMap::new();
    for (k, v) in types_map {
        let typ = expect_text(k)?;
        let schema_type = parse_schema_type(v)?;
        types.insert(typ, schema_type);
    }

    let mut implements = Vec::new();
    if let Some(implements_val) = implements_val {
        let items = expect_array(implements_val)?;
        for item in items {
            implements.push(expect_text(item)?);
        }
    }

    Ok(SchemaManifest {
        v: version,
        name,
        implements,
        types,
    })
}

fn parse_schema_type(value: &Value) -> Result<SchemaType, DharmaError> {
    let map = expect_map(value)?;
    let body_val =
        map_get(map, "body").ok_or_else(|| DharmaError::Schema("missing body".to_string()))?;
    let required_val = map_get(map, "required")
        .ok_or_else(|| DharmaError::Schema("missing required".to_string()))?;
    let allow_extra_val = map_get(map, "allow_extra")
        .ok_or_else(|| DharmaError::Schema("missing allow_extra".to_string()))?;

    let body_map = expect_map(body_val)?;
    let mut body = BTreeMap::new();
    for (k, v) in body_map {
        let field = expect_text(k)?;
        let desc = parse_type_desc(v)?;
        body.insert(field, desc);
    }

    let required_list = expect_array(required_val)?;
    let mut required = BTreeSet::new();
    for entry in required_list {
        required.insert(expect_text(entry)?);
    }

    let allow_extra = expect_bool(allow_extra_val)?;

    Ok(SchemaType {
        body,
        required,
        allow_extra,
    })
}

fn parse_type_desc(value: &Value) -> Result<TypeDesc, DharmaError> {
    match value {
        Value::Text(text) => match text.as_str() {
            "bool" => Ok(TypeDesc::Bool),
            "int" => Ok(TypeDesc::Int),
            "bytes" => Ok(TypeDesc::Bytes),
            "text" => Ok(TypeDesc::Text),
            "id32" => Ok(TypeDesc::Id32),
            "pubkey32" => Ok(TypeDesc::PubKey32),
            _ => Err(DharmaError::Schema("unknown type".to_string())),
        },
        Value::Map(map) => {
            if let Some(list_val) = map_get(map, "list") {
                let inner = parse_type_desc(list_val)?;
                return Ok(TypeDesc::List(Box::new(inner)));
            }
            if let Some(map_val) = map_get(map, "map") {
                let items = expect_array(map_val)?;
                if items.len() != 2 {
                    return Err(DharmaError::Schema("map expects 2 types".to_string()));
                }
                let key = parse_type_desc(&items[0])?;
                let value = parse_type_desc(&items[1])?;
                return Ok(TypeDesc::Map(Box::new(key), Box::new(value)));
            }
            if let Some(enum_val) = map_get(map, "enum") {
                let items = expect_array(enum_val)?;
                let mut variants = Vec::new();
                for item in items {
                    variants.push(expect_text(item)?);
                }
                return Ok(TypeDesc::Enum(variants));
            }
            Err(DharmaError::Schema("invalid type descriptor".to_string()))
        }
        _ => Err(DharmaError::Schema("invalid type descriptor".to_string())),
    }
}

pub fn validate_body(schema: &SchemaManifest, typ: &str, body: &Value) -> Result<(), DharmaError> {
    let schema_type = schema
        .types
        .get(typ)
        .ok_or_else(|| DharmaError::Schema("unknown typ".to_string()))?;
    let map = expect_map(body)?;
    let mut seen = BTreeSet::new();
    for (k, v) in map {
        let field = expect_text(k)?;
        seen.insert(field.clone());
        match schema_type.body.get(&field) {
            Some(desc) => validate_type_desc(desc, v)?,
            None => {
                if !schema_type.allow_extra {
                    return Err(DharmaError::Schema("unexpected field".to_string()));
                }
            }
        }
    }
    for req in &schema_type.required {
        if !seen.contains(req) {
            return Err(DharmaError::Schema("missing required field".to_string()));
        }
    }
    Ok(())
}

fn validate_type_desc(desc: &TypeDesc, value: &Value) -> Result<(), DharmaError> {
    match desc {
        TypeDesc::Bool => match value {
            Value::Bool(_) => Ok(()),
            _ => Err(DharmaError::Schema("expected bool".to_string())),
        },
        TypeDesc::Int => match value {
            Value::Integer(_) => Ok(()),
            _ => Err(DharmaError::Schema("expected int".to_string())),
        },
        TypeDesc::Bytes => match value {
            Value::Bytes(_) => Ok(()),
            _ => Err(DharmaError::Schema("expected bytes".to_string())),
        },
        TypeDesc::Text => match value {
            Value::Text(_) => Ok(()),
            _ => Err(DharmaError::Schema("expected text".to_string())),
        },
        TypeDesc::Id32 | TypeDesc::PubKey32 => match value {
            Value::Bytes(bytes) if bytes.len() == 32 => Ok(()),
            _ => Err(DharmaError::Schema("expected 32-byte".to_string())),
        },
        TypeDesc::List(inner) => match value {
            Value::Array(items) => {
                for item in items {
                    validate_type_desc(inner, item)?;
                }
                Ok(())
            }
            _ => Err(DharmaError::Schema("expected array".to_string())),
        },
        TypeDesc::Map(key_desc, value_desc) => match value {
            Value::Map(entries) => {
                for (k, v) in entries {
                    validate_type_desc(key_desc, k)?;
                    validate_type_desc(value_desc, v)?;
                }
                Ok(())
            }
            _ => Err(DharmaError::Schema("expected map".to_string())),
        },
        TypeDesc::Enum(options) => match value {
            Value::Text(text) if options.contains(text) => Ok(()),
            _ => Err(DharmaError::Schema("expected enum".to_string())),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_value() -> Value {
        Value::Map(vec![
            (Value::Text("v".to_string()), Value::Integer(1.into())),
            (
                Value::Text("name".to_string()),
                Value::Text("demo".to_string()),
            ),
            (
                Value::Text("types".to_string()),
                Value::Map(vec![(
                    Value::Text("task.create".to_string()),
                    Value::Map(vec![
                        (
                            Value::Text("body".to_string()),
                            Value::Map(vec![
                                (
                                    Value::Text("title".to_string()),
                                    Value::Text("text".to_string()),
                                ),
                                (
                                    Value::Text("priority".to_string()),
                                    Value::Map(vec![(
                                        Value::Text("enum".to_string()),
                                        Value::Array(vec![
                                            Value::Text("low".to_string()),
                                            Value::Text("high".to_string()),
                                        ]),
                                    )]),
                                ),
                            ]),
                        ),
                        (
                            Value::Text("required".to_string()),
                            Value::Array(vec![Value::Text("title".to_string())]),
                        ),
                        (Value::Text("allow_extra".to_string()), Value::Bool(false)),
                    ]),
                )]),
            ),
        ])
    }

    #[test]
    fn parse_and_validate_schema() {
        let schema = parse_schema_value(&schema_value()).unwrap();
        let body = Value::Map(vec![
            (
                Value::Text("title".to_string()),
                Value::Text("Do".to_string()),
            ),
            (
                Value::Text("priority".to_string()),
                Value::Text("low".to_string()),
            ),
        ]);
        validate_body(&schema, "task.create", &body).unwrap();
    }

    #[test]
    fn schema_rejects_missing_required() {
        let schema = parse_schema_value(&schema_value()).unwrap();
        let body = Value::Map(vec![]);
        assert!(validate_body(&schema, "task.create", &body).is_err());
    }

    #[test]
    fn schema_rejects_extra_field() {
        let schema = parse_schema_value(&schema_value()).unwrap();
        let body = Value::Map(vec![
            (
                Value::Text("title".to_string()),
                Value::Text("Do".to_string()),
            ),
            (
                Value::Text("extra".to_string()),
                Value::Text("no".to_string()),
            ),
        ]);
        assert!(validate_body(&schema, "task.create", &body).is_err());
    }

    #[test]
    fn parse_schema_bytes_roundtrip() {
        let bytes = crate::cbor::encode_canonical_value(&schema_value()).unwrap();
        let schema = parse_schema(&bytes).unwrap();
        assert_eq!(schema.v, 1);
        assert!(schema.types.contains_key("task.create"));
    }

    #[test]
    fn parse_schema_rejects_invalid_type_desc() {
        let mut value = schema_value();
        if let Value::Map(ref mut entries) = value {
            if let Some((_, types)) = entries
                .iter_mut()
                .find(|(k, _)| matches!(k, Value::Text(text) if text == "types"))
            {
                if let Value::Map(ref mut types_map) = types {
                    if let Some((_, schema_type)) = types_map
                        .iter_mut()
                        .find(|(k, _)| matches!(k, Value::Text(text) if text == "task.create"))
                    {
                        if let Value::Map(ref mut schema_entries) = schema_type {
                            if let Some((_, body)) = schema_entries
                                .iter_mut()
                                .find(|(k, _)| matches!(k, Value::Text(text) if text == "body"))
                            {
                                *body = Value::Text("invalid".to_string());
                            }
                        }
                    }
                }
            }
        }
        assert!(parse_schema_value(&value).is_err());
    }
}
