use crate::error::DharmaError;
use ciborium::value::{CanonicalValue, Value};

pub fn decode_value(bytes: &[u8]) -> Result<Value, DharmaError> {
    let value: Value = ciborium::de::from_reader(bytes)?;
    Ok(value)
}

pub fn encode_canonical_value(value: &Value) -> Result<Vec<u8>, DharmaError> {
    let mut canonical = value.clone();
    canonicalize(&mut canonical);
    let mut out = Vec::new();
    ciborium::ser::into_writer(&canonical, &mut out)?;
    Ok(out)
}

pub fn ensure_canonical(bytes: &[u8]) -> Result<Value, DharmaError> {
    let value = decode_value(bytes)?;
    let canonical = encode_canonical_value(&value)?;
    if canonical == bytes {
        Ok(value)
    } else {
        Err(DharmaError::NonCanonicalCbor)
    }
}

pub fn canonicalize(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items.iter_mut() {
                canonicalize(item);
            }
        }
        Value::Map(entries) => {
            for (k, v) in entries.iter_mut() {
                canonicalize(k);
                canonicalize(v);
            }
            entries.sort_by(|(a, _), (b, _)| {
                let ca = CanonicalValue::from(a.clone());
                let cb = CanonicalValue::from(b.clone());
                ca.cmp(&cb)
            });
        }
        Value::Tag(_, boxed) => canonicalize(boxed),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_map_keys() {
        let mut value = Value::Map(vec![
            (Value::Text("b".to_string()), Value::Integer(1.into())),
            (Value::Text("a".to_string()), Value::Integer(2.into())),
        ]);
        canonicalize(&mut value);
        if let Value::Map(entries) = value {
            assert_eq!(entries[0].0, Value::Text("a".to_string()));
            assert_eq!(entries[1].0, Value::Text("b".to_string()));
        } else {
            panic!("expected map");
        }
    }

    #[test]
    fn ensure_canonical_rejects_noncanonical() {
        let mut value = Value::Map(vec![
            (Value::Text("b".to_string()), Value::Integer(1.into())),
            (Value::Text("a".to_string()), Value::Integer(2.into())),
        ]);
        let mut raw = Vec::new();
        ciborium::ser::into_writer(&value, &mut raw).unwrap();
        assert!(ensure_canonical(&raw).is_err());
        canonicalize(&mut value);
        let canonical = encode_canonical_value(&value).unwrap();
        assert!(ensure_canonical(&canonical).is_ok());
    }
}
