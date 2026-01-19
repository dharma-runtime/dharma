use crate::error::DharmaError;
use ciborium::value::Value;

pub fn map_get<'a>(map: &'a [(Value, Value)], key: &str) -> Option<&'a Value> {
    map.iter().find_map(|(k, v)| match k {
        Value::Text(text) if text == key => Some(v),
        _ => None,
    })
}

pub fn expect_map(value: &Value) -> Result<&Vec<(Value, Value)>, DharmaError> {
    match value {
        Value::Map(entries) => Ok(entries),
        _ => Err(DharmaError::Validation("expected map".to_string())),
    }
}

pub fn expect_array(value: &Value) -> Result<&Vec<Value>, DharmaError> {
    match value {
        Value::Array(items) => Ok(items),
        _ => Err(DharmaError::Validation("expected array".to_string())),
    }
}

pub fn expect_text(value: &Value) -> Result<String, DharmaError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(DharmaError::Validation("expected text".to_string())),
    }
}

pub fn expect_bytes(value: &Value) -> Result<Vec<u8>, DharmaError> {
    match value {
        Value::Bytes(bytes) => Ok(bytes.clone()),
        _ => Err(DharmaError::Validation("expected bytes".to_string())),
    }
}

pub fn expect_bool(value: &Value) -> Result<bool, DharmaError> {
    match value {
        Value::Bool(b) => Ok(*b),
        _ => Err(DharmaError::Validation("expected bool".to_string())),
    }
}

pub fn expect_uint(value: &Value) -> Result<u64, DharmaError> {
    match value {
        Value::Integer(int) => (*int)
            .try_into()
            .map_err(|_| DharmaError::Validation("expected uint".to_string())),
        _ => Err(DharmaError::Validation("expected uint".to_string())),
    }
}

pub fn expect_int(value: &Value) -> Result<i64, DharmaError> {
    match value {
        Value::Integer(int) => (*int)
            .try_into()
            .map_err(|_| DharmaError::Validation("expected int".to_string())),
        _ => Err(DharmaError::Validation("expected int".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_get_finds_text_key() {
        let value = Value::Map(vec![(Value::Text("k".to_string()), Value::Bool(true))]);
        let map = expect_map(&value).unwrap();
        assert_eq!(expect_bool(map_get(map, "k").unwrap()).unwrap(), true);
    }

    #[test]
    fn expect_bytes_rejects_text() {
        let err = expect_bytes(&Value::Text("no".to_string())).unwrap_err();
        match err {
            DharmaError::Validation(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn expect_uint_rejects_negative() {
        let err = expect_uint(&Value::Integer((-1).into())).unwrap_err();
        match err {
            DharmaError::Validation(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
