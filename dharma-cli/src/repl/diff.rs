use ciborium::value::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq)]
pub struct DiffEntry {
    pub path: String,
    pub before: Option<Value>,
    pub after: Option<Value>,
}

pub fn diff_values(before: &Value, after: &Value) -> Vec<DiffEntry> {
    let mut out = Vec::new();
    diff_values_inner(before, after, "", &mut out);
    out
}

fn diff_values_inner(before: &Value, after: &Value, prefix: &str, out: &mut Vec<DiffEntry>) {
    if let (Value::Map(a), Value::Map(b)) = (before, after) {
        let map_a = map_as_text_keyed(a);
        let map_b = map_as_text_keyed(b);
        let mut keys = map_a.keys().cloned().collect::<Vec<_>>();
        for key in map_b.keys() {
            if !map_a.contains_key(key) {
                keys.push(key.clone());
            }
        }
        keys.sort();
        keys.dedup();
        for key in keys {
            let next_path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            let value_a = map_a.get(&key);
            let value_b = map_b.get(&key);
            match (value_a, value_b) {
                (Some(a_val), Some(b_val)) => {
                    if a_val == b_val {
                        continue;
                    }
                    diff_values_inner(a_val, b_val, &next_path, out);
                }
                _ => out.push(DiffEntry {
                    path: next_path,
                    before: value_a.cloned(),
                    after: value_b.cloned(),
                }),
            }
        }
        return;
    }
    if before != after {
        out.push(DiffEntry {
            path: prefix.to_string(),
            before: Some(before.clone()),
            after: Some(after.clone()),
        });
    }
}

fn map_as_text_keyed(entries: &[(Value, Value)]) -> BTreeMap<String, Value> {
    let mut map = BTreeMap::new();
    for (k, v) in entries {
        if let Value::Text(key) = k {
            map.insert(key.clone(), v.clone());
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_values_detects_changes() {
        let before = Value::Map(vec![
            (Value::Text("a".to_string()), Value::Integer(1.into())),
            (Value::Text("b".to_string()), Value::Bool(true)),
        ]);
        let after = Value::Map(vec![
            (Value::Text("a".to_string()), Value::Integer(2.into())),
            (Value::Text("c".to_string()), Value::Text("new".to_string())),
        ]);
        let diff = diff_values(&before, &after);
        assert_eq!(diff.len(), 3);
        assert!(diff.iter().any(|d| d.path == "a"));
        assert!(diff.iter().any(|d| d.path == "b"));
        assert!(diff.iter().any(|d| d.path == "c"));
    }
}
