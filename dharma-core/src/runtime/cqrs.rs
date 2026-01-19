use crate::assertion::{signer_from_meta, AssertionPlaintext};
use crate::env::Env;
use crate::error::DharmaError;
use crate::pdl::schema::{
    layout_action, layout_private, layout_public, list_capacity, map_capacity, type_size, ActionSchema,
    CqrsSchema, TypeSpec, DEFAULT_TEXT_LEN,
};
use crate::runtime::vm::{RuntimeVm, OVERLAY_BASE, STATE_SIZE};
use crate::store::state::{list_assertions, list_overlays, load_latest_snapshot_for_ver};
use crate::types::{AssertionId, SubjectId};
use ciborium::value::Value;
use std::collections::{BTreeMap, HashMap};

pub struct LoadedState {
    pub memory: Vec<u8>,
    pub last_seq: u64,
    pub last_object: Option<AssertionId>,
    pub last_overlay_seq: u64,
    pub last_overlay_object: Option<AssertionId>,
}

pub fn load_state(
    env: &dyn Env,
    subject: &SubjectId,
    schema: &CqrsSchema,
    contract: &[u8],
    ver: u64,
) -> Result<LoadedState, DharmaError> {
    load_state_until(env, subject, schema, contract, ver, None)
}

pub fn default_state_memory(schema: &CqrsSchema) -> Vec<u8> {
    default_state(schema)
}

pub fn load_state_until(
    env: &dyn Env,
    subject: &SubjectId,
    schema: &CqrsSchema,
    contract: &[u8],
    ver: u64,
    stop_at: Option<AssertionId>,
) -> Result<LoadedState, DharmaError> {
    let mut memory = match load_latest_snapshot_for_ver(env, subject, ver)? {
        Some(snapshot) => snapshot.memory,
        None => default_state(schema),
    };
    if memory.len() != STATE_SIZE {
        memory.resize(STATE_SIZE, 0);
    }

    let mut last_seq = 0;
    let mut last_object = None;
    if let Some(snapshot) = load_latest_snapshot_for_ver(env, subject, ver)? {
        last_seq = snapshot.header.seq;
        last_object = Some(snapshot.header.last_assertion);
    }

    let mut last_overlay_seq = 0;
    let mut last_overlay_object = None;
    let mut overlay_by_ref: BTreeMap<AssertionId, Value> = BTreeMap::new();
    let overlay_records = list_overlays(env, subject)?;
    for record in overlay_records {
        let overlay = AssertionPlaintext::from_cbor(&record.bytes)?;
        if overlay.header.ver != ver {
            continue;
        }
        if record.seq > last_overlay_seq {
            last_overlay_seq = record.seq;
            last_overlay_object = Some(record.assertion_id);
        }
        if let Some(ref_id) = overlay.header.refs.first() {
            overlay_by_ref.insert(*ref_id, overlay.body.clone());
        }
    }

    let mut assertions: HashMap<AssertionId, AssertionPlaintext> = HashMap::new();
    let records = list_assertions(env, subject)?;
    for record in records {
        if record.seq <= last_seq {
            continue;
        }
        let assertion = AssertionPlaintext::from_cbor(&record.bytes)?;
        if assertion.header.ver != ver {
            continue;
        }
        assertions.insert(record.assertion_id, assertion);
    }

    let order = crate::validation::order_assertions(&assertions)?;
    let vm = RuntimeVm::new(contract.to_vec());
    let mut found_stop = stop_at.is_none();
    for assertion_id in order {
        let assertion = assertions
            .get(&assertion_id)
            .ok_or_else(|| DharmaError::Validation("missing assertion".to_string()))?;
        if assertion.header.typ == "core.merge" {
            if assertion.header.seq >= last_seq {
                last_seq = assertion.header.seq;
                last_object = Some(assertion_id);
            }
            if let Some(stop) = stop_at {
                if assertion_id == stop {
                    found_stop = true;
                    break;
                }
            }
            continue;
        }
        let action_name = assertion
            .header
            .typ
            .strip_prefix("action.")
            .unwrap_or(&assertion.header.typ);
        let action_schema = schema
            .action(action_name)
            .ok_or_else(|| DharmaError::Schema("unknown action".to_string()))?;
        let action_index = action_index(schema, action_name)?;
        let overlay = overlay_by_ref.get(&assertion_id);
        let merged = merge_args(&assertion.body, overlay)?;
        let args_buffer = encode_args_buffer(action_schema, action_index, &merged, true)?;
        let context = context_buffer_for_assertion(assertion);
        vm.reduce(env, &mut memory, &args_buffer, Some(&context))?;
        if assertion.header.seq >= last_seq {
            last_seq = assertion.header.seq;
            last_object = Some(assertion_id);
        }
        if let Some(stop) = stop_at {
            if assertion_id == stop {
                found_stop = true;
                break;
            }
        }
    }
    if !found_stop {
        return Err(DharmaError::Validation("stop-at object not found".to_string()));
    }

    Ok(LoadedState {
        memory,
        last_seq,
        last_object,
        last_overlay_seq,
        last_overlay_object,
    })
}

fn context_buffer_for_assertion(assertion: &AssertionPlaintext) -> Vec<u8> {
    let mut buf = vec![0u8; 40];
    let signer = signer_from_meta(&assertion.header.meta).unwrap_or(assertion.header.sub);
    buf[..32].copy_from_slice(signer.as_bytes());
    let ts = assertion.header.ts.unwrap_or(0);
    buf[32..40].copy_from_slice(&ts.to_le_bytes());
    buf
}

pub fn decode_state(memory: &[u8], schema: &CqrsSchema) -> Result<Value, DharmaError> {
    let mut out = BTreeMap::new();
    let public_layout = layout_public(schema);
    let private_layout = layout_private(schema);
    decode_layout(memory, &public_layout, 0, &mut out)?;
    decode_layout(memory, &private_layout, OVERLAY_BASE, &mut out)?;
    let entries = out
        .into_iter()
        .map(|(k, v)| (Value::Text(k), v))
        .collect();
    Ok(Value::Map(entries))
}

fn decode_layout(
    memory: &[u8],
    layout: &[crate::pdl::schema::LayoutEntry],
    base: usize,
    out: &mut BTreeMap<String, Value>,
) -> Result<(), DharmaError> {
    for entry in layout {
        let offset = base + entry.offset;
        let value = decode_value_at(&entry.typ, memory, offset)?;
        out.insert(entry.name.clone(), value);
    }
    Ok(())
}

fn decode_value_at(typ: &TypeSpec, memory: &[u8], offset: usize) -> Result<Value, DharmaError> {
    match typ {
        TypeSpec::Optional(inner) => {
            if memory[offset] == 0 {
                return Ok(Value::Null);
            }
            return decode_value_at(inner, memory, offset + 1);
        }
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&memory[offset..offset + 8]);
            Ok(Value::Integer(i64::from_le_bytes(buf).into()))
        }
        TypeSpec::Bool => Ok(Value::Bool(memory[offset] != 0)),
        TypeSpec::Enum(variants) => {
            let mut buf = [0u8; 4];
            buf.copy_from_slice(&memory[offset..offset + 4]);
            let idx = u32::from_le_bytes(buf) as usize;
            let name = variants.get(idx).cloned().unwrap_or_default();
            Ok(Value::Text(name))
        }
        TypeSpec::Identity | TypeSpec::Ref(_) => {
            let bytes = memory[offset..offset + 32].to_vec();
            Ok(Value::Bytes(bytes))
        }
        TypeSpec::Text(len) => {
            let max = len.unwrap_or(DEFAULT_TEXT_LEN);
            let mut len_buf = [0u8; 4];
            len_buf.copy_from_slice(&memory[offset..offset + 4]);
            let size = u32::from_le_bytes(len_buf) as usize;
            let start = offset + 4;
            let end = start + max;
            let slice = &memory[start..end];
            let text = String::from_utf8_lossy(&slice[..size.min(slice.len())]).to_string();
            Ok(Value::Text(text))
        }
        TypeSpec::Currency => {
            let max = DEFAULT_TEXT_LEN;
            let mut len_buf = [0u8; 4];
            len_buf.copy_from_slice(&memory[offset..offset + 4]);
            let size = u32::from_le_bytes(len_buf) as usize;
            let start = offset + 4;
            let end = start + max;
            let slice = &memory[start..end];
            let text = String::from_utf8_lossy(&slice[..size.min(slice.len())]).to_string();
            Ok(Value::Text(text))
        }
        TypeSpec::GeoPoint => {
            let mut lat_buf = [0u8; 4];
            let mut lon_buf = [0u8; 4];
            lat_buf.copy_from_slice(&memory[offset..offset + 4]);
            lon_buf.copy_from_slice(&memory[offset + 4..offset + 8]);
            let lat = i32::from_le_bytes(lat_buf) as i64;
            let lon = i32::from_le_bytes(lon_buf) as i64;
            Ok(Value::Map(vec![
                (Value::Text("lat".to_string()), Value::Integer(lat.into())),
                (Value::Text("lon".to_string()), Value::Integer(lon.into())),
            ]))
        }
        TypeSpec::Ratio => {
            let mut num_buf = [0u8; 8];
            let mut den_buf = [0u8; 8];
            num_buf.copy_from_slice(&memory[offset..offset + 8]);
            den_buf.copy_from_slice(&memory[offset + 8..offset + 16]);
            let num = i64::from_le_bytes(num_buf);
            let den = i64::from_le_bytes(den_buf);
            Ok(Value::Map(vec![
                (Value::Text("num".to_string()), Value::Integer(num.into())),
                (Value::Text("den".to_string()), Value::Integer(den.into())),
            ]))
        }
        TypeSpec::List(inner) => {
            let cap = list_capacity(inner);
            let elem_size = type_size(inner);
            let mut len_buf = [0u8; 4];
            len_buf.copy_from_slice(&memory[offset..offset + 4]);
            let len = u32::from_le_bytes(len_buf) as usize;
            if len > cap {
                return Err(DharmaError::Validation("list length exceeds capacity".to_string()));
            }
            let mut out = Vec::with_capacity(len);
            let mut cursor = offset + 4;
            for _ in 0..len {
                out.push(decode_value_at(inner, memory, cursor)?);
                cursor += elem_size;
            }
            Ok(Value::Array(out))
        }
        TypeSpec::Map(key, val) => {
            let cap = map_capacity(key, val);
            let key_size = type_size(key);
            let val_size = type_size(val);
            let entry_size = key_size + val_size;
            let mut len_buf = [0u8; 4];
            len_buf.copy_from_slice(&memory[offset..offset + 4]);
            let len = u32::from_le_bytes(len_buf) as usize;
            if len > cap {
                return Err(DharmaError::Validation("map length exceeds capacity".to_string()));
            }
            let mut out = Vec::with_capacity(len);
            let mut cursor = offset + 4;
            for _ in 0..len {
                let key_val = decode_value_at(key, memory, cursor)?;
                let val_val = decode_value_at(val, memory, cursor + key_size)?;
                out.push((key_val, val_val));
                cursor += entry_size;
            }
            Ok(Value::Map(out))
        }
    }
}

pub fn encode_args_buffer(
    action: &ActionSchema,
    action_index: u32,
    args_value: &Value,
    fill_missing: bool,
) -> Result<Vec<u8>, DharmaError> {
    let layout = layout_action(action);
    let total = layout
        .last()
        .map(|entry| entry.offset + entry.size)
        .unwrap_or(4);
    let mut buffer = vec![0u8; total.max(4)];
    buffer[..4].copy_from_slice(&action_index.to_le_bytes());
    let map = crate::value::expect_map(args_value)?;
    let mut values = BTreeMap::new();
    for (k, v) in map {
        let key = crate::value::expect_text(k)?;
        values.insert(key, v.clone());
    }
    for entry in layout {
        let value = if let Some(value) = values.get(&entry.name) {
            value.clone()
        } else if let TypeSpec::Optional(_) = &entry.typ {
            Value::Null
        } else if fill_missing {
            let default = default_value_for_type(&entry.typ);
            values.insert(entry.name.clone(), default.clone());
            default
        } else {
            return Err(DharmaError::Validation("missing arg".to_string()));
        };
        encode_value_at(&entry.typ, &value, &mut buffer, entry.offset)?;
    }
    Ok(buffer)
}

pub fn decode_args_buffer(action: &ActionSchema, buffer: &[u8]) -> Result<Value, DharmaError> {
    let layout = layout_action(action);
    let mut entries = Vec::new();
    for entry in layout {
        let value = decode_value_at(&entry.typ, buffer, entry.offset)?;
        entries.push((Value::Text(entry.name.clone()), value));
    }
    Ok(Value::Map(entries))
}

fn encode_value_at(
    typ: &TypeSpec,
    value: &Value,
    buffer: &mut [u8],
    offset: usize,
) -> Result<(), DharmaError> {
    match typ {
        TypeSpec::Optional(inner) => {
            if matches!(value, Value::Null) {
                buffer[offset] = 0;
                let size = type_size(inner);
                let start = offset + 1;
                buffer[start..start + size].fill(0);
                return Ok(());
            }
            buffer[offset] = 1;
            return encode_value_at(inner, value, buffer, offset + 1);
        }
        TypeSpec::List(inner) => {
            let cap = list_capacity(inner);
            let elem_size = type_size(inner);
            let items = match value {
                Value::Array(items) => items,
                _ => return Err(DharmaError::Validation("invalid list".to_string())),
            };
            if items.len() > cap {
                return Err(DharmaError::Validation("list length exceeds capacity".to_string()));
            }
            buffer[offset..offset + 4]
                .copy_from_slice(&(items.len() as u32).to_le_bytes());
            let mut cursor = offset + 4;
            for item in items {
                encode_value_at(inner, item, buffer, cursor)?;
                cursor += elem_size;
            }
            let remaining = cap.saturating_sub(items.len());
            if remaining > 0 {
                let start = offset + 4 + items.len() * elem_size;
                let end = start + remaining * elem_size;
                buffer[start..end].fill(0);
            }
            return Ok(());
        }
        TypeSpec::Map(key, val) => {
            let cap = map_capacity(key, val);
            let key_size = type_size(key);
            let val_size = type_size(val);
            let entry_size = key_size + val_size;
            let entries = match value {
                Value::Map(entries) => entries,
                _ => return Err(DharmaError::Validation("invalid map".to_string())),
            };
            if entries.len() > cap {
                return Err(DharmaError::Validation("map length exceeds capacity".to_string()));
            }
            buffer[offset..offset + 4]
                .copy_from_slice(&(entries.len() as u32).to_le_bytes());
            let mut cursor = offset + 4;
            for (k, v) in entries {
                encode_value_at(key, k, buffer, cursor)?;
                encode_value_at(val, v, buffer, cursor + key_size)?;
                cursor += entry_size;
            }
            let remaining = cap.saturating_sub(entries.len());
            if remaining > 0 {
                let start = offset + 4 + entries.len() * entry_size;
                let end = start + remaining * entry_size;
                buffer[start..end].fill(0);
            }
            return Ok(());
        }
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            let int = match value {
                Value::Integer(int) => (*int).try_into().unwrap_or(0i64),
                _ => return Err(DharmaError::Validation("invalid int".to_string())),
            };
            buffer[offset..offset + 8].copy_from_slice(&int.to_le_bytes());
        }
        TypeSpec::Bool => {
            let b = match value {
                Value::Bool(b) => b,
                _ => return Err(DharmaError::Validation("invalid bool".to_string())),
            };
            buffer[offset] = if *b { 1 } else { 0 };
        }
        TypeSpec::Enum(variants) => {
            let text = match value {
                Value::Text(text) => text,
                _ => return Err(DharmaError::Validation("invalid enum".to_string())),
            };
            let idx = variants.iter().position(|v| v == text).unwrap_or(0) as u32;
            buffer[offset..offset + 4].copy_from_slice(&idx.to_le_bytes());
        }
        TypeSpec::Identity | TypeSpec::Ref(_) => {
            let bytes = match value {
                Value::Bytes(bytes) if bytes.len() == 32 => bytes,
                _ => return Err(DharmaError::Validation("invalid identity".to_string())),
            };
            buffer[offset..offset + 32].copy_from_slice(bytes);
        }
        TypeSpec::Text(len) => {
            let max = len.unwrap_or(DEFAULT_TEXT_LEN);
            let text = match value {
                Value::Text(text) => text,
                _ => return Err(DharmaError::Validation("invalid text".to_string())),
            };
            let bytes = text.as_bytes();
            let copy_len = bytes.len().min(max);
            buffer[offset..offset + 4]
                .copy_from_slice(&(copy_len as u32).to_le_bytes());
            let start = offset + 4;
            let end = start + max;
            buffer[start..start + copy_len].copy_from_slice(&bytes[..copy_len]);
            if end > start + copy_len {
                buffer[start + copy_len..end].fill(0);
            }
        }
        TypeSpec::Currency => {
            let max = DEFAULT_TEXT_LEN;
            let text = match value {
                Value::Text(text) => text,
                _ => return Err(DharmaError::Validation("invalid text".to_string())),
            };
            let bytes = text.as_bytes();
            let copy_len = bytes.len().min(max);
            buffer[offset..offset + 4]
                .copy_from_slice(&(copy_len as u32).to_le_bytes());
            let start = offset + 4;
            let end = start + max;
            buffer[start..start + copy_len].copy_from_slice(&bytes[..copy_len]);
            if end > start + copy_len {
                buffer[start + copy_len..end].fill(0);
            }
        }
        TypeSpec::GeoPoint => {
            let (lat, lon) = match value {
                Value::Map(entries) => {
                    let mut lat = None;
                    let mut lon = None;
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
                    match (lat, lon) {
                        (Some(lat), Some(lon)) => (lat, lon),
                        _ => return Err(DharmaError::Validation("invalid geopoint".to_string())),
                    }
                }
                Value::Array(items) => {
                        if items.len() != 2 {
                            return Err(DharmaError::Validation("invalid geopoint".to_string()));
                        }
                        let lat = match &items[0] {
                            Value::Integer(int) => (*int).try_into().unwrap_or(0i32),
                            _ => return Err(DharmaError::Validation("invalid geopoint".to_string())),
                        };
                        let lon = match &items[1] {
                            Value::Integer(int) => (*int).try_into().unwrap_or(0i32),
                            _ => return Err(DharmaError::Validation("invalid geopoint".to_string())),
                        };
                    (lat, lon)
                }
                _ => return Err(DharmaError::Validation("invalid geopoint".to_string())),
            };
            buffer[offset..offset + 4].copy_from_slice(&lat.to_le_bytes());
            buffer[offset + 4..offset + 8].copy_from_slice(&lon.to_le_bytes());
        }
        TypeSpec::Ratio => {
            let (num, den) = match value {
                Value::Map(entries) => {
                    let mut num = None;
                    let mut den = None;
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
                    match (num, den) {
                        (Some(num), Some(den)) => (num, den),
                        _ => return Err(DharmaError::Validation("invalid ratio".to_string())),
                    }
                }
                Value::Array(items) => {
                    if items.len() != 2 {
                        return Err(DharmaError::Validation("invalid ratio".to_string()));
                    }
                    let num = match &items[0] {
                        Value::Integer(int) => (*int).try_into().unwrap_or(0i64),
                        _ => return Err(DharmaError::Validation("invalid ratio".to_string())),
                    };
                    let den = match &items[1] {
                        Value::Integer(int) => (*int).try_into().unwrap_or(0i64),
                        _ => return Err(DharmaError::Validation("invalid ratio".to_string())),
                    };
                    (num, den)
                }
                _ => return Err(DharmaError::Validation("invalid ratio".to_string())),
            };
            buffer[offset..offset + 8].copy_from_slice(&num.to_le_bytes());
            buffer[offset + 8..offset + 16].copy_from_slice(&den.to_le_bytes());
        }
    }
    Ok(())
}

pub fn action_index(schema: &CqrsSchema, action: &str) -> Result<u32, DharmaError> {
    let mut names = schema.actions.keys().cloned().collect::<Vec<_>>();
    names.sort();
    names
        .iter()
        .position(|name| name == action)
        .map(|idx| idx as u32)
        .ok_or_else(|| DharmaError::Schema("unknown action".to_string()))
}

pub fn merge_args(base: &Value, overlay: Option<&Value>) -> Result<Value, DharmaError> {
    let mut merged: BTreeMap<String, Value> = BTreeMap::new();
    for (k, v) in crate::value::expect_map(base)? {
        let name = crate::value::expect_text(k)?;
        merged.insert(name, v.clone());
    }
    if let Some(overlay) = overlay {
        for (k, v) in crate::value::expect_map(overlay)? {
            let name = crate::value::expect_text(k)?;
            merged.insert(name, v.clone());
        }
    }
    let entries = merged
        .into_iter()
        .map(|(k, v)| (Value::Text(k), v))
        .collect();
    Ok(Value::Map(entries))
}

fn default_state(schema: &CqrsSchema) -> Vec<u8> {
    let mut memory = vec![0u8; STATE_SIZE];
    let public_layout = layout_public(schema);
    let private_layout = layout_private(schema);
    apply_defaults(&mut memory, &public_layout, 0, schema);
    apply_defaults(&mut memory, &private_layout, OVERLAY_BASE, schema);
    memory
}

fn default_value_for_type(typ: &TypeSpec) -> Value {
    match typ {
        TypeSpec::Optional(_) => Value::Null,
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            Value::Integer(0.into())
        }
        TypeSpec::Bool => Value::Bool(false),
        TypeSpec::Text(_) | TypeSpec::Currency => Value::Text(String::new()),
        TypeSpec::Enum(variants) => Value::Text(variants.first().cloned().unwrap_or_default()),
        TypeSpec::Identity | TypeSpec::Ref(_) => Value::Bytes(vec![0u8; 32]),
        TypeSpec::GeoPoint => Value::Map(vec![
            (Value::Text("lat".to_string()), Value::Integer(0.into())),
            (Value::Text("lon".to_string()), Value::Integer(0.into())),
        ]),
        TypeSpec::Ratio => Value::Map(vec![
            (Value::Text("num".to_string()), Value::Integer(0.into())),
            (Value::Text("den".to_string()), Value::Integer(0.into())),
        ]),
        TypeSpec::List(_) => Value::Array(Vec::new()),
        TypeSpec::Map(_, _) => Value::Map(Vec::new()),
    }
}

fn apply_defaults(
    memory: &mut [u8],
    layout: &[crate::pdl::schema::LayoutEntry],
    base: usize,
    schema: &CqrsSchema,
) {
    for entry in layout {
        let offset = base + entry.offset;
        match &entry.typ {
            TypeSpec::Optional(inner) => {
                let default = schema.fields[&entry.name].default.clone().unwrap_or(Value::Null);
                if matches!(default, Value::Null) {
                    memory[offset] = 0;
                    let size = type_size(inner);
                    memory[offset + 1..offset + 1 + size].fill(0);
                } else {
                    memory[offset] = 1;
                    let _ = encode_value_at(inner, &default, memory, offset + 1);
                }
            }
            TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
                let value = match &schema.fields[&entry.name].default {
                    Some(Value::Integer(int)) => (*int).try_into().unwrap_or(0i64),
                    _ => 0i64,
                };
                memory[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            }
            TypeSpec::Bool => {
                let value = match &schema.fields[&entry.name].default {
                    Some(Value::Bool(b)) => *b,
                    _ => false,
                };
                memory[offset] = if value { 1 } else { 0 };
            }
            TypeSpec::Text(len) => {
                let max = len.unwrap_or(DEFAULT_TEXT_LEN);
                let text = match &schema.fields[&entry.name].default {
                    Some(Value::Text(text)) => text.clone(),
                    _ => String::new(),
                };
                let bytes = text.as_bytes();
                let copy_len = bytes.len().min(max);
                memory[offset..offset + 4].copy_from_slice(&(copy_len as u32).to_le_bytes());
                let start = offset + 4;
                let end = start + max;
                memory[start..start + copy_len].copy_from_slice(&bytes[..copy_len]);
                if end > start + copy_len {
                    memory[start + copy_len..end].fill(0);
                }
            }
            TypeSpec::Currency => {
                let max = DEFAULT_TEXT_LEN;
                let text = match &schema.fields[&entry.name].default {
                    Some(Value::Text(text)) => text.clone(),
                    _ => String::new(),
                };
                let bytes = text.as_bytes();
                let copy_len = bytes.len().min(max);
                memory[offset..offset + 4].copy_from_slice(&(copy_len as u32).to_le_bytes());
                let start = offset + 4;
                let end = start + max;
                memory[start..start + copy_len].copy_from_slice(&bytes[..copy_len]);
                if end > start + copy_len {
                    memory[start + copy_len..end].fill(0);
                }
            }
            TypeSpec::Enum(variants) => {
                let value = match &schema.fields[&entry.name].default {
                    Some(Value::Text(text)) => variants.iter().position(|v| v == text).unwrap_or(0),
                    _ => 0,
                };
                memory[offset..offset + 4].copy_from_slice(&(value as u32).to_le_bytes());
            }
            TypeSpec::Identity | TypeSpec::Ref(_) => {
                let bytes = match &schema.fields[&entry.name].default {
                    Some(Value::Bytes(bytes)) if bytes.len() == 32 => bytes.clone(),
                    _ => vec![0u8; 32],
                };
                memory[offset..offset + 32].copy_from_slice(&bytes[..32]);
            }
            TypeSpec::GeoPoint => {
                let (lat, lon) = match &schema.fields[&entry.name].default {
                    Some(Value::Map(entries)) => {
                        let mut lat = None;
                        let mut lon = None;
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
                        (lat.unwrap_or(0i32), lon.unwrap_or(0i32))
                    }
                    Some(Value::Array(items)) => {
                        if items.len() == 2 {
                            let lat = match &items[0] {
                                Value::Integer(int) => (*int).try_into().unwrap_or(0i32),
                                _ => 0i32,
                            };
                            let lon = match &items[1] {
                                Value::Integer(int) => (*int).try_into().unwrap_or(0i32),
                                _ => 0i32,
                            };
                            (lat, lon)
                        } else {
                            (0, 0)
                        }
                    }
                    _ => (0, 0),
                };
                memory[offset..offset + 4].copy_from_slice(&lat.to_le_bytes());
                memory[offset + 4..offset + 8].copy_from_slice(&lon.to_le_bytes());
            }
            TypeSpec::Ratio => {
                let default = schema.fields[&entry.name]
                    .default
                    .clone()
                    .unwrap_or_else(|| default_value_for_type(&entry.typ));
                let _ = encode_value_at(&entry.typ, &default, memory, offset);
            }
            TypeSpec::List(_) | TypeSpec::Map(_, _) => {
                let default = schema.fields[&entry.name]
                    .default
                    .clone()
                    .unwrap_or_else(|| match &entry.typ {
                        TypeSpec::List(_) => Value::Array(Vec::new()),
                        TypeSpec::Map(_, _) => Value::Map(Vec::new()),
                        _ => Value::Null,
                    });
                let _ = encode_value_at(&entry.typ, &default, memory, offset);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::crypto;
    use crate::pdl::schema::{ActionSchema, ConcurrencyMode, FieldSchema, Visibility};
    use crate::store::state::append_assertion;
    use crate::types::{ContractId, IdentityKey, SchemaId};
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::collections::BTreeMap;

    #[test]
    fn load_state_orders_by_dependencies() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let subject = SubjectId::from_bytes([1u8; 32]);
        let mut rng = StdRng::seed_from_u64(7);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);

        let mut fields = BTreeMap::new();
        fields.insert(
            "value".to_string(),
            FieldSchema {
                typ: TypeSpec::Int,
                default: Some(Value::Integer(0.into())),
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("value".to_string(), TypeSpec::Int);
        let mut arg_vis = BTreeMap::new();
        arg_vis.insert("value".to_string(), Visibility::Public);
        actions.insert(
            "Set".to_string(),
            ActionSchema {
                args,
                arg_vis,
                doc: None,
            },
        );
        let schema = CqrsSchema {
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Dummy".to_string(),
            extends: None,
            fields,
            actions,
            concurrency: ConcurrencyMode::Strict,
        };

        let wasm = wat::parse_str(
            r#"(module
                (memory (export "memory") 1)
                (func (export "validate") (result i32)
                  i32.const 0)
                (func (export "reduce") (result i32)
                  (local $val i64)
                  i32.const 0x2004
                  i64.load
                  local.set $val
                  i32.const 0
                  local.get $val
                  i64.store
                  i32.const 0)
              )"#,
        )
        .unwrap();

        let header_a = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 2,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let body_a = Value::Map(vec![(Value::Text("value".to_string()), Value::Integer(1.into()))]);
        let assertion_a = AssertionPlaintext::sign(header_a, body_a, &signing_key).unwrap();
        let bytes_a = assertion_a.to_cbor().unwrap();
        let assertion_id_a = assertion_a.assertion_id().unwrap();
        let envelope_id_a = crypto::envelope_id(&bytes_a);

        let header_b = AssertionHeader {
            seq: 1,
            prev: Some(assertion_id_a),
            ..assertion_a.header.clone()
        };
        let body_b = Value::Map(vec![(Value::Text("value".to_string()), Value::Integer(2.into()))]);
        let assertion_b = AssertionPlaintext::sign(header_b, body_b, &signing_key).unwrap();
        let bytes_b = assertion_b.to_cbor().unwrap();
        let assertion_id_b = assertion_b.assertion_id().unwrap();
        let envelope_id_b = crypto::envelope_id(&bytes_b);

        append_assertion(
            &env,
            &subject,
            2,
            assertion_id_a,
            envelope_id_a,
            "Set",
            &bytes_a,
        )
        .unwrap();
        append_assertion(
            &env,
            &subject,
            1,
            assertion_id_b,
            envelope_id_b,
            "Set",
            &bytes_b,
        )
        .unwrap();

        let state = load_state(&env, &subject, &schema, &wasm, DEFAULT_DATA_VERSION).unwrap();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&state.memory[..8]);
        let value = i64::from_le_bytes(buf);
        assert_eq!(value, 2);
    }

    #[test]
    fn encode_optional_arg_allows_null() {
        let mut args = BTreeMap::new();
        args.insert("note".to_string(), TypeSpec::Optional(Box::new(TypeSpec::Int)));
        let action = ActionSchema {
            args,
            arg_vis: BTreeMap::new(),
            doc: None,
        };
        let args_value = Value::Map(vec![]);
        let buffer = encode_args_buffer(&action, 0, &args_value, false).unwrap();
        assert_eq!(buffer[4], 0);
    }

    #[test]
    fn list_roundtrip_layout() {
        let typ = TypeSpec::List(Box::new(TypeSpec::Int));
        let value = Value::Array(vec![Value::Integer(1.into()), Value::Integer(2.into())]);
        let mut buf = vec![0u8; type_size(&typ)];
        encode_value_at(&typ, &value, &mut buf, 0).unwrap();
        let decoded = decode_value_at(&typ, &buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn map_roundtrip_layout() {
        let typ = TypeSpec::Map(Box::new(TypeSpec::Text(Some(4))), Box::new(TypeSpec::Int));
        let value = Value::Map(vec![
            (Value::Text("a".to_string()), Value::Integer(1.into())),
            (Value::Text("b".to_string()), Value::Integer(2.into())),
        ]);
        let mut buf = vec![0u8; type_size(&typ)];
        encode_value_at(&typ, &value, &mut buf, 0).unwrap();
        let decoded = decode_value_at(&typ, &buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn load_state_until_stops_at_object() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let subject = SubjectId::from_bytes([2u8; 32]);
        let mut rng = StdRng::seed_from_u64(9);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);

        let mut fields = BTreeMap::new();
        fields.insert(
            "value".to_string(),
            FieldSchema {
                typ: TypeSpec::Int,
                default: Some(Value::Integer(0.into())),
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("value".to_string(), TypeSpec::Int);
        let mut arg_vis = BTreeMap::new();
        arg_vis.insert("value".to_string(), Visibility::Public);
        actions.insert(
            "Set".to_string(),
            ActionSchema {
                args,
                arg_vis,
                doc: None,
            },
        );
        let schema = CqrsSchema {
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Dummy".to_string(),
            extends: None,
            fields,
            actions,
            concurrency: ConcurrencyMode::Strict,
        };

        let wasm = wat::parse_str(
            r#"(module
                (memory (export "memory") 1)
                (func (export "validate") (result i32)
                  i32.const 0)
                (func (export "reduce") (result i32)
                  (local $val i64)
                  i32.const 0x2004
                  i64.load
                  local.set $val
                  i32.const 0
                  local.get $val
                  i64.store
                  i32.const 0)
              )"#,
        )
        .unwrap();

        let header_a = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let body_a = Value::Map(vec![(Value::Text("value".to_string()), Value::Integer(5.into()))]);
        let assertion_a = AssertionPlaintext::sign(header_a, body_a, &signing_key).unwrap();
        let bytes_a = assertion_a.to_cbor().unwrap();
        let assertion_id_a = assertion_a.assertion_id().unwrap();
        let envelope_id_a = crypto::envelope_id(&bytes_a);

        let header_b = AssertionHeader {
            seq: 2,
            prev: Some(assertion_id_a),
            ..assertion_a.header.clone()
        };
        let body_b = Value::Map(vec![(Value::Text("value".to_string()), Value::Integer(9.into()))]);
        let assertion_b = AssertionPlaintext::sign(header_b, body_b, &signing_key).unwrap();
        let bytes_b = assertion_b.to_cbor().unwrap();
        let assertion_id_b = assertion_b.assertion_id().unwrap();
        let envelope_id_b = crypto::envelope_id(&bytes_b);

        append_assertion(
            &env,
            &subject,
            1,
            assertion_id_a,
            envelope_id_a,
            "Set",
            &bytes_a,
        )
        .unwrap();
        append_assertion(
            &env,
            &subject,
            2,
            assertion_id_b,
            envelope_id_b,
            "Set",
            &bytes_b,
        )
        .unwrap();

        let state = load_state_until(
            &env,
            &subject,
            &schema,
            &wasm,
            DEFAULT_DATA_VERSION,
            Some(assertion_id_a),
        )
        .unwrap();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&state.memory[..8]);
        let value = i64::from_le_bytes(buf);
        assert_eq!(value, 5);
    }

    #[test]
    fn decode_state_reads_public_and_private_fields() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "count".to_string(),
            FieldSchema {
                typ: TypeSpec::Int,
                default: Some(Value::Integer(0.into())),
                visibility: Visibility::Public,
            },
        );
        fields.insert(
            "flag".to_string(),
            FieldSchema {
                typ: TypeSpec::Bool,
                default: Some(Value::Bool(false)),
                visibility: Visibility::Private,
            },
        );
        let schema = CqrsSchema {
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Dummy".to_string(),
            extends: None,
            fields,
            actions: BTreeMap::new(),
            concurrency: ConcurrencyMode::Strict,
        };
        let mut memory = vec![0u8; STATE_SIZE];
        memory[..8].copy_from_slice(&7i64.to_le_bytes());
        memory[OVERLAY_BASE] = 1;
        let value = decode_state(&memory, &schema).unwrap();
        let map = crate::value::expect_map(&value).unwrap();
        let count = crate::value::map_get(map, "count").unwrap().clone();
        let flag = crate::value::map_get(map, "flag").unwrap().clone();
        assert_eq!(count, Value::Integer(7.into()));
        assert_eq!(flag, Value::Bool(true));
    }

    #[test]
    fn encode_decode_decimal_ratio() {
        let mut memory = vec![0u8; 64];
        let decimal_type = TypeSpec::Decimal(Some(2));
        let decimal_val = Value::Integer(1234.into());
        encode_value_at(&decimal_type, &decimal_val, &mut memory, 0).unwrap();
        let decoded = decode_value_at(&decimal_type, &memory, 0).unwrap();
        assert_eq!(decoded, decimal_val);

        let ratio_type = TypeSpec::Ratio;
        let ratio_val = Value::Map(vec![
            (Value::Text("num".to_string()), Value::Integer(3.into())),
            (Value::Text("den".to_string()), Value::Integer(5.into())),
        ]);
        encode_value_at(&ratio_type, &ratio_val, &mut memory, 16).unwrap();
        let decoded_ratio = decode_value_at(&ratio_type, &memory, 16).unwrap();
        let map = crate::value::expect_map(&decoded_ratio).unwrap();
        let num = crate::value::map_get(map, "num").unwrap().clone();
        let den = crate::value::map_get(map, "den").unwrap().clone();
        assert_eq!(num, Value::Integer(3.into()));
        assert_eq!(den, Value::Integer(5.into()));
    }
}
