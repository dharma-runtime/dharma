use dharma::runtime::vm::RuntimeVm;
use dharma::runtime::cqrs::{action_index, encode_args_buffer, load_state};
use dharma::store::state::{append_assertion, append_overlay, save_snapshot, Snapshot, SnapshotHeader};
use dharma::DharmaError;
use ciborium::value::Value;
use dharma::assertion::{add_signer_meta, AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
use dharma::crypto;
use dharma::pdl::schema::{validate_args, ConcurrencyMode, CqrsSchema, TypeSpec, Visibility};
use dharma::store::index::FrontierIndex;
use dharma::types::{AssertionId, ContractId, SchemaId, SubjectId};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn action_cmd(subject_hex: &str, action: &str, args: &[String]) -> Result<(), DharmaError> {
    let data_dir = crate::ensure_data_dir()?;
    let env = dharma::env::StdEnv::new(&data_dir);
    crate::ensure_identity_present(&env)?;
    let identity = crate::load_identity(&env)?;
    let _head = crate::mount_self(&env, &identity)?;
    let store = dharma::Store::new(&env);

    let subject = SubjectId::from_hex(subject_hex)?;
    let mut keys = HashMap::new();
    keys.insert(identity.subject_id, identity.subject_key);
    if !store.subject_dir(&subject).join("assertions").exists() {
        store.rebuild_subject_views(&keys)?;
    }
    let (ver, filtered_args) = extract_ver(args);
    let (schema_id, contract_id) = load_contract_ids_for_ver(&data_dir, ver)?;
    let schema_bytes = load_schema_bytes(&data_dir, &schema_id)?;
    let contract_bytes = load_contract_bytes(&data_dir, &contract_id)?;
    let schema = CqrsSchema::from_cbor(&schema_bytes)?;

    let action_schema = schema
        .action(action)
        .ok_or_else(|| DharmaError::Schema("unknown action".to_string()))?;
    let args_value = parse_args(action_schema, &filtered_args)?;
    let (_assertion_id, seq) = apply_action_prepared(
        &data_dir,
        &identity,
        subject,
        action,
        args_value,
        ver,
        schema_id,
        contract_id,
        &schema,
        &contract_bytes,
        None,
    )?;
    println!("Applied {action} seq {seq}");
    Ok(())
}

pub(crate) fn apply_action_prepared(
    data_dir: &PathBuf,
    identity: &dharma::IdentityState,
    subject: SubjectId,
    action: &str,
    args_value: Value,
    ver: u64,
    schema_id: SchemaId,
    contract_id: ContractId,
    schema: &CqrsSchema,
    contract_bytes: &[u8],
    meta: Option<Value>,
) -> Result<(AssertionId, u64), DharmaError> {
    let action_schema = schema
        .action(action)
        .ok_or_else(|| DharmaError::Schema("unknown action".to_string()))?;
    validate_args(action_schema, &args_value)?;
    let action_index = action_index(schema, action)?;
    let args_buffer = encode_args_buffer(action_schema, action_index, &args_value, false)?;
    let (base_args, overlay_args) = split_args(action_schema, &args_value)?;

    let env = dharma::env::StdEnv::new(data_dir);
    let mut state = load_state(&env, &subject, schema, contract_bytes, ver)?;
    let index = FrontierIndex::new(data_dir)?;
    ensure_concurrency(schema, &index, &subject, ver, state.last_object)?;

    let last_seq = state.last_seq;
    let prev = state.last_object;
    let last_overlay_seq = state.last_overlay_seq;
    let last_overlay_object = state.last_overlay_object;

    let vm = RuntimeVm::new(contract_bytes.to_vec());
    let context = build_context(identity);
    vm.validate(&env, &mut state.memory, &args_buffer, Some(&context))?;
    vm.reduce(&env, &mut state.memory, &args_buffer, Some(&context))?;

    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver,
        sub: subject,
        typ: format!("action.{action}"),
        auth: identity.public_key,
        seq: last_seq + 1,
        prev,
        refs: Vec::new(),
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(meta.clone(), &identity.subject_id),
    };
    let assertion = AssertionPlaintext::sign(header, base_args.clone(), &identity.signing_key)?;
    let bytes = assertion.to_cbor()?;
    let assertion_id = assertion.assertion_id()?;
    let envelope_id = crypto::envelope_id(&bytes);

    let store = dharma::Store::new(&env);
    store.put_assertion(&subject, &envelope_id, &bytes)?;
    store.record_semantic(&assertion_id, &envelope_id)?;
    append_assertion(
        &env,
        &subject,
        last_seq + 1,
        assertion_id,
        envelope_id,
        action,
        &bytes,
    )?;

    if !is_empty_args(&overlay_args) {
        let overlay_meta = overlay_meta(meta.clone());
        let overlay_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver,
            sub: subject,
            typ: format!("action.{action}"),
            auth: identity.public_key,
            seq: last_overlay_seq + 1,
            prev: last_overlay_object,
            refs: vec![assertion_id],
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: None,
            meta: add_signer_meta(overlay_meta, &identity.subject_id),
        };
        let overlay_assertion =
            AssertionPlaintext::sign(overlay_header, overlay_args.clone(), &identity.signing_key)?;
        let overlay_bytes = overlay_assertion.to_cbor()?;
        let overlay_assertion_id = overlay_assertion.assertion_id()?;
        let overlay_envelope_id = crypto::envelope_id(&overlay_bytes);
        store.put_assertion(&subject, &overlay_envelope_id, &overlay_bytes)?;
        store.record_semantic(&overlay_assertion_id, &overlay_envelope_id)?;
        append_overlay(
            &env,
            &subject,
            last_overlay_seq + 1,
            overlay_assertion_id,
            overlay_envelope_id,
            action,
            &overlay_bytes,
        )?;
    }

    if (last_seq + 1) % 50 == 0 {
        let snapshot = Snapshot {
            header: SnapshotHeader {
                seq: last_seq + 1,
                ver,
                last_assertion: assertion_id,
                timestamp: now_timestamp(),
            },
            memory: state.memory.clone(),
        };
        save_snapshot(&env, &subject, &snapshot)?;
    }

    Ok((assertion_id, last_seq + 1))
}

fn overlay_meta(meta: Option<Value>) -> Option<Value> {
    let mut entries = match meta {
        Some(Value::Map(entries)) => entries,
        _ => Vec::new(),
    };
    entries.push((Value::Text("overlay".to_string()), Value::Bool(true)));
    Some(Value::Map(entries))
}

fn parse_args(action: &crate::pdl::schema::ActionSchema, args: &[String]) -> Result<Value, DharmaError> {
    let mut supplied = BTreeMap::new();
    for arg in args {
        let (key, value) = arg
            .split_once('=')
            .ok_or_else(|| DharmaError::Validation("invalid arg".to_string()))?;
        supplied.insert(key.to_string(), value.to_string());
    }
    for key in supplied.keys() {
        if !action.args.contains_key(key) {
            return Err(DharmaError::Validation("unknown arg".to_string()));
        }
    }
    let mut entries = Vec::new();
    for (name, typ) in &action.args {
        let raw = supplied
            .get(name)
            .ok_or_else(|| DharmaError::Validation("missing arg".to_string()))?;
        let value = parse_value(raw, typ)?;
        entries.push((Value::Text(name.clone()), value));
    }
    Ok(Value::Map(entries))
}

fn ensure_concurrency(
    schema: &CqrsSchema,
    index: &FrontierIndex,
    subject: &SubjectId,
    ver: u64,
    prev: Option<AssertionId>,
) -> Result<(), DharmaError> {
    if schema.concurrency != ConcurrencyMode::Strict {
        return Ok(());
    }
    let tips = index.get_tips_for_ver(subject, ver);
    if tips.len() > 1 {
        return Err(DharmaError::Validation(
            "fork detected; merge required".to_string(),
        ));
    }
    if let Some(prev_id) = prev {
        if tips.len() == 1 && tips[0] != prev_id {
            return Err(DharmaError::Validation(
                "fork detected; merge required".to_string(),
            ));
        }
    }
    Ok(())
}

fn split_args(
    action: &crate::pdl::schema::ActionSchema,
    args_value: &Value,
) -> Result<(Value, Value), DharmaError> {
    let map = dharma::value::expect_map(args_value)?;
    let mut public_entries = Vec::new();
    let mut private_entries = Vec::new();
    for (k, v) in map {
        let name = dharma::value::expect_text(k)?;
        let visibility = action
            .arg_vis
            .get(&name)
            .copied()
            .unwrap_or(Visibility::Public);
        let entry = (Value::Text(name), v.clone());
        match visibility {
            Visibility::Public => public_entries.push(entry),
            Visibility::Private => private_entries.push(entry),
        }
    }
    Ok((Value::Map(public_entries), Value::Map(private_entries)))
}

fn is_empty_args(value: &Value) -> bool {
    match value {
        Value::Map(map) => map.is_empty(),
        _ => true,
    }
}

fn parse_value(raw: &str, typ: &TypeSpec) -> Result<Value, DharmaError> {
    match typ {
        TypeSpec::Optional(inner) => {
            if raw.trim() == "null" {
                return Ok(Value::Null);
            }
            parse_value(raw, inner)
        }
        TypeSpec::Int | TypeSpec::Duration | TypeSpec::Timestamp => raw
            .parse::<i64>()
            .map(|v| Value::Integer(v.into()))
            .map_err(|_| DharmaError::Validation("invalid int".to_string())),
        TypeSpec::Decimal(scale) => {
            let mantissa = parse_decimal_arg(raw, *scale)?;
            Ok(Value::Integer(mantissa.into()))
        }
        TypeSpec::Bool => match raw {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => Err(DharmaError::Validation("invalid bool".to_string())),
        },
        TypeSpec::Text(_) | TypeSpec::Currency => Ok(Value::Text(raw.to_string())),
        TypeSpec::Enum(_) => Ok(Value::Text(raw.to_string())),
        TypeSpec::Identity | TypeSpec::Ref(_) => {
            let bytes = dharma::types::hex_decode(raw)?;
            if bytes.len() != 32 {
                return Err(DharmaError::Validation("invalid identity".to_string()));
            }
            Ok(Value::Bytes(bytes))
        }
        TypeSpec::GeoPoint => {
            let parts: Vec<&str> = raw.split(',').collect();
            if parts.len() != 2 {
                return Err(DharmaError::Validation("invalid geopoint".to_string()));
            }
            let lat = parts[0].trim().parse::<i64>().map_err(|_| {
                DharmaError::Validation("invalid geopoint".to_string())
            })?;
            let lon = parts[1].trim().parse::<i64>().map_err(|_| {
                DharmaError::Validation("invalid geopoint".to_string())
            })?;
            Ok(Value::Array(vec![
                Value::Integer(lat.into()),
                Value::Integer(lon.into()),
            ]))
        }
        TypeSpec::Ratio => {
            let (num, den) = parse_ratio_arg(raw)?;
            Ok(Value::Map(vec![
                (Value::Text("num".to_string()), Value::Integer(num.into())),
                (Value::Text("den".to_string()), Value::Integer(den.into())),
            ]))
        }
        TypeSpec::List(_) | TypeSpec::Map(_, _) => {
            Err(DharmaError::Validation("collection args unsupported".to_string()))
        }
    }
}

fn parse_decimal_arg(raw: &str, scale: Option<u32>) -> Result<i64, DharmaError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(DharmaError::Validation("invalid decimal".to_string()));
    }
    let negative = trimmed.starts_with('-');
    let unsigned = trimmed.strip_prefix('-').unwrap_or(trimmed);
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((left, right)) => (left, Some(right)),
        None => (unsigned, None),
    };
    let scale = scale.unwrap_or(0);
    if frac_part.is_some() && scale == 0 {
        return Err(DharmaError::Validation("decimal scale required".to_string()));
    }
    let int_str = if int_part.is_empty() { "0" } else { int_part };
    let int_val = int_str
        .parse::<i64>()
        .map_err(|_| DharmaError::Validation("invalid decimal".to_string()))?;
    let factor = pow10(scale)?;
    let mut mantissa = int_val
        .checked_mul(factor)
        .ok_or_else(|| DharmaError::Validation("decimal overflow".to_string()))?;
    if let Some(frac) = frac_part {
        if frac.len() > scale as usize {
            return Err(DharmaError::Validation("decimal scale overflow".to_string()));
        }
        let mut frac_buf = String::from(frac);
        while frac_buf.len() < scale as usize {
            frac_buf.push('0');
        }
        let frac_val = if frac_buf.is_empty() {
            0i64
        } else {
            frac_buf
                .parse::<i64>()
                .map_err(|_| DharmaError::Validation("invalid decimal".to_string()))?
        };
        mantissa = mantissa
            .checked_add(frac_val)
            .ok_or_else(|| DharmaError::Validation("decimal overflow".to_string()))?;
    }
    if negative {
        Ok(-mantissa)
    } else {
        Ok(mantissa)
    }
}

fn parse_ratio_arg(raw: &str) -> Result<(i64, i64), DharmaError> {
    let trimmed = raw.trim();
    let (num_raw, den_raw) = if let Some(pair) = trimmed.split_once('/') {
        pair
    } else if let Some(pair) = trimmed.split_once(',') {
        pair
    } else {
        return Err(DharmaError::Validation("invalid ratio".to_string()));
    };
    let num = num_raw
        .trim()
        .parse::<i64>()
        .map_err(|_| DharmaError::Validation("invalid ratio".to_string()))?;
    let den = den_raw
        .trim()
        .parse::<i64>()
        .map_err(|_| DharmaError::Validation("invalid ratio".to_string()))?;
    Ok((num, den))
}

fn pow10(scale: u32) -> Result<i64, DharmaError> {
    let mut out = 1i64;
    for _ in 0..scale {
        out = out
            .checked_mul(10)
            .ok_or_else(|| DharmaError::Validation("decimal overflow".to_string()))?;
    }
    Ok(out)
}

#[cfg(test)]
mod parse_tests {
    use super::*;

    #[test]
    fn parse_decimal_arg_scales() {
        assert_eq!(parse_decimal_arg("12.30", Some(2)).unwrap(), 1230);
        assert_eq!(parse_decimal_arg("-1.5", Some(2)).unwrap(), -150);
    }

    #[test]
    fn parse_ratio_arg_parses() {
        assert_eq!(parse_ratio_arg("3/5").unwrap(), (3, 5));
        assert_eq!(parse_ratio_arg("7,9").unwrap(), (7, 9));
    }
}


pub(crate) fn build_context(identity: &dharma::IdentityState) -> Vec<u8> {
    let mut buf = vec![0u8; 40];
    buf[..32].copy_from_slice(identity.subject_id.as_bytes());
    let timestamp = now_timestamp() as i64;
    buf[32..40].copy_from_slice(&timestamp.to_le_bytes());
    buf
}

pub(crate) fn load_contract_ids_for_ver(
    root: &PathBuf,
    ver: u64,
) -> Result<(SchemaId, ContractId), DharmaError> {
    let config = fs::read_to_string(root.join("dharma.toml")).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            DharmaError::Config("missing dharma.toml".to_string())
        } else {
            DharmaError::from(err)
        }
    })?;
    let mut schema_hex = None;
    let mut contract_hex = None;
    let schema_key = format!("schema_v{ver}");
    let contract_key = format!("contract_v{ver}");
    for line in config.lines() {
        let line = line.trim();
        if let Some((k, v)) = line.split_once('=') {
            let key = k.trim();
            let value = v.trim().trim_matches('"');
            match key {
                k if k == schema_key => schema_hex = Some(value.to_string()),
                k if k == contract_key => contract_hex = Some(value.to_string()),
                "schema" if ver == DEFAULT_DATA_VERSION => schema_hex = Some(value.to_string()),
                "contract" if ver == DEFAULT_DATA_VERSION => contract_hex = Some(value.to_string()),
                _ => {}
            }
        }
    }
    let schema_hex = schema_hex.ok_or_else(|| DharmaError::Config("missing schema in dharma.toml".to_string()))?;
    let contract_hex = contract_hex.ok_or_else(|| DharmaError::Config("missing contract in dharma.toml".to_string()))?;
    Ok((SchemaId::from_hex(&schema_hex)?, ContractId::from_hex(&contract_hex)?))
}

pub(crate) fn load_schema_bytes(root: &PathBuf, id: &SchemaId) -> Result<Vec<u8>, DharmaError> {
    let path = root.join("objects").join(format!("{}.obj", id.to_hex()));
    fs::read(path).map_err(Into::into)
}

pub(crate) fn load_contract_bytes(root: &PathBuf, id: &ContractId) -> Result<Vec<u8>, DharmaError> {
    let path = root.join("objects").join(format!("{}.obj", id.to_hex()));
    fs::read(path).map_err(Into::into)
}

pub(crate) fn now_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn extract_ver(args: &[String]) -> (u64, Vec<String>) {
    let mut ver = DEFAULT_DATA_VERSION;
    let mut filtered = Vec::new();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if let Some(value) = arg.strip_prefix("--ver=") {
            if let Ok(parsed) = value.parse::<u64>() {
                ver = parsed;
            }
            continue;
        }
        if let Some(value) = arg.strip_prefix("--data_ver=") {
            if let Ok(parsed) = value.parse::<u64>() {
                ver = parsed;
            }
            continue;
        }
        if arg == "--ver" || arg == "--data_ver" {
            if let Some(next) = iter.next() {
                if let Ok(parsed) = next.parse::<u64>() {
                    ver = parsed;
                }
            }
            continue;
        }
        filtered.push(arg.clone());
    }
    (ver, filtered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn extract_ver_filters_flags() {
        let args = vec![
            "--ver=2".to_string(),
            "amount=10".to_string(),
            "--data_ver".to_string(),
            "3".to_string(),
            "note=ok".to_string(),
        ];
        let (ver, filtered) = extract_ver(&args);
        assert_eq!(ver, 3);
        assert_eq!(
            filtered,
            vec!["amount=10".to_string(), "note=ok".to_string()]
        );
    }

    #[test]
    fn load_contract_ids_for_ver_prefers_versioned_keys() {
        let temp = tempdir().unwrap();
        let schema_v1 = SchemaId::from_bytes([1u8; 32]);
        let contract_v1 = ContractId::from_bytes([2u8; 32]);
        let schema_v2 = SchemaId::from_bytes([3u8; 32]);
        let contract_v2 = ContractId::from_bytes([4u8; 32]);
        let config = format!(
            "schema = \"{}\"\ncontract = \"{}\"\nschema_v2 = \"{}\"\ncontract_v2 = \"{}\"\n",
            schema_v1.to_hex(),
            contract_v1.to_hex(),
            schema_v2.to_hex(),
            contract_v2.to_hex()
        );
        std::fs::write(temp.path().join("dharma.toml"), config).unwrap();

        let (schema, contract) = load_contract_ids_for_ver(&temp.path().to_path_buf(), 2).unwrap();
        assert_eq!(schema, schema_v2);
        assert_eq!(contract, contract_v2);

        let (schema, contract) =
            load_contract_ids_for_ver(&temp.path().to_path_buf(), DEFAULT_DATA_VERSION).unwrap();
        assert_eq!(schema, schema_v1);
        assert_eq!(contract, contract_v1);
    }
}
