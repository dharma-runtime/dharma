use crate::cmd::action::{
    apply_action_prepared, load_contract_bytes, load_contract_ids_for_ver, load_schema_bytes,
};
use crate::cmd::ops::expire_reserve_holds;
use crate::DharmaError;
use ciborium::value::Value;
use dharma::assertion::{is_overlay, signer_from_meta, AssertionPlaintext};
use dharma::config::Config;
use dharma::env::StdEnv;
use dharma::pdl::schema::CqrsSchema;
use dharma::reactor::ReactorVm;
use dharma::runtime::cqrs::{action_index, decode_args_buffer, encode_args_buffer};
use dharma::runtime::vm::{ARGS_BASE, CONTEXT_BASE, STATE_BASE};
use dharma::store::state::list_assertions;
use dharma::types::{EnvelopeId, SubjectId};
use dharma::vault::drain_archive_queue;
use dharma::{IdentityState, Store};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

type ContractKey = (u64, dharma::types::SchemaId, dharma::types::ContractId);

const RESERVE_EXPIRE_INTERVAL_SECS: i64 = 60;

pub fn spawn_daemon(data_dir: PathBuf, identity: IdentityState) {
    let _ = thread::Builder::new()
        .name("dharma-reactor".to_string())
        .spawn(move || {
            if let Err(err) = run_daemon(&data_dir, &identity) {
                eprintln!("Reactor daemon error: {err}");
            }
        });
}

fn run_daemon(data_dir: &Path, identity: &IdentityState) -> Result<(), DharmaError> {
    let store = Store::from_root(data_dir);
    let env = StdEnv::new(data_dir);
    let reactor_ids = load_reactor_ids(data_dir)?;
    if reactor_ids.is_empty() {
        return Ok(());
    }
    let mut vms = load_reactor_vms(&store, &reactor_ids)?;
    if vms.is_empty() {
        return Ok(());
    }
    let mut cron_specs: HashMap<u64, Vec<Option<CronSpec>>> = HashMap::new();
    for (ver, vm) in vms.iter() {
        let specs = vm
            .plan()
            .reactors
            .iter()
            .map(|reactor| parse_cron_trigger(reactor.trigger.as_deref()))
            .collect::<Vec<_>>();
        if specs.iter().any(|spec| spec.is_some()) {
            cron_specs.insert(*ver, specs);
        }
    }
    let mut last_cron_fire: HashMap<(SubjectId, u64, usize), i64> = HashMap::new();

    let mut last_seen: HashMap<SubjectId, u64> = HashMap::new();
    for subject in store.list_subjects()? {
        let last = last_seq_for_subject(&env, &subject)?;
        last_seen.insert(subject, last);
    }

    let mut prepared: HashMap<ContractKey, PreparedContract> = HashMap::new();
    let mut last_reserve_check: i64 = 0;

    loop {
        let now_ts = crate::cmd::action::now_timestamp() as i64;
        let minute_stamp = now_ts.div_euclid(60);
        for subject in store.list_subjects()? {
            let last = *last_seen.get(&subject).unwrap_or(&0);
            let records = list_assertions(&env, &subject)?;
            for record in records.iter().filter(|r| r.seq > last) {
                let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
                    Ok(assertion) => assertion,
                    Err(err) => {
                        eprintln!("Reactor: invalid assertion: {err}");
                        continue;
                    }
                };
                if is_overlay(&assertion.header) {
                    continue;
                }
                if is_reactor_generated(&assertion) {
                    continue;
                }
                let prepared_contract =
                    prepared_contract_for_assertion(data_dir, &assertion.header, &mut prepared)?;
                let event_action = normalize_action_name(&assertion.header.typ);
                let action_schema = match prepared_contract.schema.action(&event_action) {
                    Some(schema) => schema,
                    None => continue,
                };
                let action_idx = action_index(&prepared_contract.schema, &event_action)?;
                let args_buffer = match encode_args_buffer(
                    action_schema,
                    &prepared_contract.schema.structs,
                    action_idx,
                    &assertion.body,
                    false,
                ) {
                    Ok(buf) => buf,
                    Err(err) => {
                        eprintln!(
                            "Reactor: encode args error for action {} (ver={}, schema={}, contract={}): {err}",
                            event_action,
                            assertion.header.ver,
                            assertion.header.schema.to_hex(),
                            assertion.header.contract.to_hex()
                        );
                        continue;
                    }
                };
                let state = dharma::runtime::cqrs::load_state(
                    &env,
                    &assertion.header.sub,
                    &prepared_contract.schema,
                    &prepared_contract.contract_bytes,
                    assertion.header.ver,
                )?;
                let ctx_buf = build_context_buffer(&assertion);

                if let Some(vm) = vms.get_mut(&assertion.header.ver) {
                    vm.write_memory(STATE_BASE, &state.memory)?;
                    vm.write_memory(ARGS_BASE, &args_buffer)?;
                    vm.write_memory(CONTEXT_BASE, &ctx_buf)?;

                    let reactors = vm.plan().reactors.clone();
                    for (r_idx, reactor) in reactors.iter().enumerate() {
                        if !scope_matches(reactor.scope.as_deref(), &prepared_contract.schema) {
                            continue;
                        }
                        if !trigger_matches(reactor.trigger.as_deref(), &assertion.header.typ) {
                            continue;
                        }
                        let passes = match vm.check(r_idx) {
                            Ok(passes) => passes,
                            Err(err) => {
                                eprintln!("Reactor: validate error: {err}");
                                continue;
                            }
                        };
                        if !passes {
                            continue;
                        }
                        for (e_idx, emit) in reactor.emits.iter().enumerate() {
                            let emit_action = normalize_action_name(&emit.action);
                            let emit_schema = match prepared_contract.schema.action(&emit_action) {
                                Some(schema) => schema,
                                None => {
                                    eprintln!("Reactor: unknown emit action {}", emit.action);
                                    continue;
                                }
                            };
                            let layout = dharma::pdl::schema::layout_action(
                                emit_schema,
                                &prepared_contract.schema.structs,
                            );
                            let out_len = layout
                                .last()
                                .map(|entry| entry.offset + entry.size)
                                .unwrap_or(4)
                                .max(4);
                            if let Err(err) = vm.emit(r_idx, e_idx) {
                                eprintln!("Reactor: emit error: {err}");
                                continue;
                            }
                            let mut out = vec![0u8; out_len];
                            let out_base = vm.out_base();
                            if let Err(err) = vm.read_memory(out_base, &mut out) {
                                eprintln!("Reactor: read emit buffer error: {err}");
                                continue;
                            }
                            let args_value = match decode_args_buffer(
                                emit_schema,
                                &prepared_contract.schema.structs,
                                &out,
                            ) {
                                Ok(value) => value,
                                Err(err) => {
                                    eprintln!("Reactor: decode args error: {err}");
                                    continue;
                                }
                            };
                            let meta = reactor_meta(&reactor.name);
                            if let Err(err) = apply_action_prepared(
                                &data_dir.to_path_buf(),
                                identity,
                                assertion.header.sub,
                                &emit_action,
                                args_value,
                                assertion.header.ver,
                                prepared_contract.schema_id,
                                prepared_contract.contract_id,
                                &prepared_contract.schema,
                                &prepared_contract.contract_bytes,
                                Some(meta),
                            ) {
                                eprintln!("Reactor: apply error: {err}");
                            }
                        }
                    }
                }
            }
            if let Some(last_record) = records.last() {
                last_seen.insert(subject, last_record.seq);
            }
            if !cron_specs.is_empty() {
                for (ver, specs) in cron_specs.iter() {
                    let Some(vm) = vms.get_mut(ver) else {
                        continue;
                    };
                    let prepared_contract =
                        prepared_contract_for_ver(data_dir, *ver, &mut prepared)?;
                    let state = dharma::runtime::cqrs::load_state(
                        &env,
                        &subject,
                        &prepared_contract.schema,
                        &prepared_contract.contract_bytes,
                        *ver,
                    )?;
                    let ctx_buf = build_cron_context_buffer(identity, now_ts);
                    let args_buffer = vec![0u8; 4];
                    vm.write_memory(STATE_BASE, &state.memory)?;
                    vm.write_memory(ARGS_BASE, &args_buffer)?;
                    vm.write_memory(CONTEXT_BASE, &ctx_buf)?;

                    let reactors = vm.plan().reactors.clone();
                    for (r_idx, reactor) in reactors.iter().enumerate() {
                        let Some(spec) = specs.get(r_idx).and_then(|spec| spec.as_ref()) else {
                            continue;
                        };
                        if !scope_matches(reactor.scope.as_deref(), &prepared_contract.schema) {
                            continue;
                        }
                        if !cron_matches(spec, now_ts) {
                            continue;
                        }
                        let key = (subject, *ver, r_idx);
                        if last_cron_fire.get(&key).copied() == Some(minute_stamp) {
                            continue;
                        }
                        last_cron_fire.insert(key, minute_stamp);
                        let passes = match vm.check(r_idx) {
                            Ok(passes) => passes,
                            Err(err) => {
                                eprintln!("Reactor: validate error: {err}");
                                continue;
                            }
                        };
                        if !passes {
                            continue;
                        }
                        for (e_idx, emit) in reactor.emits.iter().enumerate() {
                            let emit_action = normalize_action_name(&emit.action);
                            let emit_schema = match prepared_contract.schema.action(&emit_action) {
                                Some(schema) => schema,
                                None => {
                                    eprintln!("Reactor: unknown emit action {}", emit.action);
                                    continue;
                                }
                            };
                            let layout = dharma::pdl::schema::layout_action(
                                emit_schema,
                                &prepared_contract.schema.structs,
                            );
                            let out_len = layout
                                .last()
                                .map(|entry| entry.offset + entry.size)
                                .unwrap_or(4)
                                .max(4);
                            if let Err(err) = vm.emit(r_idx, e_idx) {
                                eprintln!("Reactor: emit error: {err}");
                                continue;
                            }
                            let mut out = vec![0u8; out_len];
                            let out_base = vm.out_base();
                            if let Err(err) = vm.read_memory(out_base, &mut out) {
                                eprintln!("Reactor: read emit buffer error: {err}");
                                continue;
                            }
                            let args_value = match decode_args_buffer(
                                emit_schema,
                                &prepared_contract.schema.structs,
                                &out,
                            ) {
                                Ok(value) => value,
                                Err(err) => {
                                    eprintln!("Reactor: decode args error: {err}");
                                    continue;
                                }
                            };
                            let meta = reactor_meta(&reactor.name);
                            if let Err(err) = apply_action_prepared(
                                &data_dir.to_path_buf(),
                                identity,
                                subject,
                                &emit_action,
                                args_value,
                                *ver,
                                prepared_contract.schema_id,
                                prepared_contract.contract_id,
                                &prepared_contract.schema,
                                &prepared_contract.contract_bytes,
                                Some(meta),
                            ) {
                                eprintln!("Reactor: apply error: {err}");
                            }
                        }
                    }
                }
            }
        }
        if let Ok(root) = std::env::current_dir() {
            if let Ok(config) = Config::load(&root) {
                if let Err(err) = drain_archive_queue(&store, &config, identity) {
                    eprintln!("Vault archive error: {err}");
                }
            }
        }
        if RESERVE_EXPIRE_INTERVAL_SECS > 0 {
            let now_ts = crate::cmd::action::now_timestamp() as i64;
            if now_ts.saturating_sub(last_reserve_check) >= RESERVE_EXPIRE_INTERVAL_SECS {
                last_reserve_check = now_ts;
                if let Err(err) = expire_reserve_holds(data_dir, identity, false, now_ts) {
                    eprintln!("Reserve expire error: {err}");
                }
            }
        }
        thread::sleep(Duration::from_millis(500));
    }
}

fn trigger_matches(trigger: Option<&str>, typ: &str) -> bool {
    let Some(trigger) = trigger else {
        return false;
    };
    let trigger = trigger.trim();
    if trigger.is_empty() {
        return false;
    }
    if is_cron_trigger(trigger) {
        return false;
    }
    if let Some(inner) = trigger.strip_prefix("when(") {
        if let Some(end) = inner.find(')') {
            let inside = &inner[..end];
            let name = inside.split_whitespace().next().unwrap_or("").trim();
            if name.is_empty() {
                return false;
            }
            return typ == name || typ == format!("action.{name}");
        }
    }
    if trigger.starts_with("action.") {
        return typ == trigger;
    }
    if trigger.starts_with("action:") {
        let name = trigger.trim_start_matches("action:");
        return typ == format!("action.{name}") || typ == trigger;
    }
    typ == trigger || typ == format!("action.{trigger}")
}

fn scope_matches(scope: Option<&str>, schema: &CqrsSchema) -> bool {
    let Some(scope) = scope else {
        return true;
    };
    let scope = scope.trim();
    if scope.is_empty() {
        return true;
    }
    if scope == schema.namespace {
        return true;
    }
    if scope == schema.aggregate {
        return true;
    }
    let full = format!("{}.{}", schema.namespace, schema.aggregate);
    scope == full
}

fn normalize_action_name(value: &str) -> String {
    let trimmed = value.trim();
    if let Some(rest) = trimmed.strip_prefix("action.") {
        return rest.trim().to_string();
    }
    if let Some(rest) = trimmed.strip_prefix("action:") {
        return rest.trim().to_string();
    }
    trimmed.to_string()
}

fn is_reactor_generated(assertion: &AssertionPlaintext) -> bool {
    let Some(Value::Map(entries)) = &assertion.header.meta else {
        return false;
    };
    for (k, v) in entries {
        if let Value::Text(name) = k {
            if name == "reactor" {
                return matches!(v, Value::Bool(true));
            }
        }
    }
    false
}

fn reactor_meta(name: &str) -> Value {
    Value::Map(vec![
        (Value::Text("reactor".to_string()), Value::Bool(true)),
        (
            Value::Text("reactor_name".to_string()),
            Value::Text(name.to_string()),
        ),
    ])
}

fn last_seq_for_subject(
    env: &dyn dharma::env::Env,
    subject: &SubjectId,
) -> Result<u64, DharmaError> {
    let records = list_assertions(env, subject)?;
    Ok(records.last().map(|r| r.seq).unwrap_or(0))
}

fn load_reactor_ids(root: &Path) -> Result<HashMap<u64, EnvelopeId>, DharmaError> {
    let config = match std::fs::read_to_string(root.join("dharma.toml")) {
        Ok(config) => config,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(err) => return Err(DharmaError::from(err)),
    };
    let mut out = HashMap::new();
    let mut default = None;
    for line in config.lines() {
        let line = line.trim();
        if let Some((k, v)) = line.split_once('=') {
            let key = k.trim();
            let value = v.trim().trim_matches('"');
            if let Some(ver) = key.strip_prefix("reactor_v") {
                if let Ok(parsed) = ver.parse::<u64>() {
                    out.insert(parsed, EnvelopeId::from_hex(value)?);
                }
            } else if key == "reactor" {
                default = Some(EnvelopeId::from_hex(value)?);
            }
        }
    }
    if let Some(default) = default {
        out.entry(dharma::assertion::DEFAULT_DATA_VERSION)
            .or_insert(default);
    }
    Ok(out)
}

fn load_reactor_vms(
    store: &Store,
    ids: &HashMap<u64, EnvelopeId>,
) -> Result<HashMap<u64, ReactorVm>, DharmaError> {
    let mut out = HashMap::new();
    for (ver, id) in ids {
        if let Some(bytes) = store.get_object_any(id)? {
            let vm = ReactorVm::new_with_root(bytes, store.root().to_path_buf())?;
            out.insert(*ver, vm);
        }
    }
    Ok(out)
}

struct PreparedContract {
    schema_id: dharma::types::SchemaId,
    contract_id: dharma::types::ContractId,
    schema: CqrsSchema,
    contract_bytes: Vec<u8>,
}

fn prepared_contract_for_assertion(
    root: &Path,
    header: &dharma::assertion::AssertionHeader,
    cache: &mut HashMap<ContractKey, PreparedContract>,
) -> Result<PreparedContract, DharmaError> {
    prepared_contract_by_ids(root, header.ver, header.schema, header.contract, cache)
}

fn prepared_contract_for_ver(
    root: &Path,
    ver: u64,
    cache: &mut HashMap<ContractKey, PreparedContract>,
) -> Result<PreparedContract, DharmaError> {
    let (schema_id, contract_id) = load_contract_ids_for_ver(&root.to_path_buf(), ver)?;
    prepared_contract_by_ids(root, ver, schema_id, contract_id, cache)
}

fn prepared_contract_by_ids(
    root: &Path,
    ver: u64,
    schema_id: dharma::types::SchemaId,
    contract_id: dharma::types::ContractId,
    cache: &mut HashMap<ContractKey, PreparedContract>,
) -> Result<PreparedContract, DharmaError> {
    let key = (ver, schema_id, contract_id);
    if let Some(found) = cache.get(&key) {
        return Ok(PreparedContract {
            schema_id: found.schema_id,
            contract_id: found.contract_id,
            schema: found.schema.clone(),
            contract_bytes: found.contract_bytes.clone(),
        });
    }
    let schema_bytes = load_schema_bytes(&root.to_path_buf(), &schema_id)?;
    let contract_bytes = load_contract_bytes(&root.to_path_buf(), &contract_id)?;
    let schema = CqrsSchema::from_cbor(&schema_bytes)?;
    let prepared = PreparedContract {
        schema_id,
        contract_id,
        schema,
        contract_bytes,
    };
    cache.insert(
        key,
        PreparedContract {
            schema_id: prepared.schema_id,
            contract_id: prepared.contract_id,
            schema: prepared.schema.clone(),
            contract_bytes: prepared.contract_bytes.clone(),
        },
    );
    Ok(prepared)
}

fn build_context_buffer(assertion: &AssertionPlaintext) -> Vec<u8> {
    let mut buf = vec![0u8; 40];
    let signer = signer_from_meta(&assertion.header.meta).unwrap_or_else(|| assertion.header.sub);
    buf[..32].copy_from_slice(signer.as_bytes());
    let ts = assertion
        .header
        .ts
        .unwrap_or_else(|| crate::cmd::action::now_timestamp() as i64);
    buf[32..40].copy_from_slice(&ts.to_le_bytes());
    buf
}

fn build_cron_context_buffer(identity: &IdentityState, now_ts: i64) -> Vec<u8> {
    let mut buf = vec![0u8; 40];
    buf[..32].copy_from_slice(identity.subject_id.as_bytes());
    buf[32..40].copy_from_slice(&now_ts.to_le_bytes());
    buf
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CronSpec {
    minute: Option<u32>,
    hour: Option<u32>,
    day: Option<u32>,
    month: Option<u32>,
    weekday: Option<u32>,
}

fn parse_cron_trigger(trigger: Option<&str>) -> Option<CronSpec> {
    let trigger = trigger?;
    parse_cron_spec(trigger)
}

fn parse_cron_spec(trigger: &str) -> Option<CronSpec> {
    let trimmed = trigger.trim();
    let inner = if let Some(rest) = trimmed.strip_prefix("Cron(") {
        rest
    } else if let Some(rest) = trimmed.strip_prefix("cron(") {
        rest
    } else {
        return None;
    };
    let inner = inner.strip_suffix(')')?.trim();
    let inner = strip_wrapping_quotes(inner);
    let fields = inner.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 {
        return None;
    }
    let minute = parse_cron_field(fields[0], 0, 59)?;
    let hour = parse_cron_field(fields[1], 0, 23)?;
    let day = parse_cron_field(fields[2], 1, 31)?;
    let month = parse_cron_field(fields[3], 1, 12)?;
    let weekday = parse_cron_weekday(fields[4])?;
    Some(CronSpec {
        minute,
        hour,
        day,
        month,
        weekday,
    })
}

fn strip_wrapping_quotes(input: &str) -> &str {
    let trimmed = input.trim();
    if trimmed.len() >= 2 {
        let first = trimmed.as_bytes()[0];
        let last = trimmed.as_bytes()[trimmed.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return trimmed[1..trimmed.len() - 1].trim();
        }
    }
    trimmed
}

fn parse_cron_field(token: &str, min: u32, max: u32) -> Option<Option<u32>> {
    let token = token.trim();
    if token == "*" {
        return Some(None);
    }
    let value = token.parse::<u32>().ok()?;
    if value < min || value > max {
        return None;
    }
    Some(Some(value))
}

fn parse_cron_weekday(token: &str) -> Option<Option<u32>> {
    let token = token.trim();
    if token == "*" {
        return Some(None);
    }
    let mut value = token.parse::<u32>().ok()?;
    if value == 7 {
        value = 0;
    }
    if value > 6 {
        return None;
    }
    Some(Some(value))
}

fn cron_matches(spec: &CronSpec, ts: i64) -> bool {
    let comp = utc_components(ts);
    if let Some(min) = spec.minute {
        if comp.minute != min {
            return false;
        }
    }
    if let Some(hour) = spec.hour {
        if comp.hour != hour {
            return false;
        }
    }
    if let Some(day) = spec.day {
        if comp.day != day {
            return false;
        }
    }
    if let Some(month) = spec.month {
        if comp.month != month {
            return false;
        }
    }
    if let Some(weekday) = spec.weekday {
        if comp.weekday != weekday {
            return false;
        }
    }
    true
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct UtcComponents {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    weekday: u32,
}

fn utc_components(ts: i64) -> UtcComponents {
    let days = ts.div_euclid(86_400);
    let secs = ts.rem_euclid(86_400);
    let hour = (secs / 3_600) as u32;
    let minute = ((secs % 3_600) / 60) as u32;
    let (year, month, day) = civil_from_days(days);
    let weekday = ((days + 4).rem_euclid(7)) as u32;
    UtcComponents {
        year,
        month,
        day,
        hour,
        minute,
        weekday,
    }
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    if m <= 2 {
        y += 1;
    }
    (y, m as u32, d as u32)
}

fn is_cron_trigger(trigger: &str) -> bool {
    let trimmed = trigger.trim();
    trimmed.starts_with("Cron(") || trimmed.starts_with("cron(")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdl::parser;
    use dharma::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use dharma::crypto;
    use dharma::pdl::schema::layout_action;
    use dharma::pdl::schema::ConcurrencyMode;
    use dharma::reactor::{Expr, Op, ReactorPlan, ReactorSpec, ReactorVm};
    use dharma::runtime::cqrs::{
        action_index, decode_args_buffer, default_state_memory, encode_args_buffer,
    };
    use dharma::store::state::append_assertion;
    use dharma::types::{ContractId, SchemaId};
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::collections::BTreeMap;

    #[test]
    fn trigger_matching_accepts_action_prefix() {
        assert!(trigger_matches(Some("action.Approve"), "action.Approve"));
        assert!(trigger_matches(Some("Approve"), "action.Approve"));
        assert!(trigger_matches(Some("action:Approve"), "action.Approve"));
        assert!(!trigger_matches(Some("action.Reject"), "action.Approve"));
    }

    #[test]
    fn reactor_scope_matches_schema() {
        let schema = CqrsSchema {
            namespace: "std.task".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Task".to_string(),
            extends: None,
            implements: Vec::new(),
            structs: BTreeMap::new(),
            fields: BTreeMap::new(),
            actions: BTreeMap::new(),
            queries: BTreeMap::new(),
            projections: BTreeMap::new(),
            concurrency: ConcurrencyMode::Strict,
        };
        assert!(scope_matches(None, &schema));
        assert!(scope_matches(Some("std.task"), &schema));
        assert!(scope_matches(Some("Task"), &schema));
        assert!(scope_matches(Some("std.task.Task"), &schema));
        assert!(!scope_matches(Some("other"), &schema));
    }

    #[test]
    fn reactor_plan_roundtrip_still_valid() {
        let plan = ReactorPlan {
            version: 1,
            reactors: vec![ReactorSpec {
                name: "Auto".to_string(),
                trigger: Some("action.Send".to_string()),
                scope: None,
                validates: vec![Expr::Binary(
                    Op::Gt,
                    Box::new(Expr::Path(vec!["amount".to_string()])),
                    Box::new(Expr::Literal(Value::Integer(3.into()))),
                )],
                emits: Vec::new(),
            }],
        };
        let bytes = plan.to_cbor().unwrap();
        let decoded = dharma::reactor::ReactorPlan::from_cbor(&bytes).unwrap();
        assert_eq!(decoded.reactors.len(), 1);
    }

    #[test]
    fn reactor_vm_emits_args_from_event() {
        let temp = tempfile::tempdir().unwrap();
        let doc = r#"```dhl
aggregate Box
    state
        total: Int = 0

action Send(amount: Int)
    apply
        state.total = amount

action Approve(amount: Int)
    apply
        state.total = amount

reactor Auto
    trigger: action.Send
    validate
        amount > 10
    emit action.Approve(amount = amount)
```"#;
        let ast = parser::parse(doc).unwrap();
        let reactor_bytes = crate::pdl::codegen::wasm::compile_reactor(&ast).unwrap();
        let mut vm = ReactorVm::new_with_root(reactor_bytes, temp.path().to_path_buf()).unwrap();
        assert_eq!(vm.plan().reactors.len(), 1);

        let schema_bytes = crate::pdl::codegen::schema::compile_schema(&ast).unwrap();
        let schema = dharma::pdl::schema::CqrsSchema::from_cbor(&schema_bytes).unwrap();
        let action_schema = schema.action("Send").unwrap();
        let idx = action_index(&schema, "Send").unwrap();
        let args_value = Value::Map(vec![(
            Value::Text("amount".to_string()),
            Value::Integer(12.into()),
        )]);
        let args_buffer =
            encode_args_buffer(action_schema, &schema.structs, idx, &args_value, false).unwrap();
        let state = default_state_memory(&schema);
        let context = vec![0u8; 40];

        vm.write_memory(STATE_BASE, &state).unwrap();
        vm.write_memory(ARGS_BASE, &args_buffer).unwrap();
        vm.write_memory(CONTEXT_BASE, &context).unwrap();
        assert!(vm.check(0).unwrap());
        vm.emit(0, 0).unwrap();

        let emit_schema = schema.action("Approve").unwrap();
        let layout = layout_action(emit_schema, &schema.structs);
        let out_len = layout
            .last()
            .map(|entry| entry.offset + entry.size)
            .unwrap_or(4)
            .max(4);
        let mut out = vec![0u8; out_len];
        vm.read_memory(vm.out_base(), &mut out).unwrap();
        let decoded = decode_args_buffer(emit_schema, &schema.structs, &out).unwrap();
        if let Value::Map(entries) = decoded {
            let amount = entries
                .iter()
                .find(|(k, _)| matches!(k, Value::Text(name) if name == "amount"))
                .map(|(_, v)| v)
                .unwrap();
            assert_eq!(amount, &Value::Integer(12.into()));
        } else {
            panic!("expected map");
        }
    }

    #[test]
    fn reactor_vm_has_role_uses_identity_profile() {
        let temp = tempfile::tempdir().unwrap();
        let env = dharma::env::StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(11);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([7u8; 32]);

        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "core.genesis".to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let genesis =
            AssertionPlaintext::sign(genesis_header, Value::Map(vec![]), &root_sk).unwrap();
        let genesis_bytes = genesis.to_cbor().unwrap();
        let genesis_id = genesis.assertion_id().unwrap();
        let genesis_env = crypto::envelope_id(&genesis_bytes);
        append_assertion(
            &env,
            &subject,
            1,
            genesis_id,
            genesis_env,
            "core.genesis",
            &genesis_bytes,
        )
        .unwrap();

        let profile_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "identity.profile".to_string(),
            auth: root_id,
            seq: 2,
            prev: Some(genesis_id),
            refs: vec![genesis_id],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![(
            Value::Text("roles".to_string()),
            Value::Array(vec![Value::Text("finance.approver".to_string())]),
        )]);
        let profile = AssertionPlaintext::sign(profile_header, body, &root_sk).unwrap();
        let profile_bytes = profile.to_cbor().unwrap();
        let profile_id = profile.assertion_id().unwrap();
        let profile_env = crypto::envelope_id(&profile_bytes);
        append_assertion(
            &env,
            &subject,
            2,
            profile_id,
            profile_env,
            "identity.profile",
            &profile_bytes,
        )
        .unwrap();

        let doc = r#"```dhl
aggregate Box
    state
        total: Int = 0

action Send()
    apply
        state.total = 1

reactor Auto
    trigger: action.Send
    validate
        has_role(context.signer, context.signer, "finance.approver")
```"#;
        let ast = parser::parse(doc).unwrap();
        let reactor_bytes = crate::pdl::codegen::wasm::compile_reactor(&ast).unwrap();
        let mut vm = ReactorVm::new_with_root(reactor_bytes, temp.path().to_path_buf()).unwrap();

        let schema_bytes = crate::pdl::codegen::schema::compile_schema(&ast).unwrap();
        let schema = dharma::pdl::schema::CqrsSchema::from_cbor(&schema_bytes).unwrap();
        let action_schema = schema.action("Send").unwrap();
        let idx = action_index(&schema, "Send").unwrap();
        let args_value = Value::Map(vec![]);
        let args_buffer =
            encode_args_buffer(action_schema, &schema.structs, idx, &args_value, false).unwrap();
        let state = default_state_memory(&schema);
        let mut context = vec![0u8; 40];
        context[..32].copy_from_slice(subject.as_bytes());
        context[32..40].copy_from_slice(&0i64.to_le_bytes());

        vm.write_memory(STATE_BASE, &state).unwrap();
        vm.write_memory(ARGS_BASE, &args_buffer).unwrap();
        vm.write_memory(CONTEXT_BASE, &context).unwrap();
        assert!(vm.check(0).unwrap());

        let other_subject = SubjectId::from_bytes([8u8; 32]);
        context[..32].copy_from_slice(other_subject.as_bytes());
        vm.write_memory(CONTEXT_BASE, &context).unwrap();
        assert!(!vm.check(0).unwrap());
    }

    #[test]
    fn reactor_vm_has_role_accepts_delegate_scope() {
        let temp = tempfile::tempdir().unwrap();
        let env = dharma::env::StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(12);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let (device_sk, _device_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([10u8; 32]);

        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "core.genesis".to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let genesis =
            AssertionPlaintext::sign(genesis_header, Value::Map(vec![]), &root_sk).unwrap();
        let genesis_bytes = genesis.to_cbor().unwrap();
        let genesis_id = genesis.assertion_id().unwrap();
        let genesis_env = crypto::envelope_id(&genesis_bytes);
        append_assertion(
            &env,
            &subject,
            1,
            genesis_id,
            genesis_env,
            "core.genesis",
            &genesis_bytes,
        )
        .unwrap();

        let delegate_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "iam.delegate".to_string(),
            auth: root_id,
            seq: 2,
            prev: Some(genesis_id),
            refs: vec![genesis_id],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let delegate_body = Value::Map(vec![
            (
                Value::Text("delegate".to_string()),
                Value::Bytes(device_sk.verifying_key().to_bytes().to_vec()),
            ),
            (
                Value::Text("scope".to_string()),
                Value::Text("finance.viewer".to_string()),
            ),
            (Value::Text("expires".to_string()), Value::Integer(0.into())),
        ]);
        let delegate = AssertionPlaintext::sign(delegate_header, delegate_body, &root_sk).unwrap();
        let delegate_bytes = delegate.to_cbor().unwrap();
        let delegate_id = delegate.assertion_id().unwrap();
        let delegate_env = crypto::envelope_id(&delegate_bytes);
        append_assertion(
            &env,
            &subject,
            2,
            delegate_id,
            delegate_env,
            "iam.delegate",
            &delegate_bytes,
        )
        .unwrap();

        let doc = r#"```dhl
aggregate Box
    state
        total: Int = 0

action Send()
    apply
        state.total = 1

reactor Auto
    trigger: action.Send
    validate
        has_role(context.signer, "finance.viewer")
```"#;
        let ast = parser::parse(doc).unwrap();
        let reactor_bytes = crate::pdl::codegen::wasm::compile_reactor(&ast).unwrap();
        let mut vm = ReactorVm::new_with_root(reactor_bytes, temp.path().to_path_buf()).unwrap();

        let schema_bytes = crate::pdl::codegen::schema::compile_schema(&ast).unwrap();
        let schema = dharma::pdl::schema::CqrsSchema::from_cbor(&schema_bytes).unwrap();
        let action_schema = schema.action("Send").unwrap();
        let idx = action_index(&schema, "Send").unwrap();
        let args_value = Value::Map(vec![]);
        let args_buffer =
            encode_args_buffer(action_schema, &schema.structs, idx, &args_value, false).unwrap();
        let state = default_state_memory(&schema);
        let mut context = vec![0u8; 40];
        context[..32].copy_from_slice(subject.as_bytes());
        context[32..40].copy_from_slice(&0i64.to_le_bytes());

        vm.write_memory(STATE_BASE, &state).unwrap();
        vm.write_memory(ARGS_BASE, &args_buffer).unwrap();
        vm.write_memory(CONTEXT_BASE, &context).unwrap();
        assert!(vm.check(0).unwrap());
    }

    #[test]
    fn cron_parse_accepts_basic() {
        let spec = parse_cron_spec("Cron(\"0 0 * * *\")").unwrap();
        assert_eq!(spec.minute, Some(0));
        assert_eq!(spec.hour, Some(0));
        assert_eq!(spec.day, None);
        assert_eq!(spec.month, None);
        assert_eq!(spec.weekday, None);
    }

    #[test]
    fn cron_matches_epoch() {
        let spec = parse_cron_spec("Cron(\"0 0 1 1 4\")").unwrap();
        assert!(cron_matches(&spec, 0));
        let spec = parse_cron_spec("Cron(\"1 0 1 1 4\")").unwrap();
        assert!(!cron_matches(&spec, 0));
    }

    #[test]
    fn trigger_matches_ignores_cron() {
        assert!(!trigger_matches(
            Some("Cron(\"0 0 * * *\")"),
            "action.Approve"
        ));
    }

    #[test]
    fn reactor_cron_trigger_compiles() {
        let doc = r#"```dhl
aggregate Box
    state
        total: Int = 0

action Tick()
    apply
        state.total = state.total

reactor Auto
    trigger: Cron("0 0 * * *")
    validate
        context.clock.time >= 0
    emit action.Tick()
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = crate::pdl::codegen::wasm::compile_reactor(&ast).unwrap();
        assert!(!bytes.is_empty());
    }
}
