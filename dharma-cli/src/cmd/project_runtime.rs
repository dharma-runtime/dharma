use crate::cmd::action::apply_action_prepared;
use crate::DharmaError;
use blake3;
use ciborium::value::Value;
use dharma::dhlp::ProjectionPlan;
use dharma::pdl::schema::{CqrsSchema, TypeSpec};
use dharma::reactor::{eval_expr, EvalContext};
use dharma::store::state::list_assertions;
use dharma::types::{ContractId, EnvelopeId, SchemaId, SubjectId};
use dharma::value::{expect_map, map_get};
use dharma::Store;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Default)]
struct ProjectionStats {
    plans: usize,
    writes: usize,
    prunes: usize,
}

#[derive(Clone, Debug)]
struct ProjectionUnit {
    namespace: String,
    projection_name: String,
    plan: ProjectionPlan,
}

#[derive(Clone)]
struct PreparedContract {
    schema_id: SchemaId,
    contract_id: ContractId,
    schema: CqrsSchema,
    contract_bytes: Vec<u8>,
}

#[derive(Clone)]
struct EmitRow {
    subject: SubjectId,
    subject_hex: String,
    args: Value,
    key_hash: Option<Vec<u8>>,
}

struct ProjectionRuntime {
    root: PathBuf,
    data_dir: PathBuf,
    identity: dharma::IdentityState,
    plans: Vec<ProjectionUnit>,
    target_cache: HashMap<String, PreparedContract>,
}

pub fn rebuild(scope: &str) -> Result<(), DharmaError> {
    let mut runtime = ProjectionRuntime::new(scope)?;
    let stats = runtime.run_cycle()?;
    println!(
        "project rebuild complete scope={} plans={} writes={} prunes={}",
        scope, stats.plans, stats.writes, stats.prunes
    );
    Ok(())
}

pub fn watch(scope: &str, interval: Duration) -> Result<(), DharmaError> {
    watch_with_limit(scope, interval, None)
}

pub(crate) fn watch_with_limit(
    scope: &str,
    interval: Duration,
    max_cycles: Option<usize>,
) -> Result<(), DharmaError> {
    let mut runtime = ProjectionRuntime::new(scope)?;
    let mut seen = snapshot_subject_heads(&runtime.data_dir)?;
    let mut cycles = 0usize;

    loop {
        if let Some(max) = max_cycles {
            if cycles >= max {
                break;
            }
        }

        let should_run = if cycles == 0 {
            true
        } else {
            has_new_assertions(&runtime.data_dir, &mut seen)?
        };

        if should_run {
            let stats = runtime.run_cycle()?;
            println!(
                "project watch tick scope={} plans={} writes={} prunes={}",
                scope, stats.plans, stats.writes, stats.prunes
            );
            seen = snapshot_subject_heads(&runtime.data_dir)?;
        }

        cycles += 1;
        thread::sleep(interval);
    }

    Ok(())
}

impl ProjectionRuntime {
    fn new(scope: &str) -> Result<Self, DharmaError> {
        let root = std::env::current_dir()?;
        let data_dir = crate::ensure_data_dir()?;
        let env = dharma::env::StdEnv::new(&data_dir);
        crate::ensure_identity_present(&env)?;
        let identity = if let Ok(passphrase) = std::env::var("DHARMA_PASSPHRASE") {
            dharma::identity_store::load_identity(&env, &passphrase)?
        } else {
            crate::load_identity(&env)?
        };
        let _head = crate::mount_self(&env, &identity)?;

        let mut plans = load_projection_units(&root, scope)?;
        plans.sort_by(|a, b| {
            a.namespace
                .cmp(&b.namespace)
                .then_with(|| a.projection_name.cmp(&b.projection_name))
        });

        Ok(Self {
            root,
            data_dir,
            identity,
            plans,
            target_cache: HashMap::new(),
        })
    }

    fn run_cycle(&mut self) -> Result<ProjectionStats, DharmaError> {
        let mut stats = ProjectionStats {
            plans: self.plans.len(),
            writes: 0,
            prunes: 0,
        };
        let plans = self.plans.clone();
        for unit in &plans {
            self.run_projection(unit, &mut stats)?;
        }
        Ok(stats)
    }

    fn run_projection(
        &mut self,
        unit: &ProjectionUnit,
        stats: &mut ProjectionStats,
    ) -> Result<(), DharmaError> {
        let rows = dharma::dhlq::execute(&self.data_dir, &unit.plan.query, &Value::Array(vec![]))
            .map_err(|err| {
            DharmaError::Validation(format!(
                "projection {}.{} query failed: {}",
                unit.namespace, unit.projection_name, err
            ))
        })?;
        let target = self.resolve_target_contract(&unit.plan.emit.target)?;
        let emit_action = resolve_action_name(&target.schema, &unit.plan.emit.verb)?;
        let action_schema = target
            .schema
            .action(&emit_action)
            .ok_or_else(|| DharmaError::Schema("unknown action".to_string()))?;

        let mut emits = Vec::new();
        let mut active_keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        for row in rows {
            let mut emit = self.evaluate_emit_row(unit, &row)?;
            if let Some(prune_spec) = &unit.plan.prune {
                let key_hash = compute_key_hash(
                    prune_spec,
                    &emit.args,
                    &row,
                    &unit.namespace,
                    &unit.projection_name,
                )?;
                active_keys.insert(key_hash.clone());
                emit.key_hash = Some(key_hash);
            }
            emits.push(emit);
        }

        emits.sort_by(|a, b| a.subject_hex.cmp(&b.subject_hex));

        for emit in &emits {
            let coerced_args = coerce_args_for_action(
                &emit.args,
                action_schema,
                &unit.namespace,
                &unit.projection_name,
                &emit.subject_hex,
            )?;
            apply_action_prepared(
                &self.data_dir,
                &self.identity,
                emit.subject,
                &emit_action,
                coerced_args,
                dharma::assertion::DEFAULT_DATA_VERSION,
                target.schema_id,
                target.contract_id,
                &target.schema,
                &target.contract_bytes,
                None,
            )
            .map_err(|err| {
                DharmaError::Validation(format!(
                    "projection {}.{} apply {} failed for {} args={:?}: {}",
                    unit.namespace,
                    unit.projection_name,
                    emit_action,
                    emit.subject_hex,
                    emit.args,
                    err
                ))
            })?;
            stats.writes += 1;
        }

        if let Some(prune_spec) = &unit.plan.prune {
            stats.prunes += self.prune_stale(unit, prune_spec, &active_keys)?;
        }

        Ok(())
    }

    fn evaluate_emit_row(
        &self,
        unit: &ProjectionUnit,
        row: &Value,
    ) -> Result<EmitRow, DharmaError> {
        let header = Value::Map(vec![
            (
                Value::Text("namespace".to_string()),
                Value::Text(unit.namespace.clone()),
            ),
            (
                Value::Text("projection".to_string()),
                Value::Text(unit.projection_name.clone()),
            ),
        ]);
        let context = Value::Map(vec![(
            Value::Text("timestamp".to_string()),
            Value::Integer((now_timestamp() as i64).into()),
        )]);
        let eval_ctx = EvalContext {
            event: row.clone(),
            header,
            context,
        };

        let mut args_entries = Vec::new();
        for (name, expr) in &unit.plan.emit.args {
            let value = match eval_expr(expr, &eval_ctx) {
                Ok(value) => value,
                Err(DharmaError::Validation(msg)) if msg == "missing path" => Value::Null,
                Err(err) => {
                    return Err(DharmaError::Validation(format!(
                        "projection {}.{} emit {} failed: {}",
                        unit.namespace, unit.projection_name, name, err
                    )))
                }
            };
            args_entries.push((Value::Text(name.clone()), value));
        }

        let id_value = remove_map_entry(&mut args_entries, "id")
            .or_else(|| map_value_by_text(&args_entries, "subject").cloned())
            .or_else(|| {
                expect_map(row)
                    .ok()
                    .and_then(|entries| map_get(entries, "subject").cloned())
            });
        let subject =
            subject_from_projection_id(&unit.plan.emit.target, id_value.as_ref(), &args_entries)?;

        Ok(EmitRow {
            subject,
            subject_hex: subject.to_hex(),
            args: Value::Map(args_entries),
            key_hash: None,
        })
    }

    fn prune_stale(
        &mut self,
        unit: &ProjectionUnit,
        prune_spec: &dharma::dhlp::PruneSpec,
        active_keys: &BTreeSet<Vec<u8>>,
    ) -> Result<usize, DharmaError> {
        let target = self.resolve_target_contract(&unit.plan.emit.target)?;
        let action = resolve_prune_action(&target.schema).map_err(|err| {
            DharmaError::Validation(format!(
                "projection {}.{} prune action resolution failed: {}",
                unit.namespace, unit.projection_name, err
            ))
        })?;

        let mut select_parts = vec!["subject as subject".to_string()];
        for key in &prune_spec.keys {
            select_parts.push(format!("{key} as {key}"));
        }
        let query = format!(
            "{}\n| sel {}",
            unit.plan.emit.target,
            select_parts.join(", ")
        );
        let plan = crate::dhlq::parse_plan(&query, 1)?;
        let rows =
            dharma::dhlq::execute(&self.data_dir, &plan, &Value::Array(vec![])).map_err(|err| {
                DharmaError::Validation(format!(
                    "projection {}.{} prune query failed: {}",
                    unit.namespace, unit.projection_name, err
                ))
            })?;

        let mut stale = Vec::new();
        for row in rows {
            let row_map = expect_map(&row)?;
            let subject_val = map_get(row_map, "subject").ok_or_else(|| {
                DharmaError::Validation("projection row missing subject".to_string())
            })?;
            let subject = subject_from_value(Some(subject_val))?;

            let mut key_values = Vec::new();
            for key in &prune_spec.keys {
                let value = map_get(row_map, key).cloned().ok_or_else(|| {
                    DharmaError::Validation(format!(
                        "projection {}.{} prune key '{}' missing from prune row for subject {}",
                        unit.namespace,
                        unit.projection_name,
                        key,
                        subject.to_hex()
                    ))
                })?;
                key_values.push(value);
            }
            let hash = dharma::cbor::encode_canonical_value(&Value::Array(key_values))?;
            if !active_keys.contains(&hash) {
                stale.push((subject.to_hex(), subject));
            }
        }

        stale.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, subject) in stale.iter() {
            apply_action_prepared(
                &self.data_dir,
                &self.identity,
                *subject,
                &action,
                Value::Map(vec![]),
                dharma::assertion::DEFAULT_DATA_VERSION,
                target.schema_id,
                target.contract_id,
                &target.schema,
                &target.contract_bytes,
                None,
            )
            .map_err(|err| {
                DharmaError::Validation(format!(
                    "projection {}.{} prune {} failed for {}: {}",
                    unit.namespace,
                    unit.projection_name,
                    action,
                    subject.to_hex(),
                    err
                ))
            })?;
        }

        Ok(stale.len())
    }

    fn resolve_target_contract(
        &mut self,
        namespace: &str,
    ) -> Result<PreparedContract, DharmaError> {
        if let Some(cached) = self.target_cache.get(namespace) {
            return Ok(cached.clone());
        }

        let source = find_contract_source_for_namespace(&self.root, namespace)?;
        compile_contract_preserving_config(&self.root, &source)?;
        let stem = crate::output_stem_for_source(&source, None)?;
        let schema_bytes = fs::read(stem.with_extension("schema"))?;
        let contract_bytes = fs::read(stem.with_extension("contract"))?;

        let schema_id = SchemaId::from_bytes(dharma::crypto::sha256(&schema_bytes));
        let contract_id = ContractId::from_bytes(dharma::crypto::sha256(&contract_bytes));
        let schema = CqrsSchema::from_cbor(&schema_bytes)?;

        let store = Store::from_root(&self.data_dir);
        store.put_object(
            &EnvelopeId::from_bytes(*schema_id.as_bytes()),
            &schema_bytes,
        )?;
        store.put_object(
            &EnvelopeId::from_bytes(*contract_id.as_bytes()),
            &contract_bytes,
        )?;

        let prepared = PreparedContract {
            schema_id,
            contract_id,
            schema,
            contract_bytes,
        };
        self.target_cache
            .insert(namespace.to_string(), prepared.clone());
        Ok(prepared)
    }
}

fn resolve_action_name(schema: &CqrsSchema, verb: &str) -> Result<String, DharmaError> {
    if schema.actions.contains_key(verb) {
        return Ok(verb.to_string());
    }

    let mut candidates = schema
        .actions
        .keys()
        .filter(|name| name.eq_ignore_ascii_case(verb))
        .cloned()
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        let mut chars = verb.chars();
        if let Some(first) = chars.next() {
            let title = format!("{}{}", first.to_uppercase(), chars.as_str());
            if schema.actions.contains_key(&title) {
                return Ok(title);
            }
        }
    }

    candidates.sort();
    candidates.dedup();
    if candidates.len() == 1 {
        return Ok(candidates[0].clone());
    }

    Err(DharmaError::Validation(format!(
        "unknown projection emit action '{verb}' for namespace {}",
        schema.namespace
    )))
}

fn resolve_prune_action(schema: &CqrsSchema) -> Result<String, DharmaError> {
    for candidate in ["Clear", "Deactivate"] {
        if schema.actions.contains_key(candidate) {
            return Ok(candidate.to_string());
        }
    }
    let mut matches = schema
        .actions
        .keys()
        .filter(|name| {
            name.eq_ignore_ascii_case("clear") || name.eq_ignore_ascii_case("deactivate")
        })
        .cloned()
        .collect::<Vec<_>>();
    matches.sort();
    matches.dedup();
    if matches.len() == 1 {
        return Ok(matches[0].clone());
    }
    Err(DharmaError::Validation(format!(
        "projection target {} must expose Clear or Deactivate for prune",
        schema.namespace
    )))
}

fn compute_key_hash(
    prune_spec: &dharma::dhlp::PruneSpec,
    args_value: &Value,
    row: &Value,
    namespace: &str,
    projection_name: &str,
) -> Result<Vec<u8>, DharmaError> {
    let args_map = expect_map(args_value)?;
    let row_map = expect_map(row).ok();
    let mut values = Vec::new();
    for key in &prune_spec.keys {
        let value = map_get(args_map, key)
            .cloned()
            .or_else(|| row_map.and_then(|map| map_get(map, key).cloned()))
            .ok_or_else(|| {
                DharmaError::Validation(format!(
                    "projection {}.{} prune key '{}' missing from emit args and query row",
                    namespace, projection_name, key
                ))
            })?;
        values.push(value);
    }
    dharma::cbor::encode_canonical_value(&Value::Array(values))
}

fn remove_map_entry(entries: &mut Vec<(Value, Value)>, key: &str) -> Option<Value> {
    let idx = entries
        .iter()
        .position(|(k, _)| matches!(k, Value::Text(name) if name == key))?;
    Some(entries.remove(idx).1)
}

fn map_value_by_text<'a>(entries: &'a [(Value, Value)], key: &str) -> Option<&'a Value> {
    for (k, v) in entries {
        if let Value::Text(name) = k {
            if name == key {
                return Some(v);
            }
        }
    }
    None
}

fn coerce_args_for_action(
    args: &Value,
    action_schema: &dharma::pdl::schema::ActionSchema,
    namespace: &str,
    projection_name: &str,
    subject_hex: &str,
) -> Result<Value, DharmaError> {
    let map = expect_map(args)?;
    let mut entries = Vec::new();

    for (name, typ) in &action_schema.args {
        let value = map_get(map, name).cloned();
        let coerced = match value {
            Some(Value::Null) if !matches!(typ, TypeSpec::Optional(_)) => {
                return Err(DharmaError::Validation(format!(
                    "projection {}.{} emit subject={} arg '{}' is null but required",
                    namespace, projection_name, subject_hex, name
                )))
            }
            Some(value) => value,
            None if matches!(typ, TypeSpec::Optional(_)) => Value::Null,
            None => {
                return Err(DharmaError::Validation(format!(
                    "projection {}.{} emit subject={} missing required arg '{}'",
                    namespace, projection_name, subject_hex, name
                )))
            }
        };
        entries.push((Value::Text(name.clone()), coerced));
    }

    Ok(Value::Map(entries))
}

fn subject_from_projection_id(
    target_namespace: &str,
    id_value: Option<&Value>,
    fallback_args: &[(Value, Value)],
) -> Result<SubjectId, DharmaError> {
    let id_material = id_value
        .cloned()
        .unwrap_or_else(|| Value::Map(fallback_args.to_vec()));
    let seed = Value::Array(vec![Value::Text(target_namespace.to_string()), id_material]);
    let bytes = dharma::cbor::encode_canonical_value(&seed)?;
    Ok(SubjectId::from_bytes(*blake3::hash(&bytes).as_bytes()))
}

fn subject_from_value(id_value: Option<&Value>) -> Result<SubjectId, DharmaError> {
    let id_value = id_value
        .ok_or_else(|| DharmaError::Validation("projection row missing subject".to_string()))?;
    match id_value {
        Value::Text(hex) => SubjectId::from_hex(hex),
        Value::Bytes(bytes) => SubjectId::from_slice(bytes),
        _ => Err(DharmaError::Validation(
            "projection row subject must be text or bytes".to_string(),
        )),
    }
}

fn load_projection_units(root: &Path, scope: &str) -> Result<Vec<ProjectionUnit>, DharmaError> {
    let mut units = Vec::new();
    let contracts_dir = root.join("contracts");
    let mut files = Vec::new();
    collect_contract_files(&contracts_dir, &mut files)?;
    files.sort();

    for source in files {
        let namespace = extract_namespace(&source)?;
        if !scope_matches_namespace(scope, &namespace) {
            continue;
        }

        compile_contract_preserving_config(root, &source)?;
        let stem = crate::output_stem_for_source(&source, None)?;
        let schema_bytes = fs::read(stem.with_extension("schema"))?;
        let schema = CqrsSchema::from_cbor(&schema_bytes)?;

        for (projection_name, projection_schema) in &schema.projections {
            let plan = ProjectionPlan::from_cbor(&projection_schema.plan)?;
            units.push(ProjectionUnit {
                namespace: schema.namespace.clone(),
                projection_name: projection_name.clone(),
                plan,
            });
        }
    }

    Ok(units)
}

fn scope_matches_namespace(scope: &str, namespace: &str) -> bool {
    namespace == scope || namespace.starts_with(&format!("{scope}."))
}

fn compile_contract_preserving_config(root: &Path, source: &Path) -> Result<(), DharmaError> {
    let config_path = root.join("dharma.toml");
    let original = fs::read(&config_path)?;
    let compile_result = crate::compile_dhl(
        source
            .to_str()
            .ok_or_else(|| DharmaError::Validation("non-utf8 contract path".to_string()))?,
        None,
    );
    fs::write(&config_path, original)?;
    compile_result
}

fn collect_contract_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), DharmaError> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_contract_files(&path, out)?;
        } else if path.extension().and_then(|v| v.to_str()) == Some("dhl") {
            out.push(path);
        }
    }
    Ok(())
}

fn extract_namespace(path: &Path) -> Result<String, DharmaError> {
    let content = fs::read_to_string(path)?;
    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(ns) = trimmed.strip_prefix("namespace:") {
            let namespace = ns.trim();
            if !namespace.is_empty() {
                return Ok(namespace.to_string());
            }
        }
    }
    Err(DharmaError::Validation(format!(
        "missing namespace in {}",
        path.display()
    )))
}

fn find_contract_source_for_namespace(
    root: &Path,
    namespace: &str,
) -> Result<PathBuf, DharmaError> {
    if let Some(rest) = namespace.strip_prefix("std.") {
        let guessed = root
            .join("contracts")
            .join("std")
            .join(format!("{}.dhl", rest.replace('.', "_")));
        if guessed.is_file() {
            return Ok(guessed);
        }
    }

    let contracts_dir = root.join("contracts");
    let mut files = Vec::new();
    collect_contract_files(&contracts_dir, &mut files)?;
    files.sort();

    for file in files {
        if extract_namespace(&file)? == namespace {
            return Ok(file);
        }
    }

    Err(DharmaError::Validation(format!(
        "unable to locate contract source for namespace {namespace}"
    )))
}

fn now_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn snapshot_subject_heads(data_dir: &Path) -> Result<HashMap<SubjectId, u64>, DharmaError> {
    let env = dharma::env::StdEnv::new(data_dir);
    let store = Store::from_root(data_dir);
    let mut out = HashMap::new();
    for subject in store.list_subjects()? {
        let records = list_assertions(&env, &subject)?;
        let seq = records.last().map(|r| r.seq).unwrap_or(0);
        out.insert(subject, seq);
    }
    Ok(out)
}

fn has_new_assertions(
    data_dir: &Path,
    seen: &mut HashMap<SubjectId, u64>,
) -> Result<bool, DharmaError> {
    let env = dharma::env::StdEnv::new(data_dir);
    let store = Store::from_root(data_dir);
    let mut changed = false;

    for subject in store.list_subjects()? {
        let records = list_assertions(&env, &subject)?;
        let seq = records.last().map(|r| r.seq).unwrap_or(0);
        let prev = seen.get(&subject).copied().unwrap_or(0);
        if seq > prev {
            changed = true;
            seen.insert(subject, seq);
        }
    }

    Ok(changed)
}
