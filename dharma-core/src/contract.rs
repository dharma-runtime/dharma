use crate::cbor;
use crate::env::Env;
use crate::error::DharmaError;
use crate::identity;
use crate::metrics;
use crate::pdl::schema::DEFAULT_TEXT_LEN;
use crate::runtime::remote;
use crate::runtime::vm::VmLimits;
use crate::types::{ContractId, SubjectId};
use crate::value::{
    expect_array,
    expect_bool,
    expect_bytes,
    expect_int,
    expect_map,
    expect_text,
    expect_uint,
    map_get,
};
use ciborium::value::Value;
use std::collections::{BTreeMap, BTreeSet};
use wasmi::{
    Caller,
    Config,
    Engine,
    Extern,
    FuelConsumptionMode,
    Instance,
    Linker,
    Memory,
    Module,
    Store,
    StoreLimits,
    StoreLimitsBuilder,
    TypedFunc,
};
use wasmi::core::{Trap, TrapCode};

const MAX_ROLE_LEN: usize = 128;
const MAX_PATH_LEN: usize = 256;
const ELEM_KIND_TEXT: i32 = 1;
const ELEM_KIND_IDENTITY: i32 = 2;
const ELEM_KIND_SUBJECT_REF: i32 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContractStatus {
    Accept,
    Reject,
    Pending,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContractResult {
    pub ok: bool,
    pub status: ContractStatus,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SummaryDecision {
    Allow,
    Deny,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PermissionRule {
    pub roles: BTreeSet<String>,
    pub exhaustive: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct PublicPermissions {
    pub actions: BTreeSet<String>,
    pub queries: BTreeSet<String>,
    pub scopes: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PermissionSummary {
    pub v: u64,
    pub contract: ContractId,
    pub ver: u64,
    pub actions: BTreeMap<String, PermissionRule>,
    pub queries: BTreeMap<String, PermissionRule>,
    pub role_scopes: BTreeMap<String, BTreeSet<String>>,
    pub public: PublicPermissions,
}

impl PermissionSummary {
    pub fn empty(contract: ContractId, ver: u64) -> Self {
        Self {
            v: 1,
            contract,
            ver,
            actions: BTreeMap::new(),
            queries: BTreeMap::new(),
            role_scopes: BTreeMap::new(),
            public: PublicPermissions::default(),
        }
    }

    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.to_value())
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        Self::from_value(&value)
    }

    pub fn allows_action(&self, roles: &[String], action: &str) -> SummaryDecision {
        if self.public.actions.contains(action) || self.public.actions.contains("*") {
            return SummaryDecision::Allow;
        }
        if let Some(rule) = self.actions.get(action) {
            if role_matches(&rule.roles, roles) {
                return SummaryDecision::Allow;
            }
            return if rule.exhaustive {
                SummaryDecision::Deny
            } else {
                SummaryDecision::Unknown
            };
        }
        SummaryDecision::Unknown
    }

    pub fn allows_query(&self, roles: &[String], query: &str) -> SummaryDecision {
        if self.public.queries.contains(query) || self.public.queries.contains("*") {
            return SummaryDecision::Allow;
        }
        if let Some(rule) = self.queries.get(query) {
            if role_matches(&rule.roles, roles) {
                return SummaryDecision::Allow;
            }
            return if rule.exhaustive {
                SummaryDecision::Deny
            } else {
                SummaryDecision::Unknown
            };
        }
        SummaryDecision::Unknown
    }

    pub fn to_value(&self) -> Value {
        let actions = self
            .actions
            .iter()
            .map(|(name, rule)| {
                (
                    Value::Text(name.clone()),
                    Value::Map(vec![
                        (
                            Value::Text("roles".to_string()),
                            Value::Array(rule.roles.iter().cloned().map(Value::Text).collect()),
                        ),
                        (
                            Value::Text("exhaustive".to_string()),
                            Value::Bool(rule.exhaustive),
                        ),
                    ]),
                )
            })
            .collect();
        let queries = self
            .queries
            .iter()
            .map(|(name, rule)| {
                (
                    Value::Text(name.clone()),
                    Value::Map(vec![
                        (
                            Value::Text("roles".to_string()),
                            Value::Array(rule.roles.iter().cloned().map(Value::Text).collect()),
                        ),
                        (
                            Value::Text("exhaustive".to_string()),
                            Value::Bool(rule.exhaustive),
                        ),
                    ]),
                )
            })
            .collect();
        let role_scopes = self
            .role_scopes
            .iter()
            .map(|(role, scopes)| {
                (
                    Value::Text(role.clone()),
                    Value::Array(scopes.iter().cloned().map(Value::Text).collect()),
                )
            })
            .collect();
        let public = Value::Map(vec![
            (
                Value::Text("actions".to_string()),
                Value::Array(self.public.actions.iter().cloned().map(Value::Text).collect()),
            ),
            (
                Value::Text("queries".to_string()),
                Value::Array(self.public.queries.iter().cloned().map(Value::Text).collect()),
            ),
            (
                Value::Text("scopes".to_string()),
                Value::Array(self.public.scopes.iter().cloned().map(Value::Text).collect()),
            ),
        ]);
        Value::Map(vec![
            (Value::Text("v".to_string()), Value::Integer(self.v.into())),
            (
                Value::Text("contract".to_string()),
                Value::Bytes(self.contract.as_bytes().to_vec()),
            ),
            (Value::Text("ver".to_string()), Value::Integer(self.ver.into())),
            (Value::Text("actions".to_string()), Value::Map(actions)),
            (Value::Text("queries".to_string()), Value::Map(queries)),
            (Value::Text("role_scopes".to_string()), Value::Map(role_scopes)),
            (Value::Text("public".to_string()), public),
        ])
    }

    pub fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let v = expect_uint(map_get(map, "v").ok_or_else(|| {
            DharmaError::Validation("missing summary version".to_string())
        })?)?;
        let contract_bytes = crate::value::expect_bytes(map_get(map, "contract").ok_or_else(|| {
            DharmaError::Validation("missing summary contract".to_string())
        })?)?;
        let contract = ContractId::from_slice(&contract_bytes)?;
        let ver = expect_uint(map_get(map, "ver").ok_or_else(|| {
            DharmaError::Validation("missing summary ver".to_string())
        })?)?;
        let actions_val = map_get(map, "actions")
            .ok_or_else(|| DharmaError::Validation("missing summary actions".to_string()))?;
        let queries_val = map_get(map, "queries")
            .ok_or_else(|| DharmaError::Validation("missing summary queries".to_string()))?;
        let scopes_val = map_get(map, "role_scopes")
            .ok_or_else(|| DharmaError::Validation("missing summary role_scopes".to_string()))?;
        let public_val = map_get(map, "public")
            .ok_or_else(|| DharmaError::Validation("missing summary public".to_string()))?;

        let actions = parse_rule_map(actions_val)?;
        let queries = parse_rule_map(queries_val)?;
        let role_scopes = parse_scope_map(scopes_val)?;
        let public = parse_public(public_val)?;

        Ok(Self {
            v,
            contract,
            ver,
            actions,
            queries,
            role_scopes,
            public,
        })
    }
}

fn parse_rule_map(value: &Value) -> Result<BTreeMap<String, PermissionRule>, DharmaError> {
    let map = expect_map(value)?;
    let mut out = BTreeMap::new();
    for (key, val) in map {
        let name = expect_text(key)?;
        let rule_map = expect_map(val)?;
        let roles_val = map_get(rule_map, "roles")
            .ok_or_else(|| DharmaError::Validation("missing roles".to_string()))?;
        let roles = parse_text_set(roles_val)?;
        let exhaustive_val = map_get(rule_map, "exhaustive")
            .ok_or_else(|| DharmaError::Validation("missing exhaustive".to_string()))?;
        let exhaustive = expect_bool(exhaustive_val)?;
        out.insert(
            name,
            PermissionRule {
                roles,
                exhaustive,
            },
        );
    }
    Ok(out)
}

fn parse_scope_map(value: &Value) -> Result<BTreeMap<String, BTreeSet<String>>, DharmaError> {
    let map = expect_map(value)?;
    let mut out = BTreeMap::new();
    for (key, val) in map {
        let name = expect_text(key)?;
        let scopes = parse_text_set(val)?;
        out.insert(name, scopes);
    }
    Ok(out)
}

fn parse_public(value: &Value) -> Result<PublicPermissions, DharmaError> {
    let map = expect_map(value)?;
    let actions = match map_get(map, "actions") {
        Some(value) => parse_text_set(value)?,
        None => BTreeSet::new(),
    };
    let queries = match map_get(map, "queries") {
        Some(value) => parse_text_set(value)?,
        None => BTreeSet::new(),
    };
    let scopes = match map_get(map, "scopes") {
        Some(value) => parse_text_set(value)?,
        None => BTreeSet::new(),
    };
    Ok(PublicPermissions {
        actions,
        queries,
        scopes,
    })
}

fn parse_text_set(value: &Value) -> Result<BTreeSet<String>, DharmaError> {
    let arr = expect_array(value)?;
    let mut out = BTreeSet::new();
    for entry in arr {
        out.insert(expect_text(entry)?);
    }
    Ok(out)
}

fn role_matches(allowed: &BTreeSet<String>, roles: &[String]) -> bool {
    if allowed.contains("*") {
        return true;
    }
    roles.iter().any(|role| allowed.contains(role))
}

pub struct ContractEngine {
    wasm: Vec<u8>,
    limits: VmLimits,
}

impl ContractEngine {
    pub fn new(wasm: Vec<u8>) -> Self {
        Self {
            wasm,
            limits: VmLimits::default(),
        }
    }

    pub fn new_with_limits(wasm: Vec<u8>, limits: VmLimits) -> Self {
        let mut limits = limits;
        let defaults = VmLimits::default();
        if limits.fuel == 0 {
            limits.fuel = defaults.fuel;
        }
        if limits.memory_bytes == 0 {
            limits.memory_bytes = defaults.memory_bytes;
        }
        Self { wasm, limits }
    }

    pub fn validate(&self, assertion: &[u8], context: &[u8]) -> Result<ContractResult, DharmaError> {
        let output = self.call_validate(None, assertion, context)?;
        parse_contract_result(&output)
    }

    pub fn validate_with_env(
        &self,
        env: &dyn Env,
        assertion: &[u8],
        context: &[u8],
    ) -> Result<ContractResult, DharmaError> {
        let output = self.call_validate(Some(env), assertion, context)?;
        parse_contract_result(&output)
    }

    pub fn reduce(&self, accepted: &[u8]) -> Result<Vec<u8>, DharmaError> {
        self.call_reduce(None, accepted)
    }

    pub fn reduce_with_env(
        &self,
        env: &dyn Env,
        accepted: &[u8],
    ) -> Result<Vec<u8>, DharmaError> {
        self.call_reduce(Some(env), accepted)
    }

    pub fn accept_all() -> Self {
        let wasm = include_bytes!(concat!(env!("OUT_DIR"), "/accept.wasm")).to_vec();
        ContractEngine::new(wasm)
    }

    fn instantiate<'a>(
        &self,
        env: Option<&'a dyn Env>,
    ) -> Result<(Store<ContractHost<'a>>, Instance), DharmaError> {
        let mut config = Config::default();
        config.consume_fuel(true);
        config.fuel_consumption_mode(FuelConsumptionMode::Eager);
        let engine = Engine::new(&config);
        let cursor = std::io::Cursor::new(&self.wasm);
        let module = Module::new(&engine, cursor)
            .map_err(map_wasmi_error)?;
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.limits.memory_bytes)
            .build();
        let mut store = Store::new(&engine, ContractHost::new(env, limits));
        store.limiter(|host| &mut host.limits);
        store
            .add_fuel(self.limits.fuel)
            .map_err(map_fuel_error)?;
        let mut linker = Linker::new(&engine);
        linker
            .func_wrap("env", "has_role", has_role_host)
            .map_err(|e| map_wasmi_error(e.into()))?;
        linker
            .func_wrap("env", "read_int", read_int_host)
            .map_err(|e| map_wasmi_error(e.into()))?;
        linker
            .func_wrap("env", "read_bool", read_bool_host)
            .map_err(|e| map_wasmi_error(e.into()))?;
        linker
            .func_wrap("env", "read_text", read_text_host)
            .map_err(|e| map_wasmi_error(e.into()))?;
        linker
            .func_wrap("env", "read_identity", read_identity_host)
            .map_err(|e| map_wasmi_error(e.into()))?;
        linker
            .func_wrap("env", "read_subject_ref", read_subject_ref_host)
            .map_err(|e| map_wasmi_error(e.into()))?;
        linker
            .func_wrap("env", "subject_id", subject_id_host)
            .map_err(|e| map_wasmi_error(e.into()))?;
        linker
            .func_wrap("env", "remote_intersects", remote_intersects_host)
            .map_err(|e| map_wasmi_error(e.into()))?;
        linker
            .func_wrap("env", "normalize_text_list", normalize_text_list_host)
            .map_err(|e| map_wasmi_error(e.into()))?;
        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(map_wasmi_error)?
            .start(&mut store)
            .map_err(map_wasmi_error)?;
        Ok((store, instance))
    }

    fn call_validate(
        &self,
        env: Option<&dyn Env>,
        assertion: &[u8],
        context: &[u8],
    ) -> Result<Vec<u8>, DharmaError> {
        metrics::wasm_executions_inc();
        let (mut store, instance) = self.instantiate(env)?;
        let memory = get_memory(&instance, &store)?;
        let alloc = get_func::<_, i32, i32>(&instance, &store, "alloc")?;
        let result_len = get_func::<_, (), i32>(&instance, &store, "result_len")?;
        let validate = get_func::<_, (i32, i32, i32, i32), i32>(&instance, &store, "validate")?;

        let a_ptr = alloc
            .call(&mut store, assertion.len() as i32)
            .map_err(|e| map_wasmi_error(e.into()))?;
        write_memory(&memory, &mut store, a_ptr, assertion)?;
        let c_ptr = alloc
            .call(&mut store, context.len() as i32)
            .map_err(|e| map_wasmi_error(e.into()))?;
        write_memory(&memory, &mut store, c_ptr, context)?;

        let out_ptr = validate
            .call(&mut store, (a_ptr, assertion.len() as i32, c_ptr, context.len() as i32))
            .map_err(|e| map_wasmi_error(e.into()))?;
        let out_len = result_len
            .call(&mut store, ())
            .map_err(|e| map_wasmi_error(e.into()))?;
        read_memory(&memory, &mut store, out_ptr, out_len)
    }

    fn call_reduce(&self, env: Option<&dyn Env>, accepted: &[u8]) -> Result<Vec<u8>, DharmaError> {
        metrics::wasm_executions_inc();
        let (mut store, instance) = self.instantiate(env)?;
        let memory = get_memory(&instance, &store)?;
        let alloc = get_func::<_, i32, i32>(&instance, &store, "alloc")?;
        let result_len = get_func::<_, (), i32>(&instance, &store, "result_len")?;
        let reduce = get_func::<_, (i32, i32), i32>(&instance, &store, "reduce")?;

        let a_ptr = alloc
            .call(&mut store, accepted.len() as i32)
            .map_err(|e| map_wasmi_error(e.into()))?;
        write_memory(&memory, &mut store, a_ptr, accepted)?;

        let out_ptr = reduce
            .call(&mut store, (a_ptr, accepted.len() as i32))
            .map_err(|e| map_wasmi_error(e.into()))?;
        let out_len = result_len
            .call(&mut store, ())
            .map_err(|e| map_wasmi_error(e.into()))?;
        read_memory(&memory, &mut store, out_ptr, out_len)
    }
}

struct ContractHost<'a> {
    env: Option<&'a dyn Env>,
    limits: StoreLimits,
}

impl<'a> ContractHost<'a> {
    fn new(env: Option<&'a dyn Env>, limits: StoreLimits) -> Self {
        Self { env, limits }
    }
}

fn has_role_host(
    caller: Caller<'_, ContractHost<'_>>,
    subject_ptr: i32,
    _identity_ptr: i32,
    role_ptr: i32,
) -> Result<i32, Trap> {
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("missing memory")),
    };
    let subject_ptr = subject_ptr as usize;
    let role_ptr = role_ptr as usize;
    let mut id_bytes = [0u8; 32];
    memory
        .read(&caller, subject_ptr, &mut id_bytes)
        .map_err(|_| Trap::new("invalid identity pointer"))?;
    let subject = SubjectId::from_bytes(id_bytes);
    let mut len_bytes = [0u8; 4];
    memory
        .read(&caller, role_ptr, &mut len_bytes)
        .map_err(|_| Trap::new("invalid role pointer"))?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > MAX_ROLE_LEN {
        return Ok(0);
    }
    let mut role_bytes = vec![0u8; len];
    if len > 0 {
        memory
            .read(&caller, role_ptr + 4, &mut role_bytes)
            .map_err(|_| Trap::new("invalid role data"))?;
    }
    let role = match std::str::from_utf8(&role_bytes) {
        Ok(role) => role,
        Err(_) => return Ok(0),
    };
    let Some(env) = caller.data().env else {
        return Ok(0);
    };
    let allowed = identity::has_role(env, &subject, role).unwrap_or(false);
    Ok(if allowed { 1 } else { 0 })
}

fn read_subject_ref_arg(
    memory: &Memory,
    caller: &Caller<'_, ContractHost<'_>>,
    ptr: i32,
) -> Result<(SubjectId, u64), Trap> {
    let ptr = ptr as usize;
    let mut id_bytes = [0u8; 32];
    memory
        .read(caller, ptr, &mut id_bytes)
        .map_err(|_| Trap::new("invalid subject_ref id"))?;
    let mut seq_bytes = [0u8; 8];
    memory
        .read(caller, ptr + 32, &mut seq_bytes)
        .map_err(|_| Trap::new("invalid subject_ref seq"))?;
    let seq = u64::from_le_bytes(seq_bytes);
    if seq == 0 {
        return Err(Trap::new("invalid subject_ref seq"));
    }
    Ok((SubjectId::from_bytes(id_bytes), seq))
}

fn read_text_arg(
    memory: &Memory,
    caller: &Caller<'_, ContractHost<'_>>,
    ptr: i32,
) -> Result<String, Trap> {
    let ptr = ptr as usize;
    let mut len_bytes = [0u8; 4];
    memory
        .read(caller, ptr, &mut len_bytes)
        .map_err(|_| Trap::new("invalid text pointer"))?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > MAX_PATH_LEN {
        return Err(Trap::new("text too long"));
    }
    let mut buf = vec![0u8; len];
    if len > 0 {
        memory
            .read(caller, ptr + 4, &mut buf)
            .map_err(|_| Trap::new("invalid text data"))?;
    }
    String::from_utf8(buf).map_err(|_| Trap::new("invalid utf8"))
}

fn read_int_host(
    caller: Caller<'_, ContractHost<'_>>,
    subject_ptr: i32,
    path_ptr: i32,
) -> Result<i64, Trap> {
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("missing memory")),
    };
    let Some(env) = caller.data().env else {
        return Err(Trap::new("missing env"));
    };
    let (subject, seq) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    let path = read_text_arg(&memory, &caller, path_ptr)?;
    let (typ, value) = remote::read_remote_field(env, &subject, seq, &path)
        .map_err(|e| Trap::new(e.to_string()))?;
    if !matches!(
        typ,
        crate::pdl::schema::TypeSpec::Int
            | crate::pdl::schema::TypeSpec::Decimal(_)
            | crate::pdl::schema::TypeSpec::Duration
            | crate::pdl::schema::TypeSpec::Timestamp
    ) {
        return Err(Trap::new("read_int type mismatch"));
    }
    let val = expect_int(&value).map_err(|e| Trap::new(e.to_string()))?;
    Ok(val)
}

fn read_bool_host(
    caller: Caller<'_, ContractHost<'_>>,
    subject_ptr: i32,
    path_ptr: i32,
) -> Result<i32, Trap> {
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("missing memory")),
    };
    let Some(env) = caller.data().env else {
        return Err(Trap::new("missing env"));
    };
    let (subject, seq) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    let path = read_text_arg(&memory, &caller, path_ptr)?;
    let (typ, value) = remote::read_remote_field(env, &subject, seq, &path)
        .map_err(|e| Trap::new(e.to_string()))?;
    if !matches!(typ, crate::pdl::schema::TypeSpec::Bool) {
        return Err(Trap::new("read_bool type mismatch"));
    }
    let val = expect_bool(&value).map_err(|e| Trap::new(e.to_string()))?;
    Ok(if val { 1 } else { 0 })
}

fn read_text_host(
    caller: Caller<'_, ContractHost<'_>>,
    subject_ptr: i32,
    path_ptr: i32,
    out_ptr: i32,
) -> Result<i32, Trap> {
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("missing memory")),
    };
    let Some(env) = caller.data().env else {
        return Err(Trap::new("missing env"));
    };
    let (subject, seq) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    let path = read_text_arg(&memory, &caller, path_ptr)?;
    let (typ, value) = remote::read_remote_field(env, &subject, seq, &path)
        .map_err(|e| Trap::new(e.to_string()))?;
    if !matches!(typ, crate::pdl::schema::TypeSpec::Text(_) | crate::pdl::schema::TypeSpec::Currency | crate::pdl::schema::TypeSpec::Enum(_)) {
        return Err(Trap::new("read_text type mismatch"));
    }
    let text = expect_text(&value).map_err(|e| Trap::new(e.to_string()))?;
    if text.len() > DEFAULT_TEXT_LEN {
        return Err(Trap::new("read_text overflow"));
    }
    let mut buf = vec![0u8; 4 + DEFAULT_TEXT_LEN];
    buf[..4].copy_from_slice(&(text.len() as u32).to_le_bytes());
    buf[4..4 + text.len()].copy_from_slice(text.as_bytes());
    memory
        .write(caller, out_ptr as usize, &buf)
        .map_err(|_| Trap::new("invalid text output"))?;
    Ok(out_ptr)
}

fn read_identity_host(
    caller: Caller<'_, ContractHost<'_>>,
    subject_ptr: i32,
    path_ptr: i32,
    out_ptr: i32,
) -> Result<i32, Trap> {
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("missing memory")),
    };
    let Some(env) = caller.data().env else {
        return Err(Trap::new("missing env"));
    };
    let (subject, seq) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    let path = read_text_arg(&memory, &caller, path_ptr)?;
    let (typ, value) = remote::read_remote_field(env, &subject, seq, &path)
        .map_err(|e| Trap::new(e.to_string()))?;
    if !matches!(typ, crate::pdl::schema::TypeSpec::Identity | crate::pdl::schema::TypeSpec::Ref(_)) {
        return Err(Trap::new("read_identity type mismatch"));
    }
    let bytes = expect_bytes(&value).map_err(|e| Trap::new(e.to_string()))?;
    if bytes.len() != 32 {
        return Err(Trap::new("invalid identity bytes"));
    }
    memory
        .write(caller, out_ptr as usize, &bytes)
        .map_err(|_| Trap::new("invalid identity output"))?;
    Ok(out_ptr)
}

fn read_subject_ref_host(
    caller: Caller<'_, ContractHost<'_>>,
    subject_ptr: i32,
    path_ptr: i32,
    out_ptr: i32,
) -> Result<i32, Trap> {
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("missing memory")),
    };
    let Some(env) = caller.data().env else {
        return Err(Trap::new("missing env"));
    };
    let (subject, seq) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    let path = read_text_arg(&memory, &caller, path_ptr)?;
    let (typ, value) = remote::read_remote_field(env, &subject, seq, &path)
        .map_err(|e| Trap::new(e.to_string()))?;
    if !matches!(typ, crate::pdl::schema::TypeSpec::SubjectRef(_)) {
        return Err(Trap::new("read_subject_ref type mismatch"));
    }
    let map = expect_map(&value).map_err(|e| Trap::new(e.to_string()))?;
    let id_bytes = expect_bytes(
        map_get(map, "id").ok_or_else(|| Trap::new("subject_ref missing id"))?
    )
    .map_err(|e| Trap::new(e.to_string()))?;
    let seq_val = expect_uint(
        map_get(map, "seq").ok_or_else(|| Trap::new("subject_ref missing seq"))?
    )
    .map_err(|e| Trap::new(e.to_string()))?;
    if id_bytes.len() != 32 {
        return Err(Trap::new("subject_ref id invalid"));
    }
    let mut buf = [0u8; 40];
    buf[..32].copy_from_slice(&id_bytes);
    buf[32..40].copy_from_slice(&seq_val.to_le_bytes());
    memory
        .write(caller, out_ptr as usize, &buf)
        .map_err(|_| Trap::new("invalid subject_ref output"))?;
    Ok(out_ptr)
}

fn subject_id_host(
    caller: Caller<'_, ContractHost<'_>>,
    subject_ptr: i32,
    out_ptr: i32,
) -> Result<i32, Trap> {
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("missing memory")),
    };
    let (subject, _) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    memory
        .write(caller, out_ptr as usize, subject.as_bytes())
        .map_err(|_| Trap::new("invalid subject_id output"))?;
    Ok(out_ptr)
}

fn remote_intersects_host(
    caller: Caller<'_, ContractHost<'_>>,
    subject_ptr: i32,
    path_ptr: i32,
    list_ptr: i32,
    elem_kind: i32,
    elem_size: i32,
) -> Result<i32, Trap> {
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("missing memory")),
    };
    let Some(env) = caller.data().env else {
        return Err(Trap::new("missing env"));
    };
    let (subject, seq) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    let path = read_text_arg(&memory, &caller, path_ptr)?;
    let (_typ, remote_value) = remote::read_remote_field(env, &subject, seq, &path)
        .map_err(|e| Trap::new(e.to_string()))?;
    let remote_list = expect_array(&remote_value).map_err(|e| Trap::new(e.to_string()))?;

    let list_ptr = list_ptr as usize;
    let mut len_bytes = [0u8; 4];
    memory
        .read(&caller, list_ptr, &mut len_bytes)
        .map_err(|_| Trap::new("invalid list pointer"))?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len == 0 {
        return Ok(0);
    }

    match elem_kind {
        ELEM_KIND_TEXT => {
            let elem_size = elem_size as usize;
            let mut local = Vec::with_capacity(len);
            for idx in 0..len {
                let elem_ptr = list_ptr + 4 + idx * elem_size;
                let mut lbytes = [0u8; 4];
                memory
                    .read(&caller, elem_ptr, &mut lbytes)
                    .map_err(|_| Trap::new("invalid text element"))?;
                let l = u32::from_le_bytes(lbytes) as usize;
                if l > elem_size.saturating_sub(4) {
                    return Err(Trap::new("invalid text length"));
                }
                let mut buf = vec![0u8; l];
                if l > 0 {
                    memory
                        .read(&caller, elem_ptr + 4, &mut buf)
                        .map_err(|_| Trap::new("invalid text bytes"))?;
                }
                let text = String::from_utf8(buf).map_err(|_| Trap::new("invalid utf8"))?;
                local.push(text);
            }
            for entry in remote_list {
                let text = expect_text(entry).map_err(|e| Trap::new(e.to_string()))?;
                if local.iter().any(|l| l == &text) {
                    return Ok(1);
                }
            }
            Ok(0)
        }
        ELEM_KIND_IDENTITY => {
            let elem_size = elem_size as usize;
            if elem_size < 32 {
                return Err(Trap::new("identity elem size invalid"));
            }
            let mut local = Vec::with_capacity(len);
            for idx in 0..len {
                let elem_ptr = list_ptr + 4 + idx * elem_size;
                let mut buf = vec![0u8; 32];
                memory
                    .read(&caller, elem_ptr, &mut buf)
                    .map_err(|_| Trap::new("invalid identity bytes"))?;
                local.push(buf);
            }
            for entry in remote_list {
                let bytes = expect_bytes(entry).map_err(|e| Trap::new(e.to_string()))?;
                if bytes.len() != 32 {
                    return Err(Trap::new("remote identity invalid"));
                }
                if local.iter().any(|l| l == &bytes) {
                    return Ok(1);
                }
            }
            Ok(0)
        }
        ELEM_KIND_SUBJECT_REF => {
            let elem_size = elem_size as usize;
            if elem_size < 40 {
                return Err(Trap::new("subject_ref elem size invalid"));
            }
            let mut local: Vec<([u8; 32], i64)> = Vec::with_capacity(len);
            for idx in 0..len {
                let elem_ptr = list_ptr + 4 + idx * elem_size;
                let mut id = [0u8; 32];
                let mut seq_bytes = [0u8; 8];
                memory
                    .read(&caller, elem_ptr, &mut id)
                    .map_err(|_| Trap::new("invalid subject_ref id"))?;
                memory
                    .read(&caller, elem_ptr + 32, &mut seq_bytes)
                    .map_err(|_| Trap::new("invalid subject_ref seq"))?;
                let seq = i64::from_le_bytes(seq_bytes);
                local.push((id, seq));
            }
            for entry in remote_list {
                let map = expect_map(entry).map_err(|e| Trap::new(e.to_string()))?;
                let id_bytes = expect_bytes(
                    map_get(map, "id").ok_or_else(|| Trap::new("subject_ref missing id"))?
                )
                .map_err(|e| Trap::new(e.to_string()))?;
                let seq_val = expect_int(
                    map_get(map, "seq").ok_or_else(|| Trap::new("subject_ref missing seq"))?
                )
                .map_err(|e| Trap::new(e.to_string()))?;
                if id_bytes.len() != 32 {
                    return Err(Trap::new("subject_ref id invalid"));
                }
                let mut id = [0u8; 32];
                id.copy_from_slice(&id_bytes);
                if local.iter().any(|(lid, lseq)| lid == &id && *lseq == seq_val) {
                    return Ok(1);
                }
            }
            Ok(0)
        }
        _ => Err(Trap::new("unsupported elem kind")),
    }
}

fn normalize_text_list_host(
    mut caller: Caller<'_, ContractHost<'_>>,
    list_ptr: i32,
    elem_max: i32,
    cap: i32,
) -> Result<i32, Trap> {
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("missing memory")),
    };
    let list_ptr = list_ptr as usize;
    let elem_max = elem_max as usize;
    let cap = cap as usize;
    if elem_max == 0 {
        return Err(Trap::new("invalid text max"));
    }
    let mut len_bytes = [0u8; 4];
    memory
        .read(&caller, list_ptr, &mut len_bytes)
        .map_err(|_| Trap::new("invalid list pointer"))?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > cap {
        return Err(Trap::new("list length exceeds capacity"));
    }
    let elem_size = 4 + elem_max;
    let mut items = Vec::with_capacity(len);
    for idx in 0..len {
        let elem_ptr = list_ptr + 4 + idx * elem_size;
        let mut lbytes = [0u8; 4];
        memory
            .read(&caller, elem_ptr, &mut lbytes)
            .map_err(|_| Trap::new("invalid text element"))?;
        let l = u32::from_le_bytes(lbytes) as usize;
        if l > elem_max {
            return Err(Trap::new("invalid text length"));
        }
        let mut buf = vec![0u8; l];
        if l > 0 {
            memory
                .read(&caller, elem_ptr + 4, &mut buf)
                .map_err(|_| Trap::new("invalid text bytes"))?;
        }
        let text = String::from_utf8_lossy(&buf).to_string();
        items.push(text);
    }
    items.sort();
    items.dedup();

    let new_len = items.len().min(cap);
    let new_len_bytes = (new_len as u32).to_le_bytes();
    memory
        .write(&mut caller, list_ptr, &new_len_bytes)
        .map_err(|_| Trap::new("invalid list write"))?;

    for idx in 0..cap {
        let elem_ptr = list_ptr + 4 + idx * elem_size;
        let mut buf = vec![0u8; elem_size];
        if idx < new_len {
            let bytes = items[idx].as_bytes();
            let copy_len = bytes.len().min(elem_max);
            buf[..4].copy_from_slice(&(copy_len as u32).to_le_bytes());
            buf[4..4 + copy_len].copy_from_slice(&bytes[..copy_len]);
        }
        memory
            .write(&mut caller, elem_ptr, &buf)
            .map_err(|_| Trap::new("invalid list element write"))?;
    }
    Ok(0)
}

pub fn accept_wasm_bytes() -> Vec<u8> {
    include_bytes!(concat!(env!("OUT_DIR"), "/accept.wasm")).to_vec()
}

fn get_memory<T>(instance: &Instance, store: &Store<T>) -> Result<Memory, DharmaError> {
    instance
        .get_memory(store, "memory")
        .ok_or_else(|| DharmaError::Contract("missing memory export".to_string()))
}

fn map_wasmi_error(err: wasmi::Error) -> DharmaError {
    match err {
        wasmi::Error::Trap(trap) if matches!(trap.trap_code(), Some(TrapCode::OutOfFuel)) => {
            DharmaError::OutOfFuel
        }
        wasmi::Error::Store(fuel_err)
            if matches!(fuel_err, wasmi::errors::FuelError::OutOfFuel) =>
        {
            DharmaError::OutOfFuel
        }
        other => DharmaError::Contract(other.to_string()),
    }
}

fn map_fuel_error(err: wasmi::errors::FuelError) -> DharmaError {
    match err {
        wasmi::errors::FuelError::OutOfFuel => DharmaError::OutOfFuel,
        other => DharmaError::Contract(other.to_string()),
    }
}

fn get_func<T, Params, Results>(
    instance: &Instance,
    store: &Store<T>,
    name: &str,
) -> Result<TypedFunc<Params, Results>, DharmaError>
where
    Params: wasmi::WasmParams,
    Results: wasmi::WasmResults,
{
    instance
        .get_typed_func::<Params, Results>(store, name)
        .map_err(map_wasmi_error)
}

fn write_memory<T>(
    memory: &Memory,
    store: &mut Store<T>,
    ptr: i32,
    data: &[u8],
) -> Result<(), DharmaError> {
    let offset = to_usize(ptr)?;
    memory
        .write(store, offset, data)
        .map_err(|e| DharmaError::Contract(e.to_string()))
}

fn read_memory<T>(
    memory: &Memory,
    store: &mut Store<T>,
    ptr: i32,
    len: i32,
) -> Result<Vec<u8>, DharmaError> {
    let offset = to_usize(ptr)?;
    let length = to_usize(len)?;
    let mut buf = vec![0u8; length];
    memory
        .read(store, offset, &mut buf)
        .map_err(|e| DharmaError::Contract(e.to_string()))?;
    Ok(buf)
}

fn to_usize(value: i32) -> Result<usize, DharmaError> {
    if value < 0 {
        return Err(DharmaError::Contract("negative pointer".to_string()));
    }
    Ok(value as usize)
}

fn parse_contract_result(bytes: &[u8]) -> Result<ContractResult, DharmaError> {
    let value = cbor::ensure_canonical(bytes)?;
    let map = expect_map(&value)?;
    let ok = expect_bool(map_get(map, "ok").ok_or_else(|| DharmaError::Contract("missing ok".to_string()))?)?;
    let status_text = expect_text(map_get(map, "status").ok_or_else(|| DharmaError::Contract("missing status".to_string()))?)?;
    let status = match status_text.as_str() {
        "accept" => ContractStatus::Accept,
        "reject" => ContractStatus::Reject,
        "pending" => ContractStatus::Pending,
        _ => return Err(DharmaError::Contract("invalid status".to_string())),
    };
    let reason = match map_get(map, "reason") {
        None => None,
        Some(Value::Null) => None,
        Some(other) => Some(expect_text(other)?),
    };
    Ok(ContractResult { ok, status, reason })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::test_wasm_bytes;
    use crate::types::ContractId;

    #[test]
    fn wasm_contract_validate() {
        let engine = ContractEngine::new(test_wasm_bytes());
        let result = engine.validate(&[1, 2], &[3, 4]).unwrap();
        assert_eq!(result.status, ContractStatus::Accept);
        assert!(result.ok);
        assert!(result.reason.is_none());
    }

    #[test]
    fn wasm_contract_reduce() {
        let engine = ContractEngine::new(test_wasm_bytes());
        let bytes = engine.reduce(&[9, 9, 9]).unwrap();
        let result = parse_contract_result(&bytes).unwrap();
        assert_eq!(result.status, ContractStatus::Accept);
    }

    #[test]
    fn wasm_contract_reject_status() {
        let engine = ContractEngine::new(test_result_wasm_bytes("reject", false, Some("rule failed")));
        let result = engine.validate(&[1, 2], &[3, 4]).unwrap();
        assert_eq!(result.status, ContractStatus::Reject);
        assert!(!result.ok);
        assert_eq!(result.reason.as_deref(), Some("rule failed"));
    }

    #[test]
    fn wasm_contract_pending_status() {
        let engine = ContractEngine::new(test_result_wasm_bytes("pending", false, Some("awaiting approval")));
        let result = engine.validate(&[1, 2], &[3, 4]).unwrap();
        assert_eq!(result.status, ContractStatus::Pending);
        assert!(!result.ok);
        assert_eq!(result.reason.as_deref(), Some("awaiting approval"));
    }

    #[test]
    fn to_usize_rejects_negative() {
        assert!(super::to_usize(-1).is_err());
    }

    #[test]
    fn wasm_contract_fuel_exhausts_on_loop() {
        let mut limits = VmLimits::default();
        limits.fuel = 1_000;
        let engine = ContractEngine::new_with_limits(test_loop_wasm_bytes(), limits);
        let err = engine.validate(&[1, 2], &[3, 4]).unwrap_err();
        assert!(matches!(err, DharmaError::OutOfFuel), "unexpected error: {err}");
    }

    #[test]
    fn wasm_contract_zero_limits_use_defaults() {
        let limits = VmLimits {
            fuel: 0,
            memory_bytes: 0,
        };
        let engine = ContractEngine::new_with_limits(test_wasm_bytes(), limits);
        let result = engine.validate(&[1, 2], &[3, 4]).unwrap();
        assert!(result.ok);
    }

    #[test]
    fn wasm_contract_rejects_large_memory_module() {
        let limits = VmLimits {
            fuel: 1_000_000,
            memory_bytes: 64 * 1024,
        };
        let engine = ContractEngine::new_with_limits(test_large_memory_wasm_bytes(), limits);
        let err = engine.validate(&[1, 2], &[3, 4]).unwrap_err();
        match err {
            DharmaError::Contract(msg) => assert!(msg.to_lowercase().contains("memory")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn wasm_contract_blocks_unknown_imports() {
        let engine = ContractEngine::new(test_sandbox_escape_wasm_bytes());
        let err = engine.validate(&[1, 2], &[3, 4]).unwrap_err();
        match err {
            DharmaError::Contract(msg) => assert!(!msg.is_empty()),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn summary_format_roundtrip() {
        let mut actions = BTreeMap::new();
        let mut roles = BTreeSet::new();
        roles.insert("admin".to_string());
        actions.insert(
            "Approve".to_string(),
            PermissionRule {
                roles,
                exhaustive: true,
            },
        );
        let mut role_scopes = BTreeMap::new();
        let mut scopes = BTreeSet::new();
        scopes.insert("read".to_string());
        role_scopes.insert("admin".to_string(), scopes);
        let summary = PermissionSummary {
            v: 1,
            contract: ContractId::from_bytes([9u8; 32]),
            ver: 1,
            actions,
            queries: BTreeMap::new(),
            role_scopes,
            public: PublicPermissions::default(),
        };
        let bytes = summary.to_cbor().unwrap();
        let decoded = PermissionSummary::from_cbor(&bytes).unwrap();
        assert_eq!(summary, decoded);
    }
}

#[cfg(test)]
pub(crate) fn test_wasm_bytes() -> Vec<u8> {
    let wat = r#"
        (module
          (memory (export "memory") 1)
          (global $heap (mut i32) (i32.const 64))
          (global $len (mut i32) (i32.const 0))
          (func $alloc (export "alloc") (param $size i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $heap))
            (global.set $heap (i32.add (global.get $heap) (local.get $size)))
            (local.get $ptr)
          )
          (func (export "result_len") (result i32)
            (global.get $len)
          )
          (func (export "validate") (param i32 i32 i32 i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (call $write_result))
            (local.get $ptr)
          )
          (func (export "reduce") (param i32 i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (call $write_result))
            (local.get $ptr)
          )
          (func $write_result (result i32)
            (local $ptr i32)
            (local.set $ptr (call $alloc (i32.const 27)))
            ;; CBOR map: {"ok": true, "reason": null, "status": "accept"}
            (i32.store8 (local.get $ptr) (i32.const 0xa3))
            ;; "ok"
            (i32.store8 (i32.add (local.get $ptr) (i32.const 1)) (i32.const 0x62))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 2)) (i32.const 0x6f))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 3)) (i32.const 0x6b))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 4)) (i32.const 0xf5))
            ;; "reason"
            (i32.store8 (i32.add (local.get $ptr) (i32.const 5)) (i32.const 0x66))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 6)) (i32.const 0x72))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 7)) (i32.const 0x65))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 8)) (i32.const 0x61))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 9)) (i32.const 0x73))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 10)) (i32.const 0x6f))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 11)) (i32.const 0x6e))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 12)) (i32.const 0xf6))
            ;; "status"
            (i32.store8 (i32.add (local.get $ptr) (i32.const 13)) (i32.const 0x66))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 14)) (i32.const 0x73))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 15)) (i32.const 0x74))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 16)) (i32.const 0x61))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 17)) (i32.const 0x74))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 18)) (i32.const 0x75))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 19)) (i32.const 0x73))
            ;; "accept"
            (i32.store8 (i32.add (local.get $ptr) (i32.const 20)) (i32.const 0x66))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 21)) (i32.const 0x61))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 22)) (i32.const 0x63))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 23)) (i32.const 0x63))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 24)) (i32.const 0x65))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 25)) (i32.const 0x70))
            (i32.store8 (i32.add (local.get $ptr) (i32.const 26)) (i32.const 0x74))
            (global.set $len (i32.const 27))
            (local.get $ptr)
          )
        )
        "#;
    wat::parse_str(wat).unwrap()
}

#[cfg(test)]
fn test_loop_wasm_bytes() -> Vec<u8> {
    let wat = r#"
        (module
          (memory (export "memory") 1)
          (global $heap (mut i32) (i32.const 64))
          (func $alloc (export "alloc") (param $size i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $heap))
            (global.set $heap (i32.add (global.get $heap) (local.get $size)))
            (local.get $ptr)
          )
          (func (export "result_len") (result i32)
            (i32.const 0)
          )
          (func (export "validate") (param i32 i32 i32 i32) (result i32)
            (loop $spin
              (br $spin)
            )
            (i32.const 0)
          )
          (func (export "reduce") (param i32 i32) (result i32)
            (i32.const 0)
          )
        )
        "#;
    wat::parse_str(wat).unwrap()
}

#[cfg(test)]
fn test_result_wasm_bytes(status: &str, ok: bool, reason: Option<&str>) -> Vec<u8> {
    let result = Value::Map(vec![
        (Value::Text("ok".to_string()), Value::Bool(ok)),
        (
            Value::Text("reason".to_string()),
            reason
                .map(|value| Value::Text(value.to_string()))
                .unwrap_or(Value::Null),
        ),
        (
            Value::Text("status".to_string()),
            Value::Text(status.to_string()),
        ),
    ]);
    let bytes = cbor::encode_canonical_value(&result).unwrap();
    let encoded = bytes
        .iter()
        .map(|byte| format!("\\{:02x}", byte))
        .collect::<String>();
    let len = bytes.len();
    let wat = format!(
        r#"
        (module
          (memory (export "memory") 1)
          (global $heap (mut i32) (i32.const 64))
          (global $len (mut i32) (i32.const {len}))
          (func (export "alloc") (param $size i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $heap))
            (global.set $heap (i32.add (global.get $heap) (local.get $size)))
            (local.get $ptr)
          )
          (func (export "result_len") (result i32)
            (global.get $len)
          )
          (func (export "validate") (param i32 i32 i32 i32) (result i32)
            (i32.const 4096)
          )
          (func (export "reduce") (param i32 i32) (result i32)
            (i32.const 4096)
          )
          (data (i32.const 4096) "{encoded}")
        )
        "#
    );
    wat::parse_str(wat).unwrap()
}

#[cfg(test)]
fn test_large_memory_wasm_bytes() -> Vec<u8> {
    let wat = r#"
        (module
          (memory (export "memory") 64)
          (func (export "alloc") (param i32) (result i32)
            (i32.const 0)
          )
          (func (export "result_len") (result i32)
            (i32.const 0)
          )
          (func (export "validate") (param i32 i32 i32 i32) (result i32)
            (i32.const 0)
          )
          (func (export "reduce") (param i32 i32) (result i32)
            (i32.const 0)
          )
        )
        "#;
    wat::parse_str(wat).unwrap()
}

#[cfg(test)]
fn test_sandbox_escape_wasm_bytes() -> Vec<u8> {
    let wat = r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (func (export "alloc") (param i32) (result i32)
            (i32.const 0)
          )
          (func (export "result_len") (result i32)
            (i32.const 0)
          )
          (func (export "validate") (param i32 i32 i32 i32) (result i32)
            (call $fd_write (i32.const 1) (i32.const 0) (i32.const 0) (i32.const 0))
            (i32.const 0)
          )
          (func (export "reduce") (param i32 i32) (result i32)
            (i32.const 0)
          )
        )
        "#;
    wat::parse_str(wat).unwrap()
}
