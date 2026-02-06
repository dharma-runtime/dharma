use crate::cbor;
use crate::error::DharmaError;
use crate::identity;
use crate::runtime::vm::VmLimits;
use crate::types::hex_encode;
use crate::types::SubjectId;
use ciborium::value::Value;
use blake3;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
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
    Store as WasmStore,
    StoreLimits,
    StoreLimitsBuilder,
    Value as WasmValue,
};
use wasmi::core::Trap;

#[derive(Clone, Debug, PartialEq)]
pub struct ReactorPlan {
    pub version: u8,
    pub reactors: Vec<ReactorSpec>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReactorSpec {
    pub name: String,
    pub trigger: Option<String>,
    pub scope: Option<String>,
    pub validates: Vec<Expr>,
    pub emits: Vec<EmitSpec>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EmitSpec {
    pub action: String,
    pub args: Vec<(String, Expr)>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Literal(Value),
    Path(Vec<String>),
    Unary(Op, Box<Expr>),
    Binary(Op, Box<Expr>, Box<Expr>),
    Call(String, Vec<Expr>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    In,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
    Not,
    Neg,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EvalContext {
    pub event: Value,
    pub header: Value,
    pub context: Value,
}

pub struct ReactorVm {
    store: WasmStore<ReactorHost>,
    instance: Instance,
    memory: Memory,
    plan: ReactorPlan,
    out_base: usize,
}

impl ReactorVm {
    pub fn new(bytes: Vec<u8>) -> Result<Self, DharmaError> {
        Self::new_with_root(bytes, PathBuf::from("."))
    }

    pub fn new_with_root(bytes: Vec<u8>, root: PathBuf) -> Result<Self, DharmaError> {
        let limits = VmLimits::default();
        let mut config = Config::default();
        config.consume_fuel(true);
        config.fuel_consumption_mode(FuelConsumptionMode::Eager);
        let engine = Engine::new(&config);
        let module =
            Module::new(&engine, std::io::Cursor::new(&bytes)).map_err(|e| {
                DharmaError::Contract(e.to_string())
            })?;
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(limits.memory_bytes)
            .build();
        let mut store = WasmStore::new(&engine, ReactorHost::new(root, store_limits));
        store.limiter(|host| &mut host.limits);
        store
            .add_fuel(limits.fuel)
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        let mut linker = Linker::new(&engine);
        linker
            .func_wrap("env", "has_role", has_role_host)
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(|e| DharmaError::Contract(e.to_string()))?
            .start(&mut store)
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        let memory = instance
            .get_memory(&store, "memory")
            .ok_or_else(|| DharmaError::Contract("missing memory".to_string()))?;

        let plan_ptr = read_global_i32(&instance, &store, "reactor_plan_ptr")? as usize;
        let plan_len = read_global_i32(&instance, &store, "reactor_plan_len")? as usize;
        let out_base = read_global_i32(&instance, &store, "reactor_out_base")? as usize;
        let mut plan_buf = vec![0u8; plan_len];
        if plan_len > 0 {
            memory
                .read(&store, plan_ptr, &mut plan_buf)
                .map_err(|e| DharmaError::Contract(e.to_string()))?;
        }
        let plan = if plan_len == 0 {
            ReactorPlan {
                version: 1,
                reactors: Vec::new(),
            }
        } else {
            ReactorPlan::from_cbor(&plan_buf)?
        };

        Ok(Self {
            store,
            instance,
            memory,
            plan,
            out_base,
        })
    }

    pub fn plan(&self) -> &ReactorPlan {
        &self.plan
    }

    pub fn out_base(&self) -> usize {
        self.out_base
    }

    pub fn write_memory(&mut self, offset: usize, data: &[u8]) -> Result<(), DharmaError> {
        self.memory
            .write(&mut self.store, offset, data)
            .map_err(|e| DharmaError::Contract(e.to_string()))
    }

    pub fn read_memory(&mut self, offset: usize, out: &mut [u8]) -> Result<(), DharmaError> {
        self.memory
            .read(&self.store, offset, out)
            .map_err(|e| DharmaError::Contract(e.to_string()))
    }

    pub fn check(&mut self, idx: usize) -> Result<bool, DharmaError> {
        let name = format!("reactor_check_{idx}");
        let func = self
            .instance
            .get_typed_func::<(), i32>(&self.store, &name)
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        let result = func
            .call(&mut self.store, ())
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        Ok(result != 0)
    }

    pub fn emit(&mut self, reactor_idx: usize, emit_idx: usize) -> Result<(), DharmaError> {
        let name = format!("reactor_emit_{reactor_idx}_{emit_idx}");
        let func = self
            .instance
            .get_typed_func::<(), i32>(&self.store, &name)
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        let _ = func
            .call(&mut self.store, ())
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        Ok(())
    }
}

const MAX_ROLE_LEN: usize = 128;

struct ReactorHost {
    root: PathBuf,
    limits: StoreLimits,
}

impl ReactorHost {
    fn new(root: PathBuf, limits: StoreLimits) -> Self {
        Self { root, limits }
    }
}

fn has_role_host(
    caller: Caller<'_, ReactorHost>,
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
    let env = crate::env::StdEnv::new(&caller.data().root);
    let allowed = identity::has_role(&env, &subject, role).unwrap_or(false);
    Ok(if allowed { 1 } else { 0 })
}

fn read_global_i32(
    instance: &Instance,
    store: &WasmStore<ReactorHost>,
    name: &str,
) -> Result<i32, DharmaError> {
    let global = instance
        .get_global(store, name)
        .ok_or_else(|| DharmaError::Contract("missing reactor global".to_string()))?;
    match global.get(store) {
        WasmValue::I32(value) => Ok(value),
        _ => Err(DharmaError::Contract("invalid reactor global".to_string())),
    }
}

impl ReactorPlan {
    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        let value = plan_to_value(self);
        cbor::encode_canonical_value(&value)
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        plan_from_value(&value)
    }
}

pub fn eval_bool(expr: &Expr, ctx: &EvalContext) -> Result<bool, DharmaError> {
    let value = eval_expr(expr, ctx)?;
    match value {
        Value::Bool(flag) => Ok(flag),
        _ => Err(DharmaError::Validation("expected bool".to_string())),
    }
}

pub fn eval_expr(expr: &Expr, ctx: &EvalContext) -> Result<Value, DharmaError> {
    match expr {
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Path(path) => resolve_path(path, ctx),
        Expr::Unary(op, inner) => match op {
            Op::Not => Ok(Value::Bool(!as_bool(&eval_expr(inner, ctx)?)?)),
            Op::Neg => Ok(Value::Integer((-as_i64(&eval_expr(inner, ctx)?)?).into())),
            _ => Err(DharmaError::Validation("unsupported unary op".to_string())),
        },
        Expr::Binary(op, left, right) => {
            let left_val = eval_expr(left, ctx)?;
            let right_val = eval_expr(right, ctx)?;
            match op {
                Op::Add => Ok(Value::Integer((as_i64(&left_val)? + as_i64(&right_val)?).into())),
                Op::Sub => Ok(Value::Integer((as_i64(&left_val)? - as_i64(&right_val)?).into())),
                Op::Mul => Ok(Value::Integer((as_i64(&left_val)? * as_i64(&right_val)?).into())),
                Op::Div => Ok(Value::Integer((as_i64(&left_val)? / as_i64(&right_val)?).into())),
                Op::Mod => Ok(Value::Integer((as_i64(&left_val)? % as_i64(&right_val)?).into())),
                Op::Eq => Ok(Value::Bool(value_eq(&left_val, &right_val))),
                Op::Neq => Ok(Value::Bool(!value_eq(&left_val, &right_val))),
                Op::Gt => Ok(Value::Bool(as_i64(&left_val)? > as_i64(&right_val)?)),
                Op::Lt => Ok(Value::Bool(as_i64(&left_val)? < as_i64(&right_val)?)),
                Op::Gte => Ok(Value::Bool(as_i64(&left_val)? >= as_i64(&right_val)?)),
                Op::Lte => Ok(Value::Bool(as_i64(&left_val)? <= as_i64(&right_val)?)),
                Op::And => Ok(Value::Bool(as_bool(&left_val)? && as_bool(&right_val)?)),
                Op::Or => Ok(Value::Bool(as_bool(&left_val)? || as_bool(&right_val)?)),
                Op::In => eval_in(&left_val, &right_val),
                _ => Err(DharmaError::Validation("unsupported binary op".to_string())),
            }
        }
        Expr::Call(name, args) => eval_call(name, args, ctx),
    }
}

fn eval_call(name: &str, args: &[Expr], ctx: &EvalContext) -> Result<Value, DharmaError> {
    match name {
        "len" => {
            if args.len() != 1 {
                return Err(DharmaError::Validation("len expects one arg".to_string()));
            }
            let value = eval_expr(&args[0], ctx)?;
            let len = match value {
                Value::Text(text) => text.len() as i64,
                Value::Array(items) => items.len() as i64,
                Value::Map(entries) => entries.len() as i64,
                _ => return Err(DharmaError::Validation("len unsupported".to_string())),
            };
            Ok(Value::Integer(len.into()))
        }
        "contains" => {
            if args.len() != 2 {
                return Err(DharmaError::Validation("contains expects two args".to_string()));
            }
            let hay = eval_expr(&args[0], ctx)?;
            let needle = eval_expr(&args[1], ctx)?;
            match hay {
                Value::Array(items) => Ok(Value::Bool(items.iter().any(|v| value_eq(v, &needle)))),
                Value::Map(entries) => Ok(Value::Bool(entries.iter().any(|(k, _)| value_eq(k, &needle)))),
                Value::Text(text) => match needle {
                    Value::Text(needle) => Ok(Value::Bool(text.contains(&needle))),
                    _ => Err(DharmaError::Validation("contains expects text needle".to_string())),
                },
                _ => Err(DharmaError::Validation("contains unsupported".to_string())),
            }
        }
        "index" | "get" => {
            if args.len() != 2 {
                return Err(DharmaError::Validation("index expects two args".to_string()));
            }
            let target = eval_expr(&args[0], ctx)?;
            let key = eval_expr(&args[1], ctx)?;
            match target {
                Value::Array(items) => {
                    let idx = as_i64(&key)?;
                    let idx: usize = idx
                        .try_into()
                        .map_err(|_| DharmaError::Validation("index out of range".to_string()))?;
                    Ok(items.get(idx).cloned().unwrap_or(Value::Null))
                }
                Value::Map(entries) => {
                    for (k, v) in entries {
                        if value_eq(&k, &key) {
                            return Ok(v);
                        }
                    }
                    Ok(Value::Null)
                }
                _ => Err(DharmaError::Validation("index unsupported".to_string())),
            }
        }
        "now" => {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            Ok(Value::Integer(ts.into()))
        }
        "days_between" => {
            if args.len() != 2 {
                return Err(DharmaError::Validation(
                    "days_between expects two args".to_string(),
                ));
            }
            let start = eval_expr(&args[0], ctx)?;
            let end = eval_expr(&args[1], ctx)?;
            let days = days_between_i64(as_i64(&start)?, as_i64(&end)?);
            Ok(Value::Integer(days.into()))
        }
        "days_until" => {
            if args.len() != 2 {
                return Err(DharmaError::Validation(
                    "days_until expects two args".to_string(),
                ));
            }
            let expiry = eval_expr(&args[0], ctx)?;
            let at = eval_expr(&args[1], ctx)?;
            let days = days_between_i64(as_i64(&at)?, as_i64(&expiry)?);
            Ok(Value::Integer(days.into()))
        }
        "has_role" => Ok(Value::Bool(false)),
        "distance" => {
            if args.len() != 2 {
                return Err(DharmaError::Validation("distance expects two args".to_string()));
            }
            let left = eval_expr(&args[0], ctx)?;
            let right = eval_expr(&args[1], ctx)?;
            let (lat1, lon1) = geopoint_from_value(&left)?;
            let (lat2, lon2) = geopoint_from_value(&right)?;
            let dist = (lat1 - lat2).abs() as i64 + (lon1 - lon2).abs() as i64;
            Ok(Value::Integer(dist.into()))
        }
        "sum" => {
            if args.len() != 1 {
                return Err(DharmaError::Validation("sum expects one arg".to_string()));
            }
            let list = eval_expr(&args[0], ctx)?;
            let mut total = 0i64;
            match list {
                Value::Array(items) => {
                    for item in items {
                        total += as_i64(&item)?;
                    }
                }
                _ => return Err(DharmaError::Validation("sum expects list".to_string())),
            }
            Ok(Value::Integer(total.into()))
        }
        "proj_id" => {
            if args.is_empty() {
                return Err(DharmaError::Validation("proj_id expects at least one arg".to_string()));
            }
            let mut items = Vec::new();
            for arg in args {
                items.push(eval_expr(arg, ctx)?);
            }
            let list = Value::Array(items);
            let bytes = cbor::encode_canonical_value(&list)?;
            let hash = *blake3::hash(&bytes).as_bytes();
            Ok(Value::Text(hex_encode(hash)))
        }
        _ => Err(DharmaError::Validation("unknown call".to_string())),
    }
}

fn days_between_i64(start: i64, end: i64) -> i64 {
    let diff = end - start;
    if diff >= 0 {
        diff / 86_400
    } else {
        (diff - 86_399) / 86_400
    }
}

fn eval_in(left: &Value, right: &Value) -> Result<Value, DharmaError> {
    match right {
        Value::Array(items) => Ok(Value::Bool(items.iter().any(|v| value_eq(v, left)))),
        Value::Map(entries) => Ok(Value::Bool(entries.iter().any(|(k, _)| value_eq(k, left)))),
        _ => Err(DharmaError::Validation("in expects list or map".to_string())),
    }
}

fn resolve_path(path: &[String], ctx: &EvalContext) -> Result<Value, DharmaError> {
    if path.is_empty() {
        return Err(DharmaError::Validation("empty path".to_string()));
    }
    match path[0].as_str() {
        "event" | "trigger" => lookup(&ctx.event, &path[1..]),
        "header" => lookup(&ctx.header, &path[1..]),
        "context" => lookup(&ctx.context, &path[1..]),
        _ => lookup(&ctx.event, path),
    }
    .ok_or_else(|| DharmaError::Validation("missing path".to_string()))
}

fn lookup(root: &Value, path: &[String]) -> Option<Value> {
    if path.is_empty() {
        return Some(root.clone());
    }
    match root {
        Value::Map(entries) => {
            for (k, v) in entries {
                if let Value::Text(name) = k {
                    if name == &path[0] {
                        return lookup(v, &path[1..]);
                    }
                }
            }
            None
        }
        Value::Array(items) => {
            let idx = path[0].parse::<usize>().ok()?;
            let next = items.get(idx)?;
            lookup(next, &path[1..])
        }
        _ => None,
    }
}

fn as_i64(value: &Value) -> Result<i64, DharmaError> {
    match value {
        Value::Integer(int) => (*int)
            .try_into()
            .map_err(|_| DharmaError::Validation("invalid int".to_string())),
        _ => Err(DharmaError::Validation("expected int".to_string())),
    }
}

fn as_bool(value: &Value) -> Result<bool, DharmaError> {
    match value {
        Value::Bool(flag) => Ok(*flag),
        _ => Err(DharmaError::Validation("expected bool".to_string())),
    }
}

fn value_eq(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Text(a), Value::Text(b)) => a == b,
        (Value::Bytes(a), Value::Bytes(b)) => a == b,
        _ => left == right,
    }
}

fn geopoint_from_value(value: &Value) -> Result<(i32, i32), DharmaError> {
    match value {
        Value::Map(entries) => {
            let mut lat = None;
            let mut lon = None;
            for (k, v) in entries {
                if let Value::Text(name) = k {
                    if name == "lat" {
                        if let Value::Integer(val) = v {
                            lat = i32::try_from(*val).ok();
                        }
                    } else if name == "lon" {
                        if let Value::Integer(val) = v {
                            lon = i32::try_from(*val).ok();
                        }
                    }
                }
            }
            match (lat, lon) {
                (Some(lat), Some(lon)) => Ok((lat, lon)),
                _ => Err(DharmaError::Validation("invalid geopoint".to_string())),
            }
        }
        Value::Array(items) => {
            if items.len() != 2 {
                return Err(DharmaError::Validation("invalid geopoint".to_string()));
            }
            let lat = match &items[0] {
                Value::Integer(val) => i32::try_from(*val).map_err(|_| {
                    DharmaError::Validation("invalid geopoint".to_string())
                })?,
                _ => return Err(DharmaError::Validation("invalid geopoint".to_string())),
            };
            let lon = match &items[1] {
                Value::Integer(val) => i32::try_from(*val).map_err(|_| {
                    DharmaError::Validation("invalid geopoint".to_string())
                })?,
                _ => return Err(DharmaError::Validation("invalid geopoint".to_string())),
            };
            Ok((lat, lon))
        }
        _ => Err(DharmaError::Validation("invalid geopoint".to_string())),
    }
}

fn plan_to_value(plan: &ReactorPlan) -> Value {
    let reactors = plan
        .reactors
        .iter()
        .map(reactor_to_value)
        .collect::<Vec<_>>();
    Value::Map(vec![
        (Value::Text("v".to_string()), Value::Integer(plan.version.into())),
        (Value::Text("reactors".to_string()), Value::Array(reactors)),
    ])
}

fn plan_from_value(value: &Value) -> Result<ReactorPlan, DharmaError> {
    let map = expect_map(value)?;
    let version = map_get(map, "v")
        .and_then(expect_int)
        .unwrap_or(1);
    let reactors_value = map_get(map, "reactors").ok_or_else(|| {
        DharmaError::Validation("missing reactors".to_string())
    })?;
    let reactors_array = expect_array(reactors_value)?;
    let mut reactors = Vec::new();
    for item in reactors_array {
        reactors.push(reactor_from_value(item)?);
    }
    Ok(ReactorPlan { version, reactors })
}

fn reactor_to_value(spec: &ReactorSpec) -> Value {
    let mut entries = vec![
        (Value::Text("name".to_string()), Value::Text(spec.name.clone())),
    ];
    if let Some(trigger) = &spec.trigger {
        entries.push((Value::Text("trigger".to_string()), Value::Text(trigger.clone())));
    }
    if let Some(scope) = &spec.scope {
        entries.push((Value::Text("scope".to_string()), Value::Text(scope.clone())));
    }
    let validates = spec
        .validates
        .iter()
        .map(expr_to_value)
        .collect::<Vec<_>>();
    entries.push((Value::Text("validates".to_string()), Value::Array(validates)));
    let emits = spec.emits.iter().map(emit_to_value).collect::<Vec<_>>();
    entries.push((Value::Text("emits".to_string()), Value::Array(emits)));
    Value::Map(entries)
}

fn reactor_from_value(value: &Value) -> Result<ReactorSpec, DharmaError> {
    let map = expect_map(value)?;
    let name = map_get(map, "name")
        .and_then(expect_text)
        .ok_or_else(|| DharmaError::Validation("reactor missing name".to_string()))?;
    let trigger = map_get(map, "trigger").and_then(expect_text);
    let scope = map_get(map, "scope").and_then(expect_text);
    let empty_validates = Value::Array(Vec::new());
    let validates_value = map_get(map, "validates").unwrap_or(&empty_validates);
    let mut validates = Vec::new();
    for item in expect_array(validates_value)? {
        validates.push(expr_from_value(item)?);
    }
    let empty_emits = Value::Array(Vec::new());
    let emits_value = map_get(map, "emits").unwrap_or(&empty_emits);
    let mut emits = Vec::new();
    for item in expect_array(emits_value)? {
        emits.push(emit_from_value(item)?);
    }
    Ok(ReactorSpec {
        name,
        trigger,
        scope,
        validates,
        emits,
    })
}

fn emit_to_value(emit: &EmitSpec) -> Value {
    let args = emit
        .args
        .iter()
        .map(|(k, v)| (Value::Text(k.clone()), expr_to_value(v)))
        .collect::<Vec<_>>();
    Value::Map(vec![
        (Value::Text("action".to_string()), Value::Text(emit.action.clone())),
        (Value::Text("args".to_string()), Value::Map(args)),
    ])
}

fn emit_from_value(value: &Value) -> Result<EmitSpec, DharmaError> {
    let map = expect_map(value)?;
    let action = map_get(map, "action")
        .and_then(expect_text)
        .ok_or_else(|| DharmaError::Validation("emit missing action".to_string()))?;
    let mut args = Vec::new();
    if let Some(Value::Map(entries)) = map_get(map, "args") {
        for (k, v) in entries {
            let key = expect_text(k).ok_or_else(|| {
                DharmaError::Validation("emit arg key must be text".to_string())
            })?;
            let expr = expr_from_value(v)?;
            args.push((key, expr));
        }
    }
    Ok(EmitSpec { action, args })
}

fn expr_to_value(expr: &Expr) -> Value {
    match expr {
        Expr::Literal(value) => Value::Map(vec![
            (Value::Text("t".to_string()), Value::Text("lit".to_string())),
            (Value::Text("v".to_string()), value.clone()),
        ]),
        Expr::Path(parts) => Value::Map(vec![
            (Value::Text("t".to_string()), Value::Text("path".to_string())),
            (
                Value::Text("v".to_string()),
                Value::Array(parts.iter().map(|p| Value::Text(p.clone())).collect()),
            ),
        ]),
        Expr::Unary(op, inner) => Value::Map(vec![
            (Value::Text("t".to_string()), Value::Text("unary".to_string())),
            (Value::Text("op".to_string()), Value::Text(op_to_str(*op).to_string())),
            (Value::Text("v".to_string()), expr_to_value(inner)),
        ]),
        Expr::Binary(op, left, right) => Value::Map(vec![
            (Value::Text("t".to_string()), Value::Text("binary".to_string())),
            (Value::Text("op".to_string()), Value::Text(op_to_str(*op).to_string())),
            (Value::Text("l".to_string()), expr_to_value(left)),
            (Value::Text("r".to_string()), expr_to_value(right)),
        ]),
        Expr::Call(name, args) => Value::Map(vec![
            (Value::Text("t".to_string()), Value::Text("call".to_string())),
            (Value::Text("name".to_string()), Value::Text(name.clone())),
            (
                Value::Text("args".to_string()),
                Value::Array(args.iter().map(expr_to_value).collect()),
            ),
        ]),
    }
}

fn expr_from_value(value: &Value) -> Result<Expr, DharmaError> {
    let map = expect_map(value)?;
    let typ = map_get(map, "t")
        .and_then(expect_text)
        .ok_or_else(|| DharmaError::Validation("expr missing type".to_string()))?;
    match typ.as_str() {
        "lit" => Ok(Expr::Literal(
            map_get(map, "v")
                .cloned()
                .ok_or_else(|| DharmaError::Validation("literal missing value".to_string()))?,
        )),
        "path" => {
            let arr = map_get(map, "v")
                .ok_or_else(|| DharmaError::Validation("path missing value".to_string()))?;
            let mut parts = Vec::new();
            for item in expect_array(arr)? {
                let text = expect_text(item).ok_or_else(|| {
                    DharmaError::Validation("path segment must be text".to_string())
                })?;
                parts.push(text);
            }
            Ok(Expr::Path(parts))
        }
        "unary" => {
            let op = map_get(map, "op")
                .and_then(expect_text)
                .ok_or_else(|| DharmaError::Validation("unary missing op".to_string()))?;
            let inner = map_get(map, "v")
                .ok_or_else(|| DharmaError::Validation("unary missing value".to_string()))?;
            Ok(Expr::Unary(
                op_from_str(&op)?,
                Box::new(expr_from_value(inner)?),
            ))
        }
        "binary" => {
            let op = map_get(map, "op")
                .and_then(expect_text)
                .ok_or_else(|| DharmaError::Validation("binary missing op".to_string()))?;
            let left = map_get(map, "l")
                .ok_or_else(|| DharmaError::Validation("binary missing left".to_string()))?;
            let right = map_get(map, "r")
                .ok_or_else(|| DharmaError::Validation("binary missing right".to_string()))?;
            Ok(Expr::Binary(
                op_from_str(&op)?,
                Box::new(expr_from_value(left)?),
                Box::new(expr_from_value(right)?),
            ))
        }
        "call" => {
            let name = map_get(map, "name")
                .and_then(expect_text)
                .ok_or_else(|| DharmaError::Validation("call missing name".to_string()))?;
            let empty_args = Value::Array(Vec::new());
            let args_value = map_get(map, "args").unwrap_or(&empty_args);
            let mut args = Vec::new();
            for item in expect_array(args_value)? {
                args.push(expr_from_value(item)?);
            }
            Ok(Expr::Call(name, args))
        }
        _ => Err(DharmaError::Validation("unknown expr type".to_string())),
    }
}

fn op_to_str(op: Op) -> &'static str {
    match op {
        Op::Add => "add",
        Op::Sub => "sub",
        Op::Mul => "mul",
        Op::Div => "div",
        Op::Mod => "mod",
        Op::In => "in",
        Op::Eq => "eq",
        Op::Neq => "neq",
        Op::Gt => "gt",
        Op::Lt => "lt",
        Op::Gte => "gte",
        Op::Lte => "lte",
        Op::And => "and",
        Op::Or => "or",
        Op::Not => "not",
        Op::Neg => "neg",
    }
}

fn op_from_str(value: &str) -> Result<Op, DharmaError> {
    match value {
        "add" => Ok(Op::Add),
        "sub" => Ok(Op::Sub),
        "mul" => Ok(Op::Mul),
        "div" => Ok(Op::Div),
        "mod" => Ok(Op::Mod),
        "in" => Ok(Op::In),
        "eq" => Ok(Op::Eq),
        "neq" => Ok(Op::Neq),
        "gt" => Ok(Op::Gt),
        "lt" => Ok(Op::Lt),
        "gte" => Ok(Op::Gte),
        "lte" => Ok(Op::Lte),
        "and" => Ok(Op::And),
        "or" => Ok(Op::Or),
        "not" => Ok(Op::Not),
        "neg" => Ok(Op::Neg),
        _ => Err(DharmaError::Validation("unknown op".to_string())),
    }
}

fn map_get<'a>(entries: &'a [(Value, Value)], key: &str) -> Option<&'a Value> {
    entries.iter().find_map(|(k, v)| {
        if let Value::Text(name) = k {
            if name == key {
                return Some(v);
            }
        }
        None
    })
}

fn expect_map(value: &Value) -> Result<&Vec<(Value, Value)>, DharmaError> {
    match value {
        Value::Map(entries) => Ok(entries),
        _ => Err(DharmaError::Validation("expected map".to_string())),
    }
}

fn expect_array(value: &Value) -> Result<&Vec<Value>, DharmaError> {
    match value {
        Value::Array(items) => Ok(items),
        _ => Err(DharmaError::Validation("expected array".to_string())),
    }
}

fn expect_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        _ => None,
    }
}

fn expect_int(value: &Value) -> Option<u8> {
    match value {
        Value::Integer(int) => u8::try_from(*int).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_roundtrip() {
        let plan = ReactorPlan {
            version: 1,
            reactors: vec![ReactorSpec {
                name: "Auto".to_string(),
                trigger: Some("action.Send".to_string()),
                scope: None,
                validates: vec![Expr::Binary(
                    Op::Gt,
                    Box::new(Expr::Path(vec!["amount".to_string()])),
                    Box::new(Expr::Literal(Value::Integer(10.into()))),
                )],
                emits: vec![EmitSpec {
                    action: "Approve".to_string(),
                    args: vec![(
                        "amount".to_string(),
                        Expr::Path(vec!["amount".to_string()]),
                    )],
                }],
            }],
        };
        let bytes = plan.to_cbor().unwrap();
        let decoded = ReactorPlan::from_cbor(&bytes).unwrap();
        assert_eq!(decoded, plan);
    }

    #[test]
    fn eval_expr_reads_event() {
        let ctx = EvalContext {
            event: Value::Map(vec![(
                Value::Text("amount".to_string()),
                Value::Integer(12.into()),
            )]),
            header: Value::Map(Vec::new()),
            context: Value::Map(Vec::new()),
        };
        let expr = Expr::Binary(
            Op::Gt,
            Box::new(Expr::Path(vec!["amount".to_string()])),
            Box::new(Expr::Literal(Value::Integer(10.into()))),
        );
        let result = eval_bool(&expr, &ctx).unwrap();
        assert!(result);
    }
}
