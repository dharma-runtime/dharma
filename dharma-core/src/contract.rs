use crate::cbor;
use crate::env::Env;
use crate::error::DharmaError;
use crate::identity;
use crate::runtime::vm::VmLimits;
use crate::types::SubjectId;
use crate::value::{expect_bool, expect_map, expect_text, map_get};
use ciborium::value::Value;
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
