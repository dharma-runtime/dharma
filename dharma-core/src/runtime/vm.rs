use crate::env::Env;
use crate::error::DharmaError;
use crate::identity;
use crate::types::SubjectId;
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
};
use wasmi::core::Trap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

pub const STATE_BASE: usize = 0x0000;
pub const OVERLAY_BASE: usize = 0x1000;
pub const ARGS_BASE: usize = 0x2000;
pub const CONTEXT_BASE: usize = 0x3000;
pub const STATE_SIZE: usize = 0x2000;

const DEFAULT_FUEL: u64 = 1_000_000;
const DEFAULT_MEMORY_BYTES: usize = 640 * 1024;
static DEFAULT_FUEL_LIMIT: AtomicU64 = AtomicU64::new(DEFAULT_FUEL);
static DEFAULT_MEMORY_LIMIT: AtomicUsize = AtomicUsize::new(DEFAULT_MEMORY_BYTES);
const MAX_ROLE_LEN: usize = 128;

#[derive(Clone, Copy, Debug)]
pub struct VmLimits {
    pub fuel: u64,
    pub memory_bytes: usize,
}

impl Default for VmLimits {
    fn default() -> Self {
        Self {
            fuel: DEFAULT_FUEL_LIMIT.load(Ordering::Relaxed),
            memory_bytes: DEFAULT_MEMORY_LIMIT.load(Ordering::Relaxed),
        }
    }
}

pub fn set_default_limits(limits: VmLimits) {
    if limits.fuel > 0 {
        DEFAULT_FUEL_LIMIT.store(limits.fuel, Ordering::Relaxed);
    }
    if limits.memory_bytes > 0 {
        DEFAULT_MEMORY_LIMIT.store(limits.memory_bytes, Ordering::Relaxed);
    }
}

pub struct RuntimeVm {
    wasm: Vec<u8>,
    limits: VmLimits,
}

impl RuntimeVm {
    pub fn new(wasm: Vec<u8>) -> Self {
        Self {
            wasm,
            limits: VmLimits::default(),
        }
    }

    pub fn new_with_limits(wasm: Vec<u8>, limits: VmLimits) -> Self {
        Self { wasm, limits }
    }

    pub fn validate(
        &self,
        env: &dyn Env,
        state: &mut [u8],
        args: &[u8],
        context: Option<&[u8]>,
    ) -> Result<(), DharmaError> {
        let (mut store, instance, memory) = self.instantiate(env)?;
        write_memory(&memory, &mut store, STATE_BASE, state)?;
        write_memory(&memory, &mut store, ARGS_BASE, args)?;
        if let Some(ctx) = context {
            write_memory(&memory, &mut store, CONTEXT_BASE, ctx)?;
        }
        let validate = instance
            .get_typed_func::<(), i32>(&store, "validate")
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        let result = validate
            .call(&mut store, ())
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        if result != 0 {
            return Err(DharmaError::Validation("contract rejected".to_string()));
        }
        read_memory(&memory, &mut store, STATE_BASE, state)?;
        Ok(())
    }

    pub fn reduce(
        &self,
        env: &dyn Env,
        state: &mut [u8],
        args: &[u8],
        context: Option<&[u8]>,
    ) -> Result<(), DharmaError> {
        let (mut store, instance, memory) = self.instantiate(env)?;
        write_memory(&memory, &mut store, STATE_BASE, state)?;
        write_memory(&memory, &mut store, ARGS_BASE, args)?;
        if let Some(ctx) = context {
            write_memory(&memory, &mut store, CONTEXT_BASE, ctx)?;
        }
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        let result = reduce
            .call(&mut store, ())
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        if result != 0 {
            return Err(DharmaError::Validation("contract rejected".to_string()));
        }
        read_memory(&memory, &mut store, STATE_BASE, state)?;
        Ok(())
    }

    fn instantiate<'a>(
        &'a self,
        env: &'a dyn Env,
    ) -> Result<(Store<ContractHost<'a>>, Instance, Memory), DharmaError> {
        let mut config = Config::default();
        config.consume_fuel(true);
        config.fuel_consumption_mode(FuelConsumptionMode::Eager);
        let engine = Engine::new(&config);
        let cursor = std::io::Cursor::new(&self.wasm);
        let module =
            Module::new(&engine, cursor).map_err(|e| DharmaError::Contract(e.to_string()))?;
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.limits.memory_bytes)
            .build();
        let mut store = Store::new(&engine, ContractHost::new(env, limits));
        store.limiter(|host| &mut host.limits);
        store
            .add_fuel(self.limits.fuel)
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
        Ok((store, instance, memory))
    }
}

struct ContractHost<'a> {
    env: &'a dyn Env,
    limits: StoreLimits,
}

impl<'a> ContractHost<'a> {
    fn new(env: &'a dyn Env, limits: StoreLimits) -> Self {
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
    let allowed = identity::has_role(caller.data().env, &subject, role).unwrap_or(false);
    Ok(if allowed { 1 } else { 0 })
}

fn write_memory<T>(
    memory: &Memory,
    store: &mut Store<T>,
    offset: usize,
    data: &[u8],
) -> Result<(), DharmaError> {
    memory
        .write(store, offset, data)
        .map_err(|e| DharmaError::Contract(e.to_string()))
}

fn read_memory<T>(
    memory: &Memory,
    store: &mut Store<T>,
    offset: usize,
    out: &mut [u8],
) -> Result<(), DharmaError> {
    memory
        .read(store, offset, out)
        .map_err(|e| DharmaError::Contract(e.to_string()))
}

#[cfg(all(test, feature = "compiler"))]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::crypto;
    use crate::env::StdEnv;
    use crate::pdl::codegen::wasm;
    use crate::pdl::parser;
    use crate::store::state::append_assertion;
    use crate::types::{ContractId, SchemaId};
    use rand_chacha::ChaCha20Rng;
    use rand_core::SeedableRng;

    #[test]
    fn vm_validate_and_reduce_ok() {
        let doc = r#"```dhl
aggregate Dummy
    state
        status: Int = 1

action Touch()
    validate
        state.status == 1
    apply
        state.status = 2
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = wasm::compile(&ast).unwrap();
        let vm = RuntimeVm::new(bytes);
        let temp = tempfile::tempdir().unwrap();
        let env = StdEnv::new(temp.path());
        let mut state = vec![0u8; STATE_SIZE];
        state[..8].copy_from_slice(&1i64.to_le_bytes());
        let mut args = vec![0u8; 4];
        args[..4].copy_from_slice(&0u32.to_le_bytes());
        vm.validate(&env, &mut state, &args, None).unwrap();
        vm.reduce(&env, &mut state, &args, None).unwrap();
    }

    #[test]
    fn vm_has_role_checks_identity_profile() {
        let temp = tempfile::tempdir().unwrap();
        let env = StdEnv::new(temp.path());
        let mut rng = ChaCha20Rng::seed_from_u64(7);
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
        let genesis = AssertionPlaintext::sign(genesis_header, ciborium::value::Value::Map(vec![]), &root_sk).unwrap();
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
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("roles".to_string()),
            ciborium::value::Value::Array(vec![ciborium::value::Value::Text(
                "finance.approver".to_string(),
            )]),
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
        ok: Bool = false

action Touch()
    validate
        has_role(context.signer, "finance.approver")
    apply
        state.ok = true
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = wasm::compile(&ast).unwrap();
        let vm = RuntimeVm::new(bytes);
        let mut state = vec![0u8; STATE_SIZE];
        let mut args = vec![0u8; 4];
        args[..4].copy_from_slice(&0u32.to_le_bytes());
        let mut context = vec![0u8; 40];
        context[..32].copy_from_slice(subject.as_bytes());
        vm.validate(&env, &mut state, &args, Some(&context)).unwrap();

        let other = SubjectId::from_bytes([9u8; 32]);
        context[..32].copy_from_slice(other.as_bytes());
        let err = vm
            .validate(&env, &mut state, &args, Some(&context))
            .unwrap_err();
        assert!(err.to_string().contains("rejected"));
    }

    #[test]
    fn vm_fuel_exhausts_on_loop() {
        let wat = r#"
(module
  (memory (export "memory") 1)
  (func (export "validate") (result i32)
    (loop $loop
      br $loop
    )
    i32.const 0)
  (func (export "reduce") (result i32)
    i32.const 0)
)
"#;
        let wasm = wat::parse_str(wat).unwrap();
        let limits = VmLimits {
            fuel: 1_000,
            memory_bytes: 64 * 1024,
        };
        let vm = RuntimeVm::new_with_limits(wasm, limits);
        let temp = tempfile::tempdir().unwrap();
        let env = StdEnv::new(temp.path());
        let mut state = vec![0u8; STATE_SIZE];
        let args = vec![0u8; 4];
        let err = vm.validate(&env, &mut state, &args, None).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("fuel"));
    }
}
