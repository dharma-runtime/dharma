use crate::env::Env;
use crate::error::DharmaError;
use crate::identity;
use crate::pdl::schema::DEFAULT_TEXT_LEN;
use crate::runtime::remote;
use crate::types::SubjectId;
use crate::value::{expect_array, expect_bool, expect_bytes, expect_int, expect_map, expect_text, expect_uint, map_get};
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
const MAX_PATH_LEN: usize = 256;
const ELEM_KIND_TEXT: i32 = 1;
const ELEM_KIND_IDENTITY: i32 = 2;
const ELEM_KIND_SUBJECT_REF: i32 = 3;

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
        linker
            .func_wrap("env", "read_int", read_int_host)
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        linker
            .func_wrap("env", "read_bool", read_bool_host)
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        linker
            .func_wrap("env", "read_text", read_text_host)
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        linker
            .func_wrap("env", "read_identity", read_identity_host)
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        linker
            .func_wrap("env", "read_subject_ref", read_subject_ref_host)
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        linker
            .func_wrap("env", "subject_id", subject_id_host)
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        linker
            .func_wrap("env", "remote_intersects", remote_intersects_host)
            .map_err(|e| DharmaError::Contract(e.to_string()))?;
        linker
            .func_wrap("env", "normalize_text_list", normalize_text_list_host)
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
    let (subject, seq) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    let path = read_text_arg(&memory, &caller, path_ptr)?;
    let (typ, value) =
        remote::read_remote_field(caller.data().env, &subject, seq, &path)
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
    let (subject, seq) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    let path = read_text_arg(&memory, &caller, path_ptr)?;
    let (typ, value) =
        remote::read_remote_field(caller.data().env, &subject, seq, &path)
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
    let (subject, seq) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    let path = read_text_arg(&memory, &caller, path_ptr)?;
    let (typ, value) =
        remote::read_remote_field(caller.data().env, &subject, seq, &path)
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
    let (subject, seq) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    let path = read_text_arg(&memory, &caller, path_ptr)?;
    let (typ, value) =
        remote::read_remote_field(caller.data().env, &subject, seq, &path)
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
    let (subject, seq) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    let path = read_text_arg(&memory, &caller, path_ptr)?;
    let (typ, value) =
        remote::read_remote_field(caller.data().env, &subject, seq, &path)
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
    let (subject, seq) = read_subject_ref_arg(&memory, &caller, subject_ptr)?;
    let path = read_text_arg(&memory, &caller, path_ptr)?;
    let (_typ, remote_value) =
        remote::read_remote_field(caller.data().env, &subject, seq, &path)
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
