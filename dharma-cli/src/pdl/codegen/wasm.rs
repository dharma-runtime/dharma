use crate::error::DharmaError;
use crate::pdl::ast::{ActionDef, Assignment, AstFile, EmitDef, Expr, Literal, Op, ReactorDef};
use crate::pdl::codegen::schema as schema_codegen;
use crate::pdl::schema::{
    layout_action, layout_private, layout_public, list_capacity, map_capacity, type_size,
    CqrsSchema, TypeSpec, DEFAULT_TEXT_LEN,
};
use crate::pdl::typecheck;
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use wasm_encoder::{
    BlockType, CodeSection, DataSection, ExportKind, ExportSection, Function, FunctionSection,
    GlobalSection, GlobalType, ImportSection, Instruction, MemorySection, Module, TypeSection,
    ValType,
};

pub const STATE_BASE: u32 = 0x0000;
pub const OVERLAY_BASE: u32 = 0x1000;
pub const ARGS_BASE: u32 = 0x2000;
pub const CONTEXT_BASE: u32 = 0x3000;
pub const LITERAL_BASE: u32 = 0x4000;
pub const SCRATCH_BASE: u32 = 0x4800;
pub const REACTOR_OUT_BASE: u32 = 0x4000;
pub const REACTOR_PLAN_BASE: u32 = 0x5000;
pub const STATE_SIZE: u32 = 0x1000;
pub const ARGS_SIZE: u32 = 0x1000;
const MAX_PATH_LEN: usize = 256;

pub fn compile(ast: &AstFile) -> Result<Vec<u8>, DharmaError> {
    typecheck::check_ast(ast)?;
    let schema_bytes = schema_codegen::compile_schema(ast)?;
    let schema = CqrsSchema::from_cbor(&schema_bytes)?;
    let public_layout = layout_public(&schema);
    let private_layout = layout_private(&schema);
    let public_size = public_layout
        .last()
        .map(|f| f.offset + f.size)
        .unwrap_or(0);
    if public_size > STATE_SIZE as usize {
        return Err(DharmaError::Validation("public state exceeds 0x1000".to_string()));
    }
    let private_size = private_layout
        .last()
        .map(|f| f.offset + f.size)
        .unwrap_or(0);
    if private_size > STATE_SIZE as usize {
        return Err(DharmaError::Validation("private state exceeds 0x1000".to_string()));
    }

    let mut action_order = schema.actions.keys().cloned().collect::<Vec<_>>();
    action_order.sort();
    let mut action_layouts = HashMap::new();
    for name in &action_order {
        let action_schema = schema
            .actions
            .get(name)
            .ok_or_else(|| DharmaError::Validation("missing action".to_string()))?;
        let layout = layout_action(action_schema, &schema.structs);
        let total = layout.last().map(|f| f.offset + f.size).unwrap_or(4);
        if total > ARGS_SIZE as usize {
            return Err(DharmaError::Validation("args size exceeds 0x1000".to_string()));
        }
        action_layouts.insert(name.clone(), layout);
    }

    let mut types = TypeSection::new();
    types.function([ValType::I32, ValType::I32, ValType::I32], [ValType::I32]);
    let has_role_type = 0u32;
    types.function([ValType::I32, ValType::I32], [ValType::I64]);
    let read_int_type = 1u32;
    types.function([ValType::I32, ValType::I32], [ValType::I32]);
    let read_bool_type = 2u32;
    types.function([ValType::I32, ValType::I32, ValType::I32], [ValType::I32]);
    let read_text_type = 3u32;
    types.function([ValType::I32, ValType::I32, ValType::I32, ValType::I32, ValType::I32], [ValType::I32]);
    let remote_intersects_type = 4u32;
    types.function([], [ValType::I32]);
    let validate_type = 5u32;
    types.function([], [ValType::I32]);
    let reduce_type = 6u32;

    let mut module = Module::new();
    module.section(&types);

    let mut imports = ImportSection::new();
    imports.import("env", "has_role", wasm_encoder::EntityType::Function(has_role_type));
    imports.import("env", "read_int", wasm_encoder::EntityType::Function(read_int_type));
    imports.import("env", "read_bool", wasm_encoder::EntityType::Function(read_bool_type));
    imports.import("env", "read_text", wasm_encoder::EntityType::Function(read_text_type));
    imports.import("env", "read_identity", wasm_encoder::EntityType::Function(read_text_type));
    imports.import("env", "read_subject_ref", wasm_encoder::EntityType::Function(read_text_type));
    imports.import("env", "subject_id", wasm_encoder::EntityType::Function(read_bool_type));
    imports.import(
        "env",
        "remote_intersects",
        wasm_encoder::EntityType::Function(remote_intersects_type),
    );
    imports.import(
        "env",
        "normalize_text_list",
        wasm_encoder::EntityType::Function(has_role_type),
    );
    module.section(&imports);

    let mut functions = FunctionSection::new();
    functions.function(validate_type);
    functions.function(reduce_type);
    module.section(&functions);

    let mut memories = MemorySection::new();
    memories.memory(wasm_encoder::MemoryType {
        minimum: 1,
        maximum: None,
        memory64: false,
        shared: false,
    });
    module.section(&memories);

    let mut exports = ExportSection::new();
    exports.export("memory", ExportKind::Memory, 0);
    let import_count = 9u32;
    exports.export("validate", ExportKind::Func, import_count);
    exports.export("reduce", ExportKind::Func, import_count + 1);
    module.section(&exports);

    let host = HostFuncs {
        has_role_func: Some(0),
        read_int_func: Some(1),
        read_bool_func: Some(2),
        read_text_func: Some(3),
        read_identity_func: Some(4),
        read_subject_ref_func: Some(5),
        subject_id_func: Some(6),
        remote_intersects_func: Some(7),
        normalize_text_list_func: Some(8),
    };

    let mut code = CodeSection::new();
    let validate = compile_validate(
        ast,
        &schema,
        &public_layout,
        &private_layout,
        &action_layouts,
        &action_order,
        host,
        LITERAL_BASE,
        SCRATCH_BASE,
    )?;
    code.function(&validate);
    let reduce = compile_reduce(
        ast,
        &schema,
        &public_layout,
        &private_layout,
        &action_layouts,
        &action_order,
        host,
        LITERAL_BASE,
        SCRATCH_BASE,
    )?;
    code.function(&reduce);
    module.section(&code);

    Ok(module.finish())
}

pub fn compile_reactor(ast: &AstFile) -> Result<Vec<u8>, DharmaError> {
    let schema_bytes = schema_codegen::compile_schema(ast)?;
    let schema = CqrsSchema::from_cbor(&schema_bytes)?;
    let public_layout = layout_public(&schema);
    let private_layout = layout_private(&schema);

    let mut action_order = schema.actions.keys().cloned().collect::<Vec<_>>();
    action_order.sort();
    let mut action_layouts = HashMap::new();
    let mut action_index = HashMap::new();
    for (idx, name) in action_order.iter().enumerate() {
        let action_schema = schema
            .actions
            .get(name)
            .ok_or_else(|| DharmaError::Validation("missing action".to_string()))?;
        let layout = layout_action(action_schema, &schema.structs);
        action_layouts.insert(name.clone(), layout);
        action_index.insert(name.clone(), idx as u32);
    }

    let plan_bytes = crate::pdl::codegen::reactor::compile_plan(ast)?;

    let mut types = TypeSection::new();
    let has_role_type = 0u32;
    types.function([ValType::I32, ValType::I32, ValType::I32], [ValType::I32]);
    let func_type = 1u32;
    types.function([], [ValType::I32]);

    let mut module = Module::new();
    module.section(&types);

    let mut imports = ImportSection::new();
    imports.import("env", "has_role", wasm_encoder::EntityType::Function(has_role_type));
    module.section(&imports);

    let mut functions = FunctionSection::new();
    let mut func_exports: Vec<(String, u32)> = Vec::new();
    let mut code = CodeSection::new();

    let import_count = 1u32;
    let mut func_index = import_count;
    let literal_base = align_u32(REACTOR_PLAN_BASE + plan_bytes.len() as u32, 8);
    let scratch_base = align_u32(literal_base + 0x400, 8);
    let empty_layout: Vec<crate::pdl::schema::LayoutEntry> = Vec::new();
    for (r_idx, reactor) in ast.reactors.iter().enumerate() {
        let trigger_layout = if is_cron_trigger(reactor.trigger.as_deref()) {
            &empty_layout
        } else {
            let trigger_action = reactor_trigger_action(reactor.trigger.as_deref())?;
            action_layouts
                .get(&trigger_action)
                .ok_or_else(|| DharmaError::Validation("unknown reactor trigger action".to_string()))?
        };
        let env = Env::new(
            &schema,
            &public_layout,
            &private_layout,
            trigger_layout,
            ARGS_BASE,
            HostFuncs::has_role_only(Some(0)),
            literal_base,
            scratch_base,
        );

        functions.function(func_type);
        let mut check_func = Function::new(vec![(4, ValType::I32), (2, ValType::I64)]);
        compile_reactor_check(reactor, &env, &mut check_func)?;
        code.function(&check_func);
        func_exports.push((format!("reactor_check_{r_idx}"), func_index));
        func_index += 1;

        for (e_idx, emit) in reactor.emits.iter().enumerate() {
            let emit_action = normalize_action_name(&emit.action);
            let emit_layout = action_layouts
                .get(&emit_action)
                .ok_or_else(|| DharmaError::Validation("unknown emit action".to_string()))?;
            let emit_index = *action_index
                .get(&emit_action)
                .ok_or_else(|| DharmaError::Validation("missing action index".to_string()))?;
            functions.function(func_type);
            let mut emit_func = Function::new(vec![(4, ValType::I32), (2, ValType::I64)]);
            compile_reactor_emit(emit, emit_layout, emit_index, &env, &mut emit_func)?;
            code.function(&emit_func);
            func_exports.push((format!("reactor_emit_{r_idx}_{e_idx}"), func_index));
            func_index += 1;
        }
    }

    module.section(&functions);

    let mut memories = MemorySection::new();
    memories.memory(wasm_encoder::MemoryType {
        minimum: 1,
        maximum: None,
        memory64: false,
        shared: false,
    });
    module.section(&memories);

    let mut globals = GlobalSection::new();
    globals.global(
        GlobalType {
            val_type: ValType::I32,
            mutable: false,
        },
        &wasm_encoder::ConstExpr::i32_const(REACTOR_PLAN_BASE as i32),
    );
    globals.global(
        GlobalType {
            val_type: ValType::I32,
            mutable: false,
        },
        &wasm_encoder::ConstExpr::i32_const(plan_bytes.len() as i32),
    );
    globals.global(
        GlobalType {
            val_type: ValType::I32,
            mutable: false,
        },
        &wasm_encoder::ConstExpr::i32_const(REACTOR_OUT_BASE as i32),
    );
    module.section(&globals);

    let mut exports = ExportSection::new();
    exports.export("memory", ExportKind::Memory, 0);
    exports.export("reactor_plan_ptr", ExportKind::Global, 0);
    exports.export("reactor_plan_len", ExportKind::Global, 1);
    exports.export("reactor_out_base", ExportKind::Global, 2);
    for (name, idx) in func_exports {
        exports.export(&name, ExportKind::Func, idx);
    }
    module.section(&exports);
    module.section(&code);

    let mut data = DataSection::new();
    data.active(
        0,
        &wasm_encoder::ConstExpr::i32_const(REACTOR_PLAN_BASE as i32),
        plan_bytes,
    );
    module.section(&data);

    Ok(module.finish())
}

fn align_u32(value: u32, align: u32) -> u32 {
    if align == 0 {
        return value;
    }
    (value + align - 1) & !(align - 1)
}

fn reactor_trigger_action(trigger: Option<&str>) -> Result<String, DharmaError> {
    let trigger = trigger
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| DharmaError::Validation("reactor trigger missing".to_string()))?;
    if let Some(rest) = trigger.strip_prefix("action.") {
        return Ok(rest.trim().to_string());
    }
    if let Some(rest) = trigger.strip_prefix("action:") {
        return Ok(rest.trim().to_string());
    }
    if let Some(inner) = trigger.strip_prefix("when(") {
        if let Some(end) = inner.find(')') {
            let inside = inner[..end].trim();
            let token = inside.split_whitespace().next().unwrap_or("").trim();
            if let Some(rest) = token.strip_prefix("action.") {
                return Ok(rest.trim().to_string());
            }
            if let Some(rest) = token.strip_prefix("action:") {
                return Ok(rest.trim().to_string());
            }
            if !token.is_empty() {
                return Ok(token.to_string());
            }
        }
    }
    Ok(trigger.to_string())
}

fn is_cron_trigger(trigger: Option<&str>) -> bool {
    let Some(trigger) = trigger else {
        return false;
    };
    let trimmed = trigger.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed.starts_with("Cron(") || trimmed.starts_with("cron(")
}

fn normalize_action_name(action: &str) -> String {
    let trimmed = action.trim();
    if let Some(rest) = trimmed.strip_prefix("action.") {
        return rest.trim().to_string();
    }
    if let Some(rest) = trimmed.strip_prefix("action:") {
        return rest.trim().to_string();
    }
    trimmed.to_string()
}

fn compile_reactor_check(
    reactor: &ReactorDef,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    for check in &reactor.validates {
        compile_bool_expr(&check.value, env, func)?;
        func.instruction(&Instruction::I32Eqz);
        func.instruction(&Instruction::If(BlockType::Empty));
        func.instruction(&Instruction::I32Const(0));
        func.instruction(&Instruction::Return);
        func.instruction(&Instruction::End);
    }
    func.instruction(&Instruction::I32Const(1));
    func.instruction(&Instruction::Return);
    func.instruction(&Instruction::End);
    Ok(())
}

fn compile_reactor_emit(
    emit: &EmitDef,
    layout: &[crate::pdl::schema::LayoutEntry],
    action_index: u32,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let mut provided = HashMap::new();
    for (name, expr) in &emit.args {
        if provided.insert(name.clone(), &expr.value).is_some() {
            return Err(DharmaError::Validation("duplicate emit arg".to_string()));
        }
    }

    push_addr(func, REACTOR_OUT_BASE, 0);
    func.instruction(&Instruction::I32Const(action_index as i32));
    func.instruction(&Instruction::I32Store(wasm_encoder::MemArg {
        offset: 0,
        align: 2,
        memory_index: 0,
    }));

    for entry in layout {
        let field = FieldInfo {
            base: REACTOR_OUT_BASE,
            offset: entry.offset as u32,
            typ: entry.typ.clone(),
        };
        if let Some(expr) = provided.remove(&entry.name) {
            compile_store_field(&field, expr, env, func)?;
        } else if matches!(field.typ, TypeSpec::Optional(_)) {
            store_optional_null(&field, &env._schema.structs, func)?;
        } else {
            return Err(DharmaError::Validation("missing emit arg".to_string()));
        }
    }
    if !provided.is_empty() {
        return Err(DharmaError::Validation("unknown emit arg".to_string()));
    }
    func.instruction(&Instruction::I32Const(1));
    func.instruction(&Instruction::Return);
    func.instruction(&Instruction::End);
    Ok(())
}

fn compile_store_field(
    target: &FieldInfo,
    expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    if let TypeSpec::Optional(inner) = &target.typ {
        if matches!(expr, Expr::Literal(Literal::Null)) {
            return store_optional_null(target, &env._schema.structs, func);
        }
        push_addr(func, target.base, target.offset);
        func.instruction(&Instruction::I32Const(1));
        func.instruction(&Instruction::I32Store8(wasm_encoder::MemArg {
            offset: 0,
            align: 0,
            memory_index: 0,
        }));
        let inner_info = FieldInfo {
            base: target.base,
            offset: target.offset + 1,
            typ: *inner.clone(),
        };
        return compile_assignment_inner(&inner_info, expr, env, func);
    }
    compile_assignment_inner(target, expr, env, func)
}

fn store_optional_null(
    target: &FieldInfo,
    structs: &BTreeMap<String, crate::pdl::schema::StructSchema>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let TypeSpec::Optional(inner) = &target.typ else {
        return Err(DharmaError::Validation("expected optional".to_string()));
    };
    push_addr(func, target.base, target.offset);
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::I32Store8(wasm_encoder::MemArg {
        offset: 0,
        align: 0,
        memory_index: 0,
    }));
    let size = type_size(inner, structs);
    zero_bytes(func, target.base, target.offset + 1, size as u32);
    Ok(())
}

fn compile_validate(
    ast: &AstFile,
    schema: &CqrsSchema,
    public_layout: &[crate::pdl::schema::LayoutEntry],
    private_layout: &[crate::pdl::schema::LayoutEntry],
    action_layouts: &HashMap<String, Vec<crate::pdl::schema::LayoutEntry>>,
    action_order: &[String],
    host: HostFuncs,
    literal_base: u32,
    scratch_base: u32,
) -> Result<Function, DharmaError> {
    let mut func = Function::new(vec![(4, ValType::I32), (2, ValType::I64)]);
    // load action id
    func.instruction(&Instruction::I32Const(ARGS_BASE as i32));
    func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
        offset: 0,
        align: 2,
        memory_index: 0,
    }));
    func.instruction(&Instruction::LocalSet(0));

    for (idx, name) in action_order.iter().enumerate() {
        let action = find_action(ast, name)?;
        let arg_layout = action_layouts
            .get(name)
            .ok_or_else(|| DharmaError::Validation("missing args layout".to_string()))?;
        let env = Env::new(
            schema,
            public_layout,
            private_layout,
            arg_layout,
            ARGS_BASE,
            host,
            literal_base,
            scratch_base,
        );
        func.instruction(&Instruction::LocalGet(0));
        func.instruction(&Instruction::I32Const(idx as i32));
        func.instruction(&Instruction::I32Eq);
        func.instruction(&Instruction::If(BlockType::Empty));
        for check in &action.validates {
            compile_bool_expr(&check.value, &env, &mut func)?;
            func.instruction(&Instruction::I32Eqz);
            func.instruction(&Instruction::If(BlockType::Empty));
            func.instruction(&Instruction::I32Const(1));
            func.instruction(&Instruction::Return);
            func.instruction(&Instruction::End);
        }
        func.instruction(&Instruction::I32Const(0));
        func.instruction(&Instruction::Return);
        func.instruction(&Instruction::End);
    }

    func.instruction(&Instruction::I32Const(1));
    func.instruction(&Instruction::Return);
    func.instruction(&Instruction::End);
    Ok(func)
}

fn compile_reduce(
    ast: &AstFile,
    schema: &CqrsSchema,
    public_layout: &[crate::pdl::schema::LayoutEntry],
    private_layout: &[crate::pdl::schema::LayoutEntry],
    action_layouts: &HashMap<String, Vec<crate::pdl::schema::LayoutEntry>>,
    action_order: &[String],
    host: HostFuncs,
    literal_base: u32,
    scratch_base: u32,
) -> Result<Function, DharmaError> {
    let mut func = Function::new(vec![(4, ValType::I32), (2, ValType::I64)]);
    func.instruction(&Instruction::I32Const(ARGS_BASE as i32));
    func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
        offset: 0,
        align: 2,
        memory_index: 0,
    }));
    func.instruction(&Instruction::LocalSet(0));

    let invariants = ast
        .aggregates
        .first()
        .map(|agg| agg.invariants.as_slice())
        .unwrap_or(&[]);

    for (idx, name) in action_order.iter().enumerate() {
        let action = find_action(ast, name)?;
        let arg_layout = action_layouts
            .get(name)
            .ok_or_else(|| DharmaError::Validation("missing args layout".to_string()))?;
        let env = Env::new(
            schema,
            public_layout,
            private_layout,
            arg_layout,
            ARGS_BASE,
            host,
            literal_base,
            scratch_base,
        );
        func.instruction(&Instruction::LocalGet(0));
        func.instruction(&Instruction::I32Const(idx as i32));
        func.instruction(&Instruction::I32Eq);
        func.instruction(&Instruction::If(BlockType::Empty));
        for assignment in &action.applies {
            compile_assignment(&assignment.value, &env, &mut func)?;
        }
        for invariant in invariants {
            compile_bool_expr(&invariant.value, &env, &mut func)?;
            func.instruction(&Instruction::I32Eqz);
            func.instruction(&Instruction::If(BlockType::Empty));
            func.instruction(&Instruction::I32Const(1));
            func.instruction(&Instruction::Return);
            func.instruction(&Instruction::End);
        }
        func.instruction(&Instruction::I32Const(0));
        func.instruction(&Instruction::Return);
        func.instruction(&Instruction::End);
    }

    func.instruction(&Instruction::I32Const(1));
    func.instruction(&Instruction::Return);
    func.instruction(&Instruction::End);
    Ok(func)
}

fn find_action<'a>(ast: &'a AstFile, name: &str) -> Result<&'a ActionDef, DharmaError> {
    ast.actions
        .iter()
        .find(|a| a.name == name)
        .ok_or_else(|| DharmaError::Validation("action not found".to_string()))
}

#[derive(Clone, Copy, Debug)]
struct HostFuncs {
    has_role_func: Option<u32>,
    read_int_func: Option<u32>,
    read_bool_func: Option<u32>,
    read_text_func: Option<u32>,
    read_identity_func: Option<u32>,
    read_subject_ref_func: Option<u32>,
    subject_id_func: Option<u32>,
    remote_intersects_func: Option<u32>,
    normalize_text_list_func: Option<u32>,
}

impl HostFuncs {
    fn has_role_only(func: Option<u32>) -> Self {
        Self {
            has_role_func: func,
            read_int_func: None,
            read_bool_func: None,
            read_text_func: None,
            read_identity_func: None,
            read_subject_ref_func: None,
            subject_id_func: None,
            remote_intersects_func: None,
            normalize_text_list_func: None,
        }
    }
}

struct Env<'a> {
    state: HashMap<String, FieldInfo>,
    args: HashMap<String, FieldInfo>,
    context: HashMap<String, FieldInfo>,
    _schema: &'a CqrsSchema,
    host: HostFuncs,
    literal_base: u32,
    scratch_base: u32,
    scratch_cursor: Cell<u32>,
}

#[derive(Clone)]
struct FieldInfo {
    base: u32,
    offset: u32,
    typ: TypeSpec,
}

impl<'a> Env<'a> {
    fn new(
        schema: &'a CqrsSchema,
        public_layout: &[crate::pdl::schema::LayoutEntry],
        private_layout: &[crate::pdl::schema::LayoutEntry],
        args_layout: &[crate::pdl::schema::LayoutEntry],
        args_base: u32,
        host: HostFuncs,
        literal_base: u32,
        scratch_base: u32,
    ) -> Self {
        let mut state = HashMap::new();
        for entry in public_layout {
            insert_field(
                &mut state,
                entry.name.clone(),
                FieldInfo {
                    base: STATE_BASE,
                    offset: entry.offset as u32,
                    typ: entry.typ.clone(),
                },
                schema,
            );
        }
        for entry in private_layout {
            insert_field(
                &mut state,
                entry.name.clone(),
                FieldInfo {
                    base: OVERLAY_BASE,
                    offset: entry.offset as u32,
                    typ: entry.typ.clone(),
                },
                schema,
            );
        }
        let mut args = HashMap::new();
        for entry in args_layout {
            insert_field(
                &mut args,
                entry.name.clone(),
                FieldInfo {
                    base: args_base,
                    offset: entry.offset as u32,
                    typ: entry.typ.clone(),
                },
                schema,
            );
        }
        let mut context = HashMap::new();
        context.insert(
            "context.signer".to_string(),
            FieldInfo {
                base: CONTEXT_BASE,
                offset: 0,
                typ: TypeSpec::Identity,
            },
        );
        context.insert(
            "context.clock.time".to_string(),
            FieldInfo {
                base: CONTEXT_BASE,
                offset: 32,
                typ: TypeSpec::Int,
            },
        );
        context.insert(
            "context.timestamp".to_string(),
            FieldInfo {
                base: CONTEXT_BASE,
                offset: 32,
                typ: TypeSpec::Int,
            },
        );
        Self {
            state,
            args,
            context,
            _schema: schema,
            host,
            literal_base,
            scratch_base,
            scratch_cursor: Cell::new(0),
        }
    }

    fn alloc_scratch(&self, size: u32) -> Result<u32, DharmaError> {
        let cursor = self.scratch_cursor.get();
        let next = cursor
            .checked_add(size)
            .ok_or_else(|| DharmaError::Validation("scratch overflow".to_string()))?;
        let max = 0x1000u32;
        if next > max {
            return Err(DharmaError::Validation("scratch overflow".to_string()));
        }
        self.scratch_cursor.set(next);
        Ok(self.scratch_base + cursor)
    }
}

fn insert_field(
    map: &mut HashMap<String, FieldInfo>,
    name: String,
    info: FieldInfo,
    schema: &CqrsSchema,
) {
    map.insert(name.clone(), info.clone());
    let mut offset = info.offset;
    let mut typ = info.typ.clone();
    if let TypeSpec::Optional(inner) = typ {
        offset = offset.saturating_add(1);
        typ = *inner;
    }
    let TypeSpec::Struct(struct_name) = typ else {
        return;
    };
    let Some(def) = schema.structs.get(&struct_name) else {
        return;
    };
    let mut cursor = 0u32;
    for (field_name, field) in &def.fields {
        let child = FieldInfo {
            base: info.base,
            offset: offset + cursor,
            typ: field.typ.clone(),
        };
        insert_field(
            map,
            format!("{name}.{field_name}"),
            child,
            schema,
        );
        cursor += crate::pdl::schema::type_size(&field.typ, &schema.structs) as u32;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExprType {
    Int,
    Bool,
    Bytes(usize),
    Text(usize),
}

fn compile_bool_expr(expr: &Expr, env: &Env<'_>, func: &mut Function) -> Result<(), DharmaError> {
    let expr_type = compile_expr(expr, env, func)?;
    if expr_type != ExprType::Bool {
        return Err(DharmaError::Validation("expected bool expression".to_string()));
    }
    Ok(())
}

fn compile_expr(expr: &Expr, env: &Env<'_>, func: &mut Function) -> Result<ExprType, DharmaError> {
    match expr {
        Expr::Literal(lit) => match lit {
            Literal::Int(value) => {
                func.instruction(&Instruction::I64Const(*value));
                Ok(ExprType::Int)
            }
            Literal::Bool(value) => {
                func.instruction(&Instruction::I32Const(if *value { 1 } else { 0 }));
                Ok(ExprType::Bool)
            }
            Literal::Text(_) => Err(DharmaError::Validation("text unsupported in expression".to_string())),
            Literal::Enum(_) => Err(DharmaError::Validation("enum literal requires context".to_string())),
            Literal::Null => Err(DharmaError::Validation("null unsupported in expression".to_string())),
            Literal::List(_) | Literal::Map(_) | Literal::Struct(_, _) => {
                Err(DharmaError::Validation("collection literal unsupported in expression".to_string()))
            }
        },
        Expr::Path(path) => compile_path_expr(path, env, func),
        Expr::UnaryOp(op, inner) => {
            match op {
                Op::Not => {
                    let t = compile_expr(inner, env, func)?;
                    if t != ExprType::Bool {
                        return Err(DharmaError::Validation("expected bool for !".to_string()));
                    }
                    func.instruction(&Instruction::I32Eqz);
                    Ok(ExprType::Bool)
                }
                Op::Neg => {
                    func.instruction(&Instruction::I64Const(0));
                    let t = compile_expr(inner, env, func)?;
                    if t != ExprType::Int {
                        return Err(DharmaError::Validation("expected int for -".to_string()));
                    }
                    func.instruction(&Instruction::I64Sub);
                    Ok(ExprType::Int)
                }
                _ => Err(DharmaError::Validation("invalid unary op".to_string())),
            }
        }
        Expr::BinaryOp(op, left, right) => match op {
            Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Mod => {
                let left_type = compile_expr(left, env, func)?;
                let right_type = compile_expr(right, env, func)?;
                if left_type != ExprType::Int || right_type != ExprType::Int {
                    return Err(DharmaError::Validation("expected int".to_string()));
                }
                match op {
                    Op::Add => {
                        func.instruction(&Instruction::I64Add);
                    }
                    Op::Sub => {
                        func.instruction(&Instruction::I64Sub);
                    }
                    Op::Mul => {
                        func.instruction(&Instruction::I64Mul);
                    }
                    Op::Div => {
                        func.instruction(&Instruction::I64DivS);
                    }
                    Op::Mod => {
                        func.instruction(&Instruction::I64RemS);
                    }
                    _ => {}
                }
                Ok(ExprType::Int)
            }
            Op::In => {
                compile_in_expr(left, right, env, func)
            }
            Op::Eq | Op::Neq => {
                if let Some((left_info, right_info)) = bytes32_paths(left, right, env)? {
                    return compile_bytes32_eq(op, &left_info, &right_info, func);
                }
                if let Some((path, lit)) = enum_literal_pair(left, right) {
                    let info = resolve_path_info(path, env)?;
                    let info = unwrap_optional_info(info);
                    if let TypeSpec::Enum(variants) = &info.typ {
                        let idx = variants
                            .iter()
                            .position(|v| v == lit)
                            .ok_or_else(|| DharmaError::Validation("unknown enum literal".to_string()))?;
                        func.instruction(&Instruction::I32Const(info.base as i32));
                        func.instruction(&Instruction::I32Const(info.offset as i32));
                        func.instruction(&Instruction::I32Add);
                        func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                            offset: 0,
                            align: 2,
                            memory_index: 0,
                        }));
                        func.instruction(&Instruction::I64ExtendI32U);
                        func.instruction(&Instruction::I64Const(idx as i64));
                        func.instruction(&Instruction::I64Eq);
                        if *op == Op::Neq {
                            func.instruction(&Instruction::I32Eqz);
                        }
                        return Ok(ExprType::Bool);
                    }
                }
                if let Some((text, other)) = text_literal_pair(left, right) {
                    let other_type = compile_expr(other, env, func)?;
                    let ExprType::Text(max) = other_type else {
                        return Err(DharmaError::Validation("text literal comparison expects text".to_string()));
                    };
                    func.instruction(&Instruction::LocalSet(2));
                    let bytes = text_bytes(text, max);
                    compile_mem_eq_const_ptr(func, 2, &bytes);
                    if *op == Op::Neq {
                        func.instruction(&Instruction::I32Eqz);
                    }
                    return Ok(ExprType::Bool);
                }
                if let Some(path) = null_literal_pair(left, right) {
                    return compile_null_eq(op, path, env, func);
                }
                let left_type = compile_expr(left, env, func)?;
                match left_type {
                    ExprType::Int => {
                        func.instruction(&Instruction::LocalSet(4));
                        let right_type = compile_expr(right, env, func)?;
                        if right_type != ExprType::Int {
                            return Err(DharmaError::Validation("invalid eq types".to_string()));
                        }
                        func.instruction(&Instruction::LocalSet(5));
                        func.instruction(&Instruction::LocalGet(4));
                        func.instruction(&Instruction::LocalGet(5));
                        func.instruction(&Instruction::I64Eq);
                    }
                    ExprType::Bool => {
                        func.instruction(&Instruction::LocalSet(2));
                        let right_type = compile_expr(right, env, func)?;
                        if right_type != ExprType::Bool {
                            return Err(DharmaError::Validation("invalid eq types".to_string()));
                        }
                        func.instruction(&Instruction::LocalSet(3));
                        func.instruction(&Instruction::LocalGet(2));
                        func.instruction(&Instruction::LocalGet(3));
                        func.instruction(&Instruction::I32Eq);
                    }
                    ExprType::Bytes(left_len) => {
                        func.instruction(&Instruction::LocalSet(2));
                        let right_type = compile_expr(right, env, func)?;
                        let ExprType::Bytes(right_len) = right_type else {
                            return Err(DharmaError::Validation("invalid eq types".to_string()));
                        };
                        if left_len != right_len {
                            return Err(DharmaError::Validation("bytes width mismatch".to_string()));
                        }
                        func.instruction(&Instruction::LocalSet(3));
                        compile_mem_eq_ptrs(func, left_len);
                    }
                    ExprType::Text(left_max) => {
                        func.instruction(&Instruction::LocalSet(2));
                        let right_type = compile_expr(right, env, func)?;
                        let ExprType::Text(right_max) = right_type else {
                            return Err(DharmaError::Validation("invalid eq types".to_string()));
                        };
                        if left_max != right_max {
                            return Err(DharmaError::Validation("text width mismatch".to_string()));
                        }
                        func.instruction(&Instruction::LocalSet(3));
                        compile_mem_eq_ptrs(func, 4 + left_max);
                    }
                }
                if *op == Op::Neq {
                    func.instruction(&Instruction::I32Eqz);
                }
                Ok(ExprType::Bool)
            }
            Op::Gt | Op::Lt | Op::Gte | Op::Lte => {
                let left_type = compile_expr(left, env, func)?;
                let right_type = compile_expr(right, env, func)?;
                if left_type != ExprType::Int || right_type != ExprType::Int {
                    return Err(DharmaError::Validation("expected int".to_string()));
                }
                if *op == Op::Gt {
                    func.instruction(&Instruction::I64GtS);
                } else if *op == Op::Lt {
                    func.instruction(&Instruction::I64LtS);
                } else if *op == Op::Gte {
                    func.instruction(&Instruction::I64GeS);
                } else {
                    func.instruction(&Instruction::I64LeS);
                }
                Ok(ExprType::Bool)
            }
            Op::And | Op::Or => {
                let left_type = compile_expr(left, env, func)?;
                if left_type != ExprType::Bool {
                    return Err(DharmaError::Validation("expected bool".to_string()));
                }
                if *op == Op::And {
                    func.instruction(&Instruction::I32Eqz);
                    func.instruction(&Instruction::If(BlockType::Result(ValType::I32)));
                    func.instruction(&Instruction::I32Const(0));
                    func.instruction(&Instruction::Else);
                    let right_type = compile_expr(right, env, func)?;
                    if right_type != ExprType::Bool {
                        return Err(DharmaError::Validation("expected bool".to_string()));
                    }
                    func.instruction(&Instruction::End);
                } else {
                    func.instruction(&Instruction::If(BlockType::Result(ValType::I32)));
                    func.instruction(&Instruction::I32Const(1));
                    func.instruction(&Instruction::Else);
                    let right_type = compile_expr(right, env, func)?;
                    if right_type != ExprType::Bool {
                        return Err(DharmaError::Validation("expected bool".to_string()));
                    }
                    func.instruction(&Instruction::End);
                }
                Ok(ExprType::Bool)
            }
            _ => Err(DharmaError::Validation("unsupported binary op".to_string())),
        },
        Expr::Call(name, args) => match name.as_str() {
            "len" => {
                if args.len() != 1 {
                    return Err(DharmaError::Validation("len expects one arg".to_string()));
                }
                match &args[0] {
                    Expr::Literal(Literal::List(items)) => {
                        func.instruction(&Instruction::I64Const(items.len() as i64));
                        Ok(ExprType::Int)
                    }
                    Expr::Literal(Literal::Map(items)) => {
                        func.instruction(&Instruction::I64Const(items.len() as i64));
                        Ok(ExprType::Int)
                    }
                    Expr::Path(path) => {
                        let info = resolve_path_info(path, env)?;
                        let info = unwrap_optional_info(info);
                        match info.typ {
                            TypeSpec::Text(_) | TypeSpec::Currency | TypeSpec::List(_) | TypeSpec::Map(_, _) => {
                                emit_len_load(&info, func);
                                func.instruction(&Instruction::I64ExtendI32U);
                                Ok(ExprType::Int)
                            }
                            _ => Err(DharmaError::Validation("len requires text or collection".to_string())),
                        }
                    }
                    _ => Err(DharmaError::Validation("len expects path or literal collection".to_string())),
                }
            }
            "contains" => {
                if args.len() != 2 {
                    return Err(DharmaError::Validation("contains expects two args".to_string()));
                }
                compile_contains_expr(&args[0], &args[1], env, func)
            }
            "index" | "get" => {
                if args.len() != 2 {
                    return Err(DharmaError::Validation("index expects two args".to_string()));
                }
                compile_index_expr(&args[0], &args[1], env, func)
            }
            "has_role" => {
                compile_has_role(args, env, func)
            }
            "now" => {
                let info = env
                    .context
                    .get("context.clock.time")
                    .ok_or_else(|| DharmaError::Validation("missing context time".to_string()))?;
                func.instruction(&Instruction::I32Const(info.base as i32));
                func.instruction(&Instruction::I32Const(info.offset as i32));
                func.instruction(&Instruction::I32Add);
                func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
                    offset: 0,
                    align: 3,
                    memory_index: 0,
                }));
                Ok(ExprType::Int)
            }
            "days_between" => compile_days_between_expr(args, env, func, false),
            "days_until" => compile_days_between_expr(args, env, func, true),
            "distance" => {
                if args.len() != 2 {
                    return Err(DharmaError::Validation("distance expects two args".to_string()));
                }
                compile_distance_expr(&args[0], &args[1], env, func)
            }
            "sum" => {
                if args.len() != 1 {
                    return Err(DharmaError::Validation("sum expects one arg".to_string()));
                }
                compile_sum_expr(&args[0], env, func)
            }
            "read_int" => compile_read_int(args, env, func),
            "read_bool" => compile_read_bool(args, env, func),
            "read_text" => compile_read_text(args, env, func),
            "read_identity" | "read_ref" => compile_read_identity(args, env, func),
            "read_subject_ref" => compile_read_subject_ref(args, env, func),
            "subject_id" => compile_subject_id(args, env, func),
            "intersects" => compile_intersects(args, env, func),
            "subset" => compile_subset(args, env, func),
            "remote_intersects" => compile_remote_intersects(args, env, func),
            _ => Err(DharmaError::Validation("unknown function".to_string())),
        },
    }
}

fn compile_days_between_expr(
    args: &[Expr],
    env: &Env<'_>,
    func: &mut Function,
    swap: bool,
) -> Result<ExprType, DharmaError> {
    if args.len() != 2 {
        return Err(DharmaError::Validation(
            "days_between expects two args".to_string(),
        ));
    }
    let (start, end) = if swap { (&args[1], &args[0]) } else { (&args[0], &args[1]) };
    let end_type = compile_expr(end, env, func)?;
    let start_type = compile_expr(start, env, func)?;
    if end_type != ExprType::Int || start_type != ExprType::Int {
        return Err(DharmaError::Validation(
            "days_between expects numeric args".to_string(),
        ));
    }
    // diff = end - start
    func.instruction(&Instruction::I64Sub);
    // floor divide by 86_400 (seconds per day)
    func.instruction(&Instruction::LocalSet(4));
    func.instruction(&Instruction::LocalGet(4));
    func.instruction(&Instruction::I64Const(0));
    func.instruction(&Instruction::I64GeS);
    func.instruction(&Instruction::If(BlockType::Result(ValType::I64)));
    func.instruction(&Instruction::LocalGet(4));
    func.instruction(&Instruction::I64Const(86_400));
    func.instruction(&Instruction::I64DivS);
    func.instruction(&Instruction::Else);
    func.instruction(&Instruction::LocalGet(4));
    func.instruction(&Instruction::I64Const(86_399));
    func.instruction(&Instruction::I64Sub);
    func.instruction(&Instruction::I64Const(86_400));
    func.instruction(&Instruction::I64DivS);
    func.instruction(&Instruction::End);
    Ok(ExprType::Int)
}

fn compile_has_role(args: &[Expr], env: &Env<'_>, func: &mut Function) -> Result<ExprType, DharmaError> {
    if !(args.len() == 2 || args.len() == 3) {
        return Err(DharmaError::Validation(
            "has_role expects two or three args".to_string(),
        ));
    }
    let Some(has_role_func) = env.host.has_role_func else {
        func.instruction(&Instruction::I32Const(1));
        return Ok(ExprType::Bool);
    };

    let (subject_expr, identity_expr, role_expr) = if args.len() == 2 {
        (&args[0], &args[0], &args[1])
    } else {
        (&args[0], &args[1], &args[2])
    };

    let subject_type = compile_expr(subject_expr, env, func)?;
    match subject_type {
        ExprType::Bytes(32) => {}
        _ => return Err(DharmaError::Validation("has_role expects identity".to_string())),
    }

    let identity_type = compile_expr(identity_expr, env, func)?;
    match identity_type {
        ExprType::Bytes(32) => {}
        _ => return Err(DharmaError::Validation("has_role expects identity".to_string())),
    }

    match role_expr {
        Expr::Literal(Literal::Text(text)) => {
            if text.len() > DEFAULT_TEXT_LEN {
                return Err(DharmaError::Validation("role literal too long".to_string()));
            }
            let bytes = text_bytes(text, DEFAULT_TEXT_LEN);
            write_const_bytes(func, env.literal_base, &bytes);
            func.instruction(&Instruction::I32Const(env.literal_base as i32));
        }
        Expr::Literal(Literal::Enum(text)) => {
            if text.len() > DEFAULT_TEXT_LEN {
                return Err(DharmaError::Validation("role literal too long".to_string()));
            }
            let bytes = text_bytes(text, DEFAULT_TEXT_LEN);
            write_const_bytes(func, env.literal_base, &bytes);
            func.instruction(&Instruction::I32Const(env.literal_base as i32));
        }
        _ => {
            let role_type = compile_expr(role_expr, env, func)?;
            match role_type {
                ExprType::Text(_) => {}
                _ => {
                    return Err(DharmaError::Validation(
                        "has_role expects text role".to_string(),
                    ))
                }
            }
        }
    }

    func.instruction(&Instruction::Call(has_role_func));
    Ok(ExprType::Bool)
}

const ELEM_KIND_TEXT: i32 = 1;
const ELEM_KIND_IDENTITY: i32 = 2;
const ELEM_KIND_SUBJECT_REF: i32 = 3;

fn compile_path_literal_arg(expr: &Expr, env: &Env<'_>, func: &mut Function) -> Result<u32, DharmaError> {
    let text = match expr {
        Expr::Literal(Literal::Text(text)) => text,
        Expr::Literal(Literal::Enum(text)) => text,
        _ => {
            return Err(DharmaError::Validation(
                "path must be literal".to_string(),
            ))
        }
    };
    if text.len() > MAX_PATH_LEN {
        return Err(DharmaError::Validation("path literal too long".to_string()));
    }
    let bytes = text_bytes(text, MAX_PATH_LEN);
    write_const_bytes(func, env.literal_base, &bytes);
    func.instruction(&Instruction::I32Const(env.literal_base as i32));
    Ok(env.literal_base)
}

fn compile_read_int(args: &[Expr], env: &Env<'_>, func: &mut Function) -> Result<ExprType, DharmaError> {
    if args.len() != 2 {
        return Err(DharmaError::Validation("read_int expects two args".to_string()));
    }
    let subject_type = compile_expr(&args[0], env, func)?;
    if subject_type != ExprType::Bytes(40) {
        return Err(DharmaError::Validation("read_int expects subject_ref".to_string()));
    }
    let _ = compile_path_literal_arg(&args[1], env, func)?;
    let Some(read_int) = env.host.read_int_func else {
        return Err(DharmaError::Validation("read_int unsupported".to_string()));
    };
    func.instruction(&Instruction::Call(read_int));
    Ok(ExprType::Int)
}

fn compile_read_bool(args: &[Expr], env: &Env<'_>, func: &mut Function) -> Result<ExprType, DharmaError> {
    if args.len() != 2 {
        return Err(DharmaError::Validation("read_bool expects two args".to_string()));
    }
    let subject_type = compile_expr(&args[0], env, func)?;
    if subject_type != ExprType::Bytes(40) {
        return Err(DharmaError::Validation("read_bool expects subject_ref".to_string()));
    }
    let _ = compile_path_literal_arg(&args[1], env, func)?;
    let Some(read_bool) = env.host.read_bool_func else {
        return Err(DharmaError::Validation("read_bool unsupported".to_string()));
    };
    func.instruction(&Instruction::Call(read_bool));
    Ok(ExprType::Bool)
}

fn compile_read_text(args: &[Expr], env: &Env<'_>, func: &mut Function) -> Result<ExprType, DharmaError> {
    if args.len() != 2 {
        return Err(DharmaError::Validation("read_text expects two args".to_string()));
    }
    let subject_type = compile_expr(&args[0], env, func)?;
    if subject_type != ExprType::Bytes(40) {
        return Err(DharmaError::Validation("read_text expects subject_ref".to_string()));
    }
    let _ = compile_path_literal_arg(&args[1], env, func)?;
    let scratch = env.alloc_scratch((4 + DEFAULT_TEXT_LEN) as u32)?;
    func.instruction(&Instruction::I32Const(scratch as i32));
    let Some(read_text) = env.host.read_text_func else {
        return Err(DharmaError::Validation("read_text unsupported".to_string()));
    };
    func.instruction(&Instruction::Call(read_text));
    Ok(ExprType::Text(DEFAULT_TEXT_LEN))
}

fn compile_read_identity(args: &[Expr], env: &Env<'_>, func: &mut Function) -> Result<ExprType, DharmaError> {
    if args.len() != 2 {
        return Err(DharmaError::Validation("read_identity expects two args".to_string()));
    }
    let subject_type = compile_expr(&args[0], env, func)?;
    if subject_type != ExprType::Bytes(40) {
        return Err(DharmaError::Validation("read_identity expects subject_ref".to_string()));
    }
    let _ = compile_path_literal_arg(&args[1], env, func)?;
    let scratch = env.alloc_scratch(32)?;
    func.instruction(&Instruction::I32Const(scratch as i32));
    let Some(read_identity) = env.host.read_identity_func else {
        return Err(DharmaError::Validation("read_identity unsupported".to_string()));
    };
    func.instruction(&Instruction::Call(read_identity));
    Ok(ExprType::Bytes(32))
}

fn compile_subject_id(args: &[Expr], env: &Env<'_>, func: &mut Function) -> Result<ExprType, DharmaError> {
    if args.len() != 1 {
        return Err(DharmaError::Validation("subject_id expects one arg".to_string()));
    }
    let subject_type = compile_expr(&args[0], env, func)?;
    if subject_type != ExprType::Bytes(40) {
        return Err(DharmaError::Validation("subject_id expects subject_ref".to_string()));
    }
    let scratch = env.alloc_scratch(32)?;
    func.instruction(&Instruction::I32Const(scratch as i32));
    let Some(subject_id) = env.host.subject_id_func else {
        return Err(DharmaError::Validation("subject_id unsupported".to_string()));
    };
    func.instruction(&Instruction::Call(subject_id));
    Ok(ExprType::Bytes(32))
}

fn compile_read_subject_ref(
    args: &[Expr],
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    if args.len() != 2 {
        return Err(DharmaError::Validation(
            "read_subject_ref expects two args".to_string(),
        ));
    }
    let subject_type = compile_expr(&args[0], env, func)?;
    if subject_type != ExprType::Bytes(40) {
        return Err(DharmaError::Validation(
            "read_subject_ref expects subject_ref".to_string(),
        ));
    }
    let _ = compile_path_literal_arg(&args[1], env, func)?;
    let scratch = env.alloc_scratch(40)?;
    func.instruction(&Instruction::I32Const(scratch as i32));
    let Some(read_subject_ref) = env.host.read_subject_ref_func else {
        return Err(DharmaError::Validation("read_subject_ref unsupported".to_string()));
    };
    func.instruction(&Instruction::Call(read_subject_ref));
    Ok(ExprType::Bytes(40))
}

fn compile_path_expr(path: &[String], env: &Env<'_>, func: &mut Function) -> Result<ExprType, DharmaError> {
    let info = resolve_path_info(path, env)?;
    let info = unwrap_optional_info(info);
    match info.typ {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
                offset: 0,
                align: 3,
                memory_index: 0,
            }));
            Ok(ExprType::Int)
        }
        TypeSpec::Bool => {
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
                offset: 0,
                align: 0,
                memory_index: 0,
            }));
            Ok(ExprType::Bool)
        }
        TypeSpec::Enum(_) => {
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I64ExtendI32U);
            Ok(ExprType::Int)
        }
        TypeSpec::Text(len) => {
            let max = len.unwrap_or(DEFAULT_TEXT_LEN);
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            Ok(ExprType::Text(max))
        }
        TypeSpec::Currency => {
            let max = DEFAULT_TEXT_LEN;
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            Ok(ExprType::Text(max))
        }
        TypeSpec::Identity | TypeSpec::Ref(_) => {
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            Ok(ExprType::Bytes(32))
        }
        TypeSpec::SubjectRef(_) => {
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            Ok(ExprType::Bytes(40))
        }
        TypeSpec::Ratio => {
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            Ok(ExprType::Bytes(16))
        }
        TypeSpec::GeoPoint => {
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            Ok(ExprType::Bytes(8))
        }
        TypeSpec::Struct(_) => Err(DharmaError::Validation("struct unsupported in expression".to_string())),
        TypeSpec::List(_) | TypeSpec::Map(_, _) => Err(DharmaError::Validation("collection unsupported in expression".to_string())),
        TypeSpec::Optional(_) => Err(DharmaError::Validation("optional unsupported in expression".to_string())),
    }
}

fn compile_in_expr(
    left: &Expr,
    right: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    match right {
        Expr::Literal(Literal::List(items)) => compile_in_list(left, items, env, func),
        Expr::Path(path) => compile_contains_path(path, left, env, func),
        _ => Err(DharmaError::Validation("in requires list literal or list path".to_string())),
    }
}

fn compile_contains_expr(
    list_expr: &Expr,
    item_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    match list_expr {
        Expr::Literal(Literal::List(items)) => compile_in_list(item_expr, items, env, func),
        Expr::Path(path) => compile_contains_path(path, item_expr, env, func),
        _ => Err(DharmaError::Validation("contains requires list literal or list path".to_string())),
    }
}

fn compile_index_expr(
    collection: &Expr,
    index: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    match collection {
        Expr::Literal(Literal::List(items)) => {
            let idx = literal_index(index)?;
            let expr = items
                .get(idx)
                .ok_or_else(|| DharmaError::Validation("index out of bounds".to_string()))?;
            compile_expr(expr, env, func)
        }
        Expr::Literal(Literal::Map(entries)) => {
            let key = literal_key(index)?;
            for (k, v) in entries {
                if let Some(k_lit) = expr_literal(k) {
                    if literal_eq(k_lit, &key) {
                        return compile_expr(v, env, func);
                    }
                }
            }
            Err(DharmaError::Validation("map key not found".to_string()))
        }
        Expr::Path(path) => {
            let info = resolve_path_info(path, env)?;
            let info = unwrap_optional_info(info);
            match &info.typ {
        TypeSpec::List(inner) => compile_list_index(&info, inner, index, env, func),
        TypeSpec::Map(key, val) => compile_map_index(&info, key, val, index, env, func),
        _ => Err(DharmaError::Validation("indexing on non-collection unsupported".to_string())),
    }
}
        _ => Err(DharmaError::Validation("indexing on non-literal unsupported".to_string())),
    }
}

fn compile_distance_expr(
    left: &Expr,
    right: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    let left_info = resolve_geopoint_info(left, env)?;
    let right_info = resolve_geopoint_info(right, env)?;
    // dlat = abs(lat1 - lat2)
    emit_geopoint_component(&left_info, 0, func);
    emit_geopoint_component(&right_info, 0, func);
    func.instruction(&Instruction::I64Sub);
    emit_abs_i64(func, 4);
    func.instruction(&Instruction::LocalSet(4));
    // dlon = abs(lon1 - lon2)
    emit_geopoint_component(&left_info, 4, func);
    emit_geopoint_component(&right_info, 4, func);
    func.instruction(&Instruction::I64Sub);
    emit_abs_i64(func, 5);
    func.instruction(&Instruction::LocalSet(5));
    // (dlat + dlon) * 111_320 / 10_000_000
    func.instruction(&Instruction::LocalGet(4));
    func.instruction(&Instruction::LocalGet(5));
    func.instruction(&Instruction::I64Add);
    func.instruction(&Instruction::I64Const(111_320));
    func.instruction(&Instruction::I64Mul);
    func.instruction(&Instruction::I64Const(10_000_000));
    func.instruction(&Instruction::I64DivS);
    Ok(ExprType::Int)
}

fn compile_sum_expr(expr: &Expr, env: &Env<'_>, func: &mut Function) -> Result<ExprType, DharmaError> {
    match expr {
        Expr::Literal(Literal::List(items)) => {
            let mut total: i64 = 0;
            for item in items {
                if let Expr::Literal(Literal::Int(value)) = item {
                    total = total
                        .checked_add(*value)
                        .ok_or_else(|| DharmaError::Validation("sum overflow".to_string()))?;
                } else {
                    return Err(DharmaError::Validation("sum literal expects int list".to_string()));
                }
            }
            func.instruction(&Instruction::I64Const(total));
            Ok(ExprType::Int)
        }
        Expr::Path(path) => {
            let info = resolve_path_info(path, env)?;
            let info = unwrap_optional_info(info);
            match &info.typ {
                TypeSpec::List(inner) => compile_sum_list(&info, inner, env, func),
                _ => Err(DharmaError::Validation("sum expects list".to_string())),
            }
        }
        _ => Err(DharmaError::Validation("sum expects list literal or path".to_string())),
    }
}

fn compile_intersects(args: &[Expr], env: &Env<'_>, func: &mut Function) -> Result<ExprType, DharmaError> {
    if args.len() != 2 {
        return Err(DharmaError::Validation("intersects expects two args".to_string()));
    }
    match (&args[0], &args[1]) {
        (Expr::Literal(Literal::List(left)), Expr::Literal(Literal::List(right))) => {
            let mut hit = false;
            for l in left {
                if let Some(lit_l) = expr_literal(l) {
                    for r in right {
                        if let Some(lit_r) = expr_literal(r) {
                            if literal_eq(lit_l, lit_r) {
                                hit = true;
                                break;
                            }
                        }
                    }
                }
                if hit {
                    break;
                }
            }
            func.instruction(&Instruction::I32Const(if hit { 1 } else { 0 }));
            Ok(ExprType::Bool)
        }
        (Expr::Literal(Literal::List(items)), Expr::Path(path))
        | (Expr::Path(path), Expr::Literal(Literal::List(items))) => {
            if items.is_empty() {
                func.instruction(&Instruction::I32Const(0));
                return Ok(ExprType::Bool);
            }
            let mut first = true;
            for item in items {
                compile_contains_path(path, item, env, func)?;
                if first {
                    first = false;
                } else {
                    func.instruction(&Instruction::I32Or);
                }
            }
            Ok(ExprType::Bool)
        }
        (Expr::Path(left), Expr::Path(right)) => {
            let left_info = unwrap_optional_info(resolve_path_info(left, env)?);
            let right_info = unwrap_optional_info(resolve_path_info(right, env)?);
            let TypeSpec::List(left_elem) = &left_info.typ else {
                return Err(DharmaError::Validation("intersects expects list".to_string()));
            };
            let TypeSpec::List(right_elem) = &right_info.typ else {
                return Err(DharmaError::Validation("intersects expects list".to_string()));
            };
            if left_elem.as_ref() != right_elem.as_ref() {
                return Err(DharmaError::Validation("intersects list type mismatch".to_string()));
            }
            compile_list_intersects_paths(&left_info, &right_info, left_elem, env, func)
        }
        _ => Err(DharmaError::Validation("intersects expects lists".to_string())),
    }
}

fn compile_subset(args: &[Expr], env: &Env<'_>, func: &mut Function) -> Result<ExprType, DharmaError> {
    if args.len() != 2 {
        return Err(DharmaError::Validation("subset expects two args".to_string()));
    }
    match (&args[0], &args[1]) {
        (Expr::Literal(Literal::List(left)), Expr::Literal(Literal::List(right))) => {
            let mut ok = true;
            for l in left {
                let mut found = false;
                if let Some(lit_l) = expr_literal(l) {
                    for r in right {
                        if let Some(lit_r) = expr_literal(r) {
                            if literal_eq(lit_l, lit_r) {
                                found = true;
                                break;
                            }
                        }
                    }
                }
                if !found {
                    ok = false;
                    break;
                }
            }
            func.instruction(&Instruction::I32Const(if ok { 1 } else { 0 }));
            Ok(ExprType::Bool)
        }
        (Expr::Literal(Literal::List(items)), Expr::Path(path)) => {
            if items.is_empty() {
                func.instruction(&Instruction::I32Const(1));
                return Ok(ExprType::Bool);
            }
            let mut first = true;
            for item in items {
                compile_contains_path(path, item, env, func)?;
                if first {
                    first = false;
                } else {
                    func.instruction(&Instruction::I32And);
                }
            }
            Ok(ExprType::Bool)
        }
        (Expr::Path(path), Expr::Literal(Literal::List(items))) => {
            let info = unwrap_optional_info(resolve_path_info(path, env)?);
            let TypeSpec::List(elem_type) = &info.typ else {
                return Err(DharmaError::Validation("subset expects list".to_string()));
            };
            compile_list_subset_path_literal(&info, elem_type, items, env, func)
        }
        (Expr::Path(left), Expr::Path(right)) => {
            let left_info = unwrap_optional_info(resolve_path_info(left, env)?);
            let right_info = unwrap_optional_info(resolve_path_info(right, env)?);
            let TypeSpec::List(left_elem) = &left_info.typ else {
                return Err(DharmaError::Validation("subset expects list".to_string()));
            };
            let TypeSpec::List(right_elem) = &right_info.typ else {
                return Err(DharmaError::Validation("subset expects list".to_string()));
            };
            if left_elem.as_ref() != right_elem.as_ref() {
                return Err(DharmaError::Validation("subset list type mismatch".to_string()));
            }
            compile_list_subset_paths(&left_info, &right_info, left_elem, env, func)
        }
        _ => Err(DharmaError::Validation("subset expects lists".to_string())),
    }
}

fn compile_remote_intersects(
    args: &[Expr],
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    if args.len() != 3 {
        return Err(DharmaError::Validation(
            "remote_intersects expects three args".to_string(),
        ));
    }
    let subject_type = compile_expr(&args[0], env, func)?;
    if subject_type != ExprType::Bytes(40) {
        return Err(DharmaError::Validation(
            "remote_intersects expects subject_ref".to_string(),
        ));
    }
    let _ = compile_path_literal_arg(&args[1], env, func)?;
    let Expr::Path(path) = &args[2] else {
        return Err(DharmaError::Validation(
            "remote_intersects expects list path".to_string(),
        ));
    };
    let info = unwrap_optional_info(resolve_path_info(path, env)?);
    let TypeSpec::List(elem) = &info.typ else {
        return Err(DharmaError::Validation(
            "remote_intersects expects list path".to_string(),
        ));
    };
    let (kind, size) = remote_elem_kind(elem, env)?;
    func.instruction(&Instruction::I32Const(info.base as i32));
    func.instruction(&Instruction::I32Const(info.offset as i32));
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::I32Const(kind));
    func.instruction(&Instruction::I32Const(size as i32));
    let Some(remote_intersects) = env.host.remote_intersects_func else {
        return Err(DharmaError::Validation(
            "remote_intersects unsupported".to_string(),
        ));
    };
    func.instruction(&Instruction::Call(remote_intersects));
    Ok(ExprType::Bool)
}

fn remote_elem_kind(elem: &TypeSpec, env: &Env<'_>) -> Result<(i32, usize), DharmaError> {
    match elem {
        TypeSpec::Text(_) | TypeSpec::Currency => Ok((ELEM_KIND_TEXT, type_size(elem, &env._schema.structs))),
        TypeSpec::Identity | TypeSpec::Ref(_) => Ok((ELEM_KIND_IDENTITY, 32)),
        TypeSpec::SubjectRef(_) => Ok((ELEM_KIND_SUBJECT_REF, 40)),
        _ => Err(DharmaError::Validation(
            "remote_intersects expects list<text|identity|subject_ref>".to_string(),
        )),
    }
}

fn compile_sum_list(
    list_info: &FieldInfo,
    elem_type: &TypeSpec,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    if !matches!(
        elem_type,
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp
    ) {
        return Err(DharmaError::Validation("sum expects list<int>".to_string()));
    }
    let cap = list_capacity(elem_type, &env._schema.structs);
    func.instruction(&Instruction::I64Const(0));
    func.instruction(&Instruction::LocalSet(4));
    if cap == 0 {
        func.instruction(&Instruction::LocalGet(4));
        return Ok(ExprType::Int);
    }
    for idx in 0..cap {
        func.instruction(&Instruction::I64Const(idx as i64));
        emit_len_load(list_info, func);
        func.instruction(&Instruction::I64ExtendI32U);
        func.instruction(&Instruction::I64LtU);
        func.instruction(&Instruction::If(BlockType::Empty));
        func.instruction(&Instruction::LocalGet(4));
        emit_list_elem_load(list_info, elem_type, idx, &env._schema.structs, func)?;
        func.instruction(&Instruction::I64Add);
        func.instruction(&Instruction::LocalSet(4));
        func.instruction(&Instruction::End);
    }
    func.instruction(&Instruction::LocalGet(4));
    Ok(ExprType::Int)
}

fn emit_list_elem_load(
    list_info: &FieldInfo,
    elem_type: &TypeSpec,
    idx: usize,
    structs: &BTreeMap<String, crate::pdl::schema::StructSchema>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let elem_size = type_size(elem_type, structs);
    let elem_offset = list_info.offset as usize + 4 + idx * elem_size;
    push_addr(func, list_info.base, elem_offset as u32);
    func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
        offset: 0,
        align: 3,
        memory_index: 0,
    }));
    Ok(())
}

fn resolve_geopoint_info(expr: &Expr, env: &Env<'_>) -> Result<FieldInfo, DharmaError> {
    let Expr::Path(path) = expr else {
        return Err(DharmaError::Validation("distance expects geopoint path".to_string()));
    };
    let info = resolve_path_info(path, env)?;
    let info = unwrap_optional_info(info);
    if !matches!(info.typ, TypeSpec::GeoPoint) {
        return Err(DharmaError::Validation("distance expects geopoint".to_string()));
    }
    Ok(info)
}

fn emit_geopoint_component(info: &FieldInfo, offset: u32, func: &mut Function) {
    func.instruction(&Instruction::I32Const(info.base as i32));
    func.instruction(&Instruction::I32Const((info.offset + offset) as i32));
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
        offset: 0,
        align: 2,
        memory_index: 0,
    }));
    func.instruction(&Instruction::I64ExtendI32S);
}

fn emit_abs_i64(func: &mut Function, tmp_local: u32) {
    func.instruction(&Instruction::LocalSet(tmp_local));
    func.instruction(&Instruction::LocalGet(tmp_local));
    func.instruction(&Instruction::I64Const(0));
    func.instruction(&Instruction::I64LtS);
    func.instruction(&Instruction::If(BlockType::Result(ValType::I64)));
    func.instruction(&Instruction::I64Const(0));
    func.instruction(&Instruction::LocalGet(tmp_local));
    func.instruction(&Instruction::I64Sub);
    func.instruction(&Instruction::Else);
    func.instruction(&Instruction::LocalGet(tmp_local));
    func.instruction(&Instruction::End);
}

fn emit_len_load(info: &FieldInfo, func: &mut Function) {
    func.instruction(&Instruction::I32Const(info.base as i32));
    func.instruction(&Instruction::I32Const(info.offset as i32));
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
        offset: 0,
        align: 2,
        memory_index: 0,
    }));
}

fn compile_contains_path(
    path: &[String],
    item_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    let info = resolve_path_info(path, env)?;
    let info = unwrap_optional_info(info);
    match &info.typ {
        TypeSpec::List(inner) => compile_list_contains(&info, inner, item_expr, env, func),
        TypeSpec::Map(key, val) => compile_map_contains(&info, key, val, item_expr, env, func),
        _ => Err(DharmaError::Validation("contains requires list or map path".to_string())),
    }
}

fn compile_list_contains(
    list_info: &FieldInfo,
    elem_type: &TypeSpec,
    item_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    let cap = list_capacity(elem_type, &env._schema.structs);
    if cap == 0 {
        func.instruction(&Instruction::I32Const(0));
        return Ok(ExprType::Bool);
    }
    let mut first = true;
    for idx in 0..cap {
        func.instruction(&Instruction::I64Const(idx as i64));
        emit_len_load(list_info, func);
        func.instruction(&Instruction::I64ExtendI32U);
        func.instruction(&Instruction::I64LtU);
        compile_list_elem_eq(list_info, elem_type, idx, item_expr, env, func)?;
        func.instruction(&Instruction::I32And);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32Or);
        }
    }
    Ok(ExprType::Bool)
}

fn compile_list_intersects_paths(
    left_info: &FieldInfo,
    right_info: &FieldInfo,
    elem_type: &TypeSpec,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    let cap_left = list_capacity(elem_type, &env._schema.structs);
    let cap_right = list_capacity(elem_type, &env._schema.structs);
    if cap_left == 0 || cap_right == 0 {
        func.instruction(&Instruction::I32Const(0));
        return Ok(ExprType::Bool);
    }
    let mut first_outer = true;
    for idx_left in 0..cap_left {
        func.instruction(&Instruction::I32Const(idx_left as i32));
        emit_len_load(left_info, func);
        func.instruction(&Instruction::I32LtU);
        func.instruction(&Instruction::If(BlockType::Result(ValType::I32)));
        let mut first_inner = true;
        for idx_right in 0..cap_right {
            emit_idx_in_len(right_info, idx_right, func);
            compile_list_elem_eq_offsets(left_info, right_info, idx_left, idx_right, elem_type, env, func)?;
            func.instruction(&Instruction::I32And);
            if first_inner {
                first_inner = false;
            } else {
                func.instruction(&Instruction::I32Or);
            }
        }
        if first_inner {
            func.instruction(&Instruction::I32Const(0));
        }
        func.instruction(&Instruction::Else);
        func.instruction(&Instruction::I32Const(0));
        func.instruction(&Instruction::End);
        if first_outer {
            first_outer = false;
        } else {
            func.instruction(&Instruction::I32Or);
        }
    }
    Ok(ExprType::Bool)
}

fn compile_list_subset_paths(
    left_info: &FieldInfo,
    right_info: &FieldInfo,
    elem_type: &TypeSpec,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    let cap_left = list_capacity(elem_type, &env._schema.structs);
    let cap_right = list_capacity(elem_type, &env._schema.structs);
    if cap_left == 0 {
        func.instruction(&Instruction::I32Const(1));
        return Ok(ExprType::Bool);
    }
    let mut first_outer = true;
    for idx_left in 0..cap_left {
        func.instruction(&Instruction::I32Const(idx_left as i32));
        emit_len_load(left_info, func);
        func.instruction(&Instruction::I32LtU);
        func.instruction(&Instruction::If(BlockType::Result(ValType::I32)));
        let mut first_inner = true;
        for idx_right in 0..cap_right {
            emit_idx_in_len(right_info, idx_right, func);
            compile_list_elem_eq_offsets(left_info, right_info, idx_left, idx_right, elem_type, env, func)?;
            func.instruction(&Instruction::I32And);
            if first_inner {
                first_inner = false;
            } else {
                func.instruction(&Instruction::I32Or);
            }
        }
        if first_inner {
            func.instruction(&Instruction::I32Const(0));
        }
        func.instruction(&Instruction::Else);
        func.instruction(&Instruction::I32Const(1));
        func.instruction(&Instruction::End);
        if first_outer {
            first_outer = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
    }
    Ok(ExprType::Bool)
}

fn compile_list_subset_path_literal(
    list_info: &FieldInfo,
    elem_type: &TypeSpec,
    items: &[Expr],
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    let cap = list_capacity(elem_type, &env._schema.structs);
    if cap == 0 {
        func.instruction(&Instruction::I32Const(1));
        return Ok(ExprType::Bool);
    }
    let mut first_outer = true;
    for idx in 0..cap {
        func.instruction(&Instruction::I32Const(idx as i32));
        emit_len_load(list_info, func);
        func.instruction(&Instruction::I32LtU);
        func.instruction(&Instruction::If(BlockType::Result(ValType::I32)));
        let mut first_inner = true;
        for item in items {
            compile_list_elem_eq(list_info, elem_type, idx, item, env, func)?;
            if first_inner {
                first_inner = false;
            } else {
                func.instruction(&Instruction::I32Or);
            }
        }
        if first_inner {
            func.instruction(&Instruction::I32Const(0));
        }
        func.instruction(&Instruction::Else);
        func.instruction(&Instruction::I32Const(1));
        func.instruction(&Instruction::End);
        if first_outer {
            first_outer = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
    }
    Ok(ExprType::Bool)
}

fn compile_map_contains(
    map_info: &FieldInfo,
    key_type: &TypeSpec,
    value_type: &TypeSpec,
    key_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    let cap = map_capacity(key_type, value_type, &env._schema.structs);
    if cap == 0 {
        func.instruction(&Instruction::I32Const(0));
        return Ok(ExprType::Bool);
    }
    let mut first = true;
    for idx in 0..cap {
        emit_idx_in_len(map_info, idx, func);
        compile_map_key_eq(map_info, key_type, value_type, key_expr, idx, env, func)?;
        func.instruction(&Instruction::I32And);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32Or);
        }
    }
    Ok(ExprType::Bool)
}

fn emit_idx_in_len(map_info: &FieldInfo, idx: usize, func: &mut Function) {
    func.instruction(&Instruction::I32Const(idx as i32));
    emit_len_load(map_info, func);
    func.instruction(&Instruction::I32LtU);
}

fn compile_map_key_eq(
    map_info: &FieldInfo,
    key_type: &TypeSpec,
    value_type: &TypeSpec,
    key_expr: &Expr,
    idx: usize,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let key_size = type_size(key_type, &env._schema.structs);
    let entry_size = key_size + type_size(value_type, &env._schema.structs);
    let key_offset = map_info.offset as usize + 4 + idx * entry_size;
    match key_type {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            push_addr(func, map_info.base, key_offset as u32);
            func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
                offset: 0,
                align: 3,
                memory_index: 0,
            }));
            compile_item_expr_int(key_expr, key_type, env, func)?;
            func.instruction(&Instruction::I64Eq);
        }
        TypeSpec::Enum(variants) => {
            push_addr(func, map_info.base, key_offset as u32);
            func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I64ExtendI32U);
            compile_item_expr_enum(key_expr, variants, env, func)?;
            func.instruction(&Instruction::I64Eq);
        }
        TypeSpec::Bool => {
            push_addr(func, map_info.base, key_offset as u32);
            func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
                offset: 0,
                align: 0,
                memory_index: 0,
            }));
            let typ = compile_expr(key_expr, env, func)?;
            if typ != ExprType::Bool {
                return Err(DharmaError::Validation("map key expects bool".to_string()));
            }
            func.instruction(&Instruction::I32Eq);
        }
        TypeSpec::Identity | TypeSpec::Ref(_) => {
            let Expr::Path(path) = key_expr else {
                return Err(DharmaError::Validation("map key expects identity path".to_string()));
            };
            let info = resolve_path_info(path, env)?;
            if !matches!(info.typ, TypeSpec::Identity | TypeSpec::Ref(_)) {
                return Err(DharmaError::Validation("map key expects identity path".to_string()));
            }
            let info = unwrap_optional_info(info);
            compile_mem_eq(
                func,
                map_info.base,
                key_offset as u32,
                info.base,
                info.offset,
                32,
            );
        }
        TypeSpec::GeoPoint => {
            let Expr::Path(path) = key_expr else {
                return Err(DharmaError::Validation("map key expects geopoint path".to_string()));
            };
            let info = resolve_path_info(path, env)?;
            if !matches!(info.typ, TypeSpec::GeoPoint) {
                return Err(DharmaError::Validation("map key expects geopoint path".to_string()));
            }
            let info = unwrap_optional_info(info);
            compile_mem_eq(
                func,
                map_info.base,
                key_offset as u32,
                info.base,
                info.offset,
                8,
            );
        }
        TypeSpec::Ratio => {
            let Expr::Path(path) = key_expr else {
                return Err(DharmaError::Validation("map key expects ratio path".to_string()));
            };
            let info = resolve_path_info(path, env)?;
            if !matches!(info.typ, TypeSpec::Ratio) {
                return Err(DharmaError::Validation("map key expects ratio path".to_string()));
            }
            let info = unwrap_optional_info(info);
            compile_mem_eq(
                func,
                map_info.base,
                key_offset as u32,
                info.base,
                info.offset,
                16,
            );
        }
        TypeSpec::Text(_) | TypeSpec::Currency => {
            let max = match key_type {
                TypeSpec::Text(len) => len.unwrap_or(DEFAULT_TEXT_LEN),
                TypeSpec::Currency => DEFAULT_TEXT_LEN,
                _ => DEFAULT_TEXT_LEN,
            };
            match key_expr {
                Expr::Literal(Literal::Text(text)) => {
                    let bytes = text_bytes(text, max);
                    compile_mem_eq_const(func, map_info.base, key_offset as u32, &bytes);
                }
                Expr::Path(path) => {
                    let info = resolve_path_info(path, env)?;
                    let info = unwrap_optional_info(info);
                    if !matches!(info.typ, TypeSpec::Text(_) | TypeSpec::Currency) {
                        return Err(DharmaError::Validation("map key expects text path".to_string()));
                    }
                    compile_mem_eq(
                        func,
                        map_info.base,
                        key_offset as u32,
                        info.base,
                        info.offset,
                        4 + max,
                    );
                }
                _ => return Err(DharmaError::Validation("map key expects text literal or path".to_string())),
            }
        }
        _ => return Err(DharmaError::Validation("map key type unsupported".to_string())),
    }
    Ok(())
}

fn compile_list_elem_eq(
    list_info: &FieldInfo,
    elem_type: &TypeSpec,
    idx: usize,
    item_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let elem_size = type_size(elem_type, &env._schema.structs);
    let elem_offset = list_info.offset as usize + 4 + idx * elem_size;
    match elem_type {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            push_addr(func, list_info.base, elem_offset as u32);
            func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
                offset: 0,
                align: 3,
                memory_index: 0,
            }));
            compile_item_expr_int(item_expr, elem_type, env, func)?;
            func.instruction(&Instruction::I64Eq);
        }
        TypeSpec::Enum(variants) => {
            push_addr(func, list_info.base, elem_offset as u32);
            func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I64ExtendI32U);
            compile_item_expr_enum(item_expr, variants, env, func)?;
            func.instruction(&Instruction::I64Eq);
        }
        TypeSpec::Bool => {
            push_addr(func, list_info.base, elem_offset as u32);
            func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
                offset: 0,
                align: 0,
                memory_index: 0,
            }));
            let typ = compile_expr(item_expr, env, func)?;
            if typ != ExprType::Bool {
                return Err(DharmaError::Validation("contains expects bool".to_string()));
            }
            func.instruction(&Instruction::I32Eq);
        }
        TypeSpec::Identity | TypeSpec::Ref(_) => {
            let Expr::Path(path) = item_expr else {
                return Err(DharmaError::Validation("contains expects identity path".to_string()));
            };
            let info = resolve_path_info(path, env)?;
            if !matches!(info.typ, TypeSpec::Identity | TypeSpec::Ref(_)) {
                return Err(DharmaError::Validation("contains expects identity path".to_string()));
            }
            let info = unwrap_optional_info(info);
            compile_mem_eq(
                func,
                list_info.base,
                elem_offset as u32,
                info.base,
                info.offset,
                32,
            );
        }
        TypeSpec::GeoPoint => {
            let Expr::Path(path) = item_expr else {
                return Err(DharmaError::Validation("contains expects geopoint path".to_string()));
            };
            let info = resolve_path_info(path, env)?;
            if !matches!(info.typ, TypeSpec::GeoPoint) {
                return Err(DharmaError::Validation("contains expects geopoint path".to_string()));
            }
            let info = unwrap_optional_info(info);
            compile_mem_eq(
                func,
                list_info.base,
                elem_offset as u32,
                info.base,
                info.offset,
                8,
            );
        }
        TypeSpec::Ratio => {
            let Expr::Path(path) = item_expr else {
                return Err(DharmaError::Validation("contains expects ratio path".to_string()));
            };
            let info = resolve_path_info(path, env)?;
            if !matches!(info.typ, TypeSpec::Ratio) {
                return Err(DharmaError::Validation("contains expects ratio path".to_string()));
            }
            let info = unwrap_optional_info(info);
            compile_mem_eq(
                func,
                list_info.base,
                elem_offset as u32,
                info.base,
                info.offset,
                16,
            );
        }
        TypeSpec::Text(_) | TypeSpec::Currency => {
            let max = match elem_type {
                TypeSpec::Text(len) => len.unwrap_or(DEFAULT_TEXT_LEN),
                TypeSpec::Currency => DEFAULT_TEXT_LEN,
                _ => DEFAULT_TEXT_LEN,
            };
            match item_expr {
                Expr::Literal(Literal::Text(text)) => {
                    let bytes = text_bytes(text, max);
                    compile_mem_eq_const(func, list_info.base, elem_offset as u32, &bytes);
                }
                Expr::Path(path) => {
                    let info = resolve_path_info(path, env)?;
                    let info = unwrap_optional_info(info);
                    if !matches!(info.typ, TypeSpec::Text(_) | TypeSpec::Currency) {
                        return Err(DharmaError::Validation("contains expects text path".to_string()));
                    }
                    compile_mem_eq(
                        func,
                        list_info.base,
                        elem_offset as u32,
                        info.base,
                        info.offset,
                        4 + max,
                    );
                }
                _ => return Err(DharmaError::Validation("contains expects text literal or path".to_string())),
            }
        }
        _ => {
            return Err(DharmaError::Validation(
                "contains supports int/bool/enum/text/identity/ratio/geopoint lists".to_string(),
            ))
        }
    }
    Ok(())
}

fn compile_item_expr_int(
    item_expr: &Expr,
    _elem_type: &TypeSpec,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let typ = compile_expr(item_expr, env, func)?;
    if typ != ExprType::Int {
        return Err(DharmaError::Validation("contains expects int".to_string()));
    }
    Ok(())
}

fn compile_item_expr_enum(
    item_expr: &Expr,
    variants: &[String],
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    if let Expr::Literal(Literal::Enum(name)) = item_expr {
        let idx = variants
            .iter()
            .position(|v| v == name)
            .ok_or_else(|| DharmaError::Validation("unknown enum literal".to_string()))?;
        func.instruction(&Instruction::I64Const(idx as i64));
        return Ok(());
    }
    let typ = compile_expr(item_expr, env, func)?;
    if typ != ExprType::Int {
        return Err(DharmaError::Validation("contains expects enum".to_string()));
    }
    Ok(())
}

fn compile_list_index(
    list_info: &FieldInfo,
    elem_type: &TypeSpec,
    index_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    compile_list_index_bounds(list_info, index_expr, env, func)?;
    let elem_size = type_size(elem_type, &env._schema.structs);
    push_addr(func, list_info.base, (list_info.offset + 4) as u32);
    let typ = compile_expr(index_expr, env, func)?;
    if typ != ExprType::Int {
        return Err(DharmaError::Validation("index expects int".to_string()));
    }
    func.instruction(&Instruction::I64Const(elem_size as i64));
    func.instruction(&Instruction::I64Mul);
    func.instruction(&Instruction::I32WrapI64);
    func.instruction(&Instruction::I32Add);
    match elem_type {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
                offset: 0,
                align: 3,
                memory_index: 0,
            }));
            Ok(ExprType::Int)
        }
        TypeSpec::Enum(_) => {
            func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I64ExtendI32U);
            Ok(ExprType::Int)
        }
        TypeSpec::Bool => {
            func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
                offset: 0,
                align: 0,
                memory_index: 0,
            }));
            Ok(ExprType::Bool)
        }
        TypeSpec::Text(len) => Ok(ExprType::Text(len.unwrap_or(DEFAULT_TEXT_LEN))),
        TypeSpec::Currency => Ok(ExprType::Text(DEFAULT_TEXT_LEN)),
        TypeSpec::Identity | TypeSpec::Ref(_) => Ok(ExprType::Bytes(32)),
        TypeSpec::SubjectRef(_) => Ok(ExprType::Bytes(40)),
        TypeSpec::Ratio => Ok(ExprType::Bytes(16)),
        TypeSpec::GeoPoint => Ok(ExprType::Bytes(8)),
        _ => Err(DharmaError::Validation(
            "index supports int/bool/enum/text/identity/subjectref/ratio/geopoint lists"
                .to_string(),
        )),
    }
}

fn compile_list_elem_eq_offsets(
    left_info: &FieldInfo,
    right_info: &FieldInfo,
    left_idx: usize,
    right_idx: usize,
    elem_type: &TypeSpec,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let elem_size = type_size(elem_type, &env._schema.structs);
    let left_offset = left_info.offset as usize + 4 + left_idx * elem_size;
    let right_offset = right_info.offset as usize + 4 + right_idx * elem_size;
    match elem_type {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            push_addr(func, left_info.base, left_offset as u32);
            func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
                offset: 0,
                align: 3,
                memory_index: 0,
            }));
            push_addr(func, right_info.base, right_offset as u32);
            func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
                offset: 0,
                align: 3,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I64Eq);
        }
        TypeSpec::Enum(_) => {
            push_addr(func, left_info.base, left_offset as u32);
            func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
            push_addr(func, right_info.base, right_offset as u32);
            func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I32Eq);
        }
        TypeSpec::Bool => {
            push_addr(func, left_info.base, left_offset as u32);
            func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
                offset: 0,
                align: 0,
                memory_index: 0,
            }));
            push_addr(func, right_info.base, right_offset as u32);
            func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
                offset: 0,
                align: 0,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I32Eq);
        }
        TypeSpec::Identity | TypeSpec::Ref(_) => {
            compile_mem_eq(
                func,
                left_info.base,
                left_offset as u32,
                right_info.base,
                right_offset as u32,
                32,
            );
        }
        TypeSpec::SubjectRef(_) => {
            compile_mem_eq(
                func,
                left_info.base,
                left_offset as u32,
                right_info.base,
                right_offset as u32,
                40,
            );
        }
        TypeSpec::GeoPoint => {
            compile_mem_eq(
                func,
                left_info.base,
                left_offset as u32,
                right_info.base,
                right_offset as u32,
                8,
            );
        }
        TypeSpec::Ratio => {
            compile_mem_eq(
                func,
                left_info.base,
                left_offset as u32,
                right_info.base,
                right_offset as u32,
                16,
            );
        }
        TypeSpec::Text(len) => {
            let max = len.unwrap_or(DEFAULT_TEXT_LEN);
            compile_mem_eq(
                func,
                left_info.base,
                left_offset as u32,
                right_info.base,
                right_offset as u32,
                4 + max,
            );
        }
        TypeSpec::Currency => {
            compile_mem_eq(
                func,
                left_info.base,
                left_offset as u32,
                right_info.base,
                right_offset as u32,
                4 + DEFAULT_TEXT_LEN,
            );
        }
        _ => {
            return Err(DharmaError::Validation(
                "list element type unsupported".to_string(),
            ))
        }
    }
    Ok(())
}

fn compile_list_mutation(
    list_info: &FieldInfo,
    elem_type: &TypeSpec,
    expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let Expr::Call(name, args) = expr else {
        return Err(DharmaError::Validation("list assignment expects push/remove".to_string()));
    };
    match name.as_str() {
        "push" => {
            if args.len() != 2 {
                return Err(DharmaError::Validation("list.push expects one arg".to_string()));
            }
            compile_list_push(list_info, elem_type, &args[1], env, func)
        }
        "remove" => {
            if args.len() != 2 {
                return Err(DharmaError::Validation("list.remove expects one arg".to_string()));
            }
            compile_list_remove(list_info, elem_type, &args[1], env, func)
        }
        "normalize" => {
            if args.len() != 1 {
                return Err(DharmaError::Validation(
                    "list.normalize expects no args".to_string(),
                ));
            }
            compile_list_normalize(list_info, elem_type, env, func)
        }
        _ => Err(DharmaError::Validation("list assignment expects push/remove".to_string())),
    }
}

fn compile_list_normalize(
    list_info: &FieldInfo,
    elem_type: &TypeSpec,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let max_len = match elem_type {
        TypeSpec::Text(len) => len.unwrap_or(DEFAULT_TEXT_LEN),
        TypeSpec::Currency => DEFAULT_TEXT_LEN,
        _ => {
            return Err(DharmaError::Validation(
                "list.normalize supports text lists only".to_string(),
            ))
        }
    };
    let cap = list_capacity(elem_type, &env._schema.structs);
    let Some(normalize) = env.host.normalize_text_list_func else {
        return Err(DharmaError::Validation(
            "list.normalize unsupported".to_string(),
        ));
    };
    push_addr(func, list_info.base, list_info.offset);
    func.instruction(&Instruction::I32Const(max_len as i32));
    func.instruction(&Instruction::I32Const(cap as i32));
    func.instruction(&Instruction::Call(normalize));
    func.instruction(&Instruction::Drop);
    Ok(())
}

fn compile_list_push(
    list_info: &FieldInfo,
    elem_type: &TypeSpec,
    item_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let cap = list_capacity(elem_type, &env._schema.structs);
    if cap == 0 {
        func.instruction(&Instruction::Unreachable);
        return Ok(());
    }
    // if len >= cap -> trap
    emit_len_load(list_info, func);
    func.instruction(&Instruction::I32Const(cap as i32));
    func.instruction(&Instruction::I32GeU);
    func.instruction(&Instruction::If(BlockType::Empty));
    func.instruction(&Instruction::Unreachable);
    func.instruction(&Instruction::End);

    // dest = base + offset + 4 + len * elem_size
    let elem_size = type_size(elem_type, &env._schema.structs);
    push_addr(func, list_info.base, (list_info.offset + 4) as u32);
    emit_len_load(list_info, func);
    func.instruction(&Instruction::I32Const(elem_size as i32));
    func.instruction(&Instruction::I32Mul);
    func.instruction(&Instruction::I32Add);
    emit_store_value_at_ptr(elem_type, item_expr, env, func)?;

    // len++
    push_addr(func, list_info.base, list_info.offset);
    emit_len_load(list_info, func);
    func.instruction(&Instruction::I32Const(1));
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::I32Store(wasm_encoder::MemArg {
        offset: 0,
        align: 2,
        memory_index: 0,
    }));
    Ok(())
}

fn compile_list_remove(
    list_info: &FieldInfo,
    elem_type: &TypeSpec,
    item_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let cap = list_capacity(elem_type, &env._schema.structs);
    if cap == 0 {
        return Ok(());
    }
    compile_list_remove_branch(0, cap, list_info, elem_type, item_expr, env, func)
}

fn compile_list_remove_branch(
    idx: usize,
    cap: usize,
    list_info: &FieldInfo,
    elem_type: &TypeSpec,
    item_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    if idx >= cap {
        return Ok(());
    }
    emit_idx_in_len(list_info, idx, func);
    compile_list_elem_eq(list_info, elem_type, idx, item_expr, env, func)?;
    func.instruction(&Instruction::I32And);
    func.instruction(&Instruction::If(BlockType::Empty));
    emit_list_remove_at_idx(list_info, elem_type, idx, &env._schema.structs, func)?;
    func.instruction(&Instruction::Else);
    compile_list_remove_branch(idx + 1, cap, list_info, elem_type, item_expr, env, func)?;
    func.instruction(&Instruction::End);
    Ok(())
}

fn emit_list_remove_at_idx(
    list_info: &FieldInfo,
    elem_type: &TypeSpec,
    idx: usize,
    structs: &BTreeMap<String, crate::pdl::schema::StructSchema>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let elem_size = type_size(elem_type, structs);
    let cap = list_capacity(elem_type, structs);
    for shift_idx in idx..cap.saturating_sub(1) {
        func.instruction(&Instruction::I32Const((shift_idx + 1) as i32));
        emit_len_load(list_info, func);
        func.instruction(&Instruction::I32LtU);
        func.instruction(&Instruction::If(BlockType::Empty));
        let dest_offset = list_info.offset as usize + 4 + shift_idx * elem_size;
        let src_offset = list_info.offset as usize + 4 + (shift_idx + 1) * elem_size;
        push_addr(func, list_info.base, dest_offset as u32);
        push_addr(func, list_info.base, src_offset as u32);
        func.instruction(&Instruction::I32Const(elem_size as i32));
        func.instruction(&Instruction::MemoryCopy { src_mem: 0, dst_mem: 0 });
        func.instruction(&Instruction::End);
    }

    // len--
    push_addr(func, list_info.base, list_info.offset);
    emit_len_load(list_info, func);
    func.instruction(&Instruction::I32Const(1));
    func.instruction(&Instruction::I32Sub);
    func.instruction(&Instruction::I32Store(wasm_encoder::MemArg {
        offset: 0,
        align: 2,
        memory_index: 0,
    }));
    Ok(())
}

fn compile_map_index(
    map_info: &FieldInfo,
    key_type: &TypeSpec,
    value_type: &TypeSpec,
    key_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    let cap = map_capacity(key_type, value_type, &env._schema.structs);
    let (expr_type, wasm_type) = match value_type {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            (ExprType::Int, ValType::I64)
        }
        TypeSpec::Enum(_) => (ExprType::Int, ValType::I64),
        TypeSpec::Bool => (ExprType::Bool, ValType::I32),
        TypeSpec::Identity | TypeSpec::Ref(_) => (ExprType::Bytes(32), ValType::I32),
        TypeSpec::SubjectRef(_) => (ExprType::Bytes(40), ValType::I32),
        TypeSpec::Ratio => (ExprType::Bytes(16), ValType::I32),
        TypeSpec::GeoPoint => (ExprType::Bytes(8), ValType::I32),
        TypeSpec::Text(len) => (ExprType::Text(len.unwrap_or(DEFAULT_TEXT_LEN)), ValType::I32),
        TypeSpec::Currency => (ExprType::Text(DEFAULT_TEXT_LEN), ValType::I32),
        _ => return Err(DharmaError::Validation("map value type unsupported".to_string())),
    };
    if cap == 0 {
        func.instruction(&Instruction::Unreachable);
        return Ok(expr_type);
    }
    compile_map_index_branch(
        0,
        cap,
        map_info,
        key_type,
        value_type,
        key_expr,
        env,
        func,
        wasm_type,
    )?;
    Ok(expr_type)
}

fn compile_map_mutation(
    map_info: &FieldInfo,
    key_type: &TypeSpec,
    value_type: &TypeSpec,
    expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let Expr::Call(name, args) = expr else {
        return Err(DharmaError::Validation("map assignment expects set".to_string()));
    };
    if name != "set" {
        return Err(DharmaError::Validation("map assignment expects set".to_string()));
    }
    if args.len() != 3 {
        return Err(DharmaError::Validation("map.set expects two args".to_string()));
    }
    let key_expr = &args[1];
    let val_expr = &args[2];
    compile_map_set(map_info, key_type, value_type, key_expr, val_expr, env, func)
}

fn compile_map_set(
    map_info: &FieldInfo,
    key_type: &TypeSpec,
    value_type: &TypeSpec,
    key_expr: &Expr,
    val_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let cap = map_capacity(key_type, value_type, &env._schema.structs);
    if cap == 0 {
        func.instruction(&Instruction::Unreachable);
        return Ok(());
    }
    compile_map_set_branch(
        0,
        cap,
        map_info,
        key_type,
        value_type,
        key_expr,
        val_expr,
        env,
        func,
    )
}

fn compile_map_set_branch(
    idx: usize,
    cap: usize,
    map_info: &FieldInfo,
    key_type: &TypeSpec,
    value_type: &TypeSpec,
    key_expr: &Expr,
    val_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    if idx >= cap {
        return emit_map_append(map_info, key_type, value_type, key_expr, val_expr, env, func);
    }
    emit_idx_in_len(map_info, idx, func);
    compile_map_key_eq(map_info, key_type, value_type, key_expr, idx, env, func)?;
    func.instruction(&Instruction::I32And);
    func.instruction(&Instruction::If(BlockType::Empty));
    emit_map_store_value_at_idx(map_info, key_type, value_type, idx, val_expr, env, func)?;
    func.instruction(&Instruction::Else);
    compile_map_set_branch(
        idx + 1,
        cap,
        map_info,
        key_type,
        value_type,
        key_expr,
        val_expr,
        env,
        func,
    )?;
    func.instruction(&Instruction::End);
    Ok(())
}

fn emit_map_store_value_at_idx(
    map_info: &FieldInfo,
    key_type: &TypeSpec,
    value_type: &TypeSpec,
    idx: usize,
    val_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let key_size = type_size(key_type, &env._schema.structs);
    let val_size = type_size(value_type, &env._schema.structs);
    let entry_size = key_size + val_size;
    let val_offset = map_info.offset as usize + 4 + idx * entry_size + key_size;
    push_addr(func, map_info.base, val_offset as u32);
    emit_store_value_at_ptr(value_type, val_expr, env, func)
}

fn emit_map_append(
    map_info: &FieldInfo,
    key_type: &TypeSpec,
    value_type: &TypeSpec,
    key_expr: &Expr,
    val_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let cap = map_capacity(key_type, value_type, &env._schema.structs);
    let key_size = type_size(key_type, &env._schema.structs);
    let val_size = type_size(value_type, &env._schema.structs);
    let entry_size = key_size + val_size;
    // if len >= cap -> trap
    emit_len_load(map_info, func);
    func.instruction(&Instruction::I32Const(cap as i32));
    func.instruction(&Instruction::I32GeU);
    func.instruction(&Instruction::If(BlockType::Empty));
    func.instruction(&Instruction::Unreachable);
    func.instruction(&Instruction::End);

    // key addr = base + offset + 4 + len * entry_size
    push_addr(func, map_info.base, (map_info.offset + 4) as u32);
    emit_len_load(map_info, func);
    func.instruction(&Instruction::I32Const(entry_size as i32));
    func.instruction(&Instruction::I32Mul);
    func.instruction(&Instruction::I32Add);
    emit_store_value_at_ptr(key_type, key_expr, env, func)?;

    // value addr = base + offset + 4 + len * entry_size + key_size
    push_addr(func, map_info.base, (map_info.offset + 4) as u32);
    emit_len_load(map_info, func);
    func.instruction(&Instruction::I32Const(entry_size as i32));
    func.instruction(&Instruction::I32Mul);
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::I32Const(key_size as i32));
    func.instruction(&Instruction::I32Add);
    emit_store_value_at_ptr(value_type, val_expr, env, func)?;

    // len++
    push_addr(func, map_info.base, map_info.offset);
    emit_len_load(map_info, func);
    func.instruction(&Instruction::I32Const(1));
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::I32Store(wasm_encoder::MemArg {
        offset: 0,
        align: 2,
        memory_index: 0,
    }));
    Ok(())
}

fn compile_map_index_branch(
    idx: usize,
    cap: usize,
    map_info: &FieldInfo,
    key_type: &TypeSpec,
    value_type: &TypeSpec,
    key_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
    wasm_type: ValType,
) -> Result<(), DharmaError> {
    if idx >= cap {
        func.instruction(&Instruction::Unreachable);
        return Ok(());
    }
    emit_idx_in_len(map_info, idx, func);
    compile_map_key_eq(map_info, key_type, value_type, key_expr, idx, env, func)?;
    func.instruction(&Instruction::I32And);
    func.instruction(&Instruction::If(BlockType::Result(wasm_type)));
    emit_map_value(map_info, key_type, value_type, idx, &env._schema.structs, func)?;
    func.instruction(&Instruction::Else);
    compile_map_index_branch(
        idx + 1,
        cap,
        map_info,
        key_type,
        value_type,
        key_expr,
        env,
        func,
        wasm_type,
    )?;
    func.instruction(&Instruction::End);
    Ok(())
}

fn emit_map_value(
    map_info: &FieldInfo,
    key_type: &TypeSpec,
    value_type: &TypeSpec,
    idx: usize,
    structs: &BTreeMap<String, crate::pdl::schema::StructSchema>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    let key_size = type_size(key_type, structs);
    let val_size = type_size(value_type, structs);
    let entry_size = key_size + val_size;
    let val_offset = map_info.offset as usize + 4 + idx * entry_size + key_size;
    if matches!(
        value_type,
        TypeSpec::Identity
            | TypeSpec::Ref(_)
            | TypeSpec::SubjectRef(_)
            | TypeSpec::Ratio
            | TypeSpec::GeoPoint
            | TypeSpec::Text(_)
            | TypeSpec::Currency
    ) {
        func.instruction(&Instruction::I32Const(map_info.base as i32));
        func.instruction(&Instruction::I32Const(val_offset as i32));
        func.instruction(&Instruction::I32Add);
        return Ok(match value_type {
            TypeSpec::Identity | TypeSpec::Ref(_) => ExprType::Bytes(32),
            TypeSpec::SubjectRef(_) => ExprType::Bytes(40),
            TypeSpec::Ratio => ExprType::Bytes(16),
            TypeSpec::GeoPoint => ExprType::Bytes(8),
            TypeSpec::Text(len) => ExprType::Text(len.unwrap_or(DEFAULT_TEXT_LEN)),
            TypeSpec::Currency => ExprType::Text(DEFAULT_TEXT_LEN),
            _ => ExprType::Int,
        });
    }
    push_addr(func, map_info.base, val_offset as u32);
    match value_type {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
                offset: 0,
                align: 3,
                memory_index: 0,
            }));
            Ok(ExprType::Int)
        }
        TypeSpec::Enum(_) => {
            func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I64ExtendI32U);
            Ok(ExprType::Int)
        }
        TypeSpec::Bool => {
            func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
                offset: 0,
                align: 0,
                memory_index: 0,
            }));
            Ok(ExprType::Bool)
        }
        TypeSpec::Identity | TypeSpec::Ref(_) => Ok(ExprType::Bytes(32)),
        TypeSpec::Ratio => Ok(ExprType::Bytes(16)),
        TypeSpec::GeoPoint => Ok(ExprType::Bytes(8)),
        TypeSpec::Text(len) => Ok(ExprType::Text(len.unwrap_or(DEFAULT_TEXT_LEN))),
        TypeSpec::Currency => Ok(ExprType::Text(DEFAULT_TEXT_LEN)),
        _ => Err(DharmaError::Validation("map value type unsupported".to_string())),
    }
}

fn compile_list_index_bounds(
    list_info: &FieldInfo,
    index_expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let typ = compile_expr(index_expr, env, func)?;
    if typ != ExprType::Int {
        return Err(DharmaError::Validation("index expects int".to_string()));
    }
    func.instruction(&Instruction::I64Const(0));
    func.instruction(&Instruction::I64LtS);
    let typ = compile_expr(index_expr, env, func)?;
    if typ != ExprType::Int {
        return Err(DharmaError::Validation("index expects int".to_string()));
    }
    emit_len_load(list_info, func);
    func.instruction(&Instruction::I64ExtendI32U);
    func.instruction(&Instruction::I64GeU);
    func.instruction(&Instruction::I32Or);
    func.instruction(&Instruction::If(BlockType::Empty));
    func.instruction(&Instruction::Unreachable);
    func.instruction(&Instruction::End);
    Ok(())
}

fn compile_mem_eq(
    func: &mut Function,
    left_base: u32,
    left_offset: u32,
    right_base: u32,
    right_offset: u32,
    size: usize,
) {
    let mut cursor = 0usize;
    let mut first = true;
    let mut remaining = size;
    while remaining >= 8 {
        push_addr(func, left_base, left_offset + cursor as u32);
        func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
            offset: 0,
            align: 3,
            memory_index: 0,
        }));
        push_addr(func, right_base, right_offset + cursor as u32);
        func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
            offset: 0,
            align: 3,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I64Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
        cursor += 8;
        remaining -= 8;
    }
    if remaining >= 4 {
        push_addr(func, left_base, left_offset + cursor as u32);
        func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
            offset: 0,
            align: 2,
            memory_index: 0,
        }));
        push_addr(func, right_base, right_offset + cursor as u32);
        func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
            offset: 0,
            align: 2,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
        cursor += 4;
        remaining -= 4;
    }
    if remaining >= 2 {
        push_addr(func, left_base, left_offset + cursor as u32);
        func.instruction(&Instruction::I32Load16U(wasm_encoder::MemArg {
            offset: 0,
            align: 1,
            memory_index: 0,
        }));
        push_addr(func, right_base, right_offset + cursor as u32);
        func.instruction(&Instruction::I32Load16U(wasm_encoder::MemArg {
            offset: 0,
            align: 1,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
        cursor += 2;
        remaining -= 2;
    }
    if remaining == 1 {
        push_addr(func, left_base, left_offset + cursor as u32);
        func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
            offset: 0,
            align: 0,
            memory_index: 0,
        }));
        push_addr(func, right_base, right_offset + cursor as u32);
        func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
            offset: 0,
            align: 0,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Eq);
        if !first {
            func.instruction(&Instruction::I32And);
        }
    }
}

fn compile_mem_eq_const(func: &mut Function, base: u32, offset: u32, bytes: &[u8]) {
    let mut cursor = 0usize;
    let mut first = true;
    let mut remaining = bytes.len();
    while remaining >= 8 {
        let mut chunk = [0u8; 8];
        chunk.copy_from_slice(&bytes[cursor..cursor + 8]);
        push_addr(func, base, offset + cursor as u32);
        func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
            offset: 0,
            align: 3,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I64Const(i64::from_le_bytes(chunk)));
        func.instruction(&Instruction::I64Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
        cursor += 8;
        remaining -= 8;
    }
    if remaining >= 4 {
        let mut chunk = [0u8; 4];
        chunk.copy_from_slice(&bytes[cursor..cursor + 4]);
        push_addr(func, base, offset + cursor as u32);
        func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
            offset: 0,
            align: 2,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Const(i32::from_le_bytes(chunk)));
        func.instruction(&Instruction::I32Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
        cursor += 4;
        remaining -= 4;
    }
    if remaining >= 2 {
        let mut chunk = [0u8; 2];
        chunk.copy_from_slice(&bytes[cursor..cursor + 2]);
        push_addr(func, base, offset + cursor as u32);
        func.instruction(&Instruction::I32Load16U(wasm_encoder::MemArg {
            offset: 0,
            align: 1,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Const(u16::from_le_bytes(chunk) as i32));
        func.instruction(&Instruction::I32Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
        cursor += 2;
        remaining -= 2;
    }
    if remaining == 1 {
        push_addr(func, base, offset + cursor as u32);
        func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
            offset: 0,
            align: 0,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Const(bytes[cursor] as i32));
        func.instruction(&Instruction::I32Eq);
        if !first {
            func.instruction(&Instruction::I32And);
        }
    }
}

fn compile_mem_eq_const_ptr(func: &mut Function, ptr_local: u32, bytes: &[u8]) {
    let mut cursor = 0usize;
    let mut first = true;
    let mut remaining = bytes.len();
    while remaining >= 8 {
        let mut chunk = [0u8; 8];
        chunk.copy_from_slice(&bytes[cursor..cursor + 8]);
        func.instruction(&Instruction::LocalGet(ptr_local));
        func.instruction(&Instruction::I32Const(cursor as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
            offset: 0,
            align: 3,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I64Const(i64::from_le_bytes(chunk)));
        func.instruction(&Instruction::I64Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
        cursor += 8;
        remaining -= 8;
    }
    if remaining >= 4 {
        let mut chunk = [0u8; 4];
        chunk.copy_from_slice(&bytes[cursor..cursor + 4]);
        func.instruction(&Instruction::LocalGet(ptr_local));
        func.instruction(&Instruction::I32Const(cursor as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
            offset: 0,
            align: 2,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Const(i32::from_le_bytes(chunk)));
        func.instruction(&Instruction::I32Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
        cursor += 4;
        remaining -= 4;
    }
    if remaining >= 2 {
        let mut chunk = [0u8; 2];
        chunk.copy_from_slice(&bytes[cursor..cursor + 2]);
        func.instruction(&Instruction::LocalGet(ptr_local));
        func.instruction(&Instruction::I32Const(cursor as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I32Load16U(wasm_encoder::MemArg {
            offset: 0,
            align: 1,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Const(u16::from_le_bytes(chunk) as i32));
        func.instruction(&Instruction::I32Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
        cursor += 2;
        remaining -= 2;
    }
    if remaining == 1 {
        func.instruction(&Instruction::LocalGet(ptr_local));
        func.instruction(&Instruction::I32Const(cursor as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
            offset: 0,
            align: 0,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Const(bytes[cursor] as i32));
        func.instruction(&Instruction::I32Eq);
        if !first {
            func.instruction(&Instruction::I32And);
        }
    }
}

fn text_bytes(text: &str, max: usize) -> Vec<u8> {
    let mut out = vec![0u8; 4 + max];
    let bytes = text.as_bytes();
    let copy_len = bytes.len().min(max);
    out[..4].copy_from_slice(&(copy_len as u32).to_le_bytes());
    out[4..4 + copy_len].copy_from_slice(&bytes[..copy_len]);
    out
}

fn write_const_bytes(func: &mut Function, base: u32, bytes: &[u8]) {
    let mut cursor = 0usize;
    let mut remaining = bytes.len();
    while remaining >= 8 {
        let mut chunk = [0u8; 8];
        chunk.copy_from_slice(&bytes[cursor..cursor + 8]);
        push_addr(func, base, cursor as u32);
        func.instruction(&Instruction::I64Const(i64::from_le_bytes(chunk)));
        func.instruction(&Instruction::I64Store(wasm_encoder::MemArg {
            offset: 0,
            align: 3,
            memory_index: 0,
        }));
        cursor += 8;
        remaining -= 8;
    }
    if remaining >= 4 {
        let mut chunk = [0u8; 4];
        chunk.copy_from_slice(&bytes[cursor..cursor + 4]);
        push_addr(func, base, cursor as u32);
        func.instruction(&Instruction::I32Const(i32::from_le_bytes(chunk)));
        func.instruction(&Instruction::I32Store(wasm_encoder::MemArg {
            offset: 0,
            align: 2,
            memory_index: 0,
        }));
        cursor += 4;
        remaining -= 4;
    }
    if remaining >= 2 {
        let mut chunk = [0u8; 2];
        chunk.copy_from_slice(&bytes[cursor..cursor + 2]);
        push_addr(func, base, cursor as u32);
        func.instruction(&Instruction::I32Const(i32::from(i16::from_le_bytes(chunk))));
        func.instruction(&Instruction::I32Store16(wasm_encoder::MemArg {
            offset: 0,
            align: 1,
            memory_index: 0,
        }));
        cursor += 2;
        remaining -= 2;
    }
    if remaining == 1 {
        push_addr(func, base, cursor as u32);
        func.instruction(&Instruction::I32Const(bytes[cursor] as i32));
        func.instruction(&Instruction::I32Store8(wasm_encoder::MemArg {
            offset: 0,
            align: 0,
            memory_index: 0,
        }));
    }
}

fn compile_mem_eq_ptrs(func: &mut Function, size: usize) {
    let mut cursor = 0usize;
    let mut first = true;
    let mut remaining = size;
    while remaining >= 8 {
        func.instruction(&Instruction::LocalGet(2));
        func.instruction(&Instruction::I32Const(cursor as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
            offset: 0,
            align: 3,
            memory_index: 0,
        }));
        func.instruction(&Instruction::LocalGet(3));
        func.instruction(&Instruction::I32Const(cursor as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
            offset: 0,
            align: 3,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I64Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
        cursor += 8;
        remaining -= 8;
    }
    if remaining >= 4 {
        func.instruction(&Instruction::LocalGet(2));
        func.instruction(&Instruction::I32Const(cursor as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
            offset: 0,
            align: 2,
            memory_index: 0,
        }));
        func.instruction(&Instruction::LocalGet(3));
        func.instruction(&Instruction::I32Const(cursor as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
            offset: 0,
            align: 2,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
        cursor += 4;
        remaining -= 4;
    }
    if remaining >= 2 {
        func.instruction(&Instruction::LocalGet(2));
        func.instruction(&Instruction::I32Const(cursor as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I32Load16U(wasm_encoder::MemArg {
            offset: 0,
            align: 1,
            memory_index: 0,
        }));
        func.instruction(&Instruction::LocalGet(3));
        func.instruction(&Instruction::I32Const(cursor as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I32Load16U(wasm_encoder::MemArg {
            offset: 0,
            align: 1,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
        cursor += 2;
        remaining -= 2;
    }
    if remaining == 1 {
        func.instruction(&Instruction::LocalGet(2));
        func.instruction(&Instruction::I32Const(cursor as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
            offset: 0,
            align: 0,
            memory_index: 0,
        }));
        func.instruction(&Instruction::LocalGet(3));
        func.instruction(&Instruction::I32Const(cursor as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
            offset: 0,
            align: 0,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Eq);
        if !first {
            func.instruction(&Instruction::I32And);
        }
    }
}

fn emit_store_value_at_ptr(
    typ: &TypeSpec,
    expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    func.instruction(&Instruction::LocalSet(1));
    match typ {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            func.instruction(&Instruction::LocalGet(1));
            let t = compile_expr(expr, env, func)?;
            if t != ExprType::Int {
                return Err(DharmaError::Validation("expected int".to_string()));
            }
            func.instruction(&Instruction::I64Store(wasm_encoder::MemArg {
                offset: 0,
                align: 3,
                memory_index: 0,
            }));
        }
        TypeSpec::Bool => {
            func.instruction(&Instruction::LocalGet(1));
            let t = compile_expr(expr, env, func)?;
            if t != ExprType::Bool {
                return Err(DharmaError::Validation("expected bool".to_string()));
            }
            func.instruction(&Instruction::I32Store8(wasm_encoder::MemArg {
                offset: 0,
                align: 0,
                memory_index: 0,
            }));
        }
        TypeSpec::Enum(variants) => {
            func.instruction(&Instruction::LocalGet(1));
            if let Expr::Literal(Literal::Enum(name)) = expr {
                let idx = variants
                    .iter()
                    .position(|v| v == name)
                    .ok_or_else(|| DharmaError::Validation("unknown enum literal".to_string()))?;
                func.instruction(&Instruction::I32Const(idx as i32));
            } else {
                let t = compile_expr(expr, env, func)?;
                if t != ExprType::Int {
                    return Err(DharmaError::Validation("expected enum index".to_string()));
                }
                func.instruction(&Instruction::I32WrapI64);
            }
            func.instruction(&Instruction::I32Store(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
        }
        TypeSpec::Identity | TypeSpec::Ref(_) => {
            let Expr::Path(path) = expr else {
                return Err(DharmaError::Validation("expected identity path".to_string()));
            };
            let info = resolve_path_info(path, env)?;
            if !matches!(info.typ, TypeSpec::Identity | TypeSpec::Ref(_)) {
                return Err(DharmaError::Validation("expected identity path".to_string()));
            }
            let info = unwrap_optional_info(info);
            func.instruction(&Instruction::LocalGet(1));
            push_addr(func, info.base, info.offset);
            func.instruction(&Instruction::I32Const(32));
            func.instruction(&Instruction::MemoryCopy { src_mem: 0, dst_mem: 0 });
        }
        TypeSpec::SubjectRef(_) => {
            let Expr::Path(path) = expr else {
                return Err(DharmaError::Validation("expected subject ref path".to_string()));
            };
            let info = resolve_path_info(path, env)?;
            if !matches!(info.typ, TypeSpec::SubjectRef(_)) {
                return Err(DharmaError::Validation("expected subject ref path".to_string()));
            }
            let info = unwrap_optional_info(info);
            func.instruction(&Instruction::LocalGet(1));
            push_addr(func, info.base, info.offset);
            func.instruction(&Instruction::I32Const(40));
            func.instruction(&Instruction::MemoryCopy { src_mem: 0, dst_mem: 0 });
        }
        TypeSpec::Ratio => {
            let Expr::Path(path) = expr else {
                return Err(DharmaError::Validation("expected ratio path".to_string()));
            };
            let info = resolve_path_info(path, env)?;
            if !matches!(info.typ, TypeSpec::Ratio) {
                return Err(DharmaError::Validation("expected ratio path".to_string()));
            }
            let info = unwrap_optional_info(info);
            func.instruction(&Instruction::LocalGet(1));
            push_addr(func, info.base, info.offset);
            func.instruction(&Instruction::I32Const(16));
            func.instruction(&Instruction::MemoryCopy { src_mem: 0, dst_mem: 0 });
        }
        TypeSpec::GeoPoint => {
            let Expr::Path(path) = expr else {
                return Err(DharmaError::Validation("expected geopoint path".to_string()));
            };
            let info = resolve_path_info(path, env)?;
            if !matches!(info.typ, TypeSpec::GeoPoint) {
                return Err(DharmaError::Validation("expected geopoint path".to_string()));
            }
            let info = unwrap_optional_info(info);
            func.instruction(&Instruction::LocalGet(1));
            push_addr(func, info.base, info.offset);
            func.instruction(&Instruction::I32Const(8));
            func.instruction(&Instruction::MemoryCopy { src_mem: 0, dst_mem: 0 });
        }
        TypeSpec::Text(_) | TypeSpec::Currency => {
            let max = match typ {
                TypeSpec::Text(len) => len.unwrap_or(DEFAULT_TEXT_LEN),
                TypeSpec::Currency => DEFAULT_TEXT_LEN,
                _ => DEFAULT_TEXT_LEN,
            };
            match expr {
                Expr::BinaryOp(Op::Add, _, _) => {
                    let mut parts = Vec::new();
                    collect_text_parts(expr, env, &mut parts)?;
                    emit_concat_text_parts(&parts, env, func, max)?;
                }
                Expr::Literal(Literal::Text(text)) => {
                    let bytes = text_bytes(text, max);
                    let len_bytes = (bytes[0] as u32)
                        | ((bytes[1] as u32) << 8)
                        | ((bytes[2] as u32) << 16)
                        | ((bytes[3] as u32) << 24);
                    func.instruction(&Instruction::LocalGet(1));
                    func.instruction(&Instruction::I32Const(len_bytes as i32));
                    func.instruction(&Instruction::I32Store(wasm_encoder::MemArg {
                        offset: 0,
                        align: 2,
                        memory_index: 0,
                    }));
                    for (idx, b) in bytes[4..].iter().enumerate() {
                        func.instruction(&Instruction::LocalGet(1));
                        func.instruction(&Instruction::I32Const((4 + idx) as i32));
                        func.instruction(&Instruction::I32Add);
                        func.instruction(&Instruction::I32Const(*b as i32));
                        func.instruction(&Instruction::I32Store8(wasm_encoder::MemArg {
                            offset: 0,
                            align: 0,
                            memory_index: 0,
                        }));
                    }
                    let rem = max.saturating_sub(len_bytes as usize);
                    if rem > 0 {
                        func.instruction(&Instruction::LocalGet(1));
                        func.instruction(&Instruction::I32Const((4 + len_bytes) as i32));
                        func.instruction(&Instruction::I32Add);
                        func.instruction(&Instruction::I32Const(0));
                        func.instruction(&Instruction::I32Const(rem as i32));
                        func.instruction(&Instruction::MemoryFill(0));
                    }
                }
                Expr::Path(path) => {
                    let info = resolve_path_info(path, env)?;
                    let info = unwrap_optional_info(info);
                    if !matches!(info.typ, TypeSpec::Text(_) | TypeSpec::Currency) {
                        return Err(DharmaError::Validation("expected text path".to_string()));
                    }
                    func.instruction(&Instruction::LocalGet(1));
                    push_addr(func, info.base, info.offset);
                    func.instruction(&Instruction::I32Const((4 + max) as i32));
                    func.instruction(&Instruction::MemoryCopy { src_mem: 0, dst_mem: 0 });
                }
                _ => {
                    return Err(DharmaError::Validation("expected text literal or path".to_string()));
                }
            }
        }
        TypeSpec::Optional(inner) => {
            match expr {
                Expr::Literal(Literal::Null) => {
                    func.instruction(&Instruction::LocalGet(1));
                    func.instruction(&Instruction::I32Const(0));
                    func.instruction(&Instruction::I32Store8(wasm_encoder::MemArg {
                        offset: 0,
                        align: 0,
                        memory_index: 0,
                    }));
                    let size = type_size(inner, &env._schema.structs);
                    func.instruction(&Instruction::LocalGet(1));
                    func.instruction(&Instruction::I32Const(1));
                    func.instruction(&Instruction::I32Add);
                    func.instruction(&Instruction::I32Const(0));
                    func.instruction(&Instruction::I32Const(size as i32));
                    func.instruction(&Instruction::MemoryFill(0));
                }
                _ => {
                    func.instruction(&Instruction::LocalGet(1));
                    func.instruction(&Instruction::I32Const(1));
                    func.instruction(&Instruction::I32Store8(wasm_encoder::MemArg {
                        offset: 0,
                        align: 0,
                        memory_index: 0,
                    }));
                    func.instruction(&Instruction::LocalGet(1));
                    func.instruction(&Instruction::I32Const(1));
                    func.instruction(&Instruction::I32Add);
                    emit_store_value_at_ptr(inner, expr, env, func)?;
                }
            }
        }
        TypeSpec::Struct(name) => {
            let mut values = BTreeMap::new();
            match expr {
                Expr::Literal(Literal::Map(entries)) => {
                    for (k, v) in entries {
                        let Expr::Literal(Literal::Text(key)) = k else {
                            return Err(DharmaError::Validation("struct field name must be text".to_string()));
                        };
                        values.insert(key.clone(), v.clone());
                    }
                }
                Expr::Literal(Literal::Struct(lit_name, entries)) => {
                    if lit_name != name {
                        return Err(DharmaError::Validation("struct literal type mismatch".to_string()));
                    }
                    for (k, v) in entries {
                        values.insert(k.clone(), v.clone());
                    }
                }
                Expr::Path(path) => {
                    let info = resolve_path_info(path, env)?;
                    let info = unwrap_optional_info(info);
                    if !matches!(info.typ, TypeSpec::Struct(_)) {
                        return Err(DharmaError::Validation("expected struct path".to_string()));
                    }
                    func.instruction(&Instruction::LocalGet(1));
                    push_addr(func, info.base, info.offset);
                    let size = type_size(typ, &env._schema.structs);
                    func.instruction(&Instruction::I32Const(size as i32));
                    func.instruction(&Instruction::MemoryCopy { src_mem: 0, dst_mem: 0 });
                    return Ok(());
                }
                _ => {
                    return Err(DharmaError::Validation("expected struct literal or path".to_string()));
                }
            }
            let Some(def) = env._schema.structs.get(name) else {
                return Err(DharmaError::Validation("unknown struct".to_string()));
            };
            let base_slot = env.alloc_scratch(4)?;
            func.instruction(&Instruction::I32Const(base_slot as i32));
            func.instruction(&Instruction::LocalGet(1));
            func.instruction(&Instruction::I32Store(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
            let mut cursor = 0usize;
            for (field_name, field) in &def.fields {
                let field_expr = match values.get(field_name) {
                    Some(expr) => expr,
                    None => {
                        if matches!(field.typ, TypeSpec::Optional(_)) {
                            func.instruction(&Instruction::I32Const(base_slot as i32));
                            func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                                offset: 0,
                                align: 2,
                                memory_index: 0,
                            }));
                            func.instruction(&Instruction::I32Const(cursor as i32));
                            func.instruction(&Instruction::I32Add);
                            emit_store_value_at_ptr(&field.typ, &Expr::Literal(Literal::Null), env, func)?;
                            cursor += type_size(&field.typ, &env._schema.structs);
                            continue;
                        }
                        return Err(DharmaError::Validation(format!(
                            "missing struct field '{}'", field_name
                        )));
                    }
                };
                func.instruction(&Instruction::I32Const(base_slot as i32));
                func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                    offset: 0,
                    align: 2,
                    memory_index: 0,
                }));
                func.instruction(&Instruction::I32Const(cursor as i32));
                func.instruction(&Instruction::I32Add);
                emit_store_value_at_ptr(&field.typ, field_expr, env, func)?;
                cursor += type_size(&field.typ, &env._schema.structs);
            }
        }
        _ => return Err(DharmaError::Validation("collection value unsupported".to_string())),
    }
    Ok(())
}

#[derive(Clone)]
enum TextPart {
    Literal(String),
    Path(Vec<String>),
}

fn collect_text_parts(
    expr: &Expr,
    env: &Env<'_>,
    parts: &mut Vec<TextPart>,
) -> Result<(), DharmaError> {
    match expr {
        Expr::BinaryOp(Op::Add, left, right) => {
            collect_text_parts(left, env, parts)?;
            collect_text_parts(right, env, parts)?;
            Ok(())
        }
        Expr::Literal(Literal::Text(text)) => {
            parts.push(TextPart::Literal(text.clone()));
            Ok(())
        }
        Expr::Path(path) => {
            let info = resolve_path_info(path, env)?;
            let info = unwrap_optional_info(info);
            if !matches!(info.typ, TypeSpec::Text(_) | TypeSpec::Currency) {
                return Err(DharmaError::Validation("concat expects text path".to_string()));
            }
            parts.push(TextPart::Path(path.clone()));
            Ok(())
        }
        _ => Err(DharmaError::Validation(
            "concat expects text literal or path".to_string(),
        )),
    }
}

fn emit_text_part_ptr(
    part: &TextPart,
    env: &Env<'_>,
    func: &mut Function,
    max: usize,
) -> Result<(), DharmaError> {
    match part {
        TextPart::Literal(text) => {
            let bytes = text_bytes(text, max);
            write_const_bytes(func, env.literal_base, &bytes);
            func.instruction(&Instruction::I32Const(env.literal_base as i32));
        }
        TextPart::Path(path) => {
            let info = resolve_path_info(path, env)?;
            let info = unwrap_optional_info(info);
            if !matches!(info.typ, TypeSpec::Text(_) | TypeSpec::Currency) {
                return Err(DharmaError::Validation("expected text path".to_string()));
            }
            push_addr(func, info.base, info.offset);
        }
    }
    Ok(())
}

fn emit_concat_text_parts(
    parts: &[TextPart],
    env: &Env<'_>,
    func: &mut Function,
    max: usize,
) -> Result<(), DharmaError> {
    if parts.is_empty() {
        return Err(DharmaError::Validation("concat expects parts".to_string()));
    }
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(0));
    for part in parts {
        emit_text_part_ptr(part, env, func, max)?;
        func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
            offset: 0,
            align: 2,
            memory_index: 0,
        }));
        func.instruction(&Instruction::LocalSet(2));

        func.instruction(&Instruction::I32Const(max as i32));
        func.instruction(&Instruction::LocalGet(0));
        func.instruction(&Instruction::I32Sub);
        func.instruction(&Instruction::LocalSet(3));

        func.instruction(&Instruction::LocalGet(2));
        func.instruction(&Instruction::LocalGet(3));
        func.instruction(&Instruction::I32GtU);
        func.instruction(&Instruction::If(BlockType::Result(ValType::I32)));
        func.instruction(&Instruction::LocalGet(3));
        func.instruction(&Instruction::Else);
        func.instruction(&Instruction::LocalGet(2));
        func.instruction(&Instruction::End);
        func.instruction(&Instruction::LocalSet(2));

        func.instruction(&Instruction::LocalGet(1));
        func.instruction(&Instruction::I32Const(4));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::LocalGet(0));
        func.instruction(&Instruction::I32Add);

        emit_text_part_ptr(part, env, func, max)?;
        func.instruction(&Instruction::I32Const(4));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::LocalGet(2));
        func.instruction(&Instruction::MemoryCopy { src_mem: 0, dst_mem: 0 });

        func.instruction(&Instruction::LocalGet(0));
        func.instruction(&Instruction::LocalGet(2));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::LocalSet(0));
    }

    func.instruction(&Instruction::LocalGet(1));
    func.instruction(&Instruction::LocalGet(0));
    func.instruction(&Instruction::I32Store(wasm_encoder::MemArg {
        offset: 0,
        align: 2,
        memory_index: 0,
    }));

    func.instruction(&Instruction::I32Const(max as i32));
    func.instruction(&Instruction::LocalGet(0));
    func.instruction(&Instruction::I32Sub);
    func.instruction(&Instruction::LocalSet(3));
    func.instruction(&Instruction::LocalGet(1));
    func.instruction(&Instruction::I32Const(4));
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::LocalGet(0));
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalGet(3));
    func.instruction(&Instruction::MemoryFill(0));

    Ok(())
}

fn literal_index(expr: &Expr) -> Result<usize, DharmaError> {
    match expr {
        Expr::Literal(Literal::Int(value)) if *value >= 0 => Ok(*value as usize),
        _ => Err(DharmaError::Validation("index must be literal int".to_string())),
    }
}

fn literal_key(expr: &Expr) -> Result<Literal, DharmaError> {
    match expr {
        Expr::Literal(lit) => Ok(lit.clone()),
        _ => Err(DharmaError::Validation("map key must be literal".to_string())),
    }
}

fn expr_literal(expr: &Expr) -> Option<&Literal> {
    if let Expr::Literal(lit) = expr {
        Some(lit)
    } else {
        None
    }
}

fn literal_eq(a: &Literal, b: &Literal) -> bool {
    match (a, b) {
        (Literal::Int(x), Literal::Int(y)) => x == y,
        (Literal::Bool(x), Literal::Bool(y)) => x == y,
        (Literal::Text(x), Literal::Text(y)) => x == y,
        (Literal::Enum(x), Literal::Enum(y)) => x == y,
        (Literal::Null, Literal::Null) => true,
        _ => false,
    }
}

fn compile_in_list(
    item_expr: &Expr,
    items: &[Expr],
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    if items.is_empty() {
        func.instruction(&Instruction::I32Const(0));
        return Ok(ExprType::Bool);
    }
    let mut first = true;
    for item in items {
        let eq_expr = Expr::BinaryOp(Op::Eq, Box::new(item_expr.clone()), Box::new(item.clone()));
        let typ = compile_expr(&eq_expr, env, func)?;
        if typ != ExprType::Bool {
            return Err(DharmaError::Validation("invalid in operand".to_string()));
        }
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32Or);
        }
    }
    Ok(ExprType::Bool)
}

fn compile_bytes32_eq(
    op: &Op,
    left_info: &FieldInfo,
    right_info: &FieldInfo,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    let mut first = true;
    for i in 0..4 {
        func.instruction(&Instruction::I32Const(left_info.base as i32));
        func.instruction(&Instruction::I32Const((left_info.offset + (i * 8)) as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
            offset: 0,
            align: 3,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Const(right_info.base as i32));
        func.instruction(&Instruction::I32Const((right_info.offset + (i * 8)) as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
            offset: 0,
            align: 3,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I64Eq);
        if first {
            first = false;
        } else {
            func.instruction(&Instruction::I32And);
        }
    }
    if *op == Op::Neq {
        func.instruction(&Instruction::I32Eqz);
    }
    Ok(ExprType::Bool)
}

fn bytes32_paths(
    left: &Expr,
    right: &Expr,
    env: &Env<'_>,
) -> Result<Option<(FieldInfo, FieldInfo)>, DharmaError> {
    let (Expr::Path(left_path), Expr::Path(right_path)) = (left, right) else {
        return Ok(None);
    };
    let left_info = resolve_path_info(left_path, env)?;
    let right_info = resolve_path_info(right_path, env)?;
    let left_un = unwrap_optional_info(left_info);
    let right_un = unwrap_optional_info(right_info);
    if matches!(left_un.typ, TypeSpec::Identity | TypeSpec::Ref(_))
        && matches!(right_un.typ, TypeSpec::Identity | TypeSpec::Ref(_))
    {
        return Ok(Some((left_un, right_un)));
    }
    Ok(None)
}

fn resolve_path_info<'a>(
    path: &[String],
    env: &'a Env<'_>,
) -> Result<&'a FieldInfo, DharmaError> {
    if path.is_empty() {
        return Err(DharmaError::Validation("empty path".to_string()));
    }
    if path[0] == "state" {
        let name = path
            .get(1)
            .ok_or_else(|| DharmaError::Validation("invalid state path".to_string()))?;
        let key = if path.len() > 2 {
            path[1..].join(".")
        } else {
            name.clone()
        };
        return env
            .state
            .get(&key)
            .ok_or_else(|| DharmaError::Validation("unknown state field".to_string()));
    }
    if path[0] == "args" {
        let name = path
            .get(1)
            .ok_or_else(|| DharmaError::Validation("invalid args path".to_string()))?;
        let key = if path.len() > 2 {
            path[1..].join(".")
        } else {
            name.clone()
        };
        return env
            .args
            .get(&key)
            .ok_or_else(|| DharmaError::Validation(format!("unknown arg '{}'", name)));
    }
    if path[0] == "context" {
        let key = path.join(".");
        return env
            .context
            .get(&key)
            .ok_or_else(|| DharmaError::Validation("unknown context".to_string()));
    }
    let key = if path.len() > 1 {
        path.join(".")
    } else {
        path[0].clone()
    };
    env.args
        .get(&key)
        .ok_or_else(|| DharmaError::Validation(format!("unknown arg '{}'", path[0])))
}

fn unwrap_optional_info(info: &FieldInfo) -> FieldInfo {
    match &info.typ {
        TypeSpec::Optional(inner) => FieldInfo {
            base: info.base,
            offset: info.offset + 1,
            typ: *inner.clone(),
        },
        _ => info.clone(),
    }
}

fn enum_literal_pair<'a>(left: &'a Expr, right: &'a Expr) -> Option<(&'a [String], &'a str)> {
    match (left, right) {
        (Expr::Path(path), Expr::Literal(Literal::Enum(name))) => Some((path, name.as_str())),
        (Expr::Literal(Literal::Enum(name)), Expr::Path(path)) => Some((path, name.as_str())),
        _ => None,
    }
}

fn text_literal_pair<'a>(left: &'a Expr, right: &'a Expr) -> Option<(&'a str, &'a Expr)> {
    match (left, right) {
        (Expr::Literal(Literal::Text(text)), other) => Some((text.as_str(), other)),
        (other, Expr::Literal(Literal::Text(text))) => Some((text.as_str(), other)),
        _ => None,
    }
}

fn null_literal_pair<'a>(left: &'a Expr, right: &'a Expr) -> Option<&'a [String]> {
    match (left, right) {
        (Expr::Path(path), Expr::Literal(Literal::Null)) => Some(path),
        (Expr::Literal(Literal::Null), Expr::Path(path)) => Some(path),
        _ => None,
    }
}

fn compile_null_eq(
    op: &Op,
    path: &[String],
    env: &Env<'_>,
    func: &mut Function,
) -> Result<ExprType, DharmaError> {
    let info = resolve_path_info(path, env)?;
    if let TypeSpec::Optional(_) = info.typ {
        func.instruction(&Instruction::I32Const(info.base as i32));
        func.instruction(&Instruction::I32Const(info.offset as i32));
        func.instruction(&Instruction::I32Add);
        func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
            offset: 0,
            align: 0,
            memory_index: 0,
        }));
        func.instruction(&Instruction::I32Const(0));
        func.instruction(&Instruction::I32Eq);
        if *op == Op::Neq {
            func.instruction(&Instruction::I32Eqz);
        }
        return Ok(ExprType::Bool);
    }
    match info.typ {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
                offset: 0,
                align: 3,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I64Const(0));
            func.instruction(&Instruction::I64Eq);
        }
        TypeSpec::Bool => {
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
                offset: 0,
                align: 0,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I32Const(0));
            func.instruction(&Instruction::I32Eq);
        }
        TypeSpec::Enum(_) => {
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I32Const(0));
            func.instruction(&Instruction::I32Eq);
        }
        TypeSpec::Text(_) | TypeSpec::Currency => {
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I32Const(0));
            func.instruction(&Instruction::I32Eq);
        }
        TypeSpec::Identity | TypeSpec::Ref(_) => {
            let mut first = true;
            for i in 0..4 {
                func.instruction(&Instruction::I32Const(info.base as i32));
                func.instruction(&Instruction::I32Const((info.offset + (i * 8)) as i32));
                func.instruction(&Instruction::I32Add);
                func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
                    offset: 0,
                    align: 3,
                    memory_index: 0,
                }));
                func.instruction(&Instruction::I64Const(0));
                func.instruction(&Instruction::I64Eq);
                if first {
                    first = false;
                } else {
                    func.instruction(&Instruction::I32And);
                }
            }
        }
        TypeSpec::Ratio => {
            let mut first = true;
            for i in 0..2 {
                func.instruction(&Instruction::I32Const(info.base as i32));
                func.instruction(&Instruction::I32Const((info.offset + (i * 8)) as i32));
                func.instruction(&Instruction::I32Add);
                func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
                    offset: 0,
                    align: 3,
                    memory_index: 0,
                }));
                func.instruction(&Instruction::I64Const(0));
                func.instruction(&Instruction::I64Eq);
                if first {
                    first = false;
                } else {
                    func.instruction(&Instruction::I32And);
                }
            }
        }
        TypeSpec::GeoPoint => {
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const(info.offset as i32));
            func.instruction(&Instruction::I32Add);
            func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I32Const(0));
            func.instruction(&Instruction::I32Eq);
            func.instruction(&Instruction::I32Const(info.base as i32));
            func.instruction(&Instruction::I32Const((info.offset + 4) as i32));
            func.instruction(&Instruction::I32Add);
            func.instruction(&Instruction::I32Load(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I32Const(0));
            func.instruction(&Instruction::I32Eq);
            func.instruction(&Instruction::I32And);
        }
        TypeSpec::List(_) | TypeSpec::Map(_, _) | TypeSpec::SubjectRef(_) | TypeSpec::Struct(_) => {
            return Err(DharmaError::Validation("null comparison unsupported".to_string()));
        }
        TypeSpec::Optional(_) => {
            return Err(DharmaError::Validation("null comparison unsupported".to_string()));
        }
    }
    if *op == Op::Neq {
        func.instruction(&Instruction::I32Eqz);
    }
    Ok(ExprType::Bool)
}

fn compile_assignment(
    assignment: &Assignment,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    let target = assignment
        .target
        .get(0)
        .map(|s| s.as_str())
        .ok_or_else(|| DharmaError::Validation("assignment target must be state".to_string()))?;
    if target != "state" {
        return Err(DharmaError::Validation("assignment target must be state".to_string()));
    }
    let field = assignment
        .target
        .get(1)
        .ok_or_else(|| DharmaError::Validation("assignment target missing field".to_string()))?;
    let target_info = env
        .state
        .get(field)
        .ok_or_else(|| DharmaError::Validation("unknown state field".to_string()))?;
    let expr = &assignment.value;
    if let TypeSpec::Optional(inner) = &target_info.typ {
        if matches!(expr, Expr::Literal(Literal::Null)) {
            push_addr(func, target_info.base, target_info.offset);
            func.instruction(&Instruction::I32Const(0));
            func.instruction(&Instruction::I32Store8(wasm_encoder::MemArg {
                offset: 0,
                align: 0,
                memory_index: 0,
            }));
            let size = type_size(inner, &env._schema.structs);
            zero_bytes(func, target_info.base, target_info.offset + 1, size as u32);
            return Ok(());
        }
        if let Expr::Path(path) = expr {
            let source = resolve_path_info(path, env)?;
            if let TypeSpec::Optional(source_inner) = &source.typ {
                if **source_inner != **inner {
                    return Err(DharmaError::Validation(
                        "optional assignment type mismatch".to_string(),
                    ));
                }
                let size = 1 + type_size(inner, &env._schema.structs);
                copy_bytes(
                    func,
                    source.base,
                    source.offset,
                    target_info.base,
                    target_info.offset,
                    size,
                )?;
                return Ok(());
            }
        }
        push_addr(func, target_info.base, target_info.offset);
        func.instruction(&Instruction::I32Const(1));
        func.instruction(&Instruction::I32Store8(wasm_encoder::MemArg {
            offset: 0,
            align: 0,
            memory_index: 0,
        }));
        let inner_info = FieldInfo {
            base: target_info.base,
            offset: target_info.offset + 1,
            typ: *inner.clone(),
        };
        return compile_assignment_inner(&inner_info, expr, env, func);
    }
    compile_assignment_inner(target_info, expr, env, func)
}

fn compile_assignment_inner(
    target_info: &FieldInfo,
    expr: &Expr,
    env: &Env<'_>,
    func: &mut Function,
) -> Result<(), DharmaError> {
    match target_info.typ.clone() {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            push_addr(func, target_info.base, target_info.offset);
            let typ = compile_expr(&expr, env, func)?;
            if typ != ExprType::Int {
                return Err(DharmaError::Validation("expected int assignment".to_string()));
            }
            func.instruction(&Instruction::I64Store(wasm_encoder::MemArg {
                offset: 0,
                align: 3,
                memory_index: 0,
            }));
        }
        TypeSpec::Bool => {
            push_addr(func, target_info.base, target_info.offset);
            let typ = compile_expr(&expr, env, func)?;
            if typ != ExprType::Bool {
                return Err(DharmaError::Validation("expected bool assignment".to_string()));
            }
            func.instruction(&Instruction::I32Store8(wasm_encoder::MemArg {
                offset: 0,
                align: 0,
                memory_index: 0,
            }));
        }
        TypeSpec::Enum(_) => {
            push_addr(func, target_info.base, target_info.offset);
            if let Expr::Literal(Literal::Enum(name)) = expr {
                if let TypeSpec::Enum(variants) = &target_info.typ {
                    let idx = variants
                        .iter()
                        .position(|v| v == name)
                        .ok_or_else(|| DharmaError::Validation("unknown enum literal".to_string()))?;
                    func.instruction(&Instruction::I64Const(idx as i64));
                    func.instruction(&Instruction::I32WrapI64);
                }
            } else {
                let typ = compile_expr(&expr, env, func)?;
                if typ != ExprType::Int {
                    return Err(DharmaError::Validation("expected enum assignment".to_string()));
                }
                func.instruction(&Instruction::I32WrapI64);
            }
            func.instruction(&Instruction::I32Store(wasm_encoder::MemArg {
                offset: 0,
                align: 2,
                memory_index: 0,
            }));
        }
        TypeSpec::Identity | TypeSpec::Ref(_) => {
            if let Expr::Path(path) = expr {
                let source = resolve_path_info(path, env)?;
                if !matches!(source.typ, TypeSpec::Identity | TypeSpec::Ref(_)) {
                    return Err(DharmaError::Validation("expected identity source".to_string()));
                }
                copy_bytes(
                    func,
                    source.base,
                    source.offset,
                    target_info.base,
                    target_info.offset,
                    32,
                )?;
            } else {
                return Err(DharmaError::Validation("expected identity assignment".to_string()));
            }
        }
        TypeSpec::SubjectRef(_) => {
            if let Expr::Path(path) = expr {
                let source = resolve_path_info(path, env)?;
                if !matches!(source.typ, TypeSpec::SubjectRef(_)) {
                    return Err(DharmaError::Validation(
                        "expected subject_ref source".to_string(),
                    ));
                }
                copy_bytes(
                    func,
                    source.base,
                    source.offset,
                    target_info.base,
                    target_info.offset,
                    40,
                )?;
            } else {
                return Err(DharmaError::Validation(
                    "expected subject_ref assignment".to_string(),
                ));
            }
        }
        TypeSpec::Ratio => {
            if let Expr::Path(path) = expr {
                let source = resolve_path_info(path, env)?;
                if !matches!(source.typ, TypeSpec::Ratio) {
                    return Err(DharmaError::Validation("expected ratio source".to_string()));
                }
                copy_bytes(
                    func,
                    source.base,
                    source.offset,
                    target_info.base,
                    target_info.offset,
                    16,
                )?;
            } else {
                return Err(DharmaError::Validation("expected ratio assignment".to_string()));
            }
        }
        TypeSpec::Text(len) => {
            push_addr(func, target_info.base, target_info.offset);
            emit_store_value_at_ptr(&TypeSpec::Text(len), expr, env, func)?;
        }
        TypeSpec::Currency => {
            push_addr(func, target_info.base, target_info.offset);
            emit_store_value_at_ptr(&TypeSpec::Currency, expr, env, func)?;
        }
        TypeSpec::GeoPoint => {
            if let Expr::Path(path) = expr {
                let source = resolve_path_info(path, env)?;
                if !matches!(source.typ, TypeSpec::GeoPoint) {
                    return Err(DharmaError::Validation("expected geopoint source".to_string()));
                }
                copy_bytes(
                    func,
                    source.base,
                    source.offset,
                    target_info.base,
                    target_info.offset,
                    8,
                )?;
            } else {
                return Err(DharmaError::Validation("expected geopoint assignment".to_string()));
            }
        }
        TypeSpec::List(inner) => {
            if let Expr::Path(path) = expr {
                let source = resolve_path_info(path, env)?;
                if !matches!(source.typ, TypeSpec::List(_)) {
                    return Err(DharmaError::Validation("expected list source".to_string()));
                }
                let size = type_size(&target_info.typ, &env._schema.structs);
                copy_bytes(
                    func,
                    source.base,
                    source.offset,
                    target_info.base,
                    target_info.offset,
                    size,
                )?;
                return Ok(());
            }
            return compile_list_mutation(target_info, &inner, expr, env, func);
        }
        TypeSpec::Map(key, val) => {
            if let Expr::Path(path) = expr {
                let source = resolve_path_info(path, env)?;
                if !matches!(source.typ, TypeSpec::Map(_, _)) {
                    return Err(DharmaError::Validation("expected map source".to_string()));
                }
                let size = type_size(&target_info.typ, &env._schema.structs);
                copy_bytes(
                    func,
                    source.base,
                    source.offset,
                    target_info.base,
                    target_info.offset,
                    size,
                )?;
                return Ok(());
            }
            return compile_map_mutation(target_info, &key, &val, expr, env, func);
        }
        TypeSpec::Optional(_) => {
            return Err(DharmaError::Validation("optional assignment unsupported".to_string()));
        }
        TypeSpec::Struct(_) => {
            push_addr(func, target_info.base, target_info.offset);
            emit_store_value_at_ptr(&target_info.typ, expr, env, func)?;
            return Ok(());
        }
    }
    Ok(())
}

fn push_addr(func: &mut Function, base: u32, offset: u32) {
    func.instruction(&Instruction::I32Const(base as i32));
    func.instruction(&Instruction::I32Const(offset as i32));
    func.instruction(&Instruction::I32Add);
}

fn copy_bytes(
    func: &mut Function,
    src_base: u32,
    src_offset: u32,
    dst_base: u32,
    dst_offset: u32,
    len: usize,
) -> Result<(), DharmaError> {
    func.instruction(&Instruction::I32Const(dst_base as i32));
    func.instruction(&Instruction::I32Const(dst_offset as i32));
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::I32Const(src_base as i32));
    func.instruction(&Instruction::I32Const(src_offset as i32));
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::I32Const(len as i32));
    func.instruction(&Instruction::MemoryCopy { src_mem: 0, dst_mem: 0 });
    Ok(())
}

fn zero_bytes(func: &mut Function, base: u32, offset: u32, len: u32) {
    push_addr(func, base, offset);
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::I32Const(len as i32));
    func.instruction(&Instruction::MemoryFill(0));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdl::parser;
    use wasmi::{Caller, Engine, Linker, Module, Store};

    fn linker_with_has_role(engine: &Engine) -> Linker<()> {
        let mut linker = Linker::new(engine);
        linker
            .func_wrap(
                "env",
                "has_role",
                |_caller: Caller<'_, ()>, _subject: i32, _identity: i32, _role: i32| -> i32 { 0 },
            )
            .unwrap();
        linker
            .func_wrap("env", "read_int", |_caller: Caller<'_, ()>, _sub: i32, _path: i32| -> i64 {
                0
            })
            .unwrap();
        linker
            .func_wrap("env", "read_bool", |_caller: Caller<'_, ()>, _sub: i32, _path: i32| -> i32 {
                0
            })
            .unwrap();
        linker
            .func_wrap(
                "env",
                "read_text",
                |_caller: Caller<'_, ()>, _sub: i32, _path: i32, out: i32| -> i32 { out },
            )
            .unwrap();
        linker
            .func_wrap(
                "env",
                "read_identity",
                |_caller: Caller<'_, ()>, _sub: i32, _path: i32, out: i32| -> i32 { out },
            )
            .unwrap();
        linker
            .func_wrap(
                "env",
                "read_subject_ref",
                |_caller: Caller<'_, ()>, _sub: i32, _path: i32, out: i32| -> i32 { out },
            )
            .unwrap();
        linker
            .func_wrap(
                "env",
                "subject_id",
                |_caller: Caller<'_, ()>, _sub: i32, out: i32| -> i32 { out },
            )
            .unwrap();
        linker
            .func_wrap(
                "env",
                "remote_intersects",
                |_caller: Caller<'_, ()>,
                 _sub: i32,
                 _path: i32,
                 _list: i32,
                 _kind: i32,
                 _size: i32|
                 -> i32 { 0 },
            )
            .unwrap();
        linker
            .func_wrap(
                "env",
                "normalize_text_list",
                |_caller: Caller<'_, ()>, _ptr: i32, _max: i32, _cap: i32| -> i32 { 0 },
            )
            .unwrap();
        linker
    }

    #[test]
    fn compile_produces_valid_wasm() {
        let doc = r#"```dhl
aggregate Dummy
    state
        count: Int = 1

action Touch(delta: Int)
    validate
        delta > 0
    apply
        state.count = state.count + delta
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let validate = instance
            .get_typed_func::<(), i32>(&store, "validate")
            .unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();
        // setup state
        memory
            .write(&mut store, 0, &1i64.to_le_bytes())
            .unwrap();
        // setup args (action id 0, delta 3)
        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &3i64.to_le_bytes())
            .unwrap();
        let result = validate.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);
        let mut buf = [0u8; 8];
        memory.read(&mut store, 0, &mut buf).unwrap();
        let value = i64::from_le_bytes(buf);
        assert_eq!(value, 4);
    }

    #[test]
    fn invariant_rejects_after_reduce() {
        let doc = r#"```dhl
aggregate Counter
    state
        count: Int = 0
    invariant
        state.count >= 0

action Bump(delta: Int)
    apply
        state.count = state.count + delta
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();
        memory
            .write(&mut store, 0, &0i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &(-1i64).to_le_bytes())
            .unwrap();
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn list_len_contains_and_index_work() {
        let doc = r#"```dhl
aggregate Box
    state
        nums: List<Int>

action Check(val: Int)
    validate
        len(state.nums) == 3
        contains(state.nums, val)
        state.nums[1] == 4
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let validate = instance
            .get_typed_func::<(), i32>(&store, "validate")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        memory
            .write(&mut store, 0, &3u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 4, &2i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 12, &4i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 20, &6i64.to_le_bytes())
            .unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &4i64.to_le_bytes())
            .unwrap();
        let result = validate.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        memory
            .write(&mut store, ARGS_BASE as usize + 4, &5i64.to_le_bytes())
            .unwrap();
        let result = validate.call(&mut store, ()).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn map_contains_and_index_work() {
        let doc = r#"```dhl
aggregate Box
    state
        pairs: Map<Int, Int>

action Check(key: Int)
    validate
        contains(state.pairs, key)
        state.pairs[key] == 7
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let validate = instance
            .get_typed_func::<(), i32>(&store, "validate")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        memory
            .write(&mut store, 0, &2u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 4, &2i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 12, &7i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 20, &5i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 28, &9i64.to_le_bytes())
            .unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &2i64.to_le_bytes())
            .unwrap();
        let result = validate.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        memory
            .write(&mut store, ARGS_BASE as usize + 4, &5i64.to_le_bytes())
            .unwrap();
        let result = validate.call(&mut store, ()).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn list_push_mutates_state() {
        let doc = r#"```dhl
aggregate Box
    state
        nums: List<Int>

action Add(val: Int)
    apply
        state.nums.push(val)
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &5i64.to_le_bytes())
            .unwrap();
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);
        let mut len_buf = [0u8; 4];
        memory.read(&mut store, 0, &mut len_buf).unwrap();
        assert_eq!(u32::from_le_bytes(len_buf), 1);
        let mut val_buf = [0u8; 8];
        memory.read(&mut store, 4, &mut val_buf).unwrap();
        assert_eq!(i64::from_le_bytes(val_buf), 5);
    }

    #[test]
    fn list_remove_is_stable() {
        let doc = r#"```dhl
aggregate Box
    state
        nums: List<Int>

action Remove(val: Int)
    apply
        state.nums.remove(val)
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        memory
            .write(&mut store, 0, &4u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 4, &1i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 12, &2i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 20, &3i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 28, &4i64.to_le_bytes())
            .unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &2i64.to_le_bytes())
            .unwrap();
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut len_buf = [0u8; 4];
        memory.read(&mut store, 0, &mut len_buf).unwrap();
        assert_eq!(u32::from_le_bytes(len_buf), 3);

        let mut val_buf = [0u8; 8];
        memory.read(&mut store, 4, &mut val_buf).unwrap();
        assert_eq!(i64::from_le_bytes(val_buf), 1);
        memory.read(&mut store, 12, &mut val_buf).unwrap();
        assert_eq!(i64::from_le_bytes(val_buf), 3);
        memory.read(&mut store, 20, &mut val_buf).unwrap();
        assert_eq!(i64::from_le_bytes(val_buf), 4);
    }

    #[test]
    fn list_remove_text() {
        let doc = r#"```dhl
aggregate Box
    state
        words: List<Text(len=4)>

action Remove(word: Text)
    apply
        state.words.remove(word)
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        let write_text = |mem: &wasmi::Memory,
                          store: &mut wasmi::Store<()>,
                          offset: usize,
                          max_len: usize,
                          text: &str| {
            let bytes = text.as_bytes();
            let len = bytes.len().min(max_len);
            mem.write(&mut *store, offset, &(len as u32).to_le_bytes()).unwrap();
            mem.write(&mut *store, offset + 4, &bytes[..len]).unwrap();
            if len < max_len {
                let zeros = vec![0u8; max_len - len];
                mem.write(&mut *store, offset + 4 + len, &zeros).unwrap();
            }
        };

        memory
            .write(&mut store, 0, &2u32.to_le_bytes())
            .unwrap();
        write_text(&memory, &mut store, 4, 4, "abcd");
        write_text(&memory, &mut store, 4 + 4 + 4, 4, "wxyz");

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        write_text(&memory, &mut store, ARGS_BASE as usize + 4, 4, "abcd");
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut len_buf = [0u8; 4];
        memory.read(&mut store, 0, &mut len_buf).unwrap();
        assert_eq!(u32::from_le_bytes(len_buf), 1);

        let mut text_len = [0u8; 4];
        memory.read(&mut store, 4, &mut text_len).unwrap();
        let len = u32::from_le_bytes(text_len) as usize;
        let mut text_buf = vec![0u8; len];
        memory.read(&mut store, 8, &mut text_buf).unwrap();
        assert_eq!(String::from_utf8_lossy(&text_buf), "wxyz");
    }

    #[test]
    fn list_remove_identity() {
        let doc = r#"```dhl
aggregate Box
    state
        ids: List<Identity>

action Remove(who: Identity)
    apply
        state.ids.remove(who)
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        let id_a = [1u8; 32];
        let id_b = [2u8; 32];
        memory
            .write(&mut store, 0, &2u32.to_le_bytes())
            .unwrap();
        memory.write(&mut store, 4, &id_a).unwrap();
        memory.write(&mut store, 36, &id_b).unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &id_a)
            .unwrap();
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut len_buf = [0u8; 4];
        memory.read(&mut store, 0, &mut len_buf).unwrap();
        assert_eq!(u32::from_le_bytes(len_buf), 1);
        let mut out = [0u8; 32];
        memory.read(&mut store, 4, &mut out).unwrap();
        assert_eq!(out, id_b);
    }

    #[test]
    fn map_index_identity_value_returns_pointer() {
        let doc = r#"```dhl
aggregate Box
    state
        owners: Map<Int, Identity>

action Check(id: Identity)
    validate
        state.owners[1] == args.id
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let validate = instance
            .get_typed_func::<(), i32>(&store, "validate")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        memory
            .write(&mut store, 0, &1u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 4, &1i64.to_le_bytes())
            .unwrap();
        let id = [9u8; 32];
        memory.write(&mut store, 12, &id).unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &id)
            .unwrap();
        let result = validate.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn text_literal_equals_path() {
        let doc = r#"```dhl
aggregate Box
    state
        name: Text(len=4)

action Check()
    validate
        state.name == "wxyz"
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let validate = instance
            .get_typed_func::<(), i32>(&store, "validate")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        memory
            .write(&mut store, 0, &4u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 4, b"wxyz")
            .unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        let result = validate.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn text_literal_assignment_writes_value() {
        let doc = r#"```dhl
aggregate Box
    state
        name: Text(len=4)

action Set()
    apply
        state.name = "wxyz"
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut len_buf = [0u8; 4];
        memory.read(&mut store, 0, &mut len_buf).unwrap();
        assert_eq!(u32::from_le_bytes(len_buf), 4);
        let mut out = [0u8; 4];
        memory.read(&mut store, 4, &mut out).unwrap();
        assert_eq!(&out, b"wxyz");
    }

    #[test]
    fn text_concat_nested_literals() {
        let doc = r#"```dhl
aggregate Box
    state
        name: Text(len=4)

action Set()
    apply
        state.name = "a" + "b" + "c"
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut len_buf = [0u8; 4];
        memory.read(&mut store, 0, &mut len_buf).unwrap();
        assert_eq!(u32::from_le_bytes(len_buf), 3);
        let mut out = [0u8; 3];
        memory.read(&mut store, 4, &mut out).unwrap();
        assert_eq!(&out, b"abc");
    }

    #[test]
    fn list_push_text_literal_writes_value() {
        let doc = r#"```dhl
aggregate Box
    state
        words: List<Text(len=4)>

action Add
    apply
        state.words.push("wxyz")
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut len_buf = [0u8; 4];
        memory.read(&mut store, 0, &mut len_buf).unwrap();
        assert_eq!(u32::from_le_bytes(len_buf), 1);
        let mut text_len = [0u8; 4];
        memory.read(&mut store, 4, &mut text_len).unwrap();
        assert_eq!(u32::from_le_bytes(text_len), 4);
        let mut text_buf = [0u8; 4];
        memory.read(&mut store, 8, &mut text_buf).unwrap();
        assert_eq!(&text_buf, b"wxyz");
    }

    #[test]
    fn list_push_identity_from_args() {
        let doc = r#"```dhl
aggregate Box
    state
        ids: List<Identity>

action Add(who: Identity)
    apply
        state.ids.push(who)
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        let id = [7u8; 32];
        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &id)
            .unwrap();
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut len_buf = [0u8; 4];
        memory.read(&mut store, 0, &mut len_buf).unwrap();
        assert_eq!(u32::from_le_bytes(len_buf), 1);
        let mut out = [0u8; 32];
        memory.read(&mut store, 4, &mut out).unwrap();
        assert_eq!(out, id);
    }

    #[test]
    fn map_set_text_literal_value() {
        let doc = r#"```dhl
aggregate Box
    state
        names: Map<Int, Text(len=4)>

action Put
    apply
        state.names.set(7, "wxyz")
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut len_buf = [0u8; 4];
        memory.read(&mut store, 0, &mut len_buf).unwrap();
        assert_eq!(u32::from_le_bytes(len_buf), 1);
        let mut key_buf = [0u8; 8];
        memory.read(&mut store, 4, &mut key_buf).unwrap();
        assert_eq!(i64::from_le_bytes(key_buf), 7);
        let mut text_len = [0u8; 4];
        memory.read(&mut store, 12, &mut text_len).unwrap();
        assert_eq!(u32::from_le_bytes(text_len), 4);
        let mut text_buf = [0u8; 4];
        memory.read(&mut store, 16, &mut text_buf).unwrap();
        assert_eq!(&text_buf, b"wxyz");
    }

    #[test]
    fn map_set_identity_value_from_args() {
        let doc = r#"```dhl
aggregate Box
    state
        owners: Map<Int, Identity>

action Put(key: Int, who: Identity)
    apply
        state.owners.set(key, who)
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        let id = [3u8; 32];
        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &9i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 12, &id)
            .unwrap();
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut len_buf = [0u8; 4];
        memory.read(&mut store, 0, &mut len_buf).unwrap();
        assert_eq!(u32::from_le_bytes(len_buf), 1);
        let mut key_buf = [0u8; 8];
        memory.read(&mut store, 4, &mut key_buf).unwrap();
        assert_eq!(i64::from_le_bytes(key_buf), 9);
        let mut out = [0u8; 32];
        memory.read(&mut store, 12, &mut out).unwrap();
        assert_eq!(out, id);
    }

    #[test]
    fn map_set_mutates_state() {
        let doc = r#"```dhl
aggregate Box
    state
        pairs: Map<Int, Int>

action Put(key: Int, val: Int)
    apply
        state.pairs.set(key, val)
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &2i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 12, &7i64.to_le_bytes())
            .unwrap();
        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut len_buf = [0u8; 4];
        memory.read(&mut store, 0, &mut len_buf).unwrap();
        assert_eq!(u32::from_le_bytes(len_buf), 1);
        let mut key_buf = [0u8; 8];
        memory.read(&mut store, 4, &mut key_buf).unwrap();
        assert_eq!(i64::from_le_bytes(key_buf), 2);
        let mut val_buf = [0u8; 8];
        memory.read(&mut store, 12, &mut val_buf).unwrap();
        assert_eq!(i64::from_le_bytes(val_buf), 7);
    }

    #[test]
    fn ratio_assignment_copies_bytes() {
        let doc = r#"```dhl
aggregate Box
    state
        frac: Ratio

action Set(frac: Ratio)
    apply
        state.frac = frac
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &3i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 12, &7i64.to_le_bytes())
            .unwrap();

        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut num_buf = [0u8; 8];
        let mut den_buf = [0u8; 8];
        memory.read(&mut store, 0, &mut num_buf).unwrap();
        memory.read(&mut store, 8, &mut den_buf).unwrap();
        assert_eq!(i64::from_le_bytes(num_buf), 3);
        assert_eq!(i64::from_le_bytes(den_buf), 7);
    }

    #[test]
    fn sum_list_int_writes_total() {
        let doc = r#"```dhl
aggregate Box
    state
        nums: List<Int>
        total: Int

action Calc()
    apply
        state.total = sum(state.nums)
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        let list_type = TypeSpec::List(Box::new(TypeSpec::Int));
        let list_size = type_size(&list_type, &BTreeMap::new());
        memory
            .write(&mut store, 0, &3u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 4, &2i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 12, &5i64.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 20, &7i64.to_le_bytes())
            .unwrap();

        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut total_buf = [0u8; 8];
        memory
            .read(&mut store, list_size, &mut total_buf)
            .unwrap();
        assert_eq!(i64::from_le_bytes(total_buf), 14);
    }

    #[test]
    fn distance_computes_manhattan_scaled() {
        let doc = r#"```dhl
aggregate Box
    state
        a: GeoPoint
        b: GeoPoint
        dist: Int

action Calc()
    apply
        state.dist = distance(state.a, state.b)
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        // a.lat = 0, a.lon = 0
        memory
            .write(&mut store, 0, &0i32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 4, &0i32.to_le_bytes())
            .unwrap();
        // b.lat = 1_000_000, b.lon = 0 (e7 degrees)
        memory
            .write(&mut store, 8, &1_000_000i32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, 12, &0i32.to_le_bytes())
            .unwrap();

        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut dist_buf = [0u8; 8];
        memory.read(&mut store, 16, &mut dist_buf).unwrap();
        let dist = i64::from_le_bytes(dist_buf);
        assert_eq!(dist, 11_132);
    }

    #[test]
    fn geopoint_assignment_copies_bytes() {
        let doc = r#"```dhl
aggregate Box
    state
        spot: GeoPoint

action Set(spot: GeoPoint)
    apply
        state.spot = spot
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile(&ast).unwrap();
        let engine = Engine::default();
        let module = Module::new(&engine, &bytes).unwrap();
        let mut store = Store::new(&engine, ());
        let linker = linker_with_has_role(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap().start(&mut store).unwrap();
        let reduce = instance
            .get_typed_func::<(), i32>(&store, "reduce")
            .unwrap();
        let memory = instance.get_memory(&store, "memory").unwrap();

        memory
            .write(&mut store, ARGS_BASE as usize, &0u32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 4, &123i32.to_le_bytes())
            .unwrap();
        memory
            .write(&mut store, ARGS_BASE as usize + 8, &(-456i32).to_le_bytes())
            .unwrap();

        let result = reduce.call(&mut store, ()).unwrap();
        assert_eq!(result, 0);

        let mut lat_buf = [0u8; 4];
        let mut lon_buf = [0u8; 4];
        memory.read(&mut store, 0, &mut lat_buf).unwrap();
        memory.read(&mut store, 4, &mut lon_buf).unwrap();
        assert_eq!(i32::from_le_bytes(lat_buf), 123);
        assert_eq!(i32::from_le_bytes(lon_buf), -456);
    }
}
