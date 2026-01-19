use crate::error::DharmaError;
use crate::pdl::ast::{ActionDef, Assignment, AstFile, Expr, Literal, Op, SourceSpan, TypeSpec};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
enum ExprType {
    Numeric,
    Bool,
    Text,
    Enum(Vec<String>),
    EnumLit(String),
    Identity,
    GeoPoint,
    Ratio,
    List(Box<ExprType>),
    Map(Box<ExprType>, Box<ExprType>),
    Optional(Box<ExprType>),
    Null,
    Unknown,
}

struct TypeEnv {
    state: HashMap<String, TypeSpec>,
    args: HashMap<String, TypeSpec>,
    context: HashMap<String, TypeSpec>,
}

pub fn check_ast(ast: &AstFile) -> Result<(), DharmaError> {
    let aggregate = ast
        .aggregates
        .first()
        .ok_or_else(|| DharmaError::Validation("missing aggregate".to_string()))?;
    let mut state = HashMap::new();
    for field in &aggregate.fields {
        state.insert(field.name.clone(), field.typ.clone());
    }
    let mut context = HashMap::new();
    context.insert("context.signer".to_string(), TypeSpec::Identity);
    context.insert("context.clock.time".to_string(), TypeSpec::Timestamp);
    let base_env = TypeEnv {
        state,
        args: HashMap::new(),
        context,
    };

    for expr in &aggregate.invariants {
        let typ = with_span(&expr.span, type_of(&expr.value, &base_env))?;
        with_span(&expr.span, ensure_bool(&typ))?;
    }

    for action in &ast.actions {
        check_action(action, &base_env)?;
    }
    enforce_external(ast)?;
    Ok(())
}

fn enforce_external(ast: &AstFile) -> Result<(), DharmaError> {
    let ext = ast.external.as_ref();
    let roles = ext.map(|e| e.roles.as_slice()).unwrap_or(&[]);
    let time = ext.map(|e| e.time.as_slice()).unwrap_or(&[]);
    let datasets = ext.map(|e| e.datasets.as_slice()).unwrap_or(&[]);
    for aggregate in &ast.aggregates {
        for expr in &aggregate.invariants {
            with_span(&expr.span, validate_external_expr(&expr.value, roles, time, datasets))?;
        }
    }
    for action in &ast.actions {
        for expr in &action.validates {
            with_span(&expr.span, validate_external_expr(&expr.value, roles, time, datasets))?;
        }
        for assignment in &action.applies {
            with_span(
                &assignment.span,
                validate_external_expr(&assignment.value.value, roles, time, datasets),
            )?;
        }
    }
    for reactor in &ast.reactors {
        for expr in &reactor.validates {
            with_span(&expr.span, validate_external_expr(&expr.value, roles, time, datasets))?;
        }
        for emit in &reactor.emits {
            for (_, expr) in &emit.args {
                with_span(&expr.span, validate_external_expr(&expr.value, roles, time, datasets))?;
            }
        }
    }
    Ok(())
}

fn with_span<T>(span: &SourceSpan, result: Result<T, DharmaError>) -> Result<T, DharmaError> {
    match result {
        Ok(value) => Ok(value),
        Err(DharmaError::Validation(msg)) => Err(DharmaError::Validation(format!(
            "{msg} at line {}:{}\n  {}",
            span.line, span.column, span.text
        ))),
        Err(err) => Err(err),
    }
}

fn validate_external_expr(
    expr: &Expr,
    roles: &[String],
    time: &[String],
    datasets: &[String],
) -> Result<(), DharmaError> {
    match expr {
        Expr::Call(name, args) => {
            match name.as_str() {
                "has_role" => {
                    if roles.is_empty() {
                        return Err(DharmaError::Validation(
                            "has_role requires external.roles".to_string(),
                        ));
                    }
                    let role_expr = args.last().ok_or_else(|| {
                        DharmaError::Validation("has_role expects role argument".to_string())
                    })?;
                    let role = role_literal(role_expr).ok_or_else(|| {
                        DharmaError::Validation("has_role role must be literal".to_string())
                    })?;
                    if !roles.iter().any(|r| r == &role) {
                        return Err(DharmaError::Validation(format!(
                            "role '{}' not declared in external.roles",
                            role
                        )));
                    }
                }
                "now" => {
                    if time.is_empty() {
                        return Err(DharmaError::Validation(
                            "now() requires external.time".to_string(),
                        ));
                    }
                }
                "dataset" => {
                    let name = dataset_literal(args).ok_or_else(|| {
                        DharmaError::Validation("dataset() expects literal name".to_string())
                    })?;
                    ensure_dataset_declared(&name, datasets)?;
                }
                _ => {}
            }
            for arg in args {
                validate_external_expr(arg, roles, time, datasets)?;
            }
        }
        Expr::Path(path) => {
            if is_dataset_path(path) {
                let dataset = dataset_from_path(path)?;
                ensure_dataset_declared(&dataset, datasets)?;
            }
        }
        Expr::BinaryOp(_, left, right) => {
            validate_external_expr(left, roles, time, datasets)?;
            validate_external_expr(right, roles, time, datasets)?;
        }
        Expr::UnaryOp(_, inner) => {
            validate_external_expr(inner, roles, time, datasets)?;
        }
        Expr::Literal(_) => {}
    }
    Ok(())
}

fn role_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(Literal::Enum(value)) => Some(value.clone()),
        Expr::Literal(Literal::Text(value)) => Some(value.clone()),
        _ => None,
    }
}

fn dataset_literal(args: &[Expr]) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    match &args[0] {
        Expr::Literal(Literal::Text(value)) => Some(value.clone()),
        Expr::Literal(Literal::Enum(value)) => Some(value.clone()),
        _ => None,
    }
}

fn is_dataset_path(path: &[String]) -> bool {
    matches!(path.first().map(|s| s.as_str()), Some("dataset" | "datasets"))
}

fn dataset_from_path(path: &[String]) -> Result<String, DharmaError> {
    if path.len() < 2 {
        return Err(DharmaError::Validation(
            "dataset reference missing id".to_string(),
        ));
    }
    Ok(path[1..].join("."))
}

fn ensure_dataset_declared(dataset: &str, datasets: &[String]) -> Result<(), DharmaError> {
    if datasets.is_empty() {
        return Err(DharmaError::Validation(
            "dataset reference requires external.datasets".to_string(),
        ));
    }
    if !datasets.iter().any(|d| d == dataset) {
        return Err(DharmaError::Validation(format!(
            "dataset '{}' not declared in external.datasets",
            dataset
        )));
    }
    Ok(())
}

fn check_action(action: &ActionDef, base_env: &TypeEnv) -> Result<(), DharmaError> {
    let mut args = HashMap::new();
    for arg in &action.args {
        args.insert(arg.name.clone(), arg.typ.clone());
    }
    let env = TypeEnv {
        state: base_env.state.clone(),
        args,
        context: base_env.context.clone(),
    };
    for expr in &action.validates {
        let typ = with_span(&expr.span, type_of(&expr.value, &env))?;
        with_span(&expr.span, ensure_bool(&typ))?;
    }
    for assignment in &action.applies {
        with_span(&assignment.span, check_assignment(&assignment.value, &env))?;
    }
    Ok(())
}

fn check_assignment(assign: &Assignment, env: &TypeEnv) -> Result<(), DharmaError> {
    let target = resolve_target(assign, env)?;
    if let Expr::Literal(Literal::Null) = assign.value {
        if matches!(target, TypeSpec::Optional(_)) {
            return Ok(());
        }
        return Err(DharmaError::Validation("null assignment requires optional".to_string()));
    }
    if let Expr::Call(name, args) = &assign.value {
        match name.as_str() {
            "push" | "remove" => {
                return check_list_call(name, args, &target, env);
            }
            "set" => {
                return check_map_call(args, &target, env);
            }
            _ => {}
        }
    }
    if matches!(target, TypeSpec::Text(_) | TypeSpec::Currency) {
        if let Expr::BinaryOp(Op::Add, _, _) = assign.value {
            ensure_text_concat(&assign.value, env)?;
            return Ok(());
        }
    }
    let expr_type = type_of(&assign.value, env)?;
    ensure_assignable(&target, &expr_type)?;
    Ok(())
}

fn ensure_text_concat(expr: &Expr, env: &TypeEnv) -> Result<(), DharmaError> {
    match expr {
        Expr::BinaryOp(Op::Add, left, right) => {
            ensure_text_concat(left, env)?;
            ensure_text_concat(right, env)?;
            Ok(())
        }
        Expr::Literal(Literal::Text(_)) => Ok(()),
        Expr::Path(path) => {
            let typ = resolve_path(path, env)?;
            if matches!(typ, TypeSpec::Text(_) | TypeSpec::Currency) {
                Ok(())
            } else {
                Err(DharmaError::Validation(
                    "concat expects text path".to_string(),
                ))
            }
        }
        _ => Err(DharmaError::Validation(
            "concat expects text literal or path".to_string(),
        )),
    }
}

fn resolve_target(assign: &Assignment, env: &TypeEnv) -> Result<TypeSpec, DharmaError> {
    let target = assign
        .target
        .get(0)
        .map(|s| s.as_str())
        .ok_or_else(|| DharmaError::Validation("assignment target must be state".to_string()))?;
    if target != "state" {
        return Err(DharmaError::Validation("assignment target must be state".to_string()));
    }
    let field = assign
        .target
        .get(1)
        .ok_or_else(|| DharmaError::Validation("assignment target missing field".to_string()))?;
    env.state
        .get(field)
        .cloned()
        .ok_or_else(|| DharmaError::Validation("unknown state field".to_string()))
}

fn check_list_call(
    name: &str,
    args: &[Expr],
    target: &TypeSpec,
    env: &TypeEnv,
) -> Result<(), DharmaError> {
    let TypeSpec::List(elem_type) = target else {
        return Err(DharmaError::Validation("list operation on non-list".to_string()));
    };
    if args.len() != 2 {
        return Err(DharmaError::Validation("list operation expects one arg".to_string()));
    }
    let list_expr_type = type_of(&args[0], env)?;
    match strip_optional(&list_expr_type) {
        ExprType::List(_) => {}
        _ => {
            return Err(DharmaError::Validation(
                "list operation requires list target".to_string(),
            ))
        }
    }
    let item_type = type_of(&args[1], env)?;
    ensure_assignable(elem_type, &item_type)
        .map_err(|_| DharmaError::Validation(format!("list.{name} type mismatch")))?;
    Ok(())
}

fn check_map_call(args: &[Expr], target: &TypeSpec, env: &TypeEnv) -> Result<(), DharmaError> {
    let TypeSpec::Map(key_type, value_type) = target else {
        return Err(DharmaError::Validation("map operation on non-map".to_string()));
    };
    if args.len() != 3 {
        return Err(DharmaError::Validation("map.set expects two args".to_string()));
    }
    let map_expr_type = type_of(&args[0], env)?;
    match strip_optional(&map_expr_type) {
        ExprType::Map(_, _) => {}
        _ => {
            return Err(DharmaError::Validation(
                "map operation requires map target".to_string(),
            ))
        }
    }
    let key_expr_type = type_of(&args[1], env)?;
    ensure_assignable(key_type, &key_expr_type)
        .map_err(|_| DharmaError::Validation("map.set key type mismatch".to_string()))?;
    let val_expr_type = type_of(&args[2], env)?;
    ensure_assignable(value_type, &val_expr_type)
        .map_err(|_| DharmaError::Validation("map.set value type mismatch".to_string()))?;
    Ok(())
}

fn type_of(expr: &Expr, env: &TypeEnv) -> Result<ExprType, DharmaError> {
    match expr {
        Expr::Literal(lit) => type_of_literal(lit, env),
        Expr::Path(path) => {
            let typ = resolve_path(path, env)?;
            Ok(expr_type_from_spec(&typ))
        }
        Expr::UnaryOp(op, inner) => match op {
            Op::Not => {
                let typ = type_of(inner, env)?;
                ensure_bool(&typ)?;
                Ok(ExprType::Bool)
            }
            Op::Neg => {
                let typ = type_of(inner, env)?;
                ensure_numeric(&typ)?;
                Ok(ExprType::Numeric)
            }
            _ => Err(DharmaError::Validation("invalid unary op".to_string())),
        },
        Expr::BinaryOp(op, left, right) => match op {
            Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Mod => {
                let left_type = type_of(left, env)?;
                let right_type = type_of(right, env)?;
                ensure_numeric(&left_type)?;
                ensure_numeric(&right_type)?;
                Ok(ExprType::Numeric)
            }
            Op::Gt | Op::Lt | Op::Gte | Op::Lte => {
                let left_type = type_of(left, env)?;
                let right_type = type_of(right, env)?;
                ensure_numeric(&left_type)?;
                ensure_numeric(&right_type)?;
                Ok(ExprType::Bool)
            }
            Op::And | Op::Or => {
                let left_type = type_of(left, env)?;
                let right_type = type_of(right, env)?;
                ensure_bool(&left_type)?;
                ensure_bool(&right_type)?;
                Ok(ExprType::Bool)
            }
            Op::Eq | Op::Neq => {
                let left_type = type_of(left, env)?;
                let right_type = type_of(right, env)?;
                ensure_eq_compatible(&left_type, &right_type)?;
                Ok(ExprType::Bool)
            }
            Op::In => check_in_expr(left, right, env),
            _ => Err(DharmaError::Validation("unsupported binary op".to_string())),
        },
        Expr::Call(name, args) => match name.as_str() {
            "len" => check_len(args, env),
            "contains" => check_contains(args, env),
            "index" | "get" => check_index(args, env),
            "has_role" => check_has_role(args, env),
            "now" => Ok(ExprType::Numeric),
            "distance" => check_distance(args, env),
            "sum" => check_sum(args, env),
            "push" | "remove" | "set" => {
                Err(DharmaError::Validation("mutation calls not allowed here".to_string()))
            }
            _ => Err(DharmaError::Validation("unknown function".to_string())),
        },
    }
}

fn type_of_literal(lit: &Literal, env: &TypeEnv) -> Result<ExprType, DharmaError> {
    match lit {
        Literal::Int(_) => Ok(ExprType::Numeric),
        Literal::Bool(_) => Ok(ExprType::Bool),
        Literal::Text(_) => Ok(ExprType::Text),
        Literal::Enum(name) => Ok(ExprType::EnumLit(name.clone())),
        Literal::Null => Ok(ExprType::Null),
        Literal::List(items) => {
            if items.is_empty() {
                return Ok(ExprType::List(Box::new(ExprType::Unknown)));
            }
            let mut item_type = ExprType::Unknown;
            for item in items {
                let typ = type_of(item, env)?;
                item_type = unify_types(&item_type, &typ)?;
            }
            Ok(ExprType::List(Box::new(item_type)))
        }
        Literal::Map(entries) => {
            if entries.is_empty() {
                return Ok(ExprType::Map(
                    Box::new(ExprType::Unknown),
                    Box::new(ExprType::Unknown),
                ));
            }
            let mut key_type = ExprType::Unknown;
            let mut val_type = ExprType::Unknown;
            for (k, v) in entries {
                let kt = type_of(k, env)?;
                let vt = type_of(v, env)?;
                key_type = unify_types(&key_type, &kt)?;
                val_type = unify_types(&val_type, &vt)?;
            }
            Ok(ExprType::Map(Box::new(key_type), Box::new(val_type)))
        }
    }
}

fn resolve_path(path: &[String], env: &TypeEnv) -> Result<TypeSpec, DharmaError> {
    if path.is_empty() {
        return Err(DharmaError::Validation("empty path".to_string()));
    }
    match path[0].as_str() {
        "state" => {
            let name = path
                .get(1)
                .ok_or_else(|| DharmaError::Validation("invalid state path".to_string()))?;
            if path.len() > 2 {
                return Err(DharmaError::Validation("nested state path unsupported".to_string()));
            }
            env.state
                .get(name)
                .cloned()
                .ok_or_else(|| DharmaError::Validation("unknown state field".to_string()))
        }
        "args" => {
            let name = path
                .get(1)
                .ok_or_else(|| DharmaError::Validation("invalid args path".to_string()))?;
            if path.len() > 2 {
                return Err(DharmaError::Validation("nested args path unsupported".to_string()));
            }
            env.args
                .get(name)
                .cloned()
                .ok_or_else(|| DharmaError::Validation(format!("unknown arg '{}'", name)))
        }
        "context" => {
            let key = path.join(".");
            env.context
                .get(&key)
                .cloned()
                .ok_or_else(|| DharmaError::Validation("unknown context".to_string()))
        }
        _ => {
            if path.len() > 1 {
                return Err(DharmaError::Validation("nested arg path unsupported".to_string()));
            }
            env.args
                .get(&path[0])
                .cloned()
                .ok_or_else(|| DharmaError::Validation(format!("unknown arg '{}'", path[0])))
        }
    }
}

fn expr_type_from_spec(typ: &TypeSpec) -> ExprType {
    match typ {
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            ExprType::Numeric
        }
        TypeSpec::Bool => ExprType::Bool,
        TypeSpec::Text(_) | TypeSpec::Currency => ExprType::Text,
        TypeSpec::Enum(variants) => ExprType::Enum(variants.clone()),
        TypeSpec::Identity | TypeSpec::Ref(_) => ExprType::Identity,
        TypeSpec::GeoPoint => ExprType::GeoPoint,
        TypeSpec::Ratio => ExprType::Ratio,
        TypeSpec::List(inner) => ExprType::List(Box::new(expr_type_from_spec(inner))),
        TypeSpec::Map(key, val) => ExprType::Map(
            Box::new(expr_type_from_spec(key)),
            Box::new(expr_type_from_spec(val)),
        ),
        TypeSpec::Optional(inner) => ExprType::Optional(Box::new(expr_type_from_spec(inner))),
    }
}

fn strip_optional(typ: &ExprType) -> &ExprType {
    if let ExprType::Optional(inner) = typ {
        inner
    } else {
        typ
    }
}

fn ensure_numeric(typ: &ExprType) -> Result<(), DharmaError> {
    match strip_optional(typ) {
        ExprType::Numeric => Ok(()),
        _ => Err(DharmaError::Validation("expected numeric".to_string())),
    }
}

fn ensure_bool(typ: &ExprType) -> Result<(), DharmaError> {
    match strip_optional(typ) {
        ExprType::Bool => Ok(()),
        _ => Err(DharmaError::Validation("expected bool".to_string())),
    }
}

fn ensure_assignable(target: &TypeSpec, expr: &ExprType) -> Result<(), DharmaError> {
    match target {
        TypeSpec::Optional(inner) => {
            if matches!(expr, ExprType::Null) {
                return Ok(());
            }
            return ensure_assignable(inner, expr);
        }
        TypeSpec::Int | TypeSpec::Decimal(_) | TypeSpec::Duration | TypeSpec::Timestamp => {
            ensure_numeric(expr)
        }
        TypeSpec::Bool => ensure_bool(expr),
        TypeSpec::Text(_) | TypeSpec::Currency => match strip_optional(expr) {
            ExprType::Text => Ok(()),
            _ => Err(DharmaError::Validation("expected text".to_string())),
        },
        TypeSpec::Enum(variants) => match strip_optional(expr) {
            ExprType::Enum(other) => {
                if other == variants {
                    Ok(())
                } else {
                    Err(DharmaError::Validation("enum mismatch".to_string()))
                }
            }
            ExprType::EnumLit(name) => {
                if variants.contains(name) {
                    Ok(())
                } else {
                    Err(DharmaError::Validation("enum mismatch".to_string()))
                }
            }
            _ => Err(DharmaError::Validation("expected enum".to_string())),
        },
        TypeSpec::Identity | TypeSpec::Ref(_) => match strip_optional(expr) {
            ExprType::Identity => Ok(()),
            _ => Err(DharmaError::Validation("expected identity".to_string())),
        },
        TypeSpec::GeoPoint => match strip_optional(expr) {
            ExprType::GeoPoint => Ok(()),
            _ => Err(DharmaError::Validation("expected geopoint".to_string())),
        },
        TypeSpec::Ratio => match strip_optional(expr) {
            ExprType::Ratio => Ok(()),
            _ => Err(DharmaError::Validation("expected ratio".to_string())),
        },
        TypeSpec::List(inner) => match strip_optional(expr) {
            ExprType::List(elem) => {
                let target_type = expr_type_from_spec(inner);
                ensure_expr_compatible(&target_type, elem)
            }
            _ => Err(DharmaError::Validation("expected list".to_string())),
        },
        TypeSpec::Map(key, val) => match strip_optional(expr) {
            ExprType::Map(expr_key, expr_val) => {
                let target_key = expr_type_from_spec(key);
                let target_val = expr_type_from_spec(val);
                ensure_expr_compatible(&target_key, expr_key)?;
                ensure_expr_compatible(&target_val, expr_val)?;
                Ok(())
            }
            _ => Err(DharmaError::Validation("expected map".to_string())),
        },
    }
}

fn ensure_expr_compatible(expected: &ExprType, actual: &ExprType) -> Result<(), DharmaError> {
    if matches!(expected, ExprType::Unknown) || matches!(actual, ExprType::Unknown) {
        return Ok(());
    }
    if matches!(expected, ExprType::Enum(_)) && matches!(actual, ExprType::EnumLit(_)) {
        return Ok(());
    }
    if expected == actual {
        return Ok(());
    }
    Err(DharmaError::Validation("type mismatch".to_string()))
}

fn ensure_eq_compatible(left: &ExprType, right: &ExprType) -> Result<(), DharmaError> {
    match (strip_optional(left), strip_optional(right)) {
        (ExprType::Null, other) | (other, ExprType::Null) => match other {
            ExprType::List(_) | ExprType::Map(_, _) => {
                Err(DharmaError::Validation("null comparison unsupported".to_string()))
            }
            _ => Ok(()),
        },
        (ExprType::Numeric, ExprType::Numeric) => Ok(()),
        (ExprType::Bool, ExprType::Bool) => Ok(()),
        (ExprType::Text, ExprType::Text) => Ok(()),
        (ExprType::Identity, ExprType::Identity) => Ok(()),
        (ExprType::GeoPoint, ExprType::GeoPoint) => Ok(()),
        (ExprType::Ratio, ExprType::Ratio) => Ok(()),
        (ExprType::Enum(variants), ExprType::Enum(other)) => {
            if variants == other {
                Ok(())
            } else {
                Err(DharmaError::Validation("enum mismatch".to_string()))
            }
        }
        (ExprType::Enum(variants), ExprType::EnumLit(name))
        | (ExprType::EnumLit(name), ExprType::Enum(variants)) => {
            if variants.contains(name) {
                Ok(())
            } else {
                Err(DharmaError::Validation("enum mismatch".to_string()))
            }
        }
        (ExprType::EnumLit(_), ExprType::EnumLit(_)) => Ok(()),
        _ => Err(DharmaError::Validation("invalid eq types".to_string())),
    }
}

fn unify_types(left: &ExprType, right: &ExprType) -> Result<ExprType, DharmaError> {
    if matches!(left, ExprType::Unknown) {
        return Ok(right.clone());
    }
    if matches!(right, ExprType::Unknown) {
        return Ok(left.clone());
    }
    if left == right {
        return Ok(left.clone());
    }
    match (left, right) {
        (ExprType::EnumLit(_), ExprType::EnumLit(_)) => Ok(ExprType::EnumLit(String::new())),
        _ => Err(DharmaError::Validation("type mismatch".to_string())),
    }
}

fn check_in_expr(left: &Expr, right: &Expr, env: &TypeEnv) -> Result<ExprType, DharmaError> {
    let left_type = type_of(left, env)?;
    match right {
        Expr::Literal(Literal::List(items)) => {
            if items.is_empty() {
                return Ok(ExprType::Bool);
            }
            for item in items {
                let item_type = type_of(item, env)?;
                ensure_eq_compatible(&left_type, &item_type)?;
            }
            Ok(ExprType::Bool)
        }
        Expr::Path(_) | Expr::Call(_, _) => {
            let right_type = type_of(right, env)?;
            match strip_optional(&right_type) {
                ExprType::List(elem) => {
                    ensure_eq_compatible(&left_type, elem)?;
                    Ok(ExprType::Bool)
                }
                _ => Err(DharmaError::Validation("in expects list".to_string())),
            }
        }
        _ => Err(DharmaError::Validation("in expects list".to_string())),
    }
}

fn check_len(args: &[Expr], env: &TypeEnv) -> Result<ExprType, DharmaError> {
    if args.len() != 1 {
        return Err(DharmaError::Validation("len expects one arg".to_string()));
    }
    match &args[0] {
        Expr::Literal(Literal::List(_)) | Expr::Literal(Literal::Map(_)) => Ok(ExprType::Numeric),
        Expr::Path(_) => {
            let typ = type_of(&args[0], env)?;
            match strip_optional(&typ) {
                ExprType::Text | ExprType::List(_) | ExprType::Map(_, _) => Ok(ExprType::Numeric),
                _ => Err(DharmaError::Validation("len expects text or collection".to_string())),
            }
        }
        _ => Err(DharmaError::Validation("len expects path or literal collection".to_string())),
    }
}

fn check_contains(args: &[Expr], env: &TypeEnv) -> Result<ExprType, DharmaError> {
    if args.len() != 2 {
        return Err(DharmaError::Validation("contains expects two args".to_string()));
    }
    match &args[0] {
        Expr::Literal(Literal::List(items)) => {
            let item_type = type_of(&args[1], env)?;
            for item in items {
                let lit_type = type_of(item, env)?;
                ensure_eq_compatible(&item_type, &lit_type)?;
            }
            Ok(ExprType::Bool)
        }
        Expr::Path(_) | Expr::Call(_, _) => {
            let list_type = type_of(&args[0], env)?;
            match strip_optional(&list_type) {
                ExprType::List(elem) => {
                    let item_type = type_of(&args[1], env)?;
                    ensure_eq_compatible(elem, &item_type)?;
                    Ok(ExprType::Bool)
                }
                ExprType::Map(key, _) => {
                    let key_type = type_of(&args[1], env)?;
                    ensure_eq_compatible(key, &key_type)?;
                    Ok(ExprType::Bool)
                }
                _ => Err(DharmaError::Validation("contains expects list or map".to_string())),
            }
        }
        _ => Err(DharmaError::Validation("contains expects list or map".to_string())),
    }
}

fn check_index(args: &[Expr], env: &TypeEnv) -> Result<ExprType, DharmaError> {
    if args.len() != 2 {
        return Err(DharmaError::Validation("index expects two args".to_string()));
    }
    match &args[0] {
        Expr::Literal(Literal::List(items)) => {
            let idx = match &args[1] {
                Expr::Literal(Literal::Int(i)) if *i >= 0 => *i as usize,
                _ => return Err(DharmaError::Validation("index expects literal int".to_string())),
            };
            let item = items
                .get(idx)
                .ok_or_else(|| DharmaError::Validation("index out of bounds".to_string()))?;
            type_of(item, env)
        }
        Expr::Literal(Literal::Map(entries)) => {
            let key_type = type_of(&args[1], env)?;
            for (k, v) in entries {
                let kt = type_of(k, env)?;
                if ensure_eq_compatible(&key_type, &kt).is_ok() {
                    return type_of(v, env);
                }
            }
            Err(DharmaError::Validation("map key not found".to_string()))
        }
        _ => {
            let col_type = type_of(&args[0], env)?;
            match strip_optional(&col_type) {
                ExprType::List(elem) => {
                    let idx_type = type_of(&args[1], env)?;
                    ensure_numeric(&idx_type)?;
                    Ok((**elem).clone())
                }
                ExprType::Map(key, value) => {
                    let key_type = type_of(&args[1], env)?;
                    ensure_eq_compatible(key, &key_type)?;
                    Ok((**value).clone())
                }
                _ => Err(DharmaError::Validation("index expects list or map".to_string())),
            }
        }
    }
}

fn check_has_role(args: &[Expr], env: &TypeEnv) -> Result<ExprType, DharmaError> {
    if !(args.len() == 2 || args.len() == 3) {
        return Err(DharmaError::Validation(
            "has_role expects two or three args".to_string(),
        ));
    }
    let (subject_expr, identity_expr, role_expr) = if args.len() == 2 {
        (&args[0], &args[0], &args[1])
    } else {
        (&args[0], &args[1], &args[2])
    };
    let subject_type = type_of(subject_expr, env)?;
    match strip_optional(&subject_type) {
        ExprType::Identity => {}
        _ => return Err(DharmaError::Validation("has_role expects identity".to_string())),
    }
    let identity_type = type_of(identity_expr, env)?;
    match strip_optional(&identity_type) {
        ExprType::Identity => {}
        _ => return Err(DharmaError::Validation("has_role expects identity".to_string())),
    }
    let role_type = type_of(role_expr, env)?;
    match strip_optional(&role_type) {
        ExprType::Text | ExprType::EnumLit(_) | ExprType::Enum(_) => Ok(ExprType::Bool),
        _ => Err(DharmaError::Validation("has_role expects text role".to_string())),
    }
}

fn check_distance(args: &[Expr], env: &TypeEnv) -> Result<ExprType, DharmaError> {
    if args.len() != 2 {
        return Err(DharmaError::Validation("distance expects two args".to_string()));
    }
    for arg in args {
        let typ = type_of(arg, env)?;
        match strip_optional(&typ) {
            ExprType::GeoPoint => {}
            _ => return Err(DharmaError::Validation("distance expects geopoint".to_string())),
        }
    }
    Ok(ExprType::Numeric)
}

fn check_sum(args: &[Expr], env: &TypeEnv) -> Result<ExprType, DharmaError> {
    if args.len() != 1 {
        return Err(DharmaError::Validation("sum expects one arg".to_string()));
    }
    match &args[0] {
        Expr::Literal(Literal::List(items)) => {
            for item in items {
                let typ = type_of(item, env)?;
                ensure_numeric(&typ)?;
            }
            Ok(ExprType::Numeric)
        }
        _ => {
            let typ = type_of(&args[0], env)?;
            match strip_optional(&typ) {
                ExprType::List(elem) => {
                    ensure_numeric(elem)?;
                    Ok(ExprType::Numeric)
                }
                _ => Err(DharmaError::Validation("sum expects list".to_string())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdl::parser;

    #[test]
    fn typecheck_accepts_valid() {
        let doc = r#"```dhl
aggregate Box
    state
        total: Int
        status: Enum(Open, Closed)

action Touch(val: Int)
    validate
        val > 0
        state.status == 'Open
    apply
        state.total = sum([1, 2, 3])
```"#;
        let ast = parser::parse(doc).unwrap();
        check_ast(&ast).unwrap();
    }

    #[test]
    fn typecheck_rejects_mismatch() {
        let doc = r#"```dhl
aggregate Box
    state
        total: Int

action Touch()
    validate
        state.total == "nope"
```"#;
        let ast = parser::parse(doc).unwrap();
        assert!(check_ast(&ast).is_err());
    }

    #[test]
    fn typecheck_rejects_bad_push() {
        let doc = r#"```dhl
aggregate Box
    state
        nums: List<Int>

action Add()
    apply
        state.nums.push("bad")
```"#;
        let ast = parser::parse(doc).unwrap();
        assert!(check_ast(&ast).is_err());
    }

    #[test]
    fn typecheck_unknown_arg_reports_line() {
        let doc = r#"```dhl
aggregate Box
    state
        total: Int

action Touch(val: Int)
    validate
        bogus > 0
```"#;
        let ast = parser::parse(doc).unwrap();
        let err = check_ast(&ast).unwrap_err().to_string();
        assert!(err.contains("unknown arg 'bogus'"));
        assert!(err.contains("line "));
        assert!(err.contains("bogus > 0"));
    }

    #[test]
    fn external_requires_declared_role() {
        let doc = r#"```dhl
aggregate Box
    state
        status: Enum(Open, Closed)

action Touch()
    validate
        has_role(context.signer, "finance.approver")
```"#;
        let ast = parser::parse(doc).unwrap();
        assert!(check_ast(&ast).is_err());

        let doc = r#"```dhl
external
    roles: [finance.viewer]

aggregate Box
    state
        status: Enum(Open, Closed)

action Touch()
    validate
        has_role(context.signer, "finance.approver")
```"#;
        let ast = parser::parse(doc).unwrap();
        assert!(check_ast(&ast).is_err());

        let doc = r#"```dhl
external
    roles: [finance.approver]

aggregate Box
    state
        status: Enum(Open, Closed)

action Touch()
    validate
        has_role(context.signer, "finance.approver")
        has_role(context.signer, context.signer, "finance.approver")
```"#;
        let ast = parser::parse(doc).unwrap();
        check_ast(&ast).unwrap();
    }

    #[test]
    fn external_requires_time_for_now() {
        let doc = r#"```dhl
aggregate Box
    state
        status: Enum(Open, Closed)

action Touch()
    validate
        now() > 0
```"#;
        let ast = parser::parse(doc).unwrap();
        assert!(check_ast(&ast).is_err());

        let doc = r#"```dhl
external
    time: [block_time]

aggregate Box
    state
        status: Enum(Open, Closed)

action Touch()
    validate
        now() > 0
```"#;
        let ast = parser::parse(doc).unwrap();
        check_ast(&ast).unwrap();
    }

    #[test]
    fn external_requires_dataset_declaration() {
        let expr = Expr::Path(vec![
            "dataset".to_string(),
            "fx_rates".to_string(),
            "v1".to_string(),
        ]);
        assert!(validate_external_expr(&expr, &[], &[], &[]).is_err());
        let datasets = vec!["fx_rates.v1".to_string()];
        validate_external_expr(&expr, &[], &[], &datasets).unwrap();

        let expr = Expr::Call(
            "dataset".to_string(),
            vec![Expr::Literal(Literal::Text("fx_rates.v1".to_string()))],
        );
        validate_external_expr(&expr, &[], &[], &datasets).unwrap();
    }
}
