use crate::error::DharmaError;
use crate::pdl::ast::{AstFile, EmitDef, Expr, Literal, Op, ReactorDef};
use ciborium::value::Value;
use dharma::reactor as runtime_reactor;

pub fn compile_plan(ast: &AstFile) -> Result<Vec<u8>, DharmaError> {
    let mut reactors = Vec::new();
    for reactor in &ast.reactors {
        reactors.push(convert_reactor(reactor)?);
    }
    let plan = runtime_reactor::ReactorPlan {
        version: 1,
        reactors,
    };
    plan.to_cbor()
}

fn convert_reactor(def: &ReactorDef) -> Result<runtime_reactor::ReactorSpec, DharmaError> {
    let validates = def
        .validates
        .iter()
        .map(|expr| convert_expr(&expr.value))
        .collect::<Result<Vec<_>, _>>()?;
    let emits = def
        .emits
        .iter()
        .map(convert_emit)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(runtime_reactor::ReactorSpec {
        name: def.name.clone(),
        trigger: def.trigger.clone(),
        scope: def.scope.clone(),
        validates,
        emits,
    })
}

fn convert_emit(def: &EmitDef) -> Result<runtime_reactor::EmitSpec, DharmaError> {
    let mut args = Vec::new();
    for (name, expr) in &def.args {
        args.push((name.clone(), convert_expr(&expr.value)?));
    }
    Ok(runtime_reactor::EmitSpec {
        action: def.action.clone(),
        args,
    })
}

fn convert_expr(expr: &Expr) -> Result<runtime_reactor::Expr, DharmaError> {
    match expr {
        Expr::Literal(lit) => Ok(runtime_reactor::Expr::Literal(convert_literal(lit)?)),
        Expr::Path(parts) => Ok(runtime_reactor::Expr::Path(parts.clone())),
        Expr::UnaryOp(op, inner) => Ok(runtime_reactor::Expr::Unary(
            convert_op(*op)?,
            Box::new(convert_expr(inner)?),
        )),
        Expr::BinaryOp(op, left, right) => Ok(runtime_reactor::Expr::Binary(
            convert_op(*op)?,
            Box::new(convert_expr(left)?),
            Box::new(convert_expr(right)?),
        )),
        Expr::Call(name, args) => {
            let mut out = Vec::new();
            for arg in args {
                out.push(convert_expr(arg)?);
            }
            Ok(runtime_reactor::Expr::Call(name.clone(), out))
        }
    }
}

fn convert_literal(lit: &Literal) -> Result<Value, DharmaError> {
    match lit {
        Literal::Int(value) => Ok(Value::Integer((*value).into())),
        Literal::Bool(value) => Ok(Value::Bool(*value)),
        Literal::Text(value) => Ok(Value::Text(value.clone())),
        Literal::Enum(value) => Ok(Value::Text(value.clone())),
        Literal::Null => Ok(Value::Null),
        Literal::List(items) => {
            let mut out = Vec::new();
            for item in items {
                match item {
                    Expr::Literal(inner) => out.push(convert_literal(inner)?),
                    _ => {
                        return Err(DharmaError::Validation(
                            "reactor list literals must be literal".to_string(),
                        ))
                    }
                }
            }
            Ok(Value::Array(out))
        }
        Literal::Map(entries) => {
            let mut out = Vec::new();
            for (k, v) in entries {
                let key = match k {
                    Expr::Literal(inner) => convert_literal(inner)?,
                    _ => {
                        return Err(DharmaError::Validation(
                            "reactor map keys must be literal".to_string(),
                        ))
                    }
                };
                let val = match v {
                    Expr::Literal(inner) => convert_literal(inner)?,
                    _ => {
                        return Err(DharmaError::Validation(
                            "reactor map values must be literal".to_string(),
                        ))
                    }
                };
                out.push((key, val));
            }
            Ok(Value::Map(out))
        }
        Literal::Struct(_, entries) => {
            let mut out = Vec::new();
            for (k, v) in entries {
                let val = match v {
                    Expr::Literal(inner) => convert_literal(inner)?,
                    _ => {
                        return Err(DharmaError::Validation(
                            "reactor struct values must be literal".to_string(),
                        ))
                    }
                };
                out.push((Value::Text(k.clone()), val));
            }
            Ok(Value::Map(out))
        }
    }
}

fn convert_op(op: Op) -> Result<runtime_reactor::Op, DharmaError> {
    Ok(match op {
        Op::Add => runtime_reactor::Op::Add,
        Op::Sub => runtime_reactor::Op::Sub,
        Op::Mul => runtime_reactor::Op::Mul,
        Op::Div => runtime_reactor::Op::Div,
        Op::Mod => runtime_reactor::Op::Mod,
        Op::In => runtime_reactor::Op::In,
        Op::Eq => runtime_reactor::Op::Eq,
        Op::Neq => runtime_reactor::Op::Neq,
        Op::Gt => runtime_reactor::Op::Gt,
        Op::Lt => runtime_reactor::Op::Lt,
        Op::Gte => runtime_reactor::Op::Gte,
        Op::Lte => runtime_reactor::Op::Lte,
        Op::And => runtime_reactor::Op::And,
        Op::Or => runtime_reactor::Op::Or,
        Op::Not => runtime_reactor::Op::Not,
        Op::Neg => runtime_reactor::Op::Neg,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdl::parser;

    #[test]
    fn compile_reactor_plan_roundtrip() {
        let doc = r#"```dhl
reactor Auto
    trigger: action.Send
    validate
        amount > 0
    emit action.Invoice.Release(amount = amount)
```"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile_plan(&ast).unwrap();
        let plan = runtime_reactor::ReactorPlan::from_cbor(&bytes).unwrap();
        assert_eq!(plan.reactors.len(), 1);
        assert_eq!(plan.reactors[0].name, "Auto");
    }
}
