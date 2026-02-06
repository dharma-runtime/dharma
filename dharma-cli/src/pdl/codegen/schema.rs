use crate::error::DharmaError;
use crate::pdl::ast::{AstFile, Literal, TypeSpec, Visibility};
use crate::{dhlp, dhlq};
use crate::pdl::schema::{ActionSchema, CqrsSchema, FieldSchema, QuerySchema};
use crate::pdl::schema::ConcurrencyMode as SchemaConcurrency;
use ciborium::value::Value;
use std::collections::BTreeMap;

pub fn compile_schema(ast: &AstFile) -> Result<Vec<u8>, DharmaError> {
    let aggregate = ast
        .aggregates
        .first()
        .ok_or_else(|| DharmaError::Validation("missing aggregate".to_string()))?;
    let mut structs = BTreeMap::new();
    for def in &ast.structs {
        let mut fields = BTreeMap::new();
        for field in &def.fields {
            let default = match field.default.as_ref() {
                Some(lit) => Some(literal_to_value(lit)?),
                None => None,
            };
            fields.insert(
                field.name.clone(),
                FieldSchema {
                    typ: type_to_schema(&field.typ),
                    default,
                    visibility: crate::pdl::schema::Visibility::Public,
                },
            );
        }
        structs.insert(def.name.clone(), crate::pdl::schema::StructSchema { fields });
    }
    let mut fields = BTreeMap::new();
    let mut field_vis = BTreeMap::new();
    for field in &aggregate.fields {
        let default = match field.default.as_ref() {
            Some(lit) => Some(literal_to_value(lit)?),
            None => None,
        };
        field_vis.insert(field.name.clone(), field.visibility);
        fields.insert(
            field.name.clone(),
            FieldSchema {
                typ: type_to_schema(&field.typ),
                default,
                visibility: match field.visibility {
                    Visibility::Public => crate::pdl::schema::Visibility::Public,
                    Visibility::Private => crate::pdl::schema::Visibility::Private,
                },
            },
        );
    }

    let mut actions = BTreeMap::new();
    for action in &ast.actions {
        let mut args = BTreeMap::new();
        let mut arg_vis = BTreeMap::new();
        for arg in &action.args {
            args.insert(arg.name.clone(), type_to_schema(&arg.typ));
        }
        let arg_flags = derive_arg_visibility(action, &field_vis)?;
        for (arg_name, (pub_used, priv_used)) in arg_flags {
            let vis = if priv_used && !pub_used {
                crate::pdl::schema::Visibility::Private
            } else {
                crate::pdl::schema::Visibility::Public
            };
            arg_vis.insert(arg_name, vis);
        }
        for arg in args.keys() {
            arg_vis.entry(arg.clone()).or_insert(crate::pdl::schema::Visibility::Public);
        }
        actions.insert(
            action.name.clone(),
            ActionSchema {
                args,
                arg_vis,
                doc: action.doc.clone(),
            },
        );
    }

    let mut queries = BTreeMap::new();
    for query in &ast.queries {
        let mut args = BTreeMap::new();
        for arg in &query.args {
            args.insert(arg.name.clone(), type_to_schema(&arg.typ));
        }
        let query_text = query.body.join("\n");
        let plan = if query_text.trim().is_empty() {
            None
        } else {
            let plan = dhlq::parse_plan(&query_text, query.start_line)?;
            Some(plan.to_cbor()?)
        };
        queries.insert(
            query.name.clone(),
            QuerySchema {
                args,
                visibility: match query.visibility {
                    Visibility::Public => crate::pdl::schema::Visibility::Public,
                    Visibility::Private => crate::pdl::schema::Visibility::Private,
                },
                query: query_text,
                plan,
                doc: query.doc.clone(),
            },
        );
    }

    let mut projections = BTreeMap::new();
    for projection in &ast.projections {
        let dsl = projection.body.join("\n");
        let plan = dhlp::parse_plan(&projection.body, projection.start_line)?.to_cbor()?;
        projections.insert(
            projection.name.clone(),
            crate::pdl::schema::ProjectionSchema {
                dsl,
                plan,
                doc: projection.doc.clone(),
            },
        );
    }

    let namespace = ast
        .package
        .clone()
        .unwrap_or_else(|| ast.header.namespace.clone());
    let schema = CqrsSchema {
        namespace,
        version: ast.header.version.clone(),
        aggregate: aggregate.name.clone(),
        extends: aggregate.extends.clone(),
        implements: ast.header.implements.clone(),
        structs,
        fields,
        actions,
        queries,
        projections,
        concurrency: match ast.header.concurrency {
            crate::pdl::ast::ConcurrencyMode::Strict => SchemaConcurrency::Strict,
            crate::pdl::ast::ConcurrencyMode::Allow => SchemaConcurrency::Allow,
        },
    };
    schema.to_cbor()
}

fn type_to_schema(typ: &TypeSpec) -> crate::pdl::schema::TypeSpec {
    match typ {
        TypeSpec::Int => crate::pdl::schema::TypeSpec::Int,
        TypeSpec::Decimal(scale) => crate::pdl::schema::TypeSpec::Decimal(*scale),
        TypeSpec::Ratio => crate::pdl::schema::TypeSpec::Ratio,
        TypeSpec::Duration => crate::pdl::schema::TypeSpec::Duration,
        TypeSpec::Timestamp => crate::pdl::schema::TypeSpec::Timestamp,
        TypeSpec::Currency => crate::pdl::schema::TypeSpec::Currency,
        TypeSpec::Bool => crate::pdl::schema::TypeSpec::Bool,
        TypeSpec::Identity => crate::pdl::schema::TypeSpec::Identity,
        TypeSpec::SubjectRef(name) => crate::pdl::schema::TypeSpec::SubjectRef(name.clone()),
        TypeSpec::Text(len) => crate::pdl::schema::TypeSpec::Text(*len),
        TypeSpec::Enum(variants) => crate::pdl::schema::TypeSpec::Enum(variants.clone()),
        TypeSpec::Ref(name) => crate::pdl::schema::TypeSpec::Ref(name.clone()),
        TypeSpec::Struct(name) => crate::pdl::schema::TypeSpec::Struct(name.clone()),
        TypeSpec::GeoPoint => crate::pdl::schema::TypeSpec::GeoPoint,
        TypeSpec::List(inner) => crate::pdl::schema::TypeSpec::List(Box::new(type_to_schema(inner))),
        TypeSpec::Map(key, value) => crate::pdl::schema::TypeSpec::Map(
            Box::new(type_to_schema(key)),
            Box::new(type_to_schema(value)),
        ),
        TypeSpec::Optional(inner) => {
            crate::pdl::schema::TypeSpec::Optional(Box::new(type_to_schema(inner)))
        }
    }
}

fn literal_to_value(lit: &Literal) -> Result<Value, DharmaError> {
    match lit {
        Literal::Int(i) => Ok(Value::Integer((*i).into())),
        Literal::Bool(b) => Ok(Value::Bool(*b)),
        Literal::Text(t) => Ok(Value::Text(t.clone())),
        Literal::Enum(e) => Ok(Value::Text(e.clone())),
        Literal::Null => Ok(Value::Null),
        Literal::List(items) => {
            let mut out = Vec::new();
            for item in items {
                out.push(expr_to_value(item)?);
            }
            Ok(Value::Array(out))
        }
        Literal::Map(entries) => {
            let mut out = Vec::new();
            for (k, v) in entries {
                out.push((expr_to_value(k)?, expr_to_value(v)?));
            }
            Ok(Value::Map(out))
        }
        Literal::Struct(_, entries) => {
            let mut out = Vec::new();
            for (k, v) in entries {
                out.push((Value::Text(k.clone()), expr_to_value(v)?));
            }
            Ok(Value::Map(out))
        }
    }
}

fn expr_to_value(expr: &crate::pdl::ast::Expr) -> Result<Value, DharmaError> {
    match expr {
        crate::pdl::ast::Expr::Literal(lit) => literal_to_value(lit),
        crate::pdl::ast::Expr::UnaryOp(crate::pdl::ast::Op::Neg, inner) => {
            if let crate::pdl::ast::Expr::Literal(Literal::Int(value)) = inner.as_ref() {
                Ok(Value::Integer((-value).into()))
            } else {
                Err(DharmaError::Validation("invalid literal".to_string()))
            }
        }
        crate::pdl::ast::Expr::UnaryOp(crate::pdl::ast::Op::Not, inner) => {
            if let crate::pdl::ast::Expr::Literal(Literal::Bool(value)) = inner.as_ref() {
                Ok(Value::Bool(!value))
            } else {
                Err(DharmaError::Validation("invalid literal".to_string()))
            }
        }
        _ => Err(DharmaError::Validation("invalid literal".to_string())),
    }
}

fn derive_arg_visibility(
    action: &crate::pdl::ast::ActionDef,
    field_vis: &BTreeMap<String, Visibility>,
) -> Result<BTreeMap<String, (bool, bool)>, DharmaError> {
    let mut flags: BTreeMap<String, (bool, bool)> = BTreeMap::new();
    for assignment in &action.applies {
        let assignment = &assignment.value;
        let target = assignment
            .target
            .first()
            .map(|s| s.as_str())
            .unwrap_or("");
        let name = if target == "state" {
            assignment.target.get(1)
        } else {
            None
        };
        let vis = name
            .and_then(|field| field_vis.get(field).copied())
            .unwrap_or(Visibility::Public);
        let args = collect_arg_refs(&assignment.value);
        for arg in args {
            let entry = flags.entry(arg).or_insert((false, false));
            match vis {
                Visibility::Public => entry.0 = true,
                Visibility::Private => entry.1 = true,
            }
        }
    }
    Ok(flags)
}

fn collect_arg_refs(expr: &crate::pdl::ast::Expr) -> Vec<String> {
    let mut out = Vec::new();
    collect_arg_refs_inner(expr, &mut out);
    out.sort();
    out.dedup();
    out
}

fn collect_arg_refs_inner(expr: &crate::pdl::ast::Expr, out: &mut Vec<String>) {
    match expr {
        crate::pdl::ast::Expr::Path(parts) => {
            if let Some(name) = path_to_arg(parts) {
                out.push(name);
            }
        }
        crate::pdl::ast::Expr::Literal(lit) => {
            match lit {
                Literal::List(items) => {
                    for item in items {
                        collect_arg_refs_inner(item, out);
                    }
                }
                Literal::Map(entries) => {
                    for (k, v) in entries {
                        collect_arg_refs_inner(k, out);
                        collect_arg_refs_inner(v, out);
                    }
                }
                _ => {}
            }
        }
        crate::pdl::ast::Expr::UnaryOp(_, inner) => {
            collect_arg_refs_inner(inner, out);
        }
        crate::pdl::ast::Expr::BinaryOp(_, left, right) => {
            collect_arg_refs_inner(left, out);
            collect_arg_refs_inner(right, out);
        }
        crate::pdl::ast::Expr::Call(_, args) => {
            for arg in args {
                collect_arg_refs_inner(arg, out);
            }
        }
    }
}

fn path_to_arg(parts: &[String]) -> Option<String> {
    if parts.is_empty() {
        return None;
    }
    if parts[0] == "state" || parts[0] == "context" {
        return None;
    }
    if parts[0] == "args" {
        return parts.get(1).cloned();
    }
    Some(parts[0].clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdl::parser;

    #[test]
    fn compile_schema_roundtrip() {
        let doc = r#"---
namespace: com.test
version: 1.0.0
---

```dhl
aggregate Ticket
    state
        status: Enum(Open, Closed) = Open

action Close(reason: Text(len=32))
    validate
        state.status == Open
    apply
        state.status = Closed
```
"#;
        let ast = parser::parse(doc).unwrap();
        let bytes = compile_schema(&ast).unwrap();
        let schema = crate::pdl::schema::CqrsSchema::from_cbor(&bytes).unwrap();
        assert_eq!(schema.aggregate, "Ticket");
        assert!(schema.actions.contains_key("Close"));
    }
}
