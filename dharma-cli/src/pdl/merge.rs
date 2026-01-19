use crate::error::DharmaError;
use crate::pdl::ast::{ActionDef, ArgDef, AstFile, AggregateDef};
use std::collections::{HashMap, HashSet};

pub fn merge_parent(
    mut child: AstFile,
    parent: &AstFile,
    parent_aggregate: &str,
) -> Result<AstFile, DharmaError> {
    let parent_agg = parent
        .aggregates
        .iter()
        .find(|agg| agg.name == parent_aggregate)
        .ok_or_else(|| {
            DharmaError::Validation(format!("parent aggregate {parent_aggregate} not found"))
        })?;

    let child_agg = child
        .aggregates
        .first_mut()
        .ok_or_else(|| DharmaError::Validation("missing aggregate".to_string()))?;

    merge_fields(child_agg, parent_agg)?;
    child.actions = merge_actions(&child.actions, &parent.actions)?;
    child_agg.invariants.extend(parent_agg.invariants.clone());
    child.reactors.extend(parent.reactors.clone());

    Ok(child)
}

fn merge_fields(child: &mut AggregateDef, parent: &AggregateDef) -> Result<(), DharmaError> {
    let mut seen = HashSet::new();
    for field in &child.fields {
        seen.insert(field.name.clone());
    }
    for field in &parent.fields {
        if seen.contains(&field.name) {
            return Err(DharmaError::Validation(format!(
                "field {} overrides parent",
                field.name
            )));
        }
        child.fields.push(field.clone());
    }
    Ok(())
}

fn merge_actions(child_actions: &[ActionDef], parent_actions: &[ActionDef]) -> Result<Vec<ActionDef>, DharmaError> {
    let mut map: HashMap<String, ActionDef> = HashMap::new();
    for action in child_actions {
        map.insert(action.name.clone(), action.clone());
    }
    for parent in parent_actions {
        if let Some(child) = map.get_mut(&parent.name) {
            merge_action(child, parent)?;
        } else {
            map.insert(parent.name.clone(), parent.clone());
        }
    }
    let mut actions = map.into_values().collect::<Vec<_>>();
    actions.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(actions)
}

fn merge_action(child: &mut ActionDef, parent: &ActionDef) -> Result<(), DharmaError> {
    let mut merged_args: Vec<ArgDef> = Vec::new();
    let mut seen: HashMap<String, ArgDef> = HashMap::new();
    for arg in &parent.args {
        seen.insert(arg.name.clone(), arg.clone());
        merged_args.push(arg.clone());
    }
    for arg in &child.args {
        if let Some(existing) = seen.get(&arg.name) {
            if existing.typ != arg.typ {
                return Err(DharmaError::Validation(format!(
                    "arg {} type mismatch",
                    arg.name
                )));
            }
            continue;
        }
        seen.insert(arg.name.clone(), arg.clone());
        merged_args.push(arg.clone());
    }
    child.args = merged_args;

    let mut validates = parent.validates.clone();
    validates.extend(child.validates.clone());
    child.validates = validates;

    let mut applies = parent.applies.clone();
    applies.extend(child.applies.clone());
    child.applies = applies;

    if child.doc.is_none() {
        child.doc = parent.doc.clone();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdl::ast::{
        ActionDef, AggregateDef, ArgDef, Assignment, AstFile, Expr, FieldDef, Header, SourceSpan,
        Spanned, TypeSpec, Visibility,
    };
    use crate::pdl::expr::parse_expr;

    #[test]
    fn merge_parent_adds_fields_and_actions() {
        let parent = AstFile {
            header: Header::default(),
            package: None,
            external: None,
            aggregates: vec![AggregateDef {
                name: "Invoice".to_string(),
                extends: None,
                fields: vec![FieldDef {
                    name: "amount".to_string(),
                    typ: TypeSpec::Int,
                    default: None,
                    visibility: Visibility::Public,
                }],
                invariants: Vec::new(),
            }],
            actions: vec![ActionDef {
                name: "Create".to_string(),
                args: vec![ArgDef {
                    name: "amount".to_string(),
                    typ: TypeSpec::Int,
                }],
                validates: vec![Spanned::new(
                    parse_expr("amount > 0").unwrap(),
                    SourceSpan::default(),
                )],
                applies: vec![Spanned::new(
                    Assignment {
                        target: vec!["state".to_string(), "amount".to_string()],
                        value: Expr::Path(vec!["amount".to_string()]),
                    },
                    SourceSpan::default(),
                )],
                doc: None,
            }],
            reactors: Vec::new(),
            views: Vec::new(),
        };
        let child = AstFile {
            header: Header::default(),
            package: None,
            external: None,
            aggregates: vec![AggregateDef {
                name: "CompanyInvoice".to_string(),
                extends: Some("std.finance.Invoice".to_string()),
                fields: vec![FieldDef {
                    name: "internal".to_string(),
                    typ: TypeSpec::Text(None),
                    default: None,
                    visibility: Visibility::Private,
                }],
                invariants: Vec::new(),
            }],
            actions: vec![ActionDef {
                name: "Create".to_string(),
                args: vec![ArgDef {
                    name: "internal".to_string(),
                    typ: TypeSpec::Text(None),
                }],
                validates: vec![Spanned::new(
                    parse_expr("internal.len > 0").unwrap(),
                    SourceSpan::default(),
                )],
                applies: vec![Spanned::new(
                    Assignment {
                        target: vec!["state".to_string(), "internal".to_string()],
                        value: Expr::Path(vec!["internal".to_string()]),
                    },
                    SourceSpan::default(),
                )],
                doc: None,
            }],
            reactors: Vec::new(),
            views: Vec::new(),
        };

        let merged = merge_parent(child, &parent, "Invoice").unwrap();
        let agg = &merged.aggregates[0];
        assert_eq!(agg.fields.len(), 2);
        assert!(merged.actions.iter().any(|a| a.name == "Create"));
        let action = merged.actions.iter().find(|a| a.name == "Create").unwrap();
        assert_eq!(action.args.len(), 2);
        assert_eq!(action.validates.len(), 2);
        assert_eq!(action.applies.len(), 2);
    }
}
