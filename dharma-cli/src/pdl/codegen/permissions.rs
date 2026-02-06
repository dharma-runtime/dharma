use crate::pdl::ast::{ActionDef, AstFile, Expr, Literal, Op};
use dharma::contract::{PermissionRule, PermissionSummary};
use dharma::types::ContractId;
use std::collections::BTreeSet;

#[derive(Default)]
struct RoleScan {
    has_or: bool,
    unknown_role: bool,
}

pub fn compile_permissions(ast: &AstFile, contract: ContractId, ver: u64) -> PermissionSummary {
    let mut summary = PermissionSummary::empty(contract, ver);
    for action in &ast.actions {
        if let Some(rule) = action_rule(action) {
            summary.actions.insert(action.name.clone(), rule);
        }
    }
    for query in &ast.queries {
        if query.visibility == crate::pdl::ast::Visibility::Public {
            summary.public.queries.insert(query.name.clone());
        }
    }
    summary
}

fn action_rule(action: &ActionDef) -> Option<PermissionRule> {
    if action.validates.is_empty() {
        return None;
    }
    let mut roles = BTreeSet::new();
    let mut scan = RoleScan::default();
    for clause in &action.validates {
        scan_expr(&clause.value, &mut roles, &mut scan);
    }
    if roles.is_empty() {
        return None;
    }
    Some(PermissionRule {
        roles,
        exhaustive: !scan.has_or && !scan.unknown_role,
    })
}

fn scan_expr(expr: &Expr, roles: &mut BTreeSet<String>, scan: &mut RoleScan) {
    match expr {
        Expr::BinaryOp(op, left, right) => {
            if matches!(op, Op::Or) {
                scan.has_or = true;
            }
            scan_expr(left, roles, scan);
            scan_expr(right, roles, scan);
        }
        Expr::UnaryOp(_, inner) => scan_expr(inner, roles, scan),
        Expr::Call(name, args) => {
            if name == "has_role" {
                if let Some(role_arg) = args.last() {
                    match role_arg {
                        Expr::Literal(Literal::Text(role)) => {
                            roles.insert(role.clone());
                        }
                        Expr::Literal(Literal::Enum(role)) => {
                            roles.insert(role.clone());
                        }
                        _ => {
                            scan.unknown_role = true;
                        }
                    }
                } else {
                    scan.unknown_role = true;
                }
            }
            for arg in args {
                scan_expr(arg, roles, scan);
            }
        }
        Expr::Path(_) | Expr::Literal(_) => {}
    }
}
