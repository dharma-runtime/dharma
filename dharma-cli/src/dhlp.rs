use crate::dhlq;
use crate::error::DharmaError;
use crate::pdl::ast::{Expr as PdlExpr, Literal as PdlLiteral, Op as PdlOp};
use crate::pdl::expr::parse_expr;
use dharma::dhlp::{EmitSpec, PruneSpec, ProjectionPlan, ScopeBinding, TriggerSpec};
use dharma::reactor::{Expr, Op};
use ciborium::value::Value;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Mode {
    Top,
    Query,
    Emit,
    Prune,
}

pub fn parse_plan(body: &[String], start_line: usize) -> Result<ProjectionPlan, DharmaError> {
    let mut triggers: Vec<TriggerSpec> = Vec::new();
    let mut scope: Vec<ScopeBinding> = Vec::new();
    let mut batch_window_ms: u64 = 250;
    let mut max_delay_ms: u64 = 1000;
    let mut query_source = "state".to_string();
    let mut query_lines: Vec<(usize, String)> = Vec::new();
    let mut emit: Option<EmitSpec> = None;
    let mut prune: Option<PruneSpec> = None;
    let mut prune_block: Option<PruneSpec> = None;
    let mut no_prune = false;
    let mut mode = Mode::Top;

    let mut idx = 0usize;
    while idx < body.len() {
        let line_no = start_line + idx;
        let raw = body[idx].trim();
        idx += 1;
        if raw.is_empty() {
            continue;
        }

        if mode == Mode::Query && is_directive_line(raw) {
            mode = Mode::Top;
        }
        if mode == Mode::Emit && is_directive_line(raw) {
            mode = Mode::Top;
        }
        if mode == Mode::Prune && is_directive_line(raw) {
            mode = Mode::Top;
        }

        match mode {
            Mode::Query => {
                query_lines.push((line_no, raw.to_string()));
                continue;
            }
            Mode::Emit => {
                let emit_ref = emit
                    .as_mut()
                    .ok_or_else(|| DharmaError::Validation("missing emit header".to_string()))?;
                let (name, expr) = parse_assignment(raw, line_no)?;
                emit_ref.args.push((name, expr));
                continue;
            }
            Mode::Prune => {
                if prune_block.is_none() {
                    prune_block = Some(PruneSpec {
                        keys: Vec::new(),
                        predicate: None,
                    });
                }
                let prune_ref = prune_block.as_mut().unwrap();
                if let Some(rest) = raw.strip_prefix("by ") {
                    prune_ref.keys = parse_list(rest, line_no)?;
                    continue;
                }
                if let Some(rest) = raw.strip_prefix("where ") {
                    prune_ref.predicate = Some(parse_expr_wrapped(rest, line_no)?);
                    continue;
                }
                return Err(DharmaError::Validation(format!(
                    "line {line_no} col 1: invalid prune directive"
                )));
            }
            Mode::Top => {}
        }

        if let Some(rest) = raw.strip_prefix("trigger ") {
            for part in rest.split(',') {
                let name = part.trim();
                if name.is_empty() {
                    continue;
                }
                triggers.push(TriggerSpec { name: name.to_string() });
            }
            continue;
        }
        if let Some(rest) = raw.strip_prefix("scope ") {
            let (name, expr) = parse_scope(rest, line_no)?;
            scope.push(ScopeBinding { name, expr });
            continue;
        }
        if let Some(rest) = raw.strip_prefix("batch_window ") {
            batch_window_ms = parse_duration_ms(rest, line_no)?;
            continue;
        }
        if let Some(rest) = raw.strip_prefix("max_delay ") {
            max_delay_ms = parse_duration_ms(rest, line_no)?;
            continue;
        }
        if let Some(rest) = raw.strip_prefix("source ") {
            let value = rest.trim();
            if value.is_empty() {
                return Err(DharmaError::Validation(format!(
                    "line {line_no} col 1: invalid source"
                )));
            }
            query_source = value.to_string();
            continue;
        }
        if raw == "query" {
            mode = Mode::Query;
            continue;
        }
        if let Some(rest) = raw.strip_prefix("emit ") {
            if emit.is_some() {
                return Err(DharmaError::Validation(format!(
                    "line {line_no} col 1: duplicate emit"
                )));
            }
            emit = Some(parse_emit_header(rest, line_no)?);
            mode = Mode::Emit;
            continue;
        }
        if raw == "prune" {
            mode = Mode::Prune;
            continue;
        }
        if let Some(rest) = raw.strip_prefix("prune ") {
            if !rest.trim().is_empty() {
                let keys = parse_list(rest, line_no)?;
                prune = Some(PruneSpec { keys, predicate: None });
                continue;
            }
            mode = Mode::Prune;
            continue;
        }
        if raw == "no_prune" {
            no_prune = true;
            continue;
        }

        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: unsupported projection directive"
        )));
    }

    if mode == Mode::Query || !query_lines.is_empty() {
        // ok
    }
    if let Some(block) = prune_block.take() {
        prune = Some(block);
    }
    if no_prune && prune.is_some() {
        return Err(DharmaError::Validation(
            "projection cannot define prune and no_prune".to_string(),
        ));
    }
    if no_prune {
        prune = None;
    }
    let emit = emit.ok_or_else(|| DharmaError::Validation("missing emit".to_string()))?;
    let query_text = query_lines
        .iter()
        .map(|(_, line)| line.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    if query_text.trim().is_empty() {
        return Err(DharmaError::Validation("missing query".to_string()));
    }
    let query_start = query_lines.first().map(|(line, _)| *line).unwrap_or(start_line);
    let query_plan = dhlq::parse_plan(&query_text, query_start)?;

    if triggers.is_empty() {
        return Err(DharmaError::Validation("missing trigger".to_string()));
    }
    if let Some(prune_spec) = prune.as_ref() {
        if prune_spec.keys.is_empty() {
            return Err(DharmaError::Validation("prune missing keys".to_string()));
        }
    }

    Ok(ProjectionPlan {
        version: 1,
        triggers,
        scope,
        batch_window_ms,
        max_delay_ms,
        query_source,
        query: query_plan,
        emit,
        prune,
    })
}

fn is_directive_line(line: &str) -> bool {
    let lower = line.trim_start().to_ascii_lowercase();
    lower.starts_with("trigger ")
        || lower.starts_with("scope ")
        || lower.starts_with("batch_window ")
        || lower.starts_with("max_delay ")
        || lower.starts_with("source ")
        || lower == "query"
        || lower.starts_with("emit ")
        || lower == "prune"
        || lower.starts_with("prune ")
        || lower == "no_prune"
}

fn parse_scope(rest: &str, line_no: usize) -> Result<(String, Expr), DharmaError> {
    let (name, expr) = rest
        .split_once('=')
        .ok_or_else(|| DharmaError::Validation(format!("line {line_no} col 1: invalid scope")))?;
    let name = name.trim();
    let expr = expr.trim();
    if name.is_empty() || expr.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid scope"
        )));
    }
    let expr = parse_expr_wrapped(expr, line_no)?;
    Ok((name.to_string(), expr))
}

fn parse_duration_ms(value: &str, line_no: usize) -> Result<u64, DharmaError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid duration"
        )));
    }
    if let Some(ms) = trimmed.strip_suffix("ms") {
        return ms.trim().parse::<u64>().map_err(|_| {
            DharmaError::Validation(format!("line {line_no} col 1: invalid duration"))
        });
    }
    if let Some(s) = trimmed.strip_suffix('s') {
        let secs = s.trim().parse::<u64>().map_err(|_| {
            DharmaError::Validation(format!("line {line_no} col 1: invalid duration"))
        })?;
        return Ok(secs * 1000);
    }
    if let Some(m) = trimmed.strip_suffix('m') {
        let mins = m.trim().parse::<u64>().map_err(|_| {
            DharmaError::Validation(format!("line {line_no} col 1: invalid duration"))
        })?;
        return Ok(mins * 60 * 1000);
    }
    if let Some(h) = trimmed.strip_suffix('h') {
        let hours = h.trim().parse::<u64>().map_err(|_| {
            DharmaError::Validation(format!("line {line_no} col 1: invalid duration"))
        })?;
        return Ok(hours * 60 * 60 * 1000);
    }
    trimmed.parse::<u64>().map_err(|_| {
        DharmaError::Validation(format!("line {line_no} col 1: invalid duration"))
    })
}

fn parse_emit_header(rest: &str, line_no: usize) -> Result<EmitSpec, DharmaError> {
    let mut parts = rest.split_whitespace();
    let verb = parts.next().unwrap_or("").trim();
    let target = parts.next().unwrap_or("").trim();
    if verb.is_empty() || target.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid emit"
        )));
    }
    Ok(EmitSpec {
        verb: verb.to_string(),
        target: target.to_string(),
        args: Vec::new(),
    })
}

fn parse_assignment(line: &str, line_no: usize) -> Result<(String, Expr), DharmaError> {
    let (name, expr) = line.split_once('=').ok_or_else(|| {
        DharmaError::Validation(format!("line {line_no} col 1: invalid emit assignment"))
    })?;
    let name = name.trim();
    let expr = expr.trim();
    if name.is_empty() || expr.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid emit assignment"
        )));
    }
    Ok((name.to_string(), parse_expr_wrapped(expr, line_no)?))
}

fn parse_list(rest: &str, line_no: usize) -> Result<Vec<String>, DharmaError> {
    let trimmed = rest.trim().trim_start_matches('(').trim_end_matches(')');
    if trimmed.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid list"
        )));
    }
    let keys = trimmed
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid list"
        )));
    }
    Ok(keys)
}

fn parse_expr_wrapped(input: &str, line_no: usize) -> Result<Expr, DharmaError> {
    let expr = parse_expr(input).map_err(|e| with_line(e, line_no))?;
    Ok(convert_expr(&expr)?)
}

fn with_line(err: DharmaError, line: usize) -> DharmaError {
    match err {
        DharmaError::Validation(msg) => {
            DharmaError::Validation(format!("line {} col 1: {}", line, msg))
        }
        other => other,
    }
}

fn convert_expr(expr: &PdlExpr) -> Result<Expr, DharmaError> {
    match expr {
        PdlExpr::Literal(lit) => Ok(Expr::Literal(convert_literal(lit)?)),
        PdlExpr::Path(parts) => Ok(Expr::Path(parts.clone())),
        PdlExpr::UnaryOp(op, inner) => Ok(Expr::Unary(convert_op(*op)?, Box::new(convert_expr(inner)?))),
        PdlExpr::BinaryOp(op, left, right) => Ok(Expr::Binary(
            convert_op(*op)?,
            Box::new(convert_expr(left)?),
            Box::new(convert_expr(right)?),
        )),
        PdlExpr::Call(name, args) => {
            let mut out = Vec::new();
            for arg in args {
                out.push(convert_expr(arg)?);
            }
            Ok(Expr::Call(name.clone(), out))
        }
    }
}

fn convert_literal(lit: &PdlLiteral) -> Result<Value, DharmaError> {
    match lit {
        PdlLiteral::Int(value) => Ok(Value::Integer((*value).into())),
        PdlLiteral::Bool(value) => Ok(Value::Bool(*value)),
        PdlLiteral::Text(value) => Ok(Value::Text(value.clone())),
        PdlLiteral::Enum(value) => Ok(Value::Text(value.clone())),
        PdlLiteral::Null => Ok(Value::Null),
        PdlLiteral::List(items) => {
            let mut out = Vec::new();
            for item in items {
                match item {
                    PdlExpr::Literal(inner) => out.push(convert_literal(inner)?),
                    _ => {
                        return Err(DharmaError::Validation(
                            "list literal must be literal".to_string(),
                        ))
                    }
                }
            }
            Ok(Value::Array(out))
        }
        PdlLiteral::Map(entries) => {
            let mut out = Vec::new();
            for (k, v) in entries {
                let key = match k {
                    PdlExpr::Literal(inner) => convert_literal(inner)?,
                    _ => {
                        return Err(DharmaError::Validation(
                            "map key must be literal".to_string(),
                        ))
                    }
                };
                let val = match v {
                    PdlExpr::Literal(inner) => convert_literal(inner)?,
                    _ => {
                        return Err(DharmaError::Validation(
                            "map value must be literal".to_string(),
                        ))
                    }
                };
                out.push((key, val));
            }
            Ok(Value::Map(out))
        }
        PdlLiteral::Struct(_, entries) => {
            let mut out = Vec::new();
            for (k, v) in entries {
                let val = match v {
                    PdlExpr::Literal(inner) => convert_literal(inner)?,
                    _ => {
                        return Err(DharmaError::Validation(
                            "struct value must be literal".to_string(),
                        ))
                    }
                };
                out.push((Value::Text(k.clone()), val));
            }
            Ok(Value::Map(out))
        }
    }
}

fn convert_op(op: PdlOp) -> Result<Op, DharmaError> {
    Ok(match op {
        PdlOp::Add => Op::Add,
        PdlOp::Sub => Op::Sub,
        PdlOp::Mul => Op::Mul,
        PdlOp::Div => Op::Div,
        PdlOp::Mod => Op::Mod,
        PdlOp::In => Op::In,
        PdlOp::Eq => Op::Eq,
        PdlOp::Neq => Op::Neq,
        PdlOp::Gt => Op::Gt,
        PdlOp::Lt => Op::Lt,
        PdlOp::Gte => Op::Gte,
        PdlOp::Lte => Op::Lte,
        PdlOp::And => Op::And,
        PdlOp::Or => Op::Or,
        PdlOp::Not => Op::Not,
        PdlOp::Neg => Op::Neg,
    })
}
