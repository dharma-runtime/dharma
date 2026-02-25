use crate::error::DharmaError;
use crate::pdl::ast::{Expr as PdlExpr, Literal as PdlLiteral, Op as PdlOp};
use crate::pdl::expr::parse_expr;
use ciborium::value::Value;
use dharma::dhlq::{
    AggFunc, AggSpec, BucketSpec, ExplodeSpec, JoinSpec, QueryOp, QueryPlan, QuerySource,
    SearchSpec, SelectItem, SortKey,
};
use dharma::reactor::{Expr, Op};

pub fn parse_plan(query: &str, start_line: usize) -> Result<QueryPlan, DharmaError> {
    let segments = split_segments(query, start_line)?;
    if segments.is_empty() {
        return Err(DharmaError::Validation("empty query".to_string()));
    }
    let first = segments[0].1.trim();
    let source = if first.starts_with("search ") {
        QuerySource::Search(parse_search(first, segments[0].0)?)
    } else if is_operator_segment(first) {
        return Err(DharmaError::Validation(format!(
            "line {} col 1: missing query source",
            segments[0].0
        )));
    } else {
        QuerySource::Table(first.to_string())
    };

    let mut ops = Vec::new();
    for (line_no, segment) in segments.iter().skip(1) {
        let seg = segment.trim();
        if seg.is_empty() {
            continue;
        }
        let op = parse_op(seg, *line_no)?;
        ops.push(op);
    }

    Ok(QueryPlan {
        version: 1,
        source,
        ops,
    })
}

fn split_segments(query: &str, start_line: usize) -> Result<Vec<(usize, String)>, DharmaError> {
    let mut segments: Vec<(usize, String)> = Vec::new();
    let mut current: Option<(usize, String)> = None;
    for (idx, raw) in query.lines().enumerate() {
        let line_no = start_line + idx;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with('|') {
            if let Some(seg) = current.take() {
                segments.push(seg);
            }
            current = Some((line_no, trimmed.trim_start_matches('|').trim().to_string()));
        } else if let Some((_, ref mut buf)) = current {
            buf.push(' ');
            buf.push_str(trimmed);
        } else {
            current = Some((line_no, trimmed.to_string()));
        }
    }
    if let Some(seg) = current.take() {
        segments.push(seg);
    }
    Ok(segments)
}

fn is_operator_segment(segment: &str) -> bool {
    let lower = segment.trim_start().to_ascii_lowercase();
    lower.starts_with("where ")
        || lower.starts_with("sort ")
        || lower.starts_with("drop ")
        || lower.starts_with("take ")
        || lower.starts_with("sel ")
        || lower.starts_with("select ")
        || lower.starts_with("join ")
        || lower.starts_with("lj ")
        || lower.starts_with("bucket ")
        || lower.starts_with("by ")
        || lower.starts_with("group ")
        || lower.starts_with("agg ")
        || lower.starts_with("explode ")
}

fn parse_op(segment: &str, line_no: usize) -> Result<QueryOp, DharmaError> {
    let mut parts = segment.splitn(2, ' ');
    let op = parts.next().unwrap_or("").trim();
    let rest = parts.next().unwrap_or("").trim();
    match op {
        "where" => Ok(QueryOp::Where(parse_where(rest, line_no)?)),
        "sort" => Ok(QueryOp::Sort(parse_sort(rest, line_no)?)),
        "drop" => Ok(QueryOp::Drop(parse_expr_wrapped(rest, line_no)?)),
        "take" => Ok(QueryOp::Take(parse_expr_wrapped(rest, line_no)?)),
        "sel" | "select" => Ok(QueryOp::Select(parse_select(rest, line_no)?)),
        "lj" | "join" => Ok(QueryOp::Join(parse_join(rest, line_no)?)),
        "bucket" => Ok(QueryOp::Bucket(parse_bucket(rest, line_no)?)),
        "by" => Ok(QueryOp::GroupBy(parse_group_by(rest, line_no)?)),
        "group" => Ok(parse_group_by_keyword(rest, line_no)?),
        "agg" => Ok(QueryOp::Agg(parse_agg(rest, line_no)?)),
        "explode" => Ok(QueryOp::Explode(parse_explode(rest, line_no)?)),
        _ => Err(DharmaError::Validation(format!(
            "line {line_no} col 1: unsupported query op '{op}'"
        ))),
    }
}

fn parse_where(input: &str, line_no: usize) -> Result<Expr, DharmaError> {
    let rewritten = normalize_where(input);
    parse_expr_wrapped(&rewritten, line_no)
}

fn parse_expr_wrapped(input: &str, line_no: usize) -> Result<Expr, DharmaError> {
    let rewritten = rewrite_params(input);
    let rewritten = rewrite_between(&rewritten);
    let expr = parse_expr(&rewritten).map_err(|e| with_line(e, line_no))?;
    Ok(convert_expr(&expr)?)
}

fn parse_search(input: &str, line_no: usize) -> Result<SearchSpec, DharmaError> {
    let rest = input.trim_start_matches("search").trim();
    if rest.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid search"
        )));
    }
    let (query_part, after_query) = split_search_query(rest);
    let query_expr = parse_expr_wrapped(query_part.trim(), line_no)?;
    let mut fields = Vec::new();
    let mut fuzz = 0u8;
    if let Some(idx) = after_query.find(" in ") {
        let field_part = &after_query[idx + 4..];
        let (fields_part, opts) = split_search_opts(field_part);
        fields = fields_part
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        for opt in opts {
            if let Some(val) = opt.strip_prefix("fuzz=") {
                fuzz = val.parse::<u8>().unwrap_or(0);
            }
        }
    }
    Ok(SearchSpec {
        query: query_expr,
        fields,
        fuzz,
    })
}

fn split_search_query(input: &str) -> (&str, &str) {
    let trimmed = input.trim();
    if trimmed.starts_with('"') {
        if let Some(end) = trimmed[1..].find('"') {
            let pos = end + 2;
            return (&trimmed[..pos], trimmed[pos..].trim());
        }
    }
    if let Some(idx) = trimmed.find(" in ") {
        return (&trimmed[..idx], &trimmed[idx..]);
    }
    (trimmed, "")
}

fn split_search_opts(input: &str) -> (String, Vec<String>) {
    let mut parts = input.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() {
        return (input.trim().to_string(), Vec::new());
    }
    let mut opts = Vec::new();
    while let Some(part) = parts.last() {
        if part.contains('=') {
            opts.push((*part).to_string());
            parts.pop();
        } else {
            break;
        }
    }
    opts.reverse();
    (parts.join(" ").trim().to_string(), opts)
}

fn parse_sort(rest: &str, line_no: usize) -> Result<Vec<SortKey>, DharmaError> {
    let mut keys = Vec::new();
    for item in rest.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let (desc, path) = if let Some(stripped) = item.strip_prefix('-') {
            (true, stripped.trim())
        } else {
            (false, item)
        };
        if path.is_empty() {
            return Err(DharmaError::Validation(format!(
                "line {line_no} col 1: invalid sort key"
            )));
        }
        keys.push(SortKey {
            path: path.to_string(),
            desc,
        });
    }
    Ok(keys)
}

fn parse_select(rest: &str, line_no: usize) -> Result<Vec<SelectItem>, DharmaError> {
    let mut items = Vec::new();
    for part in rest.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (path, alias) = if let Some((left, right)) = part.split_once(" as ") {
            (left.trim(), Some(right.trim().to_string()))
        } else {
            (part, None)
        };
        if path.is_empty() {
            return Err(DharmaError::Validation(format!(
                "line {line_no} col 1: invalid select field"
            )));
        }
        items.push(SelectItem {
            path: path.to_string(),
            alias,
        });
    }
    Ok(items)
}

fn parse_join(rest: &str, line_no: usize) -> Result<JoinSpec, DharmaError> {
    let mut parts = rest.splitn(2, " on ");
    let table = parts.next().unwrap_or("").trim();
    let clause = parts.next().unwrap_or("").trim();
    if table.is_empty() || clause.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid join"
        )));
    }
    let (left, right) = clause
        .split_once('=')
        .ok_or_else(|| DharmaError::Validation(format!("line {line_no} col 1: invalid join")))?;
    Ok(JoinSpec {
        table: table.to_string(),
        left: left.trim().to_string(),
        right: right.trim().to_string(),
    })
}

fn parse_bucket(rest: &str, line_no: usize) -> Result<BucketSpec, DharmaError> {
    let mut parts = rest.split_whitespace();
    let path = parts.next().unwrap_or("").trim();
    let size = parts.next().unwrap_or("").trim();
    if path.is_empty() || size.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid bucket"
        )));
    }
    let mut label = "bucket".to_string();
    if let Some(next) = parts.next() {
        if next == "as" {
            if let Some(name) = parts.next() {
                if !name.trim().is_empty() {
                    label = name.trim().to_string();
                }
            }
        }
    }
    Ok(BucketSpec {
        path: path.to_string(),
        size_secs: parse_duration(size).ok_or_else(|| {
            DharmaError::Validation(format!("line {line_no} col 1: invalid bucket size"))
        })?,
        label,
    })
}

fn parse_group_by(rest: &str, line_no: usize) -> Result<Vec<String>, DharmaError> {
    let trimmed = rest.trim();
    if trimmed.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid group by"
        )));
    }
    let inner = trimmed.trim_start_matches('(').trim_end_matches(')');
    let keys = inner
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid group by"
        )));
    }
    Ok(keys)
}

fn parse_group_by_keyword(rest: &str, line_no: usize) -> Result<QueryOp, DharmaError> {
    let trimmed = rest.trim();
    let rest = trimmed.strip_prefix("by").ok_or_else(|| {
        DharmaError::Validation(format!("line {line_no} col 1: invalid group by"))
    })?;
    Ok(QueryOp::GroupBy(parse_group_by(rest, line_no)?))
}

fn parse_agg(rest: &str, line_no: usize) -> Result<Vec<AggSpec>, DharmaError> {
    let mut specs = Vec::new();
    for item in rest.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let (item, alias_out) = if let Some((left, right)) = item.split_once(" as ") {
            (left.trim(), Some(right.trim().to_string()))
        } else {
            (item, None)
        };
        let (func_name, inner) = item
            .split_once('(')
            .ok_or_else(|| DharmaError::Validation(format!("line {line_no} col 1: invalid agg")))?;
        let func = AggFunc::from_str(func_name.trim())
            .map_err(|_| DharmaError::Validation(format!("line {line_no} col 1: invalid agg")))?;
        let inner = inner.trim_end_matches(')').trim();
        let path = if inner.is_empty() {
            None
        } else {
            Some(inner.to_string())
        };
        let alias = alias_out.filter(|s| !s.is_empty());
        specs.push(AggSpec { func, path, alias });
    }
    if specs.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid agg"
        )));
    }
    Ok(specs)
}

fn parse_explode(rest: &str, line_no: usize) -> Result<ExplodeSpec, DharmaError> {
    let (path, alias) = rest
        .split_once(" as ")
        .ok_or_else(|| DharmaError::Validation(format!("line {line_no} col 1: invalid explode")))?;
    let path = path.trim();
    let alias = alias.trim();
    if path.is_empty() || alias.is_empty() {
        return Err(DharmaError::Validation(format!(
            "line {line_no} col 1: invalid explode"
        )));
    }
    let mut key = None;
    let value;
    if alias.starts_with('(') {
        let inner = alias.trim_start_matches('(').trim_end_matches(')');
        let parts: Vec<&str> = inner
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        if parts.len() == 2 {
            key = Some(parts[0].to_string());
            value = parts[1].to_string();
        } else if parts.len() == 1 {
            value = parts[0].to_string();
        } else {
            return Err(DharmaError::Validation(format!(
                "line {line_no} col 1: invalid explode"
            )));
        }
    } else {
        value = alias.to_string();
    }
    Ok(ExplodeSpec {
        path: path.to_string(),
        key,
        value,
    })
}

fn parse_duration(value: &str) -> Option<u64> {
    if value.ends_with('d') {
        value
            .trim_end_matches('d')
            .parse::<u64>()
            .ok()
            .map(|v| v * 86_400)
    } else if value.ends_with('h') {
        value
            .trim_end_matches('h')
            .parse::<u64>()
            .ok()
            .map(|v| v * 3_600)
    } else if value.ends_with('m') {
        value
            .trim_end_matches('m')
            .parse::<u64>()
            .ok()
            .map(|v| v * 60)
    } else if value.ends_with('s') {
        value.trim_end_matches('s').parse::<u64>().ok()
    } else {
        value.parse::<u64>().ok()
    }
}

fn normalize_where(input: &str) -> String {
    let mut out = String::new();
    let mut depth = 0i32;
    let mut in_str = false;
    for ch in input.chars() {
        match ch {
            '"' => {
                in_str = !in_str;
                out.push(ch);
            }
            '(' if !in_str => {
                depth += 1;
                out.push(ch);
            }
            ')' if !in_str => {
                depth -= 1;
                out.push(ch);
            }
            ',' if !in_str && depth == 0 => {
                out.push_str(" and ");
            }
            _ => out.push(ch),
        }
    }
    out
}

fn rewrite_params(input: &str) -> String {
    let mut out = String::new();
    let mut chars = input.chars().peekable();
    let mut in_str = false;
    while let Some(ch) = chars.next() {
        if ch == '"' {
            in_str = !in_str;
            out.push(ch);
            continue;
        }
        if !in_str && ch == '$' {
            let mut ident = String::new();
            while let Some(next) = chars.peek().copied() {
                if next.is_ascii_alphanumeric() || next == '_' {
                    ident.push(next);
                    chars.next();
                } else {
                    break;
                }
            }
            if !ident.is_empty() {
                if ident.chars().all(|c| c.is_ascii_digit()) {
                    out.push_str(&format!("param({})", ident));
                } else {
                    out.push_str(&format!("param(\"{}\")", ident));
                }
                continue;
            }
        }
        out.push(ch);
    }
    out
}

fn rewrite_between(input: &str) -> String {
    let mut out = String::new();
    let mut rest = input;
    loop {
        let idx = find_between(rest);
        let Some(idx) = idx else {
            out.push_str(rest);
            break;
        };
        let (before, after_between) = rest.split_at(idx);
        let after_between = after_between.trim_start_matches(" between ");
        let (prefix, left) = split_left_expr(before);
        let Some((lo, hi, tail)) = parse_between_range(after_between) else {
            out.push_str(rest);
            break;
        };
        out.push_str(&prefix);
        out.push_str(&format!("between({}, {}, {})", left, lo, hi));
        rest = tail;
    }
    out
}

fn find_between(input: &str) -> Option<usize> {
    let mut in_str = false;
    let mut depth = 0i32;
    let bytes = input.as_bytes();
    let needle = b" between ";
    let mut i = 0usize;
    while i + needle.len() <= bytes.len() {
        let ch = bytes[i] as char;
        if ch == '"' {
            in_str = !in_str;
        } else if !in_str {
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
            }
        }
        if !in_str && depth == 0 && bytes[i..].starts_with(needle) {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn split_left_expr(input: &str) -> (String, String) {
    let trimmed = input.trim_end();
    let mut split_at = 0usize;
    for (idx, ch) in trimmed.char_indices().rev() {
        if ch.is_whitespace() || ch == ',' {
            split_at = idx + ch.len_utf8();
            break;
        }
    }
    let prefix = trimmed[..split_at].to_string();
    let left = trimmed[split_at..].trim().to_string();
    (prefix, left)
}

fn parse_between_range(input: &str) -> Option<(String, String, &str)> {
    let trimmed = input.trim_start();
    let mut chars = trimmed.char_indices();
    let first = chars.next()?;
    if first.1 != '(' {
        return None;
    }
    let mut depth = 0i32;
    let mut end = None;
    let mut in_str = false;
    for (idx, ch) in trimmed.char_indices() {
        if ch == '"' {
            in_str = !in_str;
        }
        if !in_str {
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
                if depth == 0 {
                    end = Some(idx);
                    break;
                }
            }
        }
    }
    let end = end?;
    let inside = &trimmed[1..end];
    let tail = &trimmed[end + 1..];
    let (lo, hi) = split_range_parts(inside)?;
    Some((lo, hi, tail))
}

fn split_range_parts(input: &str) -> Option<(String, String)> {
    let mut depth = 0i32;
    let mut in_str = false;
    for (idx, ch) in input.char_indices() {
        if ch == '"' {
            in_str = !in_str;
        }
        if !in_str {
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
            } else if ch == ',' && depth == 0 {
                let left = input[..idx].trim();
                let right = input[idx + 1..].trim();
                if left.is_empty() || right.is_empty() {
                    return None;
                }
                return Some((left.to_string(), right.to_string()));
            }
        }
    }
    None
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
        PdlExpr::UnaryOp(op, inner) => Ok(Expr::Unary(
            convert_op(*op)?,
            Box::new(convert_expr(inner)?),
        )),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_plan_group_by_and_agg() {
        let query = r#"
analytics.orders
| where status == "open"
| by (customer_id)
| agg count() as total, sum(amount) as amount_sum
"#;
        let plan = parse_plan(query, 1).unwrap();
        assert_eq!(plan.version, 1);
        assert_eq!(
            plan.source,
            QuerySource::Table("analytics.orders".to_string())
        );
        assert_eq!(plan.ops.len(), 3);
        assert!(matches!(
            &plan.ops[1],
            QueryOp::GroupBy(keys) if keys == &vec!["customer_id".to_string()]
        ));
        assert!(matches!(
            &plan.ops[2],
            QueryOp::Agg(specs)
                if specs.len() == 2
                    && specs[0].func == AggFunc::Count
                    && specs[0].path.is_none()
                    && specs[0].alias.as_deref() == Some("total")
                    && specs[1].func == AggFunc::Sum
                    && specs[1].path.as_deref() == Some("amount")
                    && specs[1].alias.as_deref() == Some("amount_sum")
        ));
    }
}
