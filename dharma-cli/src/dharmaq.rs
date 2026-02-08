use crate::pdl::ast::{Expr, Literal, Op};
use crate::DharmaError;
use dharma_core::dharmaq::{CmpOp, Filter, Predicate, QueryPlan};
use dharma_core::SubjectId;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_until};
use nom::character::complete::{digit1, space0};
use nom::combinator::map_res;
use nom::sequence::delimited;
use nom::IResult;

const DEFAULT_TABLE: &str = "assertions";
const DEFAULT_LIMIT: usize = 25;

pub fn parse_query(input: &str) -> Result<QueryPlan, DharmaError> {
    let mut input = input.trim();
    if let Some(rest) = strip_prefix_ci(input, "query") {
        input = rest.trim();
    }
    if let Some(unquoted) = strip_outer_quotes(input) {
        input = unquoted.trim();
    }
    let segments = split_pipeline(input);
    if segments.is_empty() {
        return Err(DharmaError::Validation("invalid query".to_string()));
    }
    let mut plan = QueryPlan {
        table: DEFAULT_TABLE.to_string(),
        filter: None,
        limit: DEFAULT_LIMIT,
    };
    let mut idx = 0;
    if !is_operator_segment(&segments[0]) {
        plan.table = normalize_table(&segments[0])?;
        idx = 1;
    }
    for segment in segments.iter().skip(idx) {
        if segment.is_empty() {
            continue;
        }
        if starts_with_keyword(segment, "where") {
            let filter = parse_where_segment(segment)?;
            plan.filter = merge_filter(plan.filter, filter);
            continue;
        }
        if starts_with_keyword(segment, "search") {
            let filter = parse_search_segment(segment)?;
            plan.filter = merge_filter(plan.filter, filter);
            continue;
        }
        if starts_with_keyword(segment, "take") {
            plan.limit = parse_take_segment(segment)?;
            continue;
        }
        return Err(DharmaError::Validation(format!(
            "unsupported query operator: {segment}"
        )));
    }
    Ok(plan)
}

fn normalize_table(segment: &str) -> Result<String, DharmaError> {
    let trimmed = segment.trim();
    if trimmed.is_empty() {
        return Err(DharmaError::Validation("missing table".to_string()));
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower == "assertion" || lower == "assertions" {
        return Ok(DEFAULT_TABLE.to_string());
    }
    Ok(trimmed.to_string())
}

fn parse_where_segment(segment: &str) -> Result<Filter, DharmaError> {
    let rest = strip_prefix_ci(segment, "where")
        .ok_or_else(|| DharmaError::Validation("invalid where clause".to_string()))?
        .trim();
    if rest.is_empty() {
        return Err(DharmaError::Validation("empty where clause".to_string()));
    }
    let rewritten = quote_hex_literals(rest);
    let expr = crate::pdl::expr::parse_expr(&rewritten)?;
    expr_to_filter(&expr)
}

fn parse_search_segment(segment: &str) -> Result<Filter, DharmaError> {
    let rest = strip_prefix_ci(segment, "search")
        .ok_or_else(|| DharmaError::Validation("invalid search clause".to_string()))?
        .trim();
    if rest.is_empty() {
        return Err(DharmaError::Validation("empty search query".to_string()));
    }
    let terms = split_or_terms(rest);
    let mut filters = Vec::new();
    for term in terms {
        let mut term = term.trim();
        if term.is_empty() {
            continue;
        }
        let (negated, trimmed) = strip_not_term(term);
        term = trimmed.trim();
        if term.is_empty() {
            return Err(DharmaError::Validation("empty search query".to_string()));
        }
        let query = if term.starts_with('"') || term.starts_with('\'') {
            let (_, query) = parse_quoted(term)
                .map_err(|_| DharmaError::Validation("invalid search query".to_string()))?;
            query
        } else {
            let lower = term.to_ascii_lowercase();
            let query = if let Some(pos) = lower.find(" in ") {
                term[..pos].trim()
            } else {
                term.trim()
            };
            if query.is_empty() {
                return Err(DharmaError::Validation("empty search query".to_string()));
            }
            query.to_string()
        };
        let pred = Predicate::TextSearch(query);
        let filter = if negated {
            Filter::Not(Box::new(Filter::Leaf(pred)))
        } else {
            Filter::Leaf(pred)
        };
        filters.push(filter);
    }
    if filters.is_empty() {
        return Err(DharmaError::Validation("empty search query".to_string()));
    }
    if filters.len() == 1 {
        return Ok(filters.remove(0));
    }
    Ok(Filter::Or(filters))
}

fn parse_take_segment(segment: &str) -> Result<usize, DharmaError> {
    let rest = strip_prefix_ci(segment, "take")
        .ok_or_else(|| DharmaError::Validation("invalid take clause".to_string()))?;
    let (remaining, value) = parse_number(rest)
        .map_err(|_| DharmaError::Validation("invalid take value".to_string()))?;
    if !remaining.trim().is_empty() {
        return Err(DharmaError::Validation("invalid take value".to_string()));
    }
    let value = usize::try_from(value)
        .map_err(|_| DharmaError::Validation("take out of range".to_string()))?;
    Ok(value)
}

fn parse_number(input: &str) -> IResult<&str, u64> {
    let (rest, value) = preceded_space(map_res(digit1, str::parse::<u64>))(input)?;
    Ok((rest, value))
}

fn quote_hex_literals(input: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut in_single = false;
    let mut in_double = false;
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        if ch == '\'' && !in_double {
            in_single = !in_single;
            out.push(ch);
            i += 1;
            continue;
        }
        if ch == '"' && !in_single {
            in_double = !in_double;
            out.push(ch);
            i += 1;
            continue;
        }
        if in_single || in_double {
            out.push(ch);
            i += 1;
            continue;
        }
        if is_hex_start(&chars, i) {
            let (len, token) = extract_hex_token(&chars, i);
            out.push('"');
            out.push_str(&token);
            out.push('"');
            i += len;
            continue;
        }
        out.push(ch);
        i += 1;
    }
    out
}

fn is_hex_start(chars: &[char], idx: usize) -> bool {
    if idx >= chars.len() {
        return false;
    }
    if idx > 0 && is_ident_char(chars[idx - 1]) {
        return false;
    }
    if chars[idx] == '0'
        && idx + 1 < chars.len()
        && (chars[idx + 1] == 'x' || chars[idx + 1] == 'X')
    {
        if idx + 2 + 64 > chars.len() {
            return false;
        }
        if !chars[idx + 2..idx + 2 + 64].iter().all(|c| is_hex_char(*c)) {
            return false;
        }
        if idx + 2 + 64 < chars.len() && is_ident_char(chars[idx + 2 + 64]) {
            return false;
        }
        return true;
    }
    if idx + 64 > chars.len() {
        return false;
    }
    if !chars[idx..idx + 64].iter().all(|c| is_hex_char(*c)) {
        return false;
    }
    if idx + 64 < chars.len() && is_ident_char(chars[idx + 64]) {
        return false;
    }
    true
}

fn extract_hex_token(chars: &[char], idx: usize) -> (usize, String) {
    if chars[idx] == '0'
        && idx + 1 < chars.len()
        && (chars[idx + 1] == 'x' || chars[idx + 1] == 'X')
        && idx + 2 + 64 <= chars.len()
        && chars[idx + 2..idx + 2 + 64].iter().all(|c| is_hex_char(*c))
    {
        let token: String = chars[idx..idx + 2 + 64].iter().collect();
        return (2 + 64, token);
    }
    let token: String = chars[idx..idx + 64].iter().collect();
    (64, token)
}

fn is_hex_char(ch: char) -> bool {
    ch.is_ascii_hexdigit()
}

fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'
}

fn merge_filter(existing: Option<Filter>, next: Filter) -> Option<Filter> {
    match existing {
        None => Some(next),
        Some(prev) => Some(merge_and(prev, next)),
    }
}

fn merge_and(left: Filter, right: Filter) -> Filter {
    match (left, right) {
        (Filter::And(mut left_items), Filter::And(right_items)) => {
            left_items.extend(right_items);
            Filter::And(left_items)
        }
        (Filter::And(mut items), other) => {
            items.push(other);
            Filter::And(items)
        }
        (other, Filter::And(mut items)) => {
            let mut out = Vec::with_capacity(items.len() + 1);
            out.push(other);
            out.append(&mut items);
            Filter::And(out)
        }
        (left, right) => Filter::And(vec![left, right]),
    }
}

fn merge_or(left: Filter, right: Filter) -> Filter {
    match (left, right) {
        (Filter::Or(mut left_items), Filter::Or(right_items)) => {
            left_items.extend(right_items);
            Filter::Or(left_items)
        }
        (Filter::Or(mut items), other) => {
            items.push(other);
            Filter::Or(items)
        }
        (other, Filter::Or(mut items)) => {
            let mut out = Vec::with_capacity(items.len() + 1);
            out.push(other);
            out.append(&mut items);
            Filter::Or(out)
        }
        (left, right) => Filter::Or(vec![left, right]),
    }
}

fn expr_to_filter(expr: &Expr) -> Result<Filter, DharmaError> {
    match expr {
        Expr::BinaryOp(Op::And, left, right) => {
            let left = expr_to_filter(left)?;
            let right = expr_to_filter(right)?;
            Ok(merge_and(left, right))
        }
        Expr::BinaryOp(Op::Or, left, right) => {
            let left = expr_to_filter(left)?;
            let right = expr_to_filter(right)?;
            Ok(merge_or(left, right))
        }
        Expr::UnaryOp(Op::Not, inner) => Ok(Filter::Not(Box::new(expr_to_filter(inner)?))),
        Expr::BinaryOp(op, left, right) => {
            if let Expr::UnaryOp(Op::Not, inner) = left.as_ref() {
                if is_comparison_op(*op) {
                    let cmp = comparison_to_filter(*op, inner, right)?;
                    return Ok(Filter::Not(Box::new(cmp)));
                }
            }
            comparison_to_filter(*op, left, right)
        }
        _ => Err(DharmaError::Validation(
            "unsupported where expression".to_string(),
        )),
    }
}

fn is_comparison_op(op: Op) -> bool {
    matches!(
        op,
        Op::Eq | Op::Neq | Op::Gt | Op::Gte | Op::Lt | Op::Lte | Op::In
    )
}

fn comparison_to_filter(op: Op, left: &Expr, right: &Expr) -> Result<Filter, DharmaError> {
    if op == Op::In {
        return in_list_filter(left, right);
    }
    if let Some((field, literal, flipped)) = field_literal_pair(left, right) {
        let op = if flipped { flip_op(op) } else { op };
        return build_predicate(&field, op, literal);
    }
    Err(DharmaError::Validation(
        "invalid where expression".to_string(),
    ))
}

fn in_list_filter(left: &Expr, right: &Expr) -> Result<Filter, DharmaError> {
    let Some(field) = field_from_expr(left) else {
        return Err(DharmaError::Validation(
            "in expects field on left".to_string(),
        ));
    };
    let Some(list) = list_literal_from_expr(right) else {
        return Err(DharmaError::Validation(
            "in expects list literal".to_string(),
        ));
    };
    if list.is_empty() {
        return Ok(Filter::Or(Vec::new()));
    }
    let mut filters = Vec::new();
    for item in list {
        filters.push(build_predicate(&field, Op::Eq, item)?);
    }
    if filters.len() == 1 {
        return Ok(filters.remove(0));
    }
    Ok(Filter::Or(filters))
}

fn build_predicate(field: &str, op: Op, literal: QueryLiteral) -> Result<Filter, DharmaError> {
    if op == Op::Neq {
        let eq = build_predicate(field, Op::Eq, literal)?;
        return Ok(Filter::Not(Box::new(eq)));
    }
    let field_lower = field.to_ascii_lowercase();
    match field_lower.as_str() {
        "seq" => {
            let QueryLiteral::Int(value) = literal else {
                return Err(DharmaError::Validation("seq expects int".to_string()));
            };
            if value < 0 {
                return Err(DharmaError::Validation("seq must be >= 0".to_string()));
            }
            let op = cmp_from_op(op)?;
            Ok(Filter::Leaf(Predicate::Seq {
                op,
                value: value as u64,
            }))
        }
        "typ" | "type" => {
            let text = literal_text(literal)?;
            if op != Op::Eq {
                return Err(DharmaError::Validation("typ expects ==".to_string()));
            }
            Ok(Filter::Leaf(Predicate::TypEq(text)))
        }
        "subject" | "sub" => {
            let text = literal_text(literal)?;
            let Some(bytes) = parse_bytes32_literal(&text) else {
                return Err(DharmaError::Validation(
                    "subject expects 32-byte hex".to_string(),
                ));
            };
            if op != Op::Eq {
                return Err(DharmaError::Validation("subject expects ==".to_string()));
            }
            Ok(Filter::Leaf(Predicate::SubjectEq(SubjectId::from_bytes(
                bytes,
            ))))
        }
        _ => match literal {
            QueryLiteral::Int(value) => {
                let op = cmp_from_op(op)?;
                Ok(Filter::Leaf(Predicate::DynI64 {
                    col: field.to_string(),
                    op,
                    value,
                }))
            }
            QueryLiteral::Bool(value) => {
                if op != Op::Eq {
                    return Err(DharmaError::Validation("bool expects ==".to_string()));
                }
                Ok(Filter::Leaf(Predicate::DynBool {
                    col: field.to_string(),
                    value,
                }))
            }
            QueryLiteral::Text(text) => {
                if op != Op::Eq {
                    return Err(DharmaError::Validation("text expects ==".to_string()));
                }
                if let Some(bytes) = parse_bytes32_literal(&text) {
                    return Ok(Filter::Leaf(Predicate::DynBytes32 {
                        col: field.to_string(),
                        value: bytes,
                    }));
                }
                Ok(Filter::Leaf(Predicate::DynSymbol {
                    col: field.to_string(),
                    value: text,
                }))
            }
        },
    }
}

fn cmp_from_op(op: Op) -> Result<CmpOp, DharmaError> {
    match op {
        Op::Eq => Ok(CmpOp::Eq),
        Op::Gt => Ok(CmpOp::Gt),
        Op::Gte => Ok(CmpOp::Gte),
        Op::Lt => Ok(CmpOp::Lt),
        Op::Lte => Ok(CmpOp::Lte),
        _ => Err(DharmaError::Validation("invalid comparison".to_string())),
    }
}

fn flip_op(op: Op) -> Op {
    match op {
        Op::Gt => Op::Lt,
        Op::Gte => Op::Lte,
        Op::Lt => Op::Gt,
        Op::Lte => Op::Gte,
        other => other,
    }
}

fn field_literal_pair(left: &Expr, right: &Expr) -> Option<(String, QueryLiteral, bool)> {
    if let Some(field) = field_from_expr(left) {
        if let Some(literal) = literal_from_expr(right) {
            return Some((field, literal, false));
        }
    }
    if let Some(field) = field_from_expr(right) {
        if let Some(literal) = literal_from_expr(left) {
            return Some((field, literal, true));
        }
    }
    None
}

fn field_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Path(path) if !path.is_empty() => Some(path.join(".")),
        _ => None,
    }
}

fn list_literal_from_expr(expr: &Expr) -> Option<Vec<QueryLiteral>> {
    match expr {
        Expr::Literal(Literal::List(items)) => {
            let mut out = Vec::new();
            for item in items {
                out.push(literal_from_expr(item)?);
            }
            Some(out)
        }
        _ => None,
    }
}

fn literal_from_expr(expr: &Expr) -> Option<QueryLiteral> {
    match expr {
        Expr::Literal(Literal::Int(value)) => Some(QueryLiteral::Int(*value)),
        Expr::Literal(Literal::Bool(value)) => Some(QueryLiteral::Bool(*value)),
        Expr::Literal(Literal::Text(value)) => Some(QueryLiteral::Text(value.clone())),
        Expr::Literal(Literal::Enum(value)) => Some(QueryLiteral::Text(value.clone())),
        Expr::Literal(Literal::List(_)) => None,
        Expr::UnaryOp(Op::Neg, inner) => match literal_from_expr(inner) {
            Some(QueryLiteral::Int(value)) => Some(QueryLiteral::Int(-value)),
            _ => None,
        },
        Expr::Path(path) if !path.is_empty() => Some(QueryLiteral::Text(path.join("."))),
        _ => None,
    }
}

fn literal_text(literal: QueryLiteral) -> Result<String, DharmaError> {
    match literal {
        QueryLiteral::Text(text) => Ok(text),
        _ => Err(DharmaError::Validation("expected text literal".to_string())),
    }
}

fn parse_bytes32_literal(raw: &str) -> Option<[u8; 32]> {
    let hex = raw.trim_start_matches("0x");
    if hex.len() != 64 {
        return None;
    }
    let bytes = dharma_core::types::hex_decode(hex).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Some(out)
}

#[derive(Clone, Debug)]
enum QueryLiteral {
    Int(i64),
    Bool(bool),
    Text(String),
}

fn parse_quoted(input: &str) -> IResult<&str, String> {
    let (rest, text) = alt((
        delimited(tag("\""), take_until("\""), tag("\"")),
        delimited(tag("'"), take_until("'"), tag("'")),
    ))(input)?;
    Ok((rest, text.to_string()))
}

fn preceded_space<'a, F, O>(parser: F) -> impl FnMut(&'a str) -> IResult<&'a str, O>
where
    F: FnMut(&'a str) -> IResult<&'a str, O>,
{
    delimited(space0, parser, space0)
}

fn strip_outer_quotes(input: &str) -> Option<&str> {
    let bytes = input.as_bytes();
    if bytes.len() < 2 {
        return None;
    }
    let first = bytes[0];
    let last = bytes[bytes.len() - 1];
    if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
        return Some(&input[1..input.len() - 1]);
    }
    None
}

fn split_pipeline(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    for ch in input.chars() {
        match ch {
            '\'' if !in_double => {
                in_single = !in_single;
                current.push(ch);
            }
            '"' if !in_single => {
                in_double = !in_double;
                current.push(ch);
            }
            '|' if !in_single && !in_double => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    parts.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        parts.push(trimmed.to_string());
    }
    parts
}

fn split_or_terms(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        if ch == '\'' && !in_double {
            in_single = !in_single;
            current.push(ch);
            i += 1;
            continue;
        }
        if ch == '"' && !in_single {
            in_double = !in_double;
            current.push(ch);
            i += 1;
            continue;
        }
        if !in_single && !in_double {
            if (bytes[i] == b'o' || bytes[i] == b'O')
                && i + 1 < bytes.len()
                && (bytes[i + 1] == b'r' || bytes[i + 1] == b'R')
            {
                let prev = if i == 0 { b' ' } else { bytes[i - 1] };
                let next = if i + 2 >= bytes.len() {
                    b' '
                } else {
                    bytes[i + 2]
                };
                if prev.is_ascii_whitespace() && next.is_ascii_whitespace() {
                    let trimmed = current.trim();
                    if !trimmed.is_empty() {
                        parts.push(trimmed.to_string());
                    }
                    current.clear();
                    i += 2;
                    continue;
                }
            }
        }
        current.push(ch);
        i += 1;
    }
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        parts.push(trimmed.to_string());
    }
    parts
}

fn strip_not_term(input: &str) -> (bool, &str) {
    let trimmed = input.trim_start();
    if trimmed.len() < 3 {
        return (false, input);
    }
    let head = &trimmed[..3];
    if !head.eq_ignore_ascii_case("not") {
        return (false, input);
    }
    let rest = &trimmed[3..];
    if rest.is_empty()
        || !rest
            .chars()
            .next()
            .map(|c| c.is_whitespace())
            .unwrap_or(false)
    {
        return (false, input);
    }
    (true, rest.trim_start())
}

fn starts_with_keyword(segment: &str, keyword: &str) -> bool {
    let trimmed = segment.trim_start();
    if trimmed.len() < keyword.len() {
        return false;
    }
    let head = &trimmed[..keyword.len()];
    if !head.eq_ignore_ascii_case(keyword) {
        return false;
    }
    if trimmed.len() == keyword.len() {
        return true;
    }
    trimmed[keyword.len()..]
        .chars()
        .next()
        .map(|c| c.is_whitespace())
        .unwrap_or(false)
}

fn strip_prefix_ci<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = input.trim_start();
    if trimmed.len() < keyword.len() {
        return None;
    }
    let head = &trimmed[..keyword.len()];
    if !head.eq_ignore_ascii_case(keyword) {
        return None;
    }
    Some(&trimmed[keyword.len()..])
}

fn is_operator_segment(segment: &str) -> bool {
    starts_with_keyword(segment, "where")
        || starts_with_keyword(segment, "search")
        || starts_with_keyword(segment, "take")
}

#[cfg(test)]
mod tests {
    use super::*;
    use dharma_core::dharmaq::{Filter, Predicate};

    #[test]
    fn parse_query_with_where_and_take() {
        let plan = parse_query("assertions | where seq > 10 | take 5").unwrap();
        assert_eq!(plan.table, "assertions");
        assert_eq!(plan.limit, 5);
        assert!(matches!(
            plan.filter,
            Some(Filter::Leaf(Predicate::Seq {
                op: CmpOp::Gt,
                value: 10
            }))
        ));
    }

    #[test]
    fn parse_query_with_search_default_table() {
        let plan = parse_query("search \"invoice 44\"").unwrap();
        assert_eq!(plan.table, "assertions");
        assert_eq!(plan.limit, DEFAULT_LIMIT);
        assert!(matches!(
            plan.filter,
            Some(Filter::Leaf(Predicate::TextSearch(q))) if q == "invoice 44"
        ));
    }

    #[test]
    fn parse_query_search_or_terms() {
        let plan = parse_query("search \"alpha\" or \"beta\"").unwrap();
        let Some(Filter::Or(items)) = plan.filter else {
            panic!("expected or filter");
        };
        assert_eq!(items.len(), 2);
        assert!(items
            .iter()
            .any(|item| matches!(item, Filter::Leaf(Predicate::TextSearch(q)) if q == "alpha")));
        assert!(items
            .iter()
            .any(|item| matches!(item, Filter::Leaf(Predicate::TextSearch(q)) if q == "beta")));
    }

    #[test]
    fn parse_query_subject_and_typ() {
        let subject = SubjectId::from_bytes([7u8; 32]);
        let hex = subject.to_hex();
        let query = format!("assertions | where typ == note.text and subject == {hex}");
        let plan = parse_query(&query).unwrap();
        let Some(Filter::And(items)) = plan.filter else {
            panic!("expected and filter");
        };
        assert!(items
            .iter()
            .any(|f| matches!(f, Filter::Leaf(Predicate::TypEq(v)) if v == "note.text")));
        assert!(items
            .iter()
            .any(|f| matches!(f, Filter::Leaf(Predicate::SubjectEq(s)) if *s == subject)));
    }

    #[test]
    fn parse_query_seq_gte() {
        let plan = parse_query("assertions | where seq >= 3").unwrap();
        assert!(matches!(
            plan.filter,
            Some(Filter::Leaf(Predicate::Seq {
                op: CmpOp::Gte,
                value: 3
            }))
        ));
    }

    #[test]
    fn parse_query_dynamic_int() {
        let plan = parse_query("assertions | where amount > 10").unwrap();
        assert!(matches!(
            plan.filter,
            Some(Filter::Leaf(Predicate::DynI64 { col, op: CmpOp::Gt, value: 10 })) if col == "amount"
        ));
    }

    #[test]
    fn parse_query_with_or() {
        let plan = parse_query("assertions | where amount > 10 or status == Open").unwrap();
        let Some(Filter::Or(items)) = plan.filter else {
            panic!("expected or filter");
        };
        assert!(items.iter().any(|f| matches!(f, Filter::Leaf(Predicate::DynI64 { col, op: CmpOp::Gt, value: 10 }) if col == "amount")));
        assert!(items.iter().any(|f| matches!(f, Filter::Leaf(Predicate::DynSymbol { col, value }) if col == "status" && value == "Open")));
    }

    #[test]
    fn parse_query_with_not() {
        let plan = parse_query("assertions | where not amount > 10").unwrap();
        assert!(matches!(
            plan.filter,
            Some(Filter::Not(inner)) if matches!(inner.as_ref(), Filter::Leaf(Predicate::DynI64 { col, op: CmpOp::Gt, value: 10 }) if col == "amount")
        ));
    }

    #[test]
    fn parse_query_search_not() {
        let plan = parse_query("search not \"alpha\"").unwrap();
        assert!(matches!(
            plan.filter,
            Some(Filter::Not(inner)) if matches!(inner.as_ref(), Filter::Leaf(Predicate::TextSearch(q)) if q == "alpha")
        ));
    }
}
