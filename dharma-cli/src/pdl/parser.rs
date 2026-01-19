use crate::error::DharmaError;
use crate::pdl::ast::{
    ActionDef, AggregateDef, ArgDef, Assignment, AstFile, ConcurrencyMode, EmitDef, Expr, ExternalDef,
    FieldDef, Header, Literal, Op, ReactorDef, SourceSpan, Spanned, TypeSpec, ViewDef, Visibility,
};
use crate::pdl::expr::parse_expr;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while1};
use nom::character::complete::{char, digit1, multispace0, space0};
use nom::combinator::{map, map_res, opt};
use nom::multi::separated_list1;
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::IResult;

pub fn parse(markdown: &str) -> Result<AstFile, DharmaError> {
    let (header, body, line_offset) = parse_front_matter(markdown);
    let blocks = extract_dhl_blocks(body, line_offset);
    let mut aggregates = Vec::new();
    let mut actions = Vec::new();
    let mut reactors = Vec::new();
    let mut views = Vec::new();
    let mut package = None;
    let mut external = None;
    for block in blocks {
        let doc = if block.doc.is_empty() {
            None
        } else {
            Some(block.doc.as_str())
        };
        parse_code_block(
            &block.code,
            block.start_line,
            doc,
            &mut aggregates,
            &mut actions,
            &mut reactors,
            &mut views,
            &mut package,
            &mut external,
        )?;
    }
    Ok(AstFile {
        header,
        package,
        external,
        aggregates,
        actions,
        reactors,
        views,
    })
}

struct CodeBlock {
    code: String,
    doc: String,
    start_line: usize,
}

fn extract_dhl_blocks(markdown: &str, line_offset: usize) -> Vec<CodeBlock> {
    let mut blocks = Vec::new();
    let mut prose_lines: Vec<String> = Vec::new();
    let mut in_code = false;
    let mut code_lang = String::new();
    let mut code_lines: Vec<String> = Vec::new();
    let mut fence = String::new();
    let mut start_line = 0usize;
    for (idx, raw_line) in markdown.lines().enumerate() {
        let line_no = line_offset + idx + 1;
        let trimmed = raw_line.trim();
        if !in_code {
            if let Some((marker, lang)) = parse_fence_start(trimmed) {
                in_code = true;
                fence = marker.to_string();
                code_lang = lang.to_string();
                code_lines.clear();
                start_line = line_no + 1;
            } else {
                prose_lines.push(raw_line.to_string());
            }
            continue;
        }

        if trimmed.starts_with(&fence) {
            if is_dhl_lang(&code_lang) {
                blocks.push(CodeBlock {
                    code: code_lines.join("\n"),
                    doc: prose_lines.join("\n").trim().to_string(),
                    start_line,
                });
            }
            prose_lines.clear();
            in_code = false;
            code_lang.clear();
            fence.clear();
            continue;
        }

        if is_dhl_lang(&code_lang) {
            code_lines.push(raw_line.to_string());
        }
    }
    blocks
}

fn is_dhl_lang(code_lang: &str) -> bool {
    matches!(code_lang.trim(), "dhl")
}

fn parse_front_matter(markdown: &str) -> (Header, &str, usize) {
    if !markdown.starts_with("---") {
        return (Header::default(), markdown, 0);
    }
    let mut header = Header::default();
    let mut lines = markdown.lines();
    let first = lines.next();
    if first != Some("---") {
        return (header, markdown, 0);
    }
    let mut imports: Vec<String> = Vec::new();
    let mut in_imports = false;
    let mut body_start = 0usize;
    let mut body_line = 0usize;
    for (idx, line) in markdown.lines().enumerate().skip(1) {
        if line.trim() == "---" {
            body_start = markdown
                .lines()
                .take(idx + 1)
                .map(|l| l.len() + 1)
                .sum();
            body_line = idx + 1;
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with("import:") {
            in_imports = true;
            continue;
        }
        if in_imports && trimmed.starts_with('-') {
            let item = trimmed.trim_start_matches('-').trim();
            if !item.is_empty() {
                imports.push(item.to_string());
            }
            continue;
        }
        in_imports = false;
        if let Some((key, value)) = trimmed.split_once(':') {
            let key = key.trim();
            let value = value.trim();
            match key {
                "namespace" => header.namespace = value.to_string(),
                "version" => header.version = value.to_string(),
                "concurrency" => {
                    if let Some(mode) = ConcurrencyMode::from_str(value) {
                        header.concurrency = mode;
                    }
                }
                _ => {}
            }
        }
    }
    header.imports = imports;
    let body = if body_start > 0 {
        &markdown[body_start..]
    } else {
        markdown
    };
    (header, body, body_line)
}

fn parse_code_block(
    code: &str,
    start_line: usize,
    doc: Option<&str>,
    aggregates: &mut Vec<AggregateDef>,
    actions: &mut Vec<ActionDef>,
    reactors: &mut Vec<ReactorDef>,
    views: &mut Vec<ViewDef>,
    package: &mut Option<String>,
    external: &mut Option<ExternalDef>,
) -> Result<(), DharmaError> {
    let mut current_aggregate: Option<AggregateDef> = None;
    let mut current_action: Option<ActionDef> = None;
    let mut current_reactor: Option<ReactorDef> = None;
    let mut current_view: Option<ViewDef> = None;
    let mut section: Option<Section> = None;
    let mut apply_doc = doc.unwrap_or("").trim().to_string();
    let mut flows: Vec<FlowTransition> = Vec::new();

    for (idx, raw_line) in code.lines().enumerate() {
        let line_no = start_line + idx;
        let line = raw_line.trim_end();
        if line.trim().is_empty() {
            continue;
        }
        let indent = count_indent(line)?;
        let level = indent / 4;
        let content = line.trim();
        let span = SourceSpan {
            line: line_no,
            column: indent + 1,
            text: raw_line.to_string(),
        };
        if let Some(view) = current_view.as_mut() {
            if matches!(section, Some(Section::View)) && level >= 1 {
                view.body.push(content.to_string());
                continue;
            }
        }

        match level {
            0 => {
                if let Some(action) = current_action.take() {
                    actions.push(action);
                }
                if let Some(agg) = current_aggregate.take() {
                    aggregates.push(agg);
                }
                if let Some(reactor) = current_reactor.take() {
                    reactors.push(reactor);
                }
                if let Some(view) = current_view.take() {
                    views.push(view);
                }
                if let Some(rest) = content.strip_prefix("package ") {
                    let name = rest.trim();
                    if name.is_empty() {
                        return Err(DharmaError::Validation("invalid package".to_string()));
                    }
                    match package {
                        Some(existing) if existing != name => {
                            return Err(DharmaError::Validation("duplicate package".to_string()));
                        }
                        Some(_) => {}
                        None => {
                            *package = Some(name.to_string());
                        }
                    }
                    section = None;
                } else if content == "external" {
                    if external.is_none() {
                        *external = Some(ExternalDef {
                            roles: Vec::new(),
                            time: Vec::new(),
                            datasets: Vec::new(),
                        });
                    }
                    section = Some(Section::External);
                } else if content.starts_with("view ") {
                    let name = content.trim_start_matches("view").trim();
                    if name.is_empty() {
                        return Err(DharmaError::Validation("invalid view".to_string()));
                    }
                    current_view = Some(ViewDef {
                        name: name.to_string(),
                        body: Vec::new(),
                    });
                    section = Some(Section::View);
                } else if content.starts_with("aggregate ") {
                    let rest = content.trim_start_matches("aggregate").trim();
                    let (name, extends) = if let Some((name, ext)) = rest.split_once("extends") {
                        (name.trim(), Some(ext.trim().to_string()))
                    } else {
                        (rest, None)
                    };
                    current_aggregate = Some(AggregateDef {
                        name: name.to_string(),
                        extends,
                        fields: Vec::new(),
                        invariants: Vec::new(),
                    });
                    section = None;
                } else if content.starts_with("action ") {
                    let action = parse_action_header(content)?;
                    current_action = Some(ActionDef {
                        name: action.0,
                        args: action.1,
                        validates: Vec::new(),
                        applies: Vec::new(),
                        doc: if apply_doc.is_empty() {
                            None
                        } else {
                            Some(apply_doc.clone())
                        },
                    });
                    section = None;
                } else if content.starts_with("reactor ") {
                    let name = content.trim_start_matches("reactor").trim();
                    if name.is_empty() {
                        return Err(DharmaError::Validation("invalid reactor".to_string()));
                    }
                    current_reactor = Some(ReactorDef {
                        name: name.to_string(),
                        trigger: None,
                        scope: None,
                        validates: Vec::new(),
                        emits: Vec::new(),
                    });
                    section = None;
                } else if content.starts_with("flow ") {
                    section = Some(Section::Flow);
                }
            }
            1 => {
                if let Some(reactor) = current_reactor.as_mut() {
                    if content == "validate" {
                        section = Some(Section::ReactorValidate);
                    } else if let Some(rest) = content.strip_prefix("trigger:") {
                        reactor.trigger = Some(rest.trim().to_string());
                        section = None;
                    } else if let Some(rest) = content.strip_prefix("scope:") {
                        reactor.scope = Some(rest.trim().to_string());
                        section = None;
                    } else if content.starts_with("when(") {
                        reactor.trigger = Some(content.to_string());
                        section = None;
                    } else if content.starts_with("emit ") {
                        reactor.emits.push(parse_emit_line(content, span.clone())?);
                        section = None;
                    } else {
                        section = None;
                    }
                } else if matches!(section, Some(Section::External)) {
                    apply_external_line(external, content)?;
                    section = Some(Section::External);
                } else if current_aggregate.is_some() {
                    if content == "invariant" {
                        section = Some(Section::Invariant);
                    } else {
                        section = match content {
                            "state" => Some(Section::State),
                            "validate" => Some(Section::Validate),
                            "apply" => Some(Section::Apply),
                            _ => section,
                        };
                    }
                } else if matches!(section, Some(Section::Flow)) {
                    flows.push(parse_flow_transition(content)?);
                } else {
                    section = match content {
                        "state" => Some(Section::State),
                        "validate" => Some(Section::Validate),
                        "apply" => Some(Section::Apply),
                        _ => section,
                    };
                }
            }
            2 => {
                if let Some(reactor) = current_reactor.as_mut() {
                    if content.starts_with("emit ") {
                        reactor.emits.push(parse_emit_line(content, span.clone())?);
                        continue;
                    }
                }
                match section {
                Some(Section::State) => {
                    let default_vis = current_aggregate
                        .as_ref()
                        .and_then(|agg| agg.extends.as_ref())
                        .map(|_| Visibility::Private)
                        .unwrap_or(Visibility::Public);
                    let field = parse_field_line(content, default_vis)?;
                    if let Some(agg) = current_aggregate.as_mut() {
                        agg.fields.push(field);
                    }
                }
                Some(Section::Validate) => {
                    let expr = strip_comment(content);
                    if expr.is_empty() {
                        continue;
                    }
                    if let Some(action) = current_action.as_mut() {
                        action
                            .validates
                            .push(Spanned::new(parse_expr(expr)?, span.clone()));
                    }
                }
                Some(Section::Apply) => {
                    let line = strip_comment(content);
                    if line.is_empty() {
                        continue;
                    }
                    if let Some(action) = current_action.as_mut() {
                        if let Some((target, value)) = line.split_once('=') {
                            let target_path = parse_path(target.trim())?;
                            let value_expr = parse_expr(value.trim())?;
                            action.applies.push(Spanned::new(
                                Assignment {
                                    target: target_path,
                                    value: value_expr,
                                },
                                span.clone(),
                            ));
                        } else if let Some(assign) = parse_apply_call(line)? {
                            action.applies.push(Spanned::new(assign, span.clone()));
                        }
                    }
                }
                Some(Section::ReactorValidate) => {
                    let expr = strip_comment(content);
                    if expr.is_empty() {
                        continue;
                    }
                    if let Some(reactor) = current_reactor.as_mut() {
                        let expr = strip_if(expr);
                        reactor
                            .validates
                            .push(Spanned::new(parse_expr(expr)?, span.clone()));
                    }
                }
                Some(Section::Invariant) => {
                    let expr = strip_comment(content);
                    if expr.is_empty() {
                        continue;
                    }
                    if let Some(agg) = current_aggregate.as_mut() {
                        agg.invariants.push(Spanned::new(parse_expr(expr)?, span.clone()));
                    }
                }
                Some(Section::Flow) => {}
                Some(Section::External) => {}
                Some(Section::View) => {}
                None => {}
            }
            },
            _ => {}
        }
    }

    if let Some(action) = current_action.take() {
        actions.push(action);
    }
    if let Some(agg) = current_aggregate.take() {
        aggregates.push(agg);
    }
    if let Some(reactor) = current_reactor.take() {
        reactors.push(reactor);
    }
    if let Some(view) = current_view.take() {
        views.push(view);
    }
    apply_flows(actions, &flows);
    if !apply_doc.is_empty() {
        apply_doc.clear();
    }
    Ok(())
}

fn parse_fence_start(line: &str) -> Option<(&'static str, &str)> {
    if let Some(rest) = line.strip_prefix("```") {
        return Some(("```", rest.trim()));
    }
    if let Some(rest) = line.strip_prefix("~~~") {
        return Some(("~~~", rest.trim()));
    }
    None
}

#[derive(Clone, Copy)]
enum Section {
    State,
    Validate,
    Apply,
    ReactorValidate,
    Flow,
    External,
    Invariant,
    View,
}

fn count_indent(line: &str) -> Result<usize, DharmaError> {
    let mut count = 0usize;
    for ch in line.chars() {
        if ch == ' ' {
            count += 1;
        } else {
            break;
        }
    }
    if count % 4 != 0 {
        return Err(DharmaError::Validation("invalid indentation".to_string()));
    }
    Ok(count)
}

fn parse_field_line(line: &str, default_vis: Visibility) -> Result<FieldDef, DharmaError> {
    let mut visibility = default_vis;
    let mut line = line.trim();
    if let Some(rest) = line.strip_prefix("public ") {
        visibility = Visibility::Public;
        line = rest.trim();
    } else if let Some(rest) = line.strip_prefix("private ") {
        visibility = Visibility::Private;
        line = rest.trim();
    }
    let (name, rest) = line
        .split_once(':')
        .ok_or_else(|| DharmaError::Validation("invalid field".to_string()))?;
    let rest = rest.trim();
    let (typ_str, default_str) = split_type_default(rest);
    let typ = parse_type_spec(typ_str)?;
    let default = match default_str {
        Some(value) => Some(parse_literal(value, &typ)?),
        None => None,
    };
    Ok(FieldDef {
        name: name.trim().to_string(),
        typ,
        default,
        visibility,
    })
}

fn split_type_default(rest: &str) -> (&str, Option<&str>) {
    let mut depth_paren = 0i32;
    let mut depth_angle = 0i32;
    let mut depth_bracket = 0i32;
    let mut depth_brace = 0i32;
    let mut in_str = false;
    for (idx, ch) in rest.char_indices() {
        match ch {
            '"' => in_str = !in_str,
            '(' if !in_str => depth_paren += 1,
            ')' if !in_str => depth_paren -= 1,
            '<' if !in_str => depth_angle += 1,
            '>' if !in_str => depth_angle -= 1,
            '[' if !in_str => depth_bracket += 1,
            ']' if !in_str => depth_bracket -= 1,
            '{' if !in_str => depth_brace += 1,
            '}' if !in_str => depth_brace -= 1,
            '=' if !in_str
                && depth_paren == 0
                && depth_angle == 0
                && depth_bracket == 0
                && depth_brace == 0 =>
            {
                let left = rest[..idx].trim();
                let right = rest[idx + 1..].trim();
                return (left, if right.is_empty() { None } else { Some(right) });
            }
            _ => {}
        }
    }
    (rest.trim(), None)
}

fn parse_action_header(line: &str) -> Result<(String, Vec<ArgDef>), DharmaError> {
    let line = line.trim();
    let (_, (name, args)) = action_parser(line).map_err(|_| DharmaError::Validation("invalid action".to_string()))?;
    Ok((name, args))
}

fn strip_comment(line: &str) -> &str {
    match line.find("//") {
        Some(idx) => line[..idx].trim(),
        None => line.trim(),
    }
}

fn parse_literal(value: &str, typ: &TypeSpec) -> Result<Literal, DharmaError> {
    match typ {
        TypeSpec::Optional(inner) => {
            if value.trim() == "null" {
                return Ok(Literal::Null);
            }
            return parse_literal(value, inner);
        }
        TypeSpec::Int | TypeSpec::Duration | TypeSpec::Timestamp => value
            .parse::<i64>()
            .map(Literal::Int)
            .map_err(|_| DharmaError::Validation("invalid int".to_string())),
        TypeSpec::Decimal(scale) => parse_decimal_literal(value, *scale).map(Literal::Int),
        TypeSpec::Ratio => {
            let (num, den) = parse_ratio_literal(value)?;
            Ok(Literal::Map(vec![
                (
                    Expr::Literal(Literal::Text("num".to_string())),
                    Expr::Literal(Literal::Int(num)),
                ),
                (
                    Expr::Literal(Literal::Text("den".to_string())),
                    Expr::Literal(Literal::Int(den)),
                ),
            ]))
        }
        TypeSpec::Bool => match value {
            "true" => Ok(Literal::Bool(true)),
            "false" => Ok(Literal::Bool(false)),
            _ => Err(DharmaError::Validation("invalid bool".to_string())),
        },
        TypeSpec::Text(_) => Ok(Literal::Text(value.trim_matches('"').to_string())),
        TypeSpec::Enum(_) => {
            let trimmed = value.trim();
            let name = trimmed.strip_prefix('\'').unwrap_or(trimmed);
            Ok(Literal::Enum(name.to_string()))
        }
        TypeSpec::Identity | TypeSpec::Ref(_) => Ok(Literal::Text(value.to_string())),
        TypeSpec::Currency => Ok(Literal::Text(value.to_string())),
        TypeSpec::GeoPoint => Err(DharmaError::Validation("geopoint literal unsupported".to_string())),
        TypeSpec::List(_) | TypeSpec::Map(_, _) => {
            let expr = parse_expr(value)?;
            let lit = expr_to_literal(&expr)?;
            match typ {
                TypeSpec::List(_) => match lit {
                    Literal::List(_) => Ok(lit),
                    _ => Err(DharmaError::Validation("invalid list literal".to_string())),
                },
                TypeSpec::Map(_, _) => match lit {
                    Literal::Map(_) => Ok(lit),
                    _ => Err(DharmaError::Validation("invalid map literal".to_string())),
                },
                _ => Ok(lit),
            }
        }
    }
}

fn parse_decimal_literal(value: &str, scale: Option<u32>) -> Result<i64, DharmaError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(DharmaError::Validation("invalid decimal".to_string()));
    }
    let negative = trimmed.starts_with('-');
    let unsigned = trimmed.strip_prefix('-').unwrap_or(trimmed);
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((left, right)) => (left, Some(right)),
        None => (unsigned, None),
    };
    let scale = scale.unwrap_or(0);
    if frac_part.is_some() && scale == 0 {
        return Err(DharmaError::Validation("decimal scale required".to_string()));
    }
    let int_str = if int_part.is_empty() { "0" } else { int_part };
    let int_val = int_str
        .parse::<i64>()
        .map_err(|_| DharmaError::Validation("invalid decimal".to_string()))?;
    let factor = pow10(scale)?;
    let mut mantissa = int_val
        .checked_mul(factor)
        .ok_or_else(|| DharmaError::Validation("decimal overflow".to_string()))?;
    if let Some(frac) = frac_part {
        if frac.len() > scale as usize {
            return Err(DharmaError::Validation("decimal scale overflow".to_string()));
        }
        let mut frac_buf = String::from(frac);
        while frac_buf.len() < scale as usize {
            frac_buf.push('0');
        }
        let frac_val = if frac_buf.is_empty() {
            0i64
        } else {
            frac_buf
                .parse::<i64>()
                .map_err(|_| DharmaError::Validation("invalid decimal".to_string()))?
        };
        mantissa = mantissa
            .checked_add(frac_val)
            .ok_or_else(|| DharmaError::Validation("decimal overflow".to_string()))?;
    }
    if negative {
        Ok(-mantissa)
    } else {
        Ok(mantissa)
    }
}

fn parse_ratio_literal(value: &str) -> Result<(i64, i64), DharmaError> {
    let trimmed = value.trim();
    let (num_raw, den_raw) = if let Some(pair) = trimmed.split_once('/') {
        pair
    } else if let Some(pair) = trimmed.split_once(',') {
        pair
    } else {
        return Err(DharmaError::Validation("invalid ratio".to_string()));
    };
    let num = num_raw
        .trim()
        .parse::<i64>()
        .map_err(|_| DharmaError::Validation("invalid ratio".to_string()))?;
    let den = den_raw
        .trim()
        .parse::<i64>()
        .map_err(|_| DharmaError::Validation("invalid ratio".to_string()))?;
    Ok((num, den))
}

fn pow10(scale: u32) -> Result<i64, DharmaError> {
    let mut out = 1i64;
    for _ in 0..scale {
        out = out
            .checked_mul(10)
            .ok_or_else(|| DharmaError::Validation("decimal overflow".to_string()))?;
    }
    Ok(out)
}

fn expr_to_literal(expr: &Expr) -> Result<Literal, DharmaError> {
    match expr {
        Expr::Literal(lit) => match lit {
            Literal::Null => Ok(Literal::Null),
            Literal::List(items) => {
                for item in items {
                    expr_to_literal(item)?;
                }
                Ok(lit.clone())
            }
            Literal::Map(items) => {
                for (k, v) in items {
                    expr_to_literal(k)?;
                    expr_to_literal(v)?;
                }
                Ok(lit.clone())
            }
            _ => Ok(lit.clone()),
        },
        Expr::UnaryOp(Op::Neg, inner) => match inner.as_ref() {
            Expr::Literal(Literal::Int(value)) => Ok(Literal::Int(-value)),
            _ => Err(DharmaError::Validation("invalid literal".to_string())),
        },
        Expr::UnaryOp(Op::Not, inner) => match inner.as_ref() {
            Expr::Literal(Literal::Bool(value)) => Ok(Literal::Bool(!value)),
            _ => Err(DharmaError::Validation("invalid literal".to_string())),
        },
        _ => Err(DharmaError::Validation("invalid literal".to_string())),
    }
}

fn parse_type_spec(input: &str) -> Result<TypeSpec, DharmaError> {
    let input = input.trim();
    if let Some(stripped) = input.strip_suffix('?') {
        let inner = parse_type_spec(stripped.trim())?;
        return Ok(TypeSpec::Optional(Box::new(inner)));
    }
    if let Some(inner) = input.strip_prefix("List<") {
        let inner = inner.strip_suffix('>').ok_or_else(|| DharmaError::Validation("invalid list type".to_string()))?;
        let inner = parse_type_spec(inner)?;
        return Ok(TypeSpec::List(Box::new(inner)));
    }
    if let Some(inner) = input.strip_prefix("Map<") {
        let inner = inner.strip_suffix('>').ok_or_else(|| DharmaError::Validation("invalid map type".to_string()))?;
        let parts = split_type_args(inner)?;
        if parts.len() != 2 {
            return Err(DharmaError::Validation("invalid map type".to_string()));
        }
        let key = parse_type_spec(&parts[0])?;
        let value = parse_type_spec(&parts[1])?;
        return Ok(TypeSpec::Map(Box::new(key), Box::new(value)));
    }
    let (_, typ) =
        type_parser(input).map_err(|_| DharmaError::Validation("invalid type".to_string()))?;
    Ok(typ)
}

fn parse_path(input: &str) -> Result<Vec<String>, DharmaError> {
    let parts: Vec<String> = input
        .split('.')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    if parts.is_empty() {
        return Err(DharmaError::Validation("invalid assignment target".to_string()));
    }
    Ok(parts)
}

fn strip_if(expr: &str) -> &str {
    if let Some(rest) = expr.trim_start().strip_prefix("if ") {
        rest.trim()
    } else {
        expr.trim()
    }
}

fn parse_flow_transition(line: &str) -> Result<FlowTransition, DharmaError> {
    let parts: Vec<&str> = line.split("->").map(|p| p.trim()).collect();
    if parts.len() != 3 {
        return Err(DharmaError::Validation("invalid flow transition".to_string()));
    }
    let from = strip_flow_token(parts[0])?;
    let action = strip_flow_action(parts[1])?;
    let to = strip_flow_token(parts[2])?;
    Ok(FlowTransition { action, from, to })
}

fn strip_flow_token(token: &str) -> Result<String, DharmaError> {
    let trimmed = token.trim().trim_matches('\'').trim();
    if trimmed.is_empty() {
        return Err(DharmaError::Validation("invalid flow state".to_string()));
    }
    Ok(trimmed.to_string())
}

fn strip_flow_action(token: &str) -> Result<String, DharmaError> {
    let trimmed = token
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim();
    if trimmed.is_empty() {
        return Err(DharmaError::Validation("invalid flow action".to_string()));
    }
    Ok(trimmed.to_string())
}

fn apply_flows(actions: &mut Vec<ActionDef>, flows: &[FlowTransition]) {
    for flow in flows {
        let action = match actions.iter_mut().find(|a| a.name == flow.action) {
            Some(action) => action,
            None => {
                actions.push(ActionDef {
                    name: flow.action.clone(),
                    args: Vec::new(),
                    validates: Vec::new(),
                    applies: Vec::new(),
                    doc: None,
                });
                actions.last_mut().unwrap()
            }
        };
        action.validates.push(Spanned::new(
            Expr::BinaryOp(
                Op::Eq,
                Box::new(Expr::Path(vec!["state".to_string(), "status".to_string()])),
                Box::new(Expr::Literal(Literal::Enum(flow.from.clone()))),
            ),
            SourceSpan::default(),
        ));
        action.applies.push(Spanned::new(
            Assignment {
                target: vec!["state".to_string(), "status".to_string()],
                value: Expr::Literal(Literal::Enum(flow.to.clone())),
            },
            SourceSpan::default(),
        ));
    }
}

fn parse_emit_line(line: &str, span: SourceSpan) -> Result<EmitDef, DharmaError> {
    let rest = line
        .trim()
        .strip_prefix("emit ")
        .ok_or_else(|| DharmaError::Validation("invalid emit".to_string()))?;
    let rest = rest.trim();
    if let Some(idx) = rest.find('(') {
        let action = rest[..idx].trim();
        let args_str = rest[idx + 1..].trim_end_matches(')').trim();
        let mut args = Vec::new();
        if !args_str.is_empty() {
            for part in split_emit_args(args_str)? {
                let (key, value) = part
                    .split_once('=')
                    .ok_or_else(|| DharmaError::Validation("invalid emit arg".to_string()))?;
                let expr = parse_expr(value.trim())?;
                args.push((key.trim().to_string(), Spanned::new(expr, span.clone())));
            }
        }
        Ok(EmitDef {
            action: action.to_string(),
            args,
        })
    } else {
        Ok(EmitDef {
            action: rest.to_string(),
            args: Vec::new(),
        })
    }
}

fn split_emit_args(input: &str) -> Result<Vec<String>, DharmaError> {
    let mut parts = Vec::new();
    let mut buf = String::new();
    let mut depth = 0i32;
    let mut in_str = false;
    for ch in input.chars() {
        match ch {
            '"' => {
                in_str = !in_str;
                buf.push(ch);
            }
            '(' if !in_str => {
                depth += 1;
                buf.push(ch);
            }
            ')' if !in_str => {
                depth -= 1;
                buf.push(ch);
            }
            ',' if depth == 0 && !in_str => {
                if !buf.trim().is_empty() {
                    parts.push(buf.trim().to_string());
                }
                buf.clear();
            }
            _ => buf.push(ch),
        }
    }
    if !buf.trim().is_empty() {
        parts.push(buf.trim().to_string());
    }
    if depth != 0 {
        return Err(DharmaError::Validation("invalid emit args".to_string()));
    }
    Ok(parts)
}

fn parse_apply_call(line: &str) -> Result<Option<Assignment>, DharmaError> {
    let open = match line.find('(') {
        Some(idx) => idx,
        None => return Ok(None),
    };
    if !line.ends_with(')') {
        return Ok(None);
    }
    let head = line[..open].trim();
    let args_str = &line[open + 1..line.len() - 1];
    let Some((target, method)) = head.rsplit_once('.') else {
        return Ok(None);
    };
    let method = method.trim();
    if method != "push" && method != "remove" && method != "set" {
        return Ok(None);
    }
    let target_path = parse_path(target.trim())?;
    let parts = split_emit_args(args_str)?;
    let exprs = parts
        .into_iter()
        .map(|part| parse_expr(part.trim()))
        .collect::<Result<Vec<_>, _>>()?;
    let call_args = match method {
        "push" | "remove" => {
            if exprs.len() != 1 {
                return Err(DharmaError::Validation("invalid apply call".to_string()));
            }
            vec![Expr::Path(target_path.clone()), exprs[0].clone()]
        }
        "set" => {
            if exprs.len() != 2 {
                return Err(DharmaError::Validation("invalid apply call".to_string()));
            }
            vec![
                Expr::Path(target_path.clone()),
                exprs[0].clone(),
                exprs[1].clone(),
            ]
        }
        _ => return Ok(None),
    };
    Ok(Some(Assignment {
        target: target_path,
        value: Expr::Call(method.to_string(), call_args),
    }))
}

struct FlowTransition {
    action: String,
    from: String,
    to: String,
}

fn split_type_args(input: &str) -> Result<Vec<String>, DharmaError> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut buf = String::new();
    for ch in input.chars() {
        match ch {
            '<' => {
                depth += 1;
                buf.push(ch);
            }
            '>' => {
                depth -= 1;
                buf.push(ch);
            }
            ',' if depth == 0 => {
                parts.push(buf.trim().to_string());
                buf.clear();
            }
            _ => buf.push(ch),
        }
    }
    if !buf.trim().is_empty() {
        parts.push(buf.trim().to_string());
    }
    if depth != 0 {
        return Err(DharmaError::Validation("invalid generic type".to_string()));
    }
    Ok(parts)
}

fn apply_external_line(external: &mut Option<ExternalDef>, line: &str) -> Result<(), DharmaError> {
    let Some((key, rest)) = line.split_once(':') else {
        return Err(DharmaError::Validation("invalid external line".to_string()));
    };
    let items = parse_bracket_list(rest.trim())?;
    let ext = external
        .as_mut()
        .ok_or_else(|| DharmaError::Validation("external block missing".to_string()))?;
    match key.trim() {
        "roles" => ext.roles = items,
        "time" => ext.time = items,
        "datasets" => ext.datasets = items,
        _ => {}
    }
    Ok(())
}

fn parse_bracket_list(input: &str) -> Result<Vec<String>, DharmaError> {
    let trimmed = input.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|v| v.strip_suffix(']'))
        .ok_or_else(|| DharmaError::Validation("invalid list".to_string()))?;
    let items = inner
        .split(',')
        .map(|s| s.trim().trim_matches('"'))
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    Ok(items)
}

fn action_parser(input: &str) -> IResult<&str, (String, Vec<ArgDef>)> {
    let (input, _) = tuple((tag("action"), space0))(input)?;
    let (input, name) = identifier(input)?;
    let (input, args) = opt(delimited(
        char('('),
        separated_list1(char(','), delimited(space0, arg_parser, space0)),
        char(')'),
    ))(input)?;
    Ok((input, (name.to_string(), args.unwrap_or_default())))
}

fn arg_parser(input: &str) -> IResult<&str, ArgDef> {
    let (input, name) = identifier(input)?;
    let (input, _) = delimited(space0, char(':'), space0)(input)?;
    let (input, typ_str) = take_while1(|c: char| c != ',' && c != ')')(input)?;
    let typ = parse_type_spec(typ_str.trim()).map_err(|_| nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Fail)))?;
    Ok((input, ArgDef { name: name.to_string(), typ }))
}

fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '.')(input)
}

fn type_parser(input: &str) -> IResult<&str, TypeSpec> {
    let int_t = map(tag("Int"), |_| TypeSpec::Int);
    let decimal_t = map(
        tuple((
            tag("Decimal"),
            opt(delimited(
                char('('),
                preceded(
                    tuple((space0, tag("scale"), space0, char('='), space0)),
                    map_res(digit1, |d: &str| d.parse::<u32>()),
                ),
                char(')'),
            )),
        )),
        |(_, scale)| TypeSpec::Decimal(scale),
    );
    let ratio_t = map(tag("Ratio"), |_| TypeSpec::Ratio);
    let duration_t = map(tag("Duration"), |_| TypeSpec::Duration);
    let timestamp_t = map(tag("Timestamp"), |_| TypeSpec::Timestamp);
    let currency_t = map(tag("Currency"), |_| TypeSpec::Currency);
    let bool_t = map(tag("Bool"), |_| TypeSpec::Bool);
    let identity_t = map(tag("Identity"), |_| TypeSpec::Identity);
    let geo_t = map(tag("GeoPoint"), |_| TypeSpec::GeoPoint);
    let text_t = map(
        tuple((
            tag("Text"),
            opt(delimited(
                char('('),
                preceded(
                    tuple((space0, tag("len"), space0, char('='), space0)),
                    map_res(digit1, |d: &str| d.parse::<usize>()),
                ),
                char(')'),
            )),
        )),
        |(_, len)| TypeSpec::Text(len),
    );
    let enum_t = map(
        delimited(
            tag("Enum"),
            delimited(
                char('('),
                separated_list1(
                    char(','),
                    map(delimited(space0, identifier, space0), |s: &str| s.to_string()),
                ),
                char(')'),
            ),
            multispace0,
        ),
        TypeSpec::Enum,
    );
    let ref_t = map(
        delimited(tag("Ref<"), terminated(identifier, char('>')), multispace0),
        |name: &str| TypeSpec::Ref(name.to_string()),
    );
    alt((
        int_t,
        decimal_t,
        ratio_t,
        duration_t,
        timestamp_t,
        currency_t,
        bool_t,
        identity_t,
        geo_t,
        text_t,
        enum_t,
        ref_t,
    ))(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dhl_sample() {
        let doc = r#"---
namespace: com.ph.cmdv.logistics
version: 1.0.0
import:
  - com.ph.cmdv.sales.Order
---

# Logistics Protocol V1

```dhl
aggregate Shipment

    state
        status: Enum(Pending, Shipped, Delivered) = Pending
        location: Text(len=64)
        courier: Identity

action Dispatch(current_loc: Text)

    validate
        state.status == Pending
        context.signer == state.courier
        current_loc.len > 0

    apply
        state.status = Shipped
        state.location = current_loc
```
"#;

        let ast = parse(doc).unwrap();
        assert_eq!(ast.header.namespace, "com.ph.cmdv.logistics");
        assert_eq!(ast.header.version, "1.0.0");
        assert_eq!(ast.header.imports.len(), 1);
        assert_eq!(ast.aggregates.len(), 1);
        assert_eq!(ast.actions.len(), 1);
        let agg = &ast.aggregates[0];
        assert_eq!(agg.name, "Shipment");
        assert_eq!(agg.fields.len(), 3);
        assert!(matches!(agg.fields[0].visibility, Visibility::Public));
        assert_eq!(agg.fields[1].typ, TypeSpec::Text(Some(64)));
        let action = &ast.actions[0];
        assert_eq!(action.name, "Dispatch");
        assert_eq!(action.args.len(), 1);
    }

    #[test]
    fn parse_visibility_and_extends() {
        let doc = r#"```dhl
aggregate CompanyInvoice extends std.finance.Invoice
    state
        public amount: Int
        internal_code: Text
```"#;
        let ast = parse(doc).unwrap();
        let agg = &ast.aggregates[0];
        assert_eq!(agg.extends.as_deref(), Some("std.finance.Invoice"));
        assert!(matches!(agg.fields[0].visibility, Visibility::Public));
        assert!(matches!(agg.fields[1].visibility, Visibility::Private));
    }

    #[test]
    fn parse_type_text_len() {
        let typ = parse_type_spec("Text(len=64)").unwrap();
        assert_eq!(typ, TypeSpec::Text(Some(64)));
    }

    #[test]
    fn parse_decimal_ratio_types() {
        let typ = parse_type_spec("Decimal(scale=2)").unwrap();
        assert_eq!(typ, TypeSpec::Decimal(Some(2)));
        let typ = parse_type_spec("Decimal").unwrap();
        assert_eq!(typ, TypeSpec::Decimal(None));
        let typ = parse_type_spec("Ratio").unwrap();
        assert_eq!(typ, TypeSpec::Ratio);
    }

    #[test]
    fn parse_decimal_ratio_literals() {
        let lit = parse_literal("12.34", &TypeSpec::Decimal(Some(2))).unwrap();
        assert_eq!(lit, Literal::Int(1234));
        let lit = parse_literal("3/5", &TypeSpec::Ratio).unwrap();
        match lit {
            Literal::Map(entries) => {
                assert_eq!(entries.len(), 2);
            }
            _ => panic!("expected ratio map literal"),
        }
    }

    #[test]
    fn parse_list_map_types() {
        let list = parse_type_spec("List<Text>").unwrap();
        assert_eq!(list, TypeSpec::List(Box::new(TypeSpec::Text(None))));
        let list_len = parse_type_spec("List<Text(len=4)>").unwrap();
        assert_eq!(list_len, TypeSpec::List(Box::new(TypeSpec::Text(Some(4)))));
        let map = parse_type_spec("Map<Text, Int>").unwrap();
        assert_eq!(
            map,
            TypeSpec::Map(Box::new(TypeSpec::Text(None)), Box::new(TypeSpec::Int))
        );
    }

    #[test]
    fn parse_optional_type() {
        let typ = parse_type_spec("Timestamp?").unwrap();
        assert_eq!(
            typ,
            TypeSpec::Optional(Box::new(TypeSpec::Timestamp))
        );
    }

    #[test]
    fn parse_flow_block_desugars_actions() {
        let doc = r#"```dhl
aggregate Task
    state
        status: Enum(Draft, Sent)

flow Lifecycle
    'Draft -> [Send] -> 'Sent
```"#;
        let ast = parse(doc).unwrap();
        let action = ast.actions.iter().find(|a| a.name == "Send").unwrap();
        assert_eq!(action.validates.len(), 1);
        assert_eq!(action.applies.len(), 1);
    }

    #[test]
    fn parse_reactor_block() {
        let doc = r#"```dhl
reactor AutoApprover
    trigger: action.Approve
    scope: std.invoice
    validate
        if amount > 0
    emit action.Invoice.Release(amount = amount)
```"#;
        let ast = parse(doc).unwrap();
        assert_eq!(ast.reactors.len(), 1);
        let reactor = &ast.reactors[0];
        assert_eq!(reactor.name, "AutoApprover");
        assert_eq!(reactor.emits.len(), 1);
    }

    #[test]
    fn parse_enum_default_with_quote() {
        let doc = r#"```dhl
aggregate Invoice
    state
        status: Enum(Draft, Sent) = 'Draft
```"#;
        let ast = parse(doc).unwrap();
        let status = &ast.aggregates[0].fields[0];
        assert_eq!(status.default, Some(Literal::Enum("Draft".to_string())));
    }

    #[test]
    fn parse_package_external_view_invariant() {
        let doc = r#"```dhl
package std.finance
external
    roles: [finance.approver, finance.viewer]
    time: [block_time]

aggregate Invoice
    state
        status: Enum(Draft, Paid) = 'Draft
    invariant
        state.status != 'Paid

view InvoiceDetail
    layout Column
    card { text(state.status) }
```"#;
        let ast = parse(doc).unwrap();
        assert_eq!(ast.package.as_deref(), Some("std.finance"));
        let ext = ast.external.as_ref().unwrap();
        assert_eq!(ext.roles.len(), 2);
        assert_eq!(ast.views.len(), 1);
        let agg = &ast.aggregates[0];
        assert_eq!(agg.invariants.len(), 1);
    }
}
