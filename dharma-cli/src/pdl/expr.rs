use crate::error::DharmaError;
use crate::pdl::ast::{Expr, Literal, Op};

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Ident(String),
    Int(i64),
    Bool(bool),
    Str(String),
    Null,
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Colon,
    Comma,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Implies,
    Bang,
    EqEq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
    In,
}

pub fn parse_expr(input: &str) -> Result<Expr, DharmaError> {
    let tokens = tokenize(input)?;
    let mut parser = Parser { tokens, pos: 0 };
    let expr = parser.parse_implies()?;
    if parser.pos != parser.tokens.len() {
        return Err(DharmaError::Validation("invalid expression".to_string()));
    }
    Ok(expr)
}

fn tokenize(input: &str) -> Result<Vec<Token>, DharmaError> {
    let mut out = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        match ch {
            '\'' => {
                chars.next();
                let mut ident = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' || c == '.' {
                        ident.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if ident.is_empty() {
                    return Err(DharmaError::Validation("invalid token".to_string()));
                }
                out.push(Token::Ident(ident));
            }
            '(' => {
                chars.next();
                out.push(Token::LParen);
            }
            ')' => {
                chars.next();
                out.push(Token::RParen);
            }
            '[' => {
                chars.next();
                out.push(Token::LBracket);
            }
            ']' => {
                chars.next();
                out.push(Token::RBracket);
            }
            '{' => {
                chars.next();
                out.push(Token::LBrace);
            }
            '}' => {
                chars.next();
                out.push(Token::RBrace);
            }
            ':' => {
                chars.next();
                out.push(Token::Colon);
            }
            ',' => {
                chars.next();
                out.push(Token::Comma);
            }
            '+' => {
                chars.next();
                out.push(Token::Plus);
            }
            '-' => {
                chars.next();
                if chars.peek() == Some(&'>') {
                    chars.next();
                    out.push(Token::Implies);
                } else {
                    out.push(Token::Minus);
                }
            }
            '*' => {
                chars.next();
                out.push(Token::Star);
            }
            '/' => {
                chars.next();
                out.push(Token::Slash);
            }
            '%' => {
                chars.next();
                out.push(Token::Percent);
            }
            '!' => {
                chars.next();
                if chars.peek() == Some(&'=') {
                    chars.next();
                    out.push(Token::Neq);
                } else {
                    out.push(Token::Bang);
                }
            }
            '=' => {
                chars.next();
                if chars.peek() == Some(&'=') {
                    chars.next();
                    out.push(Token::EqEq);
                } else {
                    return Err(DharmaError::Validation("invalid token".to_string()));
                }
            }
            '>' => {
                chars.next();
                if chars.peek() == Some(&'=') {
                    chars.next();
                    out.push(Token::Gte);
                } else {
                    out.push(Token::Gt);
                }
            }
            '<' => {
                chars.next();
                if chars.peek() == Some(&'=') {
                    chars.next();
                    out.push(Token::Lte);
                } else {
                    out.push(Token::Lt);
                }
            }
            '&' => {
                chars.next();
                if chars.peek() == Some(&'&') {
                    chars.next();
                    out.push(Token::And);
                } else {
                    return Err(DharmaError::Validation("invalid token".to_string()));
                }
            }
            '|' => {
                chars.next();
                if chars.peek() == Some(&'|') {
                    chars.next();
                    out.push(Token::Or);
                } else {
                    return Err(DharmaError::Validation("invalid token".to_string()));
                }
            }
            '"' => {
                chars.next();
                let mut value = String::new();
                while let Some(c) = chars.next() {
                    if c == '"' {
                        break;
                    }
                    value.push(c);
                }
                out.push(Token::Str(value));
            }
            '0'..='9' => {
                let mut num = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_ascii_digit() {
                        num.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let value = num
                    .parse::<i64>()
                    .map_err(|_| DharmaError::Validation("invalid number".to_string()))?;
                out.push(Token::Int(value));
            }
            _ => {
                let mut ident = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' || c == '.' {
                        ident.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                match ident.as_str() {
                    "true" => out.push(Token::Bool(true)),
                    "false" => out.push(Token::Bool(false)),
                    "and" => out.push(Token::And),
                    "or" => out.push(Token::Or),
                    "not" => out.push(Token::Bang),
                    "in" => out.push(Token::In),
                    "null" => out.push(Token::Null),
                    _ if !ident.is_empty() => out.push(Token::Ident(ident)),
                    _ => return Err(DharmaError::Validation("invalid token".to_string())),
                }
            }
        }
    }
    Ok(out)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn parse_implies(&mut self) -> Result<Expr, DharmaError> {
        let mut expr = self.parse_or()?;
        while let Some(Token::Implies) = self.peek() {
            self.pos += 1;
            let right = self.parse_or()?;
            let not_left = Expr::UnaryOp(Op::Not, Box::new(expr));
            expr = Expr::BinaryOp(Op::Or, Box::new(not_left), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_or(&mut self) -> Result<Expr, DharmaError> {
        let mut expr = self.parse_and()?;
        loop {
            let op = match self.peek() {
                Some(Token::Or) => Op::Or,
                _ => break,
            };
            self.pos += 1;
            let right = self.parse_and()?;
            expr = Expr::BinaryOp(op, Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Expr, DharmaError> {
        let mut expr = self.parse_equality()?;
        loop {
            let op = match self.peek() {
                Some(Token::And) => Op::And,
                _ => break,
            };
            self.pos += 1;
            let right = self.parse_equality()?;
            expr = Expr::BinaryOp(op, Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_equality(&mut self) -> Result<Expr, DharmaError> {
        let mut expr = self.parse_relational()?;
        loop {
            let op = match self.peek() {
                Some(Token::EqEq) => Op::Eq,
                Some(Token::Neq) => Op::Neq,
                Some(Token::In) => Op::In,
                _ => break,
            };
            self.pos += 1;
            let right = self.parse_relational()?;
            expr = Expr::BinaryOp(op, Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_relational(&mut self) -> Result<Expr, DharmaError> {
        let mut expr = self.parse_add()?;
        loop {
            let op = match self.peek() {
                Some(Token::Gt) => Op::Gt,
                Some(Token::Lt) => Op::Lt,
                Some(Token::Gte) => Op::Gte,
                Some(Token::Lte) => Op::Lte,
                _ => break,
            };
            self.pos += 1;
            let right = self.parse_add()?;
            expr = Expr::BinaryOp(op, Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_add(&mut self) -> Result<Expr, DharmaError> {
        let mut expr = self.parse_mul()?;
        loop {
            let op = match self.peek() {
                Some(Token::Plus) => Op::Add,
                Some(Token::Minus) => Op::Sub,
                _ => break,
            };
            self.pos += 1;
            let right = self.parse_mul()?;
            expr = Expr::BinaryOp(op, Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_mul(&mut self) -> Result<Expr, DharmaError> {
        let mut expr = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Some(Token::Star) => Op::Mul,
                Some(Token::Slash) => Op::Div,
                Some(Token::Percent) => Op::Mod,
                _ => break,
            };
            self.pos += 1;
            let right = self.parse_unary()?;
            expr = Expr::BinaryOp(op, Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<Expr, DharmaError> {
        if let Some(Token::Bang) = self.peek() {
            self.pos += 1;
            let expr = self.parse_unary()?;
            return Ok(Expr::UnaryOp(Op::Not, Box::new(expr)));
        }
        if let Some(Token::Minus) = self.peek() {
            self.pos += 1;
            let expr = self.parse_unary()?;
            return Ok(Expr::UnaryOp(Op::Neg, Box::new(expr)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, DharmaError> {
        let base = match self.next() {
            Some(Token::Int(value)) => Ok(Expr::Literal(Literal::Int(value))),
            Some(Token::Bool(value)) => Ok(Expr::Literal(Literal::Bool(value))),
            Some(Token::Str(value)) => Ok(Expr::Literal(Literal::Text(value))),
            Some(Token::Null) => Ok(Expr::Literal(Literal::Null)),
            Some(Token::Ident(value)) => {
                if self.peek_is_lbrace() {
                    self.parse_struct_literal(value)
                } else if self.peek_is_lparen() {
                    self.parse_call(value)
                } else if value.starts_with('.') {
                    Err(DharmaError::Validation("invalid path".to_string()))
                } else if let Some(stripped) = value.strip_suffix(".len") {
                    Ok(Expr::Call(
                        "len".to_string(),
                        vec![Expr::Path(split_path(stripped)?)],
                    ))
                } else if value
                    .chars()
                    .next()
                    .map(|c| c.is_uppercase())
                    .unwrap_or(false)
                {
                    Ok(Expr::Literal(Literal::Enum(value)))
                } else {
                    Ok(Expr::Path(split_path(&value)?))
                }
            }
            Some(Token::LParen) => {
                let expr = self.parse_or()?;
                match self.next() {
                    Some(Token::RParen) => Ok(expr),
                    _ => Err(DharmaError::Validation("expected ')'".to_string())),
                }
            }
            Some(Token::LBracket) => self.parse_list_literal(),
            Some(Token::LBrace) => self.parse_map_literal(),
            _ => Err(DharmaError::Validation("unexpected token".to_string())),
        }?;
        self.parse_postfix(base)
    }

    fn parse_struct_literal(&mut self, name: String) -> Result<Expr, DharmaError> {
        self.expect(Token::LBrace)?;
        let mut items = Vec::new();
        if !self.peek_is_rbrace() {
            loop {
                let field_name = match self.next() {
                    Some(Token::Ident(value)) => value,
                    Some(Token::Str(value)) => value,
                    _ => {
                        return Err(DharmaError::Validation(
                            "struct field name must be identifier or text".to_string(),
                        ))
                    }
                };
                self.expect(Token::Colon)?;
                let value = self.parse_or()?;
                items.push((field_name, value));
                if self.peek_is_rbrace() {
                    break;
                }
                self.expect(Token::Comma)?;
            }
        }
        self.expect(Token::RBrace)?;
        Ok(Expr::Literal(Literal::Struct(name, items)))
    }

    fn parse_postfix(&mut self, mut expr: Expr) -> Result<Expr, DharmaError> {
        loop {
            match self.peek() {
                Some(Token::LBracket) => {
                    self.pos += 1;
                    let index = self.parse_or()?;
                    self.expect(Token::RBracket)?;
                    expr = Expr::Call("index".to_string(), vec![expr, index]);
                }
                Some(Token::Ident(name)) if name.starts_with('.') => {
                    let name = name.trim_start_matches('.').to_string();
                    self.pos += 1;
                    if name.is_empty() {
                        return Err(DharmaError::Validation("invalid path".to_string()));
                    }
                    expr = match expr {
                        Expr::Path(mut parts) => {
                            parts.push(name);
                            Expr::Path(parts)
                        }
                        _ => Expr::Call(
                            "get".to_string(),
                            vec![expr, Expr::Literal(Literal::Text(name))],
                        ),
                    };
                }
                _ => break,
            }
        }
        Ok(expr)
    }

    fn parse_list_literal(&mut self) -> Result<Expr, DharmaError> {
        let mut items = Vec::new();
        if !self.peek_is_rbracket() {
            loop {
                let expr = self.parse_or()?;
                items.push(expr);
                if self.peek_is_rbracket() {
                    break;
                }
                self.expect(Token::Comma)?;
            }
        }
        self.expect(Token::RBracket)?;
        Ok(Expr::Literal(Literal::List(items)))
    }

    fn parse_map_literal(&mut self) -> Result<Expr, DharmaError> {
        let mut items = Vec::new();
        if !self.peek_is_rbrace() {
            loop {
                let key = self.parse_or()?;
                self.expect(Token::Colon)?;
                let value = self.parse_or()?;
                items.push((key, value));
                if self.peek_is_rbrace() {
                    break;
                }
                self.expect(Token::Comma)?;
            }
        }
        self.expect(Token::RBrace)?;
        Ok(Expr::Literal(Literal::Map(items)))
    }

    fn parse_call(&mut self, name: String) -> Result<Expr, DharmaError> {
        self.expect(Token::LParen)?;
        let mut args = Vec::new();
        if !self.peek_is_rparen() {
            loop {
                let expr = self.parse_or()?;
                args.push(expr);
                if self.peek_is_rparen() {
                    break;
                }
                self.expect(Token::Comma)?;
            }
        }
        self.expect(Token::RParen)?;
        if let Some(prefix) = name.strip_suffix(".has_role") {
            let mut all_args = vec![Expr::Path(split_path(prefix)?)];
            all_args.extend(args);
            return Ok(Expr::Call("has_role".to_string(), all_args));
        }
        if let Some(prefix) = name.strip_suffix(".len") {
            let mut all_args = vec![Expr::Path(split_path(prefix)?)];
            all_args.extend(args);
            return Ok(Expr::Call("len".to_string(), all_args));
        }
        if let Some(prefix) = name.strip_suffix(".get") {
            let mut all_args = vec![Expr::Path(split_path(prefix)?)];
            all_args.extend(args);
            return Ok(Expr::Call("get".to_string(), all_args));
        }
        Ok(Expr::Call(name, args))
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let tok = self.tokens.get(self.pos).cloned();
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn expect(&mut self, token: Token) -> Result<(), DharmaError> {
        match self.next() {
            Some(t) if t == token => Ok(()),
            _ => Err(DharmaError::Validation("invalid expression".to_string())),
        }
    }

    fn peek_is_lparen(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(Token::LParen))
    }

    fn peek_is_lbrace(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(Token::LBrace))
    }

    fn peek_is_rparen(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(Token::RParen))
    }

    fn peek_is_rbracket(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(Token::RBracket))
    }

    fn peek_is_rbrace(&self) -> bool {
        matches!(self.tokens.get(self.pos), Some(Token::RBrace))
    }
}

fn split_path(input: &str) -> Result<Vec<String>, DharmaError> {
    let parts: Vec<String> = input
        .split('.')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    if parts.is_empty() {
        return Err(DharmaError::Validation("invalid path".to_string()));
    }
    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_len_expr() {
        let expr = parse_expr("dept.len == 3").unwrap();
        match expr {
            Expr::BinaryOp(Op::Eq, _, _) => {}
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_or_expr() {
        let expr = parse_expr("a == 1 or b == 2").unwrap();
        match expr {
            Expr::BinaryOp(Op::Or, _, _) => {}
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_call_expr() {
        let expr = parse_expr("len(name)").unwrap();
        match expr {
            Expr::Call(name, args) => {
                assert_eq!(name, "len");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_enum_with_quote_prefix() {
        let expr = parse_expr("state.status == 'Draft").unwrap();
        match expr {
            Expr::BinaryOp(Op::Eq, _, right) => match *right {
                Expr::Literal(Literal::Enum(ref name)) => assert_eq!(name, "Draft"),
                _ => panic!("unexpected expr"),
            },
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_not_keyword() {
        let expr = parse_expr("not active").unwrap();
        match expr {
            Expr::UnaryOp(Op::Not, _) => {}
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_mul_precedence() {
        let expr = parse_expr("1 + 2 * 3").unwrap();
        match expr {
            Expr::BinaryOp(Op::Add, _, right) => match *right {
                Expr::BinaryOp(Op::Mul, _, _) => {}
                _ => panic!("expected mul on right"),
            },
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_list_literal() {
        let expr = parse_expr("[1, 2, 3]").unwrap();
        match expr {
            Expr::Literal(Literal::List(items)) => assert_eq!(items.len(), 3),
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_map_literal() {
        let expr = parse_expr("{\"a\": 1, \"b\": 2}").unwrap();
        match expr {
            Expr::Literal(Literal::Map(items)) => assert_eq!(items.len(), 2),
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_in_operator() {
        let expr = parse_expr("status in ['Open, 'Closed]").unwrap();
        match expr {
            Expr::BinaryOp(Op::In, _, _) => {}
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_null_literal() {
        let expr = parse_expr("null").unwrap();
        match expr {
            Expr::Literal(Literal::Null) => {}
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_implies_desugars() {
        let expr = parse_expr("a -> b").unwrap();
        match expr {
            Expr::BinaryOp(Op::Or, left, _) => match *left {
                Expr::UnaryOp(Op::Not, _) => {}
                _ => panic!("expected not on left"),
            },
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_index_postfix() {
        let expr = parse_expr("state.lines[0]").unwrap();
        match expr {
            Expr::Call(name, args) => {
                assert_eq!(name, "index");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_index_then_field() {
        let expr = parse_expr("state.lines[0].amount").unwrap();
        match expr {
            Expr::Call(name, _) => assert_eq!(name, "get"),
            _ => panic!("unexpected expr"),
        }
    }

    #[test]
    fn parse_get_method_call() {
        let expr = parse_expr("state.map.get(key)").unwrap();
        match expr {
            Expr::Call(name, args) => {
                assert_eq!(name, "get");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("unexpected expr"),
        }
    }
}
