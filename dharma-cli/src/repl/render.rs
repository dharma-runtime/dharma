use crate::types::hex_encode;
use ciborium::value::Value;
use crossterm::style::{Color, Stylize};
use std::collections::BTreeMap;

pub fn value_to_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Integer(i) => i128::from(*i).to_string(),
        Value::Float(f) => {
            if f.is_finite() {
                f.to_string()
            } else {
                "null".to_string()
            }
        }
        Value::Bytes(bytes) => format!("\"0x{}\"", hex_string(bytes)),
        Value::Text(text) => format!("\"{}\"", json_escape(text)),
        Value::Array(items) => {
            let rendered = items
                .iter()
                .map(value_to_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{rendered}]")
        }
        Value::Map(entries) => {
            let map = map_as_text_keyed(entries);
            let rendered = map
                .into_iter()
                .map(|(k, v)| format!("\"{}\":{}", json_escape(&k), value_to_json(&v)))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{rendered}}}")
        }
        Value::Tag(_, inner) => value_to_json(inner),
        _ => "null".to_string(),
    }
}

pub fn render_json(value: &Value, color: bool) -> String {
    let raw = value_to_json(value);
    if !color {
        return raw;
    }
    highlight_json(&raw)
}

pub fn value_to_text(value: &Value) -> Vec<String> {
    match value {
        Value::Map(entries) => {
            let map = map_as_text_keyed(entries);
            map.into_iter()
                .map(|(k, v)| format!("{k}: {}", value_to_json(&v)))
                .collect()
        }
        _ => vec![value_to_json(value)],
    }
}

fn highlight_json(raw: &str) -> String {
    let chars: Vec<char> = raw.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        if ch == '"' {
            let start = i;
            i += 1;
            while i < chars.len() {
                let current = chars[i];
                if current == '\\' {
                    i = (i + 2).min(chars.len());
                    continue;
                }
                if current == '"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            let token: String = chars[start..i].iter().collect();
            out.push_str(&paint(&token, Color::Green));
            continue;
        }
        if ch.is_ascii_digit()
            || (ch == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit())
        {
            let start = i;
            i += 1;
            while i < chars.len() {
                let c = chars[i];
                if c.is_ascii_digit() || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-' {
                    i += 1;
                } else {
                    break;
                }
            }
            let token: String = chars[start..i].iter().collect();
            out.push_str(&paint(&token, Color::Cyan));
            continue;
        }
        if ch.is_ascii_alphabetic() {
            let start = i;
            i += 1;
            while i < chars.len() && chars[i].is_ascii_alphabetic() {
                i += 1;
            }
            let token: String = chars[start..i].iter().collect();
            if token == "true" || token == "false" || token == "null" {
                out.push_str(&paint(&token, Color::Yellow));
            } else {
                out.push_str(&token);
            }
            continue;
        }
        if matches!(ch, '{' | '}' | '[' | ']' | ':' | ',') {
            out.push_str(&paint(&ch.to_string(), Color::DarkGrey));
            i += 1;
            continue;
        }
        out.push(ch);
        i += 1;
    }
    out
}

fn paint(text: &str, color: Color) -> String {
    format!("{}", text.with(color))
}

fn map_as_text_keyed(entries: &[(Value, Value)]) -> BTreeMap<String, Value> {
    let mut map = BTreeMap::new();
    for (k, v) in entries {
        if let Value::Text(key) = k {
            map.insert(key.clone(), v.clone());
        }
    }
    map
}

fn json_escape(input: &str) -> String {
    let mut out = String::new();
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ if ch.is_control() => {
                out.push_str(&format!("\\u{:04x}", ch as u32));
            }
            _ => out.push(ch),
        }
    }
    out
}

fn hex_string(bytes: &[u8]) -> String {
    if bytes.len() == 32 {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(bytes);
        return hex_encode(arr);
    }
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(hex_char(b >> 4));
        out.push(hex_char(b & 0x0f));
    }
    out
}

fn hex_char(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + (n - 10)) as char,
        _ => '0',
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_to_json_serializes_map() {
        let value = Value::Map(vec![
            (Value::Text("a".to_string()), Value::Integer(1.into())),
            (Value::Text("b".to_string()), Value::Bool(true)),
        ]);
        let json = value_to_json(&value);
        assert!(json.contains("\"a\":1"));
        assert!(json.contains("\"b\":true"));
    }

    #[test]
    fn value_to_text_renders_lines() {
        let value = Value::Map(vec![(
            Value::Text("name".to_string()),
            Value::Text("alice".to_string()),
        )]);
        let lines = value_to_text(&value);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("name"));
    }

    #[test]
    fn render_json_respects_color_flag() {
        let value = Value::Map(vec![(
            Value::Text("a".to_string()),
            Value::Integer(1.into()),
        )]);
        let plain = render_json(&value, false);
        assert_eq!(plain, value_to_json(&value));
        let colored = render_json(&value, true);
        assert!(colored.contains("\u{1b}["));
    }
}
