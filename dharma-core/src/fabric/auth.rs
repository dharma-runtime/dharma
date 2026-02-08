use crate::cbor;
use crate::crypto;
use crate::error::DharmaError;
use crate::fabric::types::{OracleMode, OracleTiming};
use crate::types::{IdentityKey, SubjectId};
use crate::value::{expect_array, expect_text, expect_uint, map_get};
use ciborium::value::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct CapToken {
    pub v: u8,
    pub id: [u8; 32],
    pub issuer: IdentityKey,
    pub domain: String,
    pub level: String,
    pub subject: Option<SubjectId>,
    pub scopes: Vec<Scope>,
    pub ops: Vec<Op>,
    pub actions: Vec<String>,
    pub queries: Vec<String>,
    pub flags: Vec<Flag>,
    pub oracles: Vec<OracleClaim>,
    pub constraints: Vec<Constraint>,
    pub nbf: u64,
    pub exp: u64,
    pub sig: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
    Table(String),
    Namespace(String),
    Subject(SubjectId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Read,
    Write,
    Execute,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flag {
    AllowReplication,
    AllowCustomQuery,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleClaim {
    pub name: String,
    pub mode: OracleMode,
    pub timing: OracleTiming,
    pub domain: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Constraint {
    pub key: String,
    pub value: Value,
}

impl CapToken {
    pub fn signed_value(&self) -> Value {
        Value::Map(self.base_entries())
    }

    pub fn to_value(&self) -> Value {
        let mut entries = self.base_entries();
        entries.push((
            Value::Text("sig".to_string()),
            Value::Bytes(self.sig.clone()),
        ));
        Value::Map(entries)
    }

    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.to_value())
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        Self::from_value(&value)
    }

    pub fn verify(&self) -> Result<bool, DharmaError> {
        let payload = cbor::encode_canonical_value(&self.signed_value())?;
        crypto::verify(&self.issuer, &payload, &self.sig)
    }

    pub fn check_access(&self, op: Op, scope: &Scope) -> Result<(), DharmaError> {
        if !self.ops.contains(&op) {
            return Err(DharmaError::Validation("op not allowed".to_string()));
        }
        if !self.scopes.iter().any(|s| scope_matches(s, scope)) {
            return Err(DharmaError::Validation("scope not allowed".to_string()));
        }
        Ok(())
    }

    fn base_entries(&self) -> Vec<(Value, Value)> {
        let scopes = Value::Array(self.scopes.iter().map(|s| s.to_value()).collect());
        let ops = Value::Array(
            self.ops
                .iter()
                .map(|o| Value::Text(o.as_str().to_string()))
                .collect(),
        );
        let actions = Value::Array(
            self.actions
                .iter()
                .map(|a| Value::Text(a.clone()))
                .collect(),
        );
        let queries = Value::Array(
            self.queries
                .iter()
                .map(|q| Value::Text(q.clone()))
                .collect(),
        );
        let flags = Value::Array(
            self.flags
                .iter()
                .map(|f| Value::Text(f.as_str().to_string()))
                .collect(),
        );
        let oracles = Value::Array(self.oracles.iter().map(|o| o.to_value()).collect());
        let constraints = Value::Array(self.constraints.iter().map(|c| c.to_value()).collect());
        let mut entries = vec![
            (Value::Text("v".to_string()), Value::Integer(self.v.into())),
            (
                Value::Text("id".to_string()),
                Value::Bytes(self.id.to_vec()),
            ),
            (
                Value::Text("issuer".to_string()),
                Value::Bytes(self.issuer.as_bytes().to_vec()),
            ),
            (
                Value::Text("domain".to_string()),
                Value::Text(self.domain.clone()),
            ),
            (
                Value::Text("level".to_string()),
                Value::Text(self.level.clone()),
            ),
            (Value::Text("scopes".to_string()), scopes),
            (Value::Text("ops".to_string()), ops),
            (Value::Text("actions".to_string()), actions),
            (Value::Text("queries".to_string()), queries),
            (Value::Text("flags".to_string()), flags),
            (Value::Text("oracles".to_string()), oracles),
            (Value::Text("constraints".to_string()), constraints),
            (
                Value::Text("nbf".to_string()),
                Value::Integer(self.nbf.into()),
            ),
            (
                Value::Text("exp".to_string()),
                Value::Integer(self.exp.into()),
            ),
        ];
        if let Some(subject) = &self.subject {
            entries.push((
                Value::Text("subject".to_string()),
                Value::Bytes(subject.as_bytes().to_vec()),
            ));
        }
        entries
    }

    pub fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = crate::value::expect_map(value)?;
        let v = expect_uint(
            map_get(map, "v").ok_or_else(|| DharmaError::Validation("missing v".to_string()))?,
        )?;
        let v_u8: u8 = v
            .try_into()
            .map_err(|_| DharmaError::Validation("v out of range".to_string()))?;
        let id = parse_bytes32(
            map_get(map, "id").ok_or_else(|| DharmaError::Validation("missing id".to_string()))?,
        )?;
        let issuer_bytes = crate::value::expect_bytes(
            map_get(map, "issuer")
                .ok_or_else(|| DharmaError::Validation("missing issuer".to_string()))?,
        )?;
        let issuer = IdentityKey::from_slice(&issuer_bytes)?;
        let domain = expect_text(
            map_get(map, "domain")
                .ok_or_else(|| DharmaError::Validation("missing domain".to_string()))?,
        )?;
        let level = expect_text(
            map_get(map, "level")
                .ok_or_else(|| DharmaError::Validation("missing level".to_string()))?,
        )?;
        let subject = match map_get(map, "subject") {
            Some(val) => {
                let bytes = crate::value::expect_bytes(val)?;
                Some(SubjectId::from_slice(&bytes)?)
            }
            None => None,
        };
        let scopes_val = map_get(map, "scopes")
            .ok_or_else(|| DharmaError::Validation("missing scopes".to_string()))?;
        let scopes = parse_array(scopes_val, Scope::from_value)?;
        let ops_val = map_get(map, "ops")
            .ok_or_else(|| DharmaError::Validation("missing ops".to_string()))?;
        let ops = parse_array(ops_val, Op::from_value)?;
        let actions_val = map_get(map, "actions")
            .ok_or_else(|| DharmaError::Validation("missing actions".to_string()))?;
        let actions = parse_text_array(actions_val)?;
        let queries_val = map_get(map, "queries")
            .ok_or_else(|| DharmaError::Validation("missing queries".to_string()))?;
        let queries = parse_text_array(queries_val)?;
        let flags_val = map_get(map, "flags")
            .ok_or_else(|| DharmaError::Validation("missing flags".to_string()))?;
        let flags = parse_array(flags_val, Flag::from_value)?;
        let oracles_val = map_get(map, "oracles")
            .ok_or_else(|| DharmaError::Validation("missing oracles".to_string()))?;
        let oracles = parse_array(oracles_val, OracleClaim::from_value)?;
        let constraints_val = map_get(map, "constraints")
            .ok_or_else(|| DharmaError::Validation("missing constraints".to_string()))?;
        let constraints = parse_array(constraints_val, Constraint::from_value)?;
        let nbf = expect_uint(
            map_get(map, "nbf")
                .ok_or_else(|| DharmaError::Validation("missing nbf".to_string()))?,
        )?;
        let exp = expect_uint(
            map_get(map, "exp")
                .ok_or_else(|| DharmaError::Validation("missing exp".to_string()))?,
        )?;
        let sig = map_get(map, "sig")
            .map(crate::value::expect_bytes)
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            v: v_u8,
            id,
            issuer,
            domain,
            level,
            subject,
            scopes,
            ops,
            actions,
            queries,
            flags,
            oracles,
            constraints,
            nbf,
            exp,
            sig,
        })
    }
}

impl Scope {
    fn to_value(&self) -> Value {
        let (t, v) = match self {
            Scope::Table(table) => ("table", Value::Text(table.clone())),
            Scope::Namespace(ns) => ("namespace", Value::Text(ns.clone())),
            Scope::Subject(subject) => ("subject", Value::Bytes(subject.as_bytes().to_vec())),
        };
        Value::Map(vec![
            (Value::Text("t".to_string()), Value::Text(t.to_string())),
            (Value::Text("v".to_string()), v),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = crate::value::expect_map(value)?;
        let kind = expect_text(
            map_get(map, "t")
                .ok_or_else(|| DharmaError::Validation("missing scope type".to_string()))?,
        )?;
        let val = map_get(map, "v")
            .ok_or_else(|| DharmaError::Validation("missing scope value".to_string()))?;
        match kind.as_str() {
            "table" => Ok(Scope::Table(expect_text(val)?)),
            "namespace" => Ok(Scope::Namespace(expect_text(val)?)),
            "subject" => {
                let bytes = crate::value::expect_bytes(val)?;
                Ok(Scope::Subject(SubjectId::from_slice(&bytes)?))
            }
            _ => Err(DharmaError::Validation("invalid scope type".to_string())),
        }
    }
}

impl Op {
    fn as_str(&self) -> &'static str {
        match self {
            Op::Read => "read",
            Op::Write => "write",
            Op::Execute => "execute",
        }
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let text = expect_text(value)?;
        match text.as_str() {
            "read" => Ok(Op::Read),
            "write" => Ok(Op::Write),
            "execute" => Ok(Op::Execute),
            _ => Err(DharmaError::Validation("invalid op".to_string())),
        }
    }
}

impl Flag {
    fn as_str(&self) -> &'static str {
        match self {
            Flag::AllowReplication => "allow_replication",
            Flag::AllowCustomQuery => "allow_custom_query",
        }
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let text = expect_text(value)?;
        match text.as_str() {
            "allow_replication" => Ok(Flag::AllowReplication),
            "allow_custom_query" => Ok(Flag::AllowCustomQuery),
            _ => Err(DharmaError::Validation("invalid flag".to_string())),
        }
    }
}

impl OracleClaim {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (
                Value::Text("name".to_string()),
                Value::Text(self.name.clone()),
            ),
            (
                Value::Text("domain".to_string()),
                Value::Text(self.domain.clone()),
            ),
            (
                Value::Text("mode".to_string()),
                Value::Text(self.mode.as_str().to_string()),
            ),
            (
                Value::Text("timing".to_string()),
                Value::Text(self.timing.as_str().to_string()),
            ),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = crate::value::expect_map(value)?;
        let name = expect_text(
            map_get(map, "name")
                .ok_or_else(|| DharmaError::Validation("missing oracle name".to_string()))?,
        )?;
        let domain = expect_text(
            map_get(map, "domain")
                .ok_or_else(|| DharmaError::Validation("missing oracle domain".to_string()))?,
        )?;
        let mode_text = expect_text(
            map_get(map, "mode")
                .ok_or_else(|| DharmaError::Validation("missing oracle mode".to_string()))?,
        )?;
        let timing_text = expect_text(
            map_get(map, "timing")
                .ok_or_else(|| DharmaError::Validation("missing oracle timing".to_string()))?,
        )?;
        Ok(Self {
            name,
            domain,
            mode: OracleMode::from_str(&mode_text)?,
            timing: OracleTiming::from_str(&timing_text)?,
        })
    }
}

impl Constraint {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (Value::Text("k".to_string()), Value::Text(self.key.clone())),
            (Value::Text("v".to_string()), self.value.clone()),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = crate::value::expect_map(value)?;
        let key = expect_text(
            map_get(map, "k")
                .ok_or_else(|| DharmaError::Validation("missing constraint key".to_string()))?,
        )?;
        let value = map_get(map, "v")
            .ok_or_else(|| DharmaError::Validation("missing constraint value".to_string()))?;
        Ok(Self {
            key,
            value: value.clone(),
        })
    }
}

fn parse_array<T, F>(value: &Value, parse: F) -> Result<Vec<T>, DharmaError>
where
    F: Fn(&Value) -> Result<T, DharmaError>,
{
    let items = expect_array(value)?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(parse(item)?);
    }
    Ok(out)
}

fn parse_text_array(value: &Value) -> Result<Vec<String>, DharmaError> {
    let items = expect_array(value)?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(expect_text(item)?);
    }
    Ok(out)
}

fn parse_bytes32(value: &Value) -> Result<[u8; 32], DharmaError> {
    let bytes = crate::value::expect_bytes(value)?;
    if bytes.len() != 32 {
        return Err(DharmaError::Validation("expected 32 bytes".to_string()));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn scope_matches(token: &Scope, requested: &Scope) -> bool {
    match (token, requested) {
        (Scope::Table(a), Scope::Table(b)) => a == b,
        (Scope::Namespace(a), Scope::Namespace(b)) => namespace_matches(a, b),
        (Scope::Subject(a), Scope::Subject(b)) => a == b,
        _ => false,
    }
}

fn namespace_matches(pattern: &str, value: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix('*') {
        value.starts_with(prefix)
    } else {
        pattern == value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn cap_token_roundtrip_and_verify() {
        let mut rng = StdRng::seed_from_u64(12);
        let (sk, issuer) = crypto::generate_identity_keypair(&mut rng);
        let mut token = CapToken {
            v: 1,
            id: [1u8; 32],
            issuer,
            domain: "corp.example".to_string(),
            level: "admin".to_string(),
            subject: None,
            scopes: vec![Scope::Table("invoice".to_string())],
            ops: vec![Op::Read],
            actions: vec!["Create".to_string()],
            queries: vec![],
            flags: vec![Flag::AllowCustomQuery],
            oracles: vec![OracleClaim {
                name: "email.send".to_string(),
                mode: OracleMode::RequestReply,
                timing: OracleTiming::Async,
                domain: "corp.example".to_string(),
            }],
            constraints: vec![],
            nbf: 0,
            exp: 999,
            sig: vec![],
        };
        let payload = cbor::encode_canonical_value(&token.signed_value()).unwrap();
        token.sig = crypto::sign(&sk, &payload);
        assert!(token.verify().unwrap());
        let round = CapToken::from_cbor(&token.to_cbor().unwrap()).unwrap();
        assert_eq!(round.domain, token.domain);
    }

    #[test]
    fn check_access_matches_scope() {
        let token = CapToken {
            v: 1,
            id: [1u8; 32],
            issuer: IdentityKey::from_bytes([2u8; 32]),
            domain: "corp.example".to_string(),
            level: "public".to_string(),
            subject: None,
            scopes: vec![Scope::Namespace("com.acme.*".to_string())],
            ops: vec![Op::Read],
            actions: vec![],
            queries: vec![],
            flags: vec![],
            oracles: vec![],
            constraints: vec![],
            nbf: 0,
            exp: 10,
            sig: vec![1; 64],
        };
        assert!(token
            .check_access(Op::Read, &Scope::Namespace("com.acme.invoice".to_string()))
            .is_ok());
        assert!(token
            .check_access(Op::Read, &Scope::Namespace("com.other".to_string()))
            .is_err());
    }
}
