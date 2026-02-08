use crate::cbor;
use crate::error::DharmaError;
use crate::fabric::auth::{CapToken, Flag, Op, Scope};
use crate::fabric::types::{OracleMode, OracleTiming};
use crate::types::{EnvelopeId, SubjectId};
use crate::value::{expect_bool, expect_map, expect_text, expect_uint, map_get};
use ciborium::value::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct FabricRequest {
    pub req_id: [u8; 16],
    pub cap: CapToken,
    pub op: FabricOp,
    pub deadline: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FabricOp {
    ExecAction {
        subject: SubjectId,
        action: String,
        args: Value,
    },
    ExecQuery {
        subject: SubjectId,
        query: String,
        params: Value,
        predefined: bool,
    },
    QueryFast {
        table: String,
        key: Value,
        query: String,
    },
    QueryWide {
        table: String,
        shard: u32,
        query: String,
    },
    Fetch {
        id: EnvelopeId,
    },
    OracleInvoke {
        name: String,
        mode: OracleMode,
        timing: OracleTiming,
        input: Value,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct FabricResponse {
    pub req_id: [u8; 16],
    pub status: u16,
    pub watermark: u64,
    pub payload: Vec<u8>,
    pub stats: ExecStats,
    pub provenance: Option<Vec<EnvelopeId>>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct ExecStats {
    pub elapsed_ms: u64,
    pub rows_scanned: u64,
}

pub trait FabricDispatcher {
    fn now(&self) -> u64;
    fn authorize_exec_action(
        &self,
        _req: &FabricRequest,
        _subject: &SubjectId,
        _action: &str,
    ) -> Result<(), DharmaError> {
        Ok(())
    }
    fn authorize_exec_query(
        &self,
        _req: &FabricRequest,
        _subject: &SubjectId,
        _query: &str,
        _predefined: bool,
    ) -> Result<(), DharmaError> {
        Ok(())
    }
    fn authorize_query_fast(
        &self,
        _req: &FabricRequest,
        _table: &str,
        _key: &Value,
        _query: &str,
    ) -> Result<(), DharmaError> {
        Ok(())
    }
    fn authorize_query_wide(
        &self,
        _req: &FabricRequest,
        _table: &str,
        _shard: u32,
        _query: &str,
    ) -> Result<(), DharmaError> {
        Ok(())
    }
    fn exec_action(
        &self,
        req: &FabricRequest,
        subject: &SubjectId,
        action: &str,
        args: &Value,
    ) -> Result<FabricResponse, DharmaError>;
    fn exec_query(
        &self,
        req: &FabricRequest,
        subject: &SubjectId,
        query: &str,
        params: &Value,
        predefined: bool,
    ) -> Result<FabricResponse, DharmaError>;
    fn query_fast(
        &self,
        req: &FabricRequest,
        table: &str,
        key: &Value,
        query: &str,
    ) -> Result<FabricResponse, DharmaError>;
    fn query_wide(
        &self,
        req: &FabricRequest,
        table: &str,
        shard: u32,
        query: &str,
    ) -> Result<FabricResponse, DharmaError>;
    fn fetch(&self, req: &FabricRequest, id: &EnvelopeId) -> Result<FabricResponse, DharmaError>;
    fn oracle_invoke(
        &self,
        req: &FabricRequest,
        name: &str,
        mode: OracleMode,
        timing: OracleTiming,
        input: &Value,
    ) -> Result<FabricResponse, DharmaError>;
}

pub fn dispatch<D: FabricDispatcher>(
    dispatcher: &D,
    req: &FabricRequest,
) -> Result<FabricResponse, DharmaError> {
    let now = dispatcher.now();
    if now > req.deadline {
        return Err(DharmaError::Validation("deadline exceeded".to_string()));
    }
    if now < req.cap.nbf || now >= req.cap.exp {
        return Err(DharmaError::Validation("token not valid".to_string()));
    }
    match &req.op {
        FabricOp::ExecAction {
            subject,
            action,
            args,
        } => {
            req.cap
                .check_access(Op::Execute, &Scope::Subject(*subject))?;
            dispatcher.authorize_exec_action(req, subject, action)?;
            dispatcher.exec_action(req, subject, action, args)
        }
        FabricOp::ExecQuery {
            subject,
            query,
            params,
            predefined,
        } => {
            req.cap.check_access(Op::Read, &Scope::Subject(*subject))?;
            if !*predefined && !req.cap.flags.contains(&Flag::AllowCustomQuery) {
                return Err(DharmaError::Validation(
                    "custom query not allowed".to_string(),
                ));
            }
            dispatcher.authorize_exec_query(req, subject, query, *predefined)?;
            dispatcher.exec_query(req, subject, query, params, *predefined)
        }
        FabricOp::QueryFast { table, key, query } => {
            req.cap
                .check_access(Op::Read, &Scope::Table(table.clone()))?;
            if !req.cap.flags.contains(&Flag::AllowCustomQuery) {
                return Err(DharmaError::Validation(
                    "custom query not allowed".to_string(),
                ));
            }
            dispatcher.authorize_query_fast(req, table, key, query)?;
            dispatcher.query_fast(req, table, key, query)
        }
        FabricOp::QueryWide {
            table,
            shard,
            query,
        } => {
            req.cap
                .check_access(Op::Read, &Scope::Table(table.clone()))?;
            if !req.cap.flags.contains(&Flag::AllowCustomQuery) {
                return Err(DharmaError::Validation(
                    "custom query not allowed".to_string(),
                ));
            }
            dispatcher.authorize_query_wide(req, table, *shard, query)?;
            dispatcher.query_wide(req, table, *shard, query)
        }
        FabricOp::Fetch { id } => {
            req.cap
                .check_access(Op::Read, &Scope::Table("objects".to_string()))?;
            dispatcher.fetch(req, id)
        }
        FabricOp::OracleInvoke {
            name,
            mode,
            timing,
            input,
        } => {
            if !req.cap.oracles.iter().any(|claim| {
                claim.name == *name
                    && claim.mode == *mode
                    && claim.timing == *timing
                    && claim.domain == req.cap.domain
            }) {
                return Err(DharmaError::Validation("oracle not allowed".to_string()));
            }
            dispatcher.oracle_invoke(req, name, *mode, *timing, input)
        }
    }
}

impl FabricRequest {
    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.to_value())
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        Self::from_value(&value)
    }

    fn to_value(&self) -> Value {
        Value::Map(vec![
            (
                Value::Text("req_id".to_string()),
                Value::Bytes(self.req_id.to_vec()),
            ),
            (Value::Text("cap".to_string()), self.cap.to_value()),
            (Value::Text("op".to_string()), self.op.to_value()),
            (
                Value::Text("deadline".to_string()),
                Value::Integer(self.deadline.into()),
            ),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let req_id = parse_bytes16(
            map_get(map, "req_id")
                .ok_or_else(|| DharmaError::Validation("missing req_id".to_string()))?,
        )?;
        let cap_val = map_get(map, "cap")
            .ok_or_else(|| DharmaError::Validation("missing cap".to_string()))?;
        let cap = CapToken::from_value(cap_val)?;
        let op_val =
            map_get(map, "op").ok_or_else(|| DharmaError::Validation("missing op".to_string()))?;
        let op = FabricOp::from_value(op_val)?;
        let deadline = expect_uint(
            map_get(map, "deadline")
                .ok_or_else(|| DharmaError::Validation("missing deadline".to_string()))?,
        )?;
        Ok(Self {
            req_id,
            cap,
            op,
            deadline,
        })
    }
}

impl FabricOp {
    fn to_value(&self) -> Value {
        match self {
            FabricOp::ExecAction {
                subject,
                action,
                args,
            } => Value::Map(vec![
                (
                    Value::Text("t".to_string()),
                    Value::Text("exec_action".to_string()),
                ),
                (
                    Value::Text("subject".to_string()),
                    Value::Bytes(subject.as_bytes().to_vec()),
                ),
                (
                    Value::Text("action".to_string()),
                    Value::Text(action.clone()),
                ),
                (Value::Text("args".to_string()), args.clone()),
            ]),
            FabricOp::ExecQuery {
                subject,
                query,
                params,
                predefined,
            } => Value::Map(vec![
                (
                    Value::Text("t".to_string()),
                    Value::Text("exec_query".to_string()),
                ),
                (
                    Value::Text("subject".to_string()),
                    Value::Bytes(subject.as_bytes().to_vec()),
                ),
                (Value::Text("query".to_string()), Value::Text(query.clone())),
                (Value::Text("params".to_string()), params.clone()),
                (
                    Value::Text("predefined".to_string()),
                    Value::Bool(*predefined),
                ),
            ]),
            FabricOp::QueryFast { table, key, query } => Value::Map(vec![
                (
                    Value::Text("t".to_string()),
                    Value::Text("query_fast".to_string()),
                ),
                (Value::Text("table".to_string()), Value::Text(table.clone())),
                (Value::Text("key".to_string()), key.clone()),
                (Value::Text("query".to_string()), Value::Text(query.clone())),
            ]),
            FabricOp::QueryWide {
                table,
                shard,
                query,
            } => Value::Map(vec![
                (
                    Value::Text("t".to_string()),
                    Value::Text("query_wide".to_string()),
                ),
                (Value::Text("table".to_string()), Value::Text(table.clone())),
                (
                    Value::Text("shard".to_string()),
                    Value::Integer((*shard).into()),
                ),
                (Value::Text("query".to_string()), Value::Text(query.clone())),
            ]),
            FabricOp::Fetch { id } => Value::Map(vec![
                (
                    Value::Text("t".to_string()),
                    Value::Text("fetch".to_string()),
                ),
                (
                    Value::Text("id".to_string()),
                    Value::Bytes(id.as_bytes().to_vec()),
                ),
            ]),
            FabricOp::OracleInvoke {
                name,
                mode,
                timing,
                input,
            } => Value::Map(vec![
                (
                    Value::Text("t".to_string()),
                    Value::Text("oracle".to_string()),
                ),
                (Value::Text("name".to_string()), Value::Text(name.clone())),
                (
                    Value::Text("mode".to_string()),
                    Value::Text(mode.as_str().to_string()),
                ),
                (
                    Value::Text("timing".to_string()),
                    Value::Text(timing.as_str().to_string()),
                ),
                (Value::Text("input".to_string()), input.clone()),
            ]),
        }
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let t = expect_text(
            map_get(map, "t")
                .ok_or_else(|| DharmaError::Validation("missing op type".to_string()))?,
        )?;
        match t.as_str() {
            "exec_action" => {
                let subject_val = map_get(map, "subject")
                    .ok_or_else(|| DharmaError::Validation("missing subject".to_string()))?;
                let subject = parse_subject(subject_val)?;
                let action = expect_text(
                    map_get(map, "action")
                        .ok_or_else(|| DharmaError::Validation("missing action".to_string()))?,
                )?;
                let args = map_get(map, "args")
                    .ok_or_else(|| DharmaError::Validation("missing args".to_string()))?
                    .clone();
                Ok(FabricOp::ExecAction {
                    subject,
                    action,
                    args,
                })
            }
            "exec_query" => {
                let subject_val = map_get(map, "subject")
                    .ok_or_else(|| DharmaError::Validation("missing subject".to_string()))?;
                let subject = parse_subject(subject_val)?;
                let query = expect_text(
                    map_get(map, "query")
                        .ok_or_else(|| DharmaError::Validation("missing query".to_string()))?,
                )?;
                let params = map_get(map, "params")
                    .ok_or_else(|| DharmaError::Validation("missing params".to_string()))?
                    .clone();
                let predefined = map_get(map, "predefined")
                    .map(expect_bool)
                    .transpose()?
                    .unwrap_or(false);
                Ok(FabricOp::ExecQuery {
                    subject,
                    query,
                    params,
                    predefined,
                })
            }
            "query_fast" => {
                let table = expect_text(
                    map_get(map, "table")
                        .ok_or_else(|| DharmaError::Validation("missing table".to_string()))?,
                )?;
                let key = map_get(map, "key")
                    .ok_or_else(|| DharmaError::Validation("missing key".to_string()))?
                    .clone();
                let query = expect_text(
                    map_get(map, "query")
                        .ok_or_else(|| DharmaError::Validation("missing query".to_string()))?,
                )?;
                Ok(FabricOp::QueryFast { table, key, query })
            }
            "query_wide" => {
                let table = expect_text(
                    map_get(map, "table")
                        .ok_or_else(|| DharmaError::Validation("missing table".to_string()))?,
                )?;
                let shard = expect_uint(
                    map_get(map, "shard")
                        .ok_or_else(|| DharmaError::Validation("missing shard".to_string()))?,
                )?;
                let shard_u32: u32 = shard
                    .try_into()
                    .map_err(|_| DharmaError::Validation("shard out of range".to_string()))?;
                let query = expect_text(
                    map_get(map, "query")
                        .ok_or_else(|| DharmaError::Validation("missing query".to_string()))?,
                )?;
                Ok(FabricOp::QueryWide {
                    table,
                    shard: shard_u32,
                    query,
                })
            }
            "fetch" => {
                let id_val = map_get(map, "id")
                    .ok_or_else(|| DharmaError::Validation("missing id".to_string()))?;
                let id_bytes = crate::value::expect_bytes(id_val)?;
                let id = EnvelopeId::from_slice(&id_bytes)?;
                Ok(FabricOp::Fetch { id })
            }
            "oracle" => {
                let name = expect_text(
                    map_get(map, "name")
                        .ok_or_else(|| DharmaError::Validation("missing name".to_string()))?,
                )?;
                let mode_text = expect_text(
                    map_get(map, "mode")
                        .ok_or_else(|| DharmaError::Validation("missing mode".to_string()))?,
                )?;
                let timing_text = expect_text(
                    map_get(map, "timing")
                        .ok_or_else(|| DharmaError::Validation("missing timing".to_string()))?,
                )?;
                let input = map_get(map, "input")
                    .ok_or_else(|| DharmaError::Validation("missing input".to_string()))?
                    .clone();
                Ok(FabricOp::OracleInvoke {
                    name,
                    mode: OracleMode::from_str(&mode_text)?,
                    timing: OracleTiming::from_str(&timing_text)?,
                    input,
                })
            }
            _ => Err(DharmaError::Validation("unknown op type".to_string())),
        }
    }
}

impl FabricResponse {
    pub fn ok(req_id: [u8; 16], payload: Vec<u8>) -> Self {
        Self {
            req_id,
            status: 200,
            watermark: 0,
            payload,
            stats: ExecStats::default(),
            provenance: None,
        }
    }

    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.to_value())
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        Self::from_value(&value)
    }

    fn to_value(&self) -> Value {
        let stats = self.stats.to_value();
        let provenance = match &self.provenance {
            Some(list) => Value::Array(
                list.iter()
                    .map(|id| Value::Bytes(id.as_bytes().to_vec()))
                    .collect(),
            ),
            None => Value::Null,
        };
        Value::Map(vec![
            (
                Value::Text("req_id".to_string()),
                Value::Bytes(self.req_id.to_vec()),
            ),
            (
                Value::Text("status".to_string()),
                Value::Integer(self.status.into()),
            ),
            (
                Value::Text("watermark".to_string()),
                Value::Integer(self.watermark.into()),
            ),
            (
                Value::Text("payload".to_string()),
                Value::Bytes(self.payload.clone()),
            ),
            (Value::Text("stats".to_string()), stats),
            (Value::Text("provenance".to_string()), provenance),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let req_id = parse_bytes16(
            map_get(map, "req_id")
                .ok_or_else(|| DharmaError::Validation("missing req_id".to_string()))?,
        )?;
        let status = expect_uint(
            map_get(map, "status")
                .ok_or_else(|| DharmaError::Validation("missing status".to_string()))?,
        )?;
        let status_u16: u16 = status
            .try_into()
            .map_err(|_| DharmaError::Validation("status out of range".to_string()))?;
        let watermark = expect_uint(
            map_get(map, "watermark")
                .ok_or_else(|| DharmaError::Validation("missing watermark".to_string()))?,
        )?;
        let payload = crate::value::expect_bytes(
            map_get(map, "payload")
                .ok_or_else(|| DharmaError::Validation("missing payload".to_string()))?,
        )?;
        let stats_val = map_get(map, "stats")
            .ok_or_else(|| DharmaError::Validation("missing stats".to_string()))?;
        let stats = ExecStats::from_value(stats_val)?;
        let provenance_val = map_get(map, "provenance");
        let provenance = match provenance_val {
            Some(Value::Array(items)) => {
                let mut out = Vec::new();
                for item in items {
                    let bytes = crate::value::expect_bytes(item)?;
                    out.push(EnvelopeId::from_slice(&bytes)?);
                }
                Some(out)
            }
            _ => None,
        };
        Ok(Self {
            req_id,
            status: status_u16,
            watermark,
            payload,
            stats,
            provenance,
        })
    }
}

impl ExecStats {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (
                Value::Text("elapsed_ms".to_string()),
                Value::Integer(self.elapsed_ms.into()),
            ),
            (
                Value::Text("rows_scanned".to_string()),
                Value::Integer(self.rows_scanned.into()),
            ),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let elapsed_ms = expect_uint(
            map_get(map, "elapsed_ms")
                .ok_or_else(|| DharmaError::Validation("missing elapsed_ms".to_string()))?,
        )?;
        let rows_scanned = expect_uint(
            map_get(map, "rows_scanned")
                .ok_or_else(|| DharmaError::Validation("missing rows_scanned".to_string()))?,
        )?;
        Ok(Self {
            elapsed_ms,
            rows_scanned,
        })
    }
}

fn parse_bytes16(value: &Value) -> Result<[u8; 16], DharmaError> {
    let bytes = crate::value::expect_bytes(value)?;
    if bytes.len() != 16 {
        return Err(DharmaError::Validation("expected 16 bytes".to_string()));
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn parse_subject(value: &Value) -> Result<SubjectId, DharmaError> {
    let bytes = crate::value::expect_bytes(value)?;
    SubjectId::from_slice(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto;
    use crate::types::IdentityKey;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn fabric_request_roundtrip() {
        let mut rng = StdRng::seed_from_u64(11);
        let (sk, issuer) = crypto::generate_identity_keypair(&mut rng);
        let mut token = CapToken {
            v: 1,
            id: [9u8; 32],
            issuer,
            domain: "corp.example".to_string(),
            level: "admin".to_string(),
            subject: None,
            scopes: vec![Scope::Table("invoice".to_string())],
            ops: vec![Op::Read],
            actions: vec![],
            queries: vec![],
            flags: vec![],
            oracles: vec![],
            constraints: vec![],
            nbf: 0,
            exp: 100,
            sig: vec![],
        };
        let payload = cbor::encode_canonical_value(&token.signed_value()).unwrap();
        token.sig = crypto::sign(&sk, &payload);
        let req = FabricRequest {
            req_id: [1u8; 16],
            cap: token,
            op: FabricOp::QueryFast {
                table: "invoice".to_string(),
                key: Value::Integer(7.into()),
                query: "where amount > 0".to_string(),
            },
            deadline: 999,
        };
        let bytes = req.to_cbor().unwrap();
        let round = FabricRequest::from_cbor(&bytes).unwrap();
        assert_eq!(round.req_id, req.req_id);
    }

    #[test]
    fn exec_stats_roundtrip() {
        let stats = ExecStats {
            elapsed_ms: 12,
            rows_scanned: 7,
        };
        let value = stats.to_value();
        let round = ExecStats::from_value(&value).unwrap();
        assert_eq!(round.elapsed_ms, 12);
        assert_eq!(round.rows_scanned, 7);
    }

    #[test]
    fn dispatch_rejects_custom_query_without_flag() {
        struct Dummy;
        impl FabricDispatcher for Dummy {
            fn now(&self) -> u64 {
                10
            }
            fn exec_action(
                &self,
                _: &FabricRequest,
                _: &SubjectId,
                _: &str,
                _: &Value,
            ) -> Result<FabricResponse, DharmaError> {
                unreachable!()
            }
            fn exec_query(
                &self,
                _: &FabricRequest,
                _: &SubjectId,
                _: &str,
                _: &Value,
                _: bool,
            ) -> Result<FabricResponse, DharmaError> {
                unreachable!()
            }
            fn query_fast(
                &self,
                _: &FabricRequest,
                _: &str,
                _: &Value,
                _: &str,
            ) -> Result<FabricResponse, DharmaError> {
                unreachable!()
            }
            fn query_wide(
                &self,
                _: &FabricRequest,
                _: &str,
                _: u32,
                _: &str,
            ) -> Result<FabricResponse, DharmaError> {
                unreachable!()
            }
            fn fetch(
                &self,
                _: &FabricRequest,
                _: &EnvelopeId,
            ) -> Result<FabricResponse, DharmaError> {
                unreachable!()
            }
            fn oracle_invoke(
                &self,
                _: &FabricRequest,
                _: &str,
                _: OracleMode,
                _: OracleTiming,
                _: &Value,
            ) -> Result<FabricResponse, DharmaError> {
                unreachable!()
            }
        }
        let token = CapToken {
            v: 1,
            id: [9u8; 32],
            issuer: IdentityKey::from_bytes([2u8; 32]),
            domain: "corp.example".to_string(),
            level: "public".to_string(),
            subject: None,
            scopes: vec![Scope::Table("invoice".to_string())],
            ops: vec![Op::Read],
            actions: vec![],
            queries: vec![],
            flags: vec![],
            oracles: vec![],
            constraints: vec![],
            nbf: 0,
            exp: 100,
            sig: vec![1; 64],
        };
        let req = FabricRequest {
            req_id: [0u8; 16],
            cap: token,
            op: FabricOp::QueryFast {
                table: "invoice".to_string(),
                key: Value::Integer(1.into()),
                query: "x".to_string(),
            },
            deadline: 100,
        };
        let err = dispatch(&Dummy, &req).unwrap_err();
        match err {
            DharmaError::Validation(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn dispatch_enforces_authorizer() {
        struct Dummy;
        impl FabricDispatcher for Dummy {
            fn now(&self) -> u64 {
                10
            }
            fn authorize_exec_query(
                &self,
                _: &FabricRequest,
                _: &SubjectId,
                _: &str,
                _: bool,
            ) -> Result<(), DharmaError> {
                Err(DharmaError::Validation("denied".to_string()))
            }
            fn exec_action(
                &self,
                _: &FabricRequest,
                _: &SubjectId,
                _: &str,
                _: &Value,
            ) -> Result<FabricResponse, DharmaError> {
                unreachable!()
            }
            fn exec_query(
                &self,
                _: &FabricRequest,
                _: &SubjectId,
                _: &str,
                _: &Value,
                _: bool,
            ) -> Result<FabricResponse, DharmaError> {
                unreachable!()
            }
            fn query_fast(
                &self,
                _: &FabricRequest,
                _: &str,
                _: &Value,
                _: &str,
            ) -> Result<FabricResponse, DharmaError> {
                unreachable!()
            }
            fn query_wide(
                &self,
                _: &FabricRequest,
                _: &str,
                _: u32,
                _: &str,
            ) -> Result<FabricResponse, DharmaError> {
                unreachable!()
            }
            fn fetch(
                &self,
                _: &FabricRequest,
                _: &EnvelopeId,
            ) -> Result<FabricResponse, DharmaError> {
                unreachable!()
            }
            fn oracle_invoke(
                &self,
                _: &FabricRequest,
                _: &str,
                _: OracleMode,
                _: OracleTiming,
                _: &Value,
            ) -> Result<FabricResponse, DharmaError> {
                unreachable!()
            }
        }
        let subject = SubjectId::from_bytes([4u8; 32]);
        let token = CapToken {
            v: 1,
            id: [9u8; 32],
            issuer: IdentityKey::from_bytes([2u8; 32]),
            domain: "corp.example".to_string(),
            level: "public".to_string(),
            subject: Some(subject),
            scopes: vec![Scope::Subject(subject)],
            ops: vec![Op::Read],
            actions: vec![],
            queries: vec![],
            flags: vec![Flag::AllowCustomQuery],
            oracles: vec![],
            constraints: vec![],
            nbf: 0,
            exp: 100,
            sig: vec![1; 64],
        };
        let req = FabricRequest {
            req_id: [1u8; 16],
            cap: token,
            op: FabricOp::ExecQuery {
                subject,
                query: "List".to_string(),
                params: Value::Map(vec![]),
                predefined: true,
            },
            deadline: 100,
        };
        let err = dispatch(&Dummy, &req).unwrap_err();
        match err {
            DharmaError::Validation(msg) => assert!(msg.contains("denied")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
