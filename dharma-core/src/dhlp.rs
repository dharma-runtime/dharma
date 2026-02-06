use crate::cbor;
use crate::dhlq::{expr_from_value, expr_to_value, QueryPlan};
use crate::error::DharmaError;
use crate::reactor::Expr;
use crate::value::{expect_array, expect_int, expect_map, expect_text, map_get};
use ciborium::value::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectionPlan {
    pub version: u8,
    pub triggers: Vec<TriggerSpec>,
    pub scope: Vec<ScopeBinding>,
    pub batch_window_ms: u64,
    pub max_delay_ms: u64,
    pub query_source: String,
    pub query: QueryPlan,
    pub emit: EmitSpec,
    pub prune: Option<PruneSpec>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TriggerSpec {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScopeBinding {
    pub name: String,
    pub expr: Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EmitSpec {
    pub verb: String,
    pub target: String,
    pub args: Vec<(String, Expr)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PruneSpec {
    pub keys: Vec<String>,
    pub predicate: Option<Expr>,
}

impl ProjectionPlan {
    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        let query_bytes = self.query.to_cbor()?;
        let mut scope_entries = Vec::new();
        for binding in &self.scope {
            scope_entries.push((
                Value::Text(binding.name.clone()),
                expr_to_value(&binding.expr),
            ));
        }
        let mut emit_args = Vec::new();
        for (name, expr) in &self.emit.args {
            emit_args.push((Value::Text(name.clone()), expr_to_value(expr)));
        }
        let emit_map = Value::Map(vec![
            (Value::Text("verb".to_string()), Value::Text(self.emit.verb.clone())),
            (Value::Text("target".to_string()), Value::Text(self.emit.target.clone())),
            (Value::Text("args".to_string()), Value::Map(emit_args)),
        ]);
        let triggers = Value::Array(
            self.triggers
                .iter()
                .map(|t| Value::Text(t.name.clone()))
                .collect(),
        );
        let mut entries = vec![
            (Value::Text("v".to_string()), Value::Integer((self.version as u64).into())),
            (Value::Text("triggers".to_string()), triggers),
            (Value::Text("scope".to_string()), Value::Map(scope_entries)),
            (
                Value::Text("batch_window".to_string()),
                Value::Integer(self.batch_window_ms.into()),
            ),
            (
                Value::Text("max_delay".to_string()),
                Value::Integer(self.max_delay_ms.into()),
            ),
            (
                Value::Text("query_source".to_string()),
                Value::Text(self.query_source.clone()),
            ),
            (Value::Text("query".to_string()), Value::Bytes(query_bytes)),
            (Value::Text("emit".to_string()), emit_map),
        ];
        if let Some(prune) = &self.prune {
            let mut prune_entries = Vec::new();
            prune_entries.push((
                Value::Text("by".to_string()),
                Value::Array(prune.keys.iter().map(|k| Value::Text(k.clone())).collect()),
            ));
            if let Some(expr) = &prune.predicate {
                prune_entries.push((Value::Text("where".to_string()), expr_to_value(expr)));
            }
            entries.push((Value::Text("prune".to_string()), Value::Map(prune_entries)));
        }
        cbor::encode_canonical_value(&Value::Map(entries))
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        let map = expect_map(&value)?;
        let version = map_get(map, "v").map(expect_int).transpose()?.unwrap_or(1) as u8;
        let mut triggers = Vec::new();
        if let Some(list) = map_get(map, "triggers") {
            for item in expect_array(list)? {
                triggers.push(TriggerSpec { name: expect_text(item)? });
            }
        }
        let mut scope = Vec::new();
        if let Some(scope_val) = map_get(map, "scope") {
            for (k, v) in expect_map(scope_val)? {
                let name = expect_text(k)?;
                let expr = expr_from_value(v)?;
                scope.push(ScopeBinding { name, expr });
            }
        }
        let batch_window_ms = map_get(map, "batch_window")
            .map(expect_int)
            .transpose()?
            .unwrap_or(250) as u64;
        let max_delay_ms = map_get(map, "max_delay")
            .map(expect_int)
            .transpose()?
            .unwrap_or(1000) as u64;
        let query_source = map_get(map, "query_source")
            .map(expect_text)
            .transpose()?
            .unwrap_or_else(|| "state".to_string());
        let query_bytes = map_get(map, "query")
            .map(|v| crate::value::expect_bytes(v))
            .transpose()?
            .ok_or_else(|| DharmaError::Validation("missing query".to_string()))?;
        let query = QueryPlan::from_cbor(&query_bytes)?;
        let emit_val = map_get(map, "emit")
            .ok_or_else(|| DharmaError::Validation("missing emit".to_string()))?;
        let emit_map = expect_map(emit_val)?;
        let verb = map_get(emit_map, "verb")
            .map(expect_text)
            .transpose()?
            .unwrap_or_default();
        let target = map_get(emit_map, "target")
            .map(expect_text)
            .transpose()?
            .unwrap_or_default();
        let mut args = Vec::new();
        if let Some(args_val) = map_get(emit_map, "args") {
            for (k, v) in expect_map(args_val)? {
                let name = expect_text(k)?;
                let expr = expr_from_value(v)?;
                args.push((name, expr));
            }
        }
        let emit = EmitSpec { verb, target, args };
        let prune = if let Some(prune_val) = map_get(map, "prune") {
            let prune_map = expect_map(prune_val)?;
            let mut keys = Vec::new();
            if let Some(by_val) = map_get(prune_map, "by") {
                for item in expect_array(by_val)? {
                    keys.push(expect_text(item)?);
                }
            }
            let predicate = map_get(prune_map, "where").map(expr_from_value).transpose()?;
            Some(PruneSpec { keys, predicate })
        } else {
            None
        };
        Ok(Self {
            version,
            triggers,
            scope,
            batch_window_ms,
            max_delay_ms,
            query_source,
            query,
            emit,
            prune,
        })
    }
}
