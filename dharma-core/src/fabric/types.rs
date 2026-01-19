use crate::cbor;
use crate::crypto;
use crate::error::DharmaError;
use crate::types::{IdentityKey, SchemaId};
use crate::value::{expect_array, expect_map, expect_text, expect_uint, map_get};
use ciborium::value::Value;
use crc32fast::Hasher;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardMap {
    pub table: String,
    pub strategy: ShardingStrategy,
    pub key_col: String,
    pub shard_count: u32,
    pub replication_factor: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardingStrategy {
    Hash,
    TimeAndHash,
}

impl ShardMap {
    pub fn resolve(&self, key: &[u8]) -> u32 {
        self.resolve_with_time(key, None)
    }

    pub fn resolve_with_time(&self, key: &[u8], time_bucket: Option<u64>) -> u32 {
        let mut hasher = Hasher::new();
        match self.strategy {
            ShardingStrategy::Hash => {
                hasher.update(key);
            }
            ShardingStrategy::TimeAndHash => {
                if let Some(bucket) = time_bucket {
                    hasher.update(&bucket.to_be_bytes());
                }
                hasher.update(key);
            }
        }
        let hash = hasher.finalize();
        if self.shard_count == 0 {
            return 0;
        }
        hash % self.shard_count
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Advertisement {
    pub v: u64,
    pub provider_id: IdentityKey,
    pub ts: u64,
    pub ttl: u32,
    pub endpoints: Vec<Endpoint>,
    pub shards: Vec<ShardAd>,
    pub load: u8,
    pub domain: String,
    pub policy_hash: [u8; 32],
    pub oracles: Vec<OracleAd>,
    pub sig: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub protocol: String,
    pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardAd {
    pub table: String,
    pub shard: u32,
    pub watermark: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleAd {
    pub name: String,
    pub domain: String,
    pub mode: OracleMode,
    pub timing: OracleTiming,
    pub input_schema: SchemaId,
    pub output_schema: Option<SchemaId>,
    pub max_inflight: Option<u32>,
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OracleMode {
    InputOnly,
    RequestReply,
    OutputOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OracleTiming {
    Sync,
    Async,
}

impl Advertisement {
    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.to_value())
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        Self::from_value(&value)
    }

    pub fn verify(&self) -> Result<bool, DharmaError> {
        let payload = cbor::encode_canonical_value(&self.signed_value())?;
        crypto::verify(&self.provider_id, &payload, &self.sig)
    }

    pub fn signed_value(&self) -> Value {
        Value::Map(self.base_entries())
    }

    pub fn to_value(&self) -> Value {
        let mut entries = self.base_entries();
        entries.push((Value::Text("sig".to_string()), Value::Bytes(self.sig.clone())));
        Value::Map(entries)
    }

    fn base_entries(&self) -> Vec<(Value, Value)> {
        let endpoints = Value::Array(self.endpoints.iter().map(Endpoint::to_value).collect());
        let shards = Value::Array(self.shards.iter().map(ShardAd::to_value).collect());
        let oracles = Value::Array(self.oracles.iter().map(OracleAd::to_value).collect());
        vec![
            (Value::Text("v".to_string()), Value::Integer(self.v.into())),
            (
                Value::Text("provider_id".to_string()),
                Value::Bytes(self.provider_id.as_bytes().to_vec()),
            ),
            (Value::Text("ts".to_string()), Value::Integer(self.ts.into())),
            (Value::Text("ttl".to_string()), Value::Integer(self.ttl.into())),
            (Value::Text("endpoints".to_string()), endpoints),
            (Value::Text("shards".to_string()), shards),
            (Value::Text("load".to_string()), Value::Integer(self.load.into())),
            (Value::Text("domain".to_string()), Value::Text(self.domain.clone())),
            (
                Value::Text("policy_hash".to_string()),
                Value::Bytes(self.policy_hash.to_vec()),
            ),
            (Value::Text("oracles".to_string()), oracles),
        ]
    }

    pub fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let v = expect_uint(map_get(map, "v").ok_or_else(|| DharmaError::Validation("missing v".to_string()))?)?;
        let provider_bytes = map_get(map, "provider_id")
            .ok_or_else(|| DharmaError::Validation("missing provider_id".to_string()))?;
        let provider_raw = crate::value::expect_bytes(provider_bytes)?;
        let provider_id = IdentityKey::from_slice(&provider_raw)?;
        let ts = expect_uint(map_get(map, "ts").ok_or_else(|| DharmaError::Validation("missing ts".to_string()))?)?;
        let ttl = expect_uint(map_get(map, "ttl").ok_or_else(|| DharmaError::Validation("missing ttl".to_string()))?)?;
        let ttl_u32: u32 = ttl
            .try_into()
            .map_err(|_| DharmaError::Validation("ttl out of range".to_string()))?;
        let endpoints_val = map_get(map, "endpoints")
            .ok_or_else(|| DharmaError::Validation("missing endpoints".to_string()))?;
        let endpoints = parse_array(endpoints_val, Endpoint::from_value)?;
        let shards_val = map_get(map, "shards")
            .ok_or_else(|| DharmaError::Validation("missing shards".to_string()))?;
        let shards = parse_array(shards_val, ShardAd::from_value)?;
        let load = expect_uint(map_get(map, "load").ok_or_else(|| DharmaError::Validation("missing load".to_string()))?)?;
        let load_u8: u8 = load
            .try_into()
            .map_err(|_| DharmaError::Validation("load out of range".to_string()))?;
        let domain = expect_text(
            map_get(map, "domain").ok_or_else(|| DharmaError::Validation("missing domain".to_string()))?,
        )?;
        let policy_val = map_get(map, "policy_hash")
            .ok_or_else(|| DharmaError::Validation("missing policy_hash".to_string()))?;
        let policy_bytes = crate::value::expect_bytes(policy_val)?;
        if policy_bytes.len() != 32 {
            return Err(DharmaError::Validation("policy_hash length".to_string()));
        }
        let mut policy_hash = [0u8; 32];
        policy_hash.copy_from_slice(&policy_bytes);
        let oracles_val = map_get(map, "oracles")
            .ok_or_else(|| DharmaError::Validation("missing oracles".to_string()))?;
        let oracles = parse_array(oracles_val, OracleAd::from_value)?;
        let sig = map_get(map, "sig")
            .map(crate::value::expect_bytes)
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            v,
            provider_id,
            ts,
            ttl: ttl_u32,
            endpoints,
            shards,
            load: load_u8,
            domain,
            policy_hash,
            oracles,
            sig,
        })
    }
}

impl Endpoint {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (Value::Text("proto".to_string()), Value::Text(self.protocol.clone())),
            (Value::Text("addr".to_string()), Value::Text(self.address.clone())),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let protocol = expect_text(
            map_get(map, "proto").ok_or_else(|| DharmaError::Validation("missing proto".to_string()))?,
        )?;
        let address = expect_text(
            map_get(map, "addr").ok_or_else(|| DharmaError::Validation("missing addr".to_string()))?,
        )?;
        Ok(Self { protocol, address })
    }
}

impl ShardAd {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (Value::Text("table".to_string()), Value::Text(self.table.clone())),
            (Value::Text("shard".to_string()), Value::Integer(self.shard.into())),
            (Value::Text("watermark".to_string()), Value::Integer(self.watermark.into())),
        ])
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let table = expect_text(
            map_get(map, "table").ok_or_else(|| DharmaError::Validation("missing table".to_string()))?,
        )?;
        let shard = expect_uint(
            map_get(map, "shard").ok_or_else(|| DharmaError::Validation("missing shard".to_string()))?,
        )?;
        let shard_u32: u32 = shard
            .try_into()
            .map_err(|_| DharmaError::Validation("shard out of range".to_string()))?;
        let watermark = expect_uint(
            map_get(map, "watermark").ok_or_else(|| DharmaError::Validation("missing watermark".to_string()))?,
        )?;
        Ok(Self {
            table,
            shard: shard_u32,
            watermark,
        })
    }
}

impl OracleAd {
    fn to_value(&self) -> Value {
        let mut entries = vec![
            (Value::Text("name".to_string()), Value::Text(self.name.clone())),
            (Value::Text("domain".to_string()), Value::Text(self.domain.clone())),
            (Value::Text("mode".to_string()), Value::Text(self.mode.as_str().to_string())),
            (
                Value::Text("timing".to_string()),
                Value::Text(self.timing.as_str().to_string()),
            ),
            (
                Value::Text("input_schema".to_string()),
                Value::Bytes(self.input_schema.as_bytes().to_vec()),
            ),
        ];
        if let Some(output) = &self.output_schema {
            entries.push((
                Value::Text("output_schema".to_string()),
                Value::Bytes(output.as_bytes().to_vec()),
            ));
        }
        if let Some(max_inflight) = self.max_inflight {
            entries.push((Value::Text("max_inflight".to_string()), Value::Integer(max_inflight.into())));
        }
        if let Some(timeout_ms) = self.timeout_ms {
            entries.push((Value::Text("timeout_ms".to_string()), Value::Integer(timeout_ms.into())));
        }
        Value::Map(entries)
    }

    fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let name = expect_text(
            map_get(map, "name").ok_or_else(|| DharmaError::Validation("missing name".to_string()))?,
        )?;
        let domain = expect_text(
            map_get(map, "domain").ok_or_else(|| DharmaError::Validation("missing domain".to_string()))?,
        )?;
        let mode_text = expect_text(
            map_get(map, "mode").ok_or_else(|| DharmaError::Validation("missing mode".to_string()))?,
        )?;
        let timing_text = expect_text(
            map_get(map, "timing").ok_or_else(|| DharmaError::Validation("missing timing".to_string()))?,
        )?;
        let input_schema_val = map_get(map, "input_schema")
            .ok_or_else(|| DharmaError::Validation("missing input_schema".to_string()))?;
        let input_bytes = crate::value::expect_bytes(input_schema_val)?;
        let input_schema = SchemaId::from_slice(&input_bytes)?;
        let output_schema = match map_get(map, "output_schema") {
            Some(val) => {
                let bytes = crate::value::expect_bytes(val)?;
                Some(SchemaId::from_slice(&bytes)?)
            }
            None => None,
        };
        let max_inflight = match map_get(map, "max_inflight") {
            Some(val) => {
                let raw = expect_uint(val)?;
                Some(
                    raw.try_into()
                        .map_err(|_| DharmaError::Validation("max_inflight out of range".to_string()))?,
                )
            }
            None => None,
        };
        let timeout_ms = match map_get(map, "timeout_ms") {
            Some(val) => Some(expect_uint(val)?),
            None => None,
        };
        Ok(Self {
            name,
            domain,
            mode: OracleMode::from_str(&mode_text)?,
            timing: OracleTiming::from_str(&timing_text)?,
            input_schema,
            output_schema,
            max_inflight,
            timeout_ms,
        })
    }
}

impl OracleMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            OracleMode::InputOnly => "input",
            OracleMode::RequestReply => "request_reply",
            OracleMode::OutputOnly => "output",
        }
    }

    pub fn from_str(text: &str) -> Result<Self, DharmaError> {
        match text {
            "input" => Ok(OracleMode::InputOnly),
            "request_reply" => Ok(OracleMode::RequestReply),
            "output" => Ok(OracleMode::OutputOnly),
            _ => Err(DharmaError::Validation("invalid oracle mode".to_string())),
        }
    }
}

impl OracleTiming {
    pub fn as_str(&self) -> &'static str {
        match self {
            OracleTiming::Sync => "sync",
            OracleTiming::Async => "async",
        }
    }

    pub fn from_str(text: &str) -> Result<Self, DharmaError> {
        match text {
            "sync" => Ok(OracleTiming::Sync),
            "async" => Ok(OracleTiming::Async),
            _ => Err(DharmaError::Validation("invalid oracle timing".to_string())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AdStore {
    ads: HashMap<IdentityKey, Advertisement>,
    index: HashMap<ShardKey, Vec<IdentityKey>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ShardKey {
    table: String,
    shard: u32,
}

impl AdStore {
    pub fn new() -> Self {
        Self {
            ads: HashMap::new(),
            index: HashMap::new(),
        }
    }

    pub fn insert(&mut self, ad: Advertisement) {
        let provider = ad.provider_id;
        self.ads.insert(provider, ad);
        self.rebuild_index();
    }

    pub fn get_providers_for_shard(&self, table: &str, shard: u32) -> Vec<IdentityKey> {
        let key = ShardKey {
            table: table.to_string(),
            shard,
        };
        self.index.get(&key).cloned().unwrap_or_default()
    }

    pub fn prune(&mut self, now: u64) {
        self.ads.retain(|_, ad| ad.ts.saturating_add(ad.ttl as u64) >= now);
        self.rebuild_index();
    }

    pub fn get_all(&self) -> Vec<(IdentityKey, Advertisement)> {
        self.ads.iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    fn rebuild_index(&mut self) {
        self.index.clear();
        for (provider, ad) in self.ads.iter() {
            for shard in ad.shards.iter() {
                let key = ShardKey {
                    table: shard.table.clone(),
                    shard: shard.shard,
                };
                self.index.entry(key).or_default().push(*provider);
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn shard_map_hash_is_deterministic() {
        let map = ShardMap {
            table: "invoice".to_string(),
            strategy: ShardingStrategy::Hash,
            key_col: "id".to_string(),
            shard_count: 16,
            replication_factor: 3,
        };
        let a = map.resolve(b"abc");
        let b = map.resolve(b"abc");
        assert_eq!(a, b);
    }

    #[test]
    fn advertisement_sign_and_verify() {
        let mut rng = StdRng::seed_from_u64(42);
        let (sk, pk) = crypto::generate_identity_keypair(&mut rng);
        let ad = Advertisement {
            v: 1,
            provider_id: pk,
            ts: 10,
            ttl: 30,
            endpoints: vec![Endpoint {
                protocol: "tcp".to_string(),
                address: "127.0.0.1:3000".to_string(),
            }],
            shards: vec![ShardAd {
                table: "invoice".to_string(),
                shard: 1,
                watermark: 9,
            }],
            load: 10,
            domain: "corp.example".to_string(),
            policy_hash: [7u8; 32],
            oracles: vec![],
            sig: vec![],
        };
        let payload = cbor::encode_canonical_value(&ad.signed_value()).unwrap();
        let sig = crypto::sign(&sk, &payload);
        let signed = Advertisement { sig, ..ad };
        assert!(signed.verify().unwrap());
    }

    #[test]
    fn ad_store_prune() {
        let mut rng = StdRng::seed_from_u64(5);
        let (_sk, pk) = crypto::generate_identity_keypair(&mut rng);
        let ad = Advertisement {
            v: 1,
            provider_id: pk,
            ts: 10,
            ttl: 5,
            endpoints: vec![],
            shards: vec![ShardAd {
                table: "invoice".to_string(),
                shard: 0,
                watermark: 0,
            }],
            load: 0,
            domain: "corp.example".to_string(),
            policy_hash: [0u8; 32],
            oracles: vec![],
            sig: vec![1; 64],
        };
        let mut store = AdStore::new();
        store.insert(ad);
        assert_eq!(store.get_providers_for_shard("invoice", 0).len(), 1);
        store.prune(20);
        assert_eq!(store.get_providers_for_shard("invoice", 0).len(), 0);
    }
}
