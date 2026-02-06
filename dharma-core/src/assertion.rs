use crate::cbor;
use crate::crypto;
use crate::error::DharmaError;
use crate::assertion_types::{META_OVERLAY, META_SIGNER};
use crate::types::{AssertionId, ContractId, IdentityKey, SchemaId, SubjectId};
use crate::value::{expect_array, expect_bytes, expect_int, expect_map, expect_text, expect_uint, map_get};
use ciborium::value::Value;
use ed25519_dalek::SigningKey;

pub const DEFAULT_DATA_VERSION: u64 = 1;

#[derive(Clone, Debug, PartialEq)]
pub struct AssertionHeader {
    pub v: u64,
    pub ver: u64,
    pub sub: SubjectId,
    pub typ: String,
    pub auth: IdentityKey,
    pub seq: u64,
    pub prev: Option<AssertionId>,
    pub refs: Vec<AssertionId>,
    pub ts: Option<i64>,
    pub schema: SchemaId,
    pub contract: ContractId,
    pub note: Option<String>,
    pub meta: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AssertionPlaintext {
    pub header: AssertionHeader,
    pub body: Value,
    pub sig: Vec<u8>,
}

impl AssertionHeader {
    pub fn to_value(&self) -> Value {
        let mut entries = Vec::new();
        entries.push((Value::Text("v".to_string()), Value::Integer(self.v.into())));
        entries.push((Value::Text("ver".to_string()), Value::Integer(self.ver.into())));
        entries.push((
            Value::Text("sub".to_string()),
            Value::Bytes(self.sub.as_bytes().to_vec()),
        ));
        entries.push((Value::Text("typ".to_string()), Value::Text(self.typ.clone())));
        entries.push((
            Value::Text("auth".to_string()),
            Value::Bytes(self.auth.as_bytes().to_vec()),
        ));
        entries.push((Value::Text("seq".to_string()), Value::Integer(self.seq.into())));
        entries.push((
            Value::Text("prev".to_string()),
            match &self.prev {
                Some(prev) => Value::Bytes(prev.as_bytes().to_vec()),
                None => Value::Null,
            },
        ));
        entries.push((
            Value::Text("refs".to_string()),
            Value::Array(
                self.refs
                    .iter()
                    .map(|id| Value::Bytes(id.as_bytes().to_vec()))
                    .collect(),
            ),
        ));
        entries.push((
            Value::Text("ts".to_string()),
            match self.ts {
                Some(ts) => Value::Integer(ts.into()),
                None => Value::Null,
            },
        ));
        entries.push((
            Value::Text("schema".to_string()),
            Value::Bytes(self.schema.as_bytes().to_vec()),
        ));
        entries.push((
            Value::Text("contract".to_string()),
            Value::Bytes(self.contract.as_bytes().to_vec()),
        ));
        if let Some(note) = &self.note {
            entries.push((Value::Text("note".to_string()), Value::Text(note.clone())));
        }
        if let Some(meta) = &self.meta {
            entries.push((Value::Text("meta".to_string()), meta.clone()));
        }
        Value::Map(entries)
    }

    pub fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let v = expect_uint(map_get(map, "v").ok_or_else(|| DharmaError::Validation("missing v".to_string()))?)?;
        let ver = match map_get(map, "ver") {
            Some(value) => expect_uint(value)?,
            None => DEFAULT_DATA_VERSION,
        };
        let sub_bytes = expect_bytes(map_get(map, "sub").ok_or_else(|| DharmaError::Validation("missing sub".to_string()))?)?;
        let typ = expect_text(map_get(map, "typ").ok_or_else(|| DharmaError::Validation("missing typ".to_string()))?)?;
        let auth_bytes = expect_bytes(map_get(map, "auth").ok_or_else(|| DharmaError::Validation("missing auth".to_string()))?)?;
        let seq = expect_uint(map_get(map, "seq").ok_or_else(|| DharmaError::Validation("missing seq".to_string()))?)?;
        let prev_val = map_get(map, "prev").ok_or_else(|| DharmaError::Validation("missing prev".to_string()))?;
        let prev = match prev_val {
            Value::Null => None,
            other => Some(AssertionId::from_slice(&expect_bytes(other)?)?),
        };
        let refs_val = map_get(map, "refs").ok_or_else(|| DharmaError::Validation("missing refs".to_string()))?;
        let refs_array = expect_array(refs_val)?;
        let mut refs = Vec::with_capacity(refs_array.len());
        for entry in refs_array {
            let bytes = expect_bytes(entry)?;
            refs.push(AssertionId::from_slice(&bytes)?);
        }
        let ts_val = map_get(map, "ts").ok_or_else(|| DharmaError::Validation("missing ts".to_string()))?;
        let ts = match ts_val {
            Value::Null => None,
            other => Some(expect_int(other)?),
        };
        let schema_bytes = expect_bytes(map_get(map, "schema").ok_or_else(|| DharmaError::Validation("missing schema".to_string()))?)?;
        let contract_bytes = expect_bytes(map_get(map, "contract").ok_or_else(|| DharmaError::Validation("missing contract".to_string()))?)?;
        let note = map_get(map, "note").map(|v| expect_text(v)).transpose()?;
        let meta = map_get(map, "meta").cloned();

        Ok(Self {
            v,
            ver,
            sub: SubjectId::from_slice(&sub_bytes)?,
            typ,
            auth: IdentityKey::from_slice(&auth_bytes)?,
            seq,
            prev,
            refs,
            ts,
            schema: SchemaId::from_slice(&schema_bytes)?,
            contract: ContractId::from_slice(&contract_bytes)?,
            note,
            meta,
        })
    }
}

pub fn is_overlay(header: &AssertionHeader) -> bool {
    let Some(Value::Map(entries)) = &header.meta else {
        return false;
    };
    for (key, value) in entries {
        if let Value::Text(name) = key {
            if name == META_OVERLAY {
                return matches!(value, Value::Bool(true));
            }
        }
    }
    false
}

pub fn add_signer_meta(meta: Option<Value>, signer: &SubjectId) -> Option<Value> {
    let mut entries = match meta {
        Some(Value::Map(entries)) => entries,
        _ => Vec::new(),
    };
    entries.retain(|(key, _)| !matches!(key, Value::Text(name) if name == META_SIGNER));
    entries.push((
        Value::Text(META_SIGNER.to_string()),
        Value::Bytes(signer.as_bytes().to_vec()),
    ));
    Some(Value::Map(entries))
}

pub fn signer_from_meta(meta: &Option<Value>) -> Option<SubjectId> {
    let Some(Value::Map(entries)) = meta else {
        return None;
    };
    for (key, value) in entries {
        if let Value::Text(name) = key {
            if name == META_SIGNER {
                if let Value::Bytes(bytes) = value {
                    if let Ok(subject) = SubjectId::from_slice(bytes) {
                        return Some(subject);
                    }
                }
            }
        }
    }
    None
}

impl AssertionPlaintext {
    pub fn sign(header: AssertionHeader, body: Value, key: &SigningKey) -> Result<Self, DharmaError> {
        let sig = sign_payload(&header, &body, key)?;
        Ok(Self { header, body, sig })
    }

    pub fn to_value(&self) -> Value {
        Value::Map(vec![
            (Value::Text("h".to_string()), self.header.to_value()),
            (Value::Text("b".to_string()), self.body.clone()),
            (Value::Text("sig".to_string()), Value::Bytes(self.sig.clone())),
        ])
    }

    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.to_value())
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        let map = expect_map(&value)?;
        let header_val = map_get(map, "h").ok_or_else(|| DharmaError::Validation("missing header".to_string()))?;
        let body_val = map_get(map, "b").ok_or_else(|| DharmaError::Validation("missing body".to_string()))?;
        let sig_val = map_get(map, "sig").ok_or_else(|| DharmaError::Validation("missing sig".to_string()))?;
        let header = AssertionHeader::from_value(header_val)?;
        let sig = expect_bytes(sig_val)?;
        Ok(Self { header, body: body_val.clone(), sig })
    }

    pub fn verify_signature(&self) -> Result<bool, DharmaError> {
        let bytes = payload_bytes(&self.header, &self.body)?;
        crypto::verify(&self.header.auth, &bytes, &self.sig)
    }

    pub fn assertion_id(&self) -> Result<AssertionId, DharmaError> {
        Ok(crypto::assertion_id(&payload_bytes(&self.header, &self.body)?))
    }
}

pub fn sign_payload(
    header: &AssertionHeader,
    body: &Value,
    key: &SigningKey,
) -> Result<Vec<u8>, DharmaError> {
    let bytes = payload_bytes(header, body)?;
    Ok(crypto::sign(key, &bytes))
}

pub fn payload_bytes(header: &AssertionHeader, body: &Value) -> Result<Vec<u8>, DharmaError> {
    let value = Value::Map(vec![
        (Value::Text("h".to_string()), header.to_value()),
        (Value::Text("b".to_string()), body.clone()),
    ]);
    cbor::encode_canonical_value(&value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn header() -> AssertionHeader {
        AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: SubjectId::from_bytes([1u8; 32]),
            typ: "core.genesis".to_string(),
            auth: IdentityKey::from_bytes([2u8; 32]),
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: Some("note".to_string()),
            meta: None,
        }
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let mut rng = StdRng::seed_from_u64(1);
        let (sk, id) = crypto::generate_identity_keypair(&mut rng);
        let mut header = header();
        header.auth = id;
        let body = Value::Map(vec![(
            Value::Text("doc_type".to_string()),
            Value::Text("task".to_string()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).unwrap();
        assert!(assertion.verify_signature().unwrap());
        let bytes = assertion.to_cbor().unwrap();
        let parsed = AssertionPlaintext::from_cbor(&bytes).unwrap();
        assert_eq!(assertion.header, parsed.header);
    }

    #[test]
    fn signature_rejects_tamper() {
        let mut rng = StdRng::seed_from_u64(2);
        let (sk, id) = crypto::generate_identity_keypair(&mut rng);
        let mut header = header();
        header.auth = id;
        let mut assertion = AssertionPlaintext::sign(header, Value::Null, &sk).unwrap();
        assertion.sig[0] ^= 0xff;
        assert!(!assertion.verify_signature().unwrap());
    }

    #[test]
    fn from_cbor_rejects_noncanonical() {
        let mut rng = StdRng::seed_from_u64(5);
        let (sk, id) = crypto::generate_identity_keypair(&mut rng);
        let mut header = header();
        header.auth = id;
        let assertion = AssertionPlaintext::sign(header, Value::Null, &sk).unwrap();
        let value = assertion.to_value();
        let mut raw = Vec::new();
        ciborium::ser::into_writer(&value, &mut raw).unwrap();
        assert!(AssertionPlaintext::from_cbor(&raw).is_err());
    }

    #[test]
    fn from_cbor_defaults_missing_ver() {
        let header = header();
        let mut header_map = match header.to_value() {
            Value::Map(map) => map,
            _ => panic!("expected map"),
        };
        header_map.retain(|(k, _)| match k {
            Value::Text(name) => name != "ver",
            _ => true,
        });
        let value = Value::Map(vec![
            (Value::Text("h".to_string()), Value::Map(header_map)),
            (Value::Text("b".to_string()), Value::Null),
            (Value::Text("sig".to_string()), Value::Bytes(vec![0u8; 64])),
        ]);
        let bytes = cbor::encode_canonical_value(&value).unwrap();
        let parsed = AssertionPlaintext::from_cbor(&bytes).unwrap();
        assert_eq!(parsed.header.ver, DEFAULT_DATA_VERSION);
    }
}
