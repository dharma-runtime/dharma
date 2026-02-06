use crate::cbor;
use crate::crypto::{self, ENVELOPE_VERSION, SUITE_ID};
use crate::error::DharmaError;
use crate::types::{EnvelopeId, KeyId, Nonce12};
use crate::value::{expect_bytes, expect_map, expect_uint, map_get};
use ciborium::value::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssertionEnvelope {
    pub v: u64,
    pub suite: u64,
    pub kid: KeyId,
    pub nonce: Nonce12,
    pub ct: Vec<u8>,
    pub epoch: Option<u64>,
}

impl AssertionEnvelope {
    pub fn new(kid: KeyId, nonce: Nonce12, ct: Vec<u8>) -> Self {
        Self {
            v: ENVELOPE_VERSION,
            suite: SUITE_ID,
            kid,
            nonce,
            ct,
            epoch: None,
        }
    }

    pub fn with_epoch(kid: KeyId, nonce: Nonce12, ct: Vec<u8>, epoch: u64) -> Self {
        Self {
            v: ENVELOPE_VERSION,
            suite: SUITE_ID,
            kid,
            nonce,
            ct,
            epoch: Some(epoch),
        }
    }

    pub fn aad_value(&self) -> Value {
        let mut entries = vec![
            (Value::Text("v".to_string()), Value::Integer(self.v.into())),
            (Value::Text("suite".to_string()), Value::Integer(self.suite.into())),
            (Value::Text("kid".to_string()), Value::Bytes(self.kid.as_bytes().to_vec())),
            (
                Value::Text("nonce".to_string()),
                Value::Bytes(self.nonce.as_bytes().to_vec()),
            ),
        ];
        if let Some(epoch) = self.epoch {
            entries.push((Value::Text("epoch".to_string()), Value::Integer(epoch.into())));
        }
        Value::Map(entries)
    }

    pub fn aad_bytes(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.aad_value())
    }

    pub fn to_value(&self) -> Value {
        let mut entries = vec![
            (Value::Text("v".to_string()), Value::Integer(self.v.into())),
            (Value::Text("suite".to_string()), Value::Integer(self.suite.into())),
            (Value::Text("kid".to_string()), Value::Bytes(self.kid.as_bytes().to_vec())),
            (
                Value::Text("nonce".to_string()),
                Value::Bytes(self.nonce.as_bytes().to_vec()),
            ),
            (Value::Text("ct".to_string()), Value::Bytes(self.ct.clone())),
        ];
        if let Some(epoch) = self.epoch {
            entries.push((Value::Text("epoch".to_string()), Value::Integer(epoch.into())));
        }
        Value::Map(entries)
    }

    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.to_value())
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        let map = expect_map(&value)?;
        let v = expect_uint(map_get(map, "v").ok_or_else(|| DharmaError::Validation("missing v".to_string()))?)?;
        let suite = expect_uint(map_get(map, "suite").ok_or_else(|| DharmaError::Validation("missing suite".to_string()))?)?;
        let kid_bytes = expect_bytes(map_get(map, "kid").ok_or_else(|| DharmaError::Validation("missing kid".to_string()))?)?;
        let nonce_bytes = expect_bytes(map_get(map, "nonce").ok_or_else(|| DharmaError::Validation("missing nonce".to_string()))?)?;
        let ct = expect_bytes(map_get(map, "ct").ok_or_else(|| DharmaError::Validation("missing ct".to_string()))?)?;
        let epoch = match map_get(map, "epoch") {
            Some(val) => Some(expect_uint(val)?),
            None => None,
        };
        Ok(Self {
            v,
            suite,
            kid: KeyId::from_slice(&kid_bytes)?,
            nonce: Nonce12::from_slice(&nonce_bytes)?,
            ct,
            epoch,
        })
    }

    pub fn envelope_id(&self) -> Result<EnvelopeId, DharmaError> {
        Ok(crypto::envelope_id(&self.to_cbor()?))
    }
}

pub fn encrypt_assertion(
    plaintext: &[u8],
    kid: KeyId,
    key: &[u8; 32],
    nonce: Nonce12,
) -> Result<AssertionEnvelope, DharmaError> {
    let envelope = AssertionEnvelope::new(kid, nonce, Vec::new());
    let aad = envelope.aad_bytes()?;
    let ct = crypto::aead_encrypt(key, nonce.as_bytes(), plaintext, &aad)?;
    Ok(AssertionEnvelope::new(kid, nonce, ct))
}

pub fn encrypt_assertion_with_epoch(
    plaintext: &[u8],
    kid: KeyId,
    key: &[u8; 32],
    nonce: Nonce12,
    epoch: u64,
) -> Result<AssertionEnvelope, DharmaError> {
    let envelope = AssertionEnvelope::with_epoch(kid, nonce, Vec::new(), epoch);
    let aad = envelope.aad_bytes()?;
    let ct = crypto::aead_encrypt(key, nonce.as_bytes(), plaintext, &aad)?;
    Ok(AssertionEnvelope::with_epoch(kid, nonce, ct, epoch))
}

pub fn decrypt_assertion(
    envelope: &AssertionEnvelope,
    key: &[u8; 32],
) -> Result<Vec<u8>, DharmaError> {
    let aad = envelope.aad_bytes()?;
    crypto::aead_decrypt(key, envelope.nonce.as_bytes(), &envelope.ct, &aad)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrip() {
        let kid = KeyId::from_bytes([9u8; 32]);
        let nonce = Nonce12::from_bytes([1u8; 12]);
        let envelope = AssertionEnvelope::new(kid, nonce, vec![1, 2, 3]);
        let bytes = envelope.to_cbor().unwrap();
        let parsed = AssertionEnvelope::from_cbor(&bytes).unwrap();
        assert_eq!(envelope, parsed);
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let key = [7u8; 32];
        let kid = KeyId::from_bytes([3u8; 32]);
        let nonce = Nonce12::from_bytes([4u8; 12]);
        let plaintext = b"assertion";
        let envelope = encrypt_assertion(plaintext, kid, &key, nonce).unwrap();
        let decrypted = decrypt_assertion(&envelope, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn decrypt_rejects_wrong_key() {
        let key = [1u8; 32];
        let bad_key = [2u8; 32];
        let kid = KeyId::from_bytes([9u8; 32]);
        let nonce = Nonce12::from_bytes([3u8; 12]);
        let envelope = encrypt_assertion(b"data", kid, &key, nonce).unwrap();
        assert!(decrypt_assertion(&envelope, &bad_key).is_err());
    }

    #[test]
    fn epoch_metadata_roundtrip() {
        let kid = KeyId::from_bytes([5u8; 32]);
        let nonce = Nonce12::from_bytes([7u8; 12]);
        let envelope = AssertionEnvelope::with_epoch(kid, nonce, vec![1, 2, 3], 9);
        let bytes = envelope.to_cbor().unwrap();
        let parsed = AssertionEnvelope::from_cbor(&bytes).unwrap();
        assert_eq!(parsed.epoch, Some(9));
        assert_eq!(parsed.kid, envelope.kid);
    }
}
