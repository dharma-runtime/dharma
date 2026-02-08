use crate::cbor;
use crate::crypto;
use crate::error::DharmaError;
use crate::types::{HpkePublicKey, IdentityKey, KeyId, Nonce12, SubjectId};
use crate::value::{expect_bytes, expect_map, map_get};
use ciborium::value::Value;
use rand_core::OsRng;
use std::collections::HashMap;
use x25519_dalek::{PublicKey, StaticSecret};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyEnvelope {
    pub epk: HpkePublicKey,
    pub nonce: Nonce12,
    pub ct: Vec<u8>,
}

impl KeyEnvelope {
    pub fn to_value(&self) -> Value {
        Value::Map(vec![
            (
                Value::Text("epk".to_string()),
                Value::Bytes(self.epk.as_bytes().to_vec()),
            ),
            (
                Value::Text("nonce".to_string()),
                Value::Bytes(self.nonce.as_bytes().to_vec()),
            ),
            (Value::Text("ct".to_string()), Value::Bytes(self.ct.clone())),
        ])
    }

    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        cbor::encode_canonical_value(&self.to_value())
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        let map = expect_map(&value)?;
        let epk_bytes = expect_bytes(
            map_get(map, "epk")
                .ok_or_else(|| DharmaError::Validation("missing epk".to_string()))?,
        )?;
        let nonce_bytes = expect_bytes(
            map_get(map, "nonce")
                .ok_or_else(|| DharmaError::Validation("missing nonce".to_string()))?,
        )?;
        let ct = expect_bytes(
            map_get(map, "ct").ok_or_else(|| DharmaError::Validation("missing ct".to_string()))?,
        )?;
        Ok(KeyEnvelope {
            epk: HpkePublicKey::from_slice(&epk_bytes)?,
            nonce: Nonce12::from_slice(&nonce_bytes)?,
            ct,
        })
    }
}

pub fn hpke_public_key_from_secret(secret: &[u8; 32]) -> HpkePublicKey {
    let sk = StaticSecret::from(*secret);
    let pk = PublicKey::from(&sk);
    HpkePublicKey::from_bytes(pk.to_bytes())
}

pub fn hpke_seal(recipient: &HpkePublicKey, plaintext: &[u8]) -> Result<KeyEnvelope, DharmaError> {
    let eph = StaticSecret::random_from_rng(&mut OsRng);
    let epk = PublicKey::from(&eph);
    let recipient_pk = PublicKey::from(*recipient.as_bytes());
    let shared = eph.diffie_hellman(&recipient_pk);
    let key = derive_hpke_key(shared.as_bytes());
    let nonce = derive_hpke_nonce(shared.as_bytes());
    let aad = epk.as_bytes();
    let ct = crypto::aead_encrypt(&key, &nonce, plaintext, aad)?;
    Ok(KeyEnvelope {
        epk: HpkePublicKey::from_bytes(epk.to_bytes()),
        nonce: Nonce12::from_bytes(nonce),
        ct,
    })
}

pub fn hpke_open(secret: &[u8; 32], envelope: &KeyEnvelope) -> Result<Vec<u8>, DharmaError> {
    let sk = StaticSecret::from(*secret);
    let epk = PublicKey::from(*envelope.epk.as_bytes());
    let shared = sk.diffie_hellman(&epk);
    let key = derive_hpke_key(shared.as_bytes());
    let nonce = envelope.nonce.as_bytes();
    let aad = envelope.epk.as_bytes();
    crypto::aead_decrypt(&key, nonce, &envelope.ct, aad)
}

pub fn derive_kek(domain_root: &[u8; 32], epoch: u64) -> [u8; 32] {
    let mut buf = Vec::with_capacity(8 + domain_root.len() + 8);
    buf.extend_from_slice(b"dharma.kek");
    buf.extend_from_slice(domain_root);
    buf.extend_from_slice(&epoch.to_le_bytes());
    crypto::sha256(&buf)
}

pub fn derive_sdk(kek: &[u8; 32], subject: &SubjectId, epoch: u64) -> [u8; 32] {
    let mut buf = Vec::with_capacity(8 + kek.len() + 32 + 8);
    buf.extend_from_slice(b"dharma.sdk");
    buf.extend_from_slice(kek);
    buf.extend_from_slice(subject.as_bytes());
    buf.extend_from_slice(&epoch.to_le_bytes());
    crypto::sha256(&buf)
}

pub fn key_id_for_key(key: &[u8; 32]) -> KeyId {
    crypto::key_id_from_key(key)
}

fn derive_hpke_key(shared: &[u8]) -> [u8; 32] {
    let mut buf = Vec::with_capacity(shared.len() + 12);
    buf.extend_from_slice(b"dharma.hpke.key");
    buf.extend_from_slice(shared);
    crypto::sha256(&buf)
}

fn derive_hpke_nonce(shared: &[u8]) -> [u8; 12] {
    let mut buf = Vec::with_capacity(shared.len() + 14);
    buf.extend_from_slice(b"dharma.hpke.nonce");
    buf.extend_from_slice(shared);
    let hash = crypto::sha256(&buf);
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&hash[..12]);
    nonce
}

#[derive(Clone, Debug, Default)]
pub struct Keyring {
    by_kid: HashMap<KeyId, [u8; 32]>,
    by_subject_epoch: HashMap<(SubjectId, u64), KeyId>,
    domain_roots: HashMap<SubjectId, [u8; 32]>,
    hpke_secrets: HashMap<IdentityKey, [u8; 32]>,
}

impl Keyring {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_subject_keys(keys: &HashMap<SubjectId, [u8; 32]>) -> Self {
        let mut ring = Keyring::new();
        for (subject, key) in keys {
            ring.insert_sdk(*subject, 0, *key);
        }
        ring
    }

    pub fn insert_domain_root(&mut self, domain: SubjectId, key: [u8; 32]) {
        self.domain_roots.insert(domain, key);
    }

    pub fn insert_hpke_secret(&mut self, identity: IdentityKey, secret: [u8; 32]) {
        self.hpke_secrets.insert(identity, secret);
    }

    pub fn insert_sdk(&mut self, subject: SubjectId, epoch: u64, key: [u8; 32]) -> KeyId {
        let kid = key_id_for_key(&key);
        self.by_kid.insert(kid, key);
        self.by_subject_epoch.insert((subject, epoch), kid);
        kid
    }

    pub fn key_for_kid(&self, kid: &KeyId) -> Option<&[u8; 32]> {
        self.by_kid.get(kid)
    }

    pub fn hpke_secret_for(&self, identity: &IdentityKey) -> Option<&[u8; 32]> {
        self.hpke_secrets.get(identity)
    }

    pub fn sdk_for_subject_epoch(&self, subject: &SubjectId, epoch: u64) -> Option<&[u8; 32]> {
        let kid = self.by_subject_epoch.get(&(*subject, epoch))?;
        self.by_kid.get(kid)
    }

    pub fn ensure_sdk_from_domain_root(
        &mut self,
        domain: SubjectId,
        subject: SubjectId,
        epoch: u64,
    ) -> Option<KeyId> {
        if self.by_subject_epoch.contains_key(&(subject, epoch)) {
            return self.by_subject_epoch.get(&(subject, epoch)).copied();
        }
        let drk = self.domain_roots.get(&domain)?;
        let kek = derive_kek(drk, epoch);
        let sdk = derive_sdk(&kek, &subject, epoch);
        Some(self.insert_sdk(subject, epoch, sdk))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_derivation_stable() {
        let drk = [7u8; 32];
        let subject = SubjectId::from_bytes([3u8; 32]);
        let kek1 = derive_kek(&drk, 1);
        let kek2 = derive_kek(&drk, 1);
        assert_eq!(kek1, kek2);
        let kek3 = derive_kek(&drk, 2);
        assert_ne!(kek1, kek3);

        let sdk1 = derive_sdk(&kek1, &subject, 1);
        let sdk2 = derive_sdk(&kek1, &subject, 1);
        assert_eq!(sdk1, sdk2);
        let sdk3 = derive_sdk(&kek1, &subject, 2);
        assert_ne!(sdk1, sdk3);
    }

    #[test]
    fn hpke_roundtrip() {
        let sk = [9u8; 32];
        let pk = hpke_public_key_from_secret(&sk);
        let msg = b"secret";
        let env = hpke_seal(&pk, msg).unwrap();
        let out = hpke_open(&sk, &env).unwrap();
        assert_eq!(out, msg);
    }
}
