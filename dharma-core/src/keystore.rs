use crate::cbor;
use crate::error::DharmaError;
use crate::types::{ContractId, IdentityKey, SchemaId, SubjectId};
use argon2::{Argon2, Params, Version};
use ciborium::value::Value;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use ed25519_dalek::SigningKey;
use rand_core::OsRng;
use rand_core::RngCore;

#[derive(Clone, Debug, PartialEq)]
pub struct KeystoreData {
    pub root_signing_key: SigningKey,
    pub device_signing_key: SigningKey,
    pub identity: SubjectId,
    pub subject_key: [u8; 32],
    pub noise_sk: [u8; 32],
    pub schema: SchemaId,
    pub contract: ContractId,
}

pub fn encrypt_key(data: &KeystoreData, passphrase: &str) -> Result<Vec<u8>, DharmaError> {
    let mut salt = [0u8; 16];
    let mut nonce = [0u8; 12];
    OsRng.fill_bytes(&mut salt);
    OsRng.fill_bytes(&mut nonce);

    let key = derive_key(passphrase, &salt)?;
    let plaintext = plaintext_value(data);
    let plaintext_bytes = cbor::encode_canonical_value(&plaintext)?;

    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
    let ct = cipher.encrypt(Nonce::from_slice(&nonce), plaintext_bytes.as_slice())?;

    let outer = Value::Map(vec![
        (Value::Text("v".to_string()), Value::Integer(1.into())),
        (Value::Text("kdf".to_string()), Value::Text("argon2id".to_string())),
        (Value::Text("salt".to_string()), Value::Bytes(salt.to_vec())),
        (Value::Text("nonce".to_string()), Value::Bytes(nonce.to_vec())),
        (Value::Text("ct".to_string()), Value::Bytes(ct)),
    ]);

    cbor::encode_canonical_value(&outer)
}

pub fn decrypt_key(blob: &[u8], passphrase: &str) -> Result<KeystoreData, DharmaError> {
    let value = cbor::ensure_canonical(blob)?;
    let map = crate::value::expect_map(&value)?;
    let salt = crate::value::expect_bytes(
        crate::value::map_get(map, "salt").ok_or_else(|| DharmaError::Validation("missing salt".to_string()))?,
    )?;
    let nonce = crate::value::expect_bytes(
        crate::value::map_get(map, "nonce").ok_or_else(|| DharmaError::Validation("missing nonce".to_string()))?,
    )?;
    let ct = crate::value::expect_bytes(
        crate::value::map_get(map, "ct").ok_or_else(|| DharmaError::Validation("missing ct".to_string()))?,
    )?;

    let key = derive_key(passphrase, &salt)?;
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
    let plaintext = cipher.decrypt(Nonce::from_slice(&nonce), ct.as_slice())?;
    let inner = cbor::ensure_canonical(&plaintext)?;
    parse_plaintext(&inner)
}

fn derive_key(passphrase: &str, salt: &[u8]) -> Result<[u8; 32], DharmaError> {
    let params = Params::new(1024, 3, 1, Some(32)).map_err(|e| DharmaError::Kdf(e.to_string()))?;
    let argon2 = Argon2::new(argon2::Algorithm::Argon2id, Version::V0x13, params);
    let mut key = [0u8; 32];
    argon2.hash_password_into(passphrase.as_bytes(), salt, &mut key)?;
    Ok(key)
}

fn plaintext_value(data: &KeystoreData) -> Value {
    Value::Map(vec![
        (Value::Text("v".to_string()), Value::Integer(2.into())),
        (
            Value::Text("root_sk".to_string()),
            Value::Bytes(data.root_signing_key.to_bytes().to_vec()),
        ),
        (
            Value::Text("device_sk".to_string()),
            Value::Bytes(data.device_signing_key.to_bytes().to_vec()),
        ),
        (
            Value::Text("noise_sk".to_string()),
            Value::Bytes(data.noise_sk.to_vec()),
        ),
        (
            Value::Text("identity".to_string()),
            Value::Bytes(data.identity.as_bytes().to_vec()),
        ),
        (
            Value::Text("subject_key".to_string()),
            Value::Bytes(data.subject_key.to_vec()),
        ),
        (
            Value::Text("schema".to_string()),
            Value::Bytes(data.schema.as_bytes().to_vec()),
        ),
        (
            Value::Text("contract".to_string()),
            Value::Bytes(data.contract.as_bytes().to_vec()),
        ),
    ])
}

fn parse_plaintext(value: &Value) -> Result<KeystoreData, DharmaError> {
    let map = crate::value::expect_map(value)?;
    let root_bytes = crate::value::map_get(map, "root_sk")
        .or_else(|| crate::value::map_get(map, "sk"));
    let device_bytes = crate::value::map_get(map, "device_sk")
        .or_else(|| crate::value::map_get(map, "sk"));
    let root_bytes = crate::value::expect_bytes(
        root_bytes.ok_or_else(|| DharmaError::Validation("missing root_sk".to_string()))?,
    )?;
    let device_bytes = crate::value::expect_bytes(
        device_bytes.ok_or_else(|| DharmaError::Validation("missing device_sk".to_string()))?,
    )?;
    let noise_bytes = crate::value::map_get(map, "noise_sk")
        .map(|v| crate::value::expect_bytes(v))
        .transpose()?;
    let identity_bytes = crate::value::expect_bytes(
        crate::value::map_get(map, "identity").ok_or_else(|| DharmaError::Validation("missing identity".to_string()))?,
    )?;
    let subject_key = crate::value::expect_bytes(
        crate::value::map_get(map, "subject_key").ok_or_else(|| DharmaError::Validation("missing subject_key".to_string()))?,
    )?;
    let schema_bytes = crate::value::expect_bytes(
        crate::value::map_get(map, "schema").ok_or_else(|| DharmaError::Validation("missing schema".to_string()))?,
    )?;
    let contract_bytes = crate::value::expect_bytes(
        crate::value::map_get(map, "contract").ok_or_else(|| DharmaError::Validation("missing contract".to_string()))?,
    )?;

    if root_bytes.len() != 32 || device_bytes.len() != 32 || identity_bytes.len() != 32 || subject_key.len() != 32 {
        return Err(DharmaError::Validation("invalid key lengths".to_string()));
    }
    let mut root_sk = [0u8; 32];
    root_sk.copy_from_slice(&root_bytes);
    let mut device_sk = [0u8; 32];
    device_sk.copy_from_slice(&device_bytes);
    let noise_sk = match noise_bytes {
        Some(bytes) => {
            if bytes.len() != 32 {
                return Err(DharmaError::Validation("invalid noise key length".to_string()));
            }
            let mut out = [0u8; 32];
            out.copy_from_slice(&bytes);
            out
        }
        None => derive_noise_sk(&device_sk),
    };
    let mut subject_key_arr = [0u8; 32];
    subject_key_arr.copy_from_slice(&subject_key);

    let root_signing_key = SigningKey::from_bytes(&root_sk);
    let device_signing_key = SigningKey::from_bytes(&device_sk);
    let _ = IdentityKey::from_slice(&root_signing_key.verifying_key().to_bytes())?;
    let _ = IdentityKey::from_slice(&device_signing_key.verifying_key().to_bytes())?;

    Ok(KeystoreData {
        root_signing_key,
        device_signing_key,
        identity: SubjectId::from_slice(&identity_bytes)?,
        subject_key: subject_key_arr,
        noise_sk,
        schema: SchemaId::from_slice(&schema_bytes)?,
        contract: ContractId::from_slice(&contract_bytes)?,
    })
}

fn derive_noise_sk(sk: &[u8; 32]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"noise_sk");
    hasher.update(sk);
    let out = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&out);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto;
    use rand_core::RngCore;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let mut subject_key = [0u8; 32];
        OsRng.fill_bytes(&mut subject_key);
        let (root_sk, _id) = crypto::generate_identity_keypair(&mut OsRng);
        let (device_sk, _id2) = crypto::generate_identity_keypair(&mut OsRng);
        let data = KeystoreData {
            root_signing_key: root_sk,
            device_signing_key: device_sk,
            identity: SubjectId::from_bytes([1u8; 32]),
            subject_key,
            noise_sk: [9u8; 32],
            schema: SchemaId::from_bytes([2u8; 32]),
            contract: ContractId::from_bytes([3u8; 32]),
        };
        let blob = encrypt_key(&data, "pass").unwrap();
        let out = decrypt_key(&blob, "pass").unwrap();
        assert_eq!(out.identity.as_bytes(), data.identity.as_bytes());
        assert_eq!(out.subject_key, data.subject_key);
        assert_eq!(out.noise_sk, data.noise_sk);
        assert_eq!(
            out.root_signing_key.verifying_key().to_bytes(),
            data.root_signing_key.verifying_key().to_bytes()
        );
        assert_eq!(
            out.device_signing_key.verifying_key().to_bytes(),
            data.device_signing_key.verifying_key().to_bytes()
        );
    }
}
