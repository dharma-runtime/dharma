use crate::error::DharmaError;
use crate::types::{AssertionId, EnvelopeId, IdentityKey, KeyId};
use chacha20poly1305::{aead::Aead, aead::KeyInit, ChaCha20Poly1305, Key, Nonce};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use rand_core::{CryptoRng, RngCore};
use sha2::{Digest, Sha256};

pub const PROTOCOL_VERSION: u64 = 1;
pub const ENVELOPE_VERSION: u64 = 1;
pub const SUITE_ID: u64 = 1;

pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

pub fn envelope_id(bytes: &[u8]) -> EnvelopeId {
    EnvelopeId::from_bytes(sha256(bytes))
}

pub fn assertion_id(bytes: &[u8]) -> AssertionId {
    AssertionId::from_bytes(sha256(bytes))
}

pub fn key_id_from_key(key: &[u8; 32]) -> KeyId {
    KeyId::from_bytes(sha256(key))
}

pub fn generate_identity_keypair<R: RngCore + CryptoRng>(
    rng: &mut R,
) -> (SigningKey, IdentityKey) {
    let signing_key = SigningKey::generate(rng);
    let verifying_key = signing_key.verifying_key();
    let id = IdentityKey::from_bytes(verifying_key.to_bytes());
    (signing_key, id)
}

pub fn sign(signing_key: &SigningKey, message: &[u8]) -> Vec<u8> {
    signing_key.sign(message).to_bytes().to_vec()
}

pub fn verify(identity_key: &IdentityKey, message: &[u8], sig: &[u8]) -> Result<bool, DharmaError> {
    if sig.len() != 64 {
        return Ok(false);
    }
    let verifying_key = VerifyingKey::from_bytes(identity_key.as_bytes())?;
    let sig_bytes: [u8; 64] = sig
        .try_into()
        .map_err(|_| DharmaError::InvalidLength { expected: 64, actual: sig.len() })?;
    let signature = Signature::from_bytes(&sig_bytes);
    Ok(verifying_key.verify_strict(message, &signature).is_ok())
}

pub fn aead_encrypt(
    key: &[u8; 32],
    nonce: &[u8; 12],
    plaintext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, DharmaError> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    Ok(cipher.encrypt(
        Nonce::from_slice(nonce),
        chacha20poly1305::aead::Payload { msg: plaintext, aad },
    )?)
}

pub fn aead_decrypt(
    key: &[u8; 32],
    nonce: &[u8; 12],
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, DharmaError> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    Ok(cipher.decrypt(
        Nonce::from_slice(nonce),
        chacha20poly1305::aead::Payload { msg: ciphertext, aad },
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn sign_and_verify() {
        let mut rng = StdRng::seed_from_u64(42);
        let (sk, pk) = generate_identity_keypair(&mut rng);
        let msg = b"dharma";
        let sig = sign(&sk, msg);
        assert!(verify(&pk, msg, &sig).unwrap());
        assert!(!verify(&pk, b"other", &sig).unwrap());
    }

    #[test]
    fn aead_roundtrip() {
        let mut rng = StdRng::seed_from_u64(7);
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut nonce);
        let msg = b"secret";
        let aad = b"aad";
        let ct = aead_encrypt(&key, &nonce, msg, aad).unwrap();
        let pt = aead_decrypt(&key, &nonce, &ct, aad).unwrap();
        assert_eq!(pt, msg);
    }

    #[test]
    fn verify_rejects_short_signature() {
        let mut rng = StdRng::seed_from_u64(9);
        let (_sk, pk) = generate_identity_keypair(&mut rng);
        let ok = verify(&pk, b"msg", &[1, 2, 3]).unwrap();
        assert!(!ok);
    }
}
