use crate::error::DharmaError;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use hkdf::Hkdf;
use rand_core::OsRng;
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

const PROTOCOL_NAME: &[u8] = b"Noise_XX_25519_ChaChaPoly_SHA256";

pub struct HandshakeState {
    s: StaticSecret,
    s_pub: PublicKey,
    e: Option<StaticSecret>,
    re: Option<PublicKey>,
    rs: Option<PublicKey>,
    h: [u8; 32],
    ck: [u8; 32],
    k: Option<[u8; 32]>,
    n: u64,
}

impl HandshakeState {
    pub fn initiator(s: StaticSecret) -> Self {
        HandshakeState::new(s)
    }

    pub fn responder(s: StaticSecret) -> Self {
        HandshakeState::new(s)
    }

    pub fn write_message1(&mut self) -> Vec<u8> {
        let e = StaticSecret::random_from_rng(OsRng);
        let e_pub = PublicKey::from(&e);
        self.e = Some(e);
        self.mix_hash(e_pub.as_bytes());
        e_pub.as_bytes().to_vec()
    }

    pub fn read_message1(&mut self, msg: &[u8]) -> Result<(), DharmaError> {
        let re = PublicKey::from(array_32(msg)?);
        self.re = Some(re);
        self.mix_hash(re.as_bytes());
        Ok(())
    }

    pub fn write_message2(&mut self, payload: &[u8]) -> Result<Vec<u8>, DharmaError> {
        let re = self.re.ok_or_else(|| DharmaError::Validation("missing re".to_string()))?;
        let e = StaticSecret::random_from_rng(OsRng);
        let e_pub = PublicKey::from(&e);
        self.e = Some(e);
        self.mix_hash(e_pub.as_bytes());
        let dh = self.e.as_ref().unwrap().diffie_hellman(&re);
        self.mix_key(dh.as_bytes());
        let s_pub = self.s_pub.as_bytes().to_vec();
        let ct_s = self.encrypt_and_hash(&s_pub)?;
        let dh = self.s.diffie_hellman(&re);
        self.mix_key(dh.as_bytes());
        let ct_payload = self.encrypt_and_hash(payload)?;
        let mut out = Vec::with_capacity(32 + ct_s.len() + ct_payload.len());
        out.extend_from_slice(e_pub.as_bytes());
        out.extend_from_slice(&ct_s);
        out.extend_from_slice(&ct_payload);
        Ok(out)
    }

    pub fn read_message2(&mut self, msg: &[u8]) -> Result<Vec<u8>, DharmaError> {
        if msg.len() < 32 + 48 {
            return Err(DharmaError::Validation("invalid message2".to_string()));
        }
        let re = PublicKey::from(array_32(&msg[..32])?);
        self.re = Some(re);
        self.mix_hash(re.as_bytes());
        let dh = self.e.as_ref().ok_or_else(|| DharmaError::Validation("missing e".to_string()))?.diffie_hellman(&re);
        self.mix_key(dh.as_bytes());
        let ct_s = &msg[32..80];
        let rs_bytes = self.decrypt_and_hash(ct_s)?;
        let rs = PublicKey::from(array_32(&rs_bytes)?);
        self.rs = Some(rs);
        let e = self.e.as_ref().ok_or_else(|| DharmaError::Validation("missing e".to_string()))?;
        let dh = e.diffie_hellman(&rs);
        self.mix_key(dh.as_bytes());
        let ct_payload = &msg[80..];
        let payload = self.decrypt_and_hash(ct_payload)?;
        Ok(payload)
    }

    pub fn write_message3(&mut self, payload: &[u8]) -> Result<Vec<u8>, DharmaError> {
        let re = self.re.ok_or_else(|| DharmaError::Validation("missing re".to_string()))?;
        let rs = self.rs.ok_or_else(|| DharmaError::Validation("missing rs".to_string()))?;
        let s_pub = self.s_pub.as_bytes().to_vec();
        let ct_s = self.encrypt_and_hash(&s_pub)?;
        let dh = self.s.diffie_hellman(&re);
        self.mix_key(dh.as_bytes());
        let dh = self.s.diffie_hellman(&rs);
        self.mix_key(dh.as_bytes());
        let ct_payload = self.encrypt_and_hash(payload)?;
        let mut out = Vec::with_capacity(ct_s.len() + ct_payload.len());
        out.extend_from_slice(&ct_s);
        out.extend_from_slice(&ct_payload);
        Ok(out)
    }

    pub fn read_message3(&mut self, msg: &[u8]) -> Result<Vec<u8>, DharmaError> {
        if msg.len() < 48 {
            return Err(DharmaError::Validation("invalid message3".to_string()));
        }
        let ct_s = &msg[..48];
        let rs_bytes = self.decrypt_and_hash(ct_s)?;
        let rs = PublicKey::from(array_32(&rs_bytes)?);
        self.rs = Some(rs);
        let e = self.e.as_ref().ok_or_else(|| DharmaError::Validation("missing e".to_string()))?;
        let dh = e.diffie_hellman(&rs);
        self.mix_key(dh.as_bytes());
        let dh = self.s.diffie_hellman(&rs);
        self.mix_key(dh.as_bytes());
        let ct_payload = &msg[48..];
        let payload = self.decrypt_and_hash(ct_payload)?;
        Ok(payload)
    }

    pub fn split(&self) -> Result<([u8; 32], [u8; 32]), DharmaError> {
        let hk = Hkdf::<Sha256>::new(Some(&self.ck), &[]);
        let mut okm = [0u8; 64];
        hk.expand(b"", &mut okm)
            .map_err(|_| DharmaError::Validation("hkdf expand failed".to_string()))?;
        let mut k1 = [0u8; 32];
        let mut k2 = [0u8; 32];
        k1.copy_from_slice(&okm[..32]);
        k2.copy_from_slice(&okm[32..64]);
        Ok((k1, k2))
    }

    fn new(s: StaticSecret) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(PROTOCOL_NAME);
        let h = hasher.finalize();
        let mut h_bytes = [0u8; 32];
        h_bytes.copy_from_slice(&h);
        HandshakeState {
            s_pub: PublicKey::from(&s),
            s,
            e: None,
            re: None,
            rs: None,
            h: h_bytes,
            ck: h_bytes,
            k: None,
            n: 0,
        }
    }

    fn mix_hash(&mut self, data: &[u8]) {
        let mut hasher = Sha256::new();
        hasher.update(&self.h);
        hasher.update(data);
        let out = hasher.finalize();
        self.h.copy_from_slice(&out);
    }

    fn mix_key(&mut self, input: &[u8]) {
        let hk = Hkdf::<Sha256>::new(Some(&self.ck), input);
        let mut okm = [0u8; 64];
        hk.expand(b"", &mut okm).expect("hkdf expand");
        self.ck.copy_from_slice(&okm[..32]);
        let mut key = [0u8; 32];
        key.copy_from_slice(&okm[32..64]);
        self.k = Some(key);
        self.n = 0;
    }

    fn encrypt_and_hash(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, DharmaError> {
        if let Some(k) = self.k {
            let nonce = noise_nonce(self.n);
            self.n = self.n.wrapping_add(1);
            let cipher = ChaCha20Poly1305::new(Key::from_slice(&k));
            let ct = cipher.encrypt(
                Nonce::from_slice(&nonce),
                chacha20poly1305::aead::Payload {
                    msg: plaintext,
                    aad: &self.h,
                },
            )?;
            self.mix_hash(&ct);
            Ok(ct)
        } else {
            self.mix_hash(plaintext);
            Ok(plaintext.to_vec())
        }
    }

    fn decrypt_and_hash(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, DharmaError> {
        if let Some(k) = self.k {
            let nonce = noise_nonce(self.n);
            self.n = self.n.wrapping_add(1);
            let cipher = ChaCha20Poly1305::new(Key::from_slice(&k));
            let pt = cipher.decrypt(
                Nonce::from_slice(&nonce),
                chacha20poly1305::aead::Payload {
                    msg: ciphertext,
                    aad: &self.h,
                },
            )?;
            self.mix_hash(ciphertext);
            Ok(pt)
        } else {
            self.mix_hash(ciphertext);
            Ok(ciphertext.to_vec())
        }
    }
}

fn noise_nonce(counter: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(&counter.to_le_bytes());
    nonce
}

fn array_32(bytes: &[u8]) -> Result<[u8; 32], DharmaError> {
    if bytes.len() != 32 {
        return Err(DharmaError::InvalidLength {
            expected: 32,
            actual: bytes.len(),
        });
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(bytes);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xx_roundtrip_payloads_and_keys() {
        let s1 = StaticSecret::from([1u8; 32]);
        let s2 = StaticSecret::from([2u8; 32]);
        let mut init = HandshakeState::initiator(s1);
        let mut resp = HandshakeState::responder(s2);

        let msg1 = init.write_message1();
        resp.read_message1(&msg1).unwrap();

        let msg2 = resp.write_message2(b"server").unwrap();
        let payload2 = init.read_message2(&msg2).unwrap();
        assert_eq!(payload2, b"server");

        let msg3 = init.write_message3(b"client").unwrap();
        let payload3 = resp.read_message3(&msg3).unwrap();
        assert_eq!(payload3, b"client");

        let (i_k1, i_k2) = init.split().unwrap();
        let (r_k1, r_k2) = resp.split().unwrap();
        assert_eq!(i_k1, r_k1);
        assert_eq!(i_k2, r_k2);
    }
}
