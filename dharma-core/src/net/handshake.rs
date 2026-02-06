use crate::cbor;
use crate::error::DharmaError;
use crate::identity::IdentityState;
use crate::net::noise::HandshakeState;
use crate::types::{IdentityKey, SubjectId};
use crate::value::{expect_bytes, expect_map, expect_uint, map_get};
use ciborium::value::Value;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use crate::net::io::ReadWrite;
use x25519_dalek::StaticSecret;

const MAGIC: u32 = 0x50414354;
const VERSION: u8 = 1;
const TYPE_HELLO: u8 = 1;
const TYPE_WELCOME: u8 = 2;
const TYPE_AUTH: u8 = 3;

pub struct Session {
    send_key: [u8; 32],
    recv_key: [u8; 32],
    send_counter: u64,
    recv_counter: u64,
}

pub struct PeerAuth {
    pub subject: SubjectId,
    pub public_key: IdentityKey,
    pub verified: bool,
}

impl Session {
    pub fn new(send_key: [u8; 32], recv_key: [u8; 32]) -> Self {
        Self {
            send_key,
            recv_key,
            send_counter: 0,
            recv_counter: 0,
        }
    }

    pub fn encrypt(&mut self, t: u8, payload: &[u8]) -> Result<Vec<u8>, DharmaError> {
        let nonce = next_nonce(self.send_counter);
        let next_counter = self
            .send_counter
            .checked_add(1)
            .ok_or_else(|| DharmaError::Crypto("nonce counter overflow".to_string()))?;
        let aad = build_aad(t);
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&self.send_key));
        let ct = cipher.encrypt(
            Nonce::from_slice(&nonce),
            chacha20poly1305::aead::Payload {
                msg: payload,
                aad: &aad,
            },
        )?;
        self.send_counter = next_counter;
        let frame = Value::Map(vec![
            (Value::Text("magic".to_string()), Value::Integer(MAGIC.into())),
            (Value::Text("v".to_string()), Value::Integer(VERSION.into())),
            (Value::Text("t".to_string()), Value::Integer(t.into())),
            (Value::Text("n".to_string()), Value::Bytes(nonce.to_vec())),
            (Value::Text("ct".to_string()), Value::Bytes(ct)),
        ]);
        cbor::encode_canonical_value(&frame)
    }

    pub fn decrypt(&mut self, bytes: &[u8]) -> Result<(u8, Vec<u8>), DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        let map = expect_map(&value)?;
        let magic = expect_uint(
            map_get(map, "magic").ok_or_else(|| DharmaError::Validation("missing magic".to_string()))?,
        )?;
        let version =
            expect_uint(map_get(map, "v").ok_or_else(|| DharmaError::Validation("missing v".to_string()))?)?;
        let t =
            expect_uint(map_get(map, "t").ok_or_else(|| DharmaError::Validation("missing t".to_string()))?)?
                as u8;
        if magic != MAGIC as u64 || version != VERSION as u64 {
            return Err(DharmaError::Validation("invalid frame".to_string()));
        }
        let nonce = expect_bytes(map_get(map, "n").ok_or_else(|| DharmaError::Validation("missing n".to_string()))?)?;
        let ct = expect_bytes(map_get(map, "ct").ok_or_else(|| DharmaError::Validation("missing ct".to_string()))?)?;
        let aad = build_aad(t);
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&self.recv_key));
        let next_counter = self
            .recv_counter
            .checked_add(1)
            .ok_or_else(|| DharmaError::Crypto("nonce counter overflow".to_string()))?;
        let pt = cipher.decrypt(
            Nonce::from_slice(&nonce),
            chacha20poly1305::aead::Payload { msg: &ct, aad: &aad },
        )?;
        self.recv_counter = next_counter;
        Ok((t, pt))
    }
}

pub fn client_handshake(
    stream: &mut dyn ReadWrite,
    identity: &IdentityState,
) -> Result<Session, DharmaError> {
    let mut noise = HandshakeState::initiator(StaticSecret::from(identity.noise_sk));
    let msg1 = noise.write_message1();
    let hello = encode_plain_frame(TYPE_HELLO, &msg1)?;
    crate::net::codec::write_frame(stream, &hello)?;

    let welcome_bytes = crate::net::codec::read_frame(stream)?;
    let welcome = decode_plain_frame(&welcome_bytes)?;
    if welcome.t != TYPE_WELCOME {
        return Err(DharmaError::Validation("expected welcome".to_string()));
    }
    let payload = noise.read_message2(&welcome.payload)?;
    let _ = parse_identity_payload(&payload)?;

    let auth_payload = identity_payload(identity)?;
    let msg3 = noise.write_message3(&auth_payload)?;
    let auth_frame = encode_plain_frame(TYPE_AUTH, &msg3)?;
    crate::net::codec::write_frame(stream, &auth_frame)?;

    let (send_key, recv_key) = noise.split()?;
    Ok(Session::new(send_key, recv_key))
}

pub fn server_handshake(
    stream: &mut dyn ReadWrite,
    identity: &IdentityState,
) -> Result<(Session, PeerAuth), DharmaError> {
    let mut noise = HandshakeState::responder(StaticSecret::from(identity.noise_sk));
    let hello_bytes = crate::net::codec::read_frame(stream)?;
    let hello = decode_plain_frame(&hello_bytes)?;
    if hello.t != TYPE_HELLO {
        return Err(DharmaError::Validation("expected hello".to_string()));
    }
    noise.read_message1(&hello.payload)?;

    let welcome_payload = identity_payload(identity)?;
    let msg2 = noise.write_message2(&welcome_payload)?;
    let welcome = encode_plain_frame(TYPE_WELCOME, &msg2)?;
    crate::net::codec::write_frame(stream, &welcome)?;

    let auth_bytes = crate::net::codec::read_frame(stream)?;
    let auth = decode_plain_frame(&auth_bytes)?;
    if auth.t != TYPE_AUTH {
        return Err(DharmaError::Validation("expected auth".to_string()));
    }
    let payload = noise.read_message3(&auth.payload)?;
    let (subject, public_key) = parse_identity_payload(&payload)?;

    let (recv_key, send_key) = noise.split()?;
    Ok((
        Session::new(send_key, recv_key),
        PeerAuth {
            subject,
            public_key,
            verified: false,
        },
    ))
}

fn encode_plain_frame(t: u8, payload: &[u8]) -> Result<Vec<u8>, DharmaError> {
    let entries = vec![
        (Value::Text("magic".to_string()), Value::Integer(MAGIC.into())),
        (Value::Text("v".to_string()), Value::Integer(VERSION.into())),
        (Value::Text("t".to_string()), Value::Integer(t.into())),
        (Value::Text("p".to_string()), Value::Bytes(payload.to_vec())),
    ];
    cbor::encode_canonical_value(&Value::Map(entries))
}

fn decode_plain_frame(bytes: &[u8]) -> Result<PlainFrame, DharmaError> {
    let value = cbor::ensure_canonical(bytes)?;
    let map = expect_map(&value)?;
    let magic =
        expect_uint(map_get(map, "magic").ok_or_else(|| DharmaError::Validation("missing magic".to_string()))?)?;
    let version =
        expect_uint(map_get(map, "v").ok_or_else(|| DharmaError::Validation("missing v".to_string()))?)?;
    if magic != MAGIC as u64 || version != VERSION as u64 {
        return Err(DharmaError::Validation("invalid frame".to_string()));
    }
    let t = expect_uint(map_get(map, "t").ok_or_else(|| DharmaError::Validation("missing t".to_string()))?)? as u8;
    let payload = expect_bytes(map_get(map, "p").ok_or_else(|| DharmaError::Validation("missing payload".to_string()))?)?;
    Ok(PlainFrame { t, payload })
}

#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn fuzz_decode_plain_frame(bytes: &[u8]) -> Result<(), DharmaError> {
    decode_plain_frame(bytes).map(|_| ())
}

struct PlainFrame {
    t: u8,
    payload: Vec<u8>,
}

fn identity_payload(identity: &IdentityState) -> Result<Vec<u8>, DharmaError> {
    let payload = Value::Map(vec![
        (
            Value::Text("identity_sub".to_string()),
            Value::Bytes(identity.subject_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("peer_pk".to_string()),
            Value::Bytes(identity.public_key.as_bytes().to_vec()),
        ),
    ]);
    cbor::encode_canonical_value(&payload)
}

fn parse_identity_payload(payload: &[u8]) -> Result<(SubjectId, IdentityKey), DharmaError> {
    let value = cbor::ensure_canonical(payload)?;
    let map = expect_map(&value)?;
    let identity_sub = expect_bytes(
        map_get(map, "identity_sub").ok_or_else(|| DharmaError::Validation("missing identity_sub".to_string()))?,
    )?;
    let peer_pk = expect_bytes(
        map_get(map, "peer_pk").ok_or_else(|| DharmaError::Validation("missing peer_pk".to_string()))?,
    )?;
    Ok((
        SubjectId::from_slice(&identity_sub)?,
        IdentityKey::from_slice(&peer_pk)?,
    ))
}

fn build_aad(t: u8) -> Vec<u8> {
    let mut aad = Vec::with_capacity(6);
    aad.extend_from_slice(&MAGIC.to_be_bytes());
    aad.push(VERSION);
    aad.push(t);
    aad
}

fn next_nonce(counter: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(&counter.to_le_bytes());
    nonce
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cbor;
    use crate::types::{ContractId, SchemaId};
    use ciborium::value::Value;
    use std::io::{Cursor, Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn make_identity(seed: u8) -> IdentityState {
        let sk_bytes = [seed; 32];
        let device_signing_key = ed25519_dalek::SigningKey::from_bytes(&sk_bytes);
        let public_key = IdentityKey::from_bytes(device_signing_key.verifying_key().to_bytes());
        let root_bytes = [seed.wrapping_add(8); 32];
        let root_signing_key = ed25519_dalek::SigningKey::from_bytes(&root_bytes);
        let root_public_key = IdentityKey::from_bytes(root_signing_key.verifying_key().to_bytes());
        IdentityState {
            subject_id: SubjectId::from_bytes([seed; 32]),
            signing_key: device_signing_key,
            public_key,
            root_signing_key,
            root_public_key,
            subject_key: [seed.wrapping_add(1); 32],
            noise_sk: [seed.wrapping_add(2); 32],
            schema: SchemaId::from_bytes([seed.wrapping_add(3); 32]),
            contract: ContractId::from_bytes([seed.wrapping_add(4); 32]),
        }
    }

    struct ScriptedStream {
        read: Cursor<Vec<u8>>,
        written: Vec<u8>,
    }

    impl ScriptedStream {
        fn new(read_bytes: Vec<u8>) -> Self {
            Self {
                read: Cursor::new(read_bytes),
                written: Vec::new(),
            }
        }
    }

    impl Read for ScriptedStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.read.read(buf)
        }
    }

    impl Write for ScriptedStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.written.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn framed(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        crate::net::codec::write_frame(&mut out, bytes).unwrap();
        out
    }

    #[test]
    fn plain_frame_roundtrip() {
        let bytes = encode_plain_frame(TYPE_HELLO, &[1, 2, 3]).unwrap();
        let frame = decode_plain_frame(&bytes).unwrap();
        assert_eq!(frame.t, TYPE_HELLO);
        assert_eq!(frame.payload, vec![1, 2, 3]);
    }

    #[test]
    fn decode_plain_frame_rejects_malformed_cbor() {
        let err = match decode_plain_frame(&[0xff]) {
            Ok(_) => panic!("expected malformed cbor to be rejected"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            DharmaError::Cbor(_) | DharmaError::Validation(_) | DharmaError::NonCanonicalCbor
        ));
    }

    #[test]
    fn noise_handshake_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client_id = make_identity(7);
        let server_id = make_identity(9);
        let expected_client = client_id.subject_id;

        let server = thread::spawn(move || {
            let (mut server_stream, _) = listener.accept().unwrap();
            let (mut session, peer) = server_handshake(&mut server_stream, &server_id).unwrap();
            assert_eq!(peer.subject, expected_client);
            assert!(!peer.verified);
            let frame = crate::net::codec::read_frame(&mut server_stream).unwrap();
            let (_t, payload) = session.decrypt(&frame).unwrap();
            assert_eq!(payload, b"ping");
            let reply = session.encrypt(42, b"pong").unwrap();
            crate::net::codec::write_frame(&mut server_stream, &reply).unwrap();
        });

        let mut client_stream = std::net::TcpStream::connect(addr).unwrap();
        let mut client_session = client_handshake(&mut client_stream, &client_id).unwrap();
        let msg = client_session.encrypt(42, b"ping").unwrap();
        crate::net::codec::write_frame(&mut client_stream, &msg).unwrap();
        let frame = crate::net::codec::read_frame(&mut client_stream).unwrap();
        let (_t, payload) = client_session.decrypt(&frame).unwrap();
        assert_eq!(payload, b"pong");

        server.join().unwrap();
    }

    #[test]
    fn server_handshake_rejects_invalid_message_order() {
        let frame = encode_plain_frame(TYPE_AUTH, &[1, 2, 3]).unwrap();
        let mut stream = ScriptedStream::new(framed(&frame));
        let identity = make_identity(12);
        let err = match server_handshake(&mut stream, &identity) {
            Ok(_) => panic!("expected server handshake to reject auth-first message"),
            Err(err) => err,
        };
        match err {
            DharmaError::Validation(msg) => assert!(msg.contains("expected hello")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn client_handshake_rejects_invalid_message_order() {
        let frame = encode_plain_frame(TYPE_AUTH, &[9, 9, 9]).unwrap();
        let mut stream = ScriptedStream::new(framed(&frame));
        let identity = make_identity(13);
        let err = match client_handshake(&mut stream, &identity) {
            Ok(_) => panic!("expected client handshake to reject auth-as-welcome"),
            Err(err) => err,
        };
        match err {
            DharmaError::Validation(msg) => assert!(msg.contains("expected welcome")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_identity_payload_rejects_crafted_lengths() {
        let payload = Value::Map(vec![
            (
                Value::Text("identity_sub".to_string()),
                Value::Bytes(vec![1u8; 31]),
            ),
            (
                Value::Text("peer_pk".to_string()),
                Value::Bytes(vec![2u8; 32]),
            ),
        ]);
        let bytes = cbor::encode_canonical_value(&payload).unwrap();
        let err = parse_identity_payload(&bytes).unwrap_err();
        assert!(matches!(
            err,
            DharmaError::InvalidLength {
                expected: 32,
                actual: 31
            }
        ));
    }

    #[test]
    fn nonce_counter_overflow_is_rejected() {
        let mut session = Session {
            send_key: [0u8; 32],
            recv_key: [0u8; 32],
            send_counter: u64::MAX,
            recv_counter: 0,
        };
        let err = session.encrypt(1, b"overflow").unwrap_err();
        match err {
            DharmaError::Crypto(msg) => assert!(msg.contains("nonce counter overflow")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn recv_counter_overflow_is_rejected() {
        let key = [7u8; 32];
        let mut sender = Session::new(key, key);
        let mut receiver = Session {
            send_key: key,
            recv_key: key,
            send_counter: 0,
            recv_counter: u64::MAX,
        };
        let frame = sender.encrypt(1, b"ping").unwrap();
        let err = receiver.decrypt(&frame).unwrap_err();
        match err {
            DharmaError::Crypto(msg) => assert!(msg.contains("nonce counter overflow")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn next_nonce_uses_little_endian_counter_encoding() {
        let counter = 0x0102_0304_0506_0708u64;
        let nonce = next_nonce(counter);
        assert_eq!(nonce[..4], [0u8; 4]);
        assert_eq!(nonce[4..], counter.to_le_bytes());
    }
}
