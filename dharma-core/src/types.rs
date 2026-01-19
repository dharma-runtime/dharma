use crate::error::DharmaError;
use rand_core::{CryptoRng, RngCore};

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Bytes32(pub(crate) [u8; 32]);

impl Bytes32 {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self, DharmaError> {
        if slice.len() != 32 {
            return Err(DharmaError::InvalidLength {
                expected: 32,
                actual: slice.len(),
            });
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(slice);
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn random<R: RngCore + CryptoRng>(rng: &mut R) -> Self {
        let mut bytes = [0u8; 32];
        rng.fill_bytes(&mut bytes);
        Self(bytes)
    }

    pub fn to_hex(&self) -> String {
        hex_encode(self.0)
    }
}

impl std::fmt::Debug for Bytes32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex_encode(self.0))
    }
}

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub(crate) Bytes32);

        impl $name {
            pub fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(Bytes32::from_bytes(bytes))
            }

            pub fn from_slice(slice: &[u8]) -> Result<Self, DharmaError> {
                Ok(Self(Bytes32::from_slice(slice)?))
            }

            pub fn as_bytes(&self) -> &[u8; 32] {
                self.0.as_bytes()
            }

            pub fn random<R: RngCore + CryptoRng>(rng: &mut R) -> Self {
                Self(Bytes32::random(rng))
            }

            pub fn to_hex(&self) -> String {
                self.0.to_hex()
            }

            pub fn from_hex(hex: &str) -> Result<Self, DharmaError> {
                let bytes = hex_decode(hex)?;
                if bytes.len() != 32 {
                    return Err(DharmaError::InvalidLength {
                        expected: 32,
                        actual: bytes.len(),
                    });
                }
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                Ok(Self(Bytes32::from_bytes(arr)))
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.to_hex())
            }
        }
    };
}

id_type!(SubjectId);
id_type!(EnvelopeId);
id_type!(AssertionId);
id_type!(KeyId);
id_type!(IdentityKey);
id_type!(SchemaId);
id_type!(ContractId);
id_type!(HpkePublicKey);

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Nonce12(pub(crate) [u8; 12]);

impl Nonce12 {
    pub fn from_bytes(bytes: [u8; 12]) -> Self {
        Self(bytes)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self, DharmaError> {
        if slice.len() != 12 {
            return Err(DharmaError::InvalidLength {
                expected: 12,
                actual: slice.len(),
            });
        }
        let mut bytes = [0u8; 12];
        bytes.copy_from_slice(slice);
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 12] {
        &self.0
    }

    pub fn random<R: RngCore + CryptoRng>(rng: &mut R) -> Self {
        let mut bytes = [0u8; 12];
        rng.fill_bytes(&mut bytes);
        Self(bytes)
    }
}

impl std::fmt::Debug for Nonce12 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex_encode(self.0))
    }
}

pub fn hex_encode<const N: usize>(bytes: [u8; N]) -> String {
    let mut out = String::with_capacity(N * 2);
    for b in bytes.iter() {
        let hi = b >> 4;
        let lo = b & 0x0f;
        out.push(nibble_to_hex(hi));
        out.push(nibble_to_hex(lo));
    }
    out
}

pub fn hex_decode(hex: &str) -> Result<Vec<u8>, DharmaError> {
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let chars: Vec<char> = hex.chars().collect();
    if chars.len() % 2 != 0 {
        return Err(DharmaError::InvalidLength {
            expected: chars.len() + 1,
            actual: chars.len(),
        });
    }
    let mut i = 0;
    while i < chars.len() {
        let hi = hex_value(chars[i])?;
        let lo = hex_value(chars[i + 1])?;
        bytes.push((hi << 4) | lo);
        i += 2;
    }
    Ok(bytes)
}

fn nibble_to_hex(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + (n - 10)) as char,
        _ => '0',
    }
}

fn hex_value(c: char) -> Result<u8, DharmaError> {
    match c {
        '0'..='9' => Ok((c as u8) - b'0'),
        'a'..='f' => Ok((c as u8) - b'a' + 10),
        'A'..='F' => Ok((c as u8) - b'A' + 10),
        _ => Err(DharmaError::Validation("invalid hex".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x12;
        bytes[31] = 0xfe;
        let subject = SubjectId::from_bytes(bytes);
        let hex = subject.to_hex();
        let parsed = SubjectId::from_hex(&hex).unwrap();
        assert_eq!(subject.as_bytes(), parsed.as_bytes());
    }

    #[test]
    fn hex_decode_rejects_odd_len() {
        let err = hex_decode("abc").unwrap_err();
        match err {
            DharmaError::InvalidLength { .. } => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn nonce_length_validation() {
        let err = Nonce12::from_slice(&[0u8; 11]).unwrap_err();
        match err {
            DharmaError::InvalidLength { expected, actual } => {
                assert_eq!(12, expected);
                assert_eq!(11, actual);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn hex_decode_rejects_invalid_char() {
        let err = hex_decode("zz").unwrap_err();
        match err {
            DharmaError::Validation(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
