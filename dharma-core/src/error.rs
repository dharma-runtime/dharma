use thiserror::Error;

#[derive(Debug, Error)]
pub enum DharmaError {
    #[error("invalid length: expected {expected}, got {actual}")]
    InvalidLength { expected: usize, actual: usize },
    #[error("cbor error: {0}")]
    Cbor(String),
    #[error("crypto error: {0}")]
    Crypto(String),
    #[error("kdf error: {0}")]
    Kdf(String),
    #[error("signature error: {0}")]
    Signature(String),
    #[error("decryption failed")]
    DecryptionFailed,
    #[error("missing key for kid")]
    MissingKey,
    #[error("non-canonical cbor")]
    NonCanonicalCbor,
    #[error("schema validation error: {0}")]
    Schema(String),
    #[error("contract error: {0}")]
    Contract(String),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("network error: {0}")]
    Network(String),
    #[error("lock busy")]
    LockBusy,
    #[error("io error: {0}")]
    Io(std::io::Error),
    #[error("config error: {0}")]
    Config(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("dependency cycle detected")]
    DependencyCycle,
}

impl From<ciborium::de::Error<std::io::Error>> for DharmaError {
    fn from(err: ciborium::de::Error<std::io::Error>) -> Self {
        DharmaError::Cbor(err.to_string())
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for DharmaError {
    fn from(err: ciborium::ser::Error<std::io::Error>) -> Self {
        DharmaError::Cbor(err.to_string())
    }
}

impl From<ed25519_dalek::SignatureError> for DharmaError {
    fn from(err: ed25519_dalek::SignatureError) -> Self {
        DharmaError::Signature(err.to_string())
    }
}

impl From<chacha20poly1305::Error> for DharmaError {
    fn from(_: chacha20poly1305::Error) -> Self {
        DharmaError::DecryptionFailed
    }
}

impl From<std::io::Error> for DharmaError {
    fn from(err: std::io::Error) -> Self {
        use std::io::ErrorKind;
        match err.kind() {
            ErrorKind::NotFound => DharmaError::NotFound(err.to_string()),
            ErrorKind::TimedOut
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
            | ErrorKind::NotConnected
            | ErrorKind::AddrInUse
            | ErrorKind::AddrNotAvailable
            | ErrorKind::ConnectionRefused
            | ErrorKind::WouldBlock => DharmaError::Network(err.to_string()),
            _ => DharmaError::Io(err),
        }
    }
}

impl From<argon2::password_hash::Error> for DharmaError {
    fn from(err: argon2::password_hash::Error) -> Self {
        DharmaError::Kdf(err.to_string())
    }
}

impl From<argon2::Error> for DharmaError {
    fn from(err: argon2::Error) -> Self {
        DharmaError::Kdf(err.to_string())
    }
}
