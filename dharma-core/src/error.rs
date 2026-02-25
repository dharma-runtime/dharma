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
    #[error("out of fuel")]
    OutOfFuel,
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
    #[error("sqlite error ({code:?}): {message}")]
    Sqlite {
        code: Option<String>,
        message: String,
    },
    #[error("postgres error ({code:?}): {message}")]
    Postgres {
        code: Option<String>,
        message: String,
    },
    #[error("clickhouse error ({code:?}): {message}")]
    ClickHouse {
        code: Option<String>,
        message: String,
    },
    #[error("dependency cycle detected")]
    DependencyCycle,
}

impl Clone for DharmaError {
    fn clone(&self) -> Self {
        match self {
            DharmaError::InvalidLength { expected, actual } => DharmaError::InvalidLength {
                expected: *expected,
                actual: *actual,
            },
            DharmaError::Cbor(message) => DharmaError::Cbor(message.clone()),
            DharmaError::Crypto(message) => DharmaError::Crypto(message.clone()),
            DharmaError::Kdf(message) => DharmaError::Kdf(message.clone()),
            DharmaError::Signature(message) => DharmaError::Signature(message.clone()),
            DharmaError::DecryptionFailed => DharmaError::DecryptionFailed,
            DharmaError::MissingKey => DharmaError::MissingKey,
            DharmaError::NonCanonicalCbor => DharmaError::NonCanonicalCbor,
            DharmaError::Schema(message) => DharmaError::Schema(message.clone()),
            DharmaError::Contract(message) => DharmaError::Contract(message.clone()),
            DharmaError::OutOfFuel => DharmaError::OutOfFuel,
            DharmaError::Validation(message) => DharmaError::Validation(message.clone()),
            DharmaError::Network(message) => DharmaError::Network(message.clone()),
            DharmaError::LockBusy => DharmaError::LockBusy,
            DharmaError::Io(err) => {
                DharmaError::Io(std::io::Error::new(err.kind(), err.to_string()))
            }
            DharmaError::Config(message) => DharmaError::Config(message.clone()),
            DharmaError::NotFound(message) => DharmaError::NotFound(message.clone()),
            DharmaError::Sqlite { code, message } => DharmaError::Sqlite {
                code: code.clone(),
                message: message.clone(),
            },
            DharmaError::Postgres { code, message } => DharmaError::Postgres {
                code: code.clone(),
                message: message.clone(),
            },
            DharmaError::ClickHouse { code, message } => DharmaError::ClickHouse {
                code: code.clone(),
                message: message.clone(),
            },
            DharmaError::DependencyCycle => DharmaError::DependencyCycle,
        }
    }
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

impl From<rusqlite::Error> for DharmaError {
    fn from(err: rusqlite::Error) -> Self {
        match &err {
            rusqlite::Error::SqliteFailure(inner, _) => match inner.code {
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked => {
                    DharmaError::LockBusy
                }
                _ => DharmaError::Sqlite {
                    code: Some(format!("{:?}", inner.code)),
                    message: err.to_string(),
                },
            },
            _ => DharmaError::Sqlite {
                code: None,
                message: err.to_string(),
            },
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

impl From<postgres::Error> for DharmaError {
    fn from(err: postgres::Error) -> Self {
        DharmaError::Postgres {
            code: err.as_db_error().map(|db| db.code().code().to_string()),
            message: err.to_string(),
        }
    }
}

impl From<r2d2::Error> for DharmaError {
    fn from(err: r2d2::Error) -> Self {
        DharmaError::Network(err.to_string())
    }
}

impl From<clickhouse::error::Error> for DharmaError {
    fn from(err: clickhouse::error::Error) -> Self {
        DharmaError::ClickHouse {
            code: None,
            message: err.to_string(),
        }
    }
}
