#![deny(unsafe_code)]
#![deny(clippy::float_arithmetic)]

pub mod assertion;
pub mod assertion_types;
pub mod builtins;
pub mod cbor;
pub mod contract;
pub mod config;
pub mod contacts;
pub mod crypto;
pub mod backup;
pub mod dhlq;
pub mod dhlp;
pub mod domain;
pub mod envelope;
pub mod env;
pub mod error;
pub mod fabric;
pub mod identity;
pub mod identity_store;
pub mod keystore;
pub mod lock;
pub mod keys;
pub mod metrics;
pub mod net;
pub mod ownership;
pub mod pdl;
pub mod protocols;
pub mod relay;
pub mod runtime;
pub mod reactor;
pub mod schema;
pub mod share;
pub mod store;
pub mod sync;
pub mod types;
pub mod validation;
pub mod value;
pub mod vault;
#[cfg(feature = "dharmaq")]
pub mod dharmaq;

pub use assertion::{AssertionHeader, AssertionPlaintext};
pub use contract::{ContractEngine, ContractResult, ContractStatus};
pub use config::Config;
pub use envelope::AssertionEnvelope;
pub use error::DharmaError;
pub use identity::IdentityState;
pub use keystore::{decrypt_key, encrypt_key, KeystoreData};
pub use schema::{SchemaManifest, SchemaType, TypeDesc};
pub use store::Store;
pub use store::index::FrontierIndex;
pub use sync::{ErrMsg, Get, Hello, Inventory, Obj, ObjectRef, SubjectInventory, SyncMessage};
pub use types::{
    AssertionId, ContractId, EnvelopeId, HpkePublicKey, IdentityKey, KeyId, Nonce12, Nonce24,
    SchemaId, SubjectId,
};
