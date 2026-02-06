#![deny(unsafe_code)]
#![deny(clippy::float_arithmetic)]

pub mod assertion;
pub mod backup;
pub mod builtins;
pub mod cbor;
pub mod config;
pub mod contacts;
pub mod contract;
pub mod crypto;
#[cfg(feature = "dharmaq")]
pub mod dharmaq;
pub mod dhlp;
pub mod dhlq;
pub mod domain;
pub mod env;
pub mod envelope;
pub mod error;
pub mod fabric;
pub mod identity;
pub mod identity_store;
pub mod keys;
pub mod keystore;
pub mod lock;
pub mod metrics;
pub mod net;
pub mod ownership;
pub mod pdl;
pub mod protocols;
pub mod reactor;
pub mod relay;
pub mod runtime;
pub mod schema;
pub mod share;
pub mod store;
pub mod sync;
pub mod types;
pub mod validation;
pub mod value;
pub mod vault;

pub use assertion::{AssertionHeader, AssertionPlaintext};
pub use config::Config;
pub use contract::{ContractEngine, ContractResult, ContractStatus};
pub use envelope::AssertionEnvelope;
pub use error::DharmaError;
pub use identity::IdentityState;
pub use keystore::{decrypt_key, encrypt_key, KeystoreData};
pub use schema::{SchemaManifest, SchemaType, TypeDesc};
pub use store::index::FrontierIndex;
pub use store::Store;
pub use sync::{ErrMsg, Get, Hello, Inventory, Obj, ObjectRef, SubjectInventory, SyncMessage};
pub use types::{
    AssertionId, ContractId, EnvelopeId, HpkePublicKey, IdentityKey, KeyId, Nonce12, Nonce24,
    SchemaId, SubjectId,
};
