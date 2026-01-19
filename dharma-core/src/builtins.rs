use crate::contract;
use crate::crypto;
use crate::error::DharmaError;
use crate::schema::{SchemaManifest, SchemaType, TypeDesc};
use crate::store::Store;
use crate::types::{ContractId, EnvelopeId, SchemaId};
use std::collections::{BTreeMap, BTreeSet};

pub fn note_schema() -> SchemaManifest {
    let mut types = BTreeMap::new();
    let mut body = BTreeMap::new();
    body.insert("text".to_string(), TypeDesc::Text);
    let mut required = BTreeSet::new();
    required.insert("text".to_string());
    types.insert(
        "note.text".to_string(),
        SchemaType {
            body,
            required,
            allow_extra: false,
        },
    );
    types.insert(
        "core.genesis".to_string(),
        SchemaType {
            body: BTreeMap::new(),
            required: BTreeSet::new(),
            allow_extra: true,
        },
    );
    let mut profile_body = BTreeMap::new();
    profile_body.insert("alias".to_string(), TypeDesc::Text);
    let mut profile_required = BTreeSet::new();
    profile_required.insert("alias".to_string());
    types.insert(
        "identity.profile".to_string(),
        SchemaType {
            body: profile_body,
            required: profile_required,
            allow_extra: true,
        },
    );
    let mut delegate_body = BTreeMap::new();
    delegate_body.insert("delegate".to_string(), TypeDesc::PubKey32);
    delegate_body.insert("scope".to_string(), TypeDesc::Text);
    delegate_body.insert("expires".to_string(), TypeDesc::Int);
    let mut delegate_required = BTreeSet::new();
    delegate_required.insert("delegate".to_string());
    delegate_required.insert("scope".to_string());
    types.insert(
        "iam.delegate".to_string(),
        SchemaType {
            body: delegate_body,
            required: delegate_required,
            allow_extra: true,
        },
    );
    let mut revoke_body = BTreeMap::new();
    revoke_body.insert("delegate".to_string(), TypeDesc::PubKey32);
    let mut revoke_required = BTreeSet::new();
    revoke_required.insert("delegate".to_string());
    types.insert(
        "iam.revoke".to_string(),
        SchemaType {
            body: revoke_body.clone(),
            required: revoke_required.clone(),
            allow_extra: true,
        },
    );
    types.insert(
        "iam.delegate.revoke".to_string(),
        SchemaType {
            body: revoke_body,
            required: revoke_required,
            allow_extra: true,
        },
    );
    SchemaManifest {
        v: 1,
        name: "note".to_string(),
        types,
    }
}

pub fn note_schema_bytes() -> Result<Vec<u8>, DharmaError> {
    note_schema().to_cbor()
}

pub fn note_schema_id() -> Result<SchemaId, DharmaError> {
    let bytes = note_schema_bytes()?;
    Ok(SchemaId::from_bytes(crypto::sha256(&bytes)))
}

pub fn note_contract_bytes() -> Vec<u8> {
    contract::accept_wasm_bytes()
}

pub fn note_contract_id() -> ContractId {
    ContractId::from_bytes(crypto::sha256(&note_contract_bytes()))
}

pub fn ensure_note_artifacts(store: &Store) -> Result<(SchemaId, ContractId), DharmaError> {
    let schema_bytes = note_schema_bytes()?;
    let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
    let contract_bytes = note_contract_bytes();
    let contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
    let schema_obj = EnvelopeId::from_bytes(*schema_id.as_bytes());
    let contract_obj = EnvelopeId::from_bytes(*contract_id.as_bytes());
    store.put_object(&schema_obj, &schema_bytes)?;
    store.put_object(&contract_obj, &contract_bytes)?;
    Ok((schema_id, contract_id))
}
