use crate::contract;
use crate::crypto;
use crate::error::DharmaError;
use crate::protocols::atlas_identity;
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
    let mut atlas_genesis_body = BTreeMap::new();
    atlas_genesis_body.insert("atlas_name".to_string(), TypeDesc::Text);
    atlas_genesis_body.insert("owner_key".to_string(), TypeDesc::PubKey32);
    atlas_genesis_body.insert("note".to_string(), TypeDesc::Text);
    atlas_genesis_body.insert("root_key".to_string(), TypeDesc::PubKey32);
    let mut atlas_genesis_required = BTreeSet::new();
    atlas_genesis_required.insert("atlas_name".to_string());
    atlas_genesis_required.insert("owner_key".to_string());
    types.insert(
        "atlas.identity.genesis".to_string(),
        SchemaType {
            body: atlas_genesis_body,
            required: atlas_genesis_required,
            allow_extra: true,
        },
    );
    let mut atlas_lifecycle_body = BTreeMap::new();
    atlas_lifecycle_body.insert("reason".to_string(), TypeDesc::Text);
    atlas_lifecycle_body.insert("ts".to_string(), TypeDesc::Int);
    let atlas_lifecycle_required = BTreeSet::new();
    types.insert(
        "atlas.identity.activate".to_string(),
        SchemaType {
            body: atlas_lifecycle_body.clone(),
            required: atlas_lifecycle_required.clone(),
            allow_extra: true,
        },
    );
    let mut domain_genesis_body = BTreeMap::new();
    domain_genesis_body.insert("domain".to_string(), TypeDesc::Text);
    domain_genesis_body.insert("owner".to_string(), TypeDesc::PubKey32);
    domain_genesis_body.insert("parent".to_string(), TypeDesc::Text);
    domain_genesis_body.insert("ownership_default".to_string(), TypeDesc::Text);
    domain_genesis_body.insert("transfer_policy".to_string(), TypeDesc::Text);
    domain_genesis_body.insert("note".to_string(), TypeDesc::Text);
    let mut domain_genesis_required = BTreeSet::new();
    domain_genesis_required.insert("domain".to_string());
    domain_genesis_required.insert("owner".to_string());
    types.insert(
        "atlas.domain.genesis".to_string(),
        SchemaType {
            body: domain_genesis_body,
            required: domain_genesis_required,
            allow_extra: true,
        },
    );
    let mut domain_invite_body = BTreeMap::new();
    domain_invite_body.insert("target".to_string(), TypeDesc::PubKey32);
    domain_invite_body.insert("roles".to_string(), TypeDesc::List(Box::new(TypeDesc::Text)));
    domain_invite_body.insert("scopes".to_string(), TypeDesc::List(Box::new(TypeDesc::Text)));
    domain_invite_body.insert("expires".to_string(), TypeDesc::Int);
    domain_invite_body.insert("note".to_string(), TypeDesc::Text);
    let mut domain_invite_required = BTreeSet::new();
    domain_invite_required.insert("target".to_string());
    domain_invite_required.insert("roles".to_string());
    domain_invite_required.insert("scopes".to_string());
    types.insert(
        "atlas.domain.invite".to_string(),
        SchemaType {
            body: domain_invite_body,
            required: domain_invite_required,
            allow_extra: true,
        },
    );
    let mut domain_request_body = BTreeMap::new();
    domain_request_body.insert("target".to_string(), TypeDesc::PubKey32);
    domain_request_body.insert("roles".to_string(), TypeDesc::List(Box::new(TypeDesc::Text)));
    domain_request_body.insert("scopes".to_string(), TypeDesc::List(Box::new(TypeDesc::Text)));
    domain_request_body.insert("note".to_string(), TypeDesc::Text);
    let mut domain_request_required = BTreeSet::new();
    domain_request_required.insert("target".to_string());
    types.insert(
        "atlas.domain.request".to_string(),
        SchemaType {
            body: domain_request_body,
            required: domain_request_required,
            allow_extra: true,
        },
    );
    let mut domain_approve_body = BTreeMap::new();
    domain_approve_body.insert("target".to_string(), TypeDesc::PubKey32);
    domain_approve_body.insert("roles".to_string(), TypeDesc::List(Box::new(TypeDesc::Text)));
    domain_approve_body.insert("scopes".to_string(), TypeDesc::List(Box::new(TypeDesc::Text)));
    domain_approve_body.insert("expires".to_string(), TypeDesc::Int);
    domain_approve_body.insert("note".to_string(), TypeDesc::Text);
    let mut domain_approve_required = BTreeSet::new();
    domain_approve_required.insert("target".to_string());
    domain_approve_required.insert("roles".to_string());
    domain_approve_required.insert("scopes".to_string());
    types.insert(
        "atlas.domain.approve".to_string(),
        SchemaType {
            body: domain_approve_body,
            required: domain_approve_required,
            allow_extra: true,
        },
    );
    let mut domain_revoke_body = BTreeMap::new();
    domain_revoke_body.insert("target".to_string(), TypeDesc::PubKey32);
    domain_revoke_body.insert("reason".to_string(), TypeDesc::Text);
    let mut domain_revoke_required = BTreeSet::new();
    domain_revoke_required.insert("target".to_string());
    types.insert(
        "atlas.domain.revoke".to_string(),
        SchemaType {
            body: domain_revoke_body,
            required: domain_revoke_required,
            allow_extra: true,
        },
    );
    let mut domain_leave_body = BTreeMap::new();
    domain_leave_body.insert("target".to_string(), TypeDesc::PubKey32);
    domain_leave_body.insert("note".to_string(), TypeDesc::Text);
    let mut domain_leave_required = BTreeSet::new();
    domain_leave_required.insert("target".to_string());
    types.insert(
        "atlas.domain.leave".to_string(),
        SchemaType {
            body: domain_leave_body,
            required: domain_leave_required,
            allow_extra: true,
        },
    );
    let mut domain_policy_body = BTreeMap::new();
    domain_policy_body.insert("relay_domain".to_string(), TypeDesc::Text);
    domain_policy_body.insert("relay_plan".to_string(), TypeDesc::Text);
    domain_policy_body.insert("note".to_string(), TypeDesc::Text);
    let mut domain_policy_required = BTreeSet::new();
    domain_policy_required.insert("relay_domain".to_string());
    domain_policy_required.insert("relay_plan".to_string());
    types.insert(
        "atlas.domain.policy".to_string(),
        SchemaType {
            body: domain_policy_body,
            required: domain_policy_required,
            allow_extra: true,
        },
    );
    let mut domain_control_body = BTreeMap::new();
    domain_control_body.insert("reason".to_string(), TypeDesc::Text);
    types.insert(
        "domain.freeze".to_string(),
        SchemaType {
            body: domain_control_body.clone(),
            required: BTreeSet::new(),
            allow_extra: true,
        },
    );
    types.insert(
        "domain.unfreeze".to_string(),
        SchemaType {
            body: domain_control_body.clone(),
            required: BTreeSet::new(),
            allow_extra: true,
        },
    );
    types.insert(
        "domain.compromised".to_string(),
        SchemaType {
            body: domain_control_body,
            required: BTreeSet::new(),
            allow_extra: true,
        },
    );
    types.insert(
        "atlas.identity.suspend".to_string(),
        SchemaType {
            body: atlas_lifecycle_body.clone(),
            required: atlas_lifecycle_required.clone(),
            allow_extra: true,
        },
    );
    types.insert(
        "atlas.identity.revoke".to_string(),
        SchemaType {
            body: atlas_lifecycle_body,
            required: atlas_lifecycle_required,
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
    let mut share_grant_body = BTreeMap::new();
    share_grant_body.insert("target_identity".to_string(), TypeDesc::PubKey32);
    share_grant_body.insert("target_role".to_string(), TypeDesc::Text);
    share_grant_body.insert("scopes".to_string(), TypeDesc::List(Box::new(TypeDesc::Text)));
    share_grant_body.insert("expires".to_string(), TypeDesc::Int);
    share_grant_body.insert("note".to_string(), TypeDesc::Text);
    let mut share_grant_required = BTreeSet::new();
    share_grant_required.insert("scopes".to_string());
    types.insert(
        "share.grant".to_string(),
        SchemaType {
            body: share_grant_body,
            required: share_grant_required,
            allow_extra: true,
        },
    );
    let mut share_revoke_body = BTreeMap::new();
    share_revoke_body.insert("target_identity".to_string(), TypeDesc::PubKey32);
    share_revoke_body.insert("target_role".to_string(), TypeDesc::Text);
    share_revoke_body.insert("note".to_string(), TypeDesc::Text);
    types.insert(
        "share.revoke".to_string(),
        SchemaType {
            body: share_revoke_body,
            required: BTreeSet::new(),
            allow_extra: true,
        },
    );
    let mut share_public_body = BTreeMap::new();
    share_public_body.insert("enabled".to_string(), TypeDesc::Bool);
    share_public_body.insert("scopes".to_string(), TypeDesc::List(Box::new(TypeDesc::Text)));
    share_public_body.insert("expires".to_string(), TypeDesc::Int);
    share_public_body.insert("note".to_string(), TypeDesc::Text);
    types.insert(
        "share.public".to_string(),
        SchemaType {
            body: share_public_body,
            required: BTreeSet::new(),
            allow_extra: true,
        },
    );
    let mut domain_rotate_body = BTreeMap::new();
    domain_rotate_body.insert("epoch".to_string(), TypeDesc::Int);
    domain_rotate_body.insert("kek_id".to_string(), TypeDesc::Id32);
    let mut domain_rotate_required = BTreeSet::new();
    domain_rotate_required.insert("epoch".to_string());
    domain_rotate_required.insert("kek_id".to_string());
    types.insert(
        "domain.key.rotate".to_string(),
        SchemaType {
            body: domain_rotate_body,
            required: domain_rotate_required,
            allow_extra: true,
        },
    );
    let mut subject_bind_body = BTreeMap::new();
    subject_bind_body.insert("domain".to_string(), TypeDesc::Id32);
    subject_bind_body.insert("epoch".to_string(), TypeDesc::Int);
    subject_bind_body.insert("sdk_id".to_string(), TypeDesc::Id32);
    let mut subject_bind_required = BTreeSet::new();
    subject_bind_required.insert("domain".to_string());
    subject_bind_required.insert("epoch".to_string());
    subject_bind_required.insert("sdk_id".to_string());
    types.insert(
        "subject.key.bind".to_string(),
        SchemaType {
            body: subject_bind_body,
            required: subject_bind_required,
            allow_extra: true,
        },
    );
    let mut member_grant_body = BTreeMap::new();
    member_grant_body.insert("member".to_string(), TypeDesc::PubKey32);
    member_grant_body.insert("subject".to_string(), TypeDesc::Id32);
    member_grant_body.insert("epoch".to_string(), TypeDesc::Int);
    member_grant_body.insert("sdk_id".to_string(), TypeDesc::Id32);
    member_grant_body.insert("sdk".to_string(), TypeDesc::Bytes);
    let mut member_grant_required = BTreeSet::new();
    member_grant_required.insert("member".to_string());
    member_grant_required.insert("subject".to_string());
    member_grant_required.insert("epoch".to_string());
    member_grant_required.insert("sdk_id".to_string());
    member_grant_required.insert("sdk".to_string());
    types.insert(
        "member.key.grant".to_string(),
        SchemaType {
            body: member_grant_body,
            required: member_grant_required,
            allow_extra: true,
        },
    );
    let mut transfer_body = BTreeMap::new();
    transfer_body.insert("owner_kind".to_string(), TypeDesc::Text);
    transfer_body.insert("owner".to_string(), TypeDesc::PubKey32);
    transfer_body.insert("note".to_string(), TypeDesc::Text);
    let mut transfer_required = BTreeSet::new();
    transfer_required.insert("owner_kind".to_string());
    transfer_required.insert("owner".to_string());
    types.insert(
        "subject.transfer".to_string(),
        SchemaType {
            body: transfer_body.clone(),
            required: transfer_required.clone(),
            allow_extra: true,
        },
    );
    types.insert(
        "subject.transfer.propose".to_string(),
        SchemaType {
            body: transfer_body.clone(),
            required: transfer_required.clone(),
            allow_extra: true,
        },
    );
    types.insert(
        "subject.transfer.accept".to_string(),
        SchemaType {
            body: transfer_body,
            required: transfer_required,
            allow_extra: true,
        },
    );
    SchemaManifest {
        v: 1,
        name: "note".to_string(),
        implements: vec![
            format!(
                "{}@{}",
                atlas_identity::PROTOCOL_NAME,
                atlas_identity::PROTOCOL_VERSION
            ),
            format!(
                "{}@{}",
                crate::protocols::atlas_domain::PROTOCOL_NAME,
                crate::protocols::atlas_domain::PROTOCOL_VERSION
            ),
        ],
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{parse_schema, validate_body};
    use ciborium::value::Value;

    #[test]
    fn atlas_identity_schema_roundtrip() {
        let bytes = note_schema_bytes().unwrap();
        let schema = parse_schema(&bytes).unwrap();
        assert!(schema.types.contains_key("atlas.identity.genesis"));
        assert!(schema.types.contains_key("atlas.identity.activate"));
        assert!(schema.types.contains_key("atlas.identity.suspend"));
        assert!(schema.types.contains_key("atlas.identity.revoke"));
    }

    #[test]
    fn atlas_identity_schema_required_fields() {
        let schema = note_schema();
        let body = Value::Map(vec![]);
        assert!(validate_body(&schema, "atlas.identity.genesis", &body).is_err());
        let ok_body = Value::Map(vec![
            (
                Value::Text("atlas_name".to_string()),
                Value::Text("person.local.alice_abcd".to_string()),
            ),
            (
                Value::Text("owner_key".to_string()),
                Value::Bytes(vec![0u8; 32]),
            ),
        ]);
        assert!(validate_body(&schema, "atlas.identity.genesis", &ok_body).is_ok());
    }

    #[test]
    fn atlas_identity_schema_unknown_type() {
        let schema = note_schema();
        let body = Value::Map(vec![]);
        assert!(validate_body(&schema, "atlas.identity.unknown", &body).is_err());
    }

    #[test]
    fn atlas_domain_schema_roundtrip() {
        let bytes = note_schema_bytes().unwrap();
        let schema = parse_schema(&bytes).unwrap();
        assert!(schema.types.contains_key("atlas.domain.genesis"));
        assert!(schema.types.contains_key("atlas.domain.invite"));
        assert!(schema.types.contains_key("atlas.domain.request"));
        assert!(schema.types.contains_key("atlas.domain.approve"));
        assert!(schema.types.contains_key("atlas.domain.revoke"));
        assert!(schema.types.contains_key("atlas.domain.leave"));
        assert!(schema.types.contains_key("atlas.domain.policy"));
    }

    #[test]
    fn atlas_domain_required_fields() {
        let schema = note_schema();
        let body = Value::Map(vec![]);
        assert!(validate_body(&schema, "atlas.domain.genesis", &body).is_err());
        let ok_body = Value::Map(vec![
            (Value::Text("domain".to_string()), Value::Text("corp.acme".to_string())),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(vec![0u8; 32]),
            ),
        ]);
        assert!(validate_body(&schema, "atlas.domain.genesis", &ok_body).is_ok());
    }

    #[test]
    fn atlas_domain_schema_unknown_type() {
        let schema = note_schema();
        let body = Value::Map(vec![]);
        assert!(validate_body(&schema, "atlas.domain.unknown", &body).is_err());
    }

    #[test]
    fn share_schema_roundtrip() {
        let bytes = note_schema_bytes().unwrap();
        let schema = parse_schema(&bytes).unwrap();
        assert!(schema.types.contains_key("share.grant"));
        assert!(schema.types.contains_key("share.revoke"));
        assert!(schema.types.contains_key("share.public"));
    }

    #[test]
    fn share_grant_requires_scopes() {
        let schema = note_schema();
        let body = Value::Map(vec![(
            Value::Text("target_identity".to_string()),
            Value::Bytes(vec![0u8; 32]),
        )]);
        assert!(validate_body(&schema, "share.grant", &body).is_err());
        let ok_body = Value::Map(vec![(
            Value::Text("scopes".to_string()),
            Value::Array(vec![Value::Text("read".to_string())]),
        )]);
        assert!(validate_body(&schema, "share.grant", &ok_body).is_ok());
    }

    #[test]
    fn transfer_schema_required_fields() {
        let schema = note_schema();
        let body = Value::Map(vec![]);
        assert!(validate_body(&schema, "subject.transfer", &body).is_err());
        let ok_body = Value::Map(vec![
            (
                Value::Text("owner_kind".to_string()),
                Value::Text("identity".to_string()),
            ),
            (Value::Text("owner".to_string()), Value::Bytes(vec![0u8; 32])),
        ]);
        assert!(validate_body(&schema, "subject.transfer", &ok_body).is_ok());
        assert!(validate_body(&schema, "subject.transfer.propose", &ok_body).is_ok());
        assert!(validate_body(&schema, "subject.transfer.accept", &ok_body).is_ok());
    }
}
