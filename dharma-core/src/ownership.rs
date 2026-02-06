use crate::assertion::{signer_from_meta, AssertionPlaintext};
use crate::env::Env;
use crate::error::DharmaError;
use crate::identity::root_key_for_identity;
use crate::store::state::list_assertions;
use crate::store::Store;
use crate::types::{IdentityKey, SubjectId};
use crate::value::{expect_bytes, expect_map, expect_text, map_get};
use ciborium::value::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferPolicy {
    Forbidden,
    Immediate,
    ProposeAccept,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnershipState {
    pub owner: Owner,
    pub pending: Option<Owner>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Owner {
    Identity(IdentityKey),
    Domain(SubjectId),
}

impl Owner {
    fn kind(&self) -> &'static str {
        match self {
            Owner::Identity(_) => "identity",
            Owner::Domain(_) => "domain",
        }
    }

    fn bytes(&self) -> Vec<u8> {
        match self {
            Owner::Identity(key) => key.as_bytes().to_vec(),
            Owner::Domain(subject) => subject.as_bytes().to_vec(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnershipRecord {
    pub owner: Owner,
    pub creator: IdentityKey,
    pub acting_domain: Option<SubjectId>,
    pub role: Option<String>,
}

impl OwnershipRecord {
    pub fn to_value(&self) -> Value {
        let mut entries = vec![
            (
                Value::Text("owner_kind".to_string()),
                Value::Text(self.owner.kind().to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(self.owner.bytes()),
            ),
            (
                Value::Text("creator".to_string()),
                Value::Bytes(self.creator.as_bytes().to_vec()),
            ),
        ];
        if let Some(domain) = &self.acting_domain {
            entries.push((
                Value::Text("acting_domain".to_string()),
                Value::Bytes(domain.as_bytes().to_vec()),
            ));
        }
        if let Some(role) = &self.role {
            entries.push((Value::Text("role".to_string()), Value::Text(role.clone())));
        }
        Value::Map(entries)
    }

    pub fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let owner_kind = expect_text(
            map_get(map, "owner_kind")
                .ok_or_else(|| DharmaError::Validation("missing owner_kind".to_string()))?,
        )?;
        let owner_bytes = expect_bytes(
            map_get(map, "owner")
                .ok_or_else(|| DharmaError::Validation("missing owner".to_string()))?,
        )?;
        let creator_bytes = expect_bytes(
            map_get(map, "creator")
                .ok_or_else(|| DharmaError::Validation("missing creator".to_string()))?,
        )?;
        let owner = match owner_kind.as_str() {
            "identity" => Owner::Identity(IdentityKey::from_slice(&owner_bytes)?),
            "domain" => Owner::Domain(SubjectId::from_slice(&owner_bytes)?),
            _ => return Err(DharmaError::Validation("invalid owner_kind".to_string())),
        };
        let creator = IdentityKey::from_slice(&creator_bytes)?;
        let acting_domain = match map_get(map, "acting_domain") {
            Some(value) => Some(SubjectId::from_slice(&expect_bytes(value)?)?),
            None => None,
        };
        let role = match map_get(map, "role") {
            Some(value) => {
                let text = expect_text(value)?;
                if text.is_empty() {
                    None
                } else {
                    Some(text)
                }
            }
            None => None,
        };
        Ok(Self {
            owner,
            creator,
            acting_domain,
            role,
        })
    }
}

pub fn derive_ownership_record(
    env: &dyn Env,
    assertion: &AssertionPlaintext,
) -> Result<OwnershipRecord, DharmaError> {
    let mut owner_identity: Option<IdentityKey> = None;
    let mut owner_domain: Option<SubjectId> = None;
    let mut owner_kind: Option<String> = None;
    let mut owner_bytes: Option<Vec<u8>> = None;
    let mut acting_domain: Option<SubjectId> = None;
    let mut role: Option<String> = None;
    if let Some(Value::Map(entries)) = &assertion.header.meta {
        for (key, value) in entries {
            let Value::Text(name) = key else {
                continue;
            };
            match name.as_str() {
                "owner_kind" => {
                    owner_kind = Some(expect_text(value)?);
                }
                "owner" => {
                    owner_bytes = Some(expect_bytes(value)?);
                }
                "owner_identity" => {
                    let bytes = expect_bytes(value)?;
                    owner_identity = Some(IdentityKey::from_slice(&bytes)?);
                }
                "owner_domain" => {
                    let bytes = expect_bytes(value)?;
                    owner_domain = Some(SubjectId::from_slice(&bytes)?);
                }
                "acting_domain" => {
                    let bytes = expect_bytes(value)?;
                    acting_domain = Some(SubjectId::from_slice(&bytes)?);
                }
                "acting_role" => {
                    let text = expect_text(value)?;
                    if !text.is_empty() {
                        role = Some(text);
                    }
                }
                _ => {}
            }
        }
    }
    if owner_kind.is_some() && owner_bytes.is_none() {
        return Err(DharmaError::Validation("missing owner".to_string()));
    }
    if owner_bytes.is_some() && owner_kind.is_none() {
        return Err(DharmaError::Validation("missing owner_kind".to_string()));
    }
    let mut candidates: Vec<Owner> = Vec::new();
    if let Some(identity) = owner_identity {
        candidates.push(Owner::Identity(identity));
    }
    if let Some(domain) = owner_domain {
        candidates.push(Owner::Domain(domain));
    }
    if let (Some(kind), Some(bytes)) = (owner_kind, owner_bytes) {
        let owner = match kind.as_str() {
            "identity" => Owner::Identity(IdentityKey::from_slice(&bytes)?),
            "domain" => Owner::Domain(SubjectId::from_slice(&bytes)?),
            _ => return Err(DharmaError::Validation("invalid owner_kind".to_string())),
        };
        candidates.push(owner);
    }
    if candidates.windows(2).any(|pair| pair[0] != pair[1]) {
        return Err(DharmaError::Validation("multiple owners".to_string()));
    }
    let creator = match signer_from_meta(&assertion.header.meta) {
        Some(signer) => root_key_for_identity(env, &signer)?.unwrap_or(assertion.header.auth),
        None => assertion.header.auth,
    };
    let owner = if let Some(owner) = candidates.first().cloned() {
        owner
    } else if let Some(domain) = acting_domain {
        Owner::Domain(domain)
    } else {
        Owner::Identity(creator)
    };
    Ok(OwnershipRecord {
        owner,
        creator,
        acting_domain,
        role,
    })
}

impl OwnershipState {
    pub fn load(store: &Store, subject: &SubjectId) -> Result<Option<Self>, DharmaError> {
        let Some(record) = crate::store::state::load_ownership(store.env(), subject)? else {
            return Ok(None);
        };
        let mut state = OwnershipState {
            owner: record.owner,
            pending: None,
        };
        for record in list_assertions(store.env(), subject)? {
            let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
                Ok(value) => value,
                Err(_) => continue,
            };
            if assertion.header.sub != *subject {
                continue;
            }
            if !assertion.verify_signature()? {
                continue;
            }
            if !is_transfer_type(&assertion.header.typ) {
                continue;
            }
            let _ = state.apply_transfer_event(store, &assertion);
        }
        Ok(Some(state))
    }

    pub fn validate_transfer(
        &self,
        store: &Store,
        assertion: &AssertionPlaintext,
    ) -> Result<Option<Owner>, DharmaError> {
        if !is_transfer_type(&assertion.header.typ) {
            return Ok(None);
        }
        let policy = transfer_policy_for_owner(store, &self.owner)?;
        if policy == TransferPolicy::Forbidden {
            return Err(DharmaError::Validation("transfer forbidden".to_string()));
        }
        let Some(owner_key) = owner_signer_key(store, &self.owner)? else {
            return Err(DharmaError::Validation("missing owner".to_string()));
        };
        match assertion.header.typ.as_str() {
            "subject.transfer" => {
                if policy != TransferPolicy::Immediate {
                    return Err(DharmaError::Validation(
                        "transfer requires immediate policy".to_string(),
                    ));
                }
                if assertion.header.auth.as_bytes() != owner_key.as_bytes() {
                    return Err(DharmaError::Validation("unauthorized transfer".to_string()));
                }
                let target = parse_owner_from_body(&assertion.body)?;
                Ok(Some(target))
            }
            "subject.transfer.propose" => {
                if policy != TransferPolicy::ProposeAccept {
                    return Err(DharmaError::Validation(
                        "transfer requires propose/accept".to_string(),
                    ));
                }
                if assertion.header.auth.as_bytes() != owner_key.as_bytes() {
                    return Err(DharmaError::Validation("unauthorized transfer".to_string()));
                }
                Ok(None)
            }
            "subject.transfer.accept" => {
                if policy != TransferPolicy::ProposeAccept {
                    return Err(DharmaError::Validation(
                        "transfer requires propose/accept".to_string(),
                    ));
                }
                let target = parse_owner_from_body(&assertion.body)?;
                let pending = self.pending.as_ref().ok_or_else(|| {
                    DharmaError::Validation("missing transfer proposal".to_string())
                })?;
                if pending != &target {
                    return Err(DharmaError::Validation(
                        "transfer target mismatch".to_string(),
                    ));
                }
                let Some(target_key) = owner_signer_key(store, &target)? else {
                    return Err(DharmaError::Validation(
                        "missing transfer target".to_string(),
                    ));
                };
                if assertion.header.auth.as_bytes() != target_key.as_bytes() {
                    return Err(DharmaError::Validation("unauthorized transfer".to_string()));
                }
                Ok(Some(target))
            }
            _ => Ok(None),
        }
    }

    fn apply_transfer_event(
        &mut self,
        store: &Store,
        assertion: &AssertionPlaintext,
    ) -> Result<(), DharmaError> {
        let policy = transfer_policy_for_owner(store, &self.owner)?;
        let Some(owner_key) = owner_signer_key(store, &self.owner)? else {
            return Ok(());
        };
        match assertion.header.typ.as_str() {
            "subject.transfer" => {
                if policy != TransferPolicy::Immediate {
                    return Ok(());
                }
                if assertion.header.auth.as_bytes() != owner_key.as_bytes() {
                    return Ok(());
                }
                let target = parse_owner_from_body(&assertion.body)?;
                self.owner = target;
                self.pending = None;
            }
            "subject.transfer.propose" => {
                if policy != TransferPolicy::ProposeAccept {
                    return Ok(());
                }
                if assertion.header.auth.as_bytes() != owner_key.as_bytes() {
                    return Ok(());
                }
                let target = parse_owner_from_body(&assertion.body)?;
                self.pending = Some(target);
            }
            "subject.transfer.accept" => {
                if policy != TransferPolicy::ProposeAccept {
                    return Ok(());
                }
                let target = parse_owner_from_body(&assertion.body)?;
                let Some(pending) = &self.pending else {
                    return Ok(());
                };
                if pending != &target {
                    return Ok(());
                }
                let Some(target_key) = owner_signer_key(store, &target)? else {
                    return Ok(());
                };
                if assertion.header.auth.as_bytes() != target_key.as_bytes() {
                    return Ok(());
                }
                self.owner = target;
                self.pending = None;
            }
            _ => {}
        }
        Ok(())
    }
}

fn is_transfer_type(typ: &str) -> bool {
    matches!(
        typ,
        "subject.transfer" | "subject.transfer.propose" | "subject.transfer.accept"
    )
}

fn parse_owner_from_body(body: &Value) -> Result<Owner, DharmaError> {
    let map = expect_map(body)?;
    let kind = expect_text(
        map_get(map, "owner_kind")
            .ok_or_else(|| DharmaError::Validation("missing owner_kind".to_string()))?,
    )?;
    let bytes = expect_bytes(
        map_get(map, "owner")
            .ok_or_else(|| DharmaError::Validation("missing owner".to_string()))?,
    )?;
    match kind.as_str() {
        "identity" => Ok(Owner::Identity(IdentityKey::from_slice(&bytes)?)),
        "domain" => Ok(Owner::Domain(SubjectId::from_slice(&bytes)?)),
        _ => Err(DharmaError::Validation("invalid owner_kind".to_string())),
    }
}

fn owner_signer_key(store: &Store, owner: &Owner) -> Result<Option<IdentityKey>, DharmaError> {
    match owner {
        Owner::Identity(key) => Ok(Some(*key)),
        Owner::Domain(domain_subject) => {
            let state = crate::domain::DomainState::load(store, domain_subject)?;
            Ok(state.owner)
        }
    }
}

fn transfer_policy_for_owner(store: &Store, owner: &Owner) -> Result<TransferPolicy, DharmaError> {
    match owner {
        Owner::Identity(_) => Ok(TransferPolicy::Forbidden),
        Owner::Domain(domain_subject) => {
            let state = crate::domain::DomainState::load(store, domain_subject)?;
            match state.transfer_policy.as_deref() {
                Some("immediate") => Ok(TransferPolicy::Immediate),
                Some("propose_accept") | Some("propose") | Some("accept") => {
                    Ok(TransferPolicy::ProposeAccept)
                }
                _ => Ok(TransferPolicy::Forbidden),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{
        add_signer_meta, AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION,
    };
    use crate::crypto;
    use crate::store::state::{append_assertion, save_ownership};
    use crate::store::Store;
    use crate::types::{ContractId, SchemaId};
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn make_assertion(
        meta: Option<Value>,
        auth: IdentityKey,
        signing_key: &ed25519_dalek::SigningKey,
    ) -> AssertionPlaintext {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: SubjectId::from_bytes([1u8; 32]),
            typ: "note.text".to_string(),
            auth,
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([2u8; 32]),
            contract: ContractId::from_bytes([3u8; 32]),
            note: None,
            meta,
        };
        AssertionPlaintext::sign(header, Value::Null, signing_key).unwrap()
    }

    #[test]
    fn owner_default_to_domain() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(10);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let auth = IdentityKey::from_bytes(signing_key.verifying_key().to_bytes());
        let signer = SubjectId::from_bytes([4u8; 32]);
        let acting_domain = SubjectId::from_bytes([5u8; 32]);
        let meta = Value::Map(vec![(
            Value::Text("acting_domain".to_string()),
            Value::Bytes(acting_domain.as_bytes().to_vec()),
        )]);
        let meta = add_signer_meta(Some(meta), &signer);
        let assertion = make_assertion(meta, auth, &signing_key);
        let record = derive_ownership_record(&env, &assertion).unwrap();
        assert!(matches!(record.owner, Owner::Domain(id) if id == acting_domain));
    }

    #[test]
    fn creator_attribution_recorded() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(12);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let auth = IdentityKey::from_bytes(signing_key.verifying_key().to_bytes());
        let signer = SubjectId::from_bytes([6u8; 32]);
        let meta = add_signer_meta(None, &signer);
        let assertion = make_assertion(meta, auth, &signing_key);
        let record = derive_ownership_record(&env, &assertion).unwrap();
        assert_eq!(record.creator, auth);
    }

    #[test]
    fn ownership_exclusive() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(14);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let auth = IdentityKey::from_bytes(signing_key.verifying_key().to_bytes());
        let signer = SubjectId::from_bytes([7u8; 32]);
        let owner_identity = IdentityKey::from_bytes([9u8; 32]);
        let owner_domain = SubjectId::from_bytes([8u8; 32]);
        let meta = Value::Map(vec![
            (
                Value::Text("owner_identity".to_string()),
                Value::Bytes(owner_identity.as_bytes().to_vec()),
            ),
            (
                Value::Text("owner_domain".to_string()),
                Value::Bytes(owner_domain.as_bytes().to_vec()),
            ),
        ]);
        let meta = add_signer_meta(Some(meta), &signer);
        let assertion = make_assertion(meta, auth, &signing_key);
        let err = derive_ownership_record(&env, &assertion).unwrap_err();
        match err {
            DharmaError::Validation(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    fn append_plain_assertion(
        env: &dyn crate::env::Env,
        subject: SubjectId,
        seq: u64,
        typ: &str,
        auth: IdentityKey,
        signing_key: &ed25519_dalek::SigningKey,
        body: Value,
    ) {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: typ.to_string(),
            auth,
            seq,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([11u8; 32]),
            contract: ContractId::from_bytes([12u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, body, signing_key).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        let envelope_id = crypto::envelope_id(&bytes);
        append_assertion(env, &subject, seq, assertion_id, envelope_id, typ, &bytes).unwrap();
    }

    fn append_domain_genesis(
        store: &Store,
        subject: SubjectId,
        domain: &str,
        owner: IdentityKey,
        signing_key: &ed25519_dalek::SigningKey,
        transfer_policy: &str,
    ) {
        let body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text(domain.to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(owner.as_bytes().to_vec()),
            ),
            (
                Value::Text("transfer_policy".to_string()),
                Value::Text(transfer_policy.to_string()),
            ),
        ]);
        append_plain_assertion(
            store.env(),
            subject,
            1,
            "atlas.domain.genesis",
            owner,
            signing_key,
            body,
        );
    }

    fn transfer_body(owner: &Owner) -> Value {
        let (kind, bytes) = match owner {
            Owner::Identity(key) => ("identity", key.as_bytes().to_vec()),
            Owner::Domain(subject) => ("domain", subject.as_bytes().to_vec()),
        };
        Value::Map(vec![
            (
                Value::Text("owner_kind".to_string()),
                Value::Text(kind.to_string()),
            ),
            (Value::Text("owner".to_string()), Value::Bytes(bytes)),
        ])
    }

    #[test]
    fn transfer_forbidden_by_default() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(30);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (_target_sk, target_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([10u8; 32]);
        let record = OwnershipRecord {
            owner: Owner::Identity(owner_id),
            creator: owner_id,
            acting_domain: None,
            role: None,
        };
        save_ownership(env, &subject, &record).unwrap();

        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "subject.transfer".to_string(),
            auth: owner_id,
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([13u8; 32]),
            contract: ContractId::from_bytes([14u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(
            header,
            transfer_body(&Owner::Identity(target_id)),
            &owner_sk,
        )
        .unwrap();
        let state = OwnershipState::load(&store, &subject).unwrap().unwrap();
        let err = state.validate_transfer(&store, &assertion).unwrap_err();
        match err {
            DharmaError::Validation(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn transfer_immediate() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(31);
        let (domain_owner_sk, domain_owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (_target_sk, target_id) = crypto::generate_identity_keypair(&mut rng);
        let domain_subject = SubjectId::from_bytes([11u8; 32]);
        append_domain_genesis(
            &store,
            domain_subject,
            "corp",
            domain_owner_id,
            &domain_owner_sk,
            "immediate",
        );
        let subject = SubjectId::from_bytes([12u8; 32]);
        let record = OwnershipRecord {
            owner: Owner::Domain(domain_subject),
            creator: domain_owner_id,
            acting_domain: Some(domain_subject),
            role: None,
        };
        save_ownership(env, &subject, &record).unwrap();

        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "subject.transfer".to_string(),
            auth: domain_owner_id,
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([15u8; 32]),
            contract: ContractId::from_bytes([16u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(
            header,
            transfer_body(&Owner::Identity(target_id)),
            &domain_owner_sk,
        )
        .unwrap();
        let state = OwnershipState::load(&store, &subject).unwrap().unwrap();
        let decision = state.validate_transfer(&store, &assertion).unwrap();
        assert!(matches!(decision, Some(Owner::Identity(id)) if id == target_id));
    }

    #[test]
    fn transfer_propose_accept() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(32);
        let (domain_owner_sk, domain_owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (target_sk, target_id) = crypto::generate_identity_keypair(&mut rng);
        let domain_subject = SubjectId::from_bytes([13u8; 32]);
        append_domain_genesis(
            &store,
            domain_subject,
            "corp",
            domain_owner_id,
            &domain_owner_sk,
            "propose_accept",
        );
        let subject = SubjectId::from_bytes([14u8; 32]);
        let record = OwnershipRecord {
            owner: Owner::Domain(domain_subject),
            creator: domain_owner_id,
            acting_domain: Some(domain_subject),
            role: None,
        };
        save_ownership(env, &subject, &record).unwrap();

        append_plain_assertion(
            env,
            subject,
            1,
            "subject.transfer.propose",
            domain_owner_id,
            &domain_owner_sk,
            transfer_body(&Owner::Identity(target_id)),
        );

        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "subject.transfer.accept".to_string(),
            auth: target_id,
            seq: 2,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([17u8; 32]),
            contract: ContractId::from_bytes([18u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(
            header,
            transfer_body(&Owner::Identity(target_id)),
            &target_sk,
        )
        .unwrap();
        let state = OwnershipState::load(&store, &subject).unwrap().unwrap();
        let decision = state.validate_transfer(&store, &assertion).unwrap();
        assert!(matches!(decision, Some(Owner::Identity(id)) if id == target_id));
    }
}
