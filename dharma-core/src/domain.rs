use crate::assertion::AssertionPlaintext;
use crate::error::DharmaError;
use crate::protocols::atlas_domain as atlas_proto;
use crate::store::state::list_assertions;
use crate::store::Store;
use crate::types::{IdentityKey, SubjectId};
use crate::value::{expect_array, expect_bytes, expect_int, expect_map, expect_text, map_get};
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainMember {
    pub roles: Vec<String>,
    pub scopes: Vec<String>,
    pub expires: Option<i64>,
}

#[derive(Clone, Debug, Default)]
pub struct DomainState {
    pub domain: Option<String>,
    pub owner: Option<IdentityKey>,
    pub parent: Option<String>,
    pub ownership_default: Option<String>,
    pub transfer_policy: Option<String>,
    pub backup_relay_domain: Option<String>,
    pub backup_relay_plan: Option<String>,
    pub members: HashMap<IdentityKey, DomainMember>,
    pub invites: HashMap<IdentityKey, DomainMember>,
    pub requests: HashMap<IdentityKey, DomainMember>,
    pub frozen: bool,
    pub compromised: bool,
}

impl DomainState {
    pub fn load(store: &Store, subject: &SubjectId) -> Result<Self, DharmaError> {
        let mut state = DomainState::default();
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
            match assertion.header.typ.as_str() {
                atlas_proto::ASSERTION_GENESIS => state.apply_genesis(&assertion)?,
                atlas_proto::ASSERTION_INVITE => state.apply_invite(&assertion)?,
                atlas_proto::ASSERTION_REQUEST => state.apply_request(&assertion)?,
                atlas_proto::ASSERTION_APPROVE => state.apply_approve(&assertion)?,
                atlas_proto::ASSERTION_REVOKE => state.apply_revoke(&assertion)?,
                atlas_proto::ASSERTION_LEAVE => state.apply_leave(&assertion)?,
                atlas_proto::ASSERTION_POLICY => state.apply_policy(&assertion)?,
                atlas_proto::ASSERTION_FREEZE => state.apply_freeze(&assertion)?,
                atlas_proto::ASSERTION_UNFREEZE => state.apply_unfreeze(&assertion)?,
                atlas_proto::ASSERTION_COMPROMISED => state.apply_compromised(&assertion)?,
                _ => {}
            }
        }
        Ok(state)
    }

    pub fn is_member(&self, key: &IdentityKey, now: i64) -> bool {
        self.member(key, now).is_some()
    }

    pub fn member(&self, key: &IdentityKey, now: i64) -> Option<DomainMember> {
        let member = self.members.get(key)?;
        if is_expired(member.expires, now) {
            return None;
        }
        Some(member.clone())
    }

    fn apply_genesis(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let domain = expect_text(
            map_get(map, "domain")
                .ok_or_else(|| DharmaError::Validation("missing domain".to_string()))?,
        )?;
        let owner = parse_identity(
            map_get(map, "owner")
                .ok_or_else(|| DharmaError::Validation("missing owner".to_string()))?,
        )?;
        if assertion.header.auth.as_bytes() != owner.as_bytes() {
            return Ok(());
        }
        let parent = parse_optional_text(map_get(map, "parent"))?;
        let ownership_default = parse_optional_text(map_get(map, "ownership_default"))?;
        let transfer_policy = parse_optional_text(map_get(map, "transfer_policy"))?;

        self.domain = Some(domain);
        self.owner = Some(owner);
        self.parent = parent;
        self.ownership_default =
            ownership_default.or_else(|| Some(atlas_proto::DEFAULT_OWNERSHIP.to_string()));
        self.transfer_policy =
            transfer_policy.or_else(|| Some(atlas_proto::DEFAULT_TRANSFER_POLICY.to_string()));
        self.members.insert(
            owner,
            DomainMember {
                roles: vec!["owner".to_string()],
                scopes: vec!["all".to_string()],
                expires: None,
            },
        );
        Ok(())
    }

    fn apply_invite(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let Some(owner) = self.owner else {
            return Ok(());
        };
        if assertion.header.auth.as_bytes() != owner.as_bytes() {
            return Ok(());
        }
        let map = expect_map(&assertion.body)?;
        let target = parse_identity(
            map_get(map, "target")
                .ok_or_else(|| DharmaError::Validation("missing target".to_string()))?,
        )?;
        let roles = parse_text_list(map_get(map, "roles"), true)?;
        let scopes = parse_text_list(map_get(map, "scopes"), true)?;
        let expires = parse_optional_int(map_get(map, "expires"))?;
        self.invites.insert(
            target,
            DomainMember {
                roles,
                scopes,
                expires,
            },
        );
        Ok(())
    }

    fn apply_request(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let target = parse_identity(
            map_get(map, "target")
                .ok_or_else(|| DharmaError::Validation("missing target".to_string()))?,
        )?;
        if assertion.header.auth.as_bytes() != target.as_bytes() {
            return Ok(());
        }
        let roles = parse_text_list(map_get(map, "roles"), false)?;
        let scopes = parse_text_list(map_get(map, "scopes"), false)?;
        self.requests.insert(
            target,
            DomainMember {
                roles,
                scopes,
                expires: None,
            },
        );
        Ok(())
    }

    fn apply_approve(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let Some(owner) = self.owner else {
            return Ok(());
        };
        if assertion.header.auth.as_bytes() != owner.as_bytes() {
            return Ok(());
        }
        let map = expect_map(&assertion.body)?;
        let target = parse_identity(
            map_get(map, "target")
                .ok_or_else(|| DharmaError::Validation("missing target".to_string()))?,
        )?;
        let roles = parse_text_list(map_get(map, "roles"), true)?;
        let scopes = parse_text_list(map_get(map, "scopes"), true)?;
        let expires = parse_optional_int(map_get(map, "expires"))?;
        self.members.insert(
            target,
            DomainMember {
                roles,
                scopes,
                expires,
            },
        );
        self.invites.remove(&target);
        self.requests.remove(&target);
        Ok(())
    }

    fn apply_revoke(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let Some(owner) = self.owner else {
            return Ok(());
        };
        if assertion.header.auth.as_bytes() != owner.as_bytes() {
            return Ok(());
        }
        let map = expect_map(&assertion.body)?;
        let target = parse_identity(
            map_get(map, "target")
                .ok_or_else(|| DharmaError::Validation("missing target".to_string()))?,
        )?;
        self.members.remove(&target);
        self.invites.remove(&target);
        self.requests.remove(&target);
        Ok(())
    }

    fn apply_leave(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let target = parse_identity(
            map_get(map, "target")
                .ok_or_else(|| DharmaError::Validation("missing target".to_string()))?,
        )?;
        if assertion.header.auth.as_bytes() != target.as_bytes() {
            return Ok(());
        }
        self.members.remove(&target);
        self.invites.remove(&target);
        self.requests.remove(&target);
        Ok(())
    }

    fn apply_policy(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let Some(owner) = self.owner else {
            return Ok(());
        };
        if assertion.header.auth.as_bytes() != owner.as_bytes() {
            return Ok(());
        }
        let map = expect_map(&assertion.body)?;
        let relay_domain = expect_text(
            map_get(map, "relay_domain")
                .ok_or_else(|| DharmaError::Validation("missing relay_domain".to_string()))?,
        )?;
        let relay_plan = expect_text(
            map_get(map, "relay_plan")
                .ok_or_else(|| DharmaError::Validation("missing relay_plan".to_string()))?,
        )?;
        if relay_domain.is_empty() || relay_plan.is_empty() {
            return Ok(());
        }
        self.backup_relay_domain = Some(relay_domain);
        self.backup_relay_plan = Some(relay_plan);
        Ok(())
    }

    fn apply_freeze(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let Some(owner) = self.owner else {
            return Ok(());
        };
        if assertion.header.auth.as_bytes() != owner.as_bytes() {
            return Ok(());
        }
        self.frozen = true;
        Ok(())
    }

    fn apply_unfreeze(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let Some(owner) = self.owner else {
            return Ok(());
        };
        if assertion.header.auth.as_bytes() != owner.as_bytes() {
            return Ok(());
        }
        if !self.compromised {
            self.frozen = false;
        }
        Ok(())
    }

    fn apply_compromised(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let Some(owner) = self.owner else {
            return Ok(());
        };
        if assertion.header.auth.as_bytes() != owner.as_bytes() {
            return Ok(());
        }
        self.compromised = true;
        Ok(())
    }
}

pub fn subject_for_domain(store: &Store, domain: &str) -> Result<Option<SubjectId>, DharmaError> {
    for subject in store.list_subjects()? {
        let state = DomainState::load(store, &subject)?;
        if state.domain.as_deref() == Some(domain) {
            return Ok(Some(subject));
        }
    }
    Ok(None)
}

pub fn owner_for_domain(store: &Store, domain: &str) -> Result<Option<IdentityKey>, DharmaError> {
    let Some(subject) = subject_for_domain(store, domain)? else {
        return Ok(None);
    };
    let state = DomainState::load(store, &subject)?;
    Ok(state.owner)
}

pub fn parent_name(domain: &str) -> Option<String> {
    let mut parts = domain.rsplitn(2, '.');
    let _leaf = parts.next()?;
    let parent = parts.next()?;
    Some(parent.to_string())
}

fn parse_identity(value: &ciborium::value::Value) -> Result<IdentityKey, DharmaError> {
    let bytes = expect_bytes(value)?;
    IdentityKey::from_slice(&bytes)
}

fn parse_text_list(
    value: Option<&ciborium::value::Value>,
    required: bool,
) -> Result<Vec<String>, DharmaError> {
    let Some(value) = value else {
        if required {
            return Err(DharmaError::Validation("missing list".to_string()));
        }
        return Ok(Vec::new());
    };
    let arr = expect_array(value)?;
    let mut set = BTreeSet::new();
    for item in arr {
        let text = expect_text(item)?;
        if !text.is_empty() {
            set.insert(text);
        }
    }
    Ok(set.into_iter().collect())
}

fn parse_optional_text(
    value: Option<&ciborium::value::Value>,
) -> Result<Option<String>, DharmaError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if matches!(value, ciborium::value::Value::Null) {
        return Ok(None);
    }
    Ok(Some(expect_text(value)?))
}

fn parse_optional_int(value: Option<&ciborium::value::Value>) -> Result<Option<i64>, DharmaError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if matches!(value, ciborium::value::Value::Null) {
        return Ok(None);
    }
    Ok(Some(expect_int(value)?))
}

fn is_expired(expires: Option<i64>, now: i64) -> bool {
    if let Some(exp) = expires {
        exp != 0 && exp <= now
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::crypto;
    use crate::store::state::append_assertion;
    use crate::types::{ContractId, SchemaId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn append_domain_assertion(
        env: &dyn crate::env::Env,
        subject: SubjectId,
        seq: u64,
        typ: &str,
        auth: IdentityKey,
        signing_key: &ed25519_dalek::SigningKey,
        prev: Option<crate::types::AssertionId>,
        body: Value,
    ) -> crate::types::AssertionId {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: typ.to_string(),
            auth,
            seq,
            prev,
            refs: prev.into_iter().collect(),
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, body, signing_key).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        let envelope_id = crypto::envelope_id(&bytes);
        append_assertion(env, &subject, seq, assertion_id, envelope_id, typ, &bytes).unwrap();
        assertion_id
    }

    #[test]
    fn membership_invite_approve_flow() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(100);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (_member_sk, member_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([10u8; 32]);

        let genesis_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text("corp.acme".to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(owner_id.as_bytes().to_vec()),
            ),
        ]);
        let genesis_id = append_domain_assertion(
            env,
            subject,
            1,
            "atlas.domain.genesis",
            owner_id,
            &owner_sk,
            None,
            genesis_body,
        );

        let invite_body = Value::Map(vec![
            (
                Value::Text("target".to_string()),
                Value::Bytes(member_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("roles".to_string()),
                Value::Array(vec![Value::Text("member".to_string())]),
            ),
            (
                Value::Text("scopes".to_string()),
                Value::Array(vec![Value::Text("read".to_string())]),
            ),
            (Value::Text("expires".to_string()), Value::Integer(0.into())),
        ]);
        let invite_id = append_domain_assertion(
            env,
            subject,
            2,
            "atlas.domain.invite",
            owner_id,
            &owner_sk,
            Some(genesis_id),
            invite_body,
        );

        let approve_body = Value::Map(vec![
            (
                Value::Text("target".to_string()),
                Value::Bytes(member_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("roles".to_string()),
                Value::Array(vec![Value::Text("member".to_string())]),
            ),
            (
                Value::Text("scopes".to_string()),
                Value::Array(vec![Value::Text("read".to_string())]),
            ),
            (Value::Text("expires".to_string()), Value::Integer(0.into())),
        ]);
        let _approve_id = append_domain_assertion(
            env,
            subject,
            3,
            "atlas.domain.approve",
            owner_id,
            &owner_sk,
            Some(invite_id),
            approve_body,
        );

        let state = DomainState::load(&store, &subject).unwrap();
        assert!(state.is_member(&member_id, 0));
        let member = state.member(&member_id, 0).unwrap();
        assert!(member.roles.contains(&"member".to_string()));
        assert!(member.scopes.contains(&"read".to_string()));
        let state_again = DomainState::load(&store, &subject).unwrap();
        assert!(state_again.is_member(&member_id, 0));
    }

    #[test]
    fn membership_revoke_removes_access() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(101);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (member_sk, member_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([11u8; 32]);

        let genesis_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text("corp.acme".to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(owner_id.as_bytes().to_vec()),
            ),
        ]);
        let genesis_id = append_domain_assertion(
            env,
            subject,
            1,
            "atlas.domain.genesis",
            owner_id,
            &owner_sk,
            None,
            genesis_body,
        );

        let approve_body = Value::Map(vec![
            (
                Value::Text("target".to_string()),
                Value::Bytes(member_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("roles".to_string()),
                Value::Array(vec![Value::Text("member".to_string())]),
            ),
            (
                Value::Text("scopes".to_string()),
                Value::Array(vec![Value::Text("read".to_string())]),
            ),
            (Value::Text("expires".to_string()), Value::Integer(0.into())),
        ]);
        let approve_id = append_domain_assertion(
            env,
            subject,
            2,
            "atlas.domain.approve",
            owner_id,
            &owner_sk,
            Some(genesis_id),
            approve_body,
        );

        let revoke_body = Value::Map(vec![(
            Value::Text("target".to_string()),
            Value::Bytes(member_id.as_bytes().to_vec()),
        )]);
        let _revoke_id = append_domain_assertion(
            env,
            subject,
            3,
            "atlas.domain.revoke",
            owner_id,
            &owner_sk,
            Some(approve_id),
            revoke_body,
        );

        let state = DomainState::load(&store, &subject).unwrap();
        assert!(!state.is_member(&member_id, 0));
        let _ = member_sk;
    }

    #[test]
    fn membership_expiry() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(102);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (_member_sk, member_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([12u8; 32]);

        let genesis_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text("corp.acme".to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(owner_id.as_bytes().to_vec()),
            ),
        ]);
        let genesis_id = append_domain_assertion(
            env,
            subject,
            1,
            "atlas.domain.genesis",
            owner_id,
            &owner_sk,
            None,
            genesis_body,
        );

        let approve_body = Value::Map(vec![
            (
                Value::Text("target".to_string()),
                Value::Bytes(member_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("roles".to_string()),
                Value::Array(vec![Value::Text("member".to_string())]),
            ),
            (
                Value::Text("scopes".to_string()),
                Value::Array(vec![Value::Text("read".to_string())]),
            ),
            (
                Value::Text("expires".to_string()),
                Value::Integer(10.into()),
            ),
        ]);
        let _approve_id = append_domain_assertion(
            env,
            subject,
            2,
            "atlas.domain.approve",
            owner_id,
            &owner_sk,
            Some(genesis_id),
            approve_body,
        );

        let state = DomainState::load(&store, &subject).unwrap();
        assert!(!state.is_member(&member_id, 20));
    }
}
