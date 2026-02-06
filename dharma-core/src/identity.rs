use crate::assertion::AssertionPlaintext;
use crate::domain::DomainState;
use crate::env::Env;
use crate::error::DharmaError;
use crate::keystore::KeystoreData;
use crate::protocols::atlas_identity as atlas_proto;
use crate::store::Store;
use crate::store::state::list_assertions;
use crate::types::{ContractId, HpkePublicKey, IdentityKey, SchemaId, SubjectId};
use crate::value::{expect_array, expect_bytes, expect_int, expect_map, expect_text, map_get};
use ed25519_dalek::SigningKey;

pub use crate::assertion_types::{
    CORE_GENESIS, IAM_DELEGATE, IAM_DELEGATE_REVOKE, IAM_REVOKE, IDENTITY_PROFILE,
};
pub const ATLAS_IDENTITY_GENESIS: &str = atlas_proto::ASSERTION_GENESIS;
pub const ATLAS_IDENTITY_ACTIVATE: &str = atlas_proto::ASSERTION_ACTIVATE;
pub const ATLAS_IDENTITY_SUSPEND: &str = atlas_proto::ASSERTION_SUSPEND;
pub const ATLAS_IDENTITY_REVOKE: &str = atlas_proto::ASSERTION_REVOKE;

#[derive(Clone)]
pub struct IdentityState {
    pub subject_id: SubjectId,
    pub signing_key: SigningKey,
    pub public_key: IdentityKey,
    pub root_signing_key: SigningKey,
    pub root_public_key: IdentityKey,
    pub subject_key: [u8; 32],
    pub noise_sk: [u8; 32],
    pub schema: SchemaId,
    pub contract: ContractId,
}

impl IdentityState {
    pub fn from_keystore(data: KeystoreData) -> Self {
        let public_key = IdentityKey::from_bytes(data.device_signing_key.verifying_key().to_bytes());
        let root_public_key = IdentityKey::from_bytes(data.root_signing_key.verifying_key().to_bytes());
        Self {
            subject_id: data.identity,
            signing_key: data.device_signing_key,
            public_key,
            root_signing_key: data.root_signing_key,
            root_public_key,
            subject_key: data.subject_key,
            noise_sk: data.noise_sk,
            schema: data.schema,
            contract: data.contract,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelegateInfo {
    pub key: IdentityKey,
    pub scope: String,
    pub expires: Option<i64>,
    pub signer: IdentityKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdentityStatus {
    Active,
    Suspended,
    Revoked,
}

pub fn root_key_for_identity(env: &dyn Env, subject: &SubjectId) -> Result<Option<IdentityKey>, DharmaError> {
    let records = list_assertions(env, subject)?;
    if records.is_empty() {
        return Ok(None);
    }
    let mut best: Option<(u64, IdentityKey)> = None;
    for record in records {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => {
                continue;
            }
        };
        if assertion.header.sub != *subject {
            continue;
        }
        if assertion.header.typ == ATLAS_IDENTITY_GENESIS || assertion.header.typ == CORE_GENESIS {
            return Ok(Some(assertion.header.auth));
        }
        if assertion.header.seq == 1 {
            return Ok(Some(assertion.header.auth));
        }
        if best.map(|(seq, _)| assertion.header.seq < seq).unwrap_or(true) {
            best = Some((assertion.header.seq, assertion.header.auth));
        }
    }
    Ok(best.map(|(_, key)| key))
}

pub fn identity_status(env: &dyn Env, subject: &SubjectId) -> Result<IdentityStatus, DharmaError> {
    let Some(root_key) = root_key_for_identity(env, subject)? else {
        return Ok(IdentityStatus::Active);
    };
    identity_status_with_root(env, subject, &root_key)
}

pub fn is_verified_identity(env: &dyn Env, subject: &SubjectId) -> Result<bool, DharmaError> {
    let mut root_key = None;
    for record in list_assertions(env, subject)? {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        if assertion.header.sub != *subject {
            continue;
        }
        if assertion.header.typ != ATLAS_IDENTITY_GENESIS {
            continue;
        }
        if assertion.header.seq != 1 {
            continue;
        }
        if !assertion.verify_signature()? {
            continue;
        }
        root_key = Some(assertion.header.auth);
        break;
    }
    let Some(root_key) = root_key else {
        return Ok(false);
    };
    let status = identity_status_with_root(env, subject, &root_key)?;
    Ok(status == IdentityStatus::Active)
}

fn identity_status_with_root(
    env: &dyn Env,
    subject: &SubjectId,
    root_key: &IdentityKey,
) -> Result<IdentityStatus, DharmaError> {
    let mut status = IdentityStatus::Active;
    for record in list_assertions(env, subject)? {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        if assertion.header.sub != *subject {
            continue;
        }
        if assertion.header.auth.as_bytes() != root_key.as_bytes() {
            continue;
        }
        if !assertion.verify_signature()? {
            continue;
        }
        match assertion.header.typ.as_str() {
            ATLAS_IDENTITY_SUSPEND => {
                if status != IdentityStatus::Revoked {
                    status = IdentityStatus::Suspended;
                }
            }
            ATLAS_IDENTITY_ACTIVATE => {
                if status != IdentityStatus::Revoked {
                    status = IdentityStatus::Active;
                }
            }
            ATLAS_IDENTITY_REVOKE => {
                status = IdentityStatus::Revoked;
            }
            _ => {}
        }
    }
    Ok(status)
}

pub fn load_delegates(env: &dyn Env, subject: &SubjectId) -> Result<Vec<DelegateInfo>, DharmaError> {
    let mut out = Vec::new();
    let Some(root_key) = root_key_for_identity(env, subject)? else {
        return Ok(out);
    };
    let mut revoked = std::collections::HashSet::new();
    for record in list_assertions(env, subject)? {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        if assertion.header.sub != *subject {
            continue;
        }
        if assertion.header.auth.as_bytes() != root_key.as_bytes() {
            continue;
        }
        if !assertion.verify_signature()? {
            continue;
        }
        if assertion.header.typ == IAM_REVOKE || assertion.header.typ == IAM_DELEGATE_REVOKE {
            let map = expect_map(&assertion.body)?;
            let delegate_bytes = expect_bytes(
                map_get(map, "delegate")
                    .ok_or_else(|| DharmaError::Validation("missing delegate".to_string()))?,
            )?;
            revoked.insert(IdentityKey::from_slice(&delegate_bytes)?);
            out.retain(|entry| entry.key.as_bytes() != delegate_bytes.as_slice());
            continue;
        }
        if assertion.header.typ != IAM_DELEGATE {
            continue;
        }
        let map = expect_map(&assertion.body)?;
        let delegate_bytes = expect_bytes(
            map_get(map, "delegate")
                .ok_or_else(|| DharmaError::Validation("missing delegate".to_string()))?,
        )?;
        let scope = expect_text(
            map_get(map, "scope").ok_or_else(|| DharmaError::Validation("missing scope".to_string()))?,
        )?;
        let expires = match map_get(map, "expires") {
            Some(val) => {
                if matches!(val, ciborium::value::Value::Null) {
                    None
                } else {
                    Some(expect_int(val)?)
                }
            }
            None => None,
        };
        let delegate_key = IdentityKey::from_slice(&delegate_bytes)?;
        if revoked.contains(&delegate_key) {
            revoked.remove(&delegate_key);
        }
        out.retain(|entry| entry.key.as_bytes() != delegate_key.as_bytes());
        out.push(DelegateInfo {
            key: delegate_key,
            scope,
            expires,
            signer: assertion.header.auth,
        });
    }
    Ok(out)
}

pub fn delegate_allows(
    env: &dyn Env,
    subject: &SubjectId,
    signer_key: &IdentityKey,
    action: &str,
    now: i64,
) -> Result<bool, DharmaError> {
    let Some(root_key) = root_key_for_identity(env, subject)? else {
        return Ok(false);
    };
    if signer_key.as_bytes() == root_key.as_bytes() {
        return Ok(true);
    }
    let delegates = load_delegates(env, subject)?;
    for delegate in delegates {
        if delegate.signer.as_bytes() != root_key.as_bytes() {
            continue;
        }
        if delegate.key.as_bytes() != signer_key.as_bytes() {
            continue;
        }
        if let Some(exp) = delegate.expires {
            if exp != 0 && exp <= now {
                continue;
            }
        }
        if scope_allows(&delegate.scope, action) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn device_key_revoked(
    env: &dyn Env,
    subject: &SubjectId,
    signer_key: &IdentityKey,
) -> Result<bool, DharmaError> {
    let Some(root_key) = root_key_for_identity(env, subject)? else {
        return Ok(false);
    };
    if signer_key.as_bytes() == root_key.as_bytes() {
        return Ok(false);
    }
    let mut revoked = false;
    for record in list_assertions(env, subject)? {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        if assertion.header.sub != *subject {
            continue;
        }
        if assertion.header.auth.as_bytes() != root_key.as_bytes() {
            continue;
        }
        if !assertion.verify_signature()? {
            continue;
        }
        match assertion.header.typ.as_str() {
            IAM_REVOKE | IAM_DELEGATE_REVOKE => {
                let map = expect_map(&assertion.body)?;
                let delegate_bytes = expect_bytes(
                    map_get(map, "delegate")
                        .ok_or_else(|| DharmaError::Validation("missing delegate".to_string()))?,
                )?;
                if delegate_bytes.as_slice() == signer_key.as_bytes() {
                    revoked = true;
                }
            }
            IAM_DELEGATE => {
                let map = expect_map(&assertion.body)?;
                let delegate_bytes = expect_bytes(
                    map_get(map, "delegate")
                        .ok_or_else(|| DharmaError::Validation("missing delegate".to_string()))?,
                )?;
                if delegate_bytes.as_slice() == signer_key.as_bytes() {
                    revoked = false;
                }
            }
            _ => {}
        }
    }
    Ok(revoked)
}

pub fn is_member_of_domain(
    store: &Store,
    domain_subject: &SubjectId,
    identity: &IdentityKey,
    now: i64,
) -> Result<bool, DharmaError> {
    let state = DomainState::load(store, domain_subject)?;
    Ok(state.is_member(identity, now))
}

pub fn roles_for_identity(env: &dyn Env, subject: &SubjectId) -> Result<Vec<String>, DharmaError> {
    let Some(root_key) = root_key_for_identity(env, subject)? else {
        return Ok(Vec::new());
    };
    let mut best_seq = 0u64;
    let mut roles = Vec::new();
    for record in list_assertions(env, subject)? {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        if assertion.header.sub != *subject {
            continue;
        }
        if assertion.header.auth.as_bytes() != root_key.as_bytes() {
            continue;
        }
        if !assertion.verify_signature()? {
            continue;
        }
        if assertion.header.typ == IDENTITY_PROFILE && assertion.header.seq >= best_seq {
            best_seq = assertion.header.seq;
            roles = parse_roles(&assertion.body);
        }
    }
    Ok(roles)
}

pub fn hpke_public_key_for_identity(
    env: &dyn Env,
    subject: &SubjectId,
) -> Result<Option<HpkePublicKey>, DharmaError> {
    let Some(root_key) = root_key_for_identity(env, subject)? else {
        return Ok(None);
    };
    let mut best_seq = 0u64;
    let mut hpke_key: Option<HpkePublicKey> = None;
    for record in list_assertions(env, subject)? {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        if assertion.header.sub != *subject {
            continue;
        }
        if assertion.header.auth.as_bytes() != root_key.as_bytes() {
            continue;
        }
        if !assertion.verify_signature()? {
            continue;
        }
        if assertion.header.typ == IDENTITY_PROFILE && assertion.header.seq >= best_seq {
            best_seq = assertion.header.seq;
            let map = match expect_map(&assertion.body) {
                Ok(map) => map,
                Err(_) => continue,
            };
            let Some(val) = map_get(map, "hpke_pk") else { continue };
            let bytes = match expect_bytes(val) {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            if let Ok(key) = HpkePublicKey::from_slice(&bytes) {
                hpke_key = Some(key);
            }
        }
    }
    Ok(hpke_key)
}

pub fn has_role(env: &dyn Env, subject: &SubjectId, role: &str) -> Result<bool, DharmaError> {
    let roles = roles_for_identity(env, subject)?;
    if roles.iter().any(|r| r == role) {
        return Ok(true);
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    for delegate in load_delegates(env, subject)? {
        if let Some(exp) = delegate.expires {
            if exp != 0 && exp <= now {
                continue;
            }
        }
        if delegate_scope_allows_role(&delegate.scope, role) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_roles(body: &ciborium::value::Value) -> Vec<String> {
    let map = match expect_map(body) {
        Ok(map) => map,
        Err(_) => return Vec::new(),
    };
    let Some(roles_val) = map_get(map, "roles") else {
        return Vec::new();
    };
    let arr = match expect_array(roles_val) {
        Ok(arr) => arr,
        Err(_) => return Vec::new(),
    };
    let mut roles = Vec::new();
    for item in arr {
        if let Ok(role) = expect_text(item) {
            roles.push(role);
        }
    }
    roles
}

fn delegate_scope_allows_role(scope: &str, role: &str) -> bool {
    scope == "all" || scope == role
}

fn scope_allows(scope: &str, action: &str) -> bool {
    let scope = scope.trim();
    let action = action.trim();
    if scope == "chat" && action.contains("chat") {
        return true;
    }
    scope_matches_value(scope, action)
}

fn scope_matches_value(scope: &str, value: &str) -> bool {
    if scope.is_empty() || value.is_empty() {
        return false;
    }
    if scope == "all" || scope == "*" {
        return true;
    }
    if scope == value {
        return true;
    }
    if scope_prefix_matches(scope, value) {
        return true;
    }
    has_glob_pattern(scope) && glob_matches(scope, value)
}

fn scope_prefix_matches(scope: &str, value: &str) -> bool {
    if value == scope {
        return true;
    }
    value
        .strip_prefix(scope)
        .map(|rest| rest.starts_with('.') || rest.starts_with(':') || rest.starts_with('/'))
        .unwrap_or(false)
}

fn has_glob_pattern(scope: &str) -> bool {
    scope.bytes().any(|byte| byte == b'*' || byte == b'?')
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let mut p_idx = 0usize;
    let mut v_idx = 0usize;
    let mut star_idx: Option<usize> = None;
    let mut backtrack_v_idx = 0usize;

    while v_idx < value.len() {
        if p_idx < pattern.len() && (pattern[p_idx] == b'?' || pattern[p_idx] == value[v_idx]) {
            p_idx += 1;
            v_idx += 1;
            continue;
        }
        if p_idx < pattern.len() && pattern[p_idx] == b'*' {
            star_idx = Some(p_idx);
            p_idx += 1;
            backtrack_v_idx = v_idx;
            continue;
        }
        if let Some(star) = star_idx {
            p_idx = star + 1;
            backtrack_v_idx += 1;
            v_idx = backtrack_v_idx;
            continue;
        }
        return false;
    }

    while p_idx < pattern.len() && pattern[p_idx] == b'*' {
        p_idx += 1;
    }
    p_idx == pattern.len()
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

    #[test]
    fn delegates_parse_and_allow() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(42);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let (device_sk, device_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([4u8; 32]);

        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: CORE_GENESIS.to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let genesis = AssertionPlaintext::sign(genesis_header, Value::Map(vec![]), &root_sk).unwrap();
        let genesis_bytes = genesis.to_cbor().unwrap();
        let genesis_id = genesis.assertion_id().unwrap();
        let genesis_env = crypto::envelope_id(&genesis_bytes);
        append_assertion(
            &env,
            &subject,
            1,
            genesis_id,
            genesis_env,
            CORE_GENESIS,
            &genesis_bytes,
        )
        .unwrap();

        let delegate_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: IAM_DELEGATE.to_string(),
            auth: root_id,
            seq: 2,
            prev: Some(genesis_id),
            refs: vec![genesis_id],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![
            (
                Value::Text("delegate".to_string()),
                Value::Bytes(device_sk.verifying_key().to_bytes().to_vec()),
            ),
            (Value::Text("scope".to_string()), Value::Text("all".to_string())),
            (Value::Text("expires".to_string()), Value::Integer(0.into())),
        ]);
        let delegate = AssertionPlaintext::sign(delegate_header, body, &root_sk).unwrap();
        let delegate_bytes = delegate.to_cbor().unwrap();
        let delegate_id = delegate.assertion_id().unwrap();
        let delegate_env = crypto::envelope_id(&delegate_bytes);
        append_assertion(
            &env,
            &subject,
            2,
            delegate_id,
            delegate_env,
            IAM_DELEGATE,
            &delegate_bytes,
        )
        .unwrap();

        let allowed = delegate_allows(
            &env,
            &subject,
            &IdentityKey::from_bytes(device_sk.verifying_key().to_bytes()),
            "action.Test",
            0,
        )
        .unwrap();
        assert!(allowed);

        let revoke_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: IAM_DELEGATE_REVOKE.to_string(),
            auth: root_id,
            seq: 3,
            prev: Some(delegate_id),
            refs: vec![delegate_id],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let revoke_body = Value::Map(vec![(
            Value::Text("delegate".to_string()),
            Value::Bytes(device_id.as_bytes().to_vec()),
        )]);
        let revoke = AssertionPlaintext::sign(revoke_header, revoke_body, &root_sk).unwrap();
        let revoke_bytes = revoke.to_cbor().unwrap();
        let revoke_id = revoke.assertion_id().unwrap();
        let revoke_env = crypto::envelope_id(&revoke_bytes);
        append_assertion(
            &env,
            &subject,
            3,
            revoke_id,
            revoke_env,
            IAM_DELEGATE_REVOKE,
            &revoke_bytes,
        )
        .unwrap();

        let allowed_after = delegate_allows(
            &env,
            &subject,
            &IdentityKey::from_bytes(device_sk.verifying_key().to_bytes()),
            "action.Test",
            0,
        )
        .unwrap();
        assert!(!allowed_after);

        let redelegate_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: IAM_DELEGATE.to_string(),
            auth: root_id,
            seq: 4,
            prev: Some(revoke_id),
            refs: vec![revoke_id],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let redelegate_body = Value::Map(vec![
            (
                Value::Text("delegate".to_string()),
                Value::Bytes(device_id.as_bytes().to_vec()),
            ),
            (Value::Text("scope".to_string()), Value::Text("all".to_string())),
            (Value::Text("expires".to_string()), Value::Integer(0.into())),
        ]);
        let redelegate = AssertionPlaintext::sign(redelegate_header, redelegate_body, &root_sk).unwrap();
        let redelegate_bytes = redelegate.to_cbor().unwrap();
        let redelegate_id = redelegate.assertion_id().unwrap();
        let redelegate_env = crypto::envelope_id(&redelegate_bytes);
        append_assertion(
            &env,
            &subject,
            4,
            redelegate_id,
            redelegate_env,
            IAM_DELEGATE,
            &redelegate_bytes,
        )
        .unwrap();

        let allowed_after_redelegate = delegate_allows(
            &env,
            &subject,
            &IdentityKey::from_bytes(device_sk.verifying_key().to_bytes()),
            "action.Test",
            0,
        )
        .unwrap();
        assert!(allowed_after_redelegate);
    }

    #[test]
    fn roles_from_identity_profile() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(7);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let (device_sk, _device_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([9u8; 32]);

        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: CORE_GENESIS.to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let genesis = AssertionPlaintext::sign(genesis_header, Value::Map(vec![]), &root_sk).unwrap();
        let genesis_bytes = genesis.to_cbor().unwrap();
        let genesis_id = genesis.assertion_id().unwrap();
        let genesis_env = crypto::envelope_id(&genesis_bytes);
        append_assertion(
            &env,
            &subject,
            1,
            genesis_id,
            genesis_env,
            CORE_GENESIS,
            &genesis_bytes,
        )
        .unwrap();

        let profile_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: IDENTITY_PROFILE.to_string(),
            auth: root_id,
            seq: 2,
            prev: Some(genesis_id),
            refs: vec![genesis_id],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![(
            Value::Text("roles".to_string()),
            Value::Array(vec![Value::Text("finance.approver".to_string())]),
        )]);
        let profile = AssertionPlaintext::sign(profile_header, body, &root_sk).unwrap();
        let profile_bytes = profile.to_cbor().unwrap();
        let profile_id = profile.assertion_id().unwrap();
        let profile_env = crypto::envelope_id(&profile_bytes);
        append_assertion(
            &env,
            &subject,
            2,
            profile_id,
            profile_env,
            IDENTITY_PROFILE,
            &profile_bytes,
        )
        .unwrap();

        assert!(has_role(&env, &subject, "finance.approver").unwrap());
        assert!(!has_role(&env, &subject, "finance.viewer").unwrap());

        let delegate_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: IAM_DELEGATE.to_string(),
            auth: root_id,
            seq: 3,
            prev: Some(profile_id),
            refs: vec![profile_id],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let delegate_body = Value::Map(vec![
            (
                Value::Text("delegate".to_string()),
                Value::Bytes(device_sk.verifying_key().to_bytes().to_vec()),
            ),
            (
                Value::Text("scope".to_string()),
                Value::Text("finance.viewer".to_string()),
            ),
            (Value::Text("expires".to_string()), Value::Integer(0.into())),
        ]);
        let delegate = AssertionPlaintext::sign(delegate_header, delegate_body, &root_sk).unwrap();
        let delegate_bytes = delegate.to_cbor().unwrap();
        let delegate_id = delegate.assertion_id().unwrap();
        let delegate_env = crypto::envelope_id(&delegate_bytes);
        append_assertion(
            &env,
            &subject,
            3,
            delegate_id,
            delegate_env,
            IAM_DELEGATE,
            &delegate_bytes,
        )
        .unwrap();

        assert!(has_role(&env, &subject, "finance.viewer").unwrap());
    }

    #[test]
    fn scope_allows_supports_prefix_and_glob_matching() {
        assert!(scope_allows("all", "action.Touch"));
        assert!(scope_allows("*", "action.Touch"));
        assert!(scope_allows("action.Touch", "action.Touch"));
        assert!(scope_allows("action", "action.Touch"));
        assert!(scope_allows("finance", "finance.approve"));
        assert!(scope_allows("finance.*", "finance.approve"));
        assert!(scope_allows("finance.?", "finance.a"));
        assert!(scope_allows("chat", "channel.chat.send"));

        assert!(!scope_allows("", "action.Touch"));
        assert!(!scope_allows("fin", "finance.approve"));
        assert!(!scope_allows("finance.approver", "finance.approve"));
        assert!(!scope_allows("finance.?", "finance.approve"));
    }

    #[test]
    fn atlas_status_transitions() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(12);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([11u8; 32]);
        let schema = SchemaId::from_bytes([3u8; 32]);
        let contract = ContractId::from_bytes([4u8; 32]);

        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: ATLAS_IDENTITY_GENESIS.to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema,
            contract,
            note: None,
            meta: None,
        };
        let genesis_body = Value::Map(vec![
            (
                Value::Text("atlas_name".to_string()),
                Value::Text("person.local.alice_abcd".to_string()),
            ),
            (
                Value::Text("owner_key".to_string()),
                Value::Bytes(root_id.as_bytes().to_vec()),
            ),
        ]);
        let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &root_sk).unwrap();
        let genesis_bytes = genesis.to_cbor().unwrap();
        let genesis_id = genesis.assertion_id().unwrap();
        let genesis_env = crypto::envelope_id(&genesis_bytes);
        append_assertion(
            &env,
            &subject,
            1,
            genesis_id,
            genesis_env,
            ATLAS_IDENTITY_GENESIS,
            &genesis_bytes,
        )
        .unwrap();

        assert_eq!(identity_status(&env, &subject).unwrap(), IdentityStatus::Active);

        let suspend_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: ATLAS_IDENTITY_SUSPEND.to_string(),
            auth: root_id,
            seq: 2,
            prev: Some(genesis_id),
            refs: vec![genesis_id],
            ts: None,
            schema,
            contract,
            note: None,
            meta: None,
        };
        let suspend = AssertionPlaintext::sign(suspend_header, Value::Map(vec![]), &root_sk).unwrap();
        let suspend_bytes = suspend.to_cbor().unwrap();
        let suspend_id = suspend.assertion_id().unwrap();
        let suspend_env = crypto::envelope_id(&suspend_bytes);
        append_assertion(
            &env,
            &subject,
            2,
            suspend_id,
            suspend_env,
            ATLAS_IDENTITY_SUSPEND,
            &suspend_bytes,
        )
        .unwrap();
        assert_eq!(identity_status(&env, &subject).unwrap(), IdentityStatus::Suspended);

        let activate_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: ATLAS_IDENTITY_ACTIVATE.to_string(),
            auth: root_id,
            seq: 3,
            prev: Some(suspend_id),
            refs: vec![suspend_id],
            ts: None,
            schema,
            contract,
            note: None,
            meta: None,
        };
        let activate = AssertionPlaintext::sign(activate_header, Value::Map(vec![]), &root_sk).unwrap();
        let activate_bytes = activate.to_cbor().unwrap();
        let activate_id = activate.assertion_id().unwrap();
        let activate_env = crypto::envelope_id(&activate_bytes);
        append_assertion(
            &env,
            &subject,
            3,
            activate_id,
            activate_env,
            ATLAS_IDENTITY_ACTIVATE,
            &activate_bytes,
        )
        .unwrap();
        assert_eq!(identity_status(&env, &subject).unwrap(), IdentityStatus::Active);

        let revoke_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: ATLAS_IDENTITY_REVOKE.to_string(),
            auth: root_id,
            seq: 4,
            prev: Some(activate_id),
            refs: vec![activate_id],
            ts: None,
            schema,
            contract,
            note: None,
            meta: None,
        };
        let revoke = AssertionPlaintext::sign(revoke_header, Value::Map(vec![]), &root_sk).unwrap();
        let revoke_bytes = revoke.to_cbor().unwrap();
        let revoke_id = revoke.assertion_id().unwrap();
        let revoke_env = crypto::envelope_id(&revoke_bytes);
        append_assertion(
            &env,
            &subject,
            4,
            revoke_id,
            revoke_env,
            ATLAS_IDENTITY_REVOKE,
            &revoke_bytes,
        )
        .unwrap();
        assert_eq!(identity_status(&env, &subject).unwrap(), IdentityStatus::Revoked);

        let reactivate_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: ATLAS_IDENTITY_ACTIVATE.to_string(),
            auth: root_id,
            seq: 5,
            prev: Some(revoke_id),
            refs: vec![revoke_id],
            ts: None,
            schema,
            contract,
            note: None,
            meta: None,
        };
        let reactivate = AssertionPlaintext::sign(reactivate_header, Value::Map(vec![]), &root_sk).unwrap();
        let reactivate_bytes = reactivate.to_cbor().unwrap();
        let reactivate_id = reactivate.assertion_id().unwrap();
        let reactivate_env = crypto::envelope_id(&reactivate_bytes);
        append_assertion(
            &env,
            &subject,
            5,
            reactivate_id,
            reactivate_env,
            ATLAS_IDENTITY_ACTIVATE,
            &reactivate_bytes,
        )
        .unwrap();
        assert_eq!(identity_status(&env, &subject).unwrap(), IdentityStatus::Revoked);
    }

    #[test]
    fn atlas_verified_only_if_active() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(13);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([12u8; 32]);
        let schema = SchemaId::from_bytes([3u8; 32]);
        let contract = ContractId::from_bytes([4u8; 32]);

        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: ATLAS_IDENTITY_GENESIS.to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema,
            contract,
            note: None,
            meta: None,
        };
        let genesis_body = Value::Map(vec![
            (
                Value::Text("atlas_name".to_string()),
                Value::Text("person.local.bob_abcd".to_string()),
            ),
            (
                Value::Text("owner_key".to_string()),
                Value::Bytes(root_id.as_bytes().to_vec()),
            ),
        ]);
        let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &root_sk).unwrap();
        let genesis_bytes = genesis.to_cbor().unwrap();
        let genesis_id = genesis.assertion_id().unwrap();
        let genesis_env = crypto::envelope_id(&genesis_bytes);
        append_assertion(
            &env,
            &subject,
            1,
            genesis_id,
            genesis_env,
            ATLAS_IDENTITY_GENESIS,
            &genesis_bytes,
        )
        .unwrap();
        assert!(is_verified_identity(&env, &subject).unwrap());

        let suspend_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: ATLAS_IDENTITY_SUSPEND.to_string(),
            auth: root_id,
            seq: 2,
            prev: Some(genesis_id),
            refs: vec![genesis_id],
            ts: None,
            schema,
            contract,
            note: None,
            meta: None,
        };
        let suspend = AssertionPlaintext::sign(suspend_header, Value::Map(vec![]), &root_sk).unwrap();
        let suspend_bytes = suspend.to_cbor().unwrap();
        let suspend_id = suspend.assertion_id().unwrap();
        let suspend_env = crypto::envelope_id(&suspend_bytes);
        append_assertion(
            &env,
            &subject,
            2,
            suspend_id,
            suspend_env,
            ATLAS_IDENTITY_SUSPEND,
            &suspend_bytes,
        )
        .unwrap();
        assert!(!is_verified_identity(&env, &subject).unwrap());
    }
}
