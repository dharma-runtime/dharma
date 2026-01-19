use crate::assertion::AssertionPlaintext;
use crate::env::Env;
use crate::error::DharmaError;
use crate::keystore::KeystoreData;
use crate::store::state::list_assertions;
use crate::types::{ContractId, IdentityKey, SchemaId, SubjectId};
use crate::value::{expect_array, expect_bytes, expect_int, expect_map, expect_text, map_get};
use ed25519_dalek::SigningKey;

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

pub fn root_key_for_identity(env: &dyn Env, subject: &SubjectId) -> Result<Option<IdentityKey>, DharmaError> {
    let records = list_assertions(env, subject)?;
    if records.is_empty() {
        return Ok(None);
    }
    let mut best: Option<(u64, IdentityKey)> = None;
    for record in records {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        if assertion.header.sub != *subject {
            continue;
        }
        if assertion.header.seq == 1 || assertion.header.typ == "core.genesis" {
            return Ok(Some(assertion.header.auth));
        }
        if best.map(|(seq, _)| assertion.header.seq < seq).unwrap_or(true) {
            best = Some((assertion.header.seq, assertion.header.auth));
        }
    }
    Ok(best.map(|(_, key)| key))
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
        if assertion.header.typ == "iam.revoke" || assertion.header.typ == "iam.delegate.revoke" {
            let map = expect_map(&assertion.body)?;
            let delegate_bytes = expect_bytes(
                map_get(map, "delegate")
                    .ok_or_else(|| DharmaError::Validation("missing delegate".to_string()))?,
            )?;
            revoked.insert(IdentityKey::from_slice(&delegate_bytes)?);
            out.retain(|entry| entry.key.as_bytes() != delegate_bytes.as_slice());
            continue;
        }
        if assertion.header.typ != "iam.delegate" {
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
        if assertion.header.typ == "identity.profile" && assertion.header.seq >= best_seq {
            best_seq = assertion.header.seq;
            roles = parse_roles(&assertion.body);
        }
    }
    Ok(roles)
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
    match scope {
        "all" => true,
        "chat" => action.contains("chat"),
        _ => false,
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
            typ: "core.genesis".to_string(),
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
            "core.genesis",
            &genesis_bytes,
        )
        .unwrap();

        let delegate_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "iam.delegate".to_string(),
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
            "iam.delegate",
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
            typ: "iam.delegate.revoke".to_string(),
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
            "iam.delegate.revoke",
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
            typ: "iam.delegate".to_string(),
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
            "iam.delegate",
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
            typ: "core.genesis".to_string(),
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
            "core.genesis",
            &genesis_bytes,
        )
        .unwrap();

        let profile_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "identity.profile".to_string(),
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
            "identity.profile",
            &profile_bytes,
        )
        .unwrap();

        assert!(has_role(&env, &subject, "finance.approver").unwrap());
        assert!(!has_role(&env, &subject, "finance.viewer").unwrap());

        let delegate_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "iam.delegate".to_string(),
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
            "iam.delegate",
            &delegate_bytes,
        )
        .unwrap();

        assert!(has_role(&env, &subject, "finance.viewer").unwrap());
    }
}
