use crate::assertion::AssertionPlaintext;
use crate::env::Env;
use crate::error::DharmaError;
use crate::identity::{delegate_allows, is_verified_identity, root_key_for_identity, IDENTITY_PROFILE};
use crate::net::policy::PeerClaims;
use crate::store::state::list_assertions;
use crate::types::{IdentityKey, SubjectId};
use crate::value::{expect_array, expect_map, expect_text, map_get};
use ciborium::value::Value;
pub fn verify_peer_identity(
    env: &dyn Env,
    subject: &SubjectId,
    public_key: &IdentityKey,
) -> Result<Option<PeerClaims>, DharmaError> {
    let mut claims = PeerClaims::default();
    let mut best_profile_seq = 0u64;
    let root_key = match root_key_for_identity(env, subject)? {
        Some(key) => key,
        None => return Ok(None),
    };
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
        if assertion.header.typ == IDENTITY_PROFILE && assertion.header.seq >= best_profile_seq {
            best_profile_seq = assertion.header.seq;
            claims = parse_claims(&assertion.body);
        }
    }
    if !is_verified_identity(env, subject)? {
        return Ok(None);
    }
    if public_key.as_bytes() != root_key.as_bytes() {
        let now = 0i64;
        if delegate_allows(env, subject, public_key, "sync", now)? {
            return Ok(Some(claims));
        }
        return Ok(None);
    }
    Ok(Some(claims))
}

fn parse_claims(body: &Value) -> PeerClaims {
    let map = match expect_map(body) {
        Ok(map) => map,
        Err(_) => return PeerClaims::default(),
    };
    let mut claims = PeerClaims::default();
    if let Some(org_val) = map_get(map, "org") {
        if let Ok(org) = expect_text(org_val) {
            claims.org = Some(org);
        }
    }
    if let Some(roles_val) = map_get(map, "roles") {
        if let Ok(arr) = expect_array(roles_val) {
            for item in arr {
                if let Ok(role) = expect_text(item) {
                    claims.roles.push(role);
                }
            }
        }
    }
    claims
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::crypto;
    use crate::identity::ATLAS_IDENTITY_GENESIS;
    use crate::store::state::append_assertion;
    use crate::types::{ContractId, SchemaId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn verify_peer_identity_extracts_claims() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(9);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([1u8; 32]);
        let schema = SchemaId::from_bytes([2u8; 32]);
        let contract = ContractId::from_bytes([3u8; 32]);
        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: ATLAS_IDENTITY_GENESIS.to_string(),
            auth: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: Vec::new(),
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
                Value::Bytes(signing_key.verifying_key().to_bytes().to_vec()),
            ),
        ]);
        let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &signing_key).unwrap();
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
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "identity.profile".to_string(),
            auth: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 2,
            prev: Some(genesis_id),
            refs: vec![genesis_id],
            ts: None,
            schema,
            contract,
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![
            (Value::Text("org".to_string()), Value::Text("cmdv".to_string())),
            (
                Value::Text("roles".to_string()),
                Value::Array(vec![Value::Text("Accountant".to_string())]),
            ),
        ]);
        let assertion = AssertionPlaintext::sign(header, body, &signing_key).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        let envelope_id = crypto::envelope_id(&bytes);
        append_assertion(
            &env,
            &subject,
            2,
            assertion_id,
            envelope_id,
            "identity.profile",
            &bytes,
        )
        .unwrap();

        let claims = verify_peer_identity(
            &env,
            &subject,
            &crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(claims.org.as_deref(), Some("cmdv"));
        assert!(claims.roles.contains(&"Accountant".to_string()));
    }
}
