use crate::error::DharmaError;

pub mod atlas_domain;
pub mod atlas_identity;
pub mod contacts;
pub mod iam;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProtocolId {
    pub name: String,
    pub version: u64,
}

impl ProtocolId {
    pub fn new(name: impl Into<String>, version: u64) -> Self {
        Self {
            name: name.into(),
            version,
        }
    }

    pub fn parse(raw: &str) -> Result<Self, DharmaError> {
        let (name, ver) = raw
            .rsplit_once('@')
            .ok_or_else(|| DharmaError::Validation("missing protocol version".to_string()))?;
        if name.trim().is_empty() {
            return Err(DharmaError::Validation("missing protocol name".to_string()));
        }
        let ver = ver.trim_start_matches('v');
        let version = ver
            .parse::<u64>()
            .map_err(|_| DharmaError::Validation("invalid protocol version".to_string()))?;
        Ok(Self::new(name.trim(), version))
    }

    pub fn as_string(&self) -> String {
        format!("{}@{}", self.name, self.version)
    }
}

impl std::fmt::Display for ProtocolId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.name, self.version)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolEnumDef {
    pub name: &'static str,
    pub variants: &'static [&'static str],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolInterface {
    pub id: ProtocolId,
    pub required_state_fields: &'static [&'static str],
    pub required_actions: &'static [&'static str],
    pub required_assertions: &'static [&'static str],
    pub required_enums: &'static [ProtocolEnumDef],
    pub private_fields: &'static [&'static str],
}

pub fn registry() -> Vec<ProtocolInterface> {
    vec![
        contacts::interface(),
        iam::interface(),
        atlas_identity::interface(),
        atlas_domain::interface(),
    ]
}

pub fn lookup(id: &ProtocolId) -> Option<ProtocolInterface> {
    match id.name.as_str() {
        contacts::PROTOCOL_NAME if id.version == contacts::PROTOCOL_VERSION => {
            Some(contacts::interface())
        }
        iam::PROTOCOL_NAME if id.version == iam::PROTOCOL_VERSION => Some(iam::interface()),
        atlas_identity::PROTOCOL_NAME if id.version == atlas_identity::PROTOCOL_VERSION => {
            Some(atlas_identity::interface())
        }
        atlas_domain::PROTOCOL_NAME if id.version == atlas_domain::PROTOCOL_VERSION => {
            Some(atlas_domain::interface())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::crypto;
    use crate::domain::DomainState;
    use crate::env::StdEnv;
    use crate::identity::IdentityStatus;
    use crate::store::state::append_assertion;
    use crate::store::Store;
    use crate::types::{AssertionId, ContractId, SchemaId, SubjectId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn assert_contains(list: &[&str], item: &str) {
        assert!(
            list.iter().any(|value| *value == item),
            "expected list to contain {item}"
        );
    }

    #[test]
    fn protocol_id_parse_roundtrip() {
        let id = ProtocolId::parse("std.protocol.contacts@1").unwrap();
        assert_eq!(id.name, "std.protocol.contacts");
        assert_eq!(id.version, 1);
        assert_eq!(id.as_string(), "std.protocol.contacts@1");
        let roundtrip = ProtocolId::parse(&id.as_string()).unwrap();
        assert_eq!(roundtrip, id);
    }

    #[test]
    fn contacts_interface_minimal_schema_ok() {
        let iface = contacts::interface();
        assert_contains(iface.required_actions, "Create");
        assert_contains(iface.required_actions, "Accept");
        assert_contains(iface.required_state_fields, "owner");
        assert_contains(iface.required_state_fields, "relation");
        let relation = iface
            .required_enums
            .iter()
            .find(|item| item.name == "Relation")
            .expect("relation enum present");
        assert_contains(relation.variants, "Accepted");
    }

    #[test]
    fn iam_interface_minimal_schema_ok() {
        let iface = iam::interface();
        assert_contains(iface.required_state_fields, "display_name");
        assert_contains(iface.required_state_fields, "email");
        assert_contains(iface.required_actions, "UpdateEmail");
        assert_contains(iface.private_fields, "email");
    }

    #[test]
    fn iam_interface_compatibility_ok() {
        let iface = iam::interface();
        assert_contains(iface.required_state_fields, "handle");
        assert_contains(iface.required_state_fields, "profile");
        assert_contains(iface.required_state_fields, "delegates");
        assert_contains(iface.required_actions, "Delegate");
        assert_contains(iface.required_actions, "RevokeDelegate");
    }

    #[test]
    fn iam_private_field_list_contains_display_name_email_phone() {
        let fields = iam::private_fields();
        assert_contains(fields, "display_name");
        assert_contains(fields, "email");
        assert_contains(fields, "phone");
    }

    #[test]
    fn atlas_identity_protocol_invariants_basic() {
        let iface = atlas_identity::interface();
        assert_contains(iface.required_assertions, "atlas.identity.genesis");
        assert_contains(iface.required_assertions, "atlas.identity.revoke");
    }

    #[test]
    fn atlas_identity_protocol_genesis_fields_required() {
        let iface = atlas_identity::interface();
        assert_contains(iface.required_state_fields, "atlas_name");
        assert_contains(iface.required_state_fields, "owner_key");
        assert_contains(iface.required_state_fields, "schema");
        assert_contains(iface.required_state_fields, "contract");
    }

    #[test]
    fn atlas_identity_protocol_revoked_terminal() {
        let temp = tempfile::tempdir().unwrap();
        let env = StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(91);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([91u8; 32]);
        let schema = SchemaId::from_bytes([3u8; 32]);
        let contract = ContractId::from_bytes([4u8; 32]);

        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: atlas_identity::ASSERTION_GENESIS.to_string(),
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
                Value::Text("person.local.proto_revoked".to_string()),
            ),
            (
                Value::Text("owner_key".to_string()),
                Value::Bytes(root_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("schema".to_string()),
                Value::Bytes(schema.as_bytes().to_vec()),
            ),
            (
                Value::Text("contract".to_string()),
                Value::Bytes(contract.as_bytes().to_vec()),
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
            atlas_identity::ASSERTION_GENESIS,
            &genesis_bytes,
        )
        .unwrap();

        let revoke_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: atlas_identity::ASSERTION_REVOKE.to_string(),
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
        let revoke = AssertionPlaintext::sign(revoke_header, Value::Map(vec![]), &root_sk).unwrap();
        let revoke_bytes = revoke.to_cbor().unwrap();
        let revoke_id = revoke.assertion_id().unwrap();
        let revoke_env = crypto::envelope_id(&revoke_bytes);
        append_assertion(
            &env,
            &subject,
            2,
            revoke_id,
            revoke_env,
            atlas_identity::ASSERTION_REVOKE,
            &revoke_bytes,
        )
        .unwrap();

        let activate_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: atlas_identity::ASSERTION_ACTIVATE.to_string(),
            auth: root_id,
            seq: 3,
            prev: Some(revoke_id),
            refs: vec![revoke_id],
            ts: None,
            schema,
            contract,
            note: None,
            meta: None,
        };
        let activate =
            AssertionPlaintext::sign(activate_header, Value::Map(vec![]), &root_sk).unwrap();
        let activate_bytes = activate.to_cbor().unwrap();
        let activate_id = activate.assertion_id().unwrap();
        let activate_env = crypto::envelope_id(&activate_bytes);
        append_assertion(
            &env,
            &subject,
            3,
            activate_id,
            activate_env,
            atlas_identity::ASSERTION_ACTIVATE,
            &activate_bytes,
        )
        .unwrap();

        let status = atlas_identity::identity_status_v1(&env, &subject).unwrap();
        assert_eq!(status, IdentityStatus::Revoked);
    }

    #[test]
    fn atlas_identity_protocol_suspend_unverified() {
        let temp = tempfile::tempdir().unwrap();
        let env = StdEnv::new(temp.path());
        let mut rng = StdRng::seed_from_u64(92);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([92u8; 32]);
        let schema = SchemaId::from_bytes([3u8; 32]);
        let contract = ContractId::from_bytes([4u8; 32]);

        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: atlas_identity::ASSERTION_GENESIS.to_string(),
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
                Value::Text("person.local.proto_suspend".to_string()),
            ),
            (
                Value::Text("owner_key".to_string()),
                Value::Bytes(root_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("schema".to_string()),
                Value::Bytes(schema.as_bytes().to_vec()),
            ),
            (
                Value::Text("contract".to_string()),
                Value::Bytes(contract.as_bytes().to_vec()),
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
            atlas_identity::ASSERTION_GENESIS,
            &genesis_bytes,
        )
        .unwrap();

        let suspend_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: atlas_identity::ASSERTION_SUSPEND.to_string(),
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
        let suspend =
            AssertionPlaintext::sign(suspend_header, Value::Map(vec![]), &root_sk).unwrap();
        let suspend_bytes = suspend.to_cbor().unwrap();
        let suspend_id = suspend.assertion_id().unwrap();
        let suspend_env = crypto::envelope_id(&suspend_bytes);
        append_assertion(
            &env,
            &subject,
            2,
            suspend_id,
            suspend_env,
            atlas_identity::ASSERTION_SUSPEND,
            &suspend_bytes,
        )
        .unwrap();

        let verified = atlas_identity::is_verified_v1(&env, &subject).unwrap();
        assert!(!verified);
    }

    #[test]
    fn atlas_domain_protocol_invariants_basic() {
        let iface = atlas_domain::interface();
        assert_contains(iface.required_assertions, "atlas.domain.genesis");
        assert_contains(iface.required_assertions, "atlas.domain.approve");
        assert_contains(iface.required_assertions, "atlas.domain.policy");
        assert_contains(iface.required_assertions, "domain.compromised");
    }

    #[test]
    fn atlas_domain_protocol_required_actions() {
        let iface = atlas_domain::interface();
        assert_contains(iface.required_actions, "Genesis");
        assert_contains(iface.required_actions, "Invite");
        assert_contains(iface.required_actions, "Request");
        assert_contains(iface.required_actions, "Approve");
        assert_contains(iface.required_actions, "Revoke");
        assert_contains(iface.required_actions, "Leave");
        assert_contains(iface.required_actions, "Policy");
    }

    #[test]
    fn atlas_domain_protocol_parent_authorization() {
        assert!(atlas_domain::requires_parent_authorization("corp.acme"));
        assert!(!atlas_domain::requires_parent_authorization("corp"));
    }

    #[test]
    fn atlas_domain_protocol_membership_chain_validity() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(101);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (member_sk, member_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([101u8; 32]);
        let schema = SchemaId::from_bytes([3u8; 32]);
        let contract = ContractId::from_bytes([4u8; 32]);

        let mut seq = 1u64;
        let mut prev: Option<AssertionId> = None;
        let mut append = |typ: &str, auth, sk, body| {
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
                schema,
                contract,
                note: None,
                meta: None,
            };
            let assertion = AssertionPlaintext::sign(header, body, sk).unwrap();
            let bytes = assertion.to_cbor().unwrap();
            let assertion_id = assertion.assertion_id().unwrap();
            let envelope_id = crypto::envelope_id(&bytes);
            append_assertion(env, &subject, seq, assertion_id, envelope_id, typ, &bytes).unwrap();
            prev = Some(assertion_id);
            seq += 1;
        };

        append(
            atlas_domain::ASSERTION_GENESIS,
            owner_id,
            &owner_sk,
            Value::Map(vec![
                (
                    Value::Text("domain".to_string()),
                    Value::Text("corp.proto".to_string()),
                ),
                (
                    Value::Text("owner".to_string()),
                    Value::Bytes(owner_id.as_bytes().to_vec()),
                ),
            ]),
        );

        append(
            atlas_domain::ASSERTION_INVITE,
            owner_id,
            &owner_sk,
            Value::Map(vec![
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
            ]),
        );

        append(
            atlas_domain::ASSERTION_APPROVE,
            owner_id,
            &owner_sk,
            Value::Map(vec![
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
            ]),
        );

        let state = DomainState::load(&store, &subject).unwrap();
        let member = state.member(&member_id, 0).expect("member present");
        assert!(member.roles.iter().any(|role| role == "member"));
        assert!(member.scopes.iter().any(|scope| scope == "read"));
        assert!(state.invites.is_empty());
        assert!(state.requests.is_empty());

        let leave_body = Value::Map(vec![(
            Value::Text("target".to_string()),
            Value::Bytes(member_id.as_bytes().to_vec()),
        )]);
        append(
            atlas_domain::ASSERTION_LEAVE,
            member_id,
            &member_sk,
            leave_body,
        );
        let state = DomainState::load(&store, &subject).unwrap();
        assert!(!state.is_member(&member_id, 0));
    }
}
