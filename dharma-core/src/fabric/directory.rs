use crate::assertion::AssertionPlaintext;
use crate::crypto;
use crate::error::DharmaError;
use crate::fabric::types::Advertisement;
use crate::net::handshake;
use crate::net::policy::{OverlayAccess, OverlayPolicy, PeerClaims};
use crate::net::sync::{sync_loop_with, SyncOptions};
use crate::store::index::FrontierIndex;
use crate::store::state::list_assertions;
use crate::store::Store;
use crate::sync::Subscriptions;
use crate::types::{IdentityKey, SubjectId};
use crate::value::{expect_bytes, expect_map, expect_text, map_get};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default)]
pub struct DirectoryState {
    pub domains: HashMap<String, IdentityKey>,
    pub policies: HashMap<String, [u8; 32]>,
    pub pending: HashMap<String, IdentityKey>,
    pub authorizations: HashMap<String, IdentityKey>,
}

impl DirectoryState {
    pub fn load(store: &Store, subject: &SubjectId) -> Result<Self, DharmaError> {
        let mut state = DirectoryState::default();
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
                "fabric.domain.request" => state.apply_domain_request(&assertion)?,
                "fabric.domain.authorize" => state.apply_domain_authorize(&assertion)?,
                "fabric.domain.register" => state.apply_domain_register(&assertion)?,
                "fabric.domain.policy" => state.apply_domain_policy(&assertion)?,
                _ => {}
            }
        }
        Ok(state)
    }

    pub fn owner_for_domain(&self, domain: &str) -> Option<IdentityKey> {
        self.domains.get(domain).copied()
    }

    pub fn policy_hash_for_domain(&self, domain: &str) -> Option<[u8; 32]> {
        self.policies.get(domain).copied()
    }

    pub fn validate_ad(&self, ad: &Advertisement) -> bool {
        match self.policy_hash_for_domain(&ad.domain) {
            Some(hash) => hash == ad.policy_hash,
            None => false,
        }
    }

    fn apply_domain_request(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let domain = expect_text(
            map_get(map, "domain")
                .ok_or_else(|| DharmaError::Validation("missing domain".to_string()))?,
        )?;
        let owner = parse_identity(
            map_get(map, "owner")
                .ok_or_else(|| DharmaError::Validation("missing owner".to_string()))?,
        )?;
        self.pending.insert(domain, owner);
        Ok(())
    }

    fn apply_domain_authorize(
        &mut self,
        assertion: &AssertionPlaintext,
    ) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let domain = expect_text(
            map_get(map, "domain")
                .ok_or_else(|| DharmaError::Validation("missing domain".to_string()))?,
        )?;
        let parent = expect_text(
            map_get(map, "parent")
                .ok_or_else(|| DharmaError::Validation("missing parent".to_string()))?,
        )?;
        let owner = parse_identity(
            map_get(map, "authorized_owner")
                .ok_or_else(|| DharmaError::Validation("missing authorized_owner".to_string()))?,
        )?;
        if let Some(parent_owner) = self.domains.get(&parent) {
            if parent_owner.as_bytes() != assertion.header.auth.as_bytes() {
                return Ok(());
            }
        }
        self.authorizations.insert(domain, owner);
        Ok(())
    }

    fn apply_domain_register(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let domain = expect_text(
            map_get(map, "domain")
                .ok_or_else(|| DharmaError::Validation("missing domain".to_string()))?,
        )?;
        let owner = parse_identity(
            map_get(map, "owner")
                .ok_or_else(|| DharmaError::Validation("missing owner".to_string()))?,
        )?;
        if domain.contains('.') {
            if let Some(authorized) = self.authorizations.get(&domain) {
                if authorized.as_bytes() != owner.as_bytes() {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        }
        self.domains.insert(domain, owner);
        Ok(())
    }

    fn apply_domain_policy(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let domain = expect_text(
            map_get(map, "domain")
                .ok_or_else(|| DharmaError::Validation("missing domain".to_string()))?,
        )?;
        let policy_hex = expect_text(
            map_get(map, "policy_hash")
                .ok_or_else(|| DharmaError::Validation("missing policy_hash".to_string()))?,
        )?;
        let bytes = crate::types::hex_decode(&policy_hex)?;
        if bytes.len() != 32 {
            return Err(DharmaError::Validation("policy_hash length".to_string()));
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes);
        self.policies.insert(domain, hash);
        Ok(())
    }
}

pub struct DirectoryClient {
    seeds: Vec<String>,
    subject: SubjectId,
    ad_store: Arc<Mutex<crate::fabric::types::AdStore>>,
}

impl DirectoryClient {
    pub fn new(seeds: Vec<String>, subject: SubjectId) -> Self {
        Self {
            seeds,
            subject,
            ad_store: Arc::new(Mutex::new(crate::fabric::types::AdStore::new())),
        }
    }

    pub fn default_subject() -> SubjectId {
        SubjectId::from_bytes(crypto::sha256(b"sys.directory"))
    }

    pub fn ad_store(&self) -> Arc<Mutex<crate::fabric::types::AdStore>> {
        self.ad_store.clone()
    }

    pub fn sync(
        &self,
        identity: &crate::identity::IdentityState,
        store: &Store,
        index: &mut FrontierIndex,
    ) -> Result<(), DharmaError> {
        let subs = Subscriptions {
            all: false,
            subjects: vec![self.subject],
            namespaces: Vec::new(),
        };
        for seed in &self.seeds {
            let stream = match seed.parse::<SocketAddr>() {
                Ok(sock) => TcpStream::connect(sock),
                Err(_) => TcpStream::connect(seed),
            };
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => continue,
            };
            let session = handshake::client_handshake(&mut stream, identity)?;
            let policy = OverlayPolicy::load(store.root());
            let claims = PeerClaims::default();
            let access = OverlayAccess::new(&policy, None, false, &claims);
            let mut keys = HashMap::new();
            keys.insert(identity.subject_id, identity.subject_key);
            let options = SyncOptions {
                relay: false,
                ad_store: Some(self.ad_store.clone()),
                local_subs: Some(subs.clone()),
                verbose: false,
                exit_on_idle: true,
                trace: None,
            };
            let _ = sync_loop_with(
                &mut stream,
                session,
                store,
                index,
                &keys,
                identity,
                &access,
                options,
            );
            return Ok(());
        }
        Err(DharmaError::Validation("no seeds reachable".to_string()))
    }
}

fn parse_identity(value: &ciborium::value::Value) -> Result<IdentityKey, DharmaError> {
    let bytes = expect_bytes(value)?;
    IdentityKey::from_slice(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::crypto;
    use crate::types::{ContractId, SchemaId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn directory_state_accepts_policy_hash() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut rng = StdRng::seed_from_u64(9);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([1u8; 32]);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "fabric.domain.policy".to_string(),
            auth: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([2u8; 32]),
            contract: ContractId::from_bytes([3u8; 32]),
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![
            (Value::Text("domain".to_string()), Value::Text("corp.ph.cmdv".to_string())),
            (
                Value::Text("policy_hash".to_string()),
                Value::Text("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff".to_string()),
            ),
        ]);
        let assertion = AssertionPlaintext::sign(header, body, &signing_key).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let env_id = crypto::envelope_id(&bytes);
        store.put_assertion(&subject, &env_id, &bytes).unwrap();
        crate::store::state::append_assertion(
            store.env(),
            &subject,
            1,
            assertion.assertion_id().unwrap(),
            env_id,
            "fabric.domain.policy",
            &bytes,
        )
        .unwrap();
        let state = DirectoryState::load(&store, &subject).unwrap();
        assert!(state.policy_hash_for_domain("corp.ph.cmdv").is_some());
    }
}
