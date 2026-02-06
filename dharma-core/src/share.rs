use crate::assertion::AssertionPlaintext;
use crate::domain::DomainState;
use crate::error::DharmaError;
use crate::ownership::Owner;
use crate::store::state::{list_assertions, load_ownership};
use crate::store::Store;
use crate::types::{IdentityKey, SubjectId};
use crate::value::{expect_array, expect_bool, expect_bytes, expect_int, expect_map, expect_text, map_get};
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldAccess {
    All,
    Fields(BTreeSet<String>),
}

impl FieldAccess {
    pub fn allows(&self, field: &str) -> bool {
        match self {
            FieldAccess::All => true,
            FieldAccess::Fields(fields) => fields.contains(field),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareContext {
    pub identity: IdentityKey,
    pub roles: Vec<String>,
    pub owner: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareGrant {
    pub scopes: Vec<String>,
    pub expires: Option<i64>,
}

#[derive(Clone, Debug, Default)]
pub struct ShareState {
    pub public: Option<ShareGrant>,
    pub grants: HashMap<IdentityKey, ShareGrant>,
    pub role_grants: HashMap<String, ShareGrant>,
}

impl ShareState {
    pub fn load(store: &Store, subject: &SubjectId) -> Result<Self, DharmaError> {
        let mut state = ShareState::default();
        let owner_key = share_owner_key(store, subject)?;
        let Some(owner_key) = owner_key else {
            return Ok(state);
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
            if assertion.header.auth.as_bytes() != owner_key.as_bytes() {
                continue;
            }
            match assertion.header.typ.as_str() {
                "share.grant" => state.apply_grant(&assertion)?,
                "share.revoke" => state.apply_revoke(&assertion)?,
                "share.public" => state.apply_public(&assertion)?,
                _ => {}
            }
        }
        Ok(state)
    }

    pub fn is_public(&self, now: i64, scope: &str) -> bool {
        let Some(grant) = &self.public else {
            return false;
        };
        grant_allows(grant, now, scope)
    }

    pub fn allows_identity(
        &self,
        identity: &IdentityKey,
        roles: &[String],
        now: i64,
        scope: &str,
    ) -> bool {
        if self.is_public(now, scope) {
            return true;
        }
        if let Some(grant) = self.grants.get(identity) {
            if grant_allows(grant, now, scope) {
                return true;
            }
        }
        for role in roles {
            if let Some(grant) = self.role_grants.get(role) {
                if grant_allows(grant, now, scope) {
                    return true;
                }
            }
        }
        false
    }

    pub fn field_access(&self, ctx: &ShareContext, now: i64) -> FieldAccess {
        if ctx.owner {
            return FieldAccess::All;
        }
        let grants = self.relevant_grants(ctx);
        if grants.is_empty() {
            return FieldAccess::Fields(BTreeSet::new());
        }
        let mut fields = BTreeSet::new();
        for grant in grants {
            if is_expired(grant.expires, now) {
                continue;
            }
            if grant.scopes.is_empty() {
                return FieldAccess::All;
            }
            for scope in &grant.scopes {
                if scope == "all" || scope == "read" || scope == "field:*" || scope == "fields:*" {
                    return FieldAccess::All;
                }
                if let Some(name) = field_name_from_scope(scope) {
                    fields.insert(name.to_string());
                }
            }
        }
        FieldAccess::Fields(fields)
    }

    pub fn allows_action(&self, ctx: &ShareContext, now: i64, action: &str) -> bool {
        if ctx.owner {
            return true;
        }
        for grant in self.relevant_grants(ctx) {
            if grant_allows_action(grant, now, action) {
                return true;
            }
        }
        false
    }

    pub fn allows_query(&self, ctx: &ShareContext, now: i64, query: &str) -> bool {
        if ctx.owner {
            return true;
        }
        for grant in self.relevant_grants(ctx) {
            if grant_allows_query(grant, now, query) {
                return true;
            }
        }
        false
    }

    fn relevant_grants<'a>(&'a self, ctx: &ShareContext) -> Vec<&'a ShareGrant> {
        let mut out = Vec::new();
        if let Some(public) = &self.public {
            out.push(public);
        }
        if let Some(grant) = self.grants.get(&ctx.identity) {
            out.push(grant);
        }
        for role in &ctx.roles {
            if let Some(grant) = self.role_grants.get(role) {
                out.push(grant);
            }
        }
        out
    }

    fn apply_grant(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let target_identity = parse_optional_identity(map_get(map, "target_identity"))?;
        let target_role = parse_optional_text(map_get(map, "target_role"))?;
        let scopes = parse_text_list(map_get(map, "scopes"))?;
        let expires = parse_optional_int(map_get(map, "expires"))?;

        match (target_identity, target_role) {
            (Some(identity), None) => {
                self.grants.insert(identity, ShareGrant { scopes, expires });
            }
            (None, Some(role)) if !role.is_empty() => {
                self.role_grants.insert(role, ShareGrant { scopes, expires });
            }
            _ => {}
        }
        Ok(())
    }

    fn apply_revoke(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let target_identity = parse_optional_identity(map_get(map, "target_identity"))?;
        let target_role = parse_optional_text(map_get(map, "target_role"))?;
        match (target_identity, target_role) {
            (Some(identity), None) => {
                self.grants.remove(&identity);
            }
            (None, Some(role)) if !role.is_empty() => {
                self.role_grants.remove(&role);
            }
            _ => {}
        }
        Ok(())
    }

    fn apply_public(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let enabled = match map_get(map, "enabled") {
            Some(value) => expect_bool(value)?,
            None => true,
        };
        let scopes = parse_text_list(map_get(map, "scopes"))?;
        let expires = parse_optional_int(map_get(map, "expires"))?;
        if enabled {
            self.public = Some(ShareGrant { scopes, expires });
        } else {
            self.public = None;
        }
        Ok(())
    }
}

fn share_owner_key(store: &Store, subject: &SubjectId) -> Result<Option<IdentityKey>, DharmaError> {
    let Some(record) = load_ownership(store.env(), subject)? else {
        return Ok(None);
    };
    match record.owner {
        Owner::Identity(owner) => Ok(Some(owner)),
        Owner::Domain(domain_subject) => {
            let state = DomainState::load(store, &domain_subject)?;
            Ok(state.owner)
        }
    }
}

pub fn share_context(
    store: &Store,
    subject: &SubjectId,
    identity: &IdentityKey,
    now: i64,
) -> Result<ShareContext, DharmaError> {
    let Some(record) = load_ownership(store.env(), subject)? else {
        return Ok(ShareContext {
            identity: *identity,
            roles: Vec::new(),
            owner: false,
        });
    };
    match record.owner {
        Owner::Identity(owner) => Ok(ShareContext {
            identity: *identity,
            roles: Vec::new(),
            owner: owner.as_bytes() == identity.as_bytes(),
        }),
        Owner::Domain(domain_subject) => {
            let state = DomainState::load(store, &domain_subject)?;
            let mut roles = Vec::new();
            let mut owner = false;
            if let Some(member) = state.member(identity, now) {
                roles = member.roles;
                owner = roles.iter().any(|role| role == "owner");
            }
            Ok(ShareContext {
                identity: *identity,
                roles,
                owner,
            })
        }
    }
}

fn parse_optional_identity(
    value: Option<&ciborium::value::Value>,
) -> Result<Option<IdentityKey>, DharmaError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if matches!(value, ciborium::value::Value::Null) {
        return Ok(None);
    }
    let bytes = expect_bytes(value)?;
    Ok(Some(IdentityKey::from_slice(&bytes)?))
}

fn parse_optional_text(value: Option<&ciborium::value::Value>) -> Result<Option<String>, DharmaError> {
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

fn parse_text_list(value: Option<&ciborium::value::Value>) -> Result<Vec<String>, DharmaError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if matches!(value, ciborium::value::Value::Null) {
        return Ok(Vec::new());
    }
    let items = expect_array(value)?;
    let mut out = Vec::new();
    for item in items {
        out.push(expect_text(item)?);
    }
    Ok(out)
}

fn is_expired(expires: Option<i64>, now: i64) -> bool {
    if let Some(exp) = expires {
        exp != 0 && exp <= now
    } else {
        false
    }
}

fn scope_allows(scopes: &[String], scope: &str) -> bool {
    if scopes.is_empty() {
        return true;
    }
    scopes.iter().any(|item| item == scope || item == "all")
}

fn grant_allows(grant: &ShareGrant, now: i64, scope: &str) -> bool {
    if is_expired(grant.expires, now) {
        return false;
    }
    scope_allows(&grant.scopes, scope)
}

fn grant_allows_action(grant: &ShareGrant, now: i64, action: &str) -> bool {
    if is_expired(grant.expires, now) {
        return false;
    }
    if grant.scopes.is_empty() {
        return true;
    }
    let expected = format!("action:{action}");
    grant.scopes.iter().any(|scope| {
        scope == "all" || scope == "execute" || scope == "action:*" || scope == &expected
    })
}

fn grant_allows_query(grant: &ShareGrant, now: i64, query: &str) -> bool {
    if is_expired(grant.expires, now) {
        return false;
    }
    if grant.scopes.is_empty() {
        return true;
    }
    let expected = format!("query:{query}");
    grant.scopes.iter().any(|scope| {
        scope == "all"
            || scope == "read"
            || scope == "query:*"
            || scope == &expected
            || field_name_from_scope(scope).is_some()
    })
}

fn field_name_from_scope(scope: &str) -> Option<&str> {
    scope
        .strip_prefix("field:")
        .or_else(|| scope.strip_prefix("fields:"))
        .filter(|name| !name.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::crypto;
    use crate::ownership::OwnershipRecord;
    use crate::runtime::cqrs::filter_state_value;
    use crate::store::state::{append_assertion, save_ownership};
    use crate::types::{ContractId, SchemaId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn append_share_assertion(
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
    }

    fn seed_ownership(env: &dyn crate::env::Env, subject: &SubjectId, owner: IdentityKey) {
        let record = OwnershipRecord {
            owner: Owner::Identity(owner),
            creator: owner,
            acting_domain: None,
            role: None,
        };
        save_ownership(env, subject, &record).unwrap();
    }

    #[test]
    fn share_grant_direct_identity() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(200);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (_target_sk, target_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([1u8; 32]);
        seed_ownership(env, &subject, owner_id);

        let body = Value::Map(vec![
            (
                Value::Text("target_identity".to_string()),
                Value::Bytes(target_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("scopes".to_string()),
                Value::Array(vec![Value::Text("read".to_string())]),
            ),
        ]);
        append_share_assertion(env, subject, 1, "share.grant", owner_id, &owner_sk, body);

        let state = ShareState::load(&store, &subject).unwrap();
        assert!(state.allows_identity(&target_id, &[], 0, "read"));
        assert!(!state.allows_identity(&owner_id, &[], 0, "read"));
    }

    #[test]
    fn share_revoke_blocks_future() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(201);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (_target_sk, target_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([2u8; 32]);
        seed_ownership(env, &subject, owner_id);

        let grant_body = Value::Map(vec![
            (
                Value::Text("target_identity".to_string()),
                Value::Bytes(target_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("scopes".to_string()),
                Value::Array(vec![Value::Text("read".to_string())]),
            ),
        ]);
        append_share_assertion(env, subject, 1, "share.grant", owner_id, &owner_sk, grant_body);

        let revoke_body = Value::Map(vec![(
            Value::Text("target_identity".to_string()),
            Value::Bytes(target_id.as_bytes().to_vec()),
        )]);
        append_share_assertion(env, subject, 2, "share.revoke", owner_id, &owner_sk, revoke_body);

        let state = ShareState::load(&store, &subject).unwrap();
        assert!(!state.allows_identity(&target_id, &[], 0, "read"));
    }

    #[test]
    fn share_public_explicit() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(202);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([3u8; 32]);
        seed_ownership(env, &subject, owner_id);

        let body = Value::Map(vec![(
            Value::Text("enabled".to_string()),
            Value::Bool(true),
        )]);
        append_share_assertion(env, subject, 1, "share.public", owner_id, &owner_sk, body);

        let state = ShareState::load(&store, &subject).unwrap();
        assert!(state.is_public(0, "read"));
    }

    #[test]
    fn owner_can_read_write() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(210);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([4u8; 32]);
        seed_ownership(env, &subject, owner_id);

        let state = ShareState::load(&store, &subject).unwrap();
        let ctx = share_context(&store, &subject, &owner_id, 0).unwrap();
        assert!(ctx.owner);
        assert!(state.allows_action(&ctx, 0, "DoThing"));
        assert!(state.allows_query(&ctx, 0, "List"));
        assert!(matches!(state.field_access(&ctx, 0), FieldAccess::All));

        let body = Value::Map(vec![(
            Value::Text("enabled".to_string()),
            Value::Bool(true),
        )]);
        append_share_assertion(env, subject, 1, "share.public", owner_id, &owner_sk, body);
    }

    #[test]
    fn shared_identity_can_query_scoped_fields() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(211);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (_target_sk, target_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([5u8; 32]);
        seed_ownership(env, &subject, owner_id);

        let body = Value::Map(vec![
            (
                Value::Text("target_identity".to_string()),
                Value::Bytes(target_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("scopes".to_string()),
                Value::Array(vec![Value::Text("field:alpha".to_string())]),
            ),
        ]);
        append_share_assertion(env, subject, 1, "share.grant", owner_id, &owner_sk, body);

        let state = ShareState::load(&store, &subject).unwrap();
        let ctx = share_context(&store, &subject, &target_id, 0).unwrap();
        assert!(!ctx.owner);
        assert!(state.allows_query(&ctx, 0, "List"));
        let access = state.field_access(&ctx, 0);
        let value = Value::Map(vec![
            (Value::Text("alpha".to_string()), Value::Integer(1.into())),
            (Value::Text("beta".to_string()), Value::Integer(2.into())),
        ]);
        let filtered = filter_state_value(value, &access);
        let Value::Map(entries) = filtered else {
            panic!("expected map");
        };
        assert_eq!(entries.len(), 1);
        assert!(entries.iter().any(|(k, _)| k == &Value::Text("alpha".to_string())));
    }

    #[test]
    fn unshared_identity_denied() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let env = store.env();
        let mut rng = StdRng::seed_from_u64(212);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (_target_sk, target_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([6u8; 32]);
        seed_ownership(env, &subject, owner_id);

        let body = Value::Map(vec![(
            Value::Text("enabled".to_string()),
            Value::Bool(false),
        )]);
        append_share_assertion(env, subject, 1, "share.public", owner_id, &owner_sk, body);

        let state = ShareState::load(&store, &subject).unwrap();
        let ctx = share_context(&store, &subject, &target_id, 0).unwrap();
        assert!(!state.allows_query(&ctx, 0, "List"));
        let access = state.field_access(&ctx, 0);
        match access {
            FieldAccess::Fields(fields) => assert!(fields.is_empty()),
            FieldAccess::All => panic!("unexpected access"),
        }
    }
}
