use crate::assertion::AssertionPlaintext;
use crate::contacts::{self, ContactRelation};
use crate::contract::SummaryDecision;
use crate::error::DharmaError;
use crate::fabric::auth::{CapToken, Flag};
use crate::fabric::types::{AdStore, Advertisement, OracleMode, OracleTiming, ShardMap};
use crate::identity::roles_for_identity;
use crate::ownership::Owner;
use crate::pdl::schema::CqrsSchema;
use crate::protocols::iam;
use crate::share::{share_context, FieldAccess, ShareState};
use crate::store::state::{list_assertions, load_ownership};
use crate::store::Store;
use crate::types::{ContractId, EnvelopeId, IdentityKey, SchemaId, SubjectId};
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub struct RouterConfig {
    pub hedge_count: usize,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self { hedge_count: 2 }
    }
}

#[derive(Clone, Debug)]
pub struct Router {
    shard_maps: Vec<ShardMap>,
    ads: AdStore,
    config: RouterConfig,
}

impl Router {
    pub fn new(shard_maps: Vec<ShardMap>, ads: AdStore, config: RouterConfig) -> Self {
        Self { shard_maps, ads, config }
    }

    pub fn resolve_table(&self, table: &str, key: &[u8]) -> Vec<IdentityKey> {
        let map = self.shard_maps.iter().find(|m| m.table == table);
        let Some(map) = map else {
            return vec![];
        };
        let shard_id = map.resolve(key);
        self.ads.get_providers_for_shard(table, shard_id)
    }

    pub fn resolve_oracle(
        &self,
        token: &CapToken,
        domain: &str,
        name: &str,
        mode: OracleMode,
        timing: OracleTiming,
    ) -> Vec<IdentityKey> {
        if token.domain != domain {
            return vec![];
        }
        if !token_oracle_allowed(token, domain, name, mode, timing) {
            return vec![];
        }
        let mut providers = Vec::new();
        for (provider, ad) in self.ads_snapshot() {
            if ad.domain != domain {
                continue;
            }
            if !ad_oracle_matches(ad, name, mode, timing) {
                continue;
            }
            providers.push(provider);
        }
        providers
    }

    pub fn enforce_custom_query_flag(token: &CapToken, custom: bool) -> bool {
        if !custom {
            return true;
        }
        token.flags.contains(&Flag::AllowCustomQuery)
    }

    fn ads_snapshot(&self) -> Vec<(IdentityKey, Advertisement)> {
        self.ads
            .get_all()
            .into_iter()
            .collect()
    }

    pub fn hedge_count(&self) -> usize {
        self.config.hedge_count
    }
}

pub fn enforce_subject_action(
    store: &Store,
    token: &CapToken,
    subject: &SubjectId,
    now: i64,
    action: &str,
) -> Result<(), DharmaError> {
    let state = ShareState::load(store, subject)?;
    let ctx = share_context(store, subject, &token.issuer, now)?;
    if state.allows_action(&ctx, now, action) {
        if let Some(summary) = load_subject_summary(store, subject)? {
            let roles = roles_for_token(store, token);
            if matches!(summary.allows_action(&roles, action), SummaryDecision::Deny) {
                return Err(DharmaError::Validation("summary denied".to_string()));
            }
        }
        Ok(())
    } else {
        Err(DharmaError::Validation("share denied".to_string()))
    }
}

pub fn enforce_subject_query(
    store: &Store,
    token: &CapToken,
    subject: &SubjectId,
    now: i64,
    query: &str,
) -> Result<FieldAccess, DharmaError> {
    let state = ShareState::load(store, subject)?;
    let ctx = share_context(store, subject, &token.issuer, now)?;
    if !state.allows_query(&ctx, now, query) {
        return Err(DharmaError::Validation("share denied".to_string()));
    }
    if let Some(summary) = load_subject_summary(store, subject)? {
        let roles = roles_for_token(store, token);
        if matches!(summary.allows_query(&roles, query), SummaryDecision::Deny) {
            return Err(DharmaError::Validation("summary denied".to_string()));
        }
    }
    let access = state.field_access(&ctx, now);
    apply_iam_contact_gating(store, subject, &token.issuer, ctx.owner, access)
}

fn load_subject_summary(
    store: &Store,
    subject: &SubjectId,
) -> Result<Option<crate::contract::PermissionSummary>, DharmaError> {
    let Some((contract, ver)) = latest_contract_meta(store, subject)? else {
        return Ok(None);
    };
    let summary = match store.get_permission_summary(&contract) {
        Ok(summary) => summary,
        Err(DharmaError::Validation(msg)) => return Err(DharmaError::Validation(msg)),
        Err(err) => return Err(err),
    };
    let Some(summary) = summary else {
        return Ok(None);
    };
    if summary.ver != ver {
        return Ok(None);
    }
    Ok(Some(summary))
}

fn latest_contract_meta(
    store: &Store,
    subject: &SubjectId,
) -> Result<Option<(ContractId, u64)>, DharmaError> {
    let records = list_assertions(store.env(), subject)?;
    let mut best: Option<(u64, ContractId, u64)> = None;
    for record in records {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if assertion.header.sub != *subject {
            continue;
        }
        match &best {
            Some((seq, _, _)) if *seq >= record.seq => {}
            _ => {
                best = Some((record.seq, assertion.header.contract, assertion.header.ver));
            }
        }
    }
    Ok(best.map(|(_, contract, ver)| (contract, ver)))
}

fn apply_iam_contact_gating(
    store: &Store,
    subject: &SubjectId,
    viewer: &IdentityKey,
    owner: bool,
    access: FieldAccess,
) -> Result<FieldAccess, DharmaError> {
    if owner {
        return Ok(access);
    }
    let Some(schema) = subject_schema(store, subject)? else {
        return Ok(access);
    };
    if schema.namespace != "std.iam" {
        return Ok(access);
    }
    let Some(owner_key) = subject_owner_identity(store, subject)? else {
        return Ok(access);
    };
    if owner_key.as_bytes() == viewer.as_bytes() {
        return Ok(access);
    }
    let relation = contacts::relation(store, &owner_key, viewer)?;
    if relation == ContactRelation::Accepted {
        return Ok(access);
    }
    let public_fields = iam::public_fields(&schema);
    let filtered = match access {
        FieldAccess::All => public_fields,
        FieldAccess::Fields(fields) => fields
            .intersection(&public_fields)
            .cloned()
            .collect::<BTreeSet<_>>(),
    };
    Ok(FieldAccess::Fields(filtered))
}

fn subject_owner_identity(
    store: &Store,
    subject: &SubjectId,
) -> Result<Option<IdentityKey>, DharmaError> {
    let Some(record) = load_ownership(store.env(), subject)? else {
        return Ok(None);
    };
    match record.owner {
        Owner::Identity(owner) => Ok(Some(owner)),
        Owner::Domain(_) => Ok(None),
    }
}

fn subject_schema(store: &Store, subject: &SubjectId) -> Result<Option<CqrsSchema>, DharmaError> {
    let records = list_assertions(store.env(), subject)?;
    let mut best: Option<(u64, SchemaId)> = None;
    for record in records {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if assertion.header.sub != *subject {
            continue;
        }
        match &best {
            Some((seq, _)) if *seq >= record.seq => {}
            _ => {
                best = Some((record.seq, assertion.header.schema));
            }
        }
    }
    let Some((_, schema_id)) = best else {
        return Ok(None);
    };
    let envelope_id = EnvelopeId::from_bytes(*schema_id.as_bytes());
    let Some(bytes) = store.get_object_any(&envelope_id)? else {
        return Ok(None);
    };
    let schema = CqrsSchema::from_cbor(&bytes)?;
    Ok(Some(schema))
}

fn roles_for_token(store: &Store, token: &CapToken) -> Vec<String> {
    let mut roles = BTreeSet::new();
    if let Some(subject) = token.subject {
        if let Ok(found) = roles_for_identity(store.env(), &subject) {
            roles.extend(found);
        }
    }
    if !token.level.is_empty() {
        roles.insert(token.level.clone());
    }
    roles.into_iter().collect()
}

fn token_oracle_allowed(
    token: &CapToken,
    domain: &str,
    name: &str,
    mode: OracleMode,
    timing: OracleTiming,
) -> bool {
    token.oracles.iter().any(|claim| {
        claim.domain == domain && claim.name == name && claim.mode == mode && claim.timing == timing
    })
}

fn ad_oracle_matches(ad: Advertisement, name: &str, mode: OracleMode, timing: OracleTiming) -> bool {
    ad.oracles.iter().any(|oracle| {
        oracle.name == name && oracle.mode == mode && oracle.timing == timing
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto;
    use crate::fabric::auth::{Flag, Op, Scope};
    use crate::fabric::auth::OracleClaim;
    use crate::fabric::types::{AdStore, Advertisement, Endpoint, OracleAd, ShardAd};
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn resolve_table_returns_providers() {
        let map = ShardMap {
            table: "invoice".to_string(),
            strategy: crate::fabric::types::ShardingStrategy::Hash,
            key_col: "id".to_string(),
            shard_count: 4,
            replication_factor: 1,
        };
        let mut rng = StdRng::seed_from_u64(9);
        let (_sk, pk) = crypto::generate_identity_keypair(&mut rng);
        let ad = Advertisement {
            v: 1,
            provider_id: pk,
            ts: 0,
            ttl: 10,
            endpoints: vec![Endpoint {
                protocol: "tcp".to_string(),
                address: "127.0.0.1:3000".to_string(),
            }],
            shards: vec![ShardAd {
                table: "invoice".to_string(),
                shard: map.resolve(b"abc"),
                watermark: 0,
            }],
            load: 0,
            domain: "corp.example".to_string(),
            policy_hash: [0u8; 32],
            oracles: vec![],
            sig: vec![1; 64],
        };
        let mut ads = AdStore::new();
        ads.insert(ad);
        let router = Router::new(vec![map], ads, RouterConfig::default());
        let providers = router.resolve_table("invoice", b"abc");
        assert_eq!(providers.len(), 1);
    }

    #[test]
    fn resolve_oracle_filters_by_token_and_ad() {
        let mut rng = StdRng::seed_from_u64(10);
        let (_sk, pk) = crypto::generate_identity_keypair(&mut rng);
        let ad = Advertisement {
            v: 1,
            provider_id: pk,
            ts: 0,
            ttl: 10,
            endpoints: vec![],
            shards: vec![],
            load: 0,
            domain: "corp.example".to_string(),
            policy_hash: [1u8; 32],
            oracles: vec![OracleAd {
                name: "email.send".to_string(),
                domain: "corp.example".to_string(),
                mode: OracleMode::RequestReply,
                timing: OracleTiming::Async,
                input_schema: crate::types::SchemaId::from_bytes([1u8; 32]),
                output_schema: None,
                max_inflight: None,
                timeout_ms: None,
            }],
            sig: vec![1; 64],
        };
        let mut ads = AdStore::new();
        ads.insert(ad);
        let token = CapToken {
            v: 1,
            id: [2u8; 32],
            issuer: pk,
            domain: "corp.example".to_string(),
            level: "admin".to_string(),
            subject: None,
            scopes: vec![Scope::Table("invoice".to_string())],
            ops: vec![Op::Execute],
            actions: vec![],
            queries: vec![],
            flags: vec![Flag::AllowCustomQuery],
            oracles: vec![OracleClaim {
                name: "email.send".to_string(),
                mode: OracleMode::RequestReply,
                timing: OracleTiming::Async,
                domain: "corp.example".to_string(),
            }],
            constraints: vec![],
            nbf: 0,
            exp: 100,
            sig: vec![1; 64],
        };
        let router = Router::new(vec![], ads, RouterConfig::default());
        let providers = router.resolve_oracle(
            &token,
            "corp.example",
            "email.send",
            OracleMode::RequestReply,
            OracleTiming::Async,
        );
        assert_eq!(providers.len(), 1);
    }
}
