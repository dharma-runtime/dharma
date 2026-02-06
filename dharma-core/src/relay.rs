use crate::assertion::AssertionPlaintext;
use crate::domain::DomainState;
use crate::error::DharmaError;
use crate::ownership::Owner;
use crate::store::state::list_assertions;
use crate::store::Store;
use crate::value::{expect_int, expect_map, expect_text, map_get};
use crate::{cbor, types::SubjectId};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayPlan {
    pub name: String,
    pub max_bytes: i64,
    pub max_objects: Option<i64>,
    pub retention_days: Option<i64>,
    pub note: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayGrant {
    pub domain: String,
    pub plan: String,
    pub expires: Option<i64>,
    pub disabled: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DomainUsage {
    pub bytes: u64,
    pub objects: u64,
}

#[derive(Clone, Debug, Default)]
pub struct RelayUsage {
    pub domains: HashMap<String, DomainUsage>,
}

#[derive(Clone, Debug)]
pub struct RelayPolicy {
    pub domain: String,
    pub relay_domain: String,
    pub plan: RelayPlan,
}

#[derive(Clone, Debug, Default)]
pub struct RelayState {
    pub plans: HashMap<String, RelayPlan>,
    pub grants: HashMap<String, RelayGrant>,
    pub disabled: HashMap<String, bool>,
}

impl RelayState {
    pub fn load(store: &Store, relay_domain: &str) -> Result<Option<Self>, DharmaError> {
        let Some(subject) = crate::domain::subject_for_domain(store, relay_domain)? else {
            return Ok(None);
        };
        let domain_state = DomainState::load(store, &subject)?;
        let Some(owner) = domain_state.owner else {
            return Ok(None);
        };
        let mut state = RelayState::default();
        for record in list_assertions(store.env(), &subject)? {
            let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
                Ok(value) => value,
                Err(_) => continue,
            };
            if assertion.header.sub != subject {
                continue;
            }
            if !assertion.verify_signature()? {
                continue;
            }
            if assertion.header.auth.as_bytes() != owner.as_bytes() {
                continue;
            }
            match assertion.header.typ.as_str() {
                "sys.relay.plan.define" => state.apply_plan_define(&assertion)?,
                "sys.relay.plan.revoke" => state.apply_plan_revoke(&assertion)?,
                "sys.relay.grant" => state.apply_grant(&assertion)?,
                "sys.relay.revoke" => state.apply_revoke(&assertion)?,
                "sys.relay.disable" => state.apply_disable(&assertion)?,
                _ => {}
            }
        }
        Ok(Some(state))
    }

    pub fn grant_for_domain(&self, domain: &str, now: i64) -> Option<RelayGrant> {
        let grant = self.grants.get(domain)?.clone();
        if self.disabled.get(domain).copied().unwrap_or(false) {
            return None;
        }
        if is_expired(grant.expires, now) {
            return None;
        }
        Some(grant)
    }

    pub fn plan_for_domain(&self, domain: &str, now: i64) -> Option<RelayPlan> {
        let grant = self.grant_for_domain(domain, now)?;
        self.plans.get(&grant.plan).cloned()
    }

    fn apply_plan_define(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let name = expect_text(
            map_get(map, "name")
                .ok_or_else(|| DharmaError::Validation("missing name".to_string()))?,
        )?;
        let max_bytes = expect_int(
            map_get(map, "max_bytes")
                .ok_or_else(|| DharmaError::Validation("missing max_bytes".to_string()))?,
        )?;
        let max_objects = parse_optional_int(map_get(map, "max_objects"))?;
        let retention_days = parse_optional_int(map_get(map, "retention_days"))?;
        let note = parse_optional_text(map_get(map, "note"))?;
        if name.is_empty() || max_bytes <= 0 {
            return Ok(());
        }
        self.plans.insert(
            name.clone(),
            RelayPlan {
                name,
                max_bytes,
                max_objects,
                retention_days,
                note,
            },
        );
        Ok(())
    }

    fn apply_plan_revoke(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let name = expect_text(
            map_get(map, "name")
                .ok_or_else(|| DharmaError::Validation("missing name".to_string()))?,
        )?;
        self.plans.remove(&name);
        Ok(())
    }

    fn apply_grant(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let domain = expect_text(
            map_get(map, "domain")
                .ok_or_else(|| DharmaError::Validation("missing domain".to_string()))?,
        )?;
        let plan = expect_text(
            map_get(map, "plan")
                .ok_or_else(|| DharmaError::Validation("missing plan".to_string()))?,
        )?;
        let expires = parse_optional_int(map_get(map, "expires"))?;
        if domain.is_empty() || plan.is_empty() {
            return Ok(());
        }
        self.disabled.remove(&domain);
        self.grants.insert(
            domain.clone(),
            RelayGrant {
                domain,
                plan,
                expires,
                disabled: false,
            },
        );
        Ok(())
    }

    fn apply_revoke(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let domain = expect_text(
            map_get(map, "domain")
                .ok_or_else(|| DharmaError::Validation("missing domain".to_string()))?,
        )?;
        self.grants.remove(&domain);
        self.disabled.remove(&domain);
        Ok(())
    }

    fn apply_disable(&mut self, assertion: &AssertionPlaintext) -> Result<(), DharmaError> {
        let map = expect_map(&assertion.body)?;
        let domain = expect_text(
            map_get(map, "domain")
                .ok_or_else(|| DharmaError::Validation("missing domain".to_string()))?,
        )?;
        self.disabled.insert(domain, true);
        Ok(())
    }
}

impl RelayUsage {
    fn to_value(&self) -> ciborium::value::Value {
        let domains = self
            .domains
            .iter()
            .map(|(name, usage)| (ciborium::value::Value::Text(name.clone()), usage.to_value()))
            .collect();
        ciborium::value::Value::Map(vec![
            (
                ciborium::value::Value::Text("v".to_string()),
                ciborium::value::Value::Integer(1.into()),
            ),
            (
                ciborium::value::Value::Text("domains".to_string()),
                ciborium::value::Value::Map(domains),
            ),
        ])
    }

    fn from_value(value: &ciborium::value::Value) -> Result<Self, DharmaError> {
        let map = crate::value::expect_map(value)?;
        let domains_val = map_get(map, "domains")
            .ok_or_else(|| DharmaError::Validation("missing domains".to_string()))?;
        let domains_map = crate::value::expect_map(domains_val)?;
        let mut domains = HashMap::new();
        for (key, value) in domains_map {
            let name = crate::value::expect_text(key)?;
            let usage = DomainUsage::from_value(value)?;
            domains.insert(name, usage);
        }
        Ok(Self { domains })
    }
}

impl DomainUsage {
    fn to_value(&self) -> ciborium::value::Value {
        ciborium::value::Value::Map(vec![
            (
                ciborium::value::Value::Text("bytes".to_string()),
                ciborium::value::Value::Integer(self.bytes.into()),
            ),
            (
                ciborium::value::Value::Text("objects".to_string()),
                ciborium::value::Value::Integer(self.objects.into()),
            ),
        ])
    }

    fn from_value(value: &ciborium::value::Value) -> Result<Self, DharmaError> {
        let map = crate::value::expect_map(value)?;
        let bytes = crate::value::expect_uint(
            map_get(map, "bytes")
                .ok_or_else(|| DharmaError::Validation("missing bytes".to_string()))?,
        )?;
        let objects = crate::value::expect_uint(
            map_get(map, "objects")
                .ok_or_else(|| DharmaError::Validation("missing objects".to_string()))?,
        )?;
        Ok(Self { bytes, objects })
    }
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

fn relay_usage_path(store: &Store) -> PathBuf {
    store.root().join("indexes").join("relay_usage.cbor")
}

pub fn relay_usage_for_domain(store: &Store, domain: &str) -> Result<DomainUsage, DharmaError> {
    let usage = load_or_compute_relay_usage(store)?;
    Ok(usage.domains.get(domain).copied().unwrap_or_default())
}

pub fn record_relay_usage(
    store: &Store,
    domain: &str,
    bytes: u64,
    objects: u64,
) -> Result<(), DharmaError> {
    if bytes == 0 && objects == 0 {
        return Ok(());
    }
    let mut usage = load_or_compute_relay_usage(store)?;
    let entry = usage
        .domains
        .entry(domain.to_string())
        .or_insert_with(DomainUsage::default);
    entry.bytes = entry.bytes.saturating_add(bytes);
    entry.objects = entry.objects.saturating_add(objects);
    if let Err(err) = save_relay_usage(store, &usage) {
        let _ = store.env().remove_file(&relay_usage_path(store));
        return Err(err);
    }
    Ok(())
}

fn load_or_compute_relay_usage(store: &Store) -> Result<RelayUsage, DharmaError> {
    let path = relay_usage_path(store);
    if store.env().exists(&path) {
        let bytes = store.env().read(&path)?;
        let value = cbor::ensure_canonical(&bytes)?;
        if let Ok(usage) = RelayUsage::from_value(&value) {
            return Ok(usage);
        }
    }
    let usage = compute_relay_usage(store)?;
    save_relay_usage(store, &usage)?;
    Ok(usage)
}

fn save_relay_usage(store: &Store, usage: &RelayUsage) -> Result<(), DharmaError> {
    let path = relay_usage_path(store);
    if let Some(parent) = path.parent() {
        store.env().create_dir_all(parent)?;
    }
    let bytes = cbor::encode_canonical_value(&usage.to_value())?;
    store.env().write(&path, &bytes)
}

fn compute_relay_usage(store: &Store) -> Result<RelayUsage, DharmaError> {
    let entries = crate::store::state::read_manifest(store.env())?;
    let mut subjects_by_envelope: HashMap<crate::types::EnvelopeId, SubjectId> = HashMap::new();
    for entry in entries {
        if let Some(subject) = entry.subject {
            subjects_by_envelope.insert(entry.envelope_id, subject);
        }
    }
    let mut usage = RelayUsage::default();
    let mut domain_cache: HashMap<SubjectId, Option<String>> = HashMap::new();
    for (envelope_id, subject) in subjects_by_envelope {
        let domain_name = match domain_cache.get(&subject) {
            Some(name) => name.clone(),
            None => {
                let name = domain_name_for_subject(store, &subject)?;
                domain_cache.insert(subject, name.clone());
                name
            }
        };
        let Some(domain_name) = domain_name else {
            continue;
        };
        let path = store
            .objects_dir()
            .join(format!("{}.obj", envelope_id.to_hex()));
        let size = match store.env().file_len(&path) {
            Ok(size) => size,
            Err(_) => continue,
        };
        let entry = usage
            .domains
            .entry(domain_name)
            .or_insert_with(DomainUsage::default);
        entry.bytes = entry.bytes.saturating_add(size);
        entry.objects = entry.objects.saturating_add(1);
    }
    Ok(usage)
}

fn domain_name_for_subject(
    store: &Store,
    subject: &SubjectId,
) -> Result<Option<String>, DharmaError> {
    if let Ok(state) = DomainState::load(store, subject) {
        if let Some(domain) = state.domain {
            return Ok(Some(domain));
        }
    }
    let owner = crate::store::state::load_ownership(store.env(), subject)?;
    let Some(record) = owner else {
        return Ok(None);
    };
    match record.owner {
        Owner::Domain(domain_subject) => {
            let state = DomainState::load(store, &domain_subject)?;
            Ok(state.domain)
        }
        Owner::Identity(_) => Ok(None),
    }
}

fn domain_subject_for_subject(
    store: &Store,
    subject: &SubjectId,
) -> Result<Option<(SubjectId, String)>, DharmaError> {
    if let Ok(state) = DomainState::load(store, subject) {
        if let Some(domain) = state.domain {
            return Ok(Some((*subject, domain)));
        }
    }
    let owner = crate::store::state::load_ownership(store.env(), subject)?;
    let Some(record) = owner else {
        return Ok(None);
    };
    match record.owner {
        Owner::Domain(domain_subject) => {
            let state = DomainState::load(store, &domain_subject)?;
            if let Some(domain) = state.domain {
                Ok(Some((domain_subject, domain)))
            } else {
                Ok(None)
            }
        }
        Owner::Identity(_) => Ok(None),
    }
}

pub fn resolve_relay_policy(
    store: &Store,
    subject: &SubjectId,
    now: i64,
) -> Result<RelayPolicy, DharmaError> {
    let Some((domain_subject, domain_name)) = domain_subject_for_subject(store, subject)? else {
        return Err(DharmaError::Validation(
            "missing domain ownership".to_string(),
        ));
    };
    let domain_state = DomainState::load(store, &domain_subject)?;
    let Some(relay_domain) = domain_state.backup_relay_domain else {
        return Err(DharmaError::Validation(
            "missing relay domain policy".to_string(),
        ));
    };
    let Some(plan_name) = domain_state.backup_relay_plan else {
        return Err(DharmaError::Validation(
            "missing relay plan policy".to_string(),
        ));
    };
    let Some(relay_state) = RelayState::load(store, &relay_domain)? else {
        return Err(DharmaError::Validation(
            "relay domain not found".to_string(),
        ));
    };
    let Some(plan) = relay_state.plans.get(&plan_name).cloned() else {
        return Err(DharmaError::Validation("relay plan missing".to_string()));
    };
    let Some(grant) = relay_state.grant_for_domain(&domain_name, now) else {
        return Err(DharmaError::Validation("relay grant missing".to_string()));
    };
    if grant.plan != plan.name {
        return Err(DharmaError::Validation(
            "relay plan not granted".to_string(),
        ));
    }
    Ok(RelayPolicy {
        domain: domain_name,
        relay_domain,
        plan,
    })
}

pub fn relay_identity_authorized(
    store: &Store,
    relay_domain: &str,
    identity: &crate::identity::IdentityState,
    now: i64,
) -> Result<bool, DharmaError> {
    let Some(subject) = crate::domain::subject_for_domain(store, relay_domain)? else {
        return Ok(false);
    };
    let state = DomainState::load(store, &subject)?;
    Ok(state.is_member(&identity.public_key, now))
}
