use crate::assertion::{is_overlay, signer_from_meta, AssertionPlaintext};
use crate::contract::{ContractEngine, ContractStatus, SummaryDecision};
use crate::crypto;
use crate::env::Env;
use crate::envelope::{self, AssertionEnvelope};
use crate::error::DharmaError;
use crate::metrics;
use crate::domain::DomainState;
use crate::identity::{
    delegate_allows, device_key_revoked, roles_for_identity, root_key_for_identity,
    ATLAS_IDENTITY_ACTIVATE, ATLAS_IDENTITY_GENESIS, ATLAS_IDENTITY_REVOKE, ATLAS_IDENTITY_SUSPEND,
};
use crate::ownership::{derive_ownership_record, OwnershipState};
use crate::pdl::schema::{ConcurrencyMode, CqrsSchema};
use crate::runtime::cqrs::{action_index, encode_args_buffer, load_state, merge_args};
use crate::schema as generic_schema;
use crate::keys::{hpke_open, key_id_for_key, KeyEnvelope, Keyring};
use crate::store::index::FrontierIndex;
use crate::store::state::{
    append_assertion, append_overlay, find_assertion_by_id, find_overlay_by_id, load_epoch,
    load_key_bind, overlays_for_ref, save_epoch, save_key_bind, subject_has_facts, KeyBindRecord,
    load_ownership, save_ownership, list_assertions,
};
use crate::store::Store;
use crate::types::{AssertionId, ContractId, EnvelopeId, IdentityKey, KeyId, SchemaId, SubjectId};
use crate::validation::{structural_validate, StructuralStatus};
use crate::value::{expect_array, expect_bytes, expect_int, expect_map, expect_text, expect_uint, map_get};
use crate::vault::DHBOX_VERSION_V1;
use ciborium::value::Value;
use tracing::{debug, warn};

#[derive(Debug)]
pub enum IngestError {
    MissingDependency { assertion_id: AssertionId, missing: AssertionId },
    Pending(String),
    Validation(String),
    Dharma(DharmaError),
}

#[derive(Debug, PartialEq, Eq)]
pub enum IngestStatus {
    Accepted(AssertionId),
    Pending(AssertionId, String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum RelayIngestStatus {
    Accepted(EnvelopeId, AssertionId),
    Pending(AssertionId, String),
    Opaque(EnvelopeId),
}

impl From<DharmaError> for IngestError {
    fn from(err: DharmaError) -> Self {
        IngestError::Dharma(err)
    }
}

pub fn ingest_object(
    store: &Store,
    index: &mut FrontierIndex,
    bytes: &[u8],
    keys: &mut Keyring,
) -> Result<IngestStatus, IngestError> {
    let (envelope_id, assertion_id, subject, assertion) = match decode_assertion(bytes, keys) {
        Ok(res) => res,
        Err(err) => {
            // eprintln!("ingest_object: decode failed: {:?}", err);
            return Err(err);
        }
    };

    if assertion.header.typ == ATLAS_IDENTITY_GENESIS && assertion.header.seq != 1 {
        return Err(IngestError::Validation(
            "atlas identity genesis requires seq=1".to_string(),
        ));
    }

    if index.has_envelope(&envelope_id) && !index.is_pending(&assertion_id) {
        return Ok(IngestStatus::Accepted(assertion_id));
    }

    if env_is_verbose() {
        debug!(
            assertion_id = %assertion_id.to_hex(),
            assertion_type = %assertion.header.typ,
            seq = assertion.header.seq,
            subject_id = %subject.to_hex(),
            "ingest processing assertion"
        );
    }

    store.put_assertion(&subject, &envelope_id, bytes)?;
    store.record_semantic(&assertion_id, &envelope_id)?;
    let env = store.env();
    let is_new_subject = !subject_has_facts(env, &subject)?;

    let is_action = assertion.header.typ.starts_with("action.");
    let overlay_flag = is_action && is_overlay(&assertion.header);
    let overlay_ref = if overlay_flag {
        if assertion.header.refs.len() != 1 {
            return Err(IngestError::Validation("overlay must reference base".to_string()));
        }
        assertion.header.refs.first().copied()
    } else {
        None
    };

    if let Some(base_ref) = overlay_ref {
        if find_assertion_by_id(env, &subject, &base_ref)?.is_none() {
            index.mark_known(assertion_id);
            index.mark_pending(assertion_id);
            return Err(IngestError::MissingDependency {
                assertion_id,
                missing: base_ref,
            });
        }
        let prev_assertion = match assertion.header.prev {
            Some(prev_id) => {
                let Some(prev_bytes) = find_overlay_by_id(env, &subject, &prev_id)? else {
                    index.mark_known(assertion_id);
                    index.mark_pending(assertion_id);
                    return Err(IngestError::MissingDependency {
                        assertion_id,
                        missing: prev_id,
                    });
                };
                Some(AssertionPlaintext::from_cbor(&prev_bytes)?)
            }
            None => None,
        };
        match structural_validate(&assertion, prev_assertion.as_ref())? {
            StructuralStatus::Reject(reason) => {
                warn!(
                    assertion_id = %assertion_id.to_hex(),
                    reason = %reason,
                    "ingest structural reject"
                );
                return Err(IngestError::Validation(reason));
            }
            StructuralStatus::Pending(reason) => {
                index.mark_known(assertion_id);
                index.mark_pending(assertion_id);
                return Ok(IngestStatus::Pending(assertion_id, reason));
            }
            StructuralStatus::Accept => {}
        }
        reject_local_handle(&assertion)?;
        match enforce_domain_controls(store, &assertion, is_new_subject) {
            Ok(()) => {}
            Err(IngestError::Pending(reason)) => {
                index.mark_known(assertion_id);
                index.mark_pending(assertion_id);
                return Ok(IngestStatus::Pending(assertion_id, reason));
            }
            Err(err) => return Err(err),
        }
        match validate_action_contract(store, index, &subject, &assertion, assertion_id, Some(base_ref)) {
            Ok(()) => {}
            Err(IngestError::Pending(reason)) => {
                index.mark_known(assertion_id);
                index.mark_pending(assertion_id);
                return Ok(IngestStatus::Pending(assertion_id, reason));
            }
            Err(err) => {
                warn!(
                    assertion_id = %assertion_id.to_hex(),
                    error = ?err,
                    "ingest contract validation error"
                );
                return Err(err);
            }
        }
        let action_name = assertion
            .header
            .typ
            .strip_prefix("action.")
            .unwrap_or(&assertion.header.typ);
        let plain = assertion.to_cbor()?;
        append_overlay(
            env,
            &subject,
            assertion.header.seq,
            assertion_id,
            envelope_id,
            action_name,
            &plain,
        )?;
        index.mark_known(assertion_id);
        index.clear_pending(&assertion_id);
        metrics::assertions_ingested_inc();
        return Ok(IngestStatus::Accepted(assertion_id));
    }

    if is_action {
        let prev_assertion = match assertion.header.prev {
            Some(prev_id) => {
                let Some(prev_bytes) = find_assertion_by_id(env, &subject, &prev_id)? else {
                    index.mark_known(assertion_id);
                    index.mark_pending(assertion_id);
                    return Err(IngestError::MissingDependency {
                        assertion_id,
                        missing: prev_id,
                    });
                };
                Some(AssertionPlaintext::from_cbor(&prev_bytes)?)
            }
            None => None,
        };
        match structural_validate(&assertion, prev_assertion.as_ref())? {
            StructuralStatus::Reject(reason) => {
                warn!(
                    assertion_id = %assertion_id.to_hex(),
                    reason = %reason,
                    "ingest action structural reject"
                );
                return Err(IngestError::Validation(reason));
            }
            StructuralStatus::Pending(reason) => {
                index.mark_known(assertion_id);
                index.mark_pending(assertion_id);
                return Ok(IngestStatus::Pending(assertion_id, reason));
            }
            StructuralStatus::Accept => {}
        }
        reject_local_handle(&assertion)?;
        match enforce_domain_controls(store, &assertion, is_new_subject) {
            Ok(()) => {}
            Err(IngestError::Pending(reason)) => {
                index.mark_known(assertion_id);
                index.mark_pending(assertion_id);
                return Ok(IngestStatus::Pending(assertion_id, reason));
            }
            Err(err) => return Err(err),
        }
        match validate_action_contract(store, index, &subject, &assertion, assertion_id, None) {
            Ok(()) => {}
            Err(IngestError::Pending(reason)) => {
                index.mark_known(assertion_id);
                index.mark_pending(assertion_id);
                return Ok(IngestStatus::Pending(assertion_id, reason));
            }
            Err(err) => {
                warn!(
                    assertion_id = %assertion_id.to_hex(),
                    error = ?err,
                    "ingest action contract validation error"
                );
                return Err(err);
            }
        }
        let action_name = assertion
            .header
            .typ
            .strip_prefix("action.")
            .unwrap_or(&assertion.header.typ);
        let plain = assertion.to_cbor()?;
        append_assertion(
            env,
            &subject,
            assertion.header.seq,
            assertion_id,
            envelope_id,
            action_name,
            &plain,
        )?;
        if is_new_subject {
            let record = derive_ownership_record(env, &assertion)
                .map_err(|err| IngestError::Validation(err.to_string()))?;
            save_ownership(env, &subject, &record)?;
        }
        index.update(assertion_id, envelope_id, &assertion.header)?;
        index.clear_pending(&assertion_id);
        if env_is_verbose() {
            debug!(
                assertion_id = %assertion_id.to_hex(),
                "ingest accepted action"
            );
        }
        metrics::assertions_ingested_inc();
        return Ok(IngestStatus::Accepted(assertion_id));
    }

    let prev_assertion = match assertion.header.prev {
        Some(prev_id) => {
            let Some(prev_env) = store.lookup_envelope(&prev_id)? else {
                index.mark_known(assertion_id);
                index.mark_pending(assertion_id);
                return Err(IngestError::MissingDependency {
                    assertion_id,
                    missing: prev_id,
                });
            };
            let prev_bytes = store.get_object(&prev_env)?;
            match decode_assertion(&prev_bytes, keys) {
                Ok((_prev_env_id, _prev_assertion_id, _prev_subject, prev_assertion)) => {
                    Some(prev_assertion)
                }
                Err(_) => return Err(IngestError::Validation("invalid prev assertion".to_string())),
            }
        }
        None => None,
    };

    match structural_validate(&assertion, prev_assertion.as_ref())? {
        StructuralStatus::Reject(reason) => {
            warn!(
                assertion_id = %assertion_id.to_hex(),
                reason = %reason,
                "ingest generic structural reject"
            );
            return Err(IngestError::Validation(reason));
        }
        StructuralStatus::Pending(reason) => {
            index.mark_known(assertion_id);
            index.mark_pending(assertion_id);
            return Ok(IngestStatus::Pending(assertion_id, reason));
        }
        StructuralStatus::Accept => {}
    }

    reject_local_handle(&assertion)?;

    if assertion.header.typ == ATLAS_IDENTITY_GENESIS {
        if subject_has_facts(env, &subject)? {
            return Err(IngestError::Validation(
                "atlas identity genesis must be first".to_string(),
            ));
        }
    } else {
        enforce_identity_lifecycle(env, &subject, &assertion)?;
    }
    enforce_share_permissions(store, &assertion)?;
    let transfer_update = enforce_transfer_rules(store, &assertion)?;
    match enforce_domain_controls(store, &assertion, is_new_subject) {
        Ok(()) => {}
        Err(IngestError::Pending(reason)) => {
            index.mark_known(assertion_id);
            index.mark_pending(assertion_id);
            return Ok(IngestStatus::Pending(assertion_id, reason));
        }
        Err(err) => return Err(err),
    }
    match enforce_key_actions(store, keys, &assertion) {
        Ok(()) => {}
        Err(IngestError::Pending(reason)) => {
            index.mark_known(assertion_id);
            index.mark_pending(assertion_id);
            return Ok(IngestStatus::Pending(assertion_id, reason));
        }
        Err(err) => return Err(err),
    }

    match validate_generic_contract(store, &assertion) {
        Ok(()) => {}
        Err(IngestError::Pending(reason)) => {
            index.mark_known(assertion_id);
            index.mark_pending(assertion_id);
            return Ok(IngestStatus::Pending(assertion_id, reason));
        }
        Err(err) => {
            warn!(
                assertion_id = %assertion_id.to_hex(),
                error = ?err,
                "ingest generic contract validation error"
            );
            return Err(err);
        }
    }
    let action_name = assertion.header.typ.clone();
    let plain = assertion.to_cbor()?;
        append_assertion(
            env,
            &subject,
            assertion.header.seq,
            assertion_id,
        envelope_id,
        &action_name,
        &plain,
    )?;
    if let Some(new_owner) = transfer_update {
        if let Some(mut record) = load_ownership(env, &subject)? {
            record.owner = new_owner;
            save_ownership(env, &subject, &record)?;
        }
    }
    if is_new_subject {
        let record = derive_ownership_record(env, &assertion)
            .map_err(|err| IngestError::Validation(err.to_string()))?;
        save_ownership(env, &subject, &record)?;
    }
    index.update(assertion_id, envelope_id, &assertion.header)?;
    index.clear_pending(&assertion_id);
    if env_is_verbose() {
        debug!(
            assertion_id = %assertion_id.to_hex(),
            "ingest accepted assertion"
        );
    }
    metrics::assertions_ingested_inc();
    Ok(IngestStatus::Accepted(assertion_id))
}

fn enforce_share_permissions(store: &Store, assertion: &AssertionPlaintext) -> Result<(), IngestError> {
    if !assertion.header.typ.starts_with("share.") {
        return Ok(());
    }
    validate_share_body(assertion)?;
    let Some(record) = load_ownership(store.env(), &assertion.header.sub)? else {
        return Err(IngestError::Pending("missing ownership".to_string()));
    };
    let owner_key = match record.owner {
        crate::ownership::Owner::Identity(owner) => Some(owner),
        crate::ownership::Owner::Domain(domain_subject) => {
            let state = DomainState::load(store, &domain_subject)?;
            state.owner
        }
    };
    let Some(owner_key) = owner_key else {
        return Err(IngestError::Pending("missing owner".to_string()));
    };
    if assertion.header.auth.as_bytes() != owner_key.as_bytes() {
        return Err(IngestError::Validation("unauthorized share".to_string()));
    }
    Ok(())
}

fn validate_share_body(assertion: &AssertionPlaintext) -> Result<(), IngestError> {
    let map = expect_map(&assertion.body).map_err(|err| IngestError::Validation(err.to_string()))?;
    match assertion.header.typ.as_str() {
        "share.grant" | "share.revoke" => {
            let identity = map_get(map, "target_identity")
                .map(|value| expect_bytes(value))
                .transpose()
                .map_err(|err| IngestError::Validation(err.to_string()))?;
            let role = map_get(map, "target_role")
                .map(|value| expect_text(value))
                .transpose()
                .map_err(|err| IngestError::Validation(err.to_string()))?;
            let has_identity = identity.is_some();
            let has_role = role.as_ref().map(|value| !value.is_empty()).unwrap_or(false);
            if has_identity == has_role {
                return Err(IngestError::Validation(
                    "share target must be identity or role".to_string(),
                ));
            }
        }
        "share.public" => {}
        _ => {}
    }
    Ok(())
}

fn enforce_transfer_rules(
    store: &Store,
    assertion: &AssertionPlaintext,
) -> Result<Option<crate::ownership::Owner>, IngestError> {
    if !assertion.header.typ.starts_with("subject.transfer") {
        return Ok(None);
    }
    let Some(state) = OwnershipState::load(store, &assertion.header.sub)? else {
        return Err(IngestError::Pending("missing ownership".to_string()));
    };
    let decision = state
        .validate_transfer(store, assertion)
        .map_err(|err| IngestError::Validation(err.to_string()))?;
    Ok(decision)
}

fn enforce_domain_controls(
    store: &Store,
    assertion: &AssertionPlaintext,
    is_new_subject: bool,
) -> Result<(), IngestError> {
    let domain_subject = match resolve_domain_subject(store, assertion, is_new_subject)? {
        Some(domain) => domain,
        None => return Ok(()),
    };
    let state = DomainState::load(store, &domain_subject)?;
    if state.owner.is_none() {
        if assertion.header.typ == "atlas.domain.genesis" {
            return Ok(());
        }
        return Err(IngestError::Pending("missing domain owner".to_string()));
    }
    let owner = state.owner.unwrap();
    let typ = assertion.header.typ.as_str();
    let is_control = matches!(
        typ,
        "domain.freeze" | "domain.unfreeze" | "domain.compromised"
    );
    if is_control && assertion.header.sub != domain_subject {
        return Err(IngestError::Validation(
            "domain control must target domain subject".to_string(),
        ));
    }
    if is_control && assertion.header.auth.as_bytes() != owner.as_bytes() {
        return Err(IngestError::Validation(
            "unauthorized domain control".to_string(),
        ));
    }
    if state.compromised {
        if typ == "domain.compromised" {
            return Ok(());
        }
        return Err(IngestError::Validation("domain compromised".to_string()));
    }
    if state.frozen {
        if matches!(typ, "domain.unfreeze" | "domain.compromised") {
            return Ok(());
        }
        return Err(IngestError::Validation("domain frozen".to_string()));
    }
    Ok(())
}

fn resolve_domain_subject(
    store: &Store,
    assertion: &AssertionPlaintext,
    is_new_subject: bool,
) -> Result<Option<SubjectId>, IngestError> {
    let typ = assertion.header.typ.as_str();
    if typ.starts_with("atlas.domain.") || typ.starts_with("domain.") {
        return Ok(Some(assertion.header.sub));
    }
    if let Some(record) = load_ownership(store.env(), &assertion.header.sub)? {
        if let crate::ownership::Owner::Domain(domain_subject) = record.owner {
            return Ok(Some(domain_subject));
        }
    } else if is_new_subject {
        if let Ok(record) = derive_ownership_record(store.env(), assertion) {
            if let crate::ownership::Owner::Domain(domain_subject) = record.owner {
                return Ok(Some(domain_subject));
            }
        }
    }
    Ok(None)
}

fn enforce_key_actions(
    store: &Store,
    keys: &mut Keyring,
    assertion: &AssertionPlaintext,
) -> Result<(), IngestError> {
    match assertion.header.typ.as_str() {
        "domain.key.rotate" => enforce_domain_key_rotate(store, assertion),
        "subject.key.bind" => enforce_subject_key_bind(store, assertion),
        "member.key.grant" => enforce_member_key_grant(store, keys, assertion),
        _ => Ok(()),
    }
}

fn enforce_domain_key_rotate(
    store: &Store,
    assertion: &AssertionPlaintext,
) -> Result<(), IngestError> {
    let map = expect_map(&assertion.body).map_err(|err| IngestError::Validation(err.to_string()))?;
    let epoch = expect_uint(
        map_get(map, "epoch")
            .ok_or_else(|| IngestError::Validation("missing epoch".to_string()))?,
    )
    .map_err(|err| IngestError::Validation(err.to_string()))?;
    let kek_bytes = expect_bytes(
        map_get(map, "kek_id")
            .ok_or_else(|| IngestError::Validation("missing kek_id".to_string()))?,
    )
    .map_err(|err| IngestError::Validation(err.to_string()))?;
    let _kek_id = KeyId::from_slice(&kek_bytes)?;
    let state = DomainState::load(store, &assertion.header.sub)?;
    let owner = state
        .owner
        .ok_or_else(|| IngestError::Pending("missing domain owner".to_string()))?;
    if assertion.header.auth.as_bytes() != owner.as_bytes() {
        return Err(IngestError::Validation(
            "unauthorized key rotation".to_string(),
        ));
    }
    let current = load_epoch(store.env(), &assertion.header.sub)?.unwrap_or(0);
    if epoch <= current {
        return Err(IngestError::Validation("epoch not advanced".to_string()));
    }
    save_epoch(store.env(), &assertion.header.sub, epoch)?;
    Ok(())
}

fn enforce_subject_key_bind(
    store: &Store,
    assertion: &AssertionPlaintext,
) -> Result<(), IngestError> {
    let map = expect_map(&assertion.body).map_err(|err| IngestError::Validation(err.to_string()))?;
    let domain_bytes = expect_bytes(
        map_get(map, "domain")
            .ok_or_else(|| IngestError::Validation("missing domain".to_string()))?,
    )
    .map_err(|err| IngestError::Validation(err.to_string()))?;
    let domain = SubjectId::from_slice(&domain_bytes)?;
    let epoch = expect_uint(
        map_get(map, "epoch")
            .ok_or_else(|| IngestError::Validation("missing epoch".to_string()))?,
    )
    .map_err(|err| IngestError::Validation(err.to_string()))?;
    let sdk_bytes = expect_bytes(
        map_get(map, "sdk_id")
            .ok_or_else(|| IngestError::Validation("missing sdk_id".to_string()))?,
    )
    .map_err(|err| IngestError::Validation(err.to_string()))?;
    let sdk_id = KeyId::from_slice(&sdk_bytes)?;
    let state = DomainState::load(store, &domain)?;
    let owner = state
        .owner
        .ok_or_else(|| IngestError::Pending("missing domain owner".to_string()))?;
    if assertion.header.auth.as_bytes() != owner.as_bytes() {
        return Err(IngestError::Validation(
            "unauthorized key bind".to_string(),
        ));
    }
    let domain_epoch = match load_epoch(store.env(), &domain)? {
        Some(value) => value,
        None => {
            if epoch == 0 {
                0
            } else {
                return Err(IngestError::Pending(
                    "missing domain epoch".to_string(),
                ));
            }
        }
    };
    if epoch != domain_epoch {
        return Err(IngestError::Validation("domain epoch mismatch".to_string()));
    }
    let current = load_epoch(store.env(), &assertion.header.sub)?.unwrap_or(0);
    if epoch < current {
        return Err(IngestError::Validation("epoch regressed".to_string()));
    }
    save_epoch(store.env(), &assertion.header.sub, epoch)?;
    save_key_bind(
        store.env(),
        &assertion.header.sub,
        &KeyBindRecord {
            domain,
            epoch,
            sdk_id,
        },
    )?;
    Ok(())
}

fn enforce_member_key_grant(
    store: &Store,
    keys: &mut Keyring,
    assertion: &AssertionPlaintext,
) -> Result<(), IngestError> {
    let map = expect_map(&assertion.body).map_err(|err| IngestError::Validation(err.to_string()))?;
    let member_bytes = expect_bytes(
        map_get(map, "member")
            .ok_or_else(|| IngestError::Validation("missing member".to_string()))?,
    )
    .map_err(|err| IngestError::Validation(err.to_string()))?;
    let member = IdentityKey::from_slice(&member_bytes)?;
    let subject_bytes = expect_bytes(
        map_get(map, "subject")
            .ok_or_else(|| IngestError::Validation("missing subject".to_string()))?,
    )
    .map_err(|err| IngestError::Validation(err.to_string()))?;
    let subject = SubjectId::from_slice(&subject_bytes)?;
    let epoch = expect_uint(
        map_get(map, "epoch")
            .ok_or_else(|| IngestError::Validation("missing epoch".to_string()))?,
    )
    .map_err(|err| IngestError::Validation(err.to_string()))?;
    let sdk_id_bytes = expect_bytes(
        map_get(map, "sdk_id")
            .ok_or_else(|| IngestError::Validation("missing sdk_id".to_string()))?,
    )
    .map_err(|err| IngestError::Validation(err.to_string()))?;
    let sdk_id = KeyId::from_slice(&sdk_id_bytes)?;
    let sdk_bytes = expect_bytes(
        map_get(map, "sdk")
            .ok_or_else(|| IngestError::Validation("missing sdk".to_string()))?,
    )
    .map_err(|err| IngestError::Validation(err.to_string()))?;
    let bind = load_key_bind(store.env(), &subject)?
        .ok_or_else(|| IngestError::Pending("missing key bind".to_string()))?;
    if bind.domain != assertion.header.sub {
        return Err(IngestError::Validation(
            "key grant domain mismatch".to_string(),
        ));
    }
    if bind.epoch != epoch {
        return Err(IngestError::Validation("epoch mismatch".to_string()));
    }
    if bind.sdk_id != sdk_id {
        return Err(IngestError::Validation("sdk id mismatch".to_string()));
    }
    let state = DomainState::load(store, &bind.domain)?;
    let owner = state
        .owner
        .ok_or_else(|| IngestError::Pending("missing domain owner".to_string()))?;
    if assertion.header.auth.as_bytes() != owner.as_bytes() {
        return Err(IngestError::Validation(
            "unauthorized key grant".to_string(),
        ));
    }
    let now = store.env().now();
    if !state.is_member(&member, now) {
        return Err(IngestError::Validation("member not active".to_string()));
    }
    maybe_insert_sdk_from_grant(keys, &member, subject, epoch, &sdk_id, &sdk_bytes);
    Ok(())
}

fn maybe_insert_sdk_from_grant(
    keys: &mut Keyring,
    member: &IdentityKey,
    subject: SubjectId,
    epoch: u64,
    sdk_id: &KeyId,
    sdk_bytes: &[u8],
) {
    let Some(secret) = keys.hpke_secret_for(member) else {
        return;
    };
    let Ok(envelope) = KeyEnvelope::from_cbor(sdk_bytes) else {
        return;
    };
    let Ok(sdk_plain) = hpke_open(secret, &envelope) else {
        return;
    };
    if sdk_plain.len() != 32 {
        return;
    }
    let mut sdk = [0u8; 32];
    sdk.copy_from_slice(&sdk_plain);
    if key_id_for_key(&sdk) != *sdk_id {
        return;
    }
    keys.insert_sdk(subject, epoch, sdk);
}

fn env_is_verbose() -> bool {
    std::env::var("DHARMA_VERBOSE").is_ok()
}

pub fn ingest_object_relay(
    store: &Store,
    index: &mut FrontierIndex,
    identity: &crate::identity::IdentityState,
    envelope_id: EnvelopeId,
    bytes: &[u8],
    subject_hint: Option<SubjectId>,
) -> Result<RelayIngestStatus, IngestError> {
    if crypto::envelope_id(bytes) != envelope_id {
        return Err(IngestError::Validation("envelope hash mismatch".to_string()));
    }

    let object_path = store
        .objects_dir()
        .join(format!("{}.obj", envelope_id.to_hex()));
    let is_new_object = !store.env().exists(&object_path);
    if let Ok(assertion) = AssertionPlaintext::from_cbor(bytes) {
        if !assertion.verify_signature()? {
            return Err(IngestError::Validation("invalid signature".to_string()));
        }
        let assertion_id = assertion.assertion_id()?;
        let subject = assertion.header.sub;
        let mut seeded_ownership = false;
        if is_new_object {
            if crate::store::state::load_ownership(store.env(), &subject)?.is_none() {
                if let Ok(record) = derive_ownership_record(store.env(), &assertion) {
                    if crate::store::state::save_ownership(store.env(), &subject, &record).is_ok()
                    {
                        seeded_ownership = true;
                    }
                }
            }
        }
        let is_action = assertion.header.typ.starts_with("action.");
        let overlay_flag = is_action && is_overlay(&assertion.header);
        let overlay_ref = if overlay_flag {
            if assertion.header.refs.len() != 1 {
                return Err(IngestError::Validation("overlay must reference base".to_string()));
            }
            assertion.header.refs.first().copied()
        } else {
            None
        };
        if is_new_object {
            if let Err(err) = enforce_relay_quota(store, identity, &subject, bytes.len() as u64) {
                if seeded_ownership {
                    rollback_relay_seeded_ownership(store, &subject);
                }
                return Err(err);
            }
        }
        store.put_object(&envelope_id, bytes)?;
        store.record_semantic(&assertion_id, &envelope_id)?;

        if let Some(base_ref) = overlay_ref {
            if store.lookup_envelope(&base_ref)?.is_none() {
                index.mark_pending(assertion_id);
                return Err(IngestError::MissingDependency {
                    assertion_id,
                    missing: base_ref,
                });
            }
        }

        let prev_assertion = match assertion.header.prev {
            Some(prev_id) => {
                let Some(prev_env) = store.lookup_envelope(&prev_id)? else {
                    index.mark_pending(assertion_id);
                    return Err(IngestError::MissingDependency {
                        assertion_id,
                        missing: prev_id,
                    });
                };
                let prev_bytes = store.get_object(&prev_env)?;
                match AssertionPlaintext::from_cbor(&prev_bytes) {
                    Ok(prev) => Some(prev),
                    Err(_) => {
                        index.mark_pending(assertion_id);
                        return Err(IngestError::Validation("invalid prev".to_string()));
                    }
                }
            }
            None => None,
        };

        match structural_validate(&assertion, prev_assertion.as_ref())? {
            StructuralStatus::Reject(reason) => {
                return Err(IngestError::Validation(reason));
            }
            StructuralStatus::Pending(reason) => {
                index.mark_pending(assertion_id);
                return Ok(RelayIngestStatus::Pending(assertion_id, reason));
            }
            StructuralStatus::Accept => {}
        }

        reject_local_handle(&assertion)?;

        let action_name = if is_action {
            assertion
                .header
                .typ
                .strip_prefix("action.")
                .unwrap_or(&assertion.header.typ)
        } else {
            assertion.header.typ.as_str()
        };
        let plain = assertion.to_cbor()?;
        let env = store.env();
        if overlay_flag {
            append_overlay(
                env,
                &subject,
                assertion.header.seq,
                assertion_id,
                envelope_id,
                action_name,
                &plain,
            )?;
            index.mark_known(assertion_id);
            index.clear_pending(&assertion_id);
            metrics::assertions_ingested_inc();
            return Ok(RelayIngestStatus::Accepted(envelope_id, assertion_id));
        }

        append_assertion(
            env,
            &subject,
            assertion.header.seq,
            assertion_id,
            envelope_id,
            action_name,
            &plain,
        )?;
        index.update(assertion_id, envelope_id, &assertion.header)?;
        index.clear_pending(&assertion_id);
        metrics::assertions_ingested_inc();
        if is_new_object {
            let domain = subject_domain_name(store, &subject)?;
            crate::relay::record_relay_usage(store, &domain, bytes.len() as u64, 1)?;
        }
        return Ok(RelayIngestStatus::Accepted(envelope_id, assertion_id));
    }

    if is_new_object {
        if let Some(subject) = subject_hint {
            enforce_relay_quota(store, identity, &subject, bytes.len() as u64)?;
            crate::store::state::append_manifest(store.env(), &envelope_id, Some(&subject))?;
            let domain = subject_domain_name(store, &subject)?;
            crate::relay::record_relay_usage(store, &domain, bytes.len() as u64, 1)?;
        }
    }
    store.put_object(&envelope_id, bytes)?;
    Ok(RelayIngestStatus::Opaque(envelope_id))
}

fn enforce_relay_quota(
    store: &Store,
    identity: &crate::identity::IdentityState,
    subject: &SubjectId,
    incoming_bytes: u64,
) -> Result<(), IngestError> {
    let now = store.env().now();
    let policy = crate::relay::resolve_relay_policy(store, subject, now)
        .map_err(|err| IngestError::Validation(format!("relay policy: {err}")))?;
    let authorized = crate::relay::relay_identity_authorized(store, &policy.relay_domain, identity, now)
        .map_err(|err| IngestError::Validation(format!("relay auth: {err}")))?;
    if !authorized {
        return Err(IngestError::Validation(
            "relay identity not authorized for relay domain".to_string(),
        ));
    }
    let plan_bytes = policy.plan.max_bytes.max(0) as u64;
    let usage = crate::relay::relay_usage_for_domain(store, &policy.domain)
        .map_err(|err| IngestError::Validation(format!("relay usage: {err}")))?;
    if usage.bytes.saturating_add(incoming_bytes) > plan_bytes {
        return Err(IngestError::Validation("relay quota exceeded".to_string()));
    }
    if let Some(max_objects) = policy.plan.max_objects {
        let max_objects = max_objects.max(0) as u64;
        if usage.objects.saturating_add(1) > max_objects {
            return Err(IngestError::Validation(
                "relay object quota exceeded".to_string(),
            ));
        }
    }
    Ok(())
}

fn rollback_relay_seeded_ownership(store: &Store, subject: &SubjectId) {
    let env = store.env();
    let ownership_path = crate::store::state::ownership_path(env, subject);
    if env.exists(&ownership_path) {
        let _ = env.remove_file(&ownership_path);
    }
    let indexes_dir = crate::store::state::indexes_dir(env, subject);
    if env.exists(&indexes_dir)
        && env
            .list_dir(&indexes_dir)
            .map(|entries| entries.is_empty())
            .unwrap_or(false)
    {
        let _ = env.remove_dir_all(&indexes_dir);
    }
    let subject_dir = crate::store::state::subject_dir(env, subject);
    if env.exists(&subject_dir)
        && env
            .list_dir(&subject_dir)
            .map(|entries| entries.is_empty())
            .unwrap_or(false)
    {
        let _ = env.remove_dir_all(&subject_dir);
    }
}

fn subject_domain_name(store: &Store, subject: &SubjectId) -> Result<String, IngestError> {
    let now = store.env().now();
    let policy = crate::relay::resolve_relay_policy(store, subject, now)
        .map_err(|err| IngestError::Validation(format!("relay policy: {err}")))?;
    Ok(policy.domain)
}

fn reject_local_handle(assertion: &AssertionPlaintext) -> Result<(), IngestError> {
    let map = match expect_map(&assertion.body) {
        Ok(map) => map,
        Err(_) => return Ok(()),
    };
    for (k, _) in map {
        let name = match expect_text(k) {
            Ok(name) => name,
            Err(_) => continue,
        };
        if name == "local_handle" {
            return Err(IngestError::Validation(
                "local_handle is local-only".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn retry_pending(
    store: &Store,
    index: &mut FrontierIndex,
    keys: &mut Keyring,
) -> Result<usize, IngestError> {
    let mut total_accepted = 0;
    loop {
        let pending = index.pending_objects();
        let mut pass_accepted = 0usize;
        for assertion_id in pending {
            let Some(envelope_id) = store.lookup_envelope(&assertion_id)? else {
                index.clear_pending(&assertion_id);
                continue;
            };
            let bytes = store.get_object(&envelope_id)?;
            match ingest_object(store, index, &bytes, keys) {
                Ok(IngestStatus::Accepted(_)) => {
                    pass_accepted += 1;
                }
                Ok(IngestStatus::Pending(_, _)) => {}
                Err(IngestError::MissingDependency { .. }) => {}
                Err(IngestError::Pending(_)) => {}
                Err(IngestError::Validation(_)) => {
                    index.clear_pending(&assertion_id);
                }
                Err(IngestError::Dharma(err)) => return Err(IngestError::Dharma(err)),
            }
        }
        if pass_accepted == 0 {
            break;
        }
        total_accepted += pass_accepted;
    }
    Ok(total_accepted)
}

enum SchemaKind {
    Cqrs(CqrsSchema),
    Manifest(generic_schema::SchemaManifest),
}

fn load_schema_kind(store: &Store, schema_id: &SchemaId) -> Result<SchemaKind, IngestError> {
    let envelope_id = EnvelopeId::from_bytes(*schema_id.as_bytes());
    let Some(bytes) = store.get_object_any(&envelope_id)? else {
        return Err(IngestError::Pending("missing schema".to_string()));
    };
    if let Ok(schema) = CqrsSchema::from_cbor(&bytes) {
        return Ok(SchemaKind::Cqrs(schema));
    }
    let manifest = generic_schema::parse_schema(&bytes)
        .map_err(|err| IngestError::Validation(err.to_string()))?;
    Ok(SchemaKind::Manifest(manifest))
}

fn load_contract_bytes(store: &Store, contract: &ContractId) -> Result<Vec<u8>, IngestError> {
    let envelope_id = EnvelopeId::from_bytes(*contract.as_bytes());
    match store.get_verified_contract(&envelope_id) {
        Ok(Some(bytes)) => Ok(bytes),
        Ok(None) => Err(IngestError::Pending("missing contract".to_string())),
        Err(DharmaError::Validation(msg)) => Err(IngestError::Pending(msg)),
        Err(err) => Err(IngestError::Dharma(err)),
    }
}

fn validate_action_contract(
    store: &Store,
    index: &FrontierIndex,
    subject: &SubjectId,
    assertion: &AssertionPlaintext,
    assertion_id: AssertionId,
    overlay_base: Option<AssertionId>,
) -> Result<(), IngestError> {
    let schema = match load_schema_kind(store, &assertion.header.schema)? {
        SchemaKind::Cqrs(schema) => schema,
        SchemaKind::Manifest(_) => {
            return Err(IngestError::Validation("expected cqrs schema".to_string()))
        }
    };
    enforce_concurrency(&schema, index, assertion)?;
    let signer = resolve_signer_subject(assertion)?;
    ensure_signer_authorized(
        store,
        &signer,
        &assertion.header.auth,
        &assertion.header.typ,
        assertion.header.ts.unwrap_or(0),
    )?;
    enforce_acting_context(store, assertion, &signer)?;
    let action_name = assertion
        .header
        .typ
        .strip_prefix("action.")
        .unwrap_or(&assertion.header.typ);
    if let Some(summary) = load_permission_summary(store, &assertion.header.contract)? {
        if summary.ver == assertion.header.ver {
            let roles = roles_for_identity(store.env(), &signer).unwrap_or_default();
            if matches!(
                summary.allows_action(&roles, action_name),
                SummaryDecision::Deny
            ) {
                return Err(IngestError::Validation("summary denied".to_string()));
            }
        }
    }
    let action_schema = schema
        .action(action_name)
        .ok_or_else(|| IngestError::Validation("unknown action".to_string()))?;

    let (base_args, overlay_args) = if let Some(base_ref) = overlay_base {
        let Some(base_bytes) = find_assertion_by_id(store.env(), subject, &base_ref)? else {
            return Err(IngestError::MissingDependency {
                assertion_id,
                missing: base_ref,
            });
        };
        let base_assertion = AssertionPlaintext::from_cbor(&base_bytes)?;
        (base_assertion.body, Some(assertion.body.clone()))
    } else {
        let mut overlay_body = None;
        for overlay_bytes in overlays_for_ref(store.env(), subject, &assertion_id)? {
            if let Ok(overlay) = AssertionPlaintext::from_cbor(&overlay_bytes) {
                overlay_body = Some(overlay.body);
                break;
            }
        }
        (assertion.body.clone(), overlay_body)
    };

    let merged = merge_args(&base_args, overlay_args.as_ref())?;
    for (k, _) in crate::value::expect_map(&merged)? {
        let name = crate::value::expect_text(k)?;
        if !action_schema.args.contains_key(&name) {
            return Err(IngestError::Validation("unexpected arg".to_string()));
        }
    }
    if schema.namespace == "std.sys.vault" && action_name == "Checkpoint" {
        enforce_sys_vault_checkpoint(store, subject, &assertion.header.schema, &merged)?;
    }

    let contract_bytes = load_contract_bytes(store, &assertion.header.contract)?;
    let mut state = load_state(store.env(), subject, &schema, &contract_bytes, assertion.header.ver)?;
    let action_idx = action_index(&schema, action_name)?;
    let args_buffer =
        encode_args_buffer(action_schema, &schema.structs, action_idx, &merged, true)?;
    let vm = crate::runtime::vm::RuntimeVm::new(contract_bytes);
    let context = build_action_context(&signer, assertion.header.ts);
    vm.validate(store.env(), &mut state.memory, &args_buffer, Some(&context))
        .map_err(|err| IngestError::Validation(err.to_string()))?;
    Ok(())
}

fn enforce_sys_vault_checkpoint(
    store: &Store,
    subject: &SubjectId,
    schema_id: &SchemaId,
    args: &Value,
) -> Result<(), IngestError> {
    let args_map = expect_map(args).map_err(|err| IngestError::Validation(err.to_string()))?;
    let start_seq = expect_int(require_map_value(args_map, "start_seq")?)
        .map_err(|err| IngestError::Validation(err.to_string()))?;
    let end_seq = expect_int(require_map_value(args_map, "end_seq")?)
        .map_err(|err| IngestError::Validation(err.to_string()))?;
    if start_seq < 0 || end_seq < 0 {
        return Err(IngestError::Validation("checkpoint seq negative".to_string()));
    }
    if end_seq <= start_seq {
        return Err(IngestError::Validation(
            "checkpoint range invalid".to_string(),
        ));
    }
    ensure_hash_field(args_map, "state_root")?;

    let vault_val = require_map_value(args_map, "vault")?;
    let vault_map = expect_map(vault_val).map_err(|err| IngestError::Validation(err.to_string()))?;

    let subject_bytes = expect_bytes(require_map_value(vault_map, "subject")?)
        .map_err(|err| IngestError::Validation(err.to_string()))?;
    let vault_subject =
        SubjectId::from_slice(&subject_bytes).map_err(|err| IngestError::Validation(err.to_string()))?;
    if &vault_subject != subject {
        return Err(IngestError::Validation(
            "vault subject mismatch".to_string(),
        ));
    }

    let vault_start = expect_int(require_map_value(vault_map, "seq_start")?)
        .map_err(|err| IngestError::Validation(err.to_string()))?;
    let vault_end = expect_int(require_map_value(vault_map, "seq_end")?)
        .map_err(|err| IngestError::Validation(err.to_string()))?;
    if vault_start != start_seq {
        return Err(IngestError::Validation(
            "vault seq_start mismatch".to_string(),
        ));
    }
    if vault_end != end_seq {
        return Err(IngestError::Validation(
            "vault seq_end mismatch".to_string(),
        ));
    }

    let format_version = expect_int(require_map_value(vault_map, "format_version")?)
        .map_err(|err| IngestError::Validation(err.to_string()))?;
    if format_version != i64::from(DHBOX_VERSION_V1) {
        return Err(IngestError::Validation(
            "unsupported vault format version".to_string(),
        ));
    }

    ensure_hash_field(vault_map, "hash")?;
    ensure_hash_field(vault_map, "snapshot_hash")?;
    ensure_hash_field(vault_map, "merkle_root")?;

    if let Some(value) = map_get(vault_map, "dict_hash") {
        if !matches!(value, Value::Null) {
            ensure_hash_value(value, "dict_hash")?;
        }
    }
    if let Some(value) = map_get(vault_map, "dict_size") {
        if !matches!(value, Value::Null) {
            let size = expect_int(value).map_err(|err| IngestError::Validation(err.to_string()))?;
            if size < 0 {
                return Err(IngestError::Validation("dict_size negative".to_string()));
            }
        }
    }
    if let Some(value) = map_get(vault_map, "size") {
        let size = expect_int(value).map_err(|err| IngestError::Validation(err.to_string()))?;
        if size < 0 {
            return Err(IngestError::Validation("vault size negative".to_string()));
        }
    }
    if let Some(value) = map_get(vault_map, "shards") {
        if !matches!(value, Value::Null) {
            let shards = expect_array(value).map_err(|err| IngestError::Validation(err.to_string()))?;
            for shard in shards {
                let shard_map =
                    expect_map(shard).map_err(|err| IngestError::Validation(err.to_string()))?;
                let shard_index = expect_int(require_map_value(shard_map, "shard_index")?)
                    .map_err(|err| IngestError::Validation(err.to_string()))?;
                let shard_total = expect_int(require_map_value(shard_map, "shard_total")?)
                    .map_err(|err| IngestError::Validation(err.to_string()))?;
                if shard_index < 0 || shard_total <= 0 || shard_index >= shard_total {
                    return Err(IngestError::Validation(
                        "invalid shard index".to_string(),
                    ));
                }
                ensure_hash_field(shard_map, "hash")?;
                if let Some(value) = map_get(shard_map, "size") {
                    let size =
                        expect_int(value).map_err(|err| IngestError::Validation(err.to_string()))?;
                    if size < 0 {
                        return Err(IngestError::Validation("shard size negative".to_string()));
                    }
                }
            }
        }
    }

    if let Some(last_end) = last_checkpoint_end(store.env(), subject, schema_id)? {
        if start_seq < last_end {
            return Err(IngestError::Validation("checkpoint regressed".to_string()));
        }
    }
    Ok(())
}

fn last_checkpoint_end(
    env: &dyn Env,
    subject: &SubjectId,
    schema_id: &SchemaId,
) -> Result<Option<i64>, IngestError> {
    let mut last_end: Option<i64> = None;
    for record in list_assertions(env, subject).map_err(IngestError::from)? {
        let assertion = AssertionPlaintext::from_cbor(&record.bytes)?;
        if &assertion.header.schema != schema_id {
            continue;
        }
        let action_name = assertion
            .header
            .typ
            .strip_prefix("action.")
            .unwrap_or(&assertion.header.typ);
        if action_name != "Checkpoint" {
            continue;
        }
        let map = expect_map(&assertion.body)
            .map_err(|err| IngestError::Validation(err.to_string()))?;
        let end_value = require_map_value(map, "end_seq")?;
        let end_seq =
            expect_int(end_value).map_err(|err| IngestError::Validation(err.to_string()))?;
        if end_seq < 0 {
            return Err(IngestError::Validation("checkpoint end_seq negative".to_string()));
        }
        if last_end.map_or(true, |current| end_seq > current) {
            last_end = Some(end_seq);
        }
    }
    Ok(last_end)
}

fn require_map_value<'a>(
    map: &'a Vec<(Value, Value)>,
    key: &str,
) -> Result<&'a Value, IngestError> {
    map_get(map, key).ok_or_else(|| IngestError::Validation(format!("missing {key}")))
}

fn ensure_hash_field(map: &Vec<(Value, Value)>, key: &str) -> Result<(), IngestError> {
    let value = require_map_value(map, key)?;
    ensure_hash_value(value, key)
}

fn ensure_hash_value(value: &Value, label: &str) -> Result<(), IngestError> {
    let bytes = expect_bytes(value).map_err(|err| IngestError::Validation(err.to_string()))?;
    if bytes.len() != 32 {
        return Err(IngestError::Validation(format!("{label} must be 32 bytes")));
    }
    Ok(())
}

fn load_permission_summary(
    store: &Store,
    contract: &ContractId,
) -> Result<Option<crate::contract::PermissionSummary>, IngestError> {
    match store.get_permission_summary(contract) {
        Ok(summary) => Ok(summary),
        Err(DharmaError::Validation(msg)) => Err(IngestError::Validation(msg)),
        Err(err) => Err(IngestError::Dharma(err)),
    }
}

fn resolve_signer_subject(assertion: &AssertionPlaintext) -> Result<SubjectId, IngestError> {
    signer_from_meta(&assertion.header.meta)
        .ok_or_else(|| IngestError::Validation("missing signer".to_string()))
}

fn ensure_signer_authorized(
    store: &Store,
    signer: &SubjectId,
    auth: &IdentityKey,
    action: &str,
    now: i64,
) -> Result<(), IngestError> {
    if action == "core.genesis" || action == ATLAS_IDENTITY_GENESIS {
        return Ok(());
    }
    let Some(_root_key) = root_key_for_identity(store.env(), signer)? else {
        return Err(IngestError::Pending("missing identity root".to_string()));
    };
    if device_key_revoked(store.env(), signer, auth)? {
        return Err(IngestError::Validation("device revoked".to_string()));
    }
    if delegate_allows(store.env(), signer, auth, action, now)? {
        Ok(())
    } else {
        Err(IngestError::Validation("unauthorized signer".to_string()))
    }
}

fn enforce_identity_lifecycle(
    env: &dyn Env,
    subject: &SubjectId,
    assertion: &AssertionPlaintext,
) -> Result<(), IngestError> {
    let typ = assertion.header.typ.as_str();
    if typ != ATLAS_IDENTITY_ACTIVATE && typ != ATLAS_IDENTITY_SUSPEND && typ != ATLAS_IDENTITY_REVOKE {
        return Ok(());
    }
    let Some(root_key) = root_key_for_identity(env, subject)? else {
        return Err(IngestError::Pending("missing identity root".to_string()));
    };
    if assertion.header.auth.as_bytes() != root_key.as_bytes() {
        return Err(IngestError::Validation(
            "unauthorized lifecycle signer".to_string(),
        ));
    }
    Ok(())
}

fn enforce_acting_context(
    store: &Store,
    assertion: &AssertionPlaintext,
    signer_subject: &SubjectId,
) -> Result<(), IngestError> {
    let mut acting_identity: Option<SubjectId> = None;
    let mut acting_domain: Option<SubjectId> = None;
    let mut acting_role: Option<String> = None;
    if let Some(Value::Map(entries)) = &assertion.header.meta {
        for (key, value) in entries {
            let Value::Text(name) = key else { continue };
            match name.as_str() {
                "acting_identity" => {
                    let bytes = expect_bytes(value).map_err(|err| IngestError::Validation(err.to_string()))?;
                    let id = SubjectId::from_slice(&bytes).map_err(IngestError::from)?;
                    acting_identity = Some(id);
                }
                "acting_domain" => {
                    let bytes = expect_bytes(value).map_err(|err| IngestError::Validation(err.to_string()))?;
                    let id = SubjectId::from_slice(&bytes).map_err(IngestError::from)?;
                    acting_domain = Some(id);
                }
                "acting_role" => {
                    let role = expect_text(value).map_err(|err| IngestError::Validation(err.to_string()))?;
                    if !role.is_empty() {
                        acting_role = Some(role);
                    }
                }
                _ => {}
            }
        }
    }
    if acting_identity.is_some() && acting_domain.is_some() {
        return Err(IngestError::Validation(
            "acting_identity and acting_domain are mutually exclusive".to_string(),
        ));
    }
    if let Some(acting_identity) = acting_identity {
        if acting_identity.as_bytes() != signer_subject.as_bytes() {
            return Err(IngestError::Validation(
                "acting_identity must match signer".to_string(),
            ));
        }
    }
    let Some(domain_subject) = acting_domain else {
        return Ok(());
    };
    let Some(root_key) = root_key_for_identity(store.env(), signer_subject)? else {
        return Err(IngestError::Pending("missing identity root".to_string()));
    };
    if !subject_has_facts(store.env(), &domain_subject)? {
        return Err(IngestError::Pending("missing domain membership".to_string()));
    }
    let state = DomainState::load(store, &domain_subject)?;
    let now = assertion.header.ts.unwrap_or(0);
    let member = state.member(&root_key, now).ok_or_else(|| {
        IngestError::Validation("not a domain member".to_string())
    })?;
    if let Some(role) = acting_role {
        if !member.roles.iter().any(|r| r == &role) {
            return Err(IngestError::Validation("role not granted".to_string()));
        }
    }
    Ok(())
}

fn build_action_context(signer: &SubjectId, ts: Option<i64>) -> Vec<u8> {
    let mut buf = vec![0u8; 40];
    buf[..32].copy_from_slice(signer.as_bytes());
    let timestamp = ts.unwrap_or(0);
    buf[32..40].copy_from_slice(&timestamp.to_le_bytes());
    buf
}

fn enforce_concurrency(
    schema: &CqrsSchema,
    index: &FrontierIndex,
    assertion: &AssertionPlaintext,
) -> Result<(), IngestError> {
    if schema.concurrency != ConcurrencyMode::Strict {
        return Ok(());
    }
    let tips = index.get_tips_for_ver(&assertion.header.sub, assertion.header.ver);
    if tips.len() > 1 {
        return Err(IngestError::Pending(
            "fork detected; merge required".to_string(),
        ));
    }
    if let Some(prev) = assertion.header.prev {
        if tips.len() == 1 && tips[0] != prev {
            return Err(IngestError::Pending(
                "fork detected; merge required".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_generic_contract(store: &Store, assertion: &AssertionPlaintext) -> Result<(), IngestError> {
    let schema = match load_schema_kind(store, &assertion.header.schema)? {
        SchemaKind::Manifest(schema) => schema,
        SchemaKind::Cqrs(_) => {
            return Err(IngestError::Validation("unexpected cqrs schema".to_string()))
        }
    };
    generic_schema::validate_body(&schema, &assertion.header.typ, &assertion.body)
        .map_err(|err| IngestError::Validation(err.to_string()))?;
    let contract_bytes = load_contract_bytes(store, &assertion.header.contract)?;
    let context = contract_context(&assertion.header.sub)?;
    let assertion_bytes = assertion.to_cbor()?;
    let engine = ContractEngine::new(contract_bytes);
    let result = engine
        .validate_with_env(store.env(), &assertion_bytes, &context)
        .map_err(|err| IngestError::Validation(err.to_string()))?;
    match result.status {
        ContractStatus::Accept if result.ok => Ok(()),
        ContractStatus::Pending => Err(IngestError::Pending(
            result.reason.unwrap_or_else(|| "contract pending".to_string()),
        )),
        _ => {
            warn!(
                reason = ?result.reason,
                "generic contract rejected"
            );
            Err(IngestError::Validation(
                result.reason.unwrap_or_else(|| "contract rejected".to_string()),
            ))
        }
    }
}

fn contract_context(subject: &SubjectId) -> Result<Vec<u8>, IngestError> {
    let value = Value::Map(vec![
        (
            Value::Text("subject".to_string()),
            Value::Bytes(subject.as_bytes().to_vec()),
        ),
        (Value::Text("accepted".to_string()), Value::Array(vec![])),
        (Value::Text("lookup".to_string()), Value::Map(vec![])),
    ]);
    crate::cbor::encode_canonical_value(&value).map_err(IngestError::from)
}

fn decode_assertion(
    bytes: &[u8],
    keys: &Keyring,
) -> Result<(EnvelopeId, AssertionId, SubjectId, AssertionPlaintext), IngestError> {
    if let Ok(envelope) = AssertionEnvelope::from_cbor(bytes) {
        let envelope_id = envelope.envelope_id()?;
        if let Some(key) = keys.key_for_kid(&envelope.kid) {
            if let Ok(plaintext) = envelope::decrypt_assertion(&envelope, key) {
                if let Ok(assertion) = AssertionPlaintext::from_cbor(&plaintext) {
                    let assertion_id = assertion.assertion_id()?;
                    return Ok((envelope_id, assertion_id, assertion.header.sub, assertion));
                }
            }
        }
        return Err(IngestError::Validation("unable to decrypt".to_string()));
    }

    let assertion = AssertionPlaintext::from_cbor(bytes)?;
    let envelope_id = crypto::envelope_id(bytes);
    let assertion_id = assertion.assertion_id()?;
    Ok((envelope_id, assertion_id, assertion.header.sub, assertion))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{add_signer_meta, AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::contract::{PermissionRule, PermissionSummary, PublicPermissions};
    use crate::crypto;
    use crate::keys::{hpke_public_key_from_secret, hpke_seal, key_id_for_key};
    use crate::identity::{ATLAS_IDENTITY_GENESIS, ATLAS_IDENTITY_SUSPEND};
    use crate::ownership::{Owner, OwnershipRecord};
    use crate::store::state::save_ownership;
    use crate::pdl::schema::{ActionSchema, ConcurrencyMode, CqrsSchema, FieldSchema, TypeSpec, Visibility};
    use crate::types::{AssertionId, ContractId, EnvelopeId, Nonce12, SchemaId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::RngCore;
    use rand::SeedableRng;
    use std::collections::{BTreeMap, BTreeSet};

    fn make_enveloped(
        subject: SubjectId,
        subject_key: &[u8; 32],
        signing_key: &ed25519_dalek::SigningKey,
        seq: u64,
        prev: Option<AssertionId>,
        auth_override: Option<crate::types::IdentityKey>,
        schema: SchemaId,
        contract: ContractId,
    ) -> Vec<u8> {
        let auth = auth_override.unwrap_or_else(|| {
            crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes())
        });
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth,
            seq,
            prev,
            refs: Vec::new(),
            ts: None,
            schema,
            contract,
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![(
            Value::Text("text".to_string()),
            Value::Text("hello".to_string()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, signing_key).unwrap();
        let plaintext = assertion.to_cbor().unwrap();
        let kid = crypto::key_id_from_key(subject_key);
        let envelope = envelope::encrypt_assertion(
            &plaintext,
            kid,
            subject_key,
            Nonce12::from_bytes([9u8; 12]),
        )
        .unwrap();
        envelope.to_cbor().unwrap()
    }

    fn make_plain_assertion(
        subject: SubjectId,
        typ: &str,
        signing_key: &ed25519_dalek::SigningKey,
        auth: crate::types::IdentityKey,
        seq: u64,
        prev: Option<AssertionId>,
        refs: Vec<AssertionId>,
        schema: SchemaId,
        contract: ContractId,
        body: Value,
    ) -> (Vec<u8>, AssertionId) {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: typ.to_string(),
            auth,
            seq,
            prev,
            refs,
            ts: None,
            schema,
            contract,
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, body, signing_key).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        (bytes, assertion_id)
    }

    fn setup_relay_policy(
        store: &Store,
        relay_identity: &crate::identity::IdentityState,
        relay_domain: &str,
        relay_subject: SubjectId,
        plan_name: &str,
        domain_name: &str,
        domain_subject: SubjectId,
        domain_owner_id: crate::types::IdentityKey,
        domain_owner_sk: &ed25519_dalek::SigningKey,
        target_subject: SubjectId,
    ) {
        let env = store.env();
        let schema = SchemaId::from_bytes([1u8; 32]);
        let contract = ContractId::from_bytes([2u8; 32]);

        let relay_genesis_body = Value::Map(vec![
            (Value::Text("domain".to_string()), Value::Text(relay_domain.to_string())),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(relay_identity.public_key.as_bytes().to_vec()),
            ),
        ]);
        let (relay_genesis_bytes, relay_genesis_id) = make_plain_assertion(
            relay_subject,
            "atlas.domain.genesis",
            &relay_identity.signing_key,
            relay_identity.public_key,
            1,
            None,
            Vec::new(),
            schema,
            contract,
            relay_genesis_body,
        );
        let relay_env = crypto::envelope_id(&relay_genesis_bytes);
        append_assertion(
            env,
            &relay_subject,
            1,
            relay_genesis_id,
            relay_env,
            "atlas.domain.genesis",
            &relay_genesis_bytes,
        )
        .unwrap();

        let plan_body = Value::Map(vec![
            (Value::Text("name".to_string()), Value::Text(plan_name.to_string())),
            (
                Value::Text("max_bytes".to_string()),
                Value::Integer(10_000_000u64.into()),
            ),
        ]);
        let (plan_bytes, plan_id) = make_plain_assertion(
            relay_subject,
            "sys.relay.plan.define",
            &relay_identity.signing_key,
            relay_identity.public_key,
            2,
            Some(relay_genesis_id),
            vec![relay_genesis_id],
            schema,
            contract,
            plan_body,
        );
        let plan_env = crypto::envelope_id(&plan_bytes);
        append_assertion(
            env,
            &relay_subject,
            2,
            plan_id,
            plan_env,
            "sys.relay.plan.define",
            &plan_bytes,
        )
        .unwrap();

        let grant_body = Value::Map(vec![
            (Value::Text("domain".to_string()), Value::Text(domain_name.to_string())),
            (Value::Text("plan".to_string()), Value::Text(plan_name.to_string())),
            (Value::Text("expires".to_string()), Value::Integer(0.into())),
        ]);
        let (grant_bytes, grant_id) = make_plain_assertion(
            relay_subject,
            "sys.relay.grant",
            &relay_identity.signing_key,
            relay_identity.public_key,
            3,
            Some(plan_id),
            vec![plan_id],
            schema,
            contract,
            grant_body,
        );
        let grant_env = crypto::envelope_id(&grant_bytes);
        append_assertion(
            env,
            &relay_subject,
            3,
            grant_id,
            grant_env,
            "sys.relay.grant",
            &grant_bytes,
        )
        .unwrap();

        let domain_genesis_body = Value::Map(vec![
            (Value::Text("domain".to_string()), Value::Text(domain_name.to_string())),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(domain_owner_id.as_bytes().to_vec()),
            ),
        ]);
        let (domain_genesis_bytes, domain_genesis_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.genesis",
            domain_owner_sk,
            domain_owner_id,
            1,
            None,
            Vec::new(),
            schema,
            contract,
            domain_genesis_body,
        );
        let domain_env = crypto::envelope_id(&domain_genesis_bytes);
        append_assertion(
            env,
            &domain_subject,
            1,
            domain_genesis_id,
            domain_env,
            "atlas.domain.genesis",
            &domain_genesis_bytes,
        )
        .unwrap();

        let policy_body = Value::Map(vec![
            (
                Value::Text("relay_domain".to_string()),
                Value::Text(relay_domain.to_string()),
            ),
            (
                Value::Text("relay_plan".to_string()),
                Value::Text(plan_name.to_string()),
            ),
        ]);
        let (policy_bytes, policy_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.policy",
            domain_owner_sk,
            domain_owner_id,
            2,
            Some(domain_genesis_id),
            vec![domain_genesis_id],
            schema,
            contract,
            policy_body,
        );
        let policy_env = crypto::envelope_id(&policy_bytes);
        append_assertion(
            env,
            &domain_subject,
            2,
            policy_id,
            policy_env,
            "atlas.domain.policy",
            &policy_bytes,
        )
        .unwrap();

        let record = OwnershipRecord {
            owner: Owner::Domain(domain_subject),
            creator: domain_owner_id,
            acting_domain: None,
            role: None,
        };
        save_ownership(env, &target_subject, &record).unwrap();
    }

    #[test]
    fn relay_ingest_accepts_plaintext() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::default();
        let mut rng = StdRng::seed_from_u64(55);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let (domain_owner_sk, domain_owner_id) = crypto::generate_identity_keypair(&mut rng);
        let relay_subject = SubjectId::from_bytes([1u8; 32]);
        let domain_subject = SubjectId::from_bytes([2u8; 32]);
        let subject = SubjectId::from_bytes([9u8; 32]);
        let relay_identity = crate::identity::IdentityState {
            subject_id: SubjectId::from_bytes([7u8; 32]),
            signing_key: signing_key.clone(),
            public_key: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            root_signing_key: signing_key.clone(),
            root_public_key: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            subject_key: [0u8; 32],
            noise_sk: [1u8; 32],
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
        };
        setup_relay_policy(
            &store,
            &relay_identity,
            "relays.public.dharma",
            relay_subject,
            "free",
            "corp.acme",
            domain_subject,
            domain_owner_id,
            &domain_owner_sk,
            subject,
        );
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, Value::Null, &signing_key).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let envelope_id = crypto::envelope_id(&bytes);
        let assertion_id = assertion.assertion_id().unwrap();
        let status = ingest_object_relay(
            &store,
            &mut index,
            &relay_identity,
            envelope_id,
            &bytes,
            None,
        )
        .unwrap();
        assert_eq!(status, RelayIngestStatus::Accepted(envelope_id, assertion_id));
    }

    #[test]
    fn relay_ingest_accepts_envelope_as_opaque() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::default();
        let mut rng = StdRng::seed_from_u64(77);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let (domain_owner_sk, domain_owner_id) = crypto::generate_identity_keypair(&mut rng);
        let relay_subject = SubjectId::from_bytes([10u8; 32]);
        let domain_subject = SubjectId::from_bytes([11u8; 32]);
        let subject = SubjectId::from_bytes([3u8; 32]);
        let relay_identity = crate::identity::IdentityState {
            subject_id: SubjectId::from_bytes([8u8; 32]),
            signing_key: signing_key.clone(),
            public_key: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            root_signing_key: signing_key.clone(),
            root_public_key: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            subject_key: [0u8; 32],
            noise_sk: [1u8; 32],
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
        };
        setup_relay_policy(
            &store,
            &relay_identity,
            "relays.public.dharma",
            relay_subject,
            "free",
            "corp.acme",
            domain_subject,
            domain_owner_id,
            &domain_owner_sk,
            subject,
        );
        let subject_key = [7u8; 32];
        let bytes = make_enveloped(
            subject,
            &subject_key,
            &signing_key,
            1,
            None,
            None,
            SchemaId::from_bytes([4u8; 32]),
            ContractId::from_bytes([5u8; 32]),
        );
        let envelope_id = crypto::envelope_id(&bytes);
        let status = ingest_object_relay(
            &store,
            &mut index,
            &relay_identity,
            envelope_id,
            &bytes,
            Some(subject),
        )
        .unwrap();
        assert_eq!(status, RelayIngestStatus::Opaque(envelope_id));
    }

    #[test]
    fn relay_ingest_policy_reject_rolls_back_seeded_subject() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::default();
        let mut rng = StdRng::seed_from_u64(79);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let relay_identity = crate::identity::IdentityState {
            subject_id: SubjectId::from_bytes([18u8; 32]),
            signing_key: signing_key.clone(),
            public_key: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            root_signing_key: signing_key.clone(),
            root_public_key: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            subject_key: [0u8; 32],
            noise_sk: [1u8; 32],
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
        };
        let subject = SubjectId::from_bytes([19u8; 32]);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, Value::Null, &signing_key).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let envelope_id = crypto::envelope_id(&bytes);
        let err = ingest_object_relay(
            &store,
            &mut index,
            &relay_identity,
            envelope_id,
            &bytes,
            None,
        )
        .unwrap_err();
        match err {
            IngestError::Validation(reason) => {
                assert!(reason.contains("missing domain ownership"));
            }
            other => panic!("expected relay policy validation error, got {other:?}"),
        }
        let subjects = store.list_subjects().unwrap();
        assert!(!subjects.contains(&subject));
        assert!(crate::store::state::load_ownership(store.env(), &subject)
            .unwrap()
            .is_none());
    }

    fn simple_contract_bytes() -> Vec<u8> {
        wat::parse_str(
            r#"(module
                (memory (export "memory") 1)
                (func (export "validate") (result i32)
                  i32.const 0)
                (func (export "reduce") (result i32)
                  i32.const 0)
              )"#,
        )
        .unwrap()
    }

    fn reject_contract_bytes() -> Vec<u8> {
        wat::parse_str(
            r#"(module
                (memory (export "memory") 1)
                (func (export "validate") (result i32)
                  i32.const 1)
                (func (export "reduce") (result i32)
                  i32.const 0)
              )"#,
        )
        .unwrap()
    }

    fn make_action_bytes(
        subject: SubjectId,
        signer_subject: SubjectId,
        signing_key: &ed25519_dalek::SigningKey,
        seq: u64,
        prev: Option<AssertionId>,
        schema: SchemaId,
        contract: ContractId,
        value: i64,
    ) -> Vec<u8> {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "action.Touch".to_string(),
            auth: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq,
            prev,
            refs: Vec::new(),
            ts: None,
            schema,
            contract,
            note: None,
            meta: add_signer_meta(None, &signer_subject),
        };
        let body = Value::Map(vec![(
            Value::Text("value".to_string()),
            Value::Integer(value.into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, signing_key).unwrap();
        assertion.to_cbor().unwrap()
    }

    #[test]
    fn ingest_object_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert(
            "value".to_string(),
            crate::pdl::schema::FieldSchema {
                typ: TypeSpec::Int,
                default: Some(Value::Integer(0.into())),
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("value".to_string(), TypeSpec::Int);
        actions.insert(
            "Touch".to_string(),
            ActionSchema {
                args,
                arg_vis: BTreeMap::new(),
                doc: None,
            },
        );
        let schema = CqrsSchema {
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Demo".to_string(),
            extends: None,
            implements: Vec::new(),
            structs: BTreeMap::new(),
            fields,
            actions,
            queries: BTreeMap::new(),
        projections: BTreeMap::new(),
            concurrency: ConcurrencyMode::Allow,
        };
        let schema_bytes = schema.to_cbor().unwrap();
        let action_schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
        let contract_bytes = simple_contract_bytes();
        let action_contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
        store
            .put_object(&EnvelopeId::from_bytes(*action_schema_id.as_bytes()), &schema_bytes)
            .unwrap();
        store
            .put_object(
                &EnvelopeId::from_bytes(*action_contract_id.as_bytes()),
                &contract_bytes,
            )
            .unwrap();
        let mut rng = StdRng::seed_from_u64(7);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([3u8; 32]);
        let mut subject_key = [0u8; 32];
        rng.fill_bytes(&mut subject_key);
        let bytes = make_enveloped(
            subject,
            &subject_key,
            &signing_key,
            1,
            None,
            None,
            schema_id,
            contract_id,
        );

        let mut keys = Keyring::new();
        keys.insert_sdk(subject, 0, subject_key);
        let _assertion_id = match ingest_object(&store, &mut index, &bytes, &mut keys).unwrap() {
            IngestStatus::Accepted(id) => id,
            other => panic!("expected accepted, got {other:?}"),
        };

        let envelope_id = crypto::envelope_id(&bytes);
        assert!(index.has_envelope(&envelope_id));
        let stored = store.get_assertion(&subject, &envelope_id).unwrap();
        assert_eq!(stored, bytes);
    }

    #[test]
    fn ingest_object_missing_dependency() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(11);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([4u8; 32]);
        let mut subject_key = [0u8; 32];
        rng.fill_bytes(&mut subject_key);
        let missing = AssertionId::from_bytes([8u8; 32]);
        let bytes = make_enveloped(
            subject,
            &subject_key,
            &signing_key,
            2,
            Some(missing),
            None,
            schema_id,
            contract_id,
        );

        let mut keys = Keyring::new();
        keys.insert_sdk(subject, 0, subject_key);
        let err = ingest_object(&store, &mut index, &bytes, &mut keys).unwrap_err();
        match err {
            IngestError::MissingDependency { missing: dep, .. } => assert_eq!(dep, missing),
            _ => panic!("expected missing dependency"),
        }
    }

    #[test]
    fn ingest_object_pending_missing_schema() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(14);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([6u8; 32]);
        let mut subject_key = [0u8; 32];
        rng.fill_bytes(&mut subject_key);
        let schema_id = SchemaId::from_bytes([9u8; 32]);
        let contract_id = ContractId::from_bytes([10u8; 32]);
        let bytes = make_enveloped(
            subject,
            &subject_key,
            &signing_key,
            1,
            None,
            None,
            schema_id,
            contract_id,
        );

        let mut keys = Keyring::new();
        keys.insert_sdk(subject, 0, subject_key);
        let status = ingest_object(&store, &mut index, &bytes, &mut keys).unwrap();
        match status {
            IngestStatus::Pending(_, reason) => assert!(reason.contains("schema")),
            _ => panic!("expected pending"),
        }
    }

    #[test]
    fn ingest_action_rejects_without_delegate() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(22);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let (device_sk, device_id) = crypto::generate_identity_keypair(&mut rng);
        let identity_subject = SubjectId::from_bytes([10u8; 32]);
        let (identity_schema, identity_contract) = crate::builtins::ensure_note_artifacts(&store).unwrap();

        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: identity_subject,
            typ: "core.genesis".to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: identity_schema,
            contract: identity_contract,
            note: None,
            meta: add_signer_meta(None, &identity_subject),
        };
        let genesis = AssertionPlaintext::sign(genesis_header, Value::Map(vec![]), &root_sk).unwrap();
        let genesis_bytes = genesis.to_cbor().unwrap();
        let genesis_id = genesis.assertion_id().unwrap();
        let genesis_env = crypto::envelope_id(&genesis_bytes);
        append_assertion(
            store.env(),
            &identity_subject,
            1,
            genesis_id,
            genesis_env,
            "core.genesis",
            &genesis_bytes,
        )
        .unwrap();

        let mut fields = BTreeMap::new();
        fields.insert(
            "value".to_string(),
            crate::pdl::schema::FieldSchema {
                typ: TypeSpec::Int,
                default: Some(Value::Integer(0.into())),
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("value".to_string(), TypeSpec::Int);
        actions.insert(
            "Touch".to_string(),
            ActionSchema {
                args,
                arg_vis: BTreeMap::new(),
                doc: None,
            },
        );
        let schema = CqrsSchema {
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Demo".to_string(),
            extends: None,
            implements: Vec::new(),
            structs: BTreeMap::new(),
            fields,
            actions,
            queries: BTreeMap::new(),
        projections: BTreeMap::new(),
            concurrency: ConcurrencyMode::Allow,
        };
        let schema_bytes = schema.to_cbor().unwrap();
        let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
        let contract_bytes = simple_contract_bytes();
        let contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
        store
            .put_object(&EnvelopeId::from_bytes(*schema_id.as_bytes()), &schema_bytes)
            .unwrap();
        store
            .put_object(
                &EnvelopeId::from_bytes(*contract_id.as_bytes()),
                &contract_bytes,
            )
            .unwrap();

        let action_subject = SubjectId::from_bytes([11u8; 32]);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: action_subject,
            typ: "action.Touch".to_string(),
            auth: device_id,
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: None,
            meta: add_signer_meta(None, &identity_subject),
        };
        let body = Value::Map(vec![(
            Value::Text("value".to_string()),
            Value::Integer(1.into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &device_sk).unwrap();
        let bytes = assertion.to_cbor().unwrap();

        let mut keys = Keyring::new();
        let err = ingest_object(&store, &mut index, &bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("unauthorized")),
            _ => panic!("expected unauthorized signer"),
        }
    }

    #[test]
    fn ingest_action_accepts_delegated_device() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(23);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let (device_sk, device_id) = crypto::generate_identity_keypair(&mut rng);
        let identity_subject = SubjectId::from_bytes([12u8; 32]);
        let (identity_schema, identity_contract) = crate::builtins::ensure_note_artifacts(&store).unwrap();

        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: identity_subject,
            typ: "core.genesis".to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: identity_schema,
            contract: identity_contract,
            note: None,
            meta: add_signer_meta(None, &identity_subject),
        };
        let genesis = AssertionPlaintext::sign(genesis_header, Value::Map(vec![]), &root_sk).unwrap();
        let genesis_bytes = genesis.to_cbor().unwrap();
        let genesis_id = genesis.assertion_id().unwrap();
        let genesis_env = crypto::envelope_id(&genesis_bytes);
        append_assertion(
            store.env(),
            &identity_subject,
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
            sub: identity_subject,
            typ: "iam.delegate".to_string(),
            auth: root_id,
            seq: 2,
            prev: Some(genesis_id),
            refs: vec![genesis_id],
            ts: None,
            schema: identity_schema,
            contract: identity_contract,
            note: None,
            meta: add_signer_meta(None, &identity_subject),
        };
        let delegate_body = Value::Map(vec![
            (
                Value::Text("delegate".to_string()),
                Value::Bytes(device_id.as_bytes().to_vec()),
            ),
            (Value::Text("scope".to_string()), Value::Text("all".to_string())),
            (Value::Text("expires".to_string()), Value::Integer(0.into())),
        ]);
        let delegate = AssertionPlaintext::sign(delegate_header, delegate_body, &root_sk).unwrap();
        let delegate_bytes = delegate.to_cbor().unwrap();
        let delegate_id = delegate.assertion_id().unwrap();
        let delegate_env = crypto::envelope_id(&delegate_bytes);
        append_assertion(
            store.env(),
            &identity_subject,
            2,
            delegate_id,
            delegate_env,
            "iam.delegate",
            &delegate_bytes,
        )
        .unwrap();

        let mut fields = BTreeMap::new();
        fields.insert(
            "value".to_string(),
            crate::pdl::schema::FieldSchema {
                typ: TypeSpec::Int,
                default: Some(Value::Integer(0.into())),
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("value".to_string(), TypeSpec::Int);
        actions.insert(
            "Touch".to_string(),
            ActionSchema {
                args,
                arg_vis: BTreeMap::new(),
                doc: None,
            },
        );
        let schema = CqrsSchema {
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Demo".to_string(),
            extends: None,
            implements: Vec::new(),
            structs: BTreeMap::new(),
            fields,
            actions,
            queries: BTreeMap::new(),
        projections: BTreeMap::new(),
            concurrency: ConcurrencyMode::Allow,
        };
        let schema_bytes = schema.to_cbor().unwrap();
        let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
        let contract_bytes = simple_contract_bytes();
        let contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
        store
            .put_object(&EnvelopeId::from_bytes(*schema_id.as_bytes()), &schema_bytes)
            .unwrap();
        store
            .put_object(
                &EnvelopeId::from_bytes(*contract_id.as_bytes()),
                &contract_bytes,
            )
            .unwrap();

        let action_subject = SubjectId::from_bytes([13u8; 32]);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: action_subject,
            typ: "action.Touch".to_string(),
            auth: device_id,
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: None,
            meta: add_signer_meta(None, &identity_subject),
        };
        let body = Value::Map(vec![(
            Value::Text("value".to_string()),
            Value::Integer(1.into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &device_sk).unwrap();
        let bytes = assertion.to_cbor().unwrap();

        let mut keys = Keyring::new();
        let status = ingest_object(&store, &mut index, &bytes, &mut keys).unwrap();
        match status {
            IngestStatus::Accepted(_) => {}
            _ => panic!("expected accepted"),
        }
    }

    #[test]
    fn retry_pending_accepts_after_dependency() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(31);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([7u8; 32]);

        let header1 = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: None,
            meta: None,
        };
        let body1 = Value::Map(vec![(Value::Text("text".to_string()), Value::Text("a".to_string()))]);
        let assertion1 = AssertionPlaintext::sign(header1, body1, &signing_key).unwrap();
        let bytes1 = assertion1.to_cbor().unwrap();
        let assertion_id1 = assertion1.assertion_id().unwrap();
        let _envelope_id1 = crypto::envelope_id(&bytes1);

        let header2 = AssertionHeader {
            seq: 2,
            prev: Some(assertion_id1),
            ..assertion1.header.clone()
        };
        let body2 = Value::Map(vec![(Value::Text("text".to_string()), Value::Text("b".to_string()))]);
        let assertion2 = AssertionPlaintext::sign(header2, body2, &signing_key).unwrap();
        let bytes2 = assertion2.to_cbor().unwrap();
        let assertion_id2 = assertion2.assertion_id().unwrap();
        let _envelope_id2 = crypto::envelope_id(&bytes2);

        let mut keys = Keyring::new();
        let err = ingest_object(&store, &mut index, &bytes2, &mut keys).unwrap_err();
        match err {
            IngestError::MissingDependency { assertion_id, missing } => {
                assert_eq!(assertion_id, assertion_id2);
                assert_eq!(missing, assertion_id1);
            }
            _ => panic!("expected missing dependency"),
        }
        assert!(index.is_pending(&assertion_id2));

        let status = ingest_object(&store, &mut index, &bytes1, &mut keys).unwrap();
        match status {
            IngestStatus::Accepted(id) => assert_eq!(id, assertion_id1),
            _ => panic!("expected accepted"),
        }

        let accepted = retry_pending(&store, &mut index, &mut keys).unwrap();
        assert_eq!(accepted, 1);
        assert!(!index.is_pending(&assertion_id2));
    }

    #[test]
    fn ingest_object_rejects_bad_signature() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(21);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let (_other_key, other_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([5u8; 32]);
        let mut subject_key = [0u8; 32];
        rng.fill_bytes(&mut subject_key);
        let bytes = make_enveloped(
            subject,
            &subject_key,
            &signing_key,
            1,
            None,
            Some(other_id),
            schema_id,
            contract_id,
        );

        let mut keys = Keyring::new();
        keys.insert_sdk(subject, 0, subject_key);
        let err = ingest_object(&store, &mut index, &bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("signature")),
            _ => panic!("expected validation error"),
        }
    }

    #[test]
    fn strict_concurrency_marks_action_pending_on_fork() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut rng = StdRng::seed_from_u64(41);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let (device_sk, device_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([8u8; 32]);
        let identity_subject = SubjectId::from_bytes([9u8; 32]);

        let (identity_schema, identity_contract) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: identity_subject,
            typ: "core.genesis".to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: identity_schema,
            contract: identity_contract,
            note: None,
            meta: add_signer_meta(None, &identity_subject),
        };
        let genesis = AssertionPlaintext::sign(genesis_header, Value::Map(vec![]), &root_sk).unwrap();
        let genesis_bytes = genesis.to_cbor().unwrap();
        let genesis_id = genesis.assertion_id().unwrap();
        let genesis_env = crypto::envelope_id(&genesis_bytes);
        append_assertion(
            store.env(),
            &identity_subject,
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
            sub: identity_subject,
            typ: "iam.delegate".to_string(),
            auth: root_id,
            seq: 2,
            prev: Some(genesis_id),
            refs: vec![genesis_id],
            ts: None,
            schema: identity_schema,
            contract: identity_contract,
            note: None,
            meta: add_signer_meta(None, &identity_subject),
        };
        let delegate_body = Value::Map(vec![
            (
                Value::Text("delegate".to_string()),
                Value::Bytes(device_id.as_bytes().to_vec()),
            ),
            (Value::Text("scope".to_string()), Value::Text("all".to_string())),
            (Value::Text("expires".to_string()), Value::Integer(0.into())),
        ]);
        let delegate = AssertionPlaintext::sign(delegate_header, delegate_body, &root_sk).unwrap();
        let delegate_bytes = delegate.to_cbor().unwrap();
        let delegate_id = delegate.assertion_id().unwrap();
        let delegate_env = crypto::envelope_id(&delegate_bytes);
        append_assertion(
            store.env(),
            &identity_subject,
            2,
            delegate_id,
            delegate_env,
            "iam.delegate",
            &delegate_bytes,
        )
        .unwrap();

        let mut fields = BTreeMap::new();
        fields.insert(
            "value".to_string(),
            crate::pdl::schema::FieldSchema {
                typ: TypeSpec::Int,
                default: Some(Value::Integer(0.into())),
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("value".to_string(), TypeSpec::Int);
        actions.insert(
            "Touch".to_string(),
            ActionSchema {
                args,
                arg_vis: BTreeMap::new(),
                doc: None,
            },
        );
        let schema = CqrsSchema {
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Demo".to_string(),
            extends: None,
            implements: Vec::new(),
            structs: BTreeMap::new(),
            fields,
            actions,
            queries: BTreeMap::new(),
        projections: BTreeMap::new(),
            concurrency: ConcurrencyMode::Strict,
        };
        let schema_bytes = schema.to_cbor().unwrap();
        let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
        let contract_bytes = simple_contract_bytes();
        let contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
        store
            .put_object(&EnvelopeId::from_bytes(*schema_id.as_bytes()), &schema_bytes)
            .unwrap();
        store
            .put_object(
                &EnvelopeId::from_bytes(*contract_id.as_bytes()),
                &contract_bytes,
            )
            .unwrap();

        let base_bytes = make_action_bytes(
            subject,
            identity_subject,
            &device_sk,
            1,
            None,
            schema_id,
            contract_id,
            0,
        );
        let base_assertion = AssertionPlaintext::from_cbor(&base_bytes).unwrap();
        let base_id = base_assertion.assertion_id().unwrap();
        let base_env = crypto::envelope_id(&base_bytes);
        store.put_assertion(&subject, &base_env, &base_bytes).unwrap();
        store.record_semantic(&base_id, &base_env).unwrap();
        append_assertion(
            store.env(),
            &subject,
            1,
            base_id,
            base_env,
            "Touch",
            &base_bytes,
        )
        .unwrap();

        let fork_a_bytes = make_action_bytes(
            subject,
            identity_subject,
            &device_sk,
            2,
            Some(base_id),
            schema_id,
            contract_id,
            1,
        );
        let fork_a = AssertionPlaintext::from_cbor(&fork_a_bytes).unwrap();
        let fork_a_id = fork_a.assertion_id().unwrap();
        let fork_a_env = crypto::envelope_id(&fork_a_bytes);
        store
            .put_assertion(&subject, &fork_a_env, &fork_a_bytes)
            .unwrap();
        store.record_semantic(&fork_a_id, &fork_a_env).unwrap();
        append_assertion(
            store.env(),
            &subject,
            2,
            fork_a_id,
            fork_a_env,
            "Touch",
            &fork_a_bytes,
        )
        .unwrap();

        let fork_b_bytes = make_action_bytes(
            subject,
            identity_subject,
            &device_sk,
            2,
            Some(base_id),
            schema_id,
            contract_id,
            2,
        );
        let fork_b = AssertionPlaintext::from_cbor(&fork_b_bytes).unwrap();
        let fork_b_id = fork_b.assertion_id().unwrap();
        let fork_b_env = crypto::envelope_id(&fork_b_bytes);
        store
            .put_assertion(&subject, &fork_b_env, &fork_b_bytes)
            .unwrap();
        store.record_semantic(&fork_b_id, &fork_b_env).unwrap();
        append_assertion(
            store.env(),
            &subject,
            2,
            fork_b_id,
            fork_b_env,
            "Touch",
            &fork_b_bytes,
        )
        .unwrap();

        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let next_bytes = make_action_bytes(
            subject,
            identity_subject,
            &device_sk,
            3,
            Some(fork_a_id),
            schema_id,
            contract_id,
            3,
        );
        let mut keys = Keyring::new();
        let status = ingest_object(&store, &mut index, &next_bytes, &mut keys).unwrap();
        match status {
            IngestStatus::Pending(_, reason) => {
                assert!(reason.contains("fork"));
            }
            other => panic!("expected pending, got {other:?}"),
        }
    }

    #[test]
    fn atlas_genesis_requires_seq1() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(60);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([60u8; 32]);
        let body = Value::Map(vec![
            (
                Value::Text("atlas_name".to_string()),
                Value::Text("person.local.alice_abcd".to_string()),
            ),
            (
                Value::Text("owner_key".to_string()),
                Value::Bytes(root_id.as_bytes().to_vec()),
            ),
        ]);
        let (bytes, _id) = make_plain_assertion(
            subject,
            ATLAS_IDENTITY_GENESIS,
            &root_sk,
            root_id,
            2,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            body,
        );
        let mut keys = Keyring::new();
        let err = ingest_object(&store, &mut index, &bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("seq=1")),
            _ => panic!("expected validation"),
        }
    }

    #[test]
    fn atlas_genesis_only_once() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(61);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([61u8; 32]);
        let body = Value::Map(vec![
            (
                Value::Text("atlas_name".to_string()),
                Value::Text("person.local.alice_abcd".to_string()),
            ),
            (
                Value::Text("owner_key".to_string()),
                Value::Bytes(root_id.as_bytes().to_vec()),
            ),
        ]);
        let (bytes, genesis_id) = make_plain_assertion(
            subject,
            ATLAS_IDENTITY_GENESIS,
            &root_sk,
            root_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            body.clone(),
        );
        let mut keys = Keyring::new();
        let status = ingest_object(&store, &mut index, &bytes, &mut keys).unwrap();
        match status {
            IngestStatus::Accepted(_) => {}
            other => panic!("expected accepted, got {other:?}"),
        }
        let (second_bytes, _second_id) = make_plain_assertion(
            subject,
            ATLAS_IDENTITY_GENESIS,
            &root_sk,
            root_id,
            2,
            Some(genesis_id),
            vec![genesis_id],
            schema_id,
            contract_id,
            body,
        );
        let err = ingest_object(&store, &mut index, &second_bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("genesis")),
            _ => panic!("expected validation"),
        }
    }

    #[test]
    fn atlas_genesis_requires_empty_subject() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(62);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([62u8; 32]);
        let note_body = Value::Map(vec![(
            Value::Text("text".to_string()),
            Value::Text("hello".to_string()),
        )]);
        let (note_bytes, _note_id) = make_plain_assertion(
            subject,
            "note.text",
            &root_sk,
            root_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            note_body,
        );
        let mut keys = Keyring::new();
        let status = ingest_object(&store, &mut index, &note_bytes, &mut keys).unwrap();
        match status {
            IngestStatus::Accepted(_) => {}
            other => panic!("expected accepted, got {other:?}"),
        }
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
        let (genesis_bytes, _genesis_id) = make_plain_assertion(
            subject,
            ATLAS_IDENTITY_GENESIS,
            &root_sk,
            root_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            genesis_body,
        );
        let err = ingest_object(&store, &mut index, &genesis_bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("genesis")),
            _ => panic!("expected validation"),
        }
    }

    #[test]
    fn atlas_lifecycle_requires_root_signature() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(63);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let (device_sk, device_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([63u8; 32]);
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
        let (genesis_bytes, genesis_id) = make_plain_assertion(
            subject,
            ATLAS_IDENTITY_GENESIS,
            &root_sk,
            root_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            genesis_body,
        );
        let mut keys = Keyring::new();
        let status = ingest_object(&store, &mut index, &genesis_bytes, &mut keys).unwrap();
        match status {
            IngestStatus::Accepted(_) => {}
            other => panic!("expected accepted, got {other:?}"),
        }
        let (suspend_bytes, _suspend_id) = make_plain_assertion(
            subject,
            ATLAS_IDENTITY_SUSPEND,
            &device_sk,
            device_id,
            2,
            Some(genesis_id),
            vec![genesis_id],
            schema_id,
            contract_id,
            Value::Map(vec![]),
        );
        let err = ingest_object(&store, &mut index, &suspend_bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("lifecycle")),
            _ => panic!("expected validation"),
        }
    }

    fn append_assertion_bytes(
        env: &dyn crate::env::Env,
        subject: SubjectId,
        seq: u64,
        typ: &str,
        bytes: &[u8],
    ) -> AssertionId {
        let assertion = AssertionPlaintext::from_cbor(bytes).unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        let envelope_id = crypto::envelope_id(bytes);
        append_assertion(env, &subject, seq, assertion_id, envelope_id, typ, bytes).unwrap();
        assertion_id
    }

    #[test]
    fn acting_context_requires_domain_membership() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(70);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let identity_subject = SubjectId::from_bytes([70u8; 32]);
        let domain_subject = SubjectId::from_bytes([71u8; 32]);
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();

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
        let (genesis_bytes, _genesis_id) = make_plain_assertion(
            identity_subject,
            ATLAS_IDENTITY_GENESIS,
            &root_sk,
            root_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            genesis_body,
        );
        append_assertion_bytes(store.env(), identity_subject, 1, ATLAS_IDENTITY_GENESIS, &genesis_bytes);

        let domain_genesis_body = Value::Map(vec![
            (Value::Text("domain".to_string()), Value::Text("corp.acme".to_string())),
            (Value::Text("owner".to_string()), Value::Bytes(root_id.as_bytes().to_vec())),
        ]);
        let (domain_genesis_bytes, domain_genesis_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.genesis",
            &root_sk,
            root_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            domain_genesis_body,
        );
        append_assertion_bytes(
            store.env(),
            domain_subject,
            1,
            "atlas.domain.genesis",
            &domain_genesis_bytes,
        );

        let approve_body = Value::Map(vec![
            (Value::Text("target".to_string()), Value::Bytes(root_id.as_bytes().to_vec())),
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
        let (approve_bytes, _approve_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.approve",
            &root_sk,
            root_id,
            2,
            Some(domain_genesis_id),
            vec![domain_genesis_id],
            schema_id,
            contract_id,
            approve_body,
        );
        append_assertion_bytes(
            store.env(),
            domain_subject,
            2,
            "atlas.domain.approve",
            &approve_bytes,
        );

        let mut fields = BTreeMap::new();
        fields.insert(
            "value".to_string(),
            crate::pdl::schema::FieldSchema {
                typ: TypeSpec::Int,
                default: Some(Value::Integer(0.into())),
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("value".to_string(), TypeSpec::Int);
        actions.insert(
            "Touch".to_string(),
            ActionSchema {
                args,
                arg_vis: BTreeMap::new(),
                doc: None,
            },
        );
        let schema = CqrsSchema {
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Demo".to_string(),
            extends: None,
            implements: Vec::new(),
            structs: BTreeMap::new(),
            fields,
            actions,
            queries: BTreeMap::new(),
        projections: BTreeMap::new(),
            concurrency: ConcurrencyMode::Allow,
        };
        let schema_bytes = schema.to_cbor().unwrap();
        let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
        let contract_bytes = simple_contract_bytes();
        let contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
        store
            .put_object(&EnvelopeId::from_bytes(*schema_id.as_bytes()), &schema_bytes)
            .unwrap();
        store
            .put_object(
                &EnvelopeId::from_bytes(*contract_id.as_bytes()),
                &contract_bytes,
            )
            .unwrap();

        let action_subject = SubjectId::from_bytes([72u8; 32]);
        let meta = Value::Map(vec![
            (
                Value::Text("acting_domain".to_string()),
                Value::Bytes(domain_subject.as_bytes().to_vec()),
            ),
            (Value::Text("acting_role".to_string()), Value::Text("member".to_string())),
        ]);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: action_subject,
            typ: "action.Touch".to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: None,
            meta: add_signer_meta(Some(meta), &identity_subject),
        };
        let body = Value::Map(vec![(Value::Text("value".to_string()), Value::Integer(1.into()))]);
        let assertion = AssertionPlaintext::sign(header, body, &root_sk).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let mut keys = Keyring::new();
        let status = ingest_object(&store, &mut index, &bytes, &mut keys).unwrap();
        match status {
            IngestStatus::Accepted(_) => {}
            other => panic!("expected accepted, got {other:?}"),
        }
    }

    #[test]
    fn acting_context_rejects_non_member() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(71);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let (domain_owner_sk, domain_owner_id) = crypto::generate_identity_keypair(&mut rng);
        let identity_subject = SubjectId::from_bytes([73u8; 32]);
        let domain_subject = SubjectId::from_bytes([74u8; 32]);
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();

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
        let (genesis_bytes, _genesis_id) = make_plain_assertion(
            identity_subject,
            ATLAS_IDENTITY_GENESIS,
            &root_sk,
            root_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            genesis_body,
        );
        append_assertion_bytes(store.env(), identity_subject, 1, ATLAS_IDENTITY_GENESIS, &genesis_bytes);

        let domain_genesis_body = Value::Map(vec![
            (Value::Text("domain".to_string()), Value::Text("corp.acme".to_string())),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(domain_owner_id.as_bytes().to_vec()),
            ),
        ]);
        let (domain_genesis_bytes, _domain_genesis_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.genesis",
            &domain_owner_sk,
            domain_owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            domain_genesis_body,
        );
        append_assertion_bytes(
            store.env(),
            domain_subject,
            1,
            "atlas.domain.genesis",
            &domain_genesis_bytes,
        );

        let mut fields = BTreeMap::new();
        fields.insert(
            "value".to_string(),
            crate::pdl::schema::FieldSchema {
                typ: TypeSpec::Int,
                default: Some(Value::Integer(0.into())),
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("value".to_string(), TypeSpec::Int);
        actions.insert(
            "Touch".to_string(),
            ActionSchema {
                args,
                arg_vis: BTreeMap::new(),
                doc: None,
            },
        );
        let schema = CqrsSchema {
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Demo".to_string(),
            extends: None,
            implements: Vec::new(),
            structs: BTreeMap::new(),
            fields,
            actions,
            queries: BTreeMap::new(),
        projections: BTreeMap::new(),
            concurrency: ConcurrencyMode::Allow,
        };
        let schema_bytes = schema.to_cbor().unwrap();
        let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
        let contract_bytes = simple_contract_bytes();
        let contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
        store
            .put_object(&EnvelopeId::from_bytes(*schema_id.as_bytes()), &schema_bytes)
            .unwrap();
        store
            .put_object(
                &EnvelopeId::from_bytes(*contract_id.as_bytes()),
                &contract_bytes,
            )
            .unwrap();

        let action_subject = SubjectId::from_bytes([75u8; 32]);
        let meta = Value::Map(vec![
            (
                Value::Text("acting_domain".to_string()),
                Value::Bytes(domain_subject.as_bytes().to_vec()),
            ),
            (Value::Text("acting_role".to_string()), Value::Text("member".to_string())),
        ]);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: action_subject,
            typ: "action.Touch".to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: None,
            meta: add_signer_meta(Some(meta), &identity_subject),
        };
        let body = Value::Map(vec![(Value::Text("value".to_string()), Value::Integer(1.into()))]);
        let assertion = AssertionPlaintext::sign(header, body, &root_sk).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let mut keys = Keyring::new();
        let err = ingest_object(&store, &mut index, &bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("domain member")),
            _ => panic!("expected validation"),
        }
    }

    #[test]
    fn summary_denies_fast_reject() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(55);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let identity_subject = SubjectId::from_bytes([41u8; 32]);
        let (identity_schema, identity_contract) = crate::builtins::ensure_note_artifacts(&store).unwrap();

        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: identity_subject,
            typ: "core.genesis".to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: identity_schema,
            contract: identity_contract,
            note: None,
            meta: add_signer_meta(None, &identity_subject),
        };
        let genesis = AssertionPlaintext::sign(genesis_header, Value::Map(vec![]), &root_sk).unwrap();
        let genesis_bytes = genesis.to_cbor().unwrap();
        let genesis_id = genesis.assertion_id().unwrap();
        let genesis_env = crypto::envelope_id(&genesis_bytes);
        append_assertion(
            store.env(),
            &identity_subject,
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
            sub: identity_subject,
            typ: "identity.profile".to_string(),
            auth: root_id,
            seq: 2,
            prev: Some(genesis_id),
            refs: vec![],
            ts: None,
            schema: identity_schema,
            contract: identity_contract,
            note: None,
            meta: add_signer_meta(None, &identity_subject),
        };
        let profile_body = Value::Map(vec![
            (Value::Text("alias".to_string()), Value::Text("tester".to_string())),
            (
                Value::Text("roles".to_string()),
                Value::Array(vec![Value::Text("finance.approver".to_string())]),
            ),
        ]);
        let profile = AssertionPlaintext::sign(profile_header, profile_body, &root_sk).unwrap();
        let profile_bytes = profile.to_cbor().unwrap();
        let profile_id = profile.assertion_id().unwrap();
        let profile_env = crypto::envelope_id(&profile_bytes);
        append_assertion(
            store.env(),
            &identity_subject,
            2,
            profile_id,
            profile_env,
            "identity.profile",
            &profile_bytes,
        )
        .unwrap();

        let mut fields = BTreeMap::new();
        fields.insert(
            "value".to_string(),
            FieldSchema {
                typ: TypeSpec::Int,
                default: Some(Value::Integer(0.into())),
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("value".to_string(), TypeSpec::Int);
        actions.insert(
            "Touch".to_string(),
            ActionSchema {
                args,
                arg_vis: BTreeMap::new(),
                doc: None,
            },
        );
        let schema = CqrsSchema {
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Demo".to_string(),
            extends: None,
            implements: Vec::new(),
            structs: BTreeMap::new(),
            fields,
            actions,
            queries: BTreeMap::new(),
        projections: BTreeMap::new(),
            concurrency: ConcurrencyMode::Allow,
        };
        let schema_bytes = schema.to_cbor().unwrap();
        let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
        let contract_bytes = simple_contract_bytes();
        let contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
        store
            .put_object(&EnvelopeId::from_bytes(*schema_id.as_bytes()), &schema_bytes)
            .unwrap();
        store
            .put_object(
                &EnvelopeId::from_bytes(*contract_id.as_bytes()),
                &contract_bytes,
            )
            .unwrap();

        let mut rule_roles = BTreeSet::new();
        rule_roles.insert("finance.viewer".to_string());
        let mut summary_actions = BTreeMap::new();
        summary_actions.insert(
            "Touch".to_string(),
            PermissionRule {
                roles: rule_roles,
                exhaustive: true,
            },
        );
        let summary = PermissionSummary {
            v: 1,
            contract: contract_id,
            ver: DEFAULT_DATA_VERSION,
            actions: summary_actions,
            queries: BTreeMap::new(),
            role_scopes: BTreeMap::new(),
            public: PublicPermissions::default(),
        };
        store.put_permission_summary(&summary).unwrap();

        let action_subject = SubjectId::from_bytes([42u8; 32]);
        let bytes = make_action_bytes(
            action_subject,
            identity_subject,
            &root_sk,
            1,
            None,
            schema_id,
            contract_id,
            5,
        );
        let mut keys = Keyring::new();
        let err = ingest_object(&store, &mut index, &bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("summary denied")),
            other => panic!("expected summary deny, got {other:?}"),
        }
    }

    #[test]
    fn summary_allows_but_contract_denies() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(56);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let identity_subject = SubjectId::from_bytes([44u8; 32]);
        let (identity_schema, identity_contract) = crate::builtins::ensure_note_artifacts(&store).unwrap();

        let genesis_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: identity_subject,
            typ: "core.genesis".to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: identity_schema,
            contract: identity_contract,
            note: None,
            meta: add_signer_meta(None, &identity_subject),
        };
        let genesis = AssertionPlaintext::sign(genesis_header, Value::Map(vec![]), &root_sk).unwrap();
        let genesis_bytes = genesis.to_cbor().unwrap();
        let genesis_id = genesis.assertion_id().unwrap();
        let genesis_env = crypto::envelope_id(&genesis_bytes);
        append_assertion(
            store.env(),
            &identity_subject,
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
            sub: identity_subject,
            typ: "identity.profile".to_string(),
            auth: root_id,
            seq: 2,
            prev: Some(genesis_id),
            refs: vec![],
            ts: None,
            schema: identity_schema,
            contract: identity_contract,
            note: None,
            meta: add_signer_meta(None, &identity_subject),
        };
        let profile_body = Value::Map(vec![
            (Value::Text("alias".to_string()), Value::Text("tester".to_string())),
            (
                Value::Text("roles".to_string()),
                Value::Array(vec![Value::Text("finance.approver".to_string())]),
            ),
        ]);
        let profile = AssertionPlaintext::sign(profile_header, profile_body, &root_sk).unwrap();
        let profile_bytes = profile.to_cbor().unwrap();
        let profile_id = profile.assertion_id().unwrap();
        let profile_env = crypto::envelope_id(&profile_bytes);
        append_assertion(
            store.env(),
            &identity_subject,
            2,
            profile_id,
            profile_env,
            "identity.profile",
            &profile_bytes,
        )
        .unwrap();

        let mut fields = BTreeMap::new();
        fields.insert(
            "value".to_string(),
            FieldSchema {
                typ: TypeSpec::Int,
                default: Some(Value::Integer(0.into())),
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("value".to_string(), TypeSpec::Int);
        actions.insert(
            "Touch".to_string(),
            ActionSchema {
                args,
                arg_vis: BTreeMap::new(),
                doc: None,
            },
        );
        let schema = CqrsSchema {
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Demo".to_string(),
            extends: None,
            implements: Vec::new(),
            structs: BTreeMap::new(),
            fields,
            actions,
            queries: BTreeMap::new(),
        projections: BTreeMap::new(),
            concurrency: ConcurrencyMode::Allow,
        };
        let schema_bytes = schema.to_cbor().unwrap();
        let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
        let contract_bytes = reject_contract_bytes();
        let contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
        store
            .put_object(&EnvelopeId::from_bytes(*schema_id.as_bytes()), &schema_bytes)
            .unwrap();
        store
            .put_object(
                &EnvelopeId::from_bytes(*contract_id.as_bytes()),
                &contract_bytes,
            )
            .unwrap();

        let mut rule_roles = BTreeSet::new();
        rule_roles.insert("finance.approver".to_string());
        let mut summary_actions = BTreeMap::new();
        summary_actions.insert(
            "Touch".to_string(),
            PermissionRule {
                roles: rule_roles,
                exhaustive: true,
            },
        );
        let summary = PermissionSummary {
            v: 1,
            contract: contract_id,
            ver: DEFAULT_DATA_VERSION,
            actions: summary_actions,
            queries: BTreeMap::new(),
            role_scopes: BTreeMap::new(),
            public: PublicPermissions::default(),
        };
        store.put_permission_summary(&summary).unwrap();

        let action_subject = SubjectId::from_bytes([45u8; 32]);
        let bytes = make_action_bytes(
            action_subject,
            identity_subject,
            &root_sk,
            1,
            None,
            schema_id,
            contract_id,
            5,
        );
        let mut keys = Keyring::new();
        let err = ingest_object(&store, &mut index, &bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("contract rejected")),
            other => panic!("expected contract rejection, got {other:?}"),
        }
    }

    #[test]
    fn local_handle_never_in_assertions() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(64);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([64u8; 32]);
        let body = Value::Map(vec![
            (
                Value::Text("local_handle".to_string()),
                Value::Text("alice".to_string()),
            ),
            (
                Value::Text("note".to_string()),
                Value::Text("should not sync".to_string()),
            ),
        ]);
        let (bytes, _id) = make_plain_assertion(
            subject,
            "core.genesis",
            &root_sk,
            root_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            body,
        );
        let mut keys = Keyring::new();
        let err = ingest_object(&store, &mut index, &bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("local_handle")),
            _ => panic!("expected validation"),
        }
    }

    #[test]
    fn rotation_does_not_rewrite_data() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(70);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let domain_subject = SubjectId::from_bytes([70u8; 32]);

        let domain_genesis_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text("corp".to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(owner_id.as_bytes().to_vec()),
            ),
        ]);
        let (domain_genesis_bytes, domain_genesis_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.genesis",
            &owner_sk,
            owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            domain_genesis_body,
        );

        let mut keys = Keyring::new();
        ingest_object(&store, &mut index, &domain_genesis_bytes, &mut keys).unwrap();

        let subject = SubjectId::from_bytes([71u8; 32]);
        let mut subject_key = [0u8; 32];
        rng.fill_bytes(&mut subject_key);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![(
            Value::Text("text".to_string()),
            Value::Text("hello".to_string()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &signing_key).unwrap();
        let plaintext = assertion.to_cbor().unwrap();
        let kid = crypto::key_id_from_key(&subject_key);
        let envelope = envelope::encrypt_assertion_with_epoch(
            &plaintext,
            kid,
            &subject_key,
            Nonce12::from_bytes([1u8; 12]),
            0,
        )
        .unwrap();
        let bytes = envelope.to_cbor().unwrap();
        keys.insert_sdk(subject, 0, subject_key);
        ingest_object(&store, &mut index, &bytes, &mut keys).unwrap();
        let envelope_id = envelope.envelope_id().unwrap();
        let before = store.get_object(&envelope_id).unwrap();

        let kek_key = [5u8; 32];
        let kek_id = key_id_for_key(&kek_key);
        let rotate_body = Value::Map(vec![
            (Value::Text("epoch".to_string()), Value::Integer(1.into())),
            (
                Value::Text("kek_id".to_string()),
                Value::Bytes(kek_id.as_bytes().to_vec()),
            ),
        ]);
        let (rotate_bytes, _rotate_id) = make_plain_assertion(
            domain_subject,
            "domain.key.rotate",
            &owner_sk,
            owner_id,
            2,
            Some(domain_genesis_id),
            Vec::new(),
            schema_id,
            contract_id,
            rotate_body,
        );
        ingest_object(&store, &mut index, &rotate_bytes, &mut keys).unwrap();

        let after = store.get_object(&envelope_id).unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn new_epoch_used_for_new_facts() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(71);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let domain_subject = SubjectId::from_bytes([72u8; 32]);

        let domain_genesis_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text("corp".to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(owner_id.as_bytes().to_vec()),
            ),
        ]);
        let (domain_genesis_bytes, domain_genesis_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.genesis",
            &owner_sk,
            owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            domain_genesis_body,
        );
        let mut keys = Keyring::new();
        ingest_object(&store, &mut index, &domain_genesis_bytes, &mut keys).unwrap();

        let kek_key = [7u8; 32];
        let kek_id = key_id_for_key(&kek_key);
        let rotate_body = Value::Map(vec![
            (Value::Text("epoch".to_string()), Value::Integer(1.into())),
            (
                Value::Text("kek_id".to_string()),
                Value::Bytes(kek_id.as_bytes().to_vec()),
            ),
        ]);
        let (rotate_bytes, _rotate_id) = make_plain_assertion(
            domain_subject,
            "domain.key.rotate",
            &owner_sk,
            owner_id,
            2,
            Some(domain_genesis_id),
            Vec::new(),
            schema_id,
            contract_id,
            rotate_body,
        );
        ingest_object(&store, &mut index, &rotate_bytes, &mut keys).unwrap();

        let subject = SubjectId::from_bytes([73u8; 32]);
        let sdk = [9u8; 32];
        let sdk_id = key_id_for_key(&sdk);
        let bind_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Bytes(domain_subject.as_bytes().to_vec()),
            ),
            (Value::Text("epoch".to_string()), Value::Integer(1.into())),
            (
                Value::Text("sdk_id".to_string()),
                Value::Bytes(sdk_id.as_bytes().to_vec()),
            ),
        ]);
        let (bind_bytes, bind_id) = make_plain_assertion(
            subject,
            "subject.key.bind",
            &owner_sk,
            owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            bind_body,
        );
        ingest_object(&store, &mut index, &bind_bytes, &mut keys).unwrap();

        let epoch = load_epoch(store.env(), &subject).unwrap().unwrap();
        assert_eq!(epoch, 1);

        let mut subject_key = [0u8; 32];
        rng.fill_bytes(&mut subject_key);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 2,
            prev: Some(bind_id),
            refs: Vec::new(),
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![(
            Value::Text("text".to_string()),
            Value::Text("next".to_string()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &signing_key).unwrap();
        let plaintext = assertion.to_cbor().unwrap();
        let kid = crypto::key_id_from_key(&subject_key);
        let envelope = envelope::encrypt_assertion_with_epoch(
            &plaintext,
            kid,
            &subject_key,
            Nonce12::from_bytes([2u8; 12]),
            epoch,
        )
        .unwrap();
        assert_eq!(envelope.epoch, Some(1));
        let decoded = envelope::AssertionEnvelope::from_cbor(&envelope.to_cbor().unwrap()).unwrap();
        assert_eq!(decoded.epoch, Some(1));
    }

    #[test]
    fn active_member_receives_key_grant() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(72);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (_member_sk, member_id) = crypto::generate_identity_keypair(&mut rng);
        let mut member_secret = [0u8; 32];
        rng.fill_bytes(&mut member_secret);
        let member_hpke_pk = hpke_public_key_from_secret(&member_secret);
        let domain_subject = SubjectId::from_bytes([74u8; 32]);

        let domain_genesis_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text("corp".to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(owner_id.as_bytes().to_vec()),
            ),
        ]);
        let (domain_genesis_bytes, domain_genesis_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.genesis",
            &owner_sk,
            owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            domain_genesis_body,
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
                Value::Array(vec![Value::Text("all".to_string())]),
            ),
        ]);
        let (approve_bytes, approve_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.approve",
            &owner_sk,
            owner_id,
            2,
            Some(domain_genesis_id),
            Vec::new(),
            schema_id,
            contract_id,
            approve_body,
        );

        let subject = SubjectId::from_bytes([75u8; 32]);
        let mut sdk = [0u8; 32];
        rng.fill_bytes(&mut sdk);
        let sdk_id = key_id_for_key(&sdk);
        let bind_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Bytes(domain_subject.as_bytes().to_vec()),
            ),
            (Value::Text("epoch".to_string()), Value::Integer(0.into())),
            (
                Value::Text("sdk_id".to_string()),
                Value::Bytes(sdk_id.as_bytes().to_vec()),
            ),
        ]);
        let (bind_bytes, _bind_id) = make_plain_assertion(
            subject,
            "subject.key.bind",
            &owner_sk,
            owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            bind_body,
        );

        let sdk_envelope = hpke_seal(&member_hpke_pk, &sdk).unwrap();
        let sdk_bytes = sdk_envelope.to_cbor().unwrap();
        let grant_body = Value::Map(vec![
            (
                Value::Text("member".to_string()),
                Value::Bytes(member_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("subject".to_string()),
                Value::Bytes(subject.as_bytes().to_vec()),
            ),
            (Value::Text("epoch".to_string()), Value::Integer(0.into())),
            (
                Value::Text("sdk_id".to_string()),
                Value::Bytes(sdk_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("sdk".to_string()),
                Value::Bytes(sdk_bytes),
            ),
        ]);
        let (grant_bytes, _grant_id) = make_plain_assertion(
            domain_subject,
            "member.key.grant",
            &owner_sk,
            owner_id,
            3,
            Some(approve_id),
            Vec::new(),
            schema_id,
            contract_id,
            grant_body,
        );

        let mut keys = Keyring::new();
        keys.insert_hpke_secret(member_id, member_secret);
        ingest_object(&store, &mut index, &domain_genesis_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &approve_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &bind_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &grant_bytes, &mut keys).unwrap();

        let found = keys.sdk_for_subject_epoch(&subject, 0).unwrap();
        assert_eq!(*found, sdk);
    }

    #[test]
    fn revoked_identity_cannot_decrypt_new_epoch() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(73);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (_member_sk, member_id) = crypto::generate_identity_keypair(&mut rng);
        let mut member_secret = [0u8; 32];
        rng.fill_bytes(&mut member_secret);
        let member_hpke_pk = hpke_public_key_from_secret(&member_secret);
        let domain_subject = SubjectId::from_bytes([76u8; 32]);

        let domain_genesis_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text("corp".to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(owner_id.as_bytes().to_vec()),
            ),
        ]);
        let (domain_genesis_bytes, domain_genesis_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.genesis",
            &owner_sk,
            owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            domain_genesis_body,
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
                Value::Array(vec![Value::Text("all".to_string())]),
            ),
        ]);
        let (approve_bytes, approve_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.approve",
            &owner_sk,
            owner_id,
            2,
            Some(domain_genesis_id),
            Vec::new(),
            schema_id,
            contract_id,
            approve_body,
        );

        let subject = SubjectId::from_bytes([77u8; 32]);
        let mut sdk0 = [0u8; 32];
        rng.fill_bytes(&mut sdk0);
        let sdk0_id = key_id_for_key(&sdk0);
        let bind0_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Bytes(domain_subject.as_bytes().to_vec()),
            ),
            (Value::Text("epoch".to_string()), Value::Integer(0.into())),
            (
                Value::Text("sdk_id".to_string()),
                Value::Bytes(sdk0_id.as_bytes().to_vec()),
            ),
        ]);
        let (bind0_bytes, bind0_id) = make_plain_assertion(
            subject,
            "subject.key.bind",
            &owner_sk,
            owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            bind0_body,
        );

        let sdk0_envelope = hpke_seal(&member_hpke_pk, &sdk0).unwrap();
        let sdk0_bytes = sdk0_envelope.to_cbor().unwrap();
        let grant0_body = Value::Map(vec![
            (
                Value::Text("member".to_string()),
                Value::Bytes(member_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("subject".to_string()),
                Value::Bytes(subject.as_bytes().to_vec()),
            ),
            (Value::Text("epoch".to_string()), Value::Integer(0.into())),
            (
                Value::Text("sdk_id".to_string()),
                Value::Bytes(sdk0_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("sdk".to_string()),
                Value::Bytes(sdk0_bytes),
            ),
        ]);
        let (grant0_bytes, grant0_id) = make_plain_assertion(
            domain_subject,
            "member.key.grant",
            &owner_sk,
            owner_id,
            3,
            Some(approve_id),
            Vec::new(),
            schema_id,
            contract_id,
            grant0_body,
        );

        let revoke_body = Value::Map(vec![
            (
                Value::Text("target".to_string()),
                Value::Bytes(member_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("reason".to_string()),
                Value::Text("rotating".to_string()),
            ),
        ]);
        let (revoke_bytes, revoke_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.revoke",
            &owner_sk,
            owner_id,
            4,
            Some(grant0_id),
            Vec::new(),
            schema_id,
            contract_id,
            revoke_body,
        );

        let kek_key = [10u8; 32];
        let kek_id = key_id_for_key(&kek_key);
        let rotate_body = Value::Map(vec![
            (Value::Text("epoch".to_string()), Value::Integer(1.into())),
            (
                Value::Text("kek_id".to_string()),
                Value::Bytes(kek_id.as_bytes().to_vec()),
            ),
        ]);
        let (rotate_bytes, rotate_id) = make_plain_assertion(
            domain_subject,
            "domain.key.rotate",
            &owner_sk,
            owner_id,
            5,
            Some(revoke_id),
            Vec::new(),
            schema_id,
            contract_id,
            rotate_body,
        );

        let mut sdk1 = [0u8; 32];
        rng.fill_bytes(&mut sdk1);
        let sdk1_id = key_id_for_key(&sdk1);
        let bind1_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Bytes(domain_subject.as_bytes().to_vec()),
            ),
            (Value::Text("epoch".to_string()), Value::Integer(1.into())),
            (
                Value::Text("sdk_id".to_string()),
                Value::Bytes(sdk1_id.as_bytes().to_vec()),
            ),
        ]);
        let (bind1_bytes, _bind1_id) = make_plain_assertion(
            subject,
            "subject.key.bind",
            &owner_sk,
            owner_id,
            2,
            Some(bind0_id),
            Vec::new(),
            schema_id,
            contract_id,
            bind1_body,
        );

        let sdk1_envelope = hpke_seal(&member_hpke_pk, &sdk1).unwrap();
        let sdk1_bytes = sdk1_envelope.to_cbor().unwrap();
        let grant1_body = Value::Map(vec![
            (
                Value::Text("member".to_string()),
                Value::Bytes(member_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("subject".to_string()),
                Value::Bytes(subject.as_bytes().to_vec()),
            ),
            (Value::Text("epoch".to_string()), Value::Integer(1.into())),
            (
                Value::Text("sdk_id".to_string()),
                Value::Bytes(sdk1_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("sdk".to_string()),
                Value::Bytes(sdk1_bytes),
            ),
        ]);
        let (grant1_bytes, _grant1_id) = make_plain_assertion(
            domain_subject,
            "member.key.grant",
            &owner_sk,
            owner_id,
            6,
            Some(rotate_id),
            Vec::new(),
            schema_id,
            contract_id,
            grant1_body,
        );

        let mut keys = Keyring::new();
        keys.insert_hpke_secret(member_id, member_secret);
        ingest_object(&store, &mut index, &domain_genesis_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &approve_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &bind0_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &grant0_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &revoke_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &rotate_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &bind1_bytes, &mut keys).unwrap();
        let err = ingest_object(&store, &mut index, &grant1_bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("member")),
            _ => panic!("expected validation"),
        }

        assert!(keys.sdk_for_subject_epoch(&subject, 1).is_none());
        assert!(keys.sdk_for_subject_epoch(&subject, 0).is_some());
    }

    #[test]
    fn revoked_identity_can_read_old_epoch() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(74);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (_member_sk, member_id) = crypto::generate_identity_keypair(&mut rng);
        let mut member_secret = [0u8; 32];
        rng.fill_bytes(&mut member_secret);
        let member_hpke_pk = hpke_public_key_from_secret(&member_secret);
        let domain_subject = SubjectId::from_bytes([78u8; 32]);

        let domain_genesis_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text("corp".to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(owner_id.as_bytes().to_vec()),
            ),
        ]);
        let (domain_genesis_bytes, domain_genesis_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.genesis",
            &owner_sk,
            owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            domain_genesis_body,
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
                Value::Array(vec![Value::Text("all".to_string())]),
            ),
        ]);
        let (approve_bytes, approve_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.approve",
            &owner_sk,
            owner_id,
            2,
            Some(domain_genesis_id),
            Vec::new(),
            schema_id,
            contract_id,
            approve_body,
        );

        let subject = SubjectId::from_bytes([79u8; 32]);
        let mut sdk0 = [0u8; 32];
        rng.fill_bytes(&mut sdk0);
        let sdk0_id = key_id_for_key(&sdk0);
        let bind0_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Bytes(domain_subject.as_bytes().to_vec()),
            ),
            (Value::Text("epoch".to_string()), Value::Integer(0.into())),
            (
                Value::Text("sdk_id".to_string()),
                Value::Bytes(sdk0_id.as_bytes().to_vec()),
            ),
        ]);
        let (bind0_bytes, bind0_id) = make_plain_assertion(
            subject,
            "subject.key.bind",
            &owner_sk,
            owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            bind0_body,
        );

        let sdk0_envelope = hpke_seal(&member_hpke_pk, &sdk0).unwrap();
        let sdk0_bytes = sdk0_envelope.to_cbor().unwrap();
        let grant0_body = Value::Map(vec![
            (
                Value::Text("member".to_string()),
                Value::Bytes(member_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("subject".to_string()),
                Value::Bytes(subject.as_bytes().to_vec()),
            ),
            (Value::Text("epoch".to_string()), Value::Integer(0.into())),
            (
                Value::Text("sdk_id".to_string()),
                Value::Bytes(sdk0_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("sdk".to_string()),
                Value::Bytes(sdk0_bytes),
            ),
        ]);
        let (grant0_bytes, _grant0_id) = make_plain_assertion(
            domain_subject,
            "member.key.grant",
            &owner_sk,
            owner_id,
            3,
            Some(approve_id),
            Vec::new(),
            schema_id,
            contract_id,
            grant0_body,
        );

        let mut keys = Keyring::new();
        keys.insert_hpke_secret(member_id, member_secret);
        ingest_object(&store, &mut index, &domain_genesis_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &approve_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &bind0_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &grant0_bytes, &mut keys).unwrap();

        let sdk = *keys.sdk_for_subject_epoch(&subject, 0).unwrap();
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 2,
            prev: Some(bind0_id),
            refs: Vec::new(),
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![(
            Value::Text("text".to_string()),
            Value::Text("old".to_string()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &signing_key).unwrap();
        let plaintext = assertion.to_cbor().unwrap();
        let kid = crypto::key_id_from_key(&sdk);
        let envelope = envelope::encrypt_assertion_with_epoch(
            &plaintext,
            kid,
            &sdk,
            Nonce12::from_bytes([9u8; 12]),
            0,
        )
        .unwrap();
        let decrypted = envelope::decrypt_assertion(&envelope, &sdk).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn freeze_blocks_new_facts() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(80);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let domain_subject = SubjectId::from_bytes([80u8; 32]);

        let genesis_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text("corp".to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(owner_id.as_bytes().to_vec()),
            ),
        ]);
        let (genesis_bytes, genesis_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.genesis",
            &owner_sk,
            owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            genesis_body,
        );

        let freeze_body = Value::Map(vec![(
            Value::Text("reason".to_string()),
            Value::Text("incident".to_string()),
        )]);
        let (freeze_bytes, freeze_id) = make_plain_assertion(
            domain_subject,
            "domain.freeze",
            &owner_sk,
            owner_id,
            2,
            Some(genesis_id),
            Vec::new(),
            schema_id,
            contract_id,
            freeze_body,
        );

        let invite_body = Value::Map(vec![
            (
                Value::Text("target".to_string()),
                Value::Bytes([9u8; 32].to_vec()),
            ),
            (
                Value::Text("roles".to_string()),
                Value::Array(vec![Value::Text("member".to_string())]),
            ),
            (
                Value::Text("scopes".to_string()),
                Value::Array(vec![Value::Text("all".to_string())]),
            ),
        ]);
        let (invite_bytes, _invite_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.invite",
            &owner_sk,
            owner_id,
            3,
            Some(freeze_id),
            Vec::new(),
            schema_id,
            contract_id,
            invite_body,
        );

        let mut keys = Keyring::new();
        ingest_object(&store, &mut index, &genesis_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &freeze_bytes, &mut keys).unwrap();
        let err = ingest_object(&store, &mut index, &invite_bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("frozen")),
            _ => panic!("expected frozen"),
        }
    }

    #[test]
    fn freeze_allows_read() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(81);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let domain_subject = SubjectId::from_bytes([81u8; 32]);

        let genesis_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text("corp".to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(owner_id.as_bytes().to_vec()),
            ),
        ]);
        let (genesis_bytes, genesis_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.genesis",
            &owner_sk,
            owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            genesis_body,
        );

        let freeze_body = Value::Map(vec![]);
        let (freeze_bytes, _freeze_id) = make_plain_assertion(
            domain_subject,
            "domain.freeze",
            &owner_sk,
            owner_id,
            2,
            Some(genesis_id),
            Vec::new(),
            schema_id,
            contract_id,
            freeze_body,
        );

        let mut keys = Keyring::new();
        ingest_object(&store, &mut index, &genesis_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &freeze_bytes, &mut keys).unwrap();
        let records = crate::store::state::list_assertions(store.env(), &domain_subject).unwrap();
        assert!(!records.is_empty());
    }

    #[test]
    fn domain_compromised_terminal() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut rng = StdRng::seed_from_u64(82);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let domain_subject = SubjectId::from_bytes([82u8; 32]);

        let genesis_body = Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text("corp".to_string()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(owner_id.as_bytes().to_vec()),
            ),
        ]);
        let (genesis_bytes, genesis_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.genesis",
            &owner_sk,
            owner_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            genesis_body,
        );

        let compromised_body = Value::Map(vec![(
            Value::Text("reason".to_string()),
            Value::Text("breach".to_string()),
        )]);
        let (compromised_bytes, compromised_id) = make_plain_assertion(
            domain_subject,
            "domain.compromised",
            &owner_sk,
            owner_id,
            2,
            Some(genesis_id),
            Vec::new(),
            schema_id,
            contract_id,
            compromised_body,
        );

        let invite_body = Value::Map(vec![
            (
                Value::Text("target".to_string()),
                Value::Bytes([8u8; 32].to_vec()),
            ),
            (
                Value::Text("roles".to_string()),
                Value::Array(vec![Value::Text("member".to_string())]),
            ),
            (
                Value::Text("scopes".to_string()),
                Value::Array(vec![Value::Text("all".to_string())]),
            ),
        ]);
        let (invite_bytes, _invite_id) = make_plain_assertion(
            domain_subject,
            "atlas.domain.invite",
            &owner_sk,
            owner_id,
            3,
            Some(compromised_id),
            Vec::new(),
            schema_id,
            contract_id,
            invite_body,
        );

        let mut keys = Keyring::new();
        ingest_object(&store, &mut index, &genesis_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &compromised_bytes, &mut keys).unwrap();
        let err = ingest_object(&store, &mut index, &invite_bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("compromised")),
            _ => panic!("expected compromised"),
        }
    }

    #[test]
    fn device_revoke_blocks_signer() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let (schema_id, contract_id) = crate::builtins::ensure_note_artifacts(&store).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert(
            "value".to_string(),
            crate::pdl::schema::FieldSchema {
                typ: TypeSpec::Int,
                default: Some(Value::Integer(0.into())),
                visibility: Visibility::Public,
            },
        );
        let mut actions = BTreeMap::new();
        let mut args = BTreeMap::new();
        args.insert("value".to_string(), TypeSpec::Int);
        actions.insert(
            "Touch".to_string(),
            ActionSchema {
                args,
                arg_vis: BTreeMap::new(),
                doc: None,
            },
        );
        let schema = CqrsSchema {
            namespace: "test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Demo".to_string(),
            extends: None,
            implements: Vec::new(),
            structs: BTreeMap::new(),
            fields,
            actions,
            queries: BTreeMap::new(),
        projections: BTreeMap::new(),
            concurrency: ConcurrencyMode::Allow,
        };
        let schema_bytes = schema.to_cbor().unwrap();
        let action_schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
        let contract_bytes = simple_contract_bytes();
        let action_contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
        store
            .put_object(&EnvelopeId::from_bytes(*action_schema_id.as_bytes()), &schema_bytes)
            .unwrap();
        store
            .put_object(
                &EnvelopeId::from_bytes(*action_contract_id.as_bytes()),
                &contract_bytes,
            )
            .unwrap();
        let mut rng = StdRng::seed_from_u64(83);
        let (root_sk, root_id) = crypto::generate_identity_keypair(&mut rng);
        let (device_sk, device_id) = crypto::generate_identity_keypair(&mut rng);
        let identity_subject = SubjectId::from_bytes([83u8; 32]);

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
        let (genesis_bytes, genesis_id) = make_plain_assertion(
            identity_subject,
            ATLAS_IDENTITY_GENESIS,
            &root_sk,
            root_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            genesis_body,
        );

        let delegate_body = Value::Map(vec![
            (
                Value::Text("delegate".to_string()),
                Value::Bytes(device_id.as_bytes().to_vec()),
            ),
            (Value::Text("scope".to_string()), Value::Text("all".to_string())),
            (Value::Text("expires".to_string()), Value::Integer(0.into())),
        ]);
        let (delegate_bytes, delegate_id) = make_plain_assertion(
            identity_subject,
            "iam.delegate",
            &root_sk,
            root_id,
            2,
            Some(genesis_id),
            Vec::new(),
            schema_id,
            contract_id,
            delegate_body,
        );

        let revoke_body = Value::Map(vec![(
            Value::Text("delegate".to_string()),
            Value::Bytes(device_id.as_bytes().to_vec()),
        )]);
        let (revoke_bytes, _revoke_id) = make_plain_assertion(
            identity_subject,
            "iam.delegate.revoke",
            &root_sk,
            root_id,
            3,
            Some(delegate_id),
            Vec::new(),
            schema_id,
            contract_id,
            revoke_body,
        );

        let action_subject = SubjectId::from_bytes([84u8; 32]);
        let action_bytes = make_action_bytes(
            action_subject,
            identity_subject,
            &device_sk,
            1,
            None,
            action_schema_id,
            action_contract_id,
            1,
        );

        let mut keys = Keyring::new();
        ingest_object(&store, &mut index, &genesis_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &delegate_bytes, &mut keys).unwrap();
        ingest_object(&store, &mut index, &revoke_bytes, &mut keys).unwrap();

        let err = ingest_object(&store, &mut index, &action_bytes, &mut keys).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("device revoked")),
            other => panic!("expected revoked, got {other:?}"),
        }
    }

    fn vault_args(subject: SubjectId, start_seq: i64, end_seq: i64) -> Value {
        let vault = Value::Map(vec![
            (Value::Text("driver".to_string()), Value::Text("Local".to_string())),
            (
                Value::Text("location".to_string()),
                Value::Text("local/vault/chunk.dhbox".to_string()),
            ),
            (Value::Text("hash".to_string()), Value::Bytes(vec![9u8; 32])),
            (Value::Text("size".to_string()), Value::Integer(1024.into())),
            (
                Value::Text("compression".to_string()),
                Value::Text("Zstd_19".to_string()),
            ),
            (
                Value::Text("encryption".to_string()),
                Value::Text("XChaCha20_Poly1305".to_string()),
            ),
            (
                Value::Text("format_version".to_string()),
                Value::Integer(1.into()),
            ),
            (
                Value::Text("subject".to_string()),
                Value::Bytes(subject.as_bytes().to_vec()),
            ),
            (
                Value::Text("seq_start".to_string()),
                Value::Integer(start_seq.into()),
            ),
            (Value::Text("seq_end".to_string()), Value::Integer(end_seq.into())),
            (
                Value::Text("snapshot_hash".to_string()),
                Value::Bytes(vec![7u8; 32]),
            ),
            (
                Value::Text("merkle_root".to_string()),
                Value::Bytes(vec![8u8; 32]),
            ),
            (Value::Text("dict_hash".to_string()), Value::Null),
            (Value::Text("dict_size".to_string()), Value::Null),
            (Value::Text("shards".to_string()), Value::Null),
        ]);
        Value::Map(vec![
            (
                Value::Text("start_seq".to_string()),
                Value::Integer(start_seq.into()),
            ),
            (
                Value::Text("end_seq".to_string()),
                Value::Integer(end_seq.into()),
            ),
            (
                Value::Text("state_root".to_string()),
                Value::Bytes(vec![6u8; 32]),
            ),
            (Value::Text("vault".to_string()), vault),
        ])
    }

    #[test]
    fn vault_checkpoint_rejects_subject_mismatch() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let subject = SubjectId::from_bytes([1u8; 32]);
        let schema_id = SchemaId::from_bytes([2u8; 32]);

        let other_subject = SubjectId::from_bytes([3u8; 32]);
        let args = vault_args(other_subject, 1, 2);
        let err = enforce_sys_vault_checkpoint(&store, &subject, &schema_id, &args).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("vault subject mismatch")),
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[test]
    fn vault_checkpoint_rejects_regression() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let subject = SubjectId::from_bytes([4u8; 32]);
        let schema_id = SchemaId::from_bytes([5u8; 32]);
        let contract_id = ContractId::from_bytes([6u8; 32]);

        let mut rng = StdRng::seed_from_u64(9);
        let (signing_key, signer_id) = crypto::generate_identity_keypair(&mut rng);
        let prev_body = vault_args(subject, 1, 10);
        let (prev_bytes, prev_assertion_id) = make_plain_assertion(
            subject,
            "action.Checkpoint",
            &signing_key,
            signer_id,
            1,
            None,
            Vec::new(),
            schema_id,
            contract_id,
            prev_body,
        );
        let prev_envelope_id = crypto::envelope_id(&prev_bytes);
        append_assertion(
            store.env(),
            &subject,
            1,
            prev_assertion_id,
            prev_envelope_id,
            "Checkpoint",
            &prev_bytes,
        )
        .unwrap();

        let args = vault_args(subject, 5, 6);
        let err = enforce_sys_vault_checkpoint(&store, &subject, &schema_id, &args).unwrap_err();
        match err {
            IngestError::Validation(reason) => assert!(reason.contains("checkpoint regressed")),
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[test]
    fn vault_checkpoint_rejects_bad_format_version() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let subject = SubjectId::from_bytes([7u8; 32]);
        let schema_id = SchemaId::from_bytes([8u8; 32]);

        let mut args = vault_args(subject, 1, 2);
        let args_map = match args {
            Value::Map(ref mut entries) => entries,
            _ => unreachable!(),
        };
        for (key, value) in args_map.iter_mut() {
            if matches!(key, Value::Text(name) if name == "vault") {
                if let Value::Map(vault_entries) = value {
                    for (vk, vv) in vault_entries.iter_mut() {
                        if matches!(vk, Value::Text(name) if name == "format_version") {
                            *vv = Value::Integer(99.into());
                        }
                    }
                }
            }
        }

        let err = enforce_sys_vault_checkpoint(&store, &subject, &schema_id, &args).unwrap_err();
        match err {
            IngestError::Validation(reason) => {
                assert!(reason.contains("unsupported vault format version"))
            }
            other => panic!("expected validation error, got {other:?}"),
        }
    }
}
