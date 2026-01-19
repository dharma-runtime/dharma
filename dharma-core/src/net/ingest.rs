use crate::assertion::{is_overlay, signer_from_meta, AssertionPlaintext};
use crate::contract::{ContractEngine, ContractStatus};
use crate::crypto;
use crate::envelope::{self, AssertionEnvelope};
use crate::error::DharmaError;
use crate::identity::{delegate_allows, root_key_for_identity};
use crate::pdl::schema::{ConcurrencyMode, CqrsSchema};
use crate::runtime::cqrs::{action_index, encode_args_buffer, load_state, merge_args};
use crate::schema as generic_schema;
use crate::store::index::FrontierIndex;
use crate::store::state::{
    append_assertion, append_overlay, find_assertion_by_id, find_overlay_by_id, overlays_for_ref,
};
use crate::store::Store;
use crate::types::{AssertionId, ContractId, EnvelopeId, IdentityKey, SchemaId, SubjectId};
use crate::validation::{structural_validate, StructuralStatus};
use ciborium::value::Value;
use std::collections::HashMap;

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
    Accepted(EnvelopeId),
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
    keys: &HashMap<SubjectId, [u8; 32]>,
) -> Result<IngestStatus, IngestError> {
    let (envelope_id, assertion_id, subject, assertion) = decode_assertion(bytes, keys)?;

    if index.has_envelope(&envelope_id) && !index.is_pending(&assertion_id) {
        return Ok(IngestStatus::Accepted(assertion_id));
    }

    store.put_assertion(&subject, &envelope_id, bytes)?;
    store.record_semantic(&assertion_id, &envelope_id)?;
    let env = store.env();

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
                return Err(IngestError::Validation(reason));
            }
            StructuralStatus::Pending(reason) => {
                index.mark_known(assertion_id);
                index.mark_pending(assertion_id);
                return Ok(IngestStatus::Pending(assertion_id, reason));
            }
            StructuralStatus::Accept => {}
        }
        match validate_action_contract(store, index, &subject, &assertion, assertion_id, Some(base_ref)) {
            Ok(()) => {}
            Err(IngestError::Pending(reason)) => {
                index.mark_known(assertion_id);
                index.mark_pending(assertion_id);
                return Ok(IngestStatus::Pending(assertion_id, reason));
            }
            Err(err) => return Err(err),
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
                return Err(IngestError::Validation(reason));
            }
            StructuralStatus::Pending(reason) => {
                index.mark_known(assertion_id);
                index.mark_pending(assertion_id);
                return Ok(IngestStatus::Pending(assertion_id, reason));
            }
            StructuralStatus::Accept => {}
        }
        match validate_action_contract(store, index, &subject, &assertion, assertion_id, None) {
            Ok(()) => {}
            Err(IngestError::Pending(reason)) => {
                index.mark_known(assertion_id);
                index.mark_pending(assertion_id);
                return Ok(IngestStatus::Pending(assertion_id, reason));
            }
            Err(err) => return Err(err),
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
        index.update(assertion_id, &assertion.header)?;
        index.clear_pending(&assertion_id);
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
            return Err(IngestError::Validation(reason));
        }
        StructuralStatus::Pending(reason) => {
            index.mark_known(assertion_id);
            index.mark_pending(assertion_id);
            return Ok(IngestStatus::Pending(assertion_id, reason));
        }
        StructuralStatus::Accept => {}
    }

    match validate_generic_contract(store, &assertion) {
        Ok(()) => {}
        Err(IngestError::Pending(reason)) => {
            index.mark_known(assertion_id);
            index.mark_pending(assertion_id);
            return Ok(IngestStatus::Pending(assertion_id, reason));
        }
        Err(err) => return Err(err),
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
    index.update(assertion_id, &assertion.header)?;
    index.clear_pending(&assertion_id);
    Ok(IngestStatus::Accepted(assertion_id))
}

pub fn ingest_object_relay(
    store: &Store,
    index: &mut FrontierIndex,
    envelope_id: EnvelopeId,
    bytes: &[u8],
) -> Result<RelayIngestStatus, IngestError> {
    if crypto::envelope_id(bytes) != envelope_id {
        return Err(IngestError::Validation("envelope hash mismatch".to_string()));
    }

    if let Ok(assertion) = AssertionPlaintext::from_cbor(bytes) {
        if !assertion.verify_signature()? {
            return Err(IngestError::Validation("invalid signature".to_string()));
        }
        let assertion_id = assertion.assertion_id()?;
        store.put_object(&envelope_id, bytes)?;
        store.record_semantic(&assertion_id, &envelope_id)?;

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

        index.update(assertion_id, &assertion.header)?;
        index.clear_pending(&assertion_id);
        return Ok(RelayIngestStatus::Accepted(envelope_id));
    }

    store.put_object(&envelope_id, bytes)?;
    Ok(RelayIngestStatus::Opaque(envelope_id))
}

pub fn retry_pending(
    store: &Store,
    index: &mut FrontierIndex,
    keys: &HashMap<SubjectId, [u8; 32]>,
) -> Result<usize, IngestError> {
    let pending = index.pending_objects();
    let mut accepted = 0usize;
    for assertion_id in pending {
        let Some(envelope_id) = store.lookup_envelope(&assertion_id)? else {
            index.clear_pending(&assertion_id);
            continue;
        };
        let bytes = store.get_object(&envelope_id)?;
        match ingest_object(store, index, &bytes, keys) {
            Ok(IngestStatus::Accepted(_)) => {
                accepted += 1;
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
    Ok(accepted)
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
    let action_name = assertion
        .header
        .typ
        .strip_prefix("action.")
        .unwrap_or(&assertion.header.typ);
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

    let contract_bytes = load_contract_bytes(store, &assertion.header.contract)?;
    let mut state = load_state(store.env(), subject, &schema, &contract_bytes, assertion.header.ver)?;
    let action_idx = action_index(&schema, action_name)?;
    let args_buffer = encode_args_buffer(action_schema, action_idx, &merged, true)?;
    let vm = crate::runtime::vm::RuntimeVm::new(contract_bytes);
    let context = build_action_context(&signer, assertion.header.ts);
    vm.validate(store.env(), &mut state.memory, &args_buffer, Some(&context))
        .map_err(|err| IngestError::Validation(err.to_string()))?;
    Ok(())
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
    let Some(_root_key) = root_key_for_identity(store.env(), signer)? else {
        return Err(IngestError::Pending("missing identity root".to_string()));
    };
    if delegate_allows(store.env(), signer, auth, action, now)? {
        Ok(())
    } else {
        Err(IngestError::Validation("unauthorized signer".to_string()))
    }
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
        _ => Err(IngestError::Validation(
            result.reason.unwrap_or_else(|| "contract rejected".to_string()),
        )),
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
    keys: &HashMap<SubjectId, [u8; 32]>,
) -> Result<(EnvelopeId, AssertionId, SubjectId, AssertionPlaintext), IngestError> {
    if let Ok(envelope) = AssertionEnvelope::from_cbor(bytes) {
        let envelope_id = envelope.envelope_id()?;
        for (subject, key) in keys {
            if let Ok(plaintext) = envelope::decrypt_assertion(&envelope, key) {
                if let Ok(assertion) = AssertionPlaintext::from_cbor(&plaintext) {
                    if assertion.header.sub == *subject {
                        let assertion_id = assertion.assertion_id()?;
                        return Ok((envelope_id, assertion_id, *subject, assertion));
                    }
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
    use crate::crypto;
    use crate::pdl::schema::{ActionSchema, ConcurrencyMode, CqrsSchema, TypeSpec, Visibility};
    use crate::types::{AssertionId, ContractId, EnvelopeId, Nonce12, SchemaId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::RngCore;
    use rand::SeedableRng;
    use std::collections::BTreeMap;

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

    #[test]
    fn relay_ingest_accepts_plaintext() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::default();
        let mut rng = StdRng::seed_from_u64(55);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([9u8; 32]);
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
        let status = ingest_object_relay(&store, &mut index, envelope_id, &bytes).unwrap();
        assert_eq!(status, RelayIngestStatus::Accepted(envelope_id));
    }

    #[test]
    fn relay_ingest_accepts_envelope_as_opaque() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut index = FrontierIndex::default();
        let mut rng = StdRng::seed_from_u64(77);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([3u8; 32]);
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
        let status = ingest_object_relay(&store, &mut index, envelope_id, &bytes).unwrap();
        assert_eq!(status, RelayIngestStatus::Opaque(envelope_id));
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

        let mut keys = HashMap::new();
        keys.insert(subject, subject_key);
        let _assertion_id = match ingest_object(&store, &mut index, &bytes, &keys).unwrap() {
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

        let mut keys = HashMap::new();
        keys.insert(subject, subject_key);
        let err = ingest_object(&store, &mut index, &bytes, &keys).unwrap_err();
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

        let mut keys = HashMap::new();
        keys.insert(subject, subject_key);
        let status = ingest_object(&store, &mut index, &bytes, &keys).unwrap();
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
            fields,
            actions,
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

        let keys: HashMap<SubjectId, [u8; 32]> = HashMap::new();
        let err = ingest_object(&store, &mut index, &bytes, &keys).unwrap_err();
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
            fields,
            actions,
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

        let keys: HashMap<SubjectId, [u8; 32]> = HashMap::new();
        let status = ingest_object(&store, &mut index, &bytes, &keys).unwrap();
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

        let keys: HashMap<SubjectId, [u8; 32]> = HashMap::new();
        let err = ingest_object(&store, &mut index, &bytes2, &keys).unwrap_err();
        match err {
            IngestError::MissingDependency { assertion_id, missing } => {
                assert_eq!(assertion_id, assertion_id2);
                assert_eq!(missing, assertion_id1);
            }
            _ => panic!("expected missing dependency"),
        }
        assert!(index.is_pending(&assertion_id2));

        let status = ingest_object(&store, &mut index, &bytes1, &keys).unwrap();
        match status {
            IngestStatus::Accepted(id) => assert_eq!(id, assertion_id1),
            _ => panic!("expected accepted"),
        }

        let accepted = retry_pending(&store, &mut index, &keys).unwrap();
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

        let mut keys = HashMap::new();
        keys.insert(subject, subject_key);
        let err = ingest_object(&store, &mut index, &bytes, &keys).unwrap_err();
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
            fields,
            actions,
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
        let status = ingest_object(&store, &mut index, &next_bytes, &HashMap::new()).unwrap();
        match status {
            IngestStatus::Pending(_, reason) => {
                assert!(reason.contains("fork"));
            }
            other => panic!("expected pending, got {other:?}"),
        }
    }
}
