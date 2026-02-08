use crate::assertion::AssertionPlaintext;
use crate::crypto;
use crate::error::DharmaError;
use crate::store::state::list_assertions;
use crate::store::Store;
use crate::types::{IdentityKey, SubjectId};
use crate::validation::order_assertions;
use crate::value::{expect_bytes, expect_map, map_get};
use ciborium::value::Value;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContactRelation {
    None,
    Pending,
    Accepted,
    Declined,
    Blocked,
}

impl Default for ContactRelation {
    fn default() -> Self {
        ContactRelation::None
    }
}

#[derive(Clone, Debug, Default)]
struct ContactState {
    owner: Option<IdentityKey>,
    contact: Option<IdentityKey>,
    relation: ContactRelation,
    requested_by: Option<IdentityKey>,
    blocked_by: Option<IdentityKey>,
}

impl ContactState {
    fn is_owner_or_contact(&self, signer: &IdentityKey) -> bool {
        self.owner
            .is_some_and(|owner| owner.as_bytes() == signer.as_bytes())
            || self
                .contact
                .is_some_and(|contact| contact.as_bytes() == signer.as_bytes())
    }

    fn apply_action(
        &mut self,
        signer: &IdentityKey,
        action: &str,
        body: &Value,
    ) -> Result<(), DharmaError> {
        match action {
            "Create" => {
                let contact = parse_optional_identity(body, "contact")?;
                self.owner = Some(*signer);
                self.contact = contact;
                self.relation = ContactRelation::None;
                self.requested_by = None;
                self.blocked_by = None;
            }
            "Request" => {
                let Some(other) = parse_optional_identity(body, "other")? else {
                    return Ok(());
                };
                if other.as_bytes() == signer.as_bytes() {
                    return Ok(());
                }
                if let Some(existing) = self.contact {
                    if existing.as_bytes() != other.as_bytes() {
                        return Ok(());
                    }
                }
                if matches!(
                    self.relation,
                    ContactRelation::Pending | ContactRelation::Accepted | ContactRelation::Blocked
                ) {
                    return Ok(());
                }
                self.contact = Some(other);
                self.relation = ContactRelation::Pending;
                self.requested_by = Some(*signer);
                self.blocked_by = None;
            }
            "Accept" => {
                if self.relation != ContactRelation::Pending {
                    return Ok(());
                }
                if self.contact.is_none() {
                    return Ok(());
                }
                if self
                    .requested_by
                    .is_some_and(|id| id.as_bytes() == signer.as_bytes())
                {
                    return Ok(());
                }
                if !self.is_owner_or_contact(signer) {
                    return Ok(());
                }
                self.relation = ContactRelation::Accepted;
            }
            "Decline" => {
                if self.relation != ContactRelation::Pending {
                    return Ok(());
                }
                if self.contact.is_none() {
                    return Ok(());
                }
                if self
                    .requested_by
                    .is_some_and(|id| id.as_bytes() == signer.as_bytes())
                {
                    return Ok(());
                }
                if !self.is_owner_or_contact(signer) {
                    return Ok(());
                }
                self.relation = ContactRelation::Declined;
            }
            "Block" => {
                if self.contact.is_none() {
                    return Ok(());
                }
                if !self.is_owner_or_contact(signer) {
                    return Ok(());
                }
                self.relation = ContactRelation::Blocked;
                self.blocked_by = Some(*signer);
            }
            "Unblock" => {
                if self.relation != ContactRelation::Blocked {
                    return Ok(());
                }
                if !self
                    .blocked_by
                    .is_some_and(|id| id.as_bytes() == signer.as_bytes())
                {
                    return Ok(());
                }
                self.relation = ContactRelation::None;
                self.blocked_by = None;
                self.requested_by = None;
            }
            "UpdateAlias" | "Tag" | "UpdateNotes" => {
                if !self
                    .owner
                    .is_some_and(|owner| owner.as_bytes() == signer.as_bytes())
                {
                    return Ok(());
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn parse_optional_identity(body: &Value, key: &str) -> Result<Option<IdentityKey>, DharmaError> {
    let map = expect_map(body)?;
    let Some(value) = map_get(map, key) else {
        return Ok(None);
    };
    if matches!(value, Value::Null) {
        return Ok(None);
    }
    let bytes = expect_bytes(value)?;
    Ok(Some(IdentityKey::from_slice(&bytes)?))
}

pub fn contact_subject_id(a: &IdentityKey, b: &IdentityKey) -> SubjectId {
    let (lo, hi) = if a.as_bytes() <= b.as_bytes() {
        (a, b)
    } else {
        (b, a)
    };
    let mut buf = Vec::with_capacity(7 + 64);
    buf.extend_from_slice(b"contact");
    buf.extend_from_slice(lo.as_bytes());
    buf.extend_from_slice(hi.as_bytes());
    SubjectId::from_bytes(crypto::sha256(&buf))
}

pub fn relation(
    store: &Store,
    a: &IdentityKey,
    b: &IdentityKey,
) -> Result<ContactRelation, DharmaError> {
    let subject = contact_subject_id(a, b);
    let records = list_assertions(store.env(), &subject)?;
    if records.is_empty() {
        return Ok(ContactRelation::None);
    }
    let mut assertions = HashMap::new();
    for record in records {
        let assertion = AssertionPlaintext::from_cbor(&record.bytes)?;
        if assertion.header.sub != subject {
            continue;
        }
        if !assertion.verify_signature()? {
            continue;
        }
        assertions.insert(record.assertion_id, assertion);
    }
    if assertions.is_empty() {
        return Ok(ContactRelation::None);
    }
    let order = order_assertions(&assertions)?;
    let mut state = ContactState::default();
    for assertion_id in order {
        let assertion = assertions
            .get(&assertion_id)
            .ok_or_else(|| DharmaError::Validation("missing assertion".to_string()))?;
        let action = assertion
            .header
            .typ
            .strip_prefix("action.")
            .unwrap_or(&assertion.header.typ);
        state.apply_action(&assertion.header.auth, action, &assertion.body)?;
    }
    Ok(state.relation)
}

pub fn is_accepted(store: &Store, a: &IdentityKey, b: &IdentityKey) -> Result<bool, DharmaError> {
    Ok(matches!(relation(store, a, b)?, ContactRelation::Accepted))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::builtins;
    use crate::store::state::append_assertion;
    use crate::types::{AssertionId, ContractId, SchemaId};
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn append_contact_action(
        store: &Store,
        subject: &SubjectId,
        seq: u64,
        prev: Option<AssertionId>,
        signer: IdentityKey,
        schema: SchemaId,
        contract: ContractId,
        action: &str,
        body: Value,
        sign: &ed25519_dalek::SigningKey,
    ) -> AssertionId {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: *subject,
            typ: format!("action.{action}"),
            auth: signer,
            seq,
            prev,
            refs: prev.into_iter().collect(),
            ts: None,
            schema,
            contract,
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, body, sign).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        let envelope_id = crypto::envelope_id(&bytes);
        append_assertion(
            store.env(),
            subject,
            seq,
            assertion_id,
            envelope_id,
            &assertion.header.typ,
            &bytes,
        )
        .unwrap();
        assertion_id
    }

    #[test]
    fn contact_subject_id_is_deterministic() {
        let mut rng = StdRng::seed_from_u64(7);
        let (_sk_a, a) = crypto::generate_identity_keypair(&mut rng);
        let (_sk_b, b) = crypto::generate_identity_keypair(&mut rng);
        let id1 = contact_subject_id(&a, &b);
        let id2 = contact_subject_id(&b, &a);
        assert_eq!(id1, id2);
    }

    #[test]
    fn contact_relation_request_accept() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut rng = StdRng::seed_from_u64(9);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (contact_sk, contact_id) = crypto::generate_identity_keypair(&mut rng);
        let (schema_id, contract_id) = builtins::ensure_note_artifacts(&store).unwrap();
        let subject = contact_subject_id(&owner_id, &contact_id);
        let mut seq = 1;
        let mut prev = None;
        let create_body = Value::Map(vec![
            (
                Value::Text("contact".to_string()),
                Value::Bytes(contact_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("alias".to_string()),
                Value::Text("buddy".to_string()),
            ),
        ]);
        let create_id = append_contact_action(
            &store,
            &subject,
            seq,
            prev,
            owner_id,
            schema_id,
            contract_id,
            "Create",
            create_body,
            &owner_sk,
        );
        prev = Some(create_id);
        seq += 1;
        let request_body = Value::Map(vec![(
            Value::Text("other".to_string()),
            Value::Bytes(contact_id.as_bytes().to_vec()),
        )]);
        let request_id = append_contact_action(
            &store,
            &subject,
            seq,
            prev,
            owner_id,
            schema_id,
            contract_id,
            "Request",
            request_body,
            &owner_sk,
        );
        prev = Some(request_id);
        seq += 1;
        let _ = append_contact_action(
            &store,
            &subject,
            seq,
            prev,
            contact_id,
            schema_id,
            contract_id,
            "Accept",
            Value::Map(vec![]),
            &contact_sk,
        );
        let rel = relation(&store, &owner_id, &contact_id).unwrap();
        assert_eq!(rel, ContactRelation::Accepted);
    }

    #[test]
    fn contact_relation_decline() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut rng = StdRng::seed_from_u64(11);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (contact_sk, contact_id) = crypto::generate_identity_keypair(&mut rng);
        let (schema_id, contract_id) = builtins::ensure_note_artifacts(&store).unwrap();
        let subject = contact_subject_id(&owner_id, &contact_id);
        let mut seq = 1;
        let mut prev = None;
        let create_body = Value::Map(vec![
            (
                Value::Text("contact".to_string()),
                Value::Bytes(contact_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("alias".to_string()),
                Value::Text("buddy".to_string()),
            ),
        ]);
        let create_id = append_contact_action(
            &store,
            &subject,
            seq,
            prev,
            owner_id,
            schema_id,
            contract_id,
            "Create",
            create_body,
            &owner_sk,
        );
        prev = Some(create_id);
        seq += 1;
        let request_body = Value::Map(vec![(
            Value::Text("other".to_string()),
            Value::Bytes(contact_id.as_bytes().to_vec()),
        )]);
        let request_id = append_contact_action(
            &store,
            &subject,
            seq,
            prev,
            owner_id,
            schema_id,
            contract_id,
            "Request",
            request_body,
            &owner_sk,
        );
        prev = Some(request_id);
        seq += 1;
        let _ = append_contact_action(
            &store,
            &subject,
            seq,
            prev,
            contact_id,
            schema_id,
            contract_id,
            "Decline",
            Value::Map(vec![]),
            &contact_sk,
        );
        let rel = relation(&store, &owner_id, &contact_id).unwrap();
        assert_eq!(rel, ContactRelation::Declined);
    }

    #[test]
    fn contact_relation_block_unblock() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut rng = StdRng::seed_from_u64(13);
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(&mut rng);
        let (contact_sk, contact_id) = crypto::generate_identity_keypair(&mut rng);
        let (schema_id, contract_id) = builtins::ensure_note_artifacts(&store).unwrap();
        let subject = contact_subject_id(&owner_id, &contact_id);
        let mut seq = 1;
        let mut prev = None;
        let create_body = Value::Map(vec![
            (
                Value::Text("contact".to_string()),
                Value::Bytes(contact_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("alias".to_string()),
                Value::Text("buddy".to_string()),
            ),
        ]);
        let create_id = append_contact_action(
            &store,
            &subject,
            seq,
            prev,
            owner_id,
            schema_id,
            contract_id,
            "Create",
            create_body,
            &owner_sk,
        );
        prev = Some(create_id);
        seq += 1;
        let block_id = append_contact_action(
            &store,
            &subject,
            seq,
            prev,
            contact_id,
            schema_id,
            contract_id,
            "Block",
            Value::Map(vec![]),
            &contact_sk,
        );
        prev = Some(block_id);
        seq += 1;
        let rel = relation(&store, &owner_id, &contact_id).unwrap();
        assert_eq!(rel, ContactRelation::Blocked);
        let _ = append_contact_action(
            &store,
            &subject,
            seq,
            prev,
            contact_id,
            schema_id,
            contract_id,
            "Unblock",
            Value::Map(vec![]),
            &contact_sk,
        );
        let rel = relation(&store, &owner_id, &contact_id).unwrap();
        assert_eq!(rel, ContactRelation::None);
    }
}
