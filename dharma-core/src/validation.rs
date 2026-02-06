use crate::assertion::AssertionPlaintext;
use crate::assertion_types::CORE_MERGE;
use crate::contract::{ContractEngine, ContractStatus};
use crate::crypto;
use crate::error::DharmaError;
use crate::schema::SchemaManifest;
use crate::types::{AssertionId, SubjectId};
use ciborium::value::Value;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StructuralStatus {
    Accept,
    Reject(String),
    Pending(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubjectValidation {
    pub accepted: Vec<AssertionId>,
    pub pending: Vec<AssertionId>,
    pub rejected: Vec<AssertionId>,
}

pub fn structural_validate(
    assertion: &AssertionPlaintext,
    prev: Option<&AssertionPlaintext>,
) -> Result<StructuralStatus, DharmaError> {
    if assertion.header.v != crypto::PROTOCOL_VERSION {
        return Ok(StructuralStatus::Reject("version mismatch".to_string()));
    }
    if assertion.header.ver == 0 {
        return Ok(StructuralStatus::Reject("invalid data version".to_string()));
    }
    if assertion.header.typ.is_empty() {
        return Ok(StructuralStatus::Reject("empty typ".to_string()));
    }
    if !assertion.verify_signature()? {
        return Ok(StructuralStatus::Reject("invalid signature".to_string()));
    }
    if assertion.header.typ == CORE_MERGE {
        if assertion.header.refs.len() < 2 {
            return Ok(StructuralStatus::Reject(
                "merge requires at least two refs".to_string(),
            ));
        }
        if let Some(prev) = assertion.header.prev {
            if !assertion.header.refs.contains(&prev) {
                return Ok(StructuralStatus::Reject(
                    "merge prev must be in refs".to_string(),
                ));
            }
        }
    }
    match assertion.header.seq {
        1 => {
            if assertion.header.prev.is_some() {
                return Ok(StructuralStatus::Reject("prev must be null".to_string()));
            }
        }
        _ => {
            if assertion.header.prev.is_none() {
                return Ok(StructuralStatus::Reject("missing prev".to_string()));
            }
        }
    }
    if let Some(prev_id) = assertion.header.prev {
        match prev {
            None => {
                return Ok(StructuralStatus::Pending("missing prev assertion".to_string()));
            }
            Some(prev_assertion) => {
                if prev_assertion.header.sub != assertion.header.sub
                    || prev_assertion.header.seq + 1 != assertion.header.seq
                {
                    return Ok(StructuralStatus::Reject("author chain mismatch".to_string()));
                }
                if prev_assertion.header.ver != assertion.header.ver {
                    return Ok(StructuralStatus::Reject("data version mismatch".to_string()));
                }
                let _ = prev_id; // explicit, for clarity
            }
        }
    }
    Ok(StructuralStatus::Accept)
}

pub fn order_assertions(
    assertions: &HashMap<AssertionId, AssertionPlaintext>,
) -> Result<Vec<AssertionId>, DharmaError> {
    let mut indegree: HashMap<AssertionId, usize> = HashMap::new();
    let mut deps: HashMap<AssertionId, Vec<AssertionId>> = HashMap::new();

    for (id, assertion) in assertions {
        let mut dependencies = Vec::new();
        if let Some(prev) = assertion.header.prev {
            if assertions.contains_key(&prev) {
                dependencies.push(prev);
            }
        }
        for r in &assertion.header.refs {
            if assertions.contains_key(r) {
                dependencies.push(*r);
            }
        }
        indegree.insert(*id, dependencies.len());
        for dep in dependencies {
            deps.entry(dep).or_default().push(*id);
        }
    }

    let mut ready = BTreeSet::new();
    for (id, degree) in &indegree {
        if *degree == 0 {
            ready.insert(*id);
        }
    }

    let mut ordered = Vec::with_capacity(assertions.len());
    while let Some(id) = ready.iter().next().cloned() {
        ready.remove(&id);
        ordered.push(id);
        if let Some(children) = deps.get(&id) {
            for child in children {
                if let Some(count) = indegree.get_mut(child) {
                    *count -= 1;
                    if *count == 0 {
                        ready.insert(*child);
                    }
                }
            }
        }
    }

    if ordered.len() != assertions.len() {
        return Err(DharmaError::DependencyCycle);
    }
    Ok(ordered)
}

pub fn validate_subject(
    subject: &SubjectId,
    assertions: &HashMap<AssertionId, AssertionPlaintext>,
    schema: &SchemaManifest,
    contract: &ContractEngine,
) -> Result<SubjectValidation, DharmaError> {
    let order = order_assertions(assertions)?;
    let mut accepted = Vec::new();
    let mut pending = Vec::new();
    let mut rejected = Vec::new();
    let mut accepted_assertions: Vec<Arc<AssertionPlaintext>> = Vec::new();
    let mut accepted_map: HashMap<AssertionId, Arc<AssertionPlaintext>> = HashMap::new();

    for id in order {
        let assertion = assertions.get(&id).ok_or_else(|| DharmaError::Validation("missing assertion".to_string()))?;
        let prev = assertion
            .header
            .prev
            .and_then(|prev_id| assertions.get(&prev_id));
        match structural_validate(assertion, prev)? {
            StructuralStatus::Accept => {}
            StructuralStatus::Pending(_) => {
                pending.push(id);
                continue;
            }
            StructuralStatus::Reject(_) => {
                rejected.push(id);
                continue;
            }
        }
        if let Err(_) = crate::schema::validate_body(schema, &assertion.header.typ, &assertion.body) {
            rejected.push(id);
            continue;
        }
        let context = build_context(subject, &accepted_assertions, &accepted_map, assertion);
        let context_bytes = crate::cbor::encode_canonical_value(&context)?;
        let assertion_bytes = assertion.to_cbor()?;
        let result = contract.validate(&assertion_bytes, &context_bytes)?;
        match result.status {
            ContractStatus::Accept if result.ok => {
                accepted.push(id);
                let shared_assertion = Arc::new(assertion.clone());
                accepted_assertions.push(Arc::clone(&shared_assertion));
                accepted_map.insert(id, shared_assertion);
            }
            ContractStatus::Pending => pending.push(id),
            _ => rejected.push(id),
        }
    }

    Ok(SubjectValidation { accepted, pending, rejected })
}

fn build_context(
    subject: &SubjectId,
    accepted: &[Arc<AssertionPlaintext>],
    accepted_map: &HashMap<AssertionId, Arc<AssertionPlaintext>>,
    current: &AssertionPlaintext,
) -> Value {
    let accepted_values = accepted
        .iter()
        .map(|a| a.to_value())
        .collect::<Vec<_>>();
    let mut lookup_entries = Vec::new();
    for reference in current.header.refs.iter().copied().chain(current.header.prev) {
        if let Some(assertion) = accepted_map.get(&reference) {
            lookup_entries.push((
                Value::Bytes(reference.as_bytes().to_vec()),
                assertion.to_value(),
            ));
        }
    }
    Value::Map(vec![
        (
            Value::Text("subject".to_string()),
            Value::Bytes(subject.as_bytes().to_vec()),
        ),
        (
            Value::Text("accepted".to_string()),
            Value::Array(accepted_values),
        ),
        (
            Value::Text("lookup".to_string()),
            Value::Map(lookup_entries),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::DEFAULT_DATA_VERSION;
    use crate::assertion::{AssertionHeader, AssertionPlaintext};
    use crate::contract::ContractEngine;
    use crate::crypto;
    use crate::schema::SchemaManifest;
    use crate::types::{ContractId, SchemaId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::collections::BTreeMap;

    fn schema() -> SchemaManifest {
        SchemaManifest {
            v: 1,
            name: "test".to_string(),
            implements: Vec::new(),
            types: {
                let mut map = BTreeMap::new();
                map.insert(
                    "core.genesis".to_string(),
                    crate::schema::SchemaType {
                        body: BTreeMap::new(),
                        required: Default::default(),
                        allow_extra: true,
                    },
                );
                map
            },
        }
    }

    fn contract() -> ContractEngine {
        ContractEngine::new(crate::contract::test_wasm_bytes())
    }

    #[test]
    fn order_assertions_detects_cycle() {
        let mut rng = StdRng::seed_from_u64(3);
        let (sk, id) = crypto::generate_identity_keypair(&mut rng);
        let header1 = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: SubjectId::from_bytes([1u8; 32]),
            typ: "core.genesis".to_string(),
            auth: id,
            seq: 1,
            prev: None,
            refs: vec![AssertionId::from_bytes([2u8; 32])],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let a1 = AssertionPlaintext::sign(header1, Value::Null, &sk).unwrap();
        let header2 = AssertionHeader { seq: 2, prev: Some(AssertionId::from_bytes([1u8; 32])), ..a1.header.clone() };
        let a2 = AssertionPlaintext::sign(header2, Value::Null, &sk).unwrap();
        let mut map = HashMap::new();
        map.insert(AssertionId::from_bytes([1u8; 32]), a1);
        map.insert(AssertionId::from_bytes([2u8; 32]), a2);
        assert!(order_assertions(&map).is_err());
    }

    #[test]
    fn validate_subject_accepts() {
        let mut rng = StdRng::seed_from_u64(4);
        let (sk, id) = crypto::generate_identity_keypair(&mut rng);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: SubjectId::from_bytes([9u8; 32]),
            typ: "core.genesis".to_string(),
            auth: id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, Value::Map(vec![]), &sk).unwrap();
        let mut map = HashMap::new();
        map.insert(AssertionId::from_bytes([1u8; 32]), assertion);
        let result = validate_subject(&SubjectId::from_bytes([9u8; 32]), &map, &schema(), &contract()).unwrap();
        assert_eq!(result.accepted.len(), 1);
    }

    #[test]
    fn structural_pending_when_prev_missing() {
        let mut rng = StdRng::seed_from_u64(6);
        let (sk, id) = crypto::generate_identity_keypair(&mut rng);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: SubjectId::from_bytes([5u8; 32]),
            typ: "core.genesis".to_string(),
            auth: id,
            seq: 2,
            prev: Some(AssertionId::from_bytes([1u8; 32])),
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, Value::Null, &sk).unwrap();
        let status = structural_validate(&assertion, None).unwrap();
        assert!(matches!(status, StructuralStatus::Pending(_)));
    }

    #[test]
    fn structural_rejects_missing_prev_field() {
        let mut rng = StdRng::seed_from_u64(7);
        let (sk, id) = crypto::generate_identity_keypair(&mut rng);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: SubjectId::from_bytes([5u8; 32]),
            typ: "core.genesis".to_string(),
            auth: id,
            seq: 2,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, Value::Null, &sk).unwrap();
        let status = structural_validate(&assertion, None).unwrap();
        assert!(matches!(status, StructuralStatus::Reject(_)));
    }

    #[test]
    fn structural_rejects_prev_version_mismatch() {
        let mut rng = StdRng::seed_from_u64(9);
        let (sk, id) = crypto::generate_identity_keypair(&mut rng);
        let prev_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: SubjectId::from_bytes([5u8; 32]),
            typ: "core.genesis".to_string(),
            auth: id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let prev_assertion = AssertionPlaintext::sign(prev_header, Value::Null, &sk).unwrap();
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 2,
            sub: SubjectId::from_bytes([5u8; 32]),
            typ: "core.genesis".to_string(),
            auth: id,
            seq: 2,
            prev: Some(AssertionId::from_bytes([1u8; 32])),
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, Value::Null, &sk).unwrap();
        let status = structural_validate(&assertion, Some(&prev_assertion)).unwrap();
        assert!(matches!(status, StructuralStatus::Reject(_)));
    }

    #[test]
    fn structural_rejects_merge_without_refs() {
        let mut rng = StdRng::seed_from_u64(10);
        let (sk, id) = crypto::generate_identity_keypair(&mut rng);
        let prev_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: SubjectId::from_bytes([6u8; 32]),
            typ: "action.Touch".to_string(),
            auth: id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let prev_assertion = AssertionPlaintext::sign(prev_header, Value::Null, &sk).unwrap();
        let prev_id = prev_assertion.assertion_id().unwrap();
        let merge_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: SubjectId::from_bytes([6u8; 32]),
            typ: CORE_MERGE.to_string(),
            auth: id,
            seq: 2,
            prev: Some(prev_id),
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let merge = AssertionPlaintext::sign(merge_header, Value::Null, &sk).unwrap();
        let status = structural_validate(&merge, Some(&prev_assertion)).unwrap();
        assert!(matches!(status, StructuralStatus::Reject(_)));
    }

    #[test]
    fn structural_rejects_merge_prev_not_in_refs() {
        let mut rng = StdRng::seed_from_u64(12);
        let (sk, id) = crypto::generate_identity_keypair(&mut rng);
        let prev_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: SubjectId::from_bytes([7u8; 32]),
            typ: "action.Touch".to_string(),
            auth: id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let prev_assertion = AssertionPlaintext::sign(prev_header, Value::Null, &sk).unwrap();
        let prev_id = prev_assertion.assertion_id().unwrap();
        let other_id = AssertionId::from_bytes([1u8; 32]);
        let merge_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: SubjectId::from_bytes([7u8; 32]),
            typ: CORE_MERGE.to_string(),
            auth: id,
            seq: 2,
            prev: Some(prev_id),
            refs: vec![other_id],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let merge = AssertionPlaintext::sign(merge_header, Value::Null, &sk).unwrap();
        let status = structural_validate(&merge, Some(&prev_assertion)).unwrap();
        assert!(matches!(status, StructuralStatus::Reject(_)));
    }

    #[test]
    fn order_assertions_success() {
        let mut rng = StdRng::seed_from_u64(8);
        let (sk, id) = crypto::generate_identity_keypair(&mut rng);
        let header1 = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: SubjectId::from_bytes([1u8; 32]),
            typ: "core.genesis".to_string(),
            auth: id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let a1 = AssertionPlaintext::sign(header1, Value::Null, &sk).unwrap();
        let header2 = AssertionHeader { seq: 2, prev: Some(AssertionId::from_bytes([1u8; 32])), ..a1.header.clone() };
        let a2 = AssertionPlaintext::sign(header2, Value::Null, &sk).unwrap();
        let mut map = HashMap::new();
        map.insert(AssertionId::from_bytes([1u8; 32]), a1);
        map.insert(AssertionId::from_bytes([2u8; 32]), a2);
        let order = order_assertions(&map).unwrap();
        assert_eq!(order.len(), 2);
    }
}
