use crate::assertion::AssertionPlaintext;
use crate::error::DharmaError;
use crate::store::state::AssertionRecord;
use crate::validation::{structural_validate, StructuralStatus};
use std::collections::HashMap;

#[derive(Default, Debug, Clone, Copy)]
pub struct StructuralCounts {
    pub accepted: usize,
    pub pending: usize,
    pub rejected: usize,
}

pub fn structural_counts(records: &[AssertionRecord]) -> Result<StructuralCounts, DharmaError> {
    let mut map: HashMap<_, AssertionPlaintext> = HashMap::new();
    for record in records {
        if let Ok(assertion) = AssertionPlaintext::from_cbor(&record.bytes) {
            map.insert(record.assertion_id, assertion);
        }
    }
    let mut counts = StructuralCounts::default();
    for record in records {
        let assertion = match map.get(&record.assertion_id) {
            Some(assertion) => assertion,
            None => {
                counts.rejected += 1;
                continue;
            }
        };
        let prev = assertion
            .header
            .prev
            .and_then(|prev_id| map.get(&prev_id));
        match structural_validate(assertion, prev)? {
            StructuralStatus::Accept => counts.accepted += 1,
            StructuralStatus::Pending(_) => counts.pending += 1,
            StructuralStatus::Reject(_) => counts.rejected += 1,
        }
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, DEFAULT_DATA_VERSION};
    use crate::crypto;
    use crate::types::{ContractId, IdentityKey, SchemaId, SubjectId};
    use ciborium::value::Value;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn structural_counts_accepts_chain() {
        let subject = SubjectId::from_bytes([3u8; 32]);
        let mut rng = StdRng::seed_from_u64(1);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);

        let header_a = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let body_a = Value::Map(vec![(Value::Text("v".to_string()), Value::Integer(1.into()))]);
        let assertion_a = AssertionPlaintext::sign(header_a, body_a, &signing_key).unwrap();
        let bytes_a = assertion_a.to_cbor().unwrap();
        let assertion_id_a = assertion_a.assertion_id().unwrap();
        let envelope_id_a = crypto::envelope_id(&bytes_a);

        let header_b = AssertionHeader {
            seq: 2,
            prev: Some(assertion_id_a),
            ..assertion_a.header.clone()
        };
        let body_b = Value::Map(vec![(Value::Text("v".to_string()), Value::Integer(2.into()))]);
        let assertion_b = AssertionPlaintext::sign(header_b, body_b, &signing_key).unwrap();
        let bytes_b = assertion_b.to_cbor().unwrap();
        let assertion_id_b = assertion_b.assertion_id().unwrap();
        let envelope_id_b = crypto::envelope_id(&bytes_b);

        let records = vec![
            AssertionRecord {
                seq: 1,
                assertion_id: assertion_id_a,
                envelope_id: envelope_id_a,
                bytes: bytes_a,
            },
            AssertionRecord {
                seq: 2,
                assertion_id: assertion_id_b,
                envelope_id: envelope_id_b,
                bytes: bytes_b,
            },
        ];
        let counts = structural_counts(&records).unwrap();
        assert_eq!(counts.accepted, 2);
        assert_eq!(counts.pending, 0);
        assert_eq!(counts.rejected, 0);
    }
}
