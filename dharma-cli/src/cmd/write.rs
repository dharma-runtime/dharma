use ciborium::value::Value;
use dharma::assertion::{add_signer_meta, AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
use dharma::crypto;
use dharma::envelope;
use dharma::keys::Keyring;
use dharma::net::ingest::{ingest_object, IngestError};
use dharma::store::index::FrontierIndex;
use dharma::store::Store;
use dharma::types::{AssertionId, Nonce12, SubjectId};
use dharma::{IdentityState, DharmaError};
use rand_core::OsRng;
use std::collections::HashMap;

pub fn write_cmd(subject_hex: Option<&str>, body: &str) -> Result<(), DharmaError> {
    let data_dir = crate::ensure_data_dir()?;
    let env = dharma::env::StdEnv::new(&data_dir);
    crate::ensure_identity_present(&env)?;
    let identity = crate::load_identity(&env)?;
    let _head = crate::mount_self(&env, &identity)?;

    let store = Store::new(&env);
    let mut legacy_keys = HashMap::new();
    legacy_keys.insert(identity.subject_id, identity.subject_key);
    let mut keys = Keyring::from_subject_keys(&legacy_keys);
    keys.insert_hpke_secret(identity.public_key, identity.noise_sk);
    let mut index = FrontierIndex::build(&store, &keys)?;

    let (subject, subject_key, _defaulted) = match subject_hex {
        None => (identity.subject_id, identity.subject_key, false),
        Some(hex) => {
            let requested = SubjectId::from_hex(hex)?;
            let (subject, key, defaulted) = resolve_subject(requested, &identity, &legacy_keys)?;
            if defaulted {
                eprintln!(
                    "Warning: no subject key for {}, defaulting to identity {}",
                    hex,
                    subject.to_hex()
                );
            }
            (subject, key, defaulted)
        }
    };

    let epoch = dharma::store::state::load_epoch(store.env(), &subject)?.unwrap_or(0);
    keys.insert_sdk(subject, epoch, subject_key);

    let (prev_id, prev_seq) = select_tip(&subject, &index)?;

    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: "note.text".to_string(),
        auth: identity.public_key,
        seq: prev_seq + 1,
        prev: Some(prev_id),
        refs: Vec::new(),
        ts: None,
        schema: identity.schema,
        contract: identity.contract,
        note: None,
        meta: add_signer_meta(None, &identity.subject_id),
    };
    let body = Value::Map(vec![(
        Value::Text("text".to_string()),
        Value::Text(body.to_string()),
    )]);
    let assertion = AssertionPlaintext::sign(header, body, &identity.signing_key)?;
    let assertion_id = assertion.assertion_id()?;
    let plaintext = assertion.to_cbor()?;

    let kid = crypto::key_id_from_key(&subject_key);
    let envelope = envelope::encrypt_assertion_with_epoch(
        &plaintext,
        kid,
        &subject_key,
        Nonce12::random(&mut OsRng),
        epoch,
    )?;
    let bytes = envelope.to_cbor()?;

    match ingest_object(&store, &mut index, &bytes, &mut keys) {
        Ok(dharma::net::ingest::IngestStatus::Accepted(_)) => {}
        Ok(dharma::net::ingest::IngestStatus::Pending(_, reason)) => {
            return Err(DharmaError::Validation(format!("pending: {reason}")));
        }
        Err(IngestError::MissingDependency { missing: dep, .. }) => {
            return Err(DharmaError::Validation(format!(
                "missing dependency {}",
                dep.to_hex()
            )));
        }
        Err(IngestError::Pending(reason)) => {
            return Err(DharmaError::Validation(format!("pending: {reason}")));
        }
        Err(IngestError::Validation(reason)) => {
            return Err(DharmaError::Validation(reason));
        }
        Err(IngestError::Dharma(err)) => return Err(err),
    }

    println!("Wrote {} {}", subject.to_hex(), assertion_id.to_hex());
    if subject == identity.subject_id {
        let _ = crate::vault::maybe_archive_after_write(
            &store,
            subject,
            DEFAULT_DATA_VERSION,
            identity.schema,
            identity.contract,
        );
    }
    Ok(())
}

fn resolve_subject(
    requested: SubjectId,
    identity: &IdentityState,
    keys: &HashMap<SubjectId, [u8; 32]>,
) -> Result<(SubjectId, [u8; 32], bool), DharmaError> {
    if let Some(key) = keys.get(&requested) {
        return Ok((requested, *key, false));
    }
    if let Some(key) = keys.get(&identity.subject_id) {
        return Ok((identity.subject_id, *key, true));
    }
    Err(DharmaError::Validation("no subject key available".to_string()))
}

fn select_tip(subject: &SubjectId, index: &FrontierIndex) -> Result<(AssertionId, u64), DharmaError> {
    let tips = index.get_tips(subject);
    if tips.is_empty() {
        return Err(DharmaError::Validation("no tips for subject".to_string()));
    }

    let mut best: Option<(AssertionId, u64)> = None;
    for tip in tips {
        let Some(seq) = index.tip_seq(subject, &tip) else {
            continue;
        };
        if matches!(best, Some((_, best_seq)) if best_seq >= seq) {
            continue;
        }
        best = Some((tip, seq));
    }

    best.ok_or_else(|| DharmaError::Validation("no readable tips".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dharma::crypto;
    use dharma::types::{ContractId, IdentityKey, SchemaId};
    use rand::rngs::StdRng;
    use rand::RngCore;
    use rand::SeedableRng;
    use std::fs;

    fn write_enveloped_assertion(
        store: &Store,
        subject: SubjectId,
        subject_key: &[u8; 32],
        signing_key: &ed25519_dalek::SigningKey,
        seq: u64,
        prev: Option<AssertionId>,
    ) -> AssertionId {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq,
            prev,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![(
            Value::Text("text".to_string()),
            Value::Text(format!("note {seq}")),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, signing_key).unwrap();
        let plaintext = assertion.to_cbor().unwrap();
        let kid = crypto::key_id_from_key(subject_key);
        let envelope = envelope::encrypt_assertion(
            &plaintext,
            kid,
            subject_key,
            Nonce12::from_bytes([1u8; 12]),
        )
        .unwrap();
        let bytes = envelope.to_cbor().unwrap();
        let envelope_id = envelope.envelope_id().unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        store.put_assertion(&subject, &envelope_id, &bytes).unwrap();
        store.record_semantic(&assertion_id, &envelope_id).unwrap();
        dharma::store::state::append_assertion(
            store.env(),
            &subject,
            seq,
            assertion_id,
            envelope_id,
            "note.text",
            &plaintext,
        )
        .unwrap();
        assertion_id
    }

    #[test]
    fn select_tip_picks_highest_seq() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut rng = StdRng::seed_from_u64(99);
        let mut subject_key = [0u8; 32];
        rng.fill_bytes(&mut subject_key);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([8u8; 32]);

        let first = write_enveloped_assertion(
            &store,
            subject,
            &subject_key,
            &signing_key,
            1,
            None,
        );
        let second = write_enveloped_assertion(
            &store,
            subject,
            &subject_key,
            &signing_key,
            2,
            Some(first),
        );

        let mut keys = Keyring::new();
        keys.insert_sdk(subject, 0, subject_key);
        let index = FrontierIndex::build(&store, &keys).unwrap();

        let (tip, seq) = select_tip(&subject, &index).unwrap();
        assert_eq!(tip, second);
        assert_eq!(seq, 2);
    }

    #[test]
    fn select_tip_errors_without_tips() {
        let temp = tempfile::tempdir().unwrap();
        let index = FrontierIndex::default();
        let subject = SubjectId::from_bytes([1u8; 32]);
        let err = select_tip(&subject, &index).unwrap_err();
        assert!(matches!(err, DharmaError::Validation(_)));
        fs::remove_dir_all(temp.path()).unwrap();
    }

    #[test]
    fn resolve_subject_defaults_to_identity() {
        let mut rng = StdRng::seed_from_u64(5);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let (root_signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let root_public_key = IdentityKey::from_bytes(root_signing_key.verifying_key().to_bytes());
        let identity = IdentityState {
            subject_id: SubjectId::from_bytes([1u8; 32]),
            signing_key,
            public_key: IdentityKey::from_bytes([2u8; 32]),
            root_signing_key,
            root_public_key,
            subject_key: [9u8; 32],
            noise_sk: [8u8; 32],
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
        };
        let mut keys = HashMap::new();
        keys.insert(identity.subject_id, identity.subject_key);
        let requested = SubjectId::from_bytes([7u8; 32]);
        let (subject, key, defaulted) = resolve_subject(requested, &identity, &keys).unwrap();
        assert_eq!(subject, identity.subject_id);
        assert_eq!(key, identity.subject_key);
        assert!(defaulted);
    }
}
