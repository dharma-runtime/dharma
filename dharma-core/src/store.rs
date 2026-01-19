use crate::assertion::{is_overlay, AssertionPlaintext};
use crate::cbor;
use crate::envelope::AssertionEnvelope;
use crate::env::{Env, StdEnv};
use crate::error::DharmaError;
pub mod index;
pub mod state;
use crate::types::{AssertionId, EnvelopeId, SubjectId};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Store {
    env: Arc<dyn Env + Send + Sync>,
    verified_contracts: Arc<Mutex<HashMap<EnvelopeId, Vec<u8>>>>,
}

fn is_torn_write(err: &DharmaError) -> bool {
    matches!(err, DharmaError::Io(io) if io.to_string().contains("torn write"))
}

fn write_with_retry(env: &dyn Env, path: &Path, data: &[u8]) -> Result<(), DharmaError> {
    match env.write(path, data) {
        Ok(()) => Ok(()),
        Err(err) if is_torn_write(&err) => env.write(path, data),
        Err(err) => Err(err),
    }
}

fn read_cbor_with_retry(env: &dyn Env, path: &Path, attempts: usize) -> Result<Vec<u8>, DharmaError> {
    let mut last_err: Option<DharmaError> = None;
    for _ in 0..attempts {
        let bytes = env.read(path)?;
        if looks_like_wasm(&bytes) {
            return Ok(bytes);
        }
        if cbor::ensure_canonical(&bytes).is_ok() {
            return Ok(bytes);
        }
        last_err = Some(DharmaError::Cbor("corrupt cbor".to_string()));
    }
    Err(last_err.unwrap_or_else(|| DharmaError::Cbor("corrupt cbor".to_string())))
}

pub(crate) fn looks_like_wasm(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[..4] == [0x00, 0x61, 0x73, 0x6d]
}

fn repair_semantic_index(env: &dyn Env, path: &Path) -> Result<(), DharmaError> {
    if !env.exists(path) {
        return Ok(());
    }
    let buf = env.read(path)?;
    let entry_len = 64;
    let usable = (buf.len() / entry_len) * entry_len;
    if usable == buf.len() {
        return Ok(());
    }
    if usable == 0 {
        env.remove_file(path)?;
        return Ok(());
    }
    write_with_retry(env, path, &buf[..usable])
}

impl Store {
    pub fn new<E>(env: &E) -> Self
    where
        E: Env + Clone + Send + Sync + 'static,
    {
        Self {
            env: Arc::new(env.clone()),
            verified_contracts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn from_root<P: Into<PathBuf>>(root: P) -> Self {
        let env = StdEnv::new(root);
        Self::new(&env)
    }

    pub fn root(&self) -> &Path {
        self.env.root()
    }

    pub fn env(&self) -> &dyn Env {
        self.env.as_ref()
    }

    pub fn env_arc(&self) -> Arc<dyn Env + Send + Sync> {
        Arc::clone(&self.env)
    }

    fn cached_contract(&self, envelope_id: &EnvelopeId) -> Option<Vec<u8>> {
        let Ok(guard) = self.verified_contracts.lock() else {
            return None;
        };
        guard.get(envelope_id).cloned()
    }

    fn cache_contract(&self, envelope_id: EnvelopeId, bytes: Vec<u8>) {
        if let Ok(mut guard) = self.verified_contracts.lock() {
            guard.insert(envelope_id, bytes);
        }
    }

    pub fn verify_contract_bytes(
        &self,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        if self.cached_contract(envelope_id).is_some() {
            return Ok(());
        }
        let actual = crate::crypto::envelope_id(bytes);
        if &actual != envelope_id {
            return Err(DharmaError::Validation("contract hash mismatch".to_string()));
        }
        self.cache_contract(*envelope_id, bytes.to_vec());
        Ok(())
    }

    pub fn get_verified_contract(
        &self,
        envelope_id: &EnvelopeId,
    ) -> Result<Option<Vec<u8>>, DharmaError> {
        if let Some(bytes) = self.cached_contract(envelope_id) {
            return Ok(Some(bytes));
        }
        let path = self.objects_dir().join(format!("{}.obj", envelope_id.to_hex()));
        if !self.env.exists(&path) {
            return Ok(None);
        }
        let mut last_err: Option<DharmaError> = None;
        for _ in 0..3 {
            let bytes = self.env.read(&path)?;
            let actual = crate::crypto::envelope_id(&bytes);
            if &actual == envelope_id {
                self.cache_contract(*envelope_id, bytes.clone());
                return Ok(Some(bytes));
            }
            last_err = Some(DharmaError::Validation("contract hash mismatch".to_string()));
        }
        Err(last_err.unwrap_or_else(|| DharmaError::Validation("contract hash mismatch".to_string())))
    }

    pub fn objects_dir(&self) -> PathBuf {
        self.root().join("objects")
    }

    pub fn indexes_dir(&self) -> PathBuf {
        self.root().join("indexes")
    }

    pub fn subjects_root(&self) -> PathBuf {
        self.root().join("subjects")
    }

    pub fn subject_dir(&self, subject: &SubjectId) -> PathBuf {
        self.subjects_root().join(subject.to_hex())
    }

    pub fn put_object(&self, envelope_id: &EnvelopeId, bytes: &[u8]) -> Result<(), DharmaError> {
        let dir = self.objects_dir();
        self.env.create_dir_all(&dir)?;
        let path = dir.join(format!("{}.obj", envelope_id.to_hex()));
        if self.env.exists(&path) {
            return Ok(());
        }
        write_with_retry(self.env.as_ref(), &path, bytes)?;
        crate::store::state::append_manifest(self.env.as_ref(), envelope_id, None)?;
        Ok(())
    }

    pub fn get_object(&self, envelope_id: &EnvelopeId) -> Result<Vec<u8>, DharmaError> {
        let path = self.objects_dir().join(format!("{}.obj", envelope_id.to_hex()));
        read_cbor_with_retry(self.env.as_ref(), &path, 3)
    }

    pub fn put_assertion(
        &self,
        subject: &SubjectId,
        envelope_id: &EnvelopeId,
        bytes: &[u8],
    ) -> Result<(), DharmaError> {
        let _ = subject;
        self.put_object(envelope_id, bytes)
    }

    pub fn get_assertion(
        &self,
        subject: &SubjectId,
        envelope_id: &EnvelopeId,
    ) -> Result<Vec<u8>, DharmaError> {
        let _ = subject;
        self.get_object(envelope_id)
    }

    pub fn scan_subject(&self, subject: &SubjectId) -> Result<Vec<AssertionId>, DharmaError> {
        let mut out = Vec::new();
        for record in state::list_assertions(self.env.as_ref(), subject)? {
            out.push(record.assertion_id);
        }
        Ok(out)
    }

    pub fn list_subjects(&self) -> Result<Vec<SubjectId>, DharmaError> {
        let mut out = Vec::new();
        let root = self.subjects_root();
        if !self.env.exists(&root) {
            return Ok(out);
        }
        for path in self.env.list_dir(&root)? {
            if self.env.is_dir(&path) {
                let name = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("");
                if let Ok(subject) = SubjectId::from_hex(name) {
                    out.push(subject);
                }
            }
        }
        Ok(out)
    }

    pub fn get_object_any(&self, envelope_id: &EnvelopeId) -> Result<Option<Vec<u8>>, DharmaError> {
        let path = self.objects_dir().join(format!("{}.obj", envelope_id.to_hex()));
        if !self.env.exists(&path) {
            return Ok(None);
        }
        match read_cbor_with_retry(self.env.as_ref(), &path, 3) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(DharmaError::Cbor(_)) => Ok(None),
            Err(DharmaError::Validation(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn list_objects(&self) -> Result<Vec<EnvelopeId>, DharmaError> {
        let mut out = Vec::new();
        let dir = self.objects_dir();
        if !self.env.exists(&dir) {
            return Ok(out);
        }
        for path in self.env.list_dir(&dir)? {
            if !self.env.is_file(&path) {
                continue;
            }
            let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            let hex = name.strip_suffix(".obj").unwrap_or(name);
            if let Ok(id) = EnvelopeId::from_hex(hex) {
                out.push(id);
            }
        }
        Ok(out)
    }

    pub fn rebuild_subject_views(
        &self,
        keys: &HashMap<SubjectId, [u8; 32]>,
    ) -> Result<(), DharmaError> {
        let subjects_root = self.subjects_root();
        if self.env.exists(&subjects_root) {
            self.env.remove_dir_all(&subjects_root)?;
        }
        self.env.create_dir_all(&subjects_root)?;

        for envelope_id in self.list_objects()? {
            let Some(bytes) = self.get_object_any(&envelope_id)? else {
                continue;
            };
            let Some(assertion) = decode_assertion(&bytes, keys) else {
                continue;
            };
            let assertion_id = assertion.assertion_id()?;
            let action = assertion
                .header
                .typ
                .strip_prefix("action.")
                .unwrap_or(&assertion.header.typ)
                .to_string();
            let plaintext = assertion.to_cbor()?;
            self.record_semantic(&assertion_id, &envelope_id)?;
            if is_overlay(&assertion.header) {
                state::append_overlay(
                    self.env.as_ref(),
                    &assertion.header.sub,
                    assertion.header.seq,
                    assertion_id,
                    envelope_id,
                    &action,
                    &plaintext,
                )?;
            } else {
                state::append_assertion(
                    self.env.as_ref(),
                    &assertion.header.sub,
                    assertion.header.seq,
                    assertion_id,
                    envelope_id,
                    &action,
                    &plaintext,
                )?;
            }
        }
        Ok(())
    }

    pub fn load_subject_objects(
        &self,
        subject: &SubjectId,
    ) -> Result<HashMap<AssertionId, Vec<u8>>, DharmaError> {
        let mut map = HashMap::new();
        for record in state::list_assertions(self.env.as_ref(), subject)? {
            map.insert(record.assertion_id, record.bytes);
        }
        Ok(map)
    }

    pub fn record_semantic(
        &self,
        assertion_id: &AssertionId,
        envelope_id: &EnvelopeId,
    ) -> Result<(), DharmaError> {
        let dir = self.indexes_dir();
        self.env.create_dir_all(&dir)?;
        let path = dir.join("semantic.idx");
        let mut buf = Vec::with_capacity(64);
        buf.extend_from_slice(assertion_id.as_bytes());
        buf.extend_from_slice(envelope_id.as_bytes());
        match self.env.append(&path, &buf) {
            Ok(()) => {}
            Err(err) if is_torn_write(&err) => {
                repair_semantic_index(self.env.as_ref(), &path)?;
                self.env.append(&path, &buf)?;
            }
            Err(err) => return Err(err),
        }
        Ok(())
    }

    pub fn lookup_envelope(&self, assertion_id: &AssertionId) -> Result<Option<EnvelopeId>, DharmaError> {
        let path = self.indexes_dir().join("semantic.idx");
        if !self.env.exists(&path) {
            return Ok(None);
        }
        let buf = self.env.read(&path)?;
        let usable_len = (buf.len() / 64) * 64;
        if usable_len == 0 {
            return Ok(None);
        }
        for chunk in buf[..usable_len].chunks_exact(64).rev() {
            if &chunk[..32] == assertion_id.as_bytes() {
                return Ok(Some(EnvelopeId::from_slice(&chunk[32..64])?));
            }
        }
        Ok(None)
    }
}

fn decode_assertion(
    bytes: &[u8],
    keys: &HashMap<SubjectId, [u8; 32]>,
) -> Option<AssertionPlaintext> {
    if let Ok(envelope) = AssertionEnvelope::from_cbor(bytes) {
        for key in keys.values() {
            if let Ok(plaintext) = crate::envelope::decrypt_assertion(&envelope, key) {
                if let Ok(assertion) = AssertionPlaintext::from_cbor(&plaintext) {
                    return Some(assertion);
                }
            }
        }
        return None;
    }
    AssertionPlaintext::from_cbor(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::envelope;
    use crate::types::Nonce12;
    use crate::envelope::AssertionEnvelope;
    use crate::store::state::append_assertion;
    use crate::types::{ContractId, IdentityKey, SchemaId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::{RngCore, SeedableRng};

    #[test]
    fn store_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let subject = SubjectId::from_bytes([1u8; 32]);
        let envelope = AssertionEnvelope::new(
            crypto::key_id_from_key(&[0u8; 32]),
            Nonce12::from_bytes([2u8; 12]),
            vec![1, 2, 3],
        );
        let bytes = envelope.to_cbor().unwrap();
        let envelope_id = envelope.envelope_id().unwrap();
        store.put_assertion(&subject, &envelope_id, &bytes).unwrap();
        let loaded = store.get_assertion(&subject, &envelope_id).unwrap();
        assert_eq!(bytes, loaded);
        let ids = store.list_objects().unwrap();
        assert!(ids.contains(&envelope_id));
    }

    #[test]
    fn scan_subject_empty() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let subject = SubjectId::from_bytes([2u8; 32]);
        let ids = store.scan_subject(&subject).unwrap();
        assert!(ids.is_empty());
    }

    #[test]
    fn list_subjects_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let subject = SubjectId::from_bytes([7u8; 32]);
        let mut rng = StdRng::seed_from_u64(4);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([2u8; 32]),
            contract: ContractId::from_bytes([3u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, Value::Null, &signing_key).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        let envelope_id = crypto::envelope_id(&bytes);
        store.put_assertion(&subject, &envelope_id, &bytes).unwrap();
        store.record_semantic(&assertion_id, &envelope_id).unwrap();
        append_assertion(
            store.env(),
            &subject,
            1,
            assertion_id,
            envelope_id,
            "Init",
            &bytes,
        )
        .unwrap();
        let subjects = store.list_subjects().unwrap();
        assert!(subjects.contains(&subject));
    }

    #[test]
    fn load_subject_objects_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let subject = SubjectId::from_bytes([3u8; 32]);
        let mut rng = StdRng::seed_from_u64(5);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([2u8; 32]),
            contract: ContractId::from_bytes([3u8; 32]),
            note: None,
            meta: None,
        };
        let assertion = AssertionPlaintext::sign(header, Value::Null, &signing_key).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        let envelope_id = crypto::envelope_id(&bytes);
        store.put_assertion(&subject, &envelope_id, &bytes).unwrap();
        store.record_semantic(&assertion_id, &envelope_id).unwrap();
        append_assertion(
            store.env(),
            &subject,
            1,
            assertion_id,
            envelope_id,
            "Init",
            &bytes,
        )
        .unwrap();
        let map = store.load_subject_objects(&subject).unwrap();
        assert_eq!(map.get(&assertion_id).unwrap(), &bytes);
    }

    #[test]
    fn rebuild_subject_views_creates_logs() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut rng = StdRng::seed_from_u64(42);
        let mut subject_key = [0u8; 32];
        rng.fill_bytes(&mut subject_key);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([9u8; 32]);

        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
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
        let envelope = envelope::encrypt_assertion(
            &plaintext,
            kid,
            &subject_key,
            Nonce12::from_bytes([1u8; 12]),
        )
        .unwrap();
        let envelope_id = envelope.envelope_id().unwrap();
        store.put_object(&envelope_id, &envelope.to_cbor().unwrap()).unwrap();

        let mut keys = HashMap::new();
        keys.insert(subject, subject_key);
        store.rebuild_subject_views(&keys).unwrap();

        let subjects = store.list_subjects().unwrap();
        assert!(subjects.contains(&subject));
        let env = crate::env::StdEnv::new(temp.path());
        let records = state::list_assertions(&env, &subject).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].envelope_id, envelope_id);
        let decoded = AssertionPlaintext::from_cbor(&records[0].bytes).unwrap();
        assert_eq!(decoded.header.seq, 1);
    }
}
