use crate::assertion::{AssertionHeader, AssertionPlaintext};
use crate::env::{Env, StdEnv};
use crate::error::DharmaError;
use crate::store::Store;
use crate::types::{AssertionId, EnvelopeId, SubjectId};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct FrontierIndex {
    env: Arc<dyn Env + Send + Sync>,
    tips: HashMap<SubjectId, HashMap<u64, BTreeMap<AssertionId, u64>>>,
    pending: HashSet<AssertionId>,
    log: Option<FrontierLog>,
}

impl Default for FrontierIndex {
    fn default() -> Self {
        Self {
            env: Arc::new(StdEnv::new(PathBuf::new())),
            tips: HashMap::new(),
            pending: HashSet::new(),
            log: None,
        }
    }
}

impl FrontierIndex {
    pub fn new<P: AsRef<Path>>(store_path: P) -> Result<Self, DharmaError> {
        let store = Store::from_root(store_path.as_ref());
        build_from_logs(&store)
    }

    pub fn build(
        store: &Store,
        keys: &HashMap<SubjectId, [u8; 32]>,
    ) -> Result<Self, DharmaError> {
        let _ = keys;
        build_from_logs(store)
    }

    pub fn get_tips(&self, subject: &SubjectId) -> Vec<AssertionId> {
        self.tips
            .get(subject)
            .map(|by_ver| {
                let mut out = Vec::new();
                for tips in by_ver.values() {
                    out.extend(tips.keys().copied());
                }
                out
            })
            .unwrap_or_default()
    }

    pub fn get_tips_for_ver(&self, subject: &SubjectId, ver: u64) -> Vec<AssertionId> {
        self.tips
            .get(subject)
            .and_then(|by_ver| by_ver.get(&ver))
            .map(|s| s.keys().copied().collect())
            .unwrap_or_default()
    }

    pub fn max_seq_for_subject(&self, subject: &SubjectId) -> Option<u64> {
        self.tips.get(subject).and_then(|by_ver| {
            by_ver
                .values()
                .filter_map(|tips| tips.values().copied().max())
                .max()
        })
    }

    pub fn max_seq_for_ver(&self, subject: &SubjectId, ver: u64) -> Option<u64> {
        self.tips
            .get(subject)
            .and_then(|by_ver| by_ver.get(&ver))
            .and_then(|tips| tips.values().copied().max())
    }

    pub fn tip_seq(&self, subject: &SubjectId, object_id: &AssertionId) -> Option<u64> {
        self.tips.get(subject).and_then(|by_ver| {
            for tips in by_ver.values() {
                if let Some(seq) = tips.get(object_id) {
                    return Some(*seq);
                }
            }
            None
        })
    }

    pub fn subjects(&self) -> Vec<SubjectId> {
        self.tips.keys().copied().collect()
    }

    pub fn update(&mut self, assertion_id: AssertionId, header: &AssertionHeader) -> Result<(), DharmaError> {
        let tips = self
            .tips
            .entry(header.sub)
            .or_default()
            .entry(header.ver)
            .or_default();
        if let Some(prev) = header.prev {
            tips.remove(&prev);
            if let Some(log) = &self.log {
                log.append(header.sub, header.ver, FrontierOp::Remove, prev, 0)?;
            }
        }
        if header.typ == "core.merge" {
            for ref_id in &header.refs {
                if tips.remove(ref_id).is_some() {
                    if let Some(log) = &self.log {
                        log.append(header.sub, header.ver, FrontierOp::Remove, *ref_id, 0)?;
                    }
                }
            }
        }
        tips.insert(assertion_id, header.seq);
        if let Some(log) = &self.log {
            log.append(
                header.sub,
                header.ver,
                FrontierOp::Add,
                assertion_id,
                header.seq,
            )?;
        }
        Ok(())
    }

    pub fn has_envelope(&self, envelope_id: &EnvelopeId) -> bool {
        let root = self.env.root();
        if root.as_os_str().is_empty() {
            return false;
        }
        let path = root.join("objects").join(format!("{}.obj", envelope_id.to_hex()));
        self.env.exists(&path)
    }

    pub fn mark_known(&mut self, assertion_id: AssertionId) {
        let _ = assertion_id;
    }

    pub fn mark_pending(&mut self, assertion_id: AssertionId) {
        self.pending.insert(assertion_id);
    }

    pub fn clear_pending(&mut self, assertion_id: &AssertionId) {
        self.pending.remove(assertion_id);
    }

    pub fn is_pending(&self, assertion_id: &AssertionId) -> bool {
        self.pending.contains(assertion_id)
    }

    pub fn pending_objects(&self) -> Vec<AssertionId> {
        self.pending.iter().copied().collect()
    }

    pub fn compact(&self) -> Result<(), DharmaError> {
        let Some(log) = &self.log else {
            return Ok(());
        };
        log.compact(&self.tips)
    }
}

fn build_from_logs(store: &Store) -> Result<FrontierIndex, DharmaError> {
    let env = store.env_arc();
    let log = FrontierLog::new(env.clone());
    let tips = log.replay()?;
    let tips = if tips.is_empty() {
        let snapshot = rebuild_from_subject_logs(store)?;
        log.compact(&snapshot.tips)?;
        snapshot.tips
    } else {
        tips
    };
    Ok(FrontierIndex {
        env,
        tips,
        pending: HashSet::new(),
        log: Some(log),
    })
}

#[derive(Debug)]
struct IndexSnapshot {
    tips: HashMap<SubjectId, HashMap<u64, BTreeMap<AssertionId, u64>>>,
}

fn rebuild_from_subject_logs(store: &Store) -> Result<IndexSnapshot, DharmaError> {
    let mut tips_map: HashMap<SubjectId, HashMap<u64, BTreeMap<AssertionId, u64>>> = HashMap::new();
    for subject in store.list_subjects()? {
        let mut per_ver: HashMap<u64, BTreeMap<AssertionId, u64>> = HashMap::new();
        for record in crate::store::state::list_assertions(store.env(), &subject)? {
            let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
                Ok(assertion) => assertion,
                Err(_) => continue,
            };
            let ver = assertion.header.ver;
            per_ver
                .entry(ver)
                .or_default()
                .insert(record.assertion_id, record.seq);
            if let Some(prev) = assertion.header.prev {
                if let Some(tips) = per_ver.get_mut(&ver) {
                    tips.remove(&prev);
                }
            }
            if assertion.header.typ == "core.merge" {
                for ref_id in &assertion.header.refs {
                    if let Some(tips) = per_ver.get_mut(&ver) {
                        tips.remove(ref_id);
                    }
                }
            }
        }
        per_ver.retain(|_, tips| !tips.is_empty());
        if !per_ver.is_empty() {
            tips_map.insert(subject, per_ver);
        }
    }
    Ok(IndexSnapshot {
        tips: tips_map,
    })
}

#[derive(Clone, Copy, Debug)]
enum FrontierOp {
    Add,
    Remove,
}

#[derive(Clone)]
struct FrontierLog {
    env: Arc<dyn Env + Send + Sync>,
    path: PathBuf,
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

fn repair_frontier_log(env: &dyn Env, path: &Path) -> Result<(), DharmaError> {
    if !env.exists(path) {
        return Ok(());
    }
    let buf = env.read(path)?;
    let entry_len = 32 + 8 + 1 + 8 + 32;
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

impl FrontierLog {
    fn new(env: Arc<dyn Env + Send + Sync>) -> Self {
        let path = env.root().join("indexes").join("frontier.log");
        Self { env, path }
    }

    fn append(
        &self,
        subject: SubjectId,
        ver: u64,
        op: FrontierOp,
        assertion_id: AssertionId,
        seq: u64,
    ) -> Result<(), DharmaError> {
        if let Some(parent) = self.path.parent() {
            self.env.create_dir_all(parent)?;
        }
        let op_code = match op {
            FrontierOp::Add => 1u8,
            FrontierOp::Remove => 2u8,
        };
        let mut buf = Vec::with_capacity(32 + 8 + 1 + 8 + 32);
        buf.extend_from_slice(subject.as_bytes());
        buf.extend_from_slice(&ver.to_le_bytes());
        buf.push(op_code);
        buf.extend_from_slice(&seq.to_le_bytes());
        buf.extend_from_slice(assertion_id.as_bytes());
        match self.env.append(&self.path, &buf) {
            Ok(()) => {}
            Err(err) if is_torn_write(&err) => {
                repair_frontier_log(self.env.as_ref(), &self.path)?;
                self.env.append(&self.path, &buf)?;
            }
            Err(err) => return Err(err),
        }
        Ok(())
    }

    fn replay(
        &self,
    ) -> Result<HashMap<SubjectId, HashMap<u64, BTreeMap<AssertionId, u64>>>, DharmaError> {
        if !self.env.exists(&self.path) {
            return Ok(HashMap::new());
        }
        let buf = self.env.read(&self.path)?;
        let entry_len = 32 + 8 + 1 + 8 + 32;
        let legacy_len = 32 + 8 + 1 + 32;
        if buf.len() % entry_len != 0 && buf.len() % legacy_len == 0 {
            return Ok(HashMap::new());
        }
        let usable_len = (buf.len() / entry_len) * entry_len;
        if usable_len == 0 {
            return Ok(HashMap::new());
        }
        let mut tips: HashMap<SubjectId, HashMap<u64, BTreeMap<AssertionId, u64>>> = HashMap::new();
        let mut offset = 0usize;
        let slice = &buf[..usable_len];
        while offset < slice.len() {
            let subject = match SubjectId::from_slice(&slice[offset..offset + 32]) {
                Ok(subject) => subject,
                Err(_) => return Ok(HashMap::new()),
            };
            offset += 32;
            let ver = u64::from_le_bytes(slice[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let op = slice[offset];
            offset += 1;
            let seq = u64::from_le_bytes(slice[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let assertion_id = match AssertionId::from_slice(&slice[offset..offset + 32]) {
                Ok(assertion_id) => assertion_id,
                Err(_) => return Ok(HashMap::new()),
            };
            offset += 32;
            let per_ver = tips.entry(subject).or_default();
            let set = per_ver.entry(ver).or_default();
            match op {
                1 => {
                    set.insert(assertion_id, seq);
                }
                2 => {
                    set.remove(&assertion_id);
                }
                _ => return Ok(HashMap::new()),
            }
        }
        tips.retain(|_, per_ver| {
            per_ver.retain(|_, set| !set.is_empty());
            !per_ver.is_empty()
        });
        Ok(tips)
    }

    fn compact(
        &self,
        tips: &HashMap<SubjectId, HashMap<u64, BTreeMap<AssertionId, u64>>>,
    ) -> Result<(), DharmaError> {
        if let Some(parent) = self.path.parent() {
            self.env.create_dir_all(parent)?;
        }
        let mut buf = Vec::new();
        for (subject, per_ver) in tips {
            for (ver, tips) in per_ver {
                for (assertion_id, seq) in tips {
                    buf.extend_from_slice(subject.as_bytes());
                    buf.extend_from_slice(&ver.to_le_bytes());
                    buf.push(1u8);
                    buf.extend_from_slice(&seq.to_le_bytes());
                    buf.extend_from_slice(assertion_id.as_bytes());
                }
            }
        }
        write_with_retry(self.env.as_ref(), &self.path, &buf)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use crate::crypto;
    use crate::types::{AssertionId, ContractId, SchemaId};
    use ciborium::value::Value;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn write_plain_assertion(
        store: &Store,
        subject: SubjectId,
        signing_key: &ed25519_dalek::SigningKey,
        seq: u64,
        prev: Option<AssertionId>,
    ) -> AssertionId {
        write_plain_assertion_with_type(store, subject, signing_key, seq, prev, "note.text")
    }

    fn write_plain_assertion_with_type(
        store: &Store,
        subject: SubjectId,
        signing_key: &ed25519_dalek::SigningKey,
        seq: u64,
        prev: Option<AssertionId>,
        typ: &str,
    ) -> AssertionId {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: typ.to_string(),
            auth: crate::types::IdentityKey::from_bytes(signing_key.verifying_key().to_bytes()),
            seq,
            prev,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let body = Value::Map(vec![(
            Value::Text("text".to_string()),
            Value::Text(format!("note {seq}")),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, signing_key).unwrap();
        let bytes = assertion.to_cbor().unwrap();
        let assertion_id = assertion.assertion_id().unwrap();
        let envelope_id = crypto::envelope_id(&bytes);
        store.put_assertion(&subject, &envelope_id, &bytes).unwrap();
        let action_name = typ.strip_prefix("action.").unwrap_or(typ);
        crate::store::state::append_assertion(
            store.env(),
            &subject,
            seq,
            assertion_id,
            envelope_id,
            action_name,
            &bytes,
        )
        .unwrap();
        assertion_id
    }

    #[test]
    fn new_builds_tips_from_plaintext() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut rng = StdRng::seed_from_u64(3);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([9u8; 32]);
        let first = write_plain_assertion(&store, subject, &signing_key, 1, None);
        let second =
            write_plain_assertion(&store, subject, &signing_key, 2, Some(first));

        let index = FrontierIndex::new(temp.path()).unwrap();
        let tips = index.get_tips(&subject);
        assert_eq!(tips.len(), 1);
        assert_eq!(tips[0], second);
        let objects = store.list_objects().unwrap();
        assert!(!objects.is_empty());
        assert!(index.has_envelope(&objects[0]));
    }

    #[test]
    fn new_includes_action_objects() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::from_root(temp.path());
        let mut rng = StdRng::seed_from_u64(12);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let subject = SubjectId::from_bytes([10u8; 32]);
        let action_id = write_plain_assertion_with_type(
            &store,
            subject,
            &signing_key,
            1,
            None,
            "action.Touch",
        );

        let index = FrontierIndex::new(temp.path()).unwrap();
        let tips = index.get_tips(&subject);
        assert_eq!(tips, vec![action_id]);
        let objects = store.list_objects().unwrap();
        assert_eq!(objects.len(), 1);
        assert!(index.has_envelope(&objects[0]));
    }

    #[test]
    fn update_replaces_prev_tip() {
        let mut index = FrontierIndex::default();
        let subject = SubjectId::from_bytes([1u8; 32]);
        let header1 = AssertionHeader {
            v: 1,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "note.text".to_string(),
            auth: crate::types::IdentityKey::from_bytes([2u8; 32]),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([3u8; 32]),
            contract: ContractId::from_bytes([4u8; 32]),
            note: None,
            meta: None,
        };
        let id1 = AssertionId::from_bytes([5u8; 32]);
        let id2 = AssertionId::from_bytes([7u8; 32]);
        let header2 = AssertionHeader {
            prev: Some(id1),
            seq: 2,
            ..header1.clone()
        };

        index.update(id1, &header1).unwrap();
        index.update(id2, &header2).unwrap();

        let tips = index.get_tips(&subject);
        assert_eq!(tips.len(), 1);
        assert_eq!(tips[0], id2);
    }

    #[test]
    fn update_merge_removes_branch_tips() {
        let mut index = FrontierIndex::default();
        let subject = SubjectId::from_bytes([2u8; 32]);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "action.Touch".to_string(),
            auth: crate::types::IdentityKey::from_bytes([9u8; 32]),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let base_id = AssertionId::from_bytes([10u8; 32]);
        let fork_a = AssertionId::from_bytes([11u8; 32]);
        let fork_b = AssertionId::from_bytes([12u8; 32]);
        let merge_id = AssertionId::from_bytes([13u8; 32]);

        index.update(base_id, &header).unwrap();
        let fork_header = AssertionHeader {
            seq: 2,
            prev: Some(base_id),
            ..header.clone()
        };
        index.update(fork_a, &fork_header).unwrap();
        index.update(fork_b, &fork_header).unwrap();
        let tips = index.get_tips_for_ver(&subject, DEFAULT_DATA_VERSION);
        assert_eq!(tips.len(), 2);

        let merge_header = AssertionHeader {
            seq: 3,
            prev: Some(fork_a),
            refs: vec![fork_a, fork_b],
            typ: "core.merge".to_string(),
            ..header
        };
        index.update(merge_id, &merge_header).unwrap();
        let tips = index.get_tips_for_ver(&subject, DEFAULT_DATA_VERSION);
        assert_eq!(tips, vec![merge_id]);
    }

    #[test]
    fn tips_track_versions_separately() {
        let mut index = FrontierIndex::default();
        let subject = SubjectId::from_bytes([2u8; 32]);
        let header_v1 = AssertionHeader {
            v: 1,
            ver: 1,
            sub: subject,
            typ: "note.text".to_string(),
            auth: crate::types::IdentityKey::from_bytes([3u8; 32]),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([4u8; 32]),
            contract: ContractId::from_bytes([5u8; 32]),
            note: None,
            meta: None,
        };
        let header_v2 = AssertionHeader {
            ver: 2,
            ..header_v1.clone()
        };
        let id_v1 = AssertionId::from_bytes([9u8; 32]);
        let id_v2 = AssertionId::from_bytes([10u8; 32]);
        index.update(id_v1, &header_v1).unwrap();
        index.update(id_v2, &header_v2).unwrap();
        let tips_all = index.get_tips(&subject);
        assert_eq!(tips_all.len(), 2);
        assert!(tips_all.contains(&id_v1));
        assert!(tips_all.contains(&id_v2));
        assert_eq!(index.get_tips_for_ver(&subject, 1), vec![id_v1]);
        assert_eq!(index.get_tips_for_ver(&subject, 2), vec![id_v2]);
    }

    #[test]
    fn frontier_log_replays_updates() {
        let temp = tempfile::tempdir().unwrap();
        let mut index = FrontierIndex::new(temp.path()).unwrap();
        let subject = SubjectId::from_bytes([11u8; 32]);
        let header_v1 = AssertionHeader {
            v: 1,
            ver: 1,
            sub: subject,
            typ: "note.text".to_string(),
            auth: crate::types::IdentityKey::from_bytes([3u8; 32]),
            seq: 1,
            prev: None,
            refs: Vec::new(),
            ts: None,
            schema: SchemaId::from_bytes([4u8; 32]),
            contract: ContractId::from_bytes([5u8; 32]),
            note: None,
            meta: None,
        };
        let header_v2 = AssertionHeader {
            ver: 2,
            seq: 1,
            ..header_v1.clone()
        };
        let id_v1 = AssertionId::from_bytes([9u8; 32]);
        let id_v2 = AssertionId::from_bytes([10u8; 32]);
        let id_v1_next = AssertionId::from_bytes([12u8; 32]);

        index.update(id_v1, &header_v1).unwrap();
        index.update(id_v2, &header_v2).unwrap();
        let header_v1_next = AssertionHeader {
            seq: 2,
            prev: Some(id_v1),
            ..header_v1.clone()
        };
        index.update(id_v1_next, &header_v1_next).unwrap();
        index.compact().unwrap();

        let index = FrontierIndex::new(temp.path()).unwrap();
        assert_eq!(index.get_tips_for_ver(&subject, 1), vec![id_v1_next]);
        assert_eq!(index.get_tips_for_ver(&subject, 2), vec![id_v2]);
        let tips_all = index.get_tips(&subject);
        assert_eq!(tips_all.len(), 2);
        assert!(tips_all.contains(&id_v1_next));
        assert!(tips_all.contains(&id_v2));
    }
}
