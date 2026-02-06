use crate::assertion::{AssertionPlaintext, DEFAULT_DATA_VERSION};
use crate::cbor;
use crate::crypto;
use crate::env::Env;
use crate::error::DharmaError;
use crate::ownership::OwnershipRecord;
use crate::types::{AssertionId, EnvelopeId, KeyId, SubjectId};
use crate::value::{expect_bytes, expect_map, expect_uint, map_get};
use ciborium::value::Value;
use crc32fast::Hasher;
use std::path::{Path, PathBuf};

pub const STATE_SIZE: usize = 0x2000;

#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    pub header: SnapshotHeader,
    pub memory: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotHeader {
    pub seq: u64,
    pub ver: u64,
    pub last_assertion: AssertionId,
    pub timestamp: u64,
}

#[derive(Clone, Debug)]
pub struct AssertionRecord {
    pub seq: u64,
    pub assertion_id: AssertionId,
    pub envelope_id: EnvelopeId,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub seq: u64,
    pub ver: u64,
    pub assertion_id: AssertionId,
    pub envelope_id: EnvelopeId,
    pub prev: Option<AssertionId>,
    pub action: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestEntry {
    pub envelope_id: EnvelopeId,
    pub subject: Option<SubjectId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CqrsReverseEntry {
    pub envelope_id: EnvelopeId,
    pub assertion_id: AssertionId,
    pub subject: SubjectId,
    pub is_overlay: bool,
}

pub const CQRS_REVERSE_ENTRY_LEN: usize = 32 + 32 + 32 + 1;

pub fn subject_dir(env: &dyn Env, subject: &SubjectId) -> PathBuf {
    env.root().join("subjects").join(subject.to_hex())
}

pub fn assertions_dir(env: &dyn Env, subject: &SubjectId) -> PathBuf {
    subject_dir(env, subject).join("assertions")
}

pub fn overlays_dir(env: &dyn Env, subject: &SubjectId) -> PathBuf {
    subject_dir(env, subject).join("overlays")
}

pub fn snapshots_dir(env: &dyn Env, subject: &SubjectId) -> PathBuf {
    subject_dir(env, subject).join("snapshots")
}

pub fn indexes_dir(env: &dyn Env, subject: &SubjectId) -> PathBuf {
    subject_dir(env, subject).join("indexes")
}

pub fn ownership_path(env: &dyn Env, subject: &SubjectId) -> PathBuf {
    indexes_dir(env, subject).join("ownership.cbor")
}

pub fn epoch_path(env: &dyn Env, subject: &SubjectId) -> PathBuf {
    indexes_dir(env, subject).join("epoch.cbor")
}

pub fn key_bind_path(env: &dyn Env, subject: &SubjectId) -> PathBuf {
    indexes_dir(env, subject).join("key_bind.cbor")
}

pub fn load_ownership(
    env: &dyn Env,
    subject: &SubjectId,
) -> Result<Option<OwnershipRecord>, DharmaError> {
    let path = ownership_path(env, subject);
    if !env.exists(&path) {
        return Ok(None);
    }
    let bytes = env.read(&path)?;
    let value = cbor::ensure_canonical(&bytes)?;
    Ok(Some(OwnershipRecord::from_value(&value)?))
}

pub fn save_ownership(
    env: &dyn Env,
    subject: &SubjectId,
    record: &OwnershipRecord,
) -> Result<(), DharmaError> {
    let dir = indexes_dir(env, subject);
    env.create_dir_all(&dir)?;
    let bytes = cbor::encode_canonical_value(&record.to_value())?;
    write_with_retry(env, &ownership_path(env, subject), &bytes)
}

pub fn load_epoch(env: &dyn Env, subject: &SubjectId) -> Result<Option<u64>, DharmaError> {
    let path = epoch_path(env, subject);
    if !env.exists(&path) {
        return Ok(None);
    }
    let bytes = env.read(&path)?;
    let value = cbor::ensure_canonical(&bytes)?;
    let map = crate::value::expect_map(&value)?;
    let epoch = crate::value::expect_uint(
        crate::value::map_get(map, "epoch")
            .ok_or_else(|| DharmaError::Validation("missing epoch".to_string()))?,
    )?;
    Ok(Some(epoch))
}

pub fn save_epoch(env: &dyn Env, subject: &SubjectId, epoch: u64) -> Result<(), DharmaError> {
    let dir = indexes_dir(env, subject);
    env.create_dir_all(&dir)?;
    let value = Value::Map(vec![(
        Value::Text("epoch".to_string()),
        Value::Integer(epoch.into()),
    )]);
    let bytes = cbor::encode_canonical_value(&value)?;
    write_with_retry(env, &epoch_path(env, subject), &bytes)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyBindRecord {
    pub domain: SubjectId,
    pub epoch: u64,
    pub sdk_id: KeyId,
}

impl KeyBindRecord {
    pub fn to_value(&self) -> Value {
        Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Bytes(self.domain.as_bytes().to_vec()),
            ),
            (
                Value::Text("epoch".to_string()),
                Value::Integer(self.epoch.into()),
            ),
            (
                Value::Text("sdk_id".to_string()),
                Value::Bytes(self.sdk_id.as_bytes().to_vec()),
            ),
        ])
    }

    pub fn from_value(value: &Value) -> Result<Self, DharmaError> {
        let map = expect_map(value)?;
        let domain_bytes = expect_bytes(
            map_get(map, "domain")
                .ok_or_else(|| DharmaError::Validation("missing domain".to_string()))?,
        )?;
        let epoch = expect_uint(
            map_get(map, "epoch")
                .ok_or_else(|| DharmaError::Validation("missing epoch".to_string()))?,
        )?;
        let sdk_bytes = expect_bytes(
            map_get(map, "sdk_id")
                .ok_or_else(|| DharmaError::Validation("missing sdk_id".to_string()))?,
        )?;
        Ok(KeyBindRecord {
            domain: SubjectId::from_slice(&domain_bytes)?,
            epoch,
            sdk_id: KeyId::from_slice(&sdk_bytes)?,
        })
    }
}

pub fn load_key_bind(
    env: &dyn Env,
    subject: &SubjectId,
) -> Result<Option<KeyBindRecord>, DharmaError> {
    let path = key_bind_path(env, subject);
    if !env.exists(&path) {
        return Ok(None);
    }
    let bytes = env.read(&path)?;
    let value = cbor::ensure_canonical(&bytes)?;
    Ok(Some(KeyBindRecord::from_value(&value)?))
}

pub fn save_key_bind(
    env: &dyn Env,
    subject: &SubjectId,
    record: &KeyBindRecord,
) -> Result<(), DharmaError> {
    let dir = indexes_dir(env, subject);
    env.create_dir_all(&dir)?;
    let bytes = cbor::encode_canonical_value(&record.to_value())?;
    write_with_retry(env, &key_bind_path(env, subject), &bytes)
}

fn log_path(dir: &Path) -> PathBuf {
    dir.join("log.bin")
}

fn manifest_path(env: &dyn Env) -> PathBuf {
    env.root().join("indexes").join("global.idx")
}

fn cqrs_reverse_path(env: &dyn Env) -> PathBuf {
    env.root().join("indexes").join("cqrs_reverse_v1.idx")
}

fn ensure_subject_dirs(env: &dyn Env, subject: &SubjectId) -> Result<(), DharmaError> {
    let base = subject_dir(env, subject);
    env.create_dir_all(&base.join("assertions"))?;
    env.create_dir_all(&base.join("overlays"))?;
    env.create_dir_all(&base.join("snapshots"))?;
    env.create_dir_all(&base.join("indexes"))?;
    Ok(())
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

fn truncate_file(env: &dyn Env, path: &Path, size: usize) -> Result<(), DharmaError> {
    if size == 0 {
        if env.exists(path) {
            env.remove_file(path)?;
        }
        return Ok(());
    }
    let buf = env.read(path)?;
    let size = size.min(buf.len());
    write_with_retry(env, path, &buf[..size])
}

fn last_good_log_offset(buf: &[u8]) -> usize {
    let mut offset = 0usize;
    let mut last_good = 0usize;
    while offset < buf.len() {
        let entry_start = offset;
        if offset + 8 + 8 + 1 + 32 + 32 + 32 + 2 > buf.len() {
            break;
        }
        offset += 8; // seq
        offset += 8; // ver
        offset += 1; // prev flag
        offset += 32; // prev id
        offset += 32; // assertion id
        offset += 32; // envelope id
        let len = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;
        if offset + len > buf.len() {
            break;
        }
        offset += len;
        if offset + 4 > buf.len() {
            break;
        }
        let expected = u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        let mut hasher = Hasher::new();
        hasher.update(&buf[entry_start..offset]);
        if hasher.finalize() != expected {
            break;
        }
        offset += 4;
        last_good = offset;
    }
    last_good
}

fn last_good_manifest_offset(buf: &[u8]) -> usize {
    let mut offset = 0usize;
    let mut last_good = 0usize;
    while offset < buf.len() {
        if offset + 1 + 32 > buf.len() {
            break;
        }
        let flag = buf[offset];
        offset += 1;
        offset += 32; // envelope id
        if flag == 1 {
            if offset + 32 > buf.len() {
                break;
            }
            offset += 32; // subject id
        } else if flag != 0 {
            break;
        }
        last_good = offset;
    }
    last_good
}

fn repair_log_file(env: &dyn Env, path: &Path) -> Result<(), DharmaError> {
    if !env.exists(path) {
        return Ok(());
    }
    let buf = env.read(path)?;
    let last_good = last_good_log_offset(&buf);
    truncate_file(env, path, last_good)
}

fn repair_manifest_file(env: &dyn Env, path: &Path) -> Result<(), DharmaError> {
    if !env.exists(path) {
        return Ok(());
    }
    let buf = env.read(path)?;
    let last_good = last_good_manifest_offset(&buf);
    truncate_file(env, path, last_good)
}

fn repair_cqrs_reverse_file(env: &dyn Env, path: &Path) -> Result<(), DharmaError> {
    if !env.exists(path) {
        return Ok(());
    }
    let buf = env.read(path)?;
    let usable = (buf.len() / CQRS_REVERSE_ENTRY_LEN) * CQRS_REVERSE_ENTRY_LEN;
    truncate_file(env, path, usable)
}

fn append_log_entry(env: &dyn Env, dir: &Path, entry: &LogEntry) -> Result<(), DharmaError> {
    env.create_dir_all(dir)?;
    let path = log_path(dir);
    let action_bytes = entry.action.as_bytes();
    let len: u16 = action_bytes
        .len()
        .try_into()
        .map_err(|_| DharmaError::Validation("action too long".to_string()))?;
    let mut buf = Vec::with_capacity(8 + 8 + 1 + 32 + 32 + 32 + 2 + action_bytes.len() + 4);
    buf.extend_from_slice(&entry.seq.to_le_bytes());
    buf.extend_from_slice(&entry.ver.to_le_bytes());
    match entry.prev {
        Some(prev) => {
            buf.push(1u8);
            buf.extend_from_slice(prev.as_bytes());
        }
        None => {
            buf.push(0u8);
            buf.extend_from_slice(&[0u8; 32]);
        }
    }
    buf.extend_from_slice(entry.assertion_id.as_bytes());
    buf.extend_from_slice(entry.envelope_id.as_bytes());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(action_bytes);
    let mut hasher = Hasher::new();
    hasher.update(&buf);
    let crc = hasher.finalize();
    buf.extend_from_slice(&crc.to_le_bytes());
    match env.append(&path, &buf) {
        Ok(()) => {}
        Err(err) if is_torn_write(&err) => {
            repair_log_file(env, &path)?;
            env.append(&path, &buf)?;
        }
        Err(err) => return Err(err),
    }
    Ok(())
}

fn read_log_entries(env: &dyn Env, dir: &Path) -> Result<Vec<LogEntry>, DharmaError> {
    let path = log_path(dir);
    if !env.exists(&path) {
        return Ok(Vec::new());
    }
    let buf = env.read(&path)?;
    let mut entries = Vec::new();
    let mut offset = 0usize;
    while offset < buf.len() {
        let entry_start = offset;
        if offset + 8 + 8 + 1 + 32 + 32 + 32 + 2 > buf.len() {
            return Err(DharmaError::Validation("truncated log entry".to_string()));
        }
        let seq = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let ver = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let prev_flag = buf[offset];
        offset += 1;
        let prev_bytes = &buf[offset..offset + 32];
        offset += 32;
        let prev = if prev_flag == 1 {
            Some(AssertionId::from_slice(prev_bytes)?)
        } else {
            None
        };
        let assertion_id = AssertionId::from_slice(&buf[offset..offset + 32])?;
        offset += 32;
        let envelope_id = EnvelopeId::from_slice(&buf[offset..offset + 32])?;
        offset += 32;
        let len = u16::from_le_bytes(buf[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if offset + len > buf.len() {
            return Err(DharmaError::Validation("truncated log entry".to_string()));
        }
        let action = String::from_utf8(buf[offset..offset + len].to_vec())
            .map_err(|_| DharmaError::Validation("invalid log action".to_string()))?;
        offset += len;
        if offset + 4 > buf.len() {
            return Err(DharmaError::Validation(
                "truncated log entry checksum".to_string(),
            ));
        }
        let expected = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
        let mut hasher = Hasher::new();
        hasher.update(&buf[entry_start..offset]);
        let actual = hasher.finalize();
        if actual != expected {
            return Err(DharmaError::Validation(
                "log entry checksum mismatch".to_string(),
            ));
        }
        offset += 4;
        entries.push(LogEntry {
            seq,
            ver,
            assertion_id,
            envelope_id,
            prev,
            action,
        });
    }
    Ok(entries)
}

fn read_cbor_file_with_retry(
    env: &dyn Env,
    path: &Path,
    attempts: usize,
) -> Result<Vec<u8>, DharmaError> {
    let mut last_err: Option<DharmaError> = None;
    for _ in 0..attempts {
        let bytes = env.read(path)?;
        if cbor::ensure_canonical(&bytes).is_ok() {
            return Ok(bytes);
        }
        last_err = Some(DharmaError::Cbor("corrupt cbor".to_string()));
    }
    Err(last_err.unwrap_or_else(|| DharmaError::Cbor("corrupt cbor".to_string())))
}

pub fn read_assertion_log(
    env: &dyn Env,
    subject: &SubjectId,
) -> Result<Vec<LogEntry>, DharmaError> {
    read_log_entries(env, &assertions_dir(env, subject))
}

pub fn read_overlay_log(env: &dyn Env, subject: &SubjectId) -> Result<Vec<LogEntry>, DharmaError> {
    read_log_entries(env, &overlays_dir(env, subject))
}

pub fn append_manifest(
    env: &dyn Env,
    envelope_id: &EnvelopeId,
    subject: Option<&SubjectId>,
) -> Result<(), DharmaError> {
    let dir = env.root().join("indexes");
    env.create_dir_all(&dir)?;
    let path = manifest_path(env);
    let mut buf = Vec::with_capacity(1 + 32 + 32);
    match subject {
        Some(subject) => {
            buf.push(1u8);
            buf.extend_from_slice(envelope_id.as_bytes());
            buf.extend_from_slice(subject.as_bytes());
        }
        None => {
            buf.push(0u8);
            buf.extend_from_slice(envelope_id.as_bytes());
        }
    }
    match env.append(&path, &buf) {
        Ok(()) => {}
        Err(err) if is_torn_write(&err) => {
            repair_manifest_file(env, &path)?;
            env.append(&path, &buf)?;
        }
        Err(err) => return Err(err),
    }
    Ok(())
}

pub fn append_cqrs_reverse_entry(
    env: &dyn Env,
    entry: &CqrsReverseEntry,
) -> Result<(), DharmaError> {
    let dir = env.root().join("indexes");
    env.create_dir_all(&dir)?;
    let path = cqrs_reverse_path(env);
    let mut buf = Vec::with_capacity(CQRS_REVERSE_ENTRY_LEN);
    buf.extend_from_slice(entry.envelope_id.as_bytes());
    buf.extend_from_slice(entry.assertion_id.as_bytes());
    buf.extend_from_slice(entry.subject.as_bytes());
    buf.push(if entry.is_overlay { 1u8 } else { 0u8 });
    match env.append(&path, &buf) {
        Ok(()) => {}
        Err(err) if is_torn_write(&err) => {
            repair_cqrs_reverse_file(env, &path)?;
            env.append(&path, &buf)?;
        }
        Err(err) => return Err(err),
    }
    Ok(())
}

pub fn read_cqrs_reverse_entries(env: &dyn Env) -> Result<Vec<CqrsReverseEntry>, DharmaError> {
    let path = cqrs_reverse_path(env);
    if !env.exists(&path) {
        return Ok(Vec::new());
    }
    let buf = env.read(&path)?;
    let usable_len = (buf.len() / CQRS_REVERSE_ENTRY_LEN) * CQRS_REVERSE_ENTRY_LEN;
    if usable_len == 0 {
        return Ok(Vec::new());
    }
    decode_cqrs_reverse_entries(&buf[..usable_len], 0)
}

pub fn read_cqrs_reverse_entries_since(
    env: &dyn Env,
    offset: u64,
) -> Result<Vec<CqrsReverseEntry>, DharmaError> {
    let path = cqrs_reverse_path(env);
    if !env.exists(&path) {
        return Ok(Vec::new());
    }
    let buf = env.read(&path)?;
    let usable_len = (buf.len() / CQRS_REVERSE_ENTRY_LEN) * CQRS_REVERSE_ENTRY_LEN;
    if usable_len == 0 {
        return Ok(Vec::new());
    }
    let offset = usize::try_from(offset)
        .map_err(|_| DharmaError::Validation("cqrs reverse offset overflow".to_string()))?;
    if offset > usable_len {
        return Ok(Vec::new());
    }
    if offset % CQRS_REVERSE_ENTRY_LEN != 0 {
        return Err(DharmaError::Validation(
            "cqrs reverse offset misaligned".to_string(),
        ));
    }
    decode_cqrs_reverse_entries(&buf[offset..usable_len], offset)
}

fn decode_cqrs_reverse_entries(
    buf: &[u8],
    base_offset: usize,
) -> Result<Vec<CqrsReverseEntry>, DharmaError> {
    let mut entries = Vec::with_capacity(buf.len() / CQRS_REVERSE_ENTRY_LEN);
    for (index, chunk) in buf.chunks_exact(CQRS_REVERSE_ENTRY_LEN).enumerate() {
        let is_overlay = match chunk[96] {
            0 => false,
            1 => true,
            flag => {
                let byte_offset = base_offset + (index * CQRS_REVERSE_ENTRY_LEN) + 96;
                return Err(DharmaError::Validation(format!(
                    "invalid cqrs reverse overlay flag {flag} at byte {byte_offset}"
                )));
            }
        };
        entries.push(CqrsReverseEntry {
            envelope_id: EnvelopeId::from_slice(&chunk[0..32])?,
            assertion_id: AssertionId::from_slice(&chunk[32..64])?,
            subject: SubjectId::from_slice(&chunk[64..96])?,
            is_overlay,
        });
    }
    Ok(entries)
}

pub fn read_manifest(env: &dyn Env) -> Result<Vec<ManifestEntry>, DharmaError> {
    let path = manifest_path(env);
    if !env.exists(&path) {
        return Ok(Vec::new());
    }
    let buf = env.read(&path)?;
    let mut entries = Vec::new();
    let mut offset = 0usize;
    while offset < buf.len() {
        if offset + 1 + 32 > buf.len() {
            break;
        }
        let flag = buf[offset];
        offset += 1;
        let envelope_id = EnvelopeId::from_slice(&buf[offset..offset + 32])?;
        offset += 32;
        let subject = if flag == 1 {
            if offset + 32 > buf.len() {
                break;
            }
            let subject_id = SubjectId::from_slice(&buf[offset..offset + 32])?;
            offset += 32;
            Some(subject_id)
        } else if flag == 0 {
            None
        } else {
            break;
        };
        entries.push(ManifestEntry {
            envelope_id,
            subject,
        });
    }
    Ok(entries)
}

pub fn append_assertion(
    env: &dyn Env,
    subject: &SubjectId,
    seq: u64,
    assertion_id: AssertionId,
    envelope_id: EnvelopeId,
    action: &str,
    bytes: &[u8],
) -> Result<PathBuf, DharmaError> {
    ensure_subject_dirs(env, subject)?;
    let dir = assertions_dir(env, subject);
    let safe_action = action.replace('/', "_");
    let filename = format!(
        "{:04}_{}_{}.dharma",
        seq,
        safe_action,
        assertion_id.to_hex()
    );
    let path = dir.join(filename);
    if env.exists(&path) {
        return Ok(path);
    }
    write_with_retry(env, &path, bytes)?;
    let assertion = AssertionPlaintext::from_cbor(bytes)?;
    let entry = LogEntry {
        seq,
        ver: assertion.header.ver,
        assertion_id,
        envelope_id,
        prev: assertion.header.prev,
        action: action.to_string(),
    };
    append_log_entry(env, &dir, &entry)?;
    append_cqrs_reverse_entry(
        env,
        &CqrsReverseEntry {
            envelope_id,
            assertion_id,
            subject: *subject,
            is_overlay: false,
        },
    )?;
    append_manifest(env, &envelope_id, Some(subject))?;
    Ok(path)
}

pub fn append_overlay(
    env: &dyn Env,
    subject: &SubjectId,
    seq: u64,
    assertion_id: AssertionId,
    envelope_id: EnvelopeId,
    action: &str,
    bytes: &[u8],
) -> Result<PathBuf, DharmaError> {
    ensure_subject_dirs(env, subject)?;
    let dir = overlays_dir(env, subject);
    let safe_action = action.replace('/', "_");
    let filename = format!(
        "{:04}_{}_{}.dharma",
        seq,
        safe_action,
        assertion_id.to_hex()
    );
    let path = dir.join(filename);
    if env.exists(&path) {
        return Ok(path);
    }
    write_with_retry(env, &path, bytes)?;
    let assertion = AssertionPlaintext::from_cbor(bytes)?;
    let entry = LogEntry {
        seq,
        ver: assertion.header.ver,
        assertion_id,
        envelope_id,
        prev: assertion.header.prev,
        action: action.to_string(),
    };
    append_log_entry(env, &dir, &entry)?;
    append_cqrs_reverse_entry(
        env,
        &CqrsReverseEntry {
            envelope_id,
            assertion_id,
            subject: *subject,
            is_overlay: true,
        },
    )?;
    append_manifest(env, &envelope_id, Some(subject))?;
    Ok(path)
}

pub fn list_assertions(
    env: &dyn Env,
    subject: &SubjectId,
) -> Result<Vec<AssertionRecord>, DharmaError> {
    let dir = assertions_dir(env, subject);
    if !env.exists(&dir) {
        return Ok(Vec::new());
    }
    let mut log_entries = match read_log_entries(env, &dir) {
        Ok(entries) => entries,
        Err(_) => Vec::new(),
    };
    if !log_entries.is_empty() {
        log_entries.sort_by(|a, b| {
            a.seq
                .cmp(&b.seq)
                .then_with(|| a.assertion_id.as_bytes().cmp(b.assertion_id.as_bytes()))
        });
        let mut records = Vec::new();
        for entry in log_entries {
            let safe_action = entry.action.replace('/', "_");
            let filename = format!(
                "{:04}_{}_{}.dharma",
                entry.seq,
                safe_action,
                entry.assertion_id.to_hex()
            );
            let path = dir.join(filename);
            let bytes = match read_cbor_file_with_retry(env, &path, 3) {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            records.push(AssertionRecord {
                seq: entry.seq,
                assertion_id: entry.assertion_id,
                envelope_id: entry.envelope_id,
                bytes,
            });
        }
        return Ok(records);
    }
    let mut records = Vec::new();
    for path in env.list_dir(&dir)? {
        if !env.is_file(&path) {
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) != Some("dharma") {
            continue;
        }
        let Some(name) = path.file_name() else {
            continue;
        };
        let name = name.to_string_lossy();
        let seq = match parse_seq(&name) {
            Ok(seq) => seq,
            Err(_) => continue,
        };
        let assertion_id = match parse_assertion_id(&name) {
            Ok(id) => id,
            Err(_) => continue,
        };
        let bytes = match read_cbor_file_with_retry(env, &path, 3) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let envelope_id = crypto::envelope_id(&bytes);
        records.push(AssertionRecord {
            seq,
            assertion_id,
            envelope_id,
            bytes,
        });
    }
    records.sort_by_key(|r| r.seq);
    Ok(records)
}

pub fn subject_has_facts(env: &dyn Env, subject: &SubjectId) -> Result<bool, DharmaError> {
    let dir = assertions_dir(env, subject);
    if !env.exists(&dir) {
        return Ok(false);
    }
    if let Ok(entries) = read_log_entries(env, &dir) {
        if !entries.is_empty() {
            return Ok(true);
        }
    }
    for path in env.list_dir(&dir)? {
        if !env.is_file(&path) {
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) != Some("dharma") {
            continue;
        }
        return Ok(true);
    }
    Ok(false)
}

pub fn list_overlays(
    env: &dyn Env,
    subject: &SubjectId,
) -> Result<Vec<AssertionRecord>, DharmaError> {
    let dir = overlays_dir(env, subject);
    if !env.exists(&dir) {
        return Ok(Vec::new());
    }
    let mut log_entries = match read_log_entries(env, &dir) {
        Ok(entries) => entries,
        Err(_) => Vec::new(),
    };
    if !log_entries.is_empty() {
        log_entries.sort_by(|a, b| {
            a.seq
                .cmp(&b.seq)
                .then_with(|| a.assertion_id.as_bytes().cmp(b.assertion_id.as_bytes()))
        });
        let mut records = Vec::new();
        for entry in log_entries {
            let safe_action = entry.action.replace('/', "_");
            let filename = format!(
                "{:04}_{}_{}.dharma",
                entry.seq,
                safe_action,
                entry.assertion_id.to_hex()
            );
            let path = dir.join(filename);
            let bytes = match read_cbor_file_with_retry(env, &path, 3) {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            records.push(AssertionRecord {
                seq: entry.seq,
                assertion_id: entry.assertion_id,
                envelope_id: entry.envelope_id,
                bytes,
            });
        }
        return Ok(records);
    }
    let mut records = Vec::new();
    for path in env.list_dir(&dir)? {
        if !env.is_file(&path) {
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) != Some("dharma") {
            continue;
        }
        let Some(name) = path.file_name() else {
            continue;
        };
        let name = name.to_string_lossy();
        let seq = match parse_seq(&name) {
            Ok(seq) => seq,
            Err(_) => continue,
        };
        let assertion_id = match parse_assertion_id(&name) {
            Ok(id) => id,
            Err(_) => continue,
        };
        let bytes = match read_cbor_file_with_retry(env, &path, 3) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let envelope_id = crypto::envelope_id(&bytes);
        records.push(AssertionRecord {
            seq,
            assertion_id,
            envelope_id,
            bytes,
        });
    }
    records.sort_by_key(|r| r.seq);
    Ok(records)
}

pub fn find_assertion_by_id(
    env: &dyn Env,
    subject: &SubjectId,
    assertion_id: &AssertionId,
) -> Result<Option<Vec<u8>>, DharmaError> {
    for record in list_assertions(env, subject)? {
        if &record.assertion_id == assertion_id {
            return Ok(Some(record.bytes));
        }
    }
    Ok(None)
}

pub fn find_assertion_by_seq(
    env: &dyn Env,
    subject: &SubjectId,
    seq: u64,
) -> Result<Option<Vec<u8>>, DharmaError> {
    for record in list_assertions(env, subject)? {
        if record.seq == seq {
            return Ok(Some(record.bytes));
        }
    }
    Ok(None)
}

pub fn find_overlay_by_id(
    env: &dyn Env,
    subject: &SubjectId,
    assertion_id: &AssertionId,
) -> Result<Option<Vec<u8>>, DharmaError> {
    for record in list_overlays(env, subject)? {
        if &record.assertion_id == assertion_id {
            return Ok(Some(record.bytes));
        }
    }
    Ok(None)
}

pub fn overlays_for_ref(
    env: &dyn Env,
    subject: &SubjectId,
    base_id: &AssertionId,
) -> Result<Vec<Vec<u8>>, DharmaError> {
    let mut out = Vec::new();
    for record in list_overlays(env, subject)? {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(assertion) => assertion,
            Err(_) => continue,
        };
        if assertion.header.refs.iter().any(|id| id == base_id) {
            out.push(record.bytes);
        }
    }
    Ok(out)
}

pub fn load_latest_snapshot(
    env: &dyn Env,
    subject: &SubjectId,
) -> Result<Option<Snapshot>, DharmaError> {
    load_latest_snapshot_for_ver(env, subject, DEFAULT_DATA_VERSION)
}

pub fn load_latest_snapshot_for_ver(
    env: &dyn Env,
    subject: &SubjectId,
    ver: u64,
) -> Result<Option<Snapshot>, DharmaError> {
    let dir = snapshots_dir(env, subject);
    if !env.exists(&dir) {
        return Ok(None);
    }
    let mut best: Option<(u64, PathBuf)> = None;
    for path in env.list_dir(&dir)? {
        if !env.is_file(&path) {
            continue;
        }
        let Some(name) = path.file_name() else {
            continue;
        };
        let name = name.to_string_lossy();
        if parse_snapshot_ver(&name)
            .map(|file_ver| file_ver != ver)
            .unwrap_or(false)
        {
            continue;
        }
        if let Ok(seq) = parse_seq(&name) {
            if best.as_ref().map(|(s, _)| seq > *s).unwrap_or(true) {
                best = Some((seq, path));
            }
        }
    }
    let Some((_, path)) = best else {
        return Ok(None);
    };
    let bytes = env.read(&path)?;
    let snapshot = decode_snapshot(&bytes)?;
    if snapshot.header.ver != ver {
        return Ok(None);
    }
    Ok(Some(snapshot))
}

pub fn load_snapshot_at_or_before_seq(
    env: &dyn Env,
    subject: &SubjectId,
    ver: u64,
    seq: u64,
) -> Result<Option<Snapshot>, DharmaError> {
    let dir = snapshots_dir(env, subject);
    if !env.exists(&dir) {
        return Ok(None);
    }
    let mut best: Option<(u64, PathBuf)> = None;
    for path in env.list_dir(&dir)? {
        if !env.is_file(&path) {
            continue;
        }
        let Some(name) = path.file_name() else {
            continue;
        };
        let name = name.to_string_lossy();
        if parse_snapshot_ver(&name)
            .map(|file_ver| file_ver != ver)
            .unwrap_or(false)
        {
            continue;
        }
        if let Ok(snap_seq) = parse_seq(&name) {
            if snap_seq > seq {
                continue;
            }
            if best
                .as_ref()
                .map(|(best_seq, _)| snap_seq > *best_seq)
                .unwrap_or(true)
            {
                best = Some((snap_seq, path));
            }
        }
    }
    let Some((_, path)) = best else {
        return Ok(None);
    };
    let bytes = env.read(&path)?;
    let snapshot = decode_snapshot(&bytes)?;
    if snapshot.header.ver != ver {
        return Ok(None);
    }
    Ok(Some(snapshot))
}

pub fn save_snapshot(
    env: &dyn Env,
    subject: &SubjectId,
    snapshot: &Snapshot,
) -> Result<PathBuf, DharmaError> {
    ensure_subject_dirs(env, subject)?;
    let dir = snapshots_dir(env, subject);
    let filename = format!(
        "{:04}_v{}_{}.state",
        snapshot.header.seq,
        snapshot.header.ver,
        snapshot.header.last_assertion.to_hex()
    );
    let path = dir.join(filename);
    let bytes = encode_snapshot(snapshot)?;
    env.write(&path, &bytes)?;
    Ok(path)
}

fn encode_snapshot(snapshot: &Snapshot) -> Result<Vec<u8>, DharmaError> {
    let header = Value::Map(vec![
        (
            Value::Text("seq".to_string()),
            Value::Integer((snapshot.header.seq as u64).into()),
        ),
        (
            Value::Text("ver".to_string()),
            Value::Integer((snapshot.header.ver as u64).into()),
        ),
        (
            Value::Text("last_assertion".to_string()),
            Value::Bytes(snapshot.header.last_assertion.as_bytes().to_vec()),
        ),
        (
            Value::Text("timestamp".to_string()),
            Value::Integer((snapshot.header.timestamp as u64).into()),
        ),
    ]);
    let map = Value::Map(vec![
        (Value::Text("header".to_string()), header),
        (
            Value::Text("memory".to_string()),
            Value::Bytes(snapshot.memory.clone()),
        ),
    ]);
    cbor::encode_canonical_value(&map)
}

fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot, DharmaError> {
    let value = cbor::ensure_canonical(bytes)?;
    let map = crate::value::expect_map(&value)?;
    let header_val = crate::value::map_get(map, "header")
        .ok_or_else(|| DharmaError::Validation("missing header".to_string()))?;
    let memory_val = crate::value::map_get(map, "memory")
        .ok_or_else(|| DharmaError::Validation("missing memory".to_string()))?;

    let header_map = crate::value::expect_map(header_val)?;
    let seq = crate::value::expect_uint(
        crate::value::map_get(header_map, "seq")
            .ok_or_else(|| DharmaError::Validation("missing seq".to_string()))?,
    )?;
    let ver = match crate::value::map_get(header_map, "ver") {
        Some(value) => crate::value::expect_uint(value)?,
        None => DEFAULT_DATA_VERSION,
    };
    let last_bytes = crate::value::expect_bytes(
        crate::value::map_get(header_map, "last_assertion")
            .ok_or_else(|| DharmaError::Validation("missing last_assertion".to_string()))?,
    )?;
    let timestamp = crate::value::expect_uint(
        crate::value::map_get(header_map, "timestamp")
            .ok_or_else(|| DharmaError::Validation("missing timestamp".to_string()))?,
    )?;

    let memory = crate::value::expect_bytes(memory_val)?;
    Ok(Snapshot {
        header: SnapshotHeader {
            seq,
            ver,
            last_assertion: AssertionId::from_slice(&last_bytes)?,
            timestamp,
        },
        memory,
    })
}

fn parse_seq(name: &str) -> Result<u64, DharmaError> {
    let mut digits = String::new();
    for ch in name.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
        } else {
            break;
        }
    }
    if digits.is_empty() {
        return Err(DharmaError::Validation("missing seq".to_string()));
    }
    digits
        .parse::<u64>()
        .map_err(|_| DharmaError::Validation("invalid seq".to_string()))
}

fn parse_assertion_id(name: &str) -> Result<AssertionId, DharmaError> {
    let stem = name.strip_suffix(".dharma").unwrap_or(name);
    let hex = stem
        .rsplit('_')
        .next()
        .ok_or_else(|| DharmaError::Validation("missing object id".to_string()))?;
    AssertionId::from_hex(hex)
}

fn parse_snapshot_ver(name: &str) -> Result<u64, DharmaError> {
    let stem = name.strip_suffix(".state").unwrap_or(name);
    let mut parts = stem.split('_');
    let _seq = parts
        .next()
        .ok_or_else(|| DharmaError::Validation("missing snapshot seq".to_string()))?;
    let ver_part = parts
        .next()
        .ok_or_else(|| DharmaError::Validation("missing snapshot ver".to_string()))?;
    let ver_str = ver_part.strip_prefix('v').unwrap_or(ver_part);
    ver_str
        .parse::<u64>()
        .map_err(|_| DharmaError::Validation("invalid snapshot ver".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::DEFAULT_DATA_VERSION;
    use crate::assertion::{AssertionHeader, AssertionPlaintext};
    use crate::crypto;
    use crate::env::Fs;
    use crate::ownership::{Owner, OwnershipRecord};
    use crate::types::{AssertionId, ContractId, IdentityKey, SchemaId};
    use ciborium::value::Value;
    use rand::SeedableRng;
    #[test]
    fn snapshot_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let subject = SubjectId::from_bytes([1u8; 32]);
        let snapshot = Snapshot {
            header: SnapshotHeader {
                seq: 50,
                ver: DEFAULT_DATA_VERSION,
                last_assertion: AssertionId::from_bytes([2u8; 32]),
                timestamp: 123,
            },
            memory: vec![0xaa; STATE_SIZE],
        };
        save_snapshot(&env, &subject, &snapshot).unwrap();
        let loaded = load_latest_snapshot(&env, &subject).unwrap().unwrap();
        assert_eq!(loaded.header.seq, 50);
        assert_eq!(loaded.memory.len(), STATE_SIZE);
    }

    #[test]
    fn list_assertions_orders_by_seq() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let subject = SubjectId::from_bytes([3u8; 32]);
        let mut rng = rand::rngs::StdRng::seed_from_u64(9);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let header1 = AssertionHeader {
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
        let header2 = AssertionHeader {
            seq: 2,
            ..header1.clone()
        };
        let bytes1 = AssertionPlaintext::sign(header1, Value::Null, &signing_key)
            .unwrap()
            .to_cbor()
            .unwrap();
        let bytes2 = AssertionPlaintext::sign(header2, Value::Null, &signing_key)
            .unwrap()
            .to_cbor()
            .unwrap();
        let id1 = AssertionId::from_bytes([4u8; 32]);
        let id2 = AssertionId::from_bytes([5u8; 32]);
        let env1 = crypto::envelope_id(&bytes1);
        let env2 = crypto::envelope_id(&bytes2);
        append_assertion(&env, &subject, 2, id2, env2, "Second", &bytes2).unwrap();
        append_assertion(&env, &subject, 1, id1, env1, "First", &bytes1).unwrap();
        let records = list_assertions(&env, &subject).unwrap();
        assert_eq!(records[0].seq, 1);
        assert_eq!(records[1].seq, 2);
        assert!(indexes_dir(&env, &subject).exists());
    }

    #[test]
    fn list_overlays_orders_by_seq() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let subject = SubjectId::from_bytes([6u8; 32]);
        let mut rng = rand::rngs::StdRng::seed_from_u64(11);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let header1 = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "action.Touch".to_string(),
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
        let header2 = AssertionHeader {
            seq: 2,
            ..header1.clone()
        };
        let bytes1 = AssertionPlaintext::sign(header1, Value::Null, &signing_key)
            .unwrap()
            .to_cbor()
            .unwrap();
        let bytes2 = AssertionPlaintext::sign(header2, Value::Null, &signing_key)
            .unwrap()
            .to_cbor()
            .unwrap();
        let id1 = AssertionId::from_bytes([7u8; 32]);
        let id2 = AssertionId::from_bytes([8u8; 32]);
        let env1 = crypto::envelope_id(&bytes1);
        let env2 = crypto::envelope_id(&bytes2);
        append_overlay(&env, &subject, 2, id2, env2, "Second", &bytes2).unwrap();
        append_overlay(&env, &subject, 1, id1, env1, "First", &bytes1).unwrap();
        let records = list_overlays(&env, &subject).unwrap();
        assert_eq!(records[0].seq, 1);
        assert_eq!(records[1].seq, 2);
    }

    #[test]
    fn cqrs_reverse_entries_include_assertions_and_overlays() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let subject = SubjectId::from_bytes([66u8; 32]);
        let mut rng = rand::rngs::StdRng::seed_from_u64(808);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "action.Touch".to_string(),
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
        let assertion_bytes = AssertionPlaintext::sign(header.clone(), Value::Null, &signing_key)
            .unwrap()
            .to_cbor()
            .unwrap();
        let overlay_header = AssertionHeader {
            seq: 2,
            prev: Some(AssertionId::from_bytes([3u8; 32])),
            ..header
        };
        let overlay_bytes = AssertionPlaintext::sign(overlay_header, Value::Null, &signing_key)
            .unwrap()
            .to_cbor()
            .unwrap();

        let assertion_id = AssertionId::from_bytes([3u8; 32]);
        let assertion_env = crypto::envelope_id(&assertion_bytes);
        append_assertion(
            &env,
            &subject,
            1,
            assertion_id,
            assertion_env,
            "Touch",
            &assertion_bytes,
        )
        .unwrap();

        let overlay_id = AssertionId::from_bytes([4u8; 32]);
        let overlay_env = crypto::envelope_id(&overlay_bytes);
        append_overlay(
            &env,
            &subject,
            2,
            overlay_id,
            overlay_env,
            "TouchOverlay",
            &overlay_bytes,
        )
        .unwrap();

        let entries = read_cqrs_reverse_entries(&env).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|entry| {
            entry.envelope_id == assertion_env
                && entry.assertion_id == assertion_id
                && entry.subject == subject
                && !entry.is_overlay
        }));
        assert!(entries.iter().any(|entry| {
            entry.envelope_id == overlay_env
                && entry.assertion_id == overlay_id
                && entry.subject == subject
                && entry.is_overlay
        }));
    }

    #[test]
    fn cqrs_reverse_entries_since_reads_delta() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let subject = SubjectId::from_bytes([67u8; 32]);
        let mut rng = rand::rngs::StdRng::seed_from_u64(809);
        let (signing_key, _) = crypto::generate_identity_keypair(&mut rng);
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "action.Touch".to_string(),
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
        let base_bytes = AssertionPlaintext::sign(header.clone(), Value::Null, &signing_key)
            .unwrap()
            .to_cbor()
            .unwrap();
        let overlay_header = AssertionHeader {
            seq: 2,
            prev: Some(AssertionId::from_bytes([5u8; 32])),
            ..header
        };
        let overlay_bytes = AssertionPlaintext::sign(overlay_header, Value::Null, &signing_key)
            .unwrap()
            .to_cbor()
            .unwrap();

        let base_id = AssertionId::from_bytes([5u8; 32]);
        let base_env = crypto::envelope_id(&base_bytes);
        append_assertion(&env, &subject, 1, base_id, base_env, "Touch", &base_bytes).unwrap();

        let overlay_id = AssertionId::from_bytes([6u8; 32]);
        let overlay_env = crypto::envelope_id(&overlay_bytes);
        append_overlay(
            &env,
            &subject,
            2,
            overlay_id,
            overlay_env,
            "TouchOverlay",
            &overlay_bytes,
        )
        .unwrap();

        let delta = read_cqrs_reverse_entries_since(&env, CQRS_REVERSE_ENTRY_LEN as u64).unwrap();
        assert_eq!(delta.len(), 1);
        assert_eq!(delta[0].assertion_id, overlay_id);
        assert!(delta[0].is_overlay);
    }

    #[test]
    fn cqrs_reverse_entries_reject_invalid_overlay_flag() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let indexes = temp.path().join("indexes");
        env.create_dir_all(&indexes).unwrap();
        let path = indexes.join("cqrs_reverse_v1.idx");
        let mut raw = vec![0u8; CQRS_REVERSE_ENTRY_LEN];
        raw[96] = 2;
        env.write(&path, &raw).unwrap();

        let err = read_cqrs_reverse_entries(&env).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("invalid cqrs reverse overlay flag"));
    }

    #[test]
    fn log_entries_checksum_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let subject = SubjectId::from_bytes([9u8; 32]);
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
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
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let bytes = AssertionPlaintext::sign(header, Value::Null, &signing_key)
            .unwrap()
            .to_cbor()
            .unwrap();
        let assertion_id = AssertionId::from_bytes([10u8; 32]);
        let envelope_id = crypto::envelope_id(&bytes);
        append_assertion(
            &env,
            &subject,
            1,
            assertion_id,
            envelope_id,
            "note.text",
            &bytes,
        )
        .unwrap();
        let entries = read_log_entries(&env, &assertions_dir(&env, &subject)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].seq, 1);
    }

    #[test]
    fn log_entries_checksum_detects_corruption() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let subject = SubjectId::from_bytes([11u8; 32]);
        let mut rng = rand::rngs::StdRng::seed_from_u64(77);
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
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let bytes = AssertionPlaintext::sign(header, Value::Null, &signing_key)
            .unwrap()
            .to_cbor()
            .unwrap();
        let assertion_id = AssertionId::from_bytes([12u8; 32]);
        let envelope_id = crypto::envelope_id(&bytes);
        append_assertion(
            &env,
            &subject,
            1,
            assertion_id,
            envelope_id,
            "note.text",
            &bytes,
        )
        .unwrap();
        let dir = assertions_dir(&env, &subject);
        let log_path = log_path(&dir);
        let mut log_bytes = env.read(&log_path).unwrap();
        if let Some(byte) = log_bytes.get_mut(0) {
            *byte ^= 0x01;
        }
        env.write(&log_path, &log_bytes).unwrap();
        let err = read_log_entries(&env, &dir).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("checksum"));
    }

    #[test]
    fn ownership_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let env = crate::env::StdEnv::new(temp.path());
        let subject = SubjectId::from_bytes([12u8; 32]);
        let record = OwnershipRecord {
            owner: Owner::Identity(IdentityKey::from_bytes([1u8; 32])),
            creator: IdentityKey::from_bytes([2u8; 32]),
            acting_domain: Some(SubjectId::from_bytes([3u8; 32])),
            role: Some("admin".to_string()),
        };
        save_ownership(&env, &subject, &record).unwrap();
        let loaded = load_ownership(&env, &subject).unwrap().unwrap();
        assert_eq!(loaded, record);
    }
}
