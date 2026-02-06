use crate::cbor;
use crate::crypto;
use crate::error::DharmaError;
use crate::types::{ContractId, IdentityKey, SchemaId, SubjectId};
use crate::value::{expect_array, expect_bytes, expect_map, map_get};
use chacha20poly1305::{aead::Aead, aead::KeyInit, XChaCha20Poly1305, XNonce};
use ciborium::value::Value;
use hkdf::Hkdf;
use rand_core::{CryptoRng, RngCore};
use sha2::Sha256;
use std::io::{Read, Write};

pub mod drivers;
pub mod archive;
pub mod runtime;

pub use archive::{archive_subject, VaultArchiveInput, VaultArchiveResult};
pub use runtime::{
    drain_archive_queue, enqueue_archive_job, maybe_archive_subject_with_config, VaultArchiveJob,
    VaultArchiveOutcome,
};

pub const DHBOX_MAGIC: &[u8; 5] = b"DHBOX";
pub const DHBOX_VERSION_V1: u8 = 1;

pub type VaultHash = [u8; 32];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VaultLocation {
    pub driver: String,
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VaultMeta {
    pub size: u64,
    pub hash: VaultHash,
}

pub trait VaultDriver: Send + Sync {
    fn put_chunk(&self, chunk: &DhboxChunk) -> Result<VaultLocation, DharmaError>;
    fn get_chunk(&self, location: &VaultLocation) -> Result<Vec<u8>, DharmaError>;
    fn head_chunk(&self, location: &VaultLocation) -> Result<VaultMeta, DharmaError>;
    fn list_chunks(&self, subject: &SubjectId) -> Result<Vec<VaultLocation>, DharmaError>;

    fn put_chunk_verified(
        &self,
        chunk: &DhboxChunk,
        svk: &[u8; 32],
        dict: Option<&VaultDictionary>,
    ) -> Result<VaultLocation, DharmaError> {
        let location = self.put_chunk(chunk)?;
        let bytes = self.get_chunk(&location)?;
        verify_chunk_against(chunk, &bytes, svk, dict)?;
        Ok(location)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressionAlg {
    Zstd19 = 1,
}

impl CompressionAlg {
    fn from_u8(value: u8) -> Result<Self, DharmaError> {
        match value {
            1 => Ok(CompressionAlg::Zstd19),
            _ => Err(DharmaError::Validation("unsupported compression".to_string())),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncryptionAlg {
    XChaCha20Poly1305 = 1,
}

impl EncryptionAlg {
    fn from_u8(value: u8) -> Result<Self, DharmaError> {
        match value {
            1 => Ok(EncryptionAlg::XChaCha20Poly1305),
            _ => Err(DharmaError::Validation("unsupported encryption".to_string())),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhboxHeaderV1 {
    pub subject_id: SubjectId,
    pub seq_start: u64,
    pub seq_end: u64,
    pub assertion_count: u32,
    pub schema_id: SchemaId,
    pub contract_id: ContractId,
    pub snapshot_hash: VaultHash,
    pub merkle_root: VaultHash,
    pub chunk_salt: VaultHash,
    pub dict_hash: Option<VaultHash>,
    pub dict_inline: Option<Vec<u8>>,
    pub compression: CompressionAlg,
    pub encryption: EncryptionAlg,
    pub nonce: [u8; 24],
}

impl DhboxHeaderV1 {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(DHBOX_MAGIC);
        out.push(DHBOX_VERSION_V1);
        out.extend_from_slice(self.subject_id.as_bytes());
        out.extend_from_slice(&self.seq_start.to_le_bytes());
        out.extend_from_slice(&self.seq_end.to_le_bytes());
        out.extend_from_slice(&self.assertion_count.to_le_bytes());
        out.extend_from_slice(self.schema_id.as_bytes());
        out.extend_from_slice(self.contract_id.as_bytes());
        out.extend_from_slice(&self.snapshot_hash);
        out.extend_from_slice(&self.merkle_root);
        out.extend_from_slice(&self.chunk_salt);
        match self.dict_hash {
            Some(hash) => {
                out.extend_from_slice(&hash);
                let len = self.dict_inline.as_ref().map(|d| d.len()).unwrap_or(0) as u32;
                out.extend_from_slice(&len.to_le_bytes());
                if let Some(bytes) = &self.dict_inline {
                    out.extend_from_slice(bytes);
                }
            }
            None => {
                out.extend_from_slice(&[0u8; 32]);
                out.extend_from_slice(&0u32.to_le_bytes());
            }
        }
        out.push(self.compression as u8);
        out.push(self.encryption as u8);
        out.extend_from_slice(&self.nonce);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), DharmaError> {
        let mut offset = 0usize;
        let magic = read_bytes::<5>(bytes, &mut offset)?;
        if &magic != DHBOX_MAGIC {
            return Err(DharmaError::Validation("invalid dhbox magic".to_string()));
        }
        let version = read_u8(bytes, &mut offset)?;
        if version != DHBOX_VERSION_V1 {
            return Err(DharmaError::Validation("unsupported dhbox version".to_string()));
        }
        let subject_id = SubjectId::from_slice(&read_bytes::<32>(bytes, &mut offset)?)?;
        let seq_start = read_u64(bytes, &mut offset)?;
        let seq_end = read_u64(bytes, &mut offset)?;
        let assertion_count = read_u32(bytes, &mut offset)?;
        let schema_id = SchemaId::from_slice(&read_bytes::<32>(bytes, &mut offset)?)?;
        let contract_id = ContractId::from_slice(&read_bytes::<32>(bytes, &mut offset)?)?;
        let snapshot_hash = read_bytes::<32>(bytes, &mut offset)?;
        let merkle_root = read_bytes::<32>(bytes, &mut offset)?;
        let chunk_salt = read_bytes::<32>(bytes, &mut offset)?;
        let dict_hash_raw = read_bytes::<32>(bytes, &mut offset)?;
        let dict_len = read_u32(bytes, &mut offset)? as usize;
        let dict_hash = if is_zero_hash(&dict_hash_raw) {
            None
        } else {
            Some(dict_hash_raw)
        };
        let dict_inline = if dict_len > 0 {
            let dict_bytes = read_vec(bytes, &mut offset, dict_len)?;
            let hash = blake3_hash(&dict_bytes);
            if dict_hash != Some(hash) {
                return Err(DharmaError::Validation("dict hash mismatch".to_string()));
            }
            Some(dict_bytes)
        } else {
            None
        };
        let compression = CompressionAlg::from_u8(read_u8(bytes, &mut offset)?)?;
        let encryption = EncryptionAlg::from_u8(read_u8(bytes, &mut offset)?)?;
        let nonce = read_bytes::<24>(bytes, &mut offset)?;
        let header = DhboxHeaderV1 {
            subject_id,
            seq_start,
            seq_end,
            assertion_count,
            schema_id,
            contract_id,
            snapshot_hash,
            merkle_root,
            chunk_salt,
            dict_hash,
            dict_inline,
            compression,
            encryption,
            nonce,
        };
        Ok((header, offset))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DhboxChunk {
    pub header: DhboxHeaderV1,
    pub ciphertext: Vec<u8>,
}

impl DhboxChunk {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = self.header.encode();
        out.extend_from_slice(&self.ciphertext);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DharmaError> {
        let (header, offset) = DhboxHeaderV1::decode(bytes)?;
        if bytes.len() <= offset {
            return Err(DharmaError::InvalidLength {
                expected: offset + 1,
                actual: bytes.len(),
            });
        }
        Ok(DhboxChunk {
            header,
            ciphertext: bytes[offset..].to_vec(),
        })
    }

    pub fn ciphertext_hash(&self) -> VaultHash {
        blake3_hash(&self.ciphertext)
    }

    pub fn decrypt_payload(
        &self,
        svk: &[u8; 32],
        dict: Option<&VaultDictionary>,
    ) -> Result<VaultPayload, DharmaError> {
        let dict_bytes: Option<&[u8]> = match (&self.header.dict_hash, &self.header.dict_inline) {
            (None, None) => None,
            (Some(expected), Some(bytes)) => {
                let hash = blake3_hash(bytes);
                if &hash != expected {
                    return Err(DharmaError::Validation("dict hash mismatch".to_string()));
                }
                Some(bytes.as_slice())
            }
            (Some(expected), None) => {
                let Some(dict) = dict else {
                    return Err(DharmaError::Validation("missing dictionary".to_string()));
                };
                if &dict.hash != expected {
                    return Err(DharmaError::Validation("dict hash mismatch".to_string()));
                }
                Some(dict.bytes.as_slice())
            }
            (None, Some(_)) => {
                return Err(DharmaError::Validation("dict inline without hash".to_string()));
            }
        };
        let aad = vault_aad(
            &self.header.subject_id,
            self.header.seq_start,
            self.header.seq_end,
            &self.header.schema_id,
            &self.header.contract_id,
        );
        let ck = VaultCrypto::derive_ck(
            svk,
            self.header.seq_start,
            self.header.seq_end,
            &self.header.chunk_salt,
        )?;
        let plaintext = VaultCrypto::decrypt(&ck, &self.header.nonce, &self.ciphertext, &aad)?;
        let decompressed = decompress_payload(&plaintext, dict_bytes)?;
        VaultPayload::from_cbor(&decompressed)
    }

    pub fn verify_payload(
        &self,
        svk: &[u8; 32],
        dict: Option<&VaultDictionary>,
    ) -> Result<VaultPayload, DharmaError> {
        if self.header.seq_end < self.header.seq_start {
            return Err(DharmaError::Validation("seq range invalid".to_string()));
        }
        let payload = self.decrypt_payload(svk, dict)?;
        if payload.assertions.is_empty() {
            return Err(DharmaError::Validation("empty vault payload".to_string()));
        }
        if payload.assertions.len() != self.header.assertion_count as usize {
            return Err(DharmaError::Validation(
                "assertion count mismatch".to_string(),
            ));
        }
        let snapshot_hash = blake3_hash(&payload.snapshot);
        if snapshot_hash != self.header.snapshot_hash {
            return Err(DharmaError::Validation("snapshot hash mismatch".to_string()));
        }
        let mut leaves = Vec::with_capacity(payload.assertions.len());
        for assertion in &payload.assertions {
            let id = crypto::envelope_id(assertion);
            leaves.push(*id.as_bytes());
        }
        let merkle_root = VaultMerkle::root(&leaves)?;
        if merkle_root != self.header.merkle_root {
            return Err(DharmaError::Validation("merkle root mismatch".to_string()));
        }
        Ok(payload)
    }
}

pub fn verify_chunk_against(
    expected: &DhboxChunk,
    bytes: &[u8],
    svk: &[u8; 32],
    dict: Option<&VaultDictionary>,
) -> Result<VaultPayload, DharmaError> {
    let chunk = DhboxChunk::from_bytes(bytes)?;
    if chunk.header != expected.header {
        return Err(DharmaError::Validation("chunk header mismatch".to_string()));
    }
    if chunk.ciphertext_hash() != expected.ciphertext_hash() {
        return Err(DharmaError::Validation(
            "ciphertext hash mismatch".to_string(),
        ));
    }
    chunk.verify_payload(svk, dict)
}

#[derive(Clone, Debug)]
pub struct VaultConfig {
    pub chunk_size_mb: usize,
    pub chunk_assertions: usize,
    pub dict_size: usize,
    pub compression_level: i32,
}

impl Default for VaultConfig {
    fn default() -> Self {
        Self {
            chunk_size_mb: 10,
            chunk_assertions: 10_000,
            dict_size: 32 * 1024,
            compression_level: 19,
        }
    }
}

impl VaultConfig {
    pub fn chunk_size_bytes(&self) -> usize {
        self.chunk_size_mb.saturating_mul(1024 * 1024)
    }
}

#[derive(Clone, Debug)]
pub struct VaultItem {
    pub seq: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct VaultSegment {
    pub subject_id: SubjectId,
    pub seq_start: u64,
    pub seq_end: u64,
    pub schema_id: SchemaId,
    pub contract_id: ContractId,
    pub assertions: Vec<VaultItem>,
    pub snapshot: Vec<u8>,
}

impl VaultSegment {
    pub fn new(
        subject_id: SubjectId,
        schema_id: SchemaId,
        contract_id: ContractId,
        assertions: Vec<VaultItem>,
        snapshot: Vec<u8>,
    ) -> Result<Self, DharmaError> {
        if assertions.is_empty() {
            return Err(DharmaError::Validation("empty vault segment".to_string()));
        }
        let seq_start = assertions.first().map(|a| a.seq).unwrap_or(0);
        let seq_end = assertions.last().map(|a| a.seq).unwrap_or(0);
        Ok(Self {
            subject_id,
            seq_start,
            seq_end,
            schema_id,
            contract_id,
            assertions,
            snapshot,
        })
    }

    pub fn seal<R: RngCore + CryptoRng>(
        &self,
        svk: &[u8; 32],
        dict: VaultDictionaryRef<'_>,
        rng: &mut R,
    ) -> Result<DhboxChunk, DharmaError> {
        let (dict_hash, dict_inline, dict_bytes) = dict_parts(dict);
        let payload = VaultPayload::new(&self.assertions, &self.snapshot)?;
        let payload_cbor = payload.to_cbor()?;
        let compressed = compress_payload(&payload_cbor, dict_bytes, 19)?;
        let mut nonce = [0u8; 24];
        rng.fill_bytes(&mut nonce);
        let mut chunk_salt = [0u8; 32];
        rng.fill_bytes(&mut chunk_salt);
        let ck = VaultCrypto::derive_ck(svk, self.seq_start, self.seq_end, &chunk_salt)?;
        let aad = vault_aad(
            &self.subject_id,
            self.seq_start,
            self.seq_end,
            &self.schema_id,
            &self.contract_id,
        );
        let ciphertext = VaultCrypto::encrypt(&ck, &nonce, &compressed, &aad)?;
        let snapshot_hash = blake3_hash(&self.snapshot);
        let merkle_root = VaultMerkle::root(&self.assertion_ids()?)?;
        let header = DhboxHeaderV1 {
            subject_id: self.subject_id,
            seq_start: self.seq_start,
            seq_end: self.seq_end,
            assertion_count: self.assertions.len() as u32,
            schema_id: self.schema_id,
            contract_id: self.contract_id,
            snapshot_hash,
            merkle_root,
            chunk_salt,
            dict_hash,
            dict_inline,
            compression: CompressionAlg::Zstd19,
            encryption: EncryptionAlg::XChaCha20Poly1305,
            nonce,
        };
        Ok(DhboxChunk { header, ciphertext })
    }

    fn assertion_ids(&self) -> Result<Vec<VaultHash>, DharmaError> {
        let mut ids = Vec::with_capacity(self.assertions.len());
        for assertion in &self.assertions {
            let id = crypto::envelope_id(&assertion.bytes);
            ids.push(*id.as_bytes());
        }
        Ok(ids)
    }
}

pub struct VaultSegmentBuilder {
    config: VaultConfig,
    subject_id: SubjectId,
    schema_id: SchemaId,
    contract_id: ContractId,
    assertions: Vec<VaultItem>,
    size_bytes: usize,
    last_seq: Option<u64>,
}

impl VaultSegmentBuilder {
    pub fn new(
        config: VaultConfig,
        subject_id: SubjectId,
        schema_id: SchemaId,
        contract_id: ContractId,
    ) -> Self {
        Self {
            config,
            subject_id,
            schema_id,
            contract_id,
            assertions: Vec::new(),
            size_bytes: 0,
            last_seq: None,
        }
    }

    pub fn push(&mut self, item: VaultItem, snapshot: Vec<u8>) -> Result<Option<VaultSegment>, DharmaError> {
        if let Some(last) = self.last_seq {
            if item.seq < last {
                return Err(DharmaError::Validation("vault items out of order".to_string()));
            }
        }
        self.size_bytes = self.size_bytes.saturating_add(item.bytes.len());
        self.last_seq = Some(item.seq);
        self.assertions.push(item);
        if self.assertions.len() >= self.config.chunk_assertions
            || self.size_bytes >= self.config.chunk_size_bytes()
        {
            let assertions = std::mem::take(&mut self.assertions);
            let segment = VaultSegment::new(
                self.subject_id,
                self.schema_id,
                self.contract_id,
                assertions,
                snapshot,
            )?;
            self.size_bytes = 0;
            self.last_seq = None;
            Ok(Some(segment))
        } else {
            Ok(None)
        }
    }

    pub fn finish(&mut self, snapshot: Vec<u8>) -> Result<Option<VaultSegment>, DharmaError> {
        if self.assertions.is_empty() {
            return Ok(None);
        }
        let assertions = std::mem::take(&mut self.assertions);
        self.size_bytes = 0;
        self.last_seq = None;
        let segment = VaultSegment::new(
            self.subject_id,
            self.schema_id,
            self.contract_id,
            assertions,
            snapshot,
        )?;
        Ok(Some(segment))
    }
}

pub struct VaultCrypto;

impl VaultCrypto {
    pub fn derive_vmk(root_key: &[u8; 32], identity_id: &IdentityKey) -> Result<[u8; 32], DharmaError> {
        let hk = Hkdf::<Sha256>::new(Some(identity_id.as_bytes()), root_key);
        let mut out = [0u8; 32];
        hk.expand(b"dharma:vault:master", &mut out)
            .map_err(|_| DharmaError::Kdf("hkdf vmk".to_string()))?;
        Ok(out)
    }

    pub fn derive_svk(vmk: &[u8; 32], subject_id: &SubjectId, epoch: u64) -> Result<[u8; 32], DharmaError> {
        let hk = Hkdf::<Sha256>::new(None, vmk);
        let mut info = Vec::with_capacity(32 + 8 + 22);
        info.extend_from_slice(b"dharma:vault:subject");
        info.extend_from_slice(subject_id.as_bytes());
        info.extend_from_slice(&epoch.to_le_bytes());
        let mut out = [0u8; 32];
        hk.expand(&info, &mut out)
            .map_err(|_| DharmaError::Kdf("hkdf svk".to_string()))?;
        Ok(out)
    }

    pub fn derive_ck(
        svk: &[u8; 32],
        seq_start: u64,
        seq_end: u64,
        chunk_salt: &[u8; 32],
    ) -> Result<[u8; 32], DharmaError> {
        let hk = Hkdf::<Sha256>::new(None, svk);
        let mut info = Vec::with_capacity(8 + 8 + 32 + 20);
        info.extend_from_slice(b"dharma:vault:chunk");
        info.extend_from_slice(&seq_start.to_le_bytes());
        info.extend_from_slice(&seq_end.to_le_bytes());
        info.extend_from_slice(chunk_salt);
        let mut out = [0u8; 32];
        hk.expand(&info, &mut out)
            .map_err(|_| DharmaError::Kdf("hkdf ck".to_string()))?;
        Ok(out)
    }

    pub fn encrypt(
        key: &[u8; 32],
        nonce: &[u8; 24],
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, DharmaError> {
        let cipher = XChaCha20Poly1305::new(key.into());
        Ok(cipher.encrypt(
            XNonce::from_slice(nonce),
            chacha20poly1305::aead::Payload { msg: plaintext, aad },
        )?)
    }

    pub fn decrypt(
        key: &[u8; 32],
        nonce: &[u8; 24],
        ciphertext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, DharmaError> {
        let cipher = XChaCha20Poly1305::new(key.into());
        Ok(cipher.decrypt(
            XNonce::from_slice(nonce),
            chacha20poly1305::aead::Payload { msg: ciphertext, aad },
        )?)
    }
}

pub struct VaultMerkle;

impl VaultMerkle {
    pub fn root(leaves: &[VaultHash]) -> Result<VaultHash, DharmaError> {
        if leaves.is_empty() {
            return Err(DharmaError::Validation("empty merkle tree".to_string()));
        }
        Ok(merkle_root(leaves))
    }

    pub fn proof(leaves: &[VaultHash], index: usize) -> Result<Vec<VaultHash>, DharmaError> {
        if leaves.is_empty() || index >= leaves.len() {
            return Err(DharmaError::Validation("invalid merkle index".to_string()));
        }
        Ok(merkle_proof(leaves, index))
    }

    pub fn verify(
        leaf: VaultHash,
        index: usize,
        total: usize,
        proof: &[VaultHash],
        root: &VaultHash,
    ) -> bool {
        merkle_verify(leaf, index, total, proof) == *root
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VaultDictionary {
    pub hash: VaultHash,
    pub bytes: Vec<u8>,
}

impl VaultDictionary {
    pub fn train(samples: &[Vec<u8>], dict_size: usize) -> Result<Self, DharmaError> {
        if samples.is_empty() {
            return Err(DharmaError::Validation("no samples for dictionary".to_string()));
        }
        let sample_refs: Vec<&[u8]> = samples.iter().map(|s| s.as_slice()).collect();
        let dict = zstd::dict::from_samples(&sample_refs, dict_size)?;
        let hash = blake3_hash(&dict);
        Ok(VaultDictionary { hash, bytes: dict })
    }
}

#[derive(Clone, Copy)]
pub enum VaultDictionaryRef<'a> {
    None,
    Inline(&'a VaultDictionary),
    Reference(&'a VaultDictionary),
}

fn dict_parts(dict: VaultDictionaryRef<'_>) -> (Option<VaultHash>, Option<Vec<u8>>, Option<&[u8]>) {
    match dict {
        VaultDictionaryRef::None => (None, None, None),
        VaultDictionaryRef::Inline(d) => (Some(d.hash), Some(d.bytes.clone()), Some(d.bytes.as_slice())),
        VaultDictionaryRef::Reference(d) => (Some(d.hash), None, Some(d.bytes.as_slice())),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VaultPayload {
    pub assertions: Vec<Vec<u8>>,
    pub snapshot: Vec<u8>,
}

impl VaultPayload {
    pub fn new(assertions: &[VaultItem], snapshot: &[u8]) -> Result<Self, DharmaError> {
        let mut out = Vec::with_capacity(assertions.len());
        for assertion in assertions {
            out.push(assertion.bytes.clone());
        }
        Ok(Self {
            assertions: out,
            snapshot: snapshot.to_vec(),
        })
    }

    pub fn to_cbor(&self) -> Result<Vec<u8>, DharmaError> {
        let assertions = Value::Array(
            self.assertions
                .iter()
                .map(|a| Value::Bytes(a.clone()))
                .collect(),
        );
        let snapshot = Value::Bytes(self.snapshot.clone());
        let value = Value::Map(vec![
            (Value::Text("assertions".to_string()), assertions),
            (Value::Text("snapshot".to_string()), snapshot),
        ]);
        cbor::encode_canonical_value(&value)
    }

    pub fn from_cbor(bytes: &[u8]) -> Result<Self, DharmaError> {
        let value = cbor::ensure_canonical(bytes)?;
        let map = expect_map(&value)?;
        let assertions_val = map_get(map, "assertions")
            .ok_or_else(|| DharmaError::Validation("missing assertions".to_string()))?;
        let snapshot_val = map_get(map, "snapshot")
            .ok_or_else(|| DharmaError::Validation("missing snapshot".to_string()))?;
        let assertions = expect_array(assertions_val)?
            .iter()
            .map(expect_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot = expect_bytes(snapshot_val)?;
        Ok(Self { assertions, snapshot })
    }
}

fn compress_payload(
    bytes: &[u8],
    dict: Option<&[u8]>,
    level: i32,
) -> Result<Vec<u8>, DharmaError> {
    if let Some(dict_bytes) = dict {
        let mut encoder = zstd::Encoder::with_dictionary(Vec::new(), level, dict_bytes)?;
        encoder.write_all(bytes)?;
        Ok(encoder.finish()?)
    } else {
        Ok(zstd::encode_all(bytes, level)?)
    }
}

fn decompress_payload(bytes: &[u8], dict: Option<&[u8]>) -> Result<Vec<u8>, DharmaError> {
    if let Some(dict_bytes) = dict {
        let mut decoder = zstd::Decoder::with_dictionary(bytes, dict_bytes)?;
        let mut out = Vec::new();
        decoder.read_to_end(&mut out)?;
        Ok(out)
    } else {
        Ok(zstd::decode_all(bytes)?)
    }
}

fn vault_aad(
    subject_id: &SubjectId,
    seq_start: u64,
    seq_end: u64,
    schema_id: &SchemaId,
    contract_id: &ContractId,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + 8 + 8 + 32 + 32);
    out.extend_from_slice(subject_id.as_bytes());
    out.extend_from_slice(&seq_start.to_le_bytes());
    out.extend_from_slice(&seq_end.to_le_bytes());
    out.extend_from_slice(schema_id.as_bytes());
    out.extend_from_slice(contract_id.as_bytes());
    out
}

fn blake3_hash(bytes: &[u8]) -> VaultHash {
    *blake3::hash(bytes).as_bytes()
}

fn is_zero_hash(hash: &[u8; 32]) -> bool {
    hash.iter().all(|b| *b == 0)
}

fn merkle_root(leaves: &[VaultHash]) -> VaultHash {
    let mut level = leaves.to_vec();
    while level.len() > 1 {
        let mut next = Vec::with_capacity((level.len() + 1) / 2);
        let mut idx = 0;
        while idx < level.len() {
            let left = level[idx];
            let right = if idx + 1 < level.len() {
                level[idx + 1]
            } else {
                left
            };
            next.push(merkle_parent(&left, &right));
            idx += 2;
        }
        level = next;
    }
    level[0]
}

fn merkle_parent(left: &VaultHash, right: &VaultHash) -> VaultHash {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(left);
    buf[32..].copy_from_slice(right);
    blake3_hash(&buf)
}

fn merkle_proof(leaves: &[VaultHash], index: usize) -> Vec<VaultHash> {
    let mut proof = Vec::new();
    let mut idx = index;
    let mut level = leaves.to_vec();
    while level.len() > 1 {
        let sibling_idx = if idx % 2 == 0 { idx + 1 } else { idx - 1 };
        let sibling = if sibling_idx < level.len() {
            level[sibling_idx]
        } else {
            level[idx]
        };
        proof.push(sibling);
        let mut next = Vec::with_capacity((level.len() + 1) / 2);
        let mut pos = 0;
        while pos < level.len() {
            let left = level[pos];
            let right = if pos + 1 < level.len() { level[pos + 1] } else { left };
            next.push(merkle_parent(&left, &right));
            pos += 2;
        }
        idx /= 2;
        level = next;
    }
    proof
}

fn merkle_verify(leaf: VaultHash, index: usize, total: usize, proof: &[VaultHash]) -> VaultHash {
    let mut idx = index;
    let mut current = leaf;
    let mut level_size = total;
    for sibling in proof {
        let (left, right) = if idx % 2 == 0 {
            (current, *sibling)
        } else {
            (*sibling, current)
        };
        current = merkle_parent(&left, &right);
        idx /= 2;
        level_size = (level_size + 1) / 2;
        if level_size == 0 {
            break;
        }
    }
    current
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> Result<u8, DharmaError> {
    if *offset + 1 > bytes.len() {
        return Err(DharmaError::InvalidLength {
            expected: *offset + 1,
            actual: bytes.len(),
        });
    }
    let value = bytes[*offset];
    *offset += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, DharmaError> {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(read_vec(bytes, offset, 4)?.as_slice());
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, DharmaError> {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(read_vec(bytes, offset, 8)?.as_slice());
    Ok(u64::from_le_bytes(buf))
}

fn read_bytes<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N], DharmaError> {
    let vec = read_vec(bytes, offset, N)?;
    let mut out = [0u8; N];
    out.copy_from_slice(&vec);
    Ok(out)
}

fn read_vec(bytes: &[u8], offset: &mut usize, len: usize) -> Result<Vec<u8>, DharmaError> {
    let end = offset.saturating_add(len);
    if end > bytes.len() {
        return Err(DharmaError::InvalidLength {
            expected: end,
            actual: bytes.len(),
        });
    }
    let out = bytes[*offset..end].to_vec();
    *offset = end;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn dummy_ids() -> (SubjectId, SchemaId, ContractId) {
        (
            SubjectId::from_bytes([1u8; 32]),
            SchemaId::from_bytes([2u8; 32]),
            ContractId::from_bytes([3u8; 32]),
        )
    }

    #[test]
    fn dhbox_header_roundtrip_v1() {
        let (subject_id, schema_id, contract_id) = dummy_ids();
        let dict_bytes = vec![1, 2, 3];
        let dict_hash = blake3_hash(&dict_bytes);
        let header = DhboxHeaderV1 {
            subject_id,
            seq_start: 1,
            seq_end: 3,
            assertion_count: 2,
            schema_id,
            contract_id,
            snapshot_hash: [9u8; 32],
            merkle_root: [8u8; 32],
            chunk_salt: [6u8; 32],
            dict_hash: Some(dict_hash),
            dict_inline: Some(dict_bytes),
            compression: CompressionAlg::Zstd19,
            encryption: EncryptionAlg::XChaCha20Poly1305,
            nonce: [4u8; 24],
        };
        let encoded = header.encode();
        let (decoded, used) = DhboxHeaderV1::decode(&encoded).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(header, decoded);
    }

    #[test]
    fn dhbox_ciphertext_hash_matches() {
        let (subject_id, schema_id, contract_id) = dummy_ids();
        let assertions = vec![
            VaultItem { seq: 1, bytes: b"a".to_vec() },
            VaultItem { seq: 2, bytes: b"b".to_vec() },
        ];
        let segment = VaultSegment::new(subject_id, schema_id, contract_id, assertions, b"snap".to_vec()).unwrap();
        let svk = [7u8; 32];
        let mut rng = StdRng::seed_from_u64(5);
        let chunk = segment.seal(&svk, VaultDictionaryRef::None, &mut rng).unwrap();
        let hash = chunk.ciphertext_hash();
        assert_eq!(hash, blake3_hash(&chunk.ciphertext));
    }

    #[test]
    fn dhbox_decrypt_fails_on_wrong_aad() {
        let (subject_id, schema_id, contract_id) = dummy_ids();
        let assertions = vec![VaultItem { seq: 1, bytes: b"a".to_vec() }];
        let segment = VaultSegment::new(subject_id, schema_id, contract_id, assertions, b"snap".to_vec()).unwrap();
        let svk = [9u8; 32];
        let mut rng = StdRng::seed_from_u64(9);
        let chunk = segment.seal(&svk, VaultDictionaryRef::None, &mut rng).unwrap();
        let mut tampered = chunk.clone();
        tampered.header.subject_id = SubjectId::from_bytes([9u8; 32]);
        let err = tampered.decrypt_payload(&svk, None).unwrap_err();
        match err {
            DharmaError::DecryptionFailed => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn dhbox_verify_payload_roundtrip() {
        let (subject_id, schema_id, contract_id) = dummy_ids();
        let assertions = vec![VaultItem { seq: 1, bytes: b"payload".to_vec() }];
        let segment = VaultSegment::new(subject_id, schema_id, contract_id, assertions, b"snap".to_vec()).unwrap();
        let svk = [3u8; 32];
        let mut rng = StdRng::seed_from_u64(11);
        let chunk = segment.seal(&svk, VaultDictionaryRef::None, &mut rng).unwrap();
        let payload = chunk.verify_payload(&svk, None).unwrap();
        assert_eq!(payload.snapshot, b"snap".to_vec());
        assert_eq!(payload.assertions.len(), 1);
        assert_eq!(payload.assertions[0], b"payload".to_vec());
    }

    #[test]
    fn dhbox_verify_payload_rejects_merkle_mismatch() {
        let (subject_id, schema_id, contract_id) = dummy_ids();
        let assertions = vec![VaultItem { seq: 1, bytes: b"keep".to_vec() }];
        let segment = VaultSegment::new(subject_id, schema_id, contract_id, assertions, b"snap".to_vec()).unwrap();
        let svk = [5u8; 32];
        let mut rng = StdRng::seed_from_u64(13);
        let mut chunk = segment.seal(&svk, VaultDictionaryRef::None, &mut rng).unwrap();

        let mut payload = chunk.decrypt_payload(&svk, None).unwrap();
        payload.assertions[0] = b"tampered".to_vec();
        let payload_cbor = payload.to_cbor().unwrap();
        let compressed = compress_payload(&payload_cbor, None, 19).unwrap();
        let aad = vault_aad(
            &chunk.header.subject_id,
            chunk.header.seq_start,
            chunk.header.seq_end,
            &chunk.header.schema_id,
            &chunk.header.contract_id,
        );
        let ck = VaultCrypto::derive_ck(&svk, chunk.header.seq_start, chunk.header.seq_end, &chunk.header.chunk_salt).unwrap();
        chunk.ciphertext = VaultCrypto::encrypt(&ck, &chunk.header.nonce, &compressed, &aad).unwrap();

        let err = chunk.verify_payload(&svk, None).unwrap_err();
        match err {
            DharmaError::Validation(reason) => assert!(reason.contains("merkle root mismatch")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn chunking_respects_size_and_count() {
        let (subject_id, schema_id, contract_id) = dummy_ids();
        let config = VaultConfig {
            chunk_size_mb: 1,
            chunk_assertions: 2,
            dict_size: 0,
            compression_level: 19,
        };
        let mut builder = VaultSegmentBuilder::new(config, subject_id, schema_id, contract_id);
        let snap = b"snapshot".to_vec();
        let out = builder.push(VaultItem { seq: 1, bytes: vec![1u8; 4] }, snap.clone()).unwrap();
        assert!(out.is_none());
        let out = builder.push(VaultItem { seq: 2, bytes: vec![2u8; 4] }, snap.clone()).unwrap();
        assert!(out.is_some());
    }

    #[test]
    fn dict_hash_mismatch_rejects_decode() {
        let (subject_id, schema_id, contract_id) = dummy_ids();
        let header = DhboxHeaderV1 {
            subject_id,
            seq_start: 1,
            seq_end: 1,
            assertion_count: 1,
            schema_id,
            contract_id,
            snapshot_hash: [1u8; 32],
            merkle_root: [2u8; 32],
            chunk_salt: [5u8; 32],
            dict_hash: Some([9u8; 32]),
            dict_inline: Some(vec![1, 2, 3]),
            compression: CompressionAlg::Zstd19,
            encryption: EncryptionAlg::XChaCha20Poly1305,
            nonce: [4u8; 24],
        };
        let encoded = header.encode();
        let err = DhboxHeaderV1::decode(&encoded).unwrap_err();
        match err {
            DharmaError::Validation(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn merkle_root_matches_leaves() {
        let leaves = vec![[1u8; 32], [2u8; 32], [3u8; 32]];
        let root = VaultMerkle::root(&leaves).unwrap();
        let manual = merkle_root(&leaves);
        assert_eq!(root, manual);
    }

    #[test]
    fn merkle_proof_verifies_leaf() {
        let leaves = vec![[1u8; 32], [2u8; 32], [3u8; 32], [4u8; 32]];
        let root = VaultMerkle::root(&leaves).unwrap();
        let proof = VaultMerkle::proof(&leaves, 2).unwrap();
        assert!(VaultMerkle::verify(leaves[2], 2, leaves.len(), &proof, &root));
    }
}
