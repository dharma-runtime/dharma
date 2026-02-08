use crate::assertion::{add_signer_meta, AssertionHeader, AssertionPlaintext};
use crate::crypto;
use crate::error::DharmaError;
use crate::keys::Keyring;
use crate::net::ingest::{ingest_object, IngestStatus};
use crate::store::index::FrontierIndex;
use crate::store::state::{list_assertions, load_latest_snapshot_for_ver};
use crate::store::Store;
use crate::types::{AssertionId, ContractId, EnvelopeId, IdentityKey, SchemaId, SubjectId};
use crate::vault::{
    CompressionAlg, DhboxChunk, EncryptionAlg, VaultConfig, VaultDictionary, VaultDictionaryRef,
    VaultDriver, VaultHash, VaultItem, VaultLocation, VaultSegmentBuilder, DHBOX_VERSION_V1,
};
use ciborium::value::Value;
use rand_core::OsRng;

pub struct VaultArchiveInput<'a> {
    pub subject: SubjectId,
    pub schema_id: SchemaId,
    pub contract_id: ContractId,
    pub checkpoint_schema: SchemaId,
    pub checkpoint_contract: ContractId,
    pub ver: u64,
    pub signer_subject: SubjectId,
    pub signer_key: IdentityKey,
    pub signing_key: &'a ed25519_dalek::SigningKey,
    pub svk: [u8; 32],
    pub dict: VaultDictionaryRef<'a>,
    pub driver: &'a dyn VaultDriver,
    pub config: VaultConfig,
}

pub struct VaultArchiveResult {
    pub chunk: DhboxChunk,
    pub location: VaultLocation,
    pub checkpoint_id: AssertionId,
    pub checkpoint_envelope: EnvelopeId,
}

pub fn archive_subject(
    store: &Store,
    input: VaultArchiveInput<'_>,
) -> Result<Vec<VaultArchiveResult>, DharmaError> {
    let env = store.env();
    let mut index = FrontierIndex::new(env.root())?;
    let mut keys = Keyring::new();

    let last_checkpoint = last_checkpoint_end(store, &input.subject, &input.checkpoint_schema)?;
    let snapshot = load_latest_snapshot_for_ver(env, &input.subject, input.ver)?
        .map(|snap| snap.memory)
        .unwrap_or_default();

    let mut builder = VaultSegmentBuilder::new(
        input.config,
        input.subject,
        input.schema_id,
        input.contract_id,
    );
    let mut segments = Vec::new();
    for record in list_assertions(env, &input.subject)? {
        if record.seq <= last_checkpoint {
            continue;
        }
        let assertion = AssertionPlaintext::from_cbor(&record.bytes)?;
        if assertion.header.ver != input.ver {
            continue;
        }
        if assertion.header.schema != input.schema_id
            || assertion.header.contract != input.contract_id
        {
            continue;
        }
        let item = VaultItem {
            seq: record.seq,
            bytes: record.bytes.clone(),
        };
        if let Some(segment) = builder.push(item, snapshot.clone())? {
            segments.push(segment);
        }
    }
    if let Some(segment) = builder.finish(snapshot.clone())? {
        segments.push(segment);
    }
    if segments.is_empty() {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();
    let tips = index.get_tips_for_ver(&input.subject, input.ver);
    if tips.len() > 1 {
        return Err(DharmaError::Validation(
            "fork detected; merge required".to_string(),
        ));
    }
    let mut prev = tips.into_iter().next();
    let mut seq = prev
        .and_then(|id| index.tip_seq(&input.subject, &id))
        .unwrap_or(0);

    let dict_opt = dict_option(input.dict);
    for segment in segments {
        let mut rng = OsRng;
        let chunk = segment.seal(&input.svk, input.dict, &mut rng)?;
        let location = input
            .driver
            .put_chunk_verified(&chunk, &input.svk, dict_opt)?;
        let checkpoint = build_checkpoint_assertion(
            &chunk,
            &location,
            input.checkpoint_schema,
            input.checkpoint_contract,
            input.ver,
            input.signer_subject,
            input.signer_key,
            input.signing_key,
            seq + 1,
            prev,
            &snapshot,
        )?;
        let checkpoint_bytes = checkpoint.to_cbor()?;
        let checkpoint_id = checkpoint.assertion_id()?;
        let checkpoint_env = crypto::envelope_id(&checkpoint_bytes);
        match ingest_object(store, &mut index, &checkpoint_bytes, &mut keys) {
            Ok(IngestStatus::Accepted(_)) => {}
            Ok(IngestStatus::Pending(_, reason)) => {
                return Err(DharmaError::Validation(format!(
                    "checkpoint pending: {reason}"
                )));
            }
            Err(err) => return Err(DharmaError::Validation(format!("{err:?}"))),
        }
        seq += 1;
        prev = Some(checkpoint_id);
        results.push(VaultArchiveResult {
            chunk,
            location,
            checkpoint_id,
            checkpoint_envelope: checkpoint_env,
        });
    }
    Ok(results)
}

fn build_checkpoint_assertion(
    chunk: &DhboxChunk,
    location: &VaultLocation,
    schema_id: SchemaId,
    contract_id: ContractId,
    ver: u64,
    signer_subject: SubjectId,
    signer_key: IdentityKey,
    signing_key: &ed25519_dalek::SigningKey,
    seq: u64,
    prev: Option<AssertionId>,
    snapshot: &[u8],
) -> Result<AssertionPlaintext, DharmaError> {
    let driver_name = driver_enum_name(&location.driver)?;
    let compression = compression_name(chunk.header.compression);
    let encryption = encryption_name(chunk.header.encryption);
    let dict_hash = chunk
        .header
        .dict_hash
        .map(|h| Value::Bytes(h.to_vec()))
        .unwrap_or(Value::Null);
    let dict_size = chunk
        .header
        .dict_inline
        .as_ref()
        .map(|bytes| Value::Integer((bytes.len() as u64).into()))
        .unwrap_or(Value::Null);
    let state_root = snapshot_hash(snapshot);

    let vault = Value::Map(vec![
        (Value::Text("driver".to_string()), Value::Text(driver_name)),
        (
            Value::Text("location".to_string()),
            Value::Text(location.path.clone()),
        ),
        (
            Value::Text("hash".to_string()),
            Value::Bytes(chunk.ciphertext_hash().to_vec()),
        ),
        (
            Value::Text("size".to_string()),
            Value::Integer((chunk.to_bytes().len() as u64).into()),
        ),
        (
            Value::Text("compression".to_string()),
            Value::Text(compression.to_string()),
        ),
        (
            Value::Text("encryption".to_string()),
            Value::Text(encryption.to_string()),
        ),
        (
            Value::Text("format_version".to_string()),
            Value::Integer((DHBOX_VERSION_V1 as u64).into()),
        ),
        (
            Value::Text("subject".to_string()),
            Value::Bytes(chunk.header.subject_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("seq_start".to_string()),
            Value::Integer(chunk.header.seq_start.into()),
        ),
        (
            Value::Text("seq_end".to_string()),
            Value::Integer(chunk.header.seq_end.into()),
        ),
        (
            Value::Text("snapshot_hash".to_string()),
            Value::Bytes(chunk.header.snapshot_hash.to_vec()),
        ),
        (
            Value::Text("merkle_root".to_string()),
            Value::Bytes(chunk.header.merkle_root.to_vec()),
        ),
        (Value::Text("dict_hash".to_string()), dict_hash),
        (Value::Text("dict_size".to_string()), dict_size),
        (Value::Text("shards".to_string()), Value::Null),
    ]);

    let body = Value::Map(vec![
        (
            Value::Text("start_seq".to_string()),
            Value::Integer(chunk.header.seq_start.into()),
        ),
        (
            Value::Text("end_seq".to_string()),
            Value::Integer(chunk.header.seq_end.into()),
        ),
        (
            Value::Text("state_root".to_string()),
            Value::Bytes(state_root.to_vec()),
        ),
        (Value::Text("vault".to_string()), vault),
    ]);

    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver,
        sub: chunk.header.subject_id,
        typ: "action.Checkpoint".to_string(),
        auth: signer_key,
        seq,
        prev,
        refs: Vec::new(),
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &signer_subject),
    };
    AssertionPlaintext::sign(header, body, signing_key)
}

fn snapshot_hash(snapshot: &[u8]) -> VaultHash {
    *blake3::hash(snapshot).as_bytes()
}

fn last_checkpoint_end(
    store: &Store,
    subject: &SubjectId,
    checkpoint_schema: &SchemaId,
) -> Result<u64, DharmaError> {
    let mut last_end = 0u64;
    for record in list_assertions(store.env(), subject)? {
        let assertion = AssertionPlaintext::from_cbor(&record.bytes)?;
        if &assertion.header.schema != checkpoint_schema {
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
        let map = crate::value::expect_map(&assertion.body)?;
        let end_val = crate::value::map_get(map, "end_seq")
            .ok_or_else(|| DharmaError::Validation("missing end_seq".to_string()))?;
        let end_seq = crate::value::expect_uint(end_val)?;
        if end_seq > last_end {
            last_end = end_seq;
        }
    }
    Ok(last_end)
}

fn dict_option(dict: VaultDictionaryRef<'_>) -> Option<&VaultDictionary> {
    match dict {
        VaultDictionaryRef::None => None,
        VaultDictionaryRef::Inline(dict) => Some(dict),
        VaultDictionaryRef::Reference(dict) => Some(dict),
    }
}

fn driver_enum_name(driver: &str) -> Result<String, DharmaError> {
    let name = match driver.to_lowercase().as_str() {
        "local" => "Local",
        "s3" => "S3",
        "arweave" => "Arweave",
        "ipfs" => "IPFS",
        "filecoin" => "Filecoin",
        "peer" => "Peer",
        other => {
            return Err(DharmaError::Validation(format!(
                "unknown vault driver {other}"
            )))
        }
    };
    Ok(name.to_string())
}

fn compression_name(alg: CompressionAlg) -> &'static str {
    match alg {
        CompressionAlg::Zstd19 => "Zstd_19",
    }
}

fn encryption_name(alg: EncryptionAlg) -> &'static str {
    match alg {
        EncryptionAlg::XChaCha20Poly1305 => "XChaCha20_Poly1305",
    }
}
