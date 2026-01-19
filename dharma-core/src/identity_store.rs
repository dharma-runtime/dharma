use crate::assertion::{add_signer_meta, AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
use crate::builtins;
use crate::config;
use crate::crypto;
use crate::env::Env;
use crate::error::DharmaError;
use crate::identity::IdentityState;
use crate::keystore::{decrypt_key, encrypt_key, KeystoreData};
use crate::store::Store;
use crate::store::state::append_assertion;
use crate::types::SubjectId;
use ciborium::value::Value;
use rand_core::{OsRng, RngCore};
use std::path::PathBuf;

pub const IDENTITY_DHARMA: &str = "identity.dharma";
pub const IDENTITY_KEY: &str = "identity.key";
pub const CONFIG_TOML: &str = "dharma.toml";

pub fn ensure_data_dir<E: Env>(env: &E) -> Result<(), DharmaError> {
    if !env.exists(env.root()) {
        env.create_dir_all(env.root())?;
    }
    Ok(())
}

pub fn identity_exists<E: Env>(env: &E) -> bool {
    env.exists(&env.root().join(IDENTITY_DHARMA))
}

pub fn read_identity_subject<E: Env>(env: &E) -> Result<SubjectId, DharmaError> {
    let contents = env.read(&env.root().join(IDENTITY_DHARMA))?;
    let contents = String::from_utf8(contents)
        .map_err(|_| DharmaError::Validation("invalid identity.dharma".to_string()))?;
    let hex = contents.trim();
    SubjectId::from_hex(hex)
}

pub fn ensure_identity_present<E: Env>(env: &E) -> Result<(), DharmaError> {
    if !identity_exists(env) {
        println!("Status: Uninitialized. Run 'dh identity init <name>'");
        return Err(DharmaError::Config("uninitialized".to_string()));
    }
    Ok(())
}

pub fn init_identity<E>(
    env: &E,
    alias: &str,
    passphrase: &str,
) -> Result<Option<IdentityState>, DharmaError>
where
    E: Env + Clone + Send + Sync + 'static,
{
    ensure_data_dir(env)?;
    let identity_path = env.root().join(IDENTITY_DHARMA);
    if env.exists(&identity_path) {
        return Ok(None);
    }

    let mut subject_key = [0u8; 32];
    OsRng.fill_bytes(&mut subject_key);
    let subject_id = SubjectId::random(&mut OsRng);
    let (root_signing_key, root_identity_key) = crypto::generate_identity_keypair(&mut OsRng);
    let (device_signing_key, device_identity_key) = crypto::generate_identity_keypair(&mut OsRng);

    let store = Store::new(env);
    let (schema_id, contract_id) = builtins::ensure_note_artifacts(&store)?;

    let genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject_id,
        typ: "core.genesis".to_string(),
        auth: root_identity_key,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: Some("identity genesis".to_string()),
        meta: add_signer_meta(None, &subject_id),
    };
    let genesis_body = Value::Map(vec![
        (Value::Text("doc_type".to_string()), Value::Text("identity".to_string())),
        (
            Value::Text("schema".to_string()),
            Value::Bytes(schema_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("contract".to_string()),
            Value::Bytes(contract_id.as_bytes().to_vec()),
        ),
        (Value::Text("title".to_string()), Value::Text(alias.to_string())),
        (
            Value::Text("members".to_string()),
            Value::Array(vec![Value::Bytes(root_identity_key.as_bytes().to_vec())]),
        ),
    ]);

    let genesis_typ = genesis_header.typ.clone();
    let genesis_seq = genesis_header.seq;
    let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &root_signing_key)?;
    let genesis_bytes = genesis.to_cbor()?;
    let genesis_assertion_id = genesis.assertion_id()?;
    let genesis_env_id = crypto::envelope_id(&genesis_bytes);
    store.put_assertion(&subject_id, &genesis_env_id, &genesis_bytes)?;
    store.record_semantic(&genesis_assertion_id, &genesis_env_id)?;
    append_assertion(
        env,
        &subject_id,
        genesis_seq,
        genesis_assertion_id,
        genesis_env_id,
        &genesis_typ,
        &genesis_bytes,
    )?;

    let profile_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject_id,
        typ: "identity.profile".to_string(),
        auth: root_identity_key,
        seq: 2,
        prev: Some(genesis_assertion_id),
        refs: vec![genesis_assertion_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &subject_id),
    };
    let profile_body = Value::Map(vec![(
        Value::Text("alias".to_string()),
        Value::Text(alias.to_string()),
    )]);
    let profile_typ = profile_header.typ.clone();
    let profile_seq = profile_header.seq;
    let profile = AssertionPlaintext::sign(profile_header, profile_body, &root_signing_key)?;
    let profile_bytes = profile.to_cbor()?;
    let profile_assertion_id = profile.assertion_id()?;
    let profile_env_id = crypto::envelope_id(&profile_bytes);
    store.put_assertion(&subject_id, &profile_env_id, &profile_bytes)?;
    store.record_semantic(&profile_assertion_id, &profile_env_id)?;
    append_assertion(
        env,
        &subject_id,
        profile_seq,
        profile_assertion_id,
        profile_env_id,
        &profile_typ,
        &profile_bytes,
    )?;

    let delegate_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject_id,
        typ: "iam.delegate".to_string(),
        auth: root_identity_key,
        seq: 3,
        prev: Some(profile_assertion_id),
        refs: vec![profile_assertion_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &subject_id),
    };
    let delegate_body = Value::Map(vec![
        (
            Value::Text("delegate".to_string()),
            Value::Bytes(device_identity_key.as_bytes().to_vec()),
        ),
        (Value::Text("scope".to_string()), Value::Text("all".to_string())),
        (Value::Text("expires".to_string()), Value::Integer(0.into())),
    ]);
    let delegate_typ = delegate_header.typ.clone();
    let delegate_seq = delegate_header.seq;
    let delegate = AssertionPlaintext::sign(delegate_header, delegate_body, &root_signing_key)?;
    let delegate_bytes = delegate.to_cbor()?;
    let delegate_assertion_id = delegate.assertion_id()?;
    let delegate_env_id = crypto::envelope_id(&delegate_bytes);
    store.put_assertion(&subject_id, &delegate_env_id, &delegate_bytes)?;
    store.record_semantic(&delegate_assertion_id, &delegate_env_id)?;
    append_assertion(
        env,
        &subject_id,
        delegate_seq,
        delegate_assertion_id,
        delegate_env_id,
        &delegate_typ,
        &delegate_bytes,
    )?;

    let keystore = KeystoreData {
        root_signing_key,
        device_signing_key,
        identity: subject_id,
        subject_key,
        noise_sk: {
            let mut sk = [0u8; 32];
            OsRng.fill_bytes(&mut sk);
            sk
        },
        schema: schema_id,
        contract: contract_id,
    };
    let encrypted = encrypt_key(&keystore, passphrase)?;
    let key_path = identity_key_path(env);
    if let Some(parent) = key_path.parent() {
        env.create_dir_all(parent)?;
    }
    env.write(&key_path, &encrypted)?;
    env.write(&identity_path, subject_id.to_hex().as_bytes())?;
    env.write(
        &env.root().join(CONFIG_TOML),
        format!("identity = \"{}\"\n", subject_id.to_hex()).as_bytes(),
    )?;

    Ok(Some(IdentityState::from_keystore(keystore)))
}

pub fn load_identity<E: Env>(env: &E, passphrase: &str) -> Result<IdentityState, DharmaError> {
    let subject_id = read_identity_subject(env)?;
    let mut found_any = false;
    for path in identity_key_candidates(env) {
        if !env.exists(&path) {
            continue;
        }
        found_any = true;
        let blob = env.read(&path)?;
        if let Ok(keystore) = decrypt_key(&blob, passphrase) {
            if keystore.identity.as_bytes() != subject_id.as_bytes() {
                eprintln!("Warning: identity mismatch between identity.dharma and identity.key");
            }
            return Ok(IdentityState::from_keystore(keystore));
        }
    }
    if !found_any {
        return Err(DharmaError::Config("identity key missing".to_string()));
    }
    Err(DharmaError::Validation(
        "Error: Invalid Passphrase".to_string(),
    ))
}

pub fn load_identity_if_unlocked<E: Env>(env: &E) -> Result<IdentityState, DharmaError> {
    let passphrase = std::env::var("DHARMA_PASSPHRASE")
        .map_err(|_| DharmaError::Config("identity locked".to_string()))?;
    load_identity(env, &passphrase)
}

pub fn export_identity<E: Env>(env: &E, passphrase: &str) -> Result<String, DharmaError> {
    let mut last_err = None;
    for path in identity_key_candidates(env) {
        if !env.exists(&path) {
            continue;
        }
        let blob = env.read(&path)?;
        match decrypt_key(&blob, passphrase) {
            Ok(keystore) => {
                let secret = keystore.root_signing_key.to_bytes();
                return Ok(crate::types::hex_encode(secret));
            }
            Err(err) => last_err = Some(err),
        }
    }
    let _ = last_err;
    Err(DharmaError::Validation("Invalid Passphrase".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion::AssertionPlaintext;
    use crate::env::Fs;
    use crate::store::state::list_assertions;
    use tempfile::tempdir;

    #[test]
    fn identity_init_and_load_roundtrip() {
        let dir = tempdir().unwrap();
        let env = crate::env::StdEnv::new(dir.path());
        let passphrase = "test-pass";
        let created = init_identity(&env, "alice", passphrase)
            .unwrap()
            .expect("identity should be created");
        assert!(env.exists(&env.root().join(IDENTITY_DHARMA)));
        assert!(env.exists(&identity_key_path(&env)));
        let loaded = load_identity(&env, passphrase).unwrap();
        assert_eq!(created.subject_id.as_bytes(), loaded.subject_id.as_bytes());
        assert_eq!(created.public_key.as_bytes(), loaded.public_key.as_bytes());
        assert_eq!(created.root_public_key.as_bytes(), loaded.root_public_key.as_bytes());
        assert_eq!(created.subject_key, loaded.subject_key);
    }

    #[test]
    fn identity_init_is_idempotent() {
        let dir = tempdir().unwrap();
        let env = crate::env::StdEnv::new(dir.path());
        let passphrase = "test-pass";
        let first = init_identity(&env, "alice", passphrase).unwrap();
        assert!(first.is_some());
        let second = init_identity(&env, "bob", passphrase).unwrap();
        assert!(second.is_none());
    }

    #[test]
    fn identity_init_creates_delegate() {
        let dir = tempdir().unwrap();
        let env = crate::env::StdEnv::new(dir.path());
        let passphrase = "test-pass";
        let created = init_identity(&env, "alice", passphrase)
            .unwrap()
            .expect("identity should be created");
        let records = list_assertions(&env, &created.subject_id).unwrap();
        let mut seen_delegate = false;
        for record in records {
            if let Ok(assertion) = AssertionPlaintext::from_cbor(&record.bytes) {
                if assertion.header.typ == "iam.delegate" {
                    seen_delegate = true;
                }
            }
        }
        assert!(seen_delegate);
    }
}

fn identity_key_path<E: Env>(env: &E) -> PathBuf {
    let project_root = std::env::current_dir().unwrap_or_else(|_| env.root().to_path_buf());
    if let Ok(cfg) = config::Config::load(&project_root) {
        let configured_storage = cfg.storage_path(&project_root);
        if configured_storage != env.root() {
            return env.root().join(IDENTITY_KEY);
        }
        return cfg.keystore_path_for(&project_root, env.root());
    }
    env.root().join(IDENTITY_KEY)
}

fn identity_key_candidates<E: Env>(env: &E) -> Vec<PathBuf> {
    let primary = identity_key_path(env);
    let legacy = env.root().join(IDENTITY_KEY);
    let mut out = Vec::new();
    out.push(primary.clone());
    if legacy != primary {
        out.push(legacy);
    }
    out
}
