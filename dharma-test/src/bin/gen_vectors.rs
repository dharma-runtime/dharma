use dharma_core::assertion::{AssertionHeader, AssertionPlaintext};
use dharma_core::builtins;
use dharma_core::crypto;
use dharma_core::envelope;
use dharma_core::types::{ContractId, KeyId, Nonce12, SchemaId, SubjectId};
use rand_chacha::ChaCha20Rng;
use rand_core::{RngCore, SeedableRng};
use std::fs;
use std::path::PathBuf;

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::current_dir()?;
    let base = root.join("tests").join("vectors");
    fs::create_dir_all(&base)?;

    write_cbor_vectors(&base)?;
    write_assertion_vectors(&base)?;
    write_envelope_vectors(&base)?;
    write_schema_vectors(&base)?;
    write_contract_vectors(&base)?;

    Ok(())
}

fn write_cbor_vectors(base: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let dir = base.join("cbor");
    fs::create_dir_all(&dir)?;
    let canonical = vec![0xa1, 0x61, 0x61, 0x01]; // {"a":1}
    let noncanonical = vec![0xbf, 0x61, 0x61, 0x01, 0xff]; // indefinite map
    fs::write(dir.join("valid-001.cbor"), canonical)?;
    fs::write(dir.join("valid-001.meta"), "expect = \"canonical\"\n")?;
    fs::write(dir.join("invalid-001.cbor"), noncanonical)?;
    fs::write(dir.join("invalid-001.meta"), "expect = \"reject\"\n")?;
    Ok(())
}

fn write_assertion_vectors(base: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let dir = base.join("assertion");
    fs::create_dir_all(&dir)?;
    let mut rng = ChaCha20Rng::seed_from_u64(7);
    let (sk, pk) = crypto::generate_identity_keypair(&mut rng);
    let subject = SubjectId::from_bytes(rand_bytes32(&mut rng));
    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: 1,
        sub: subject,
        typ: "action.Note".to_string(),
        auth: pk,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: SchemaId::from_bytes(rand_bytes32(&mut rng)),
        contract: ContractId::from_bytes(rand_bytes32(&mut rng)),
        note: None,
        meta: None,
    };
    let body = ciborium::value::Value::Map(vec![(
        ciborium::value::Value::Text("text".to_string()),
        ciborium::value::Value::Text("hello".to_string()),
    )]);
    let assertion = AssertionPlaintext::sign(header, body, &sk)?;
    let bytes = assertion.to_cbor()?;
    fs::write(dir.join("valid-001.cbor"), &bytes)?;
    fs::write(dir.join("valid-001.meta"), "expect = \"accept\"\n")?;

    let mut bad = assertion.clone();
    bad.body = ciborium::value::Value::Map(vec![(
        ciborium::value::Value::Text("text".to_string()),
        ciborium::value::Value::Text("oops".to_string()),
    )]);
    let bad_bytes = bad.to_cbor()?;
    fs::write(dir.join("invalid-sig-001.cbor"), &bad_bytes)?;
    fs::write(dir.join("invalid-sig-001.meta"), "expect = \"reject\"\n")?;
    Ok(())
}

fn write_envelope_vectors(base: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let dir = base.join("envelope");
    fs::create_dir_all(&dir)?;
    let mut rng = ChaCha20Rng::seed_from_u64(11);
    let mut key = [0u8; 32];
    rng.fill_bytes(&mut key);
    let kid = KeyId::from_bytes(rand_bytes32(&mut rng));
    let nonce = Nonce12::from_bytes(rand_nonce(&mut rng));
    let plaintext = b"envelope-test";
    let env = envelope::encrypt_assertion(plaintext, kid, &key, nonce)?;
    let bytes = env.to_cbor()?;
    let key_hex = hex::encode(key);
    fs::write(dir.join("valid-001.cbor"), &bytes)?;
    fs::write(
        dir.join("valid-001.meta"),
        format!("expect = \"decrypt\"\nkey = \"{}\"\n", key_hex),
    )?;
    let bad_key = [9u8; 32];
    fs::write(dir.join("invalid-key-001.cbor"), &bytes)?;
    fs::write(
        dir.join("invalid-key-001.meta"),
        format!("expect = \"reject\"\nkey = \"{}\"\n", hex::encode(bad_key)),
    )?;
    Ok(())
}

fn write_schema_vectors(base: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let dir = base.join("schema");
    fs::create_dir_all(&dir)?;
    let bytes = builtins::note_schema_bytes()?;
    fs::write(dir.join("note-001.cbor"), &bytes)?;
    fs::write(dir.join("note-001.meta"), "expect = \"accept\"\n")?;
    Ok(())
}

fn write_contract_vectors(base: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let dir = base.join("contract");
    fs::create_dir_all(&dir)?;
    let wasm = wat::parse_str(
        r#"(module
            (memory (export "memory") 1)
            (func (export "validate") (result i32)
              i32.const 0)
            (func (export "reduce") (result i32)
              i32.const 0)
          )"#,
    )?;
    fs::write(dir.join("accept-001.wasm"), &wasm)?;
    fs::write(
        dir.join("accept-001.meta"),
        "expect = \"accept\"\ndata_ext = \"wasm\"\n",
    )?;
    Ok(())
}

fn rand_bytes32(rng: &mut ChaCha20Rng) -> [u8; 32] {
    let mut out = [0u8; 32];
    rng.fill_bytes(&mut out);
    out
}

fn rand_nonce(rng: &mut ChaCha20Rng) -> [u8; 12] {
    let mut out = [0u8; 12];
    rng.fill_bytes(&mut out);
    out
}
