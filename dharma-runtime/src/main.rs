use dharma_core::assertion::AssertionPlaintext;
use dharma_core::env::StdEnv;
use dharma_core::envelope;
use dharma_core::identity::IdentityState;
use dharma_core::identity_store;
use dharma_core::net;
use dharma_core::store::state::list_assertions;
use dharma_core::store::Store;
use dharma_core::DharmaError;
use dharma_core::config::Config;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

const APP_BANNER: &str = r#"       ____                              
  ____╱ ╱ ╱_  ____ __________ ___  ____ _
 ╱ __  ╱ __ ╲╱ __ `╱ ___╱ __ `__ ╲╱ __ `╱
╱ ╱_╱ ╱ ╱ ╱ ╱ ╱_╱ ╱ ╱  ╱ ╱ ╱ ╱ ╱ ╱ ╱_╱ ╱ 
╲__,_╱_╱ ╱_╱╲__,_╱_╱  ╱_╱ ╱_╱ ╱_╱╲__,_╱  
                                         
"#;

fn main() {
    println!("{APP_BANNER}");
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), DharmaError> {
    let relay = std::env::args().any(|arg| arg == "--relay");
    let root = std::env::current_dir()?;
    let config = Config::load(&root)?;
    let data_dir = ensure_data_dir()?;
    let env = StdEnv::new(&data_dir);
    if identity_store::ensure_identity_present(&env).is_err() {
        return Ok(());
    }
    let identity = load_identity(&env)?;
    let head = mount_self(&env, &identity)?;
    println!("Identity Unlocked. Head seq: {head}");
    let store = Store::new(&env);
    let addr = format!("0.0.0.0:{}", config.network.listen_port);
    let options = net::server::ServerOptions {
        relay,
        ..Default::default()
    };
    net::server::listen_with_options(&addr, identity, store, options)?;
    Ok(())
}

fn ensure_data_dir() -> Result<PathBuf, DharmaError> {
    let root = std::env::current_dir()?;
    let config = Config::load(&root)?;
    let dir = config.storage_path(&root);
    if !dir.exists() {
        fs::create_dir_all(&dir)?;
    }
    Ok(dir)
}

fn load_identity<E: dharma_core::env::Env>(env: &E) -> Result<IdentityState, DharmaError> {
    let passphrase = prompt("Password: ")?;
    identity_store::load_identity(env, &passphrase)
}

fn prompt(label: &str) -> Result<String, DharmaError> {
    let mut input = String::new();
    print!("{label}");
    io::stdout().flush()?;
    io::stdin().read_line(&mut input)?;
    Ok(input.trim_end().to_string())
}

fn mount_self<E>(env: &E, identity: &IdentityState) -> Result<u64, DharmaError>
where
    E: dharma_core::env::Env + Clone + Send + Sync + 'static,
{
    let store = Store::new(env);
    let mut head_seq = 0;
    let mut head: Option<AssertionPlaintext> = None;

    let records = list_assertions(store.env(), &identity.subject_id)?;
    for record in records {
        let assertion = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(a) => a,
            Err(_) => continue,
        };
        if assertion.header.auth != identity.root_public_key {
            continue;
        }
        if assertion.header.seq > head_seq {
            head_seq = assertion.header.seq;
            head = Some(assertion);
        }
    }

    if head.is_none() {
        for object_id in store.list_objects()? {
            let bytes = store.get_assertion(&identity.subject_id, &object_id)?;
            let envelope = match envelope::AssertionEnvelope::from_cbor(&bytes) {
                Ok(env) => env,
                Err(_) => continue,
            };
            let plaintext = match envelope::decrypt_assertion(&envelope, &identity.subject_key) {
                Ok(pt) => pt,
                Err(_) => continue,
            };
            let assertion = match AssertionPlaintext::from_cbor(&plaintext) {
                Ok(a) => a,
                Err(_) => continue,
            };
            if assertion.header.auth != identity.root_public_key {
                continue;
            }
            if assertion.header.seq > head_seq {
                head_seq = assertion.header.seq;
                head = Some(assertion);
            }
        }
    }

    let head = head.ok_or_else(|| DharmaError::Validation("No identity assertions".to_string()))?;
    if !head.verify_signature()? {
        return Err(DharmaError::Validation("Invalid identity head signature".to_string()));
    }
    Ok(head_seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dharma_core::identity_store;

    #[test]
    fn mount_self_accepts_root_signed_identity_head() {
        let temp = tempfile::tempdir().unwrap();
        let env = dharma_core::env::StdEnv::new(temp.path());
        identity_store::init_identity(&env, "alice", "pw")
            .unwrap()
            .expect("identity should be created");
        let identity = identity_store::load_identity(&env, "pw").unwrap();
        let head = mount_self(&env, &identity).unwrap();
        assert!(head >= 3);
    }
}
