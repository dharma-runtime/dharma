extern crate dharma_core as dharma;

pub use dharma_core::{
    assertion, builtins, cbor, contract, crypto, envelope, error, identity, identity_store,
    keystore, lock, net, dharmaq as dharmaq_core, runtime, schema, store, sync, types, validation,
    value, FrontierIndex, IdentityState, DharmaError, Store,
};

pub mod cmd;
pub mod dhlq;
pub mod dhlp;
pub mod pdl;
pub mod pkg;
pub mod dharmaq;
pub mod repl;
pub mod reactor;
pub mod vault;

use dharma::assertion::AssertionPlaintext;
#[cfg(feature = "compiler")]
use dharma::assertion::DEFAULT_DATA_VERSION;
use dharma::lock::LockHandle;
use dharma::keys::Keyring;
use dharma::store::state::list_assertions;
use dharma::types::SubjectId;
#[cfg(feature = "compiler")]
use dharma::types::{ContractId, EnvelopeId, SchemaId};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::collections::HashMap;

#[cfg(feature = "compiler")]
const CONFIG_TOML: &str = "dharma.toml";

pub const APP_NAME: &str = "Dharma";
pub const APP_VERSION: &str = "0.1-alpha";
pub const APP_BANNER: &str = r#"       ____                              
  ____╱ ╱ ╱_  ____ __________ ___  ____ _
 ╱ __  ╱ __ ╲╱ __ `╱ ___╱ __ `__ ╲╱ __ `╱
╱ ╱_╱ ╱ ╱ ╱ ╱ ╱_╱ ╱ ╱  ╱ ╱ ╱ ╱ ╱ ╱ ╱_╱ ╱ 
╲__,_╱_╱ ╱_╱╲__,_╱_╱  ╱_╱ ╱_╱ ╱_╱╲__,_╱  
                                         
"#;

pub fn print_banner() {
    println!("{APP_BANNER}");
}

pub fn run() -> Result<(), DharmaError> {
    let args: Vec<String> = env::args().collect();
    let (readonly, filtered) = parse_args(args);
    let argv: Vec<&str> = filtered.iter().map(|s| s.as_str()).collect();
    let data_dir = ensure_data_dir()?;
    if readonly && requires_write(&argv) {
        return Err(DharmaError::Validation(
            "Readonly mode cannot execute write commands.".to_string(),
        ));
    }
    let _lock = if readonly {
        None
    } else {
        Some(
            LockHandle::acquire(&data_dir.join("dharma.lock")).map_err(|err| match err {
                DharmaError::LockBusy => DharmaError::Config(
                    "Another Dharma instance is running. Please stop it or use --readonly."
                        .to_string(),
                ),
                other => other,
            })?,
        )
    };
    match argv.as_slice() {
        [_, "identity", "init", alias] | [_, "init", alias] => init_identity(alias),
        [_, "identity", "export"] | [_, "export"] => export_identity(),
        [_, "connect", addr] => connect(addr, false),
        [_, "connect", addr, "--verbose"] | [_, "connect", "--verbose", addr] => {
            connect(addr, true)
        }
        [_, "config", "show"] => config_show(),
        [_, "config"] => {
            print_config_usage();
            Ok(())
        }
        [_, "compile", source] => compile_dhl(source),
        [_, "test", args @ ..] => run_tests(args),
        [_, "doctor"] => cmd::ops::doctor(),
        [_, "gc", args @ ..] => cmd::ops::gc(args),
        [_, "reserve", "expire", args @ ..] => cmd::ops::reserve_expire(args),
        [_, "backup", "export", path] => cmd::ops::backup_export(path),
        [_, "backup", "import", path] => cmd::ops::backup_import(path, false),
        [_, "backup", "import", "--force", path]
        | [_, "backup", "import", path, "--force"] => cmd::ops::backup_import(path, true),
        [_, "project", args @ ..] => cmd::project::project(args),
        [_, "action", subject, action] => cmd::action::action_cmd(subject, action, &[]),
        [_, "action", subject, action, args @ ..] => {
            cmd::action::action_cmd(
                subject,
                action,
                &args.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )
        }
        [_, "write", body] => cmd::write::write_cmd(None, body),
        [_, "write", subject, body] => cmd::write::write_cmd(Some(subject), body),
        [_, "repl"] => crate::repl::run(),
        [_, "serve"] => boot_and_listen(false, false),
        [_, "serve", "--verbose"] => boot_and_listen(false, true),
        [_, "serve", "--relay"] => boot_and_listen(true, false),
        [_, "serve", "--relay", "--verbose"]
        | [_, "serve", "--verbose", "--relay"] => boot_and_listen(true, true),
        [_] => crate::repl::run(),
        _ => {
            print_usage();
            Ok(())
        }
    }
}

fn parse_args(args: Vec<String>) -> (bool, Vec<String>) {
    let mut readonly = false;
    let mut filtered = Vec::new();
    for arg in args {
        if arg == "--readonly" {
            readonly = true;
        } else {
            filtered.push(arg);
        }
    }
    (readonly, filtered)
}

fn requires_write(argv: &[&str]) -> bool {
    match argv {
        [_, "identity", "export"] | [_, "export"] => false,
        [_, "config", "show"] | [_, "config"] => false,
        [_, "doctor"] => false,
        [_, "identity", "init", ..] | [_, "init", ..] => true,
        [_, "connect", ..] => true,
        [_, "compile", ..] => true,
        [_, "test", ..] => true,
        [_, "gc", ..] => true,
        [_, "reserve", ..] => true,
        [_, "backup", ..] => true,
        [_, "project", ..] => true,
        [_, "action", ..] => true,
        [_, "write", ..] => true,
        [_, "repl"] => true,
        [_, "serve", ..] => true,
        [_] => true,
        _ => false,
    }
}

fn print_usage() {
    println!("Usage:");
    println!("  dh <command>");
    println!("  dh identity init <alias>");
    println!("  dh identity export");
    println!("  dh connect <addr:port> [--verbose]");
    println!("  dh config show");
    println!("  dh compile <file.dhl>");
    println!("  dh test [--deep] [--chaos] [--ci] [--relay] [--external] [--replay SEED=<seed>]");
    println!("  dh doctor");
    println!("  dh gc [--dry-run] [--no-prune] [--no-dharmaq]");
    println!("  dh reserve expire [--dry-run]");
    println!("  dh backup export <path>");
    println!("  dh backup import <path> [--force]");
    println!("  dh project rebuild [--scope std.commerce]");
    println!("  dh project watch [--scope std.commerce] [--interval SECONDS]");
    println!("  dh serve [--relay] [--verbose]");
    println!("  dh");
}

fn print_config_usage() {
    println!("Usage:");
    println!("  dh config show");
}

fn config_show() -> Result<(), DharmaError> {
    let root = env::current_dir()?;
    let config = dharma::config::Config::load(&root)?;
    println!("{}", config.to_toml_string());
    Ok(())
}

fn run_tests(args: &[&str]) -> Result<(), DharmaError> {
    let mut opts = dharma_test::TestOptions {
        deep: false,
        chaos: false,
        ci: false,
        replay_seed: None,
        relay_only: false,
        external: false,
    };
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match *arg {
            "--deep" => opts.deep = true,
            "--chaos" => opts.chaos = true,
            "--ci" => opts.ci = true,
            "--relay" => opts.relay_only = true,
            "--external" => opts.external = true,
            "--replay" => {
                if let Some(next) = iter.next() {
                    opts.replay_seed = parse_replay_seed(next);
                }
            }
            arg if arg.starts_with("--replay") => {
                if let Some((_, value)) = arg.split_once('=') {
                    opts.replay_seed = parse_replay_seed(value);
                }
            }
            _ => {}
        }
    }
    let summary = dharma_test::run(opts.clone())?;
    if summary.failed > 0 {
        println!("dh test failed (seed {}).", summary.seed);
        if !summary.tickets.is_empty() {
            println!("Failure ticket: {}", summary.tickets[0].display());
        }
        return Err(DharmaError::Validation("dh test failed".to_string()));
    }
    println!(
        "dh test passed ({} run, seed {}).",
        summary.passed, summary.seed
    );
    Ok(())
}

fn parse_replay_seed(value: &str) -> Option<u64> {
    let value = value.trim();
    if let Some(rest) = value.strip_prefix("SEED=") {
        return rest.parse::<u64>().ok();
    }
    value.parse::<u64>().ok()
}

fn init_identity(alias: &str) -> Result<(), DharmaError> {
    let data_dir = ensure_data_dir()?;
    let passphrase = prompt("Password: ")?;
    let env = dharma::env::StdEnv::new(&data_dir);
    let result = identity_store::init_identity(&env, alias, &passphrase)?;
    match result {
        Some(_) => println!("Created identity."),
        None => println!("Status: Already initialized."),
    }
    Ok(())
}

#[cfg(feature = "compiler")]
pub(crate) fn compile_dhl(source: &str) -> Result<(), DharmaError> {
    let data_dir = ensure_data_dir()?;
    let source_path = Path::new(source);
    let contents = fs::read_to_string(source_path)?;
    let mut ast = pdl::parser::parse(&contents)?;
    if let Some(parent_ref) = ast
        .aggregates
        .first()
        .and_then(|agg| agg.extends.clone())
    {
        let parent_ast = resolve_parent_ast(source_path, &ast.header.imports, &parent_ref)?;
        let parent_name = parent_ref
            .rsplit('.')
            .next()
            .unwrap_or(parent_ref.as_str());
        ast = pdl::merge::merge_parent(ast, &parent_ast, parent_name)?;
    }
    let schema_bytes = pdl::codegen::schema::compile_schema(&ast)?;
    let contract_bytes = pdl::codegen::wasm::compile(&ast)?;
    let reactor_bytes = pdl::codegen::wasm::compile_reactor(&ast)?;

    let data_ver = parse_data_version(&ast.header.version);
    let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
    let contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
    let schema_obj = EnvelopeId::from_bytes(*schema_id.as_bytes());
    let contract_obj = EnvelopeId::from_bytes(*contract_id.as_bytes());
    let reactor_obj = EnvelopeId::from_bytes(crypto::sha256(&reactor_bytes));
    let summary = pdl::codegen::permissions::compile_permissions(&ast, contract_id, data_ver);
    let summary_bytes = summary.to_cbor()?;

    let stem = output_stem_for_source(source_path);
    if let Some(parent) = stem.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(stem.with_extension("schema"), &schema_bytes)?;
    fs::write(stem.with_extension("contract"), &contract_bytes)?;
    fs::write(stem.with_extension("reactor"), &reactor_bytes)?;
    fs::write(stem.with_extension("summary"), &summary_bytes)?;

    let store = Store::from_root(&data_dir);
    store.put_object(&schema_obj, &schema_bytes)?;
    store.put_object(&contract_obj, &contract_bytes)?;
    store.put_object(&reactor_obj, &reactor_bytes)?;
    store.put_permission_summary(&summary)?;

    update_config(
        &data_dir.join(CONFIG_TOML),
        &schema_id,
        &contract_id,
        Some(&reactor_obj),
        data_ver,
    )?;

    println!("Schema ID: {}", schema_id.to_hex());
    println!("Contract ID: {}", contract_id.to_hex());
    println!("Reactor ID: {}", reactor_obj.to_hex());
    println!("Data Version: {}", data_ver);
    Ok(())
}

#[cfg(feature = "compiler")]
fn output_stem_for_source(source: &Path) -> PathBuf {
    if let Some(project_root) = find_project_root(source) {
        if let Some(rel) = relative_contract_path(source, &project_root) {
            let mut out = project_root.join(".dharma").join("contracts").join(rel);
            out.set_extension("");
            return out;
        }
    }

    if let Some(contracts_root) = find_contracts_root(source) {
        if let Ok(rel) = source.strip_prefix(&contracts_root) {
            let mut out = contracts_root.join("_build").join(rel);
            out.set_extension("");
            return out;
        }
    }

    let base_dir = source.parent().unwrap_or_else(|| Path::new("."));
    let file_stem = source.file_stem().unwrap_or_default().to_os_string();
    base_dir.join("_build").join(file_stem)
}

#[cfg(feature = "compiler")]
fn find_project_root(source: &Path) -> Option<PathBuf> {
    let mut current = source.parent();
    while let Some(dir) = current {
        if dir.join(CONFIG_TOML).is_file() {
            return Some(dir.to_path_buf());
        }
        current = dir.parent();
    }
    None
}

#[cfg(feature = "compiler")]
fn relative_contract_path(source: &Path, project_root: &Path) -> Option<PathBuf> {
    let contracts_root = project_root.join("contracts");
    if let Ok(rel) = source.strip_prefix(&contracts_root) {
        return Some(rel.to_path_buf());
    }
    if let Ok(rel) = source.strip_prefix(project_root) {
        return Some(rel.to_path_buf());
    }
    None
}

#[cfg(feature = "compiler")]
fn find_contracts_root(source: &Path) -> Option<PathBuf> {
    let mut current = source.parent();
    while let Some(dir) = current {
        if dir.file_name().and_then(|s| s.to_str()) == Some("contracts") {
            return Some(dir.to_path_buf());
        }
        current = dir.parent();
    }
    None
}

#[cfg(feature = "compiler")]
fn resolve_parent_ast(
    source_path: &Path,
    imports: &[String],
    parent_ref: &str,
) -> Result<pdl::ast::AstFile, DharmaError> {
    let base_dir = source_path.parent().unwrap_or_else(|| Path::new("."));
    let mut candidates: Vec<PathBuf> = Vec::new();
    for import in imports {
        if import.ends_with(".dhl") {
            candidates.push(base_dir.join(import));
        }
        if import == parent_ref {
            candidates.push(base_dir.join(format!("{import}.dhl")));
        }
    }
    candidates.push(base_dir.join(format!("{parent_ref}.dhl")));
    if let Some(tail) = parent_ref.rsplit('.').next() {
        candidates.push(base_dir.join(format!("{tail}.dhl")));
    }

    let mut seen = std::collections::HashSet::new();
    for path in candidates {
        if !seen.insert(path.clone()) {
            continue;
        }
        if path.exists() {
            let contents = fs::read_to_string(&path)?;
            let mut ast = pdl::parser::parse(&contents)?;
            if let Some(next_parent) = ast
                .aggregates
                .first()
                .and_then(|agg| agg.extends.clone())
            {
                let parent_ast = resolve_parent_ast(&path, &ast.header.imports, &next_parent)?;
                let parent_name = next_parent
                    .rsplit('.')
                    .next()
                    .unwrap_or(next_parent.as_str());
                ast = pdl::merge::merge_parent(ast, &parent_ast, parent_name)?;
            }
            return Ok(ast);
        }
    }

    Err(DharmaError::Validation(format!(
        "missing parent DHL for {parent_ref}"
    )))
}

#[cfg(not(feature = "compiler"))]
fn compile_dhl(_source: &str) -> Result<(), DharmaError> {
    Err(DharmaError::Validation(
        "compile is disabled; build with --features compiler".to_string(),
    ))
}

fn export_identity() -> Result<(), DharmaError> {
    let data_dir = ensure_data_dir()?;
    let env = dharma::env::StdEnv::new(&data_dir);
    identity_store::ensure_identity_present(&env)?;
    let passphrase = prompt("Password: ")?;
    let paper_key = identity_store::export_identity(&env, &passphrase)?;
    println!("{paper_key}");
    Ok(())
}

fn boot_and_listen(relay: bool, verbose: bool) -> Result<(), DharmaError> {
    let root = env::current_dir()?;
    let config = dharma::config::Config::load(&root)?;
    let data_dir = ensure_data_dir()?;
    let env = dharma::env::StdEnv::new(&data_dir);
    if identity_store::ensure_identity_present(&env).is_err() {
        return Ok(());
    }
    let identity = load_identity(&env)?;
    let head = mount_self(&env, &identity)?;
    println!("Identity Unlocked. Head seq: {head}");
    reactor::spawn_daemon(data_dir.clone(), identity.clone());
    let store = Store::new(&env);
    let addr = format!("0.0.0.0:{}", config.network.listen_port);
    let options = net::server::ServerOptions {
        relay,
        verbose,
        max_connections: config.network.max_connections,
        ..Default::default()
    };
    net::server::listen_with_options(&addr, identity, store, options)?;
    Ok(())
}

fn connect(addr: &str, verbose: bool) -> Result<(), DharmaError> {
    let root = env::current_dir()?;
    let config = dharma::config::Config::load(&root)?;
    let data_dir = ensure_data_dir()?;
    let env = dharma::env::StdEnv::new(&data_dir);
    identity_store::ensure_identity_present(&env)?;
    let identity = load_identity(&env)?;
    let head = mount_self(&env, &identity)?;
    println!("Identity Unlocked. Head seq: {head}");

    let mut stream = match addr.parse::<SocketAddr>() {
        Ok(sock) => TcpStream::connect_timeout(&sock, config.connect_timeout())?,
        Err(_) => TcpStream::connect(addr)?,
    };
    config.apply_timeouts(&stream);
    let session = net::handshake::client_handshake(&mut stream, &identity)?;
    println!("Connected. Handshake Complete.");

    let store = Store::new(&env);
    let mut legacy_keys: HashMap<SubjectId, [u8; 32]> = HashMap::new();
    legacy_keys.insert(identity.subject_id, identity.subject_key);
    let mut keys = Keyring::from_subject_keys(&legacy_keys);
    keys.insert_hpke_secret(identity.public_key, identity.noise_sk);
    let mut index = FrontierIndex::build(&store, &keys)?;
    let policy = net::policy::OverlayPolicy::load(store.root());
    let claims = net::policy::PeerClaims::default();
    let access = net::policy::OverlayAccess::new(&policy, None, false, &claims);
    net::sync::sync_loop_with(
        &mut stream,
        session,
        &store,
        &mut index,
        &mut keys,
        &identity,
        &access,
        net::sync::SyncOptions {
            relay: false,
            ad_store: None,
            local_subs: None,
            verbose,
            exit_on_idle: true,
            trace: None,
        },
    )?;
    Ok(())
}

pub(crate) fn mount_self<E>(env: &E, identity: &IdentityState) -> Result<u64, DharmaError>
where
    E: dharma::env::Env + Clone + Send + Sync + 'static,
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

pub(crate) fn load_identity<E: dharma::env::Env>(env: &E) -> Result<IdentityState, DharmaError> {
    let passphrase = prompt("Password: ")?;
    match identity_store::load_identity(env, &passphrase) {
        Ok(identity) => Ok(identity),
        Err(err) => {
            if let Ok(root) = env::current_dir() {
                if let Ok(cfg) = dharma::config::Config::load(&root) {
                    let key_path = cfg.keystore_path_for(&root, env.root());
                    return Err(DharmaError::Validation(format!(
                        "{err} (key: {})",
                        key_path.display()
                    )));
                }
            }
            Err(err)
        }
    }
}

pub(crate) fn ensure_identity_present<E: dharma::env::Env>(env: &E) -> Result<(), DharmaError> {
    identity_store::ensure_identity_present(env)
}

pub(crate) fn ensure_data_dir() -> Result<PathBuf, DharmaError> {
    let root = env::current_dir()?;
    let config = dharma::config::Config::load(&root)?;
    let dir = config.storage_path(&root);
    if !dir.exists() {
        fs::create_dir_all(&dir)?;
    }
    Ok(dir)
}

fn prompt(label: &str) -> Result<String, DharmaError> {
    let mut input = String::new();
    print!("{label}");
    io::stdout().flush()?;
    io::stdin().read_line(&mut input)?;
    Ok(input.trim_end().to_string())
}


#[cfg(feature = "compiler")]
fn update_config(
    path: &Path,
    schema: &SchemaId,
    contract: &ContractId,
    reactor: Option<&EnvelopeId>,
    ver: u64,
) -> Result<(), DharmaError> {
    let schema_key = format!("schema_v{ver}");
    let contract_key = format!("contract_v{ver}");
    let reactor_key = format!("reactor_v{ver}");
    let mut lines = Vec::new();
    if path.exists() {
        let contents = fs::read_to_string(path)?;
        for line in contents.lines() {
            let trimmed = line.trim();
            let key = trimmed.split('=').next().unwrap_or("").trim();
            if key == schema_key || key == contract_key || key == reactor_key {
                continue;
            }
            if ver == DEFAULT_DATA_VERSION
                && (key == "schema" || key == "contract" || key == "reactor")
            {
                continue;
            }
            if !trimmed.is_empty() {
                lines.push(line.to_string());
            }
        }
    }
    lines.push(format!("{schema_key} = \"{}\"", schema.to_hex()));
    lines.push(format!("{contract_key} = \"{}\"", contract.to_hex()));
    if let Some(reactor) = reactor {
        lines.push(format!("{reactor_key} = \"{}\"", reactor.to_hex()));
    }
    if ver == DEFAULT_DATA_VERSION {
        lines.push(format!("schema = \"{}\"", schema.to_hex()));
        lines.push(format!("contract = \"{}\"", contract.to_hex()));
        if let Some(reactor) = reactor {
            lines.push(format!("reactor = \"{}\"", reactor.to_hex()));
        }
    }
    fs::write(path, lines.join("\n") + "\n")?;
    Ok(())
}

#[cfg(feature = "compiler")]
fn parse_data_version(version: &str) -> u64 {
    version
        .split('.')
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_DATA_VERSION)
}
