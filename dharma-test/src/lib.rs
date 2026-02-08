use ciborium::value::Value;
use dharma_core::assertion::DEFAULT_DATA_VERSION;
use dharma_core::assertion::{add_signer_meta, AssertionHeader, AssertionPlaintext};
use dharma_core::builtins;
use dharma_core::cbor;
use dharma_core::contacts::{contact_subject_id, relation as contact_relation, ContactRelation};
use dharma_core::contract::{PermissionRule, PermissionSummary, PublicPermissions};
use dharma_core::crypto;
use dharma_core::dharmaq::{self, CmpOp, Filter, Predicate, QueryPlan};
use dharma_core::domain::DomainState;
use dharma_core::env::{Env, StdEnv};
use dharma_core::envelope;
use dharma_core::fabric::auth::{CapToken, Flag, Op, Scope};
use dharma_core::fabric::directory::{DirectoryClient, DirectoryState};
use dharma_core::fabric::protocol::{
    dispatch, ExecStats, FabricDispatcher, FabricOp, FabricRequest, FabricResponse,
};
use dharma_core::fabric::types::{AdStore, Advertisement, Endpoint, ShardAd};
use dharma_core::identity::{
    self, IdentityStatus, ATLAS_IDENTITY_ACTIVATE, ATLAS_IDENTITY_GENESIS, ATLAS_IDENTITY_REVOKE,
    ATLAS_IDENTITY_SUSPEND,
};
use dharma_core::identity_store;
use dharma_core::keys::{hpke_public_key_from_secret, hpke_seal, key_id_for_key, Keyring};
use dharma_core::net::handshake::{client_handshake, server_handshake};
use dharma_core::net::ingest::{ingest_object, IngestStatus};
use dharma_core::net::policy::{OverlayAccess, OverlayPolicy, PeerClaims};
use dharma_core::net::sync::{sync_loop_with, SyncOptions};
use dharma_core::net::{self, server};
use dharma_core::pdl::schema::{
    ActionSchema, ConcurrencyMode, CqrsSchema, FieldSchema, TypeSpec, Visibility,
};
use dharma_core::runtime::cqrs::{decode_state, filter_state_value, load_state};
use dharma_core::share::FieldAccess;
use dharma_core::store::index::FrontierIndex;
use dharma_core::store::pending;
use dharma_core::store::state::{append_assertion, list_assertions, load_epoch};
use dharma_core::store::Store;
use dharma_core::sync::Subscriptions;
use dharma_core::types::{
    AssertionId, ContractId, EnvelopeId, IdentityKey, KeyId, Nonce12, SchemaId, SubjectId,
};
use dharma_core::validation::{order_assertions, structural_validate, StructuralStatus};
use dharma_core::{DharmaError, IdentityState};
use dharma_sim::{
    ClockFaultConfig, FaultConfig, FaultEvent, FaultTimeline, FsFaultConfig, NodeId, SimEnv,
    SimHub, TraceSink,
};
use rand_chacha::ChaCha20Rng;
use rand_core::{RngCore, SeedableRng};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::IsTerminal;
#[cfg(feature = "external")]
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
#[cfg(feature = "external")]
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

mod renderer;
use renderer::{HeadlessRenderer, Phase, Renderer, Status, TuiRenderer};

static LAST_LOG: Mutex<Option<Instant>> = Mutex::new(None);

#[derive(Clone, Debug)]
pub struct TestOptions {
    pub deep: bool,
    pub chaos: bool,
    pub ci: bool,
    pub replay_seed: Option<u64>,
    pub relay_only: bool,
    pub external: bool,
}

impl TestOptions {
    pub fn seed_count(&self) -> usize {
        if self.replay_seed.is_some() {
            1
        } else if self.deep {
            1000
        } else {
            1
        }
    }

    pub fn iterations(&self) -> usize {
        if self.deep {
            200
        } else {
            25
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TestSummary {
    pub passed: usize,
    pub failed: usize,
    pub tickets: Vec<PathBuf>,
    pub seed: u64,
}

#[derive(Clone, Debug)]
struct TestFailure {
    property: &'static str,
    seed: u64,
    details: String,
    trace: Option<Vec<String>>,
}

pub fn run(opts: TestOptions) -> Result<TestSummary, DharmaError> {
    let mut summary = TestSummary::default();
    let base_seed = opts.replay_seed.unwrap_or_else(random_seed);
    summary.seed = base_seed;
    let mut status = Status::new(base_seed, opts.seed_count());
    let mut renderer: Box<dyn Renderer> = if opts.ci || !std::io::stdout().is_terminal() {
        Box::new(HeadlessRenderer::new())
    } else {
        match TuiRenderer::new() {
            Ok(renderer) => Box::new(renderer),
            Err(_) => Box::new(HeadlessRenderer::new()),
        }
    };
    renderer.start(&status);
    let interactive = renderer.is_interactive();

    if opts.relay_only {
        status.phase = Phase::Properties;
        status.iterations = 1;
        renderer.update(&status);
        match run_relay_properties(base_seed, renderer.as_mut(), &mut status) {
            Ok(()) => {
                summary.passed += 1;
                status.passed = summary.passed;
            }
            Err(failure) => {
                summary.failed += 1;
                status.failed = summary.failed;
                let ticket = write_ticket(&failure, &opts)?;
                summary.tickets.push(ticket);
                let message = format!("{}: {}", failure.property, failure.details);
                status.error = Some(message.clone());
                renderer.log(&message);
                renderer.update(&status);
                if !interactive {
                    renderer.finish(&status);
                    return Ok(summary);
                }
                renderer.pause();
                status.error = None;
            }
        }
        status.phase = Phase::Done;
        renderer.finish(&status);
        return Ok(summary);
    }

    for idx in 0..opts.seed_count() {
        let seed = base_seed.wrapping_add(idx as u64);
        status.seed = seed;
        status.seed_index = idx + 1;
        status.seed_total = opts.seed_count();
        status.phase = Phase::Properties;
        status.current = None;
        status.iteration = 0;
        status.iterations = opts.iterations();
        status.nodes = 0;
        status.error = None;
        renderer.update(&status);
        if let Err(failure) = run_properties(seed, &opts, renderer.as_mut(), &mut status) {
            summary.failed += 1;
            status.failed = summary.failed;
            let ticket = write_ticket(&failure, &opts)?;
            summary.tickets.push(ticket);
            let message = format!("{}: {}", failure.property, failure.details);
            status.error = Some(message.clone());
            renderer.log(&message);
            renderer.update(&status);
            if !interactive {
                renderer.finish(&status);
                return Ok(summary);
            }
            renderer.pause();
            status.error = None;
            renderer.update(&status);
            continue;
        }
        summary.passed += 1;
        status.passed = summary.passed;
        renderer.update(&status);
    }

    status.phase = Phase::Vectors;
    status.current = None;
    status.iteration = 0;
    status.iterations = 0;
    status.nodes = 0;
    status.error = None;
    renderer.update(&status);
    let vectors_ok = match run_vectors(renderer.as_mut(), &mut status) {
        Ok(()) => true,
        Err(failure) => {
            summary.failed += 1;
            status.failed = summary.failed;
            let ticket = write_ticket(&failure, &opts)?;
            summary.tickets.push(ticket);
            let message = format!("{}: {}", failure.property, failure.details);
            status.error = Some(message.clone());
            renderer.log(&message);
            renderer.update(&status);
            if !interactive {
                renderer.finish(&status);
                return Ok(summary);
            }
            renderer.pause();
            status.error = None;
            renderer.update(&status);
            false
        }
    };
    if vectors_ok {
        summary.passed += 1;
        status.passed = summary.passed;
        renderer.update(&status);
    }
    if opts.external {
        status.phase = Phase::External;
        status.current = None;
        status.iteration = 0;
        status.iterations = 0;
        status.nodes = 0;
        status.error = None;
        renderer.update(&status);
        let ext_ok = match run_external_tests(renderer.as_mut(), &mut status) {
            Ok(()) => true,
            Err(failure) => {
                summary.failed += 1;
                status.failed = summary.failed;
                let ticket = write_ticket(&failure, &opts)?;
                summary.tickets.push(ticket);
                let message = format!("{}: {}", failure.property, failure.details);
                status.error = Some(message.clone());
                renderer.log(&message);
                renderer.update(&status);
                if !interactive {
                    renderer.finish(&status);
                    return Ok(summary);
                }
                renderer.pause();
                status.error = None;
                renderer.update(&status);
                false
            }
        };
        if ext_ok {
            summary.passed += 1;
            status.passed = summary.passed;
            renderer.update(&status);
        }
    }
    if opts.deep {
        status.phase = Phase::Simulation;
        status.current = None;
        status.iteration = 0;
        status.iterations = 0;
        status.nodes = 0;
        status.error = None;
        renderer.update(&status);
        let sim_ok = match run_simulation(base_seed, &opts, renderer.as_mut(), &mut status) {
            Ok(()) => true,
            Err(failure) => {
                summary.failed += 1;
                status.failed = summary.failed;
                let ticket = write_ticket(&failure, &opts)?;
                summary.tickets.push(ticket);
                let message = format!("{}: {}", failure.property, failure.details);
                status.error = Some(message.clone());
                renderer.log(&message);
                renderer.update(&status);
                if !interactive {
                    renderer.finish(&status);
                    return Ok(summary);
                }
                renderer.pause();
                status.error = None;
                renderer.update(&status);
                false
            }
        };
        if sim_ok {
            summary.passed += 1;
            status.passed = summary.passed;
            renderer.update(&status);
        }
    }
    status.phase = Phase::Done;
    renderer.finish(&status);
    Ok(summary)
}

#[cfg(test)]
mod permission_summary_bench {
    use dharma_core::contract::{PermissionRule, PermissionSummary, PublicPermissions};
    use dharma_core::types::ContractId;
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Instant;

    #[test]
    #[ignore]
    fn router_summary_bench() {
        let mut actions = BTreeMap::new();
        let mut roles = BTreeSet::new();
        roles.insert("admin".to_string());
        actions.insert(
            "Touch".to_string(),
            PermissionRule {
                roles,
                exhaustive: true,
            },
        );
        let summary = PermissionSummary {
            v: 1,
            contract: ContractId::from_bytes([8u8; 32]),
            ver: 1,
            actions,
            queries: BTreeMap::new(),
            role_scopes: BTreeMap::new(),
            public: PublicPermissions::default(),
        };
        let roles = vec!["admin".to_string()];
        let start = Instant::now();
        for _ in 0..200_000 {
            let _ = summary.allows_action(&roles, "Touch");
        }
        let elapsed = start.elapsed();
        eprintln!("permission summary bench: {:?}", elapsed);
    }
}

fn random_seed() -> u64 {
    let mut buf = [0u8; 8];
    rand_core::OsRng.fill_bytes(&mut buf);
    u64::from_le_bytes(buf)
}

fn run_external_tests(renderer: &mut dyn Renderer, status: &mut Status) -> Result<(), TestFailure> {
    if !cfg!(feature = "external") {
        let _ = renderer;
        let _ = status;
        return Err(TestFailure {
            property: "EXTERNAL-000",
            seed: 0,
            details: "external tests require building with --features external".to_string(),
            trace: None,
        });
    }
    #[cfg(feature = "external")]
    {
        let services = ExternalServices::start(renderer)?;
        if let Err(err) = external_s3_test(renderer, status, &services) {
            return Err(err);
        }
        if let Err(err) = external_arweave_test(renderer, status, &services) {
            return Err(err);
        }
    }
    Ok(())
}

#[cfg(feature = "external")]
struct ExternalServices {
    garage: Option<Child>,
    garage_dir: Option<TempDir>,
    garage_config: Option<std::path::PathBuf>,
    garage_bin: Option<String>,
    garage_endpoint: String,
    garage_bucket: String,
    garage_access_key: Option<String>,
    garage_secret_key: Option<String>,
    arlocal: Option<Child>,
    arlocal_dir: Option<TempDir>,
    arlocal_ready: bool,
    arweave_endpoint: String,
}

#[cfg(feature = "external")]
impl ExternalServices {
    fn start(renderer: &mut dyn Renderer) -> Result<Self, TestFailure> {
        let mut services = ExternalServices {
            garage: None,
            garage_dir: None,
            garage_config: None,
            garage_bin: None,
            garage_endpoint: "http://127.0.0.1:3900".to_string(),
            garage_bucket: "dharma-vault-test".to_string(),
            garage_access_key: None,
            garage_secret_key: None,
            arlocal: None,
            arlocal_dir: None,
            arlocal_ready: false,
            arweave_endpoint: "http://127.0.0.1:1984".to_string(),
        };
        services.start_garage(renderer)?;
        services.start_arlocal(renderer)?;
        Ok(services)
    }

    fn start_garage(&mut self, renderer: &mut dyn Renderer) -> Result<(), TestFailure> {
        let bin = garage_bin();
        let Some(bin) = bin else {
            renderer.log("garage not found, skipping S3 external test");
            return Ok(());
        };
        let dir = TempDir::new().map_err(|err| TestFailure {
            property: "EXTERNAL-S3-000",
            seed: 0,
            details: format!("garage tempdir failed: {err}"),
            trace: None,
        })?;
        let meta_dir = dir.path().join("meta");
        let data_dir = dir.path().join("data");
        fs::create_dir_all(&meta_dir).map_err(|err| TestFailure {
            property: "EXTERNAL-S3-000",
            seed: 0,
            details: format!("garage meta dir failed: {err}"),
            trace: None,
        })?;
        fs::create_dir_all(&data_dir).map_err(|err| TestFailure {
            property: "EXTERNAL-S3-000",
            seed: 0,
            details: format!("garage data dir failed: {err}"),
            trace: None,
        })?;
        let rpc_secret = random_hex(32);
        let admin_token = random_hex(32);
        let config = format!(
            "metadata_dir = \"{}\"\n\
data_dir = \"{}\"\n\
\n\
replication_factor = 1\n\
\n\
rpc_bind_addr = \"127.0.0.1:3901\"\n\
rpc_public_addr = \"127.0.0.1:3901\"\n\
rpc_secret = \"{}\"\n\
\n\
[s3_api]\n\
api_bind_addr = \"127.0.0.1:3900\"\n\
s3_region = \"garage\"\n\
root_domain = \".s3.garage.localhost\"\n\
\n\
[admin]\n\
api_bind_addr = \"127.0.0.1:3903\"\n\
admin_token = \"{}\"\n",
            meta_dir.display(),
            data_dir.display(),
            rpc_secret,
            admin_token
        );
        let config_path = dir.path().join("garage.toml");
        fs::write(&config_path, config).map_err(|err| TestFailure {
            property: "EXTERNAL-S3-000",
            seed: 0,
            details: format!("garage config write failed: {err}"),
            trace: None,
        })?;
        let mut cmd = Command::new(&bin);
        cmd.arg("-c")
            .arg(&config_path)
            .arg("server")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = cmd.spawn().map_err(|err| TestFailure {
            property: "EXTERNAL-S3-000",
            seed: 0,
            details: format!("garage launch failed: {err}"),
            trace: None,
        })?;
        wait_for_port("127.0.0.1", 3901, Duration::from_secs(10)).map_err(|err| TestFailure {
            property: "EXTERNAL-S3-000",
            seed: 0,
            details: format!("garage rpc not ready: {err}"),
            trace: None,
        })?;
        wait_for_port("127.0.0.1", 3900, Duration::from_secs(10)).map_err(|err| TestFailure {
            property: "EXTERNAL-S3-000",
            seed: 0,
            details: format!("garage s3 not ready: {err}"),
            trace: None,
        })?;
        self.garage = Some(child);
        self.garage_dir = Some(dir);
        self.garage_config = Some(config_path);
        self.garage_bin = Some(bin);
        self.bootstrap_garage(renderer)?;
        renderer.log("garage ready on 127.0.0.1:3900");
        Ok(())
    }

    fn bootstrap_garage(&mut self, renderer: &mut dyn Renderer) -> Result<(), TestFailure> {
        let status = self.garage_cmd(&["status"])?;
        let node_id = parse_garage_node_id(&status).ok_or_else(|| TestFailure {
            property: "EXTERNAL-S3-000",
            seed: 0,
            details: "unable to parse garage node id".to_string(),
            trace: Some(vec![status]),
        })?;
        self.garage_cmd(&["layout", "assign", "-z", "dc1", "-c", "1G", &node_id])?;
        self.garage_cmd(&["layout", "apply", "--version", "1"])?;
        self.garage_cmd(&["bucket", "create", &self.garage_bucket])?;
        let key_output = self.garage_cmd(&["key", "create", "dharma-test-key"])?;
        let (key_id, secret) = parse_garage_key(&key_output).ok_or_else(|| TestFailure {
            property: "EXTERNAL-S3-000",
            seed: 0,
            details: "unable to parse garage key".to_string(),
            trace: Some(vec![key_output]),
        })?;
        self.garage_cmd(&[
            "bucket",
            "allow",
            "--read",
            "--write",
            "--owner",
            &self.garage_bucket,
            "--key",
            &key_id,
        ])?;
        self.garage_access_key = Some(key_id);
        self.garage_secret_key = Some(secret);
        renderer.log("garage bucket/key configured");
        Ok(())
    }

    fn garage_cmd(&self, args: &[&str]) -> Result<String, TestFailure> {
        let Some(bin) = &self.garage_bin else {
            return Err(TestFailure {
                property: "EXTERNAL-S3-000",
                seed: 0,
                details: "garage binary missing".to_string(),
                trace: None,
            });
        };
        let Some(config) = &self.garage_config else {
            return Err(TestFailure {
                property: "EXTERNAL-S3-000",
                seed: 0,
                details: "garage config missing".to_string(),
                trace: None,
            });
        };
        let output = Command::new(bin)
            .arg("-c")
            .arg(config)
            .args(args)
            .output()
            .map_err(|err| TestFailure {
                property: "EXTERNAL-S3-000",
                seed: 0,
                details: format!("garage command failed: {err}"),
                trace: None,
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            return Err(TestFailure {
                property: "EXTERNAL-S3-000",
                seed: 0,
                details: format!("garage {:?} failed", args),
                trace: Some(vec![stdout, stderr]),
            });
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    fn start_arlocal(&mut self, renderer: &mut dyn Renderer) -> Result<(), TestFailure> {
        let port = free_port().unwrap_or(1984);
        let dir = TempDir::new().map_err(|err| TestFailure {
            property: "EXTERNAL-ARWEAVE-000",
            seed: 0,
            details: format!("arlocal tempdir failed: {err}"),
            trace: None,
        })?;
        let db_path = dir.path().join("db");
        let mut cmd = if command_exists("arlocal") {
            let mut cmd = Command::new("arlocal");
            cmd.arg(port.to_string())
                .arg("--dbpath")
                .arg(db_path.to_string_lossy().to_string());
            cmd
        } else if command_exists("npx") {
            let mut cmd = Command::new("npx");
            cmd.arg("--yes")
                .arg("arlocal")
                .arg(port.to_string())
                .arg("--dbpath")
                .arg(db_path.to_string_lossy().to_string());
            cmd
        } else {
            renderer.log("arlocal/npx not found, skipping arweave");
            return Ok(());
        };
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
        let child = cmd.spawn().map_err(|err| TestFailure {
            property: "EXTERNAL-ARWEAVE-000",
            seed: 0,
            details: format!("arlocal launch failed: {err}"),
            trace: None,
        })?;
        wait_for_port("127.0.0.1", port, Duration::from_secs(10)).map_err(|err| TestFailure {
            property: "EXTERNAL-ARWEAVE-000",
            seed: 0,
            details: format!("arlocal not ready: {err}"),
            trace: None,
        })?;
        wait_for_http_ok("127.0.0.1", port, "/info", Duration::from_secs(10)).map_err(|err| {
            TestFailure {
                property: "EXTERNAL-ARWEAVE-000",
                seed: 0,
                details: format!("arlocal /info not ready: {err}"),
                trace: None,
            }
        })?;
        renderer.log(&format!("arlocal ready on 127.0.0.1:{port}"));
        self.arlocal = Some(child);
        self.arlocal_dir = Some(dir);
        self.arlocal_ready = true;
        self.arweave_endpoint = format!("http://127.0.0.1:{port}");
        Ok(())
    }
}

#[cfg(feature = "external")]
impl Drop for ExternalServices {
    fn drop(&mut self) {
        if let Some(child) = self.garage.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(child) = self.arlocal.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(feature = "external")]
fn external_s3_test(
    renderer: &mut dyn Renderer,
    status: &mut Status,
    services: &ExternalServices,
) -> Result<(), TestFailure> {
    use dharma_core::types::{ContractId, SchemaId, SubjectId};
    use dharma_core::vault::drivers::s3::S3Options;
    use dharma_core::vault::drivers::S3Driver;
    use dharma_core::vault::{VaultDictionaryRef, VaultDriver, VaultItem, VaultSegment};
    use rand_chacha::ChaCha20Rng;
    use rand_core::SeedableRng;

    if services.garage.is_none() {
        renderer.log("skipping S3 test (garage not running)");
        return Ok(());
    }

    status.current = Some("EXTERNAL-S3".to_string());
    renderer.update(status);

    let subject = SubjectId::from_bytes([1u8; 32]);
    let schema = SchemaId::from_bytes([2u8; 32]);
    let contract = ContractId::from_bytes([3u8; 32]);
    let assertions = vec![VaultItem {
        seq: 1,
        bytes: b"ext-s3".to_vec(),
    }];
    let segment = VaultSegment::new(subject, schema, contract, assertions, b"snap".to_vec())
        .map_err(|err| TestFailure {
            property: "EXTERNAL-S3-001",
            seed: 0,
            details: format!("segment build failed: {err}"),
            trace: None,
        })?;
    let mut rng = ChaCha20Rng::seed_from_u64(99);
    let svk = [9u8; 32];
    let chunk = segment
        .seal(&svk, VaultDictionaryRef::None, &mut rng)
        .map_err(|err| TestFailure {
            property: "EXTERNAL-S3-001",
            seed: 0,
            details: format!("seal failed: {err}"),
            trace: None,
        })?;

    let Some(access) = services.garage_access_key.as_ref() else {
        return Err(TestFailure {
            property: "EXTERNAL-S3-001",
            seed: 0,
            details: "garage access key missing".to_string(),
            trace: None,
        });
    };
    let Some(secret) = services.garage_secret_key.as_ref() else {
        return Err(TestFailure {
            property: "EXTERNAL-S3-001",
            seed: 0,
            details: "garage secret key missing".to_string(),
            trace: None,
        });
    };
    std::env::set_var("AWS_ACCESS_KEY_ID", access);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", secret);
    std::env::set_var("AWS_REGION", "garage");

    let bucket = services.garage_bucket.as_str();
    let options = S3Options {
        endpoint_url: Some(services.garage_endpoint.clone()),
        force_path_style: true,
        region: Some("garage".to_string()),
    };
    let driver =
        S3Driver::new_with_options(bucket, "vault", options).map_err(|err| TestFailure {
            property: "EXTERNAL-S3-001",
            seed: 0,
            details: format!("driver init failed: {err}"),
            trace: None,
        })?;
    driver.ensure_bucket().map_err(|err| TestFailure {
        property: "EXTERNAL-S3-001",
        seed: 0,
        details: format!("ensure bucket failed: {err}"),
        trace: None,
    })?;
    driver
        .put_chunk_verified(&chunk, &svk, None)
        .map_err(|err| TestFailure {
            property: "EXTERNAL-S3-001",
            seed: 0,
            details: format!("put+verify failed: {err}"),
            trace: None,
        })?;
    renderer.log("S3 integration OK");
    Ok(())
}

#[cfg(feature = "external")]
fn external_arweave_test(
    renderer: &mut dyn Renderer,
    status: &mut Status,
    services: &ExternalServices,
) -> Result<(), TestFailure> {
    use dharma_core::types::{ContractId, SchemaId, SubjectId};
    use dharma_core::vault::drivers::ArweaveDriver;
    use dharma_core::vault::{VaultDictionaryRef, VaultDriver, VaultItem, VaultSegment};
    use rand_chacha::ChaCha20Rng;
    use rand_core::SeedableRng;

    if !services.arlocal_ready {
        renderer.log("skipping Arweave test (arlocal not ready)");
        return Ok(());
    }

    status.current = Some("EXTERNAL-ARWEAVE".to_string());
    renderer.update(status);

    let subject = SubjectId::from_bytes([4u8; 32]);
    let schema = SchemaId::from_bytes([5u8; 32]);
    let contract = ContractId::from_bytes([6u8; 32]);
    let assertions = vec![VaultItem {
        seq: 1,
        bytes: b"ext-arweave".to_vec(),
    }];
    let segment = VaultSegment::new(subject, schema, contract, assertions, b"snap".to_vec())
        .map_err(|err| TestFailure {
            property: "EXTERNAL-ARWEAVE-001",
            seed: 0,
            details: format!("segment build failed: {err}"),
            trace: None,
        })?;
    let mut rng = ChaCha20Rng::seed_from_u64(55);
    let svk = [7u8; 32];
    let chunk = segment
        .seal(&svk, VaultDictionaryRef::None, &mut rng)
        .map_err(|err| TestFailure {
            property: "EXTERNAL-ARWEAVE-001",
            seed: 0,
            details: format!("seal failed: {err}"),
            trace: None,
        })?;

    let driver = ArweaveDriver::new_arlocal(services.arweave_endpoint.clone()).map_err(|err| {
        TestFailure {
            property: "EXTERNAL-ARWEAVE-001",
            seed: 0,
            details: format!("driver init failed: {err}"),
            trace: None,
        }
    })?;
    driver
        .put_chunk_verified(&chunk, &svk, None)
        .map_err(|err| TestFailure {
            property: "EXTERNAL-ARWEAVE-001",
            seed: 0,
            details: format!("put+verify failed: {err}"),
            trace: None,
        })?;
    renderer.log("Arweave integration OK");
    Ok(())
}

#[cfg(feature = "external")]
fn command_exists(cmd: &str) -> bool {
    Command::new(cmd)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

#[cfg(feature = "external")]
fn command_exists_with(cmd: &str, args: &[&str]) -> bool {
    let mut command = Command::new(cmd);
    command
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command.status().is_ok()
}

#[cfg(feature = "external")]
fn garage_bin() -> Option<String> {
    if let Ok(bin) = std::env::var("GARAGE_BIN") {
        if command_exists_with(&bin, &["--version"]) || command_exists_with(&bin, &["-V"]) {
            return Some(bin);
        }
    }
    if command_exists_with("garage", &["--version"]) || command_exists_with("garage", &["-V"]) {
        return Some("garage".to_string());
    }
    None
}

#[cfg(feature = "external")]
fn random_hex(len_bytes: usize) -> String {
    let mut buf = vec![0u8; len_bytes];
    rand_core::OsRng.fill_bytes(&mut buf);
    hex::encode(buf)
}

#[cfg(feature = "external")]
fn parse_garage_node_id(status: &str) -> Option<String> {
    for line in status.lines() {
        if line.contains("127.0.0.1:3901") {
            let id = line.split_whitespace().next()?;
            if !id.is_empty() {
                return Some(id.to_string());
            }
        }
    }
    let mut found = false;
    for line in status.lines() {
        if line.contains("HEALTHY NODES") {
            found = true;
            continue;
        }
        if found {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let id = trimmed.split_whitespace().next()?;
            return Some(id.to_string());
        }
    }
    None
}

#[cfg(feature = "external")]
fn parse_garage_key(output: &str) -> Option<(String, String)> {
    let mut key_id = None;
    let mut secret = None;
    for line in output.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("Key ID:") {
            key_id = Some(rest.trim().to_string());
        } else if let Some(rest) = trimmed.strip_prefix("Secret key:") {
            secret = Some(rest.trim().to_string());
        }
    }
    match (key_id, secret) {
        (Some(id), Some(sec)) => Some((id, sec)),
        _ => None,
    }
}

#[cfg(feature = "external")]
fn wait_for_port(host: &str, port: u16, timeout: Duration) -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect((host, port)).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(200));
    }
    Err(format!("timeout waiting for {host}:{port}"))
}

#[cfg(feature = "external")]
fn wait_for_http_ok(host: &str, port: u16, path: &str, timeout: Duration) -> Result<(), String> {
    let start = Instant::now();
    let mut last = None;
    while start.elapsed() < timeout {
        match http_get(host, port, path) {
            Ok((status, _body)) if status == 200 => return Ok(()),
            Ok((status, _body)) => last = Some(format!("status {status}")),
            Err(err) => last = Some(err),
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(last.unwrap_or_else(|| "timed out".to_string()))
}

#[cfg(feature = "external")]
fn http_get(host: &str, port: u16, path: &str) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect((host, port)).map_err(|e| e.to_string())?;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\n\r\n",
        path, host, port
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf);
    let mut parts = text.split("\r\n\r\n");
    let head = parts.next().unwrap_or("");
    let body = parts.next().unwrap_or("").to_string();
    let mut head_lines = head.lines();
    let status_line = head_lines.next().ok_or("no status line")?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or("no status code")?
        .parse::<u16>()
        .map_err(|e| e.to_string())?;
    Ok((status, body))
}

fn run_properties(
    seed: u64,
    opts: &TestOptions,
    renderer: &mut dyn Renderer,
    status: &mut Status,
) -> Result<(), TestFailure> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let iters = opts.iterations();
    let properties: &[(&str, fn(&mut ChaCha20Rng, usize) -> Result<(), String>)] = &[
        ("P-CBOR-001", prop_cbor_roundtrip),
        ("P-CBOR-002", prop_cbor_rejects_noncanonical),
        ("P-ASSERT-001", prop_assert_signature_roundtrip),
        ("P-ASSERT-002", prop_assert_signature_rejects_bitflip),
        ("P-ASSERT-003", prop_assert_structural_rejects_seq_prev),
        ("P-ENV-001", prop_envelope_roundtrip),
        ("P-ENV-002", prop_envelope_wrong_key),
        ("P-ENV-003", prop_envelope_id_changes_on_mutation),
        ("P-ID-001", prop_identity_genesis_race),
        ("P-ID-002", prop_identity_lifecycle_requires_root),
        ("P-ID-003", prop_identity_revoked_terminal),
        ("P-ID-004", prop_identity_peer_verification),
        ("P-ID-005", prop_identity_root_recovery),
        ("P-DOM-001", prop_domain_membership_ordering),
        ("P-DOM-002", prop_domain_acting_context),
        ("P-DOM-003", prop_directory_parent_authorization),
        ("P-KEY-001", prop_key_epoch_monotonic),
        ("P-KEY-002", prop_key_grant_requires_member),
        ("P-KEY-003", prop_key_revoked_boundary),
        ("P-KEY-004", prop_key_grant_tampered_envelope),
        ("P-EMER-001", prop_emergency_freeze_blocks),
        ("P-EMER-002", prop_emergency_unfreeze_compromise),
        ("P-EMER-003", prop_emergency_device_revoke),
        ("P-PERM-001", prop_permission_summary_denies_fast_reject),
        ("P-PERM-002", prop_permission_summary_allows_contract_reject),
        ("P-PERM-003", prop_permission_summary_corrupt_fallback),
        ("P-PERM-004", prop_permission_summary_version_mismatch),
        ("P-FAB-001", prop_fabric_ad_tamper),
        ("P-FAB-002", prop_fabric_token_scope),
        ("P-FAB-003", prop_fabric_directory_split_brain),
        ("P-FAB-004", prop_fabric_ad_expiry_ttl),
        ("P-FAB-005", prop_fabric_token_expiry),
        ("P-IAM-001", prop_iam_owner_visibility),
        ("P-IAM-002", prop_iam_contact_visibility),
        ("P-IAM-003", prop_iam_non_contact_redaction),
        ("P-IAM-004", prop_iam_declined_blocked_redaction),
        ("P-DAG-001", prop_dag_ordering_respects_deps),
        ("P-DAG-002", prop_dag_detects_cycle),
        ("P-STORE-001", prop_store_ingest_idempotent),
        ("P-STORE-002", prop_store_frontier_deterministic),
        ("P-DET-001", prop_replay_deterministic),
        ("P-CONV-001", prop_convergence),
        ("P-CQRS-001", prop_cqrs_replay_deterministic),
        ("P-CQRS-002", prop_cqrs_decode_deterministic),
        ("D-Q-001", prop_dharmaq_rebuild_deterministic),
        ("D-Q-002", prop_dharmaq_and_commutative),
    ];
    for (name, prop) in properties {
        status.current = Some((*name).to_string());
        status.iteration = 0;
        status.iterations = iters;
        renderer.update(status);
        if let Err(err) = prop(&mut rng, iters) {
            return Err(TestFailure {
                property: name,
                seed,
                details: err,
                trace: None,
            });
        }
        status.iteration = iters;
        renderer.update(status);
    }
    Ok(())
}

fn run_relay_properties(
    seed: u64,
    renderer: &mut dyn Renderer,
    status: &mut Status,
) -> Result<(), TestFailure> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let properties: &[(
        &str,
        fn(&mut ChaCha20Rng, &mut Vec<String>) -> Result<(), String>,
    )] = &[
        ("P-RELAY-001", prop_relay_baseline),
        ("P-RELAY-002", prop_relay_identity_root),
    ];
    for (name, prop) in properties {
        status.current = Some((*name).to_string());
        renderer.update(status);
        let mut trace = Vec::new();
        if let Err(err) = prop(&mut rng, &mut trace) {
            return Err(TestFailure {
                property: name,
                seed,
                details: err,
                trace: Some(trace),
            });
        }
    }
    Ok(())
}

fn run_vectors(renderer: &mut dyn Renderer, status: &mut Status) -> Result<(), TestFailure> {
    let root = std::env::current_dir().map_err(|err| TestFailure {
        property: "VECTOR-ROOT",
        seed: 0,
        details: err.to_string(),
        trace: None,
    })?;
    let vectors_root = root.join("tests").join("vectors");
    if !vectors_root.exists() {
        return Ok(());
    }
    let mut failures = Vec::new();
    for entry in fs::read_dir(&vectors_root).map_err(|err| TestFailure {
        property: "VECTOR-READ",
        seed: 0,
        details: err.to_string(),
        trace: None,
    })? {
        let entry = entry.map_err(|err| TestFailure {
            property: "VECTOR-READ",
            seed: 0,
            details: err.to_string(),
            trace: None,
        })?;
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let category = entry.file_name().to_string_lossy().to_string();
        let dir = entry.path();
        let metas = match list_meta_files(&dir) {
            Ok(metas) => metas,
            Err(err) => {
                failures.push(TestFailure {
                    property: "VECTOR-META",
                    seed: 0,
                    details: err.to_string(),
                    trace: None,
                });
                continue;
            }
        };
        let total = metas.len();
        let mut idx = 0usize;
        for meta_path in metas {
            idx = idx.saturating_add(1);
            status.current = Some(format!(
                "{}/{}",
                category,
                meta_path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("vector")
            ));
            status.iteration = idx;
            status.iterations = total;
            renderer.update(status);
            if let Err(err) = run_vector_case(&category, &meta_path) {
                failures.push(err);
            }
        }
    }
    if let Some(first) = failures.into_iter().next() {
        return Err(first);
    }
    Ok(())
}

fn run_simulation(
    seed: u64,
    opts: &TestOptions,
    renderer: &mut dyn Renderer,
    status: &mut Status,
) -> Result<(), TestFailure> {
    status.current = Some("sim".to_string());
    renderer.update(status);
    match sim_convergence(seed, opts, renderer, status, false) {
        Ok(()) => Ok(()),
        Err((details, trace)) => {
            let mut full_trace = trace;
            if let Err((deep_details, deep_trace)) =
                sim_convergence(seed, opts, renderer, status, true)
            {
                if !deep_trace.is_empty() {
                    full_trace = deep_trace;
                }
                if deep_details != details {
                    full_trace.push(format!("deep_trace_details: {}", deep_details));
                }
            } else {
                full_trace.push("deep_trace_replay: ok".to_string());
            }
            Err(TestFailure {
                property: "P-SIM-001",
                seed,
                details,
                trace: Some(full_trace),
            })
        }
    }
}

fn sim_convergence(
    seed: u64,
    opts: &TestOptions,
    renderer: &mut dyn Renderer,
    status: &mut Status,
    deep_trace: bool,
) -> Result<(), (String, Vec<String>)> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed ^ 0x5a5a_1234);
    let hub = SimHub::new(rng.next_u64());
    let mut trace_extra: Vec<String> = Vec::new();
    touch_log();
    let trace_sink = if deep_trace {
        Some(Arc::new(TraceSink::new(20000)))
    } else {
        None
    };
    let trace_ref = trace_sink.as_ref().map(|sink| sink.as_ref());
    let mut timeline = FaultTimeline::new();
    record_trace(
        &mut trace_extra,
        renderer,
        format!(
            "sim_start seed={} deep={} chaos={} trace={}",
            seed,
            opts.deep,
            opts.chaos,
            if deep_trace { "deep" } else { "normal" }
        ),
    );
    if opts.chaos {
        let warmup_faults = FaultConfig {
            drop_rate: 0.0,
            dup_rate: 0.0,
            min_delay: 1,
            max_delay: 1,
        };
        let net_faults = FaultConfig {
            drop_rate: 0.1,
            dup_rate: 0.05,
            min_delay: 0,
            max_delay: 5,
        };
        let warmup_end = 4;
        timeline.schedule_net(0, warmup_faults.clone());
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script net t=0 drop={} dup={} min_delay={} max_delay={}",
                warmup_faults.drop_rate,
                warmup_faults.dup_rate,
                warmup_faults.min_delay,
                warmup_faults.max_delay
            ),
        );
        timeline.schedule_net(warmup_end, net_faults.clone());
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script net t={} drop={} dup={} min_delay={} max_delay={}",
                warmup_end,
                net_faults.drop_rate,
                net_faults.dup_rate,
                net_faults.min_delay,
                net_faults.max_delay
            ),
        );
        let clear_at = warmup_end + 25 + (rng.next_u32() % 25) as i64;
        timeline.schedule_net(clear_at, FaultConfig::default());
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script net t={} drop=0 dup=0 min_delay=0 max_delay=0",
                clear_at
            ),
        );
    }

    let mut nodes = Vec::new();
    for _ in 0..3 {
        let mut env = SimEnv::new(rng.next_u64(), 0);
        if let Some(trace) = trace_sink.as_ref() {
            env.enable_trace(Arc::clone(trace));
        }
        let mut clock_cfg = None;
        let mut fs_cfg = None;
        if opts.chaos {
            fs_cfg = Some(FsFaultConfig {
                enospc_after: Some(64 * 1024),
                torn_every: Some(17),
                torn_max: 32,
                fsync_lie_every: Some(19),
                read_corrupt_every: Some(23),
                read_corrupt_bits: 0x01,
            });
            let drift = (rng.next_u32() % 3) as i64;
            let jump_every = 5 + (rng.next_u32() % 5) as u64;
            let jump_amount = (rng.next_u32() % 20) as i64;
            let mono_every = 7 + (rng.next_u32() % 5) as u64;
            let backstep = 1 + (rng.next_u32() % 3) as i64;
            clock_cfg = Some((drift, jump_every, jump_amount, mono_every, backstep));
        }
        let store = Store::new(&env);
        let keys = Keyring::new();
        let index = FrontierIndex::build(&store, &keys)
            .map_err(|e| (e.to_string(), merge_trace(&trace_extra, &hub, trace_ref)))?;
        let id = hub.register_node();
        record_trace(
            &mut trace_extra,
            renderer,
            format!("node_init id={} root={}", id, env.root().display()),
        );
        if let Some(cfg) = fs_cfg.clone() {
            timeline.schedule_fs(0, id, cfg.clone());
            record_trace(
                &mut trace_extra,
                renderer,
                format!(
                    "fault_script fs t=0 node={} enospc_after={:?} torn_every={:?} torn_max={} fsync_lie_every={:?} read_corrupt_every={:?} read_corrupt_bits={}",
                    id,
                    cfg.enospc_after,
                    cfg.torn_every,
                    cfg.torn_max,
                    cfg.fsync_lie_every,
                    cfg.read_corrupt_every,
                    cfg.read_corrupt_bits
                ),
            );
        }
        if let Some((drift, jump_every, jump_amount, mono_every, backstep)) = clock_cfg {
            timeline.schedule_clock(
                0,
                id,
                ClockFaultConfig {
                    drift_per_call: drift,
                    jump_every: Some(jump_every),
                    jump_amount,
                    monotonic_violation_every: Some(mono_every),
                    monotonic_backstep: backstep,
                },
            );
            record_trace(
                &mut trace_extra,
                renderer,
                format!(
                    "clock_faults node={} drift={} jump_every={} jump_amount={} mono_every={} backstep={}",
                    id, drift, jump_every, jump_amount, mono_every, backstep
                ),
            );
        }
        let identity = make_identity(&mut rng);
        nodes.push(SimNode {
            id,
            env,
            store,
            index: Arc::new(Mutex::new(index)),
            schema_id: None,
            contract_id: None,
            identity,
        });
    }

    for node in &mut nodes {
        let (schema_id, contract_id) = store_cqrs_artifacts(&node.store)
            .map_err(|e| (e, merge_trace(&trace_extra, &hub, trace_ref)))?;
        node.schema_id = Some(schema_id);
        node.contract_id = Some(contract_id);
    }

    let mut identity_genesis = Vec::with_capacity(nodes.len());
    for idx in 0..nodes.len() {
        let mut index = nodes[idx].index.lock().unwrap();
        let genesis_id = seed_identity(
            &nodes[idx].store,
            &mut index,
            &nodes[idx].identity,
            &mut |line| {
                record_trace(&mut trace_extra, renderer, line);
            },
        )
        .map_err(|e| (e, merge_trace(&trace_extra, &hub, trace_ref)))?;
        identity_genesis.push(genesis_id);
    }

    for idx in 0..nodes.len() {
        let subject = nodes[idx].identity.subject_id;
        let start_seq = 2;
        let prev = Some(identity_genesis[idx]);
        nodes[idx]
            .create_chain(subject, 3, start_seq, prev, &mut |line| {
                record_trace(&mut trace_extra, renderer, line);
            })
            .map_err(|e| (e, merge_trace(&trace_extra, &hub, trace_ref)))?;
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "node_chain node={} subject={} count=3",
                nodes[idx].id,
                subject.to_hex()
            ),
        );
    }

    status.nodes = nodes.len();
    renderer.update(status);

    let mut timeline_nodes: HashMap<NodeId, TimelineNode> = HashMap::new();
    for node in &nodes {
        timeline_nodes.insert(
            node.id,
            TimelineNode {
                env: node.env.clone(),
                store: node.store.clone(),
                index: node.index.clone(),
            },
        );
    }

    if opts.chaos {
        let crash_id = nodes[1].id;
        let crash_at = 5 + (rng.next_u32() % 5) as i64;
        let restart_at = crash_at + 5 + (rng.next_u32() % 10) as i64;
        timeline.schedule_online(crash_at, crash_id, false);
        timeline.schedule_online(restart_at, crash_id, true);
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script online t={} node={} state=down",
                crash_at, crash_id
            ),
        );
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script online t={} node={} state=up",
                restart_at, crash_id
            ),
        );

        let part_at = 3 + (rng.next_u32() % 5) as i64;
        let heal_at = part_at + 4 + (rng.next_u32() % 6) as i64;
        let a = nodes[0].id;
        let b = nodes[2].id;
        timeline.schedule_partition(part_at, a, b, true);
        timeline.schedule_partition(heal_at, a, b, false);
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script partition t={} a={} b={} state=blocked",
                part_at, a, b
            ),
        );
        record_trace(
            &mut trace_extra,
            renderer,
            format!(
                "fault_script partition t={} a={} b={} state=open",
                heal_at, a, b
            ),
        );
    }

    if opts.chaos {
        apply_timeline(&mut timeline, hub.now(), &hub, &timeline_nodes);
    }

    let _peer_ids: Vec<_> = nodes.iter().map(|n| n.id).collect();
    let running = Arc::new(AtomicBool::new(true));
    let hub_runner = hub.clone();
    let running_runner = running.clone();
    let mut timeline_runner = timeline;
    let timeline_nodes_runner = timeline_nodes.clone();
    let runner = thread::spawn(move || {
        while running_runner.load(Ordering::SeqCst) {
            let stepped = hub_runner.step();
            let now = hub_runner.now();
            apply_timeline(
                &mut timeline_runner,
                now,
                &hub_runner,
                &timeline_nodes_runner,
            );
            if !stepped {
                thread::yield_now();
            }
        }
        let now = hub_runner.now();
        apply_timeline(
            &mut timeline_runner,
            now,
            &hub_runner,
            &timeline_nodes_runner,
        );
        hub_runner.step();
    });

    let sync_timeout = if opts.chaos {
        Duration::from_secs(10)
    } else {
        Duration::from_secs(5)
    };
    let max_attempts = if opts.chaos { 4 } else { 1 };
    let mut sync_pair = |a: &SimNode, b: &SimNode| -> Result<(), (String, Vec<String>)> {
        for attempt in 1..=max_attempts {
            record_trace(
                &mut trace_extra,
                renderer,
                format!("sync_pair a={} b={} attempt={}", a.id, b.id, attempt),
            );
            match sync_pair_real(a, b, hub.clone(), sync_timeout) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if opts.chaos && is_transient_sync_error(&err) {
                        record_trace(
                            &mut trace_extra,
                            renderer,
                            format!(
                                "sync_pair_err a={} b={} attempt={} err={}",
                                a.id, b.id, attempt, err
                            ),
                        );
                        if attempt < max_attempts {
                            thread::sleep(Duration::from_millis(10));
                            continue;
                        }
                        return Ok(());
                    }
                    return Err((err, merge_trace(&trace_extra, &hub, trace_ref)));
                }
            }
        }
        Ok(())
    };

    sync_pair(&nodes[0], &nodes[1])?;
    sync_pair(&nodes[1], &nodes[2])?;
    sync_pair(&nodes[0], &nodes[2])?;

    if opts.chaos {
        sync_pair(&nodes[0], &nodes[2])?;
        sync_pair(&nodes[0], &nodes[1])?;
        sync_pair(&nodes[1], &nodes[2])?;
    }

    running.store(false, Ordering::SeqCst);
    runner.join().map_err(|_| {
        (
            "net runner failed".to_string(),
            merge_trace(&trace_extra, &hub, trace_ref),
        )
    })?;

    let expected =
        snapshot_env(&nodes[0].env).map_err(|e| (e, merge_trace(&trace_extra, &hub, trace_ref)))?;
    for node in nodes.iter().skip(1) {
        let snapshot =
            snapshot_env(&node.env).map_err(|e| (e, merge_trace(&trace_extra, &hub, trace_ref)))?;
        if snapshot != expected {
            let diff = diff_snapshots(&expected, &snapshot);
            return Err((
                format!("simulation convergence mismatch (node {}): {diff}", node.id),
                merge_trace(&trace_extra, &hub, trace_ref),
            ));
        }
    }
    Ok(())
}

fn record_trace(trace: &mut Vec<String>, renderer: &mut dyn Renderer, line: String) {
    trace.push(line.clone());
    renderer.log(&line);
    touch_log();
}

fn touch_log() {
    if let Ok(mut guard) = LAST_LOG.lock() {
        *guard = Some(Instant::now());
    }
}

fn log_stale(timeout: Duration) -> bool {
    let Ok(guard) = LAST_LOG.lock() else {
        return false;
    };
    match *guard {
        Some(ts) => ts.elapsed() > timeout,
        None => false,
    }
}

fn seed_identity(
    store: &Store,
    index: &mut FrontierIndex,
    identity: &dharma_core::identity::IdentityState,
    trace: &mut dyn FnMut(String),
) -> Result<AssertionId, String> {
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(store).map_err(|e| e.to_string())?;
    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: identity.subject_id,
        typ: "core.genesis".to_string(),
        auth: identity.root_public_key,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: Some("identity genesis".to_string()),
        meta: add_signer_meta(None, &identity.subject_id),
    };
    let body = ciborium::value::Value::Map(vec![]);
    let assertion = AssertionPlaintext::sign(header, body, &identity.root_signing_key)
        .map_err(|e| e.to_string())?;
    let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
    let envelope_id = crypto::envelope_id(&bytes);
    let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
    store
        .put_assertion(&identity.subject_id, &envelope_id, &bytes)
        .map_err(|e| e.to_string())?;
    store
        .record_semantic(&assertion_id, &envelope_id)
        .map_err(|e| e.to_string())?;
    append_assertion(
        store.env(),
        &identity.subject_id,
        1,
        assertion_id,
        envelope_id,
        "core.genesis",
        &bytes,
    )
    .map_err(|e| e.to_string())?;
    index
        .update(assertion_id, envelope_id, &assertion.header)
        .map_err(|e| e.to_string())?;
    trace(format!(
        "identity_seed subject={} id={}",
        identity.subject_id.to_hex(),
        assertion_id.to_hex()
    ));
    Ok(assertion_id)
}

fn apply_timeline(
    timeline: &mut FaultTimeline,
    now: i64,
    hub: &SimHub,
    nodes: &HashMap<NodeId, TimelineNode>,
) {
    for event in timeline.drain_ready(now) {
        match event {
            FaultEvent::Net(config) => {
                hub.set_faults(config);
            }
            FaultEvent::Partition { a, b, blocked } => {
                hub.set_partition(a, b, blocked);
            }
            FaultEvent::Online { node, online } => {
                hub.set_online(node, online);
                if online {
                    if let Some(handle) = nodes.get(&node) {
                        let keys = Keyring::new();
                        if let Ok(rebuilt) = FrontierIndex::build(&handle.store, &keys) {
                            if let Ok(mut guard) = handle.index.lock() {
                                *guard = rebuilt;
                            }
                        }
                    }
                }
            }
            FaultEvent::Fs { node, config } => {
                if let Some(handle) = nodes.get(&node) {
                    handle.env.set_fs_faults(config);
                }
            }
            FaultEvent::Clock { node, config } => {
                if let Some(handle) = nodes.get(&node) {
                    handle.env.set_clock_faults(config);
                }
            }
        }
    }
}

fn merge_trace(extra: &[String], hub: &SimHub, sink: Option<&TraceSink>) -> Vec<String> {
    let mut trace = Vec::new();
    if !extra.is_empty() {
        trace.extend(extra.iter().cloned());
    }
    if let Some(sink) = sink {
        for line in sink.snapshot() {
            trace.push(format!("io {}", line));
        }
    }
    trace.extend(hub.trace_snapshot());
    trace
}

#[derive(Clone)]
struct TimelineNode {
    env: SimEnv,
    store: Store,
    index: Arc<Mutex<FrontierIndex>>,
}

struct SimNode {
    id: u64,
    env: SimEnv,
    store: Store,
    index: Arc<Mutex<FrontierIndex>>,
    schema_id: Option<SchemaId>,
    contract_id: Option<ContractId>,
    identity: dharma_core::identity::IdentityState,
}

impl SimNode {
    fn create_chain(
        &mut self,
        subject: SubjectId,
        count: u64,
        start_seq: u64,
        mut prev: Option<AssertionId>,
        trace: &mut dyn FnMut(String),
    ) -> Result<(), String> {
        let (schema_id, contract_id) =
            builtins::ensure_note_artifacts(&self.store).map_err(|e| e.to_string())?;
        let sk = &self.identity.root_signing_key;
        let pk = self.identity.root_public_key;
        for offset in 0..count {
            let seq = start_seq + offset;
            trace(format!(
                "assert_build node={} subject={} signer={} seq={} prev={}",
                self.id,
                subject.to_hex(),
                self.identity.subject_id.to_hex(),
                seq,
                prev.map(|id| id.to_hex())
                    .unwrap_or_else(|| "-".to_string())
            ));
            let header = AssertionHeader {
                v: crypto::PROTOCOL_VERSION,
                ver: DEFAULT_DATA_VERSION,
                sub: subject,
                typ: "note.text".to_string(),
                auth: pk,
                seq,
                prev,
                refs: vec![],
                ts: None,
                schema: schema_id,
                contract: contract_id,
                note: None,
                meta: add_signer_meta(None, &self.identity.subject_id),
            };
            let body = ciborium::value::Value::Map(vec![(
                ciborium::value::Value::Text("text".to_string()),
                ciborium::value::Value::Text(format!("value {}", seq)),
            )]);
            let assertion =
                AssertionPlaintext::sign(header, body, sk).map_err(|e| e.to_string())?;
            let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
            let mut index = self.index.lock().unwrap();
            let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
            let mut keys = Keyring::new();
            match ingest_object(&self.store, &mut index, &bytes, &mut keys) {
                Ok(_) => {
                    trace(format!(
                        "assert_ingest_ok node={} subject={} signer={} seq={} id={}",
                        self.id,
                        subject.to_hex(),
                        self.identity.subject_id.to_hex(),
                        seq,
                        assertion_id.to_hex()
                    ));
                }
                Err(err) => {
                    trace(format!(
                        "assert_ingest_err node={} subject={} signer={} seq={} id={} err={:?}",
                        self.id,
                        subject.to_hex(),
                        self.identity.subject_id.to_hex(),
                        seq,
                        assertion_id.to_hex(),
                        err
                    ));
                    return Err(format!("ingest failed: {err:?}"));
                }
            }
            prev = Some(assertion_id);
        }
        Ok(())
    }
}

fn sync_pair_real(
    a: &SimNode,
    b: &SimNode,
    hub: Arc<SimHub>,
    timeout: Duration,
) -> Result<(), String> {
    let (mut stream_a, mut stream_b, control) =
        dharma_sim::SimStream::pair(hub.clone(), a.id, b.id);
    let ready = Arc::new(AtomicUsize::new(0));
    let store_a = a.store.clone();
    let identity_a = a.identity.clone();
    let index_a = a.index.clone();
    let mut keys_a = Keyring::new();
    let ready_a = ready.clone();
    let thread_a = thread::spawn(move || {
        let policy = OverlayPolicy::default();
        let claims = PeerClaims::default();
        let access = OverlayAccess::new(&policy, None, false, &claims);
        let session = server_handshake(&mut stream_a, &identity_a).map_err(|e| e.to_string())?;
        let options = SyncOptions {
            relay: false,
            ad_store: None,
            local_subs: None,
            verbose: false,
            exit_on_idle: false,
            trace: None,
        };
        ready_a.fetch_add(1, Ordering::SeqCst);
        let mut index = index_a.lock().unwrap();
        sync_loop_with(
            &mut stream_a,
            session.0,
            &store_a,
            &mut index,
            &mut keys_a,
            &identity_a,
            &access,
            options,
        )
        .map_err(|e| e.to_string())
    });

    let store_b = b.store.clone();
    let identity_b = b.identity.clone();
    let index_b = b.index.clone();
    let mut keys_b = Keyring::new();
    let ready_b = ready.clone();
    let thread_b = thread::spawn(move || {
        let policy = OverlayPolicy::default();
        let claims = PeerClaims::default();
        let access = OverlayAccess::new(&policy, None, false, &claims);
        let session = client_handshake(&mut stream_b, &identity_b).map_err(|e| e.to_string())?;
        let options = SyncOptions {
            relay: false,
            ad_store: None,
            local_subs: None,
            verbose: false,
            exit_on_idle: false,
            trace: None,
        };
        ready_b.fetch_add(1, Ordering::SeqCst);
        let mut index = index_b.lock().unwrap();
        sync_loop_with(
            &mut stream_b,
            session,
            &store_b,
            &mut index,
            &mut keys_b,
            &identity_b,
            &access,
            options,
        )
        .map_err(|e| e.to_string())
    });

    let deadline = Instant::now() + timeout;
    let mut sync_result: Result<(), String> = Ok(());
    while ready.load(Ordering::SeqCst) < 2 {
        if Instant::now() >= deadline {
            sync_result = Err("sync timeout".to_string());
            break;
        }
        if hub.has_pending_between(a.id, b.id) || hub.has_pending_between(b.id, a.id) {
            touch_log();
        }
        thread::yield_now();
        thread::sleep(Duration::from_millis(1));
    }

    let quiet = Duration::from_millis(50);
    let mut quiet_since: Option<Instant> = None;
    if sync_result.is_ok() {
        loop {
            if Instant::now() >= deadline {
                sync_result = Err("sync timeout".to_string());
                break;
            }
            if log_stale(Duration::from_secs(10)) {
                sync_result = Err("no activity for 10s".to_string());
                break;
            }
            let pending =
                hub.has_pending_between(a.id, b.id) || hub.has_pending_between(b.id, a.id);
            if pending {
                touch_log();
                quiet_since = None;
            } else if let Some(since) = quiet_since {
                if since.elapsed() >= quiet {
                    break;
                }
            } else {
                quiet_since = Some(Instant::now());
            }
            thread::yield_now();
        }
    }
    control.close();
    let join_deadline = Instant::now() + timeout;
    while !thread_a.is_finished() || !thread_b.is_finished() {
        if Instant::now() >= join_deadline {
            return Err(sync_result
                .err()
                .unwrap_or_else(|| "sync timeout".to_string()));
        }
        thread::yield_now();
    }
    let res_a = thread_a
        .join()
        .map_err(|_| "sync thread A failed".to_string())?;
    let res_b = thread_b
        .join()
        .map_err(|_| "sync thread B failed".to_string())?;
    sync_result?;
    res_a?;
    res_b?;
    Ok(())
}

fn is_transient_sync_error(err: &str) -> bool {
    err.contains("network error")
        || err.contains("unexpected eof")
        || err.contains("no activity")
        || err.contains("io error")
        || err.contains("torn write")
        || err.contains("ENOSPC")
        || err.contains("sync timeout")
        || err.contains("cbor error")
        || err.contains("missing payload")
        || err.contains("missing n")
        || err.contains("invalid frame")
        || err.contains("expected hello")
        || err.contains("expected auth")
}

fn make_identity(rng: &mut ChaCha20Rng) -> dharma_core::identity::IdentityState {
    let (signing_key, public_key) = crypto::generate_identity_keypair(rng);
    let root_signing_key = signing_key.clone();
    let root_public_key = public_key;
    let mut subject_key = [0u8; 32];
    rng.fill_bytes(&mut subject_key);
    let mut noise_sk = [0u8; 32];
    rng.fill_bytes(&mut noise_sk);
    dharma_core::identity::IdentityState {
        subject_id: SubjectId::from_bytes(rand_bytes32(rng)),
        signing_key,
        public_key,
        root_signing_key,
        root_public_key,
        subject_key,
        noise_sk,
        schema: SchemaId::from_bytes([0u8; 32]),
        contract: ContractId::from_bytes([0u8; 32]),
    }
}

fn list_meta_files(dir: &Path) -> Result<Vec<PathBuf>, DharmaError> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("meta") {
            out.push(path);
        }
    }
    Ok(out)
}

fn run_vector_case(category: &str, meta_path: &Path) -> Result<(), TestFailure> {
    let meta = load_meta(meta_path).map_err(|err| TestFailure {
        property: "VECTOR-META",
        seed: 0,
        details: err.to_string(),
        trace: None,
    })?;
    let data_path = meta_data_path(meta_path, &meta).map_err(|err| TestFailure {
        property: "VECTOR-DATA",
        seed: 0,
        details: err.to_string(),
        trace: None,
    })?;
    let bytes = fs::read(&data_path).map_err(|err| TestFailure {
        property: "VECTOR-DATA",
        seed: 0,
        details: err.to_string(),
        trace: None,
    })?;
    match category {
        "cbor" => vector_cbor(&bytes, &meta).map_err(|details| TestFailure {
            property: "VECTOR-CBOR",
            seed: 0,
            details,
            trace: None,
        }),
        "assertion" => vector_assertion(&bytes, &meta).map_err(|details| TestFailure {
            property: "VECTOR-ASSERTION",
            seed: 0,
            details,
            trace: None,
        }),
        "envelope" => vector_envelope(&bytes, &meta).map_err(|details| TestFailure {
            property: "VECTOR-ENVELOPE",
            seed: 0,
            details,
            trace: None,
        }),
        "schema" => vector_schema(&bytes, &meta).map_err(|details| TestFailure {
            property: "VECTOR-SCHEMA",
            seed: 0,
            details,
            trace: None,
        }),
        "contract" => vector_contract(&bytes, &meta, &data_path).map_err(|details| TestFailure {
            property: "VECTOR-CONTRACT",
            seed: 0,
            details,
            trace: None,
        }),
        other => Err(TestFailure {
            property: "VECTOR-UNKNOWN",
            seed: 0,
            details: format!("unknown vector category {other}"),
            trace: None,
        }),
    }
}

#[derive(Clone, Debug)]
struct VectorMeta {
    expect: String,
    key_hex: Option<String>,
    data_ext: Option<String>,
}

fn load_meta(path: &Path) -> Result<VectorMeta, DharmaError> {
    let contents = fs::read_to_string(path)?;
    let mut expect = None;
    let mut key_hex = None;
    let mut data_ext = None;
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            let key = k.trim();
            let value = v.trim().trim_matches('"');
            match key {
                "expect" => expect = Some(value.to_string()),
                "key" => key_hex = Some(value.to_string()),
                "data_ext" => data_ext = Some(value.to_string()),
                _ => {}
            }
        }
    }
    let expect = expect.ok_or_else(|| DharmaError::Validation("missing expect".to_string()))?;
    Ok(VectorMeta {
        expect,
        key_hex,
        data_ext,
    })
}

fn meta_data_path(meta_path: &Path, meta: &VectorMeta) -> Result<PathBuf, DharmaError> {
    let stem = meta_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| DharmaError::Validation("invalid meta file".to_string()))?;
    let ext = meta.data_ext.clone().unwrap_or_else(|| "cbor".to_string());
    let data = meta_path.with_file_name(format!("{stem}.{ext}"));
    Ok(data)
}

fn vector_cbor(bytes: &[u8], meta: &VectorMeta) -> Result<(), String> {
    match meta.expect.as_str() {
        "canonical" => {
            cbor::ensure_canonical(bytes).map_err(|e| e.to_string())?;
            Ok(())
        }
        "reject" => {
            if cbor::ensure_canonical(bytes).is_ok() {
                return Err("expected non-canonical rejection".to_string());
            }
            Ok(())
        }
        other => Err(format!("unknown expect {other}")),
    }
}

fn vector_assertion(bytes: &[u8], meta: &VectorMeta) -> Result<(), String> {
    let assertion = match AssertionPlaintext::from_cbor(bytes) {
        Ok(a) => a,
        Err(err) => {
            return match meta.expect.as_str() {
                "reject" => Ok(()),
                _ => Err(err.to_string()),
            }
        }
    };
    let sig_ok = assertion.verify_signature().map_err(|e| e.to_string())?;
    match meta.expect.as_str() {
        "accept" => {
            if !sig_ok {
                return Err("signature rejected".to_string());
            }
            Ok(())
        }
        "reject" => {
            if sig_ok {
                return Err("expected signature rejection".to_string());
            }
            Ok(())
        }
        other => Err(format!("unknown expect {other}")),
    }
}

fn vector_envelope(bytes: &[u8], meta: &VectorMeta) -> Result<(), String> {
    let env = envelope::AssertionEnvelope::from_cbor(bytes).map_err(|e| e.to_string())?;
    let key_hex = meta
        .key_hex
        .clone()
        .ok_or_else(|| "missing key".to_string())?;
    let key_bytes = hex::decode(key_hex).map_err(|e| e.to_string())?;
    let key =
        <[u8; 32]>::try_from(key_bytes.as_slice()).map_err(|_| "invalid key length".to_string())?;
    let out = envelope::decrypt_assertion(&env, &key);
    match meta.expect.as_str() {
        "decrypt" => {
            out.map_err(|e| e.to_string())?;
            Ok(())
        }
        "reject" => {
            if out.is_ok() {
                return Err("expected decrypt failure".to_string());
            }
            Ok(())
        }
        other => Err(format!("unknown expect {other}")),
    }
}

fn vector_schema(bytes: &[u8], meta: &VectorMeta) -> Result<(), String> {
    match meta.expect.as_str() {
        "accept" => {
            dharma_core::schema::parse_schema(bytes).map_err(|e| e.to_string())?;
            Ok(())
        }
        "reject" => {
            if dharma_core::schema::parse_schema(bytes).is_ok() {
                return Err("expected schema rejection".to_string());
            }
            Ok(())
        }
        other => Err(format!("unknown expect {other}")),
    }
}

fn vector_contract(bytes: &[u8], meta: &VectorMeta, data_path: &Path) -> Result<(), String> {
    let wasm = if data_path.extension().and_then(|s| s.to_str()) == Some("wat") {
        wat::parse_str(std::str::from_utf8(bytes).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?
    } else {
        bytes.to_vec()
    };
    let vm = dharma_core::runtime::vm::RuntimeVm::new(wasm);
    let env = StdEnv::new(std::env::temp_dir());
    let mut state = vec![0u8; dharma_core::runtime::vm::STATE_SIZE];
    let args = vec![0u8; 4];
    let result = vm.validate(&env, &mut state, &args, None);
    match meta.expect.as_str() {
        "accept" => result.map_err(|e| e.to_string()),
        "reject" => {
            if result.is_ok() {
                return Err("expected validate failure".to_string());
            }
            Ok(())
        }
        other => Err(format!("unknown expect {other}")),
    }
}

fn prop_cbor_roundtrip(rng: &mut ChaCha20Rng, iterations: usize) -> Result<(), String> {
    for _ in 0..iterations {
        let value = rand_value(rng, 0);
        let bytes = cbor::encode_canonical_value(&value).map_err(|e| e.to_string())?;
        let decoded = cbor::ensure_canonical(&bytes).map_err(|e| e.to_string())?;
        let re = cbor::encode_canonical_value(&decoded).map_err(|e| e.to_string())?;
        if bytes != re {
            return Err("canonical roundtrip mismatch".to_string());
        }
    }
    Ok(())
}

fn prop_cbor_rejects_noncanonical(
    _rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let bytes = vec![0xbf, 0x61, 0x61, 0x01, 0xff];
    if cbor::ensure_canonical(&bytes).is_ok() {
        return Err("non-canonical bytes accepted".to_string());
    }
    Ok(())
}

fn prop_assert_signature_roundtrip(rng: &mut ChaCha20Rng, iterations: usize) -> Result<(), String> {
    for _ in 0..iterations {
        let (sk, pk) = crypto::generate_identity_keypair(rng);
        let subject = SubjectId::from_bytes(rand_bytes32(rng));
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: pk,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes(rand_bytes32(rng)),
            contract: ContractId::from_bytes(rand_bytes32(rng)),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("value".to_string()),
            ciborium::value::Value::Integer((rng.next_u32() as i64).into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        if !assertion.verify_signature().map_err(|e| e.to_string())? {
            return Err("signature failed".to_string());
        }
    }
    Ok(())
}

fn prop_assert_signature_rejects_bitflip(
    rng: &mut ChaCha20Rng,
    iterations: usize,
) -> Result<(), String> {
    for _ in 0..iterations {
        let (sk, pk) = crypto::generate_identity_keypair(rng);
        let subject = SubjectId::from_bytes(rand_bytes32(rng));
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: pk,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes(rand_bytes32(rng)),
            contract: ContractId::from_bytes(rand_bytes32(rng)),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("value".to_string()),
            ciborium::value::Value::Integer(1.into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let mut tampered = assertion.clone();
        tampered.body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("value".to_string()),
            ciborium::value::Value::Integer(2.into()),
        )]);
        if tampered.verify_signature().map_err(|e| e.to_string())? {
            return Err("tampered signature accepted".to_string());
        }
    }
    Ok(())
}

fn prop_assert_structural_rejects_seq_prev(
    rng: &mut ChaCha20Rng,
    iterations: usize,
) -> Result<(), String> {
    for _ in 0..iterations {
        let (sk, pk) = crypto::generate_identity_keypair(rng);
        let subject = SubjectId::from_bytes(rand_bytes32(rng));
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: pk,
            seq: 2,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes(rand_bytes32(rng)),
            contract: ContractId::from_bytes(rand_bytes32(rng)),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let status = structural_validate(&assertion, None).map_err(|e| e.to_string())?;
        if !matches!(status, StructuralStatus::Reject(_)) {
            return Err("expected structural reject".to_string());
        }
    }
    Ok(())
}

fn prop_envelope_roundtrip(rng: &mut ChaCha20Rng, iterations: usize) -> Result<(), String> {
    for _ in 0..iterations {
        let mut key = [0u8; 32];
        rng.fill_bytes(&mut key);
        let kid = KeyId::from_bytes(rand_bytes32(rng));
        let nonce = Nonce12::from_bytes(rand_nonce(rng));
        let plaintext = rand_bytes(rng, 32);
        let env =
            envelope::encrypt_assertion(&plaintext, kid, &key, nonce).map_err(|e| e.to_string())?;
        let out = envelope::decrypt_assertion(&env, &key).map_err(|e| e.to_string())?;
        if out != plaintext {
            return Err("envelope roundtrip mismatch".to_string());
        }
    }
    Ok(())
}

fn prop_envelope_wrong_key(rng: &mut ChaCha20Rng, iterations: usize) -> Result<(), String> {
    for _ in 0..iterations {
        let mut key = [0u8; 32];
        rng.fill_bytes(&mut key);
        let mut bad = [0u8; 32];
        rng.fill_bytes(&mut bad);
        let kid = KeyId::from_bytes(rand_bytes32(rng));
        let nonce = Nonce12::from_bytes(rand_nonce(rng));
        let plaintext = rand_bytes(rng, 16);
        let env =
            envelope::encrypt_assertion(&plaintext, kid, &key, nonce).map_err(|e| e.to_string())?;
        if envelope::decrypt_assertion(&env, &bad).is_ok() {
            return Err("decrypt succeeded with wrong key".to_string());
        }
    }
    Ok(())
}

fn prop_envelope_id_changes_on_mutation(
    rng: &mut ChaCha20Rng,
    iterations: usize,
) -> Result<(), String> {
    for _ in 0..iterations {
        let mut key = [0u8; 32];
        rng.fill_bytes(&mut key);
        let kid = KeyId::from_bytes(rand_bytes32(rng));
        let nonce = Nonce12::from_bytes(rand_nonce(rng));
        let plaintext = rand_bytes(rng, 16);
        let env =
            envelope::encrypt_assertion(&plaintext, kid, &key, nonce).map_err(|e| e.to_string())?;
        let bytes = env.to_cbor().map_err(|e| e.to_string())?;
        let id = crypto::envelope_id(&bytes);
        let mut mutated = bytes.clone();
        if let Some(first) = mutated.first_mut() {
            *first ^= 0x01;
        }
        let id2 = crypto::envelope_id(&mutated);
        if id == id2 {
            return Err("envelope id unchanged after mutation".to_string());
        }
    }
    Ok(())
}

fn prop_dag_ordering_respects_deps(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let mut map = HashMap::new();
    let mut prev = None;
    for seq in 1..=3 {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Step".to_string(),
            auth: pk,
            seq,
            prev,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes(rand_bytes32(rng)),
            contract: ContractId::from_bytes(rand_bytes32(rng)),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let id = assertion.assertion_id().map_err(|e| e.to_string())?;
        prev = Some(id);
        map.insert(id, assertion);
    }
    let order = order_assertions(&map).map_err(|e| e.to_string())?;
    if order.len() != 3 {
        return Err("unexpected topo order length".to_string());
    }
    Ok(())
}

fn prop_dag_detects_cycle(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let header_a = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: 1,
        sub: subject,
        typ: "action.A".to_string(),
        auth: pk,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: SchemaId::from_bytes(rand_bytes32(rng)),
        contract: ContractId::from_bytes(rand_bytes32(rng)),
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let a = AssertionPlaintext::sign(header_a, ciborium::value::Value::Map(vec![]), &sk)
        .map_err(|e| e.to_string())?;
    let id_a = a.assertion_id().map_err(|e| e.to_string())?;
    let header_b = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: 1,
        sub: subject,
        typ: "action.B".to_string(),
        auth: pk,
        seq: 2,
        prev: Some(id_a),
        refs: vec![id_a],
        ts: None,
        schema: SchemaId::from_bytes(rand_bytes32(rng)),
        contract: ContractId::from_bytes(rand_bytes32(rng)),
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let mut b = AssertionPlaintext::sign(header_b, ciborium::value::Value::Map(vec![]), &sk)
        .map_err(|e| e.to_string())?;
    let id_b = b.assertion_id().map_err(|e| e.to_string())?;
    b.header.prev = Some(id_b);
    b.header.refs = vec![id_b];
    let mut map = HashMap::new();
    map.insert(id_a, a);
    map.insert(id_b, b);
    if order_assertions(&map).is_ok() {
        return Err("cycle not detected".to_string());
    }
    Ok(())
}

fn prop_store_ingest_idempotent(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let store = Store::new(&env);
    let (schema_id, contract_id) = store_cqrs_artifacts(&store)?;
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (assertion, subject_key) = make_assertion(rng, 1, None, schema_id, contract_id)?;
    let bytes = wrap_envelope(&assertion, &subject_key)?;
    keys.insert_sdk(assertion.header.sub, 0, subject_key);
    let first =
        ingest_object(&store, &mut index, &bytes, &mut keys).map_err(|e| format!("{e:?}"))?;
    if !matches!(
        first,
        IngestStatus::Accepted(_) | IngestStatus::Pending(_, _)
    ) {
        return Err("first ingest unexpected status".to_string());
    }
    let second =
        ingest_object(&store, &mut index, &bytes, &mut keys).map_err(|e| format!("{e:?}"))?;
    if !matches!(
        second,
        IngestStatus::Accepted(_) | IngestStatus::Pending(_, _)
    ) {
        return Err("second ingest unexpected status".to_string());
    }
    Ok(())
}

fn prop_store_frontier_deterministic(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let store = Store::new(&env);
    let (schema_id, contract_id) = store_cqrs_artifacts(&store)?;
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (assertion, subject_key) = make_assertion(rng, 1, None, schema_id, contract_id)?;
    let bytes = wrap_envelope(&assertion, &subject_key)?;
    keys.insert_sdk(assertion.header.sub, 0, subject_key);
    ingest_object(&store, &mut index, &bytes, &mut keys).map_err(|e| format!("{e:?}"))?;
    let tips_a = index.get_tips(&assertion.header.sub).to_vec();
    let store_b = Store::new(&env);
    let keys_b = Keyring::new();
    let index_b = FrontierIndex::build(&store_b, &keys_b).map_err(|e| e.to_string())?;
    let tips_b = index_b.get_tips(&assertion.header.sub).to_vec();
    if tips_a != tips_b {
        return Err("frontier tips mismatch".to_string());
    }
    Ok(())
}

fn prop_replay_deterministic(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let env_a = SimEnv::new(rng.next_u64(), 0);
    let env_b = SimEnv::new(rng.next_u64(), 0);
    let store_a = Store::new(&env_a);
    let store_b = Store::new(&env_b);
    let (schema_id, contract_id) = store_cqrs_artifacts(&store_a)?;
    let _ = store_cqrs_artifacts(&store_b)?;
    let (schema, wasm) = cqrs_schema_and_wasm();
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let mut prev = None;
    for seq in 1..=5 {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: pk,
            seq,
            prev,
            refs: vec![],
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: None,
            meta: add_signer_meta(None, &subject),
        };
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("value".to_string()),
            ciborium::value::Value::Integer((seq as i64).into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
        let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
        let env_id = crypto::envelope_id(&bytes);
        append_assertion(
            &env_a,
            &subject,
            seq,
            assertion_id,
            env_id,
            "action.Set",
            &bytes,
        )
        .map_err(|e| e.to_string())?;
        prev = Some(assertion_id);
    }
    for record in list_assertions(&env_a, &subject).map_err(|e| e.to_string())? {
        append_assertion(
            &env_b,
            &subject,
            record.seq,
            record.assertion_id,
            record.envelope_id,
            "action.Set",
            &record.bytes,
        )
        .map_err(|e| e.to_string())?;
    }
    let state_a = load_state(&env_a, &subject, &schema, &wasm, 1).map_err(|e| e.to_string())?;
    let state_b = load_state(&env_b, &subject, &schema, &wasm, 1).map_err(|e| e.to_string())?;
    if state_a.memory != state_b.memory {
        return Err("replay state mismatch".to_string());
    }
    Ok(())
}

fn prop_convergence(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    struct Node {
        env: SimEnv,
        subject: SubjectId,
    }

    let mut nodes = Vec::new();
    for _ in 0..3 {
        let env = SimEnv::new(rng.next_u64(), 0);
        let subject = SubjectId::from_bytes(rand_bytes32(rng));
        for seq in 1..=3 {
            let (sk, pk) = crypto::generate_identity_keypair(rng);
            let header = AssertionHeader {
                v: crypto::PROTOCOL_VERSION,
                ver: 1,
                sub: subject,
                typ: "action.Note".to_string(),
                auth: pk,
                seq,
                prev: None,
                refs: vec![],
                ts: None,
                schema: SchemaId::from_bytes([1u8; 32]),
                contract: ContractId::from_bytes([2u8; 32]),
                note: None,
                meta: add_signer_meta(None, &subject),
            };
            let body = ciborium::value::Value::Map(vec![(
                ciborium::value::Value::Text("text".to_string()),
                ciborium::value::Value::Text(rand_text(rng, 8)),
            )]);
            let assertion =
                AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
            let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
            let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
            let env_id = crypto::envelope_id(&bytes);
            append_assertion(
                &env,
                &subject,
                seq,
                assertion_id,
                env_id,
                "action.Note",
                &bytes,
            )
            .map_err(|e| e.to_string())?;
        }
        nodes.push(Node { env, subject });
    }

    for idx in 0..nodes.len() {
        let subject = nodes[idx].subject;
        let records = list_assertions(&nodes[idx].env, &subject).map_err(|e| e.to_string())?;
        for node in &nodes {
            for record in &records {
                append_assertion(
                    &node.env,
                    &subject,
                    record.seq,
                    record.assertion_id,
                    record.envelope_id,
                    "action.Note",
                    &record.bytes,
                )
                .map_err(|e| e.to_string())?;
            }
        }
    }

    let expected = snapshot_env(&nodes[0].env)?;
    for node in nodes.iter().skip(1) {
        let snapshot = snapshot_env(&node.env)?;
        if snapshot != expected {
            return Err("convergence mismatch".to_string());
        }
    }
    Ok(())
}

fn prop_identity_genesis_race(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (root_sk_a, root_id_a) = crypto::generate_identity_keypair(rng);
    let (root_sk_b, root_id_b) = crypto::generate_identity_keypair(rng);

    let build_genesis = |root_sk, root_id: IdentityKey| -> Result<Vec<u8>, String> {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: ATLAS_IDENTITY_GENESIS.to_string(),
            auth: root_id,
            seq: 1,
            prev: None,
            refs: vec![],
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: Some("identity genesis".to_string()),
            meta: add_signer_meta(None, &subject),
        };
        let body = Value::Map(vec![
            (
                Value::Text("atlas_name".to_string()),
                Value::Text("person.local.race".to_string()),
            ),
            (
                Value::Text("owner_key".to_string()),
                Value::Bytes(root_id.as_bytes().to_vec()),
            ),
        ]);
        let assertion =
            AssertionPlaintext::sign(header, body, root_sk).map_err(|e| e.to_string())?;
        assertion.to_cbor().map_err(|e| e.to_string())
    };

    let first = build_genesis(&root_sk_a, root_id_a)?;
    match ingest_object(&store, &mut index, &first, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected accepted genesis, got {other:?}")),
    }

    let second = build_genesis(&root_sk_b, root_id_b)?;
    let err = ingest_object(&store, &mut index, &second, &mut keys).unwrap_err();
    match err {
        dharma_core::net::ingest::IngestError::Validation(reason) => {
            if !reason.contains("genesis") {
                return Err(format!("unexpected genesis error: {reason}"));
            }
        }
        other => return Err(format!("expected validation error, got {other:?}")),
    }
    Ok(())
}

fn prop_identity_lifecycle_requires_root(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (root_sk, root_id) = crypto::generate_identity_keypair(rng);
    let (device_sk, device_id) = crypto::generate_identity_keypair(rng);

    let genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: Some("identity genesis".to_string()),
        meta: add_signer_meta(None, &subject),
    };
    let genesis_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.lifecycle".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(root_id.as_bytes().to_vec()),
        ),
    ]);
    let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let genesis_bytes = genesis.to_cbor().map_err(|e| e.to_string())?;
    let genesis_id = genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected accepted genesis, got {other:?}")),
    }

    let suspend_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: ATLAS_IDENTITY_SUSPEND.to_string(),
        auth: device_id,
        seq: 2,
        prev: Some(genesis_id),
        refs: vec![genesis_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let suspend = AssertionPlaintext::sign(suspend_header, Value::Map(vec![]), &device_sk)
        .map_err(|e| e.to_string())?;
    let suspend_bytes = suspend.to_cbor().map_err(|e| e.to_string())?;
    let err = ingest_object(&store, &mut index, &suspend_bytes, &mut keys).unwrap_err();
    match err {
        dharma_core::net::ingest::IngestError::Validation(reason) => {
            if !reason.contains("lifecycle") {
                return Err(format!("unexpected lifecycle error: {reason}"));
            }
        }
        other => return Err(format!("expected validation error, got {other:?}")),
    }
    Ok(())
}

fn prop_identity_revoked_terminal(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (root_sk, root_id) = crypto::generate_identity_keypair(rng);

    let genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: Some("identity genesis".to_string()),
        meta: add_signer_meta(None, &subject),
    };
    let genesis_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.revoked".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(root_id.as_bytes().to_vec()),
        ),
    ]);
    let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let genesis_bytes = genesis.to_cbor().map_err(|e| e.to_string())?;
    let genesis_id = genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected accepted genesis, got {other:?}")),
    }

    let revoke_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: ATLAS_IDENTITY_REVOKE.to_string(),
        auth: root_id,
        seq: 2,
        prev: Some(genesis_id),
        refs: vec![genesis_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let revoke = AssertionPlaintext::sign(revoke_header, Value::Map(vec![]), &root_sk)
        .map_err(|e| e.to_string())?;
    let revoke_bytes = revoke.to_cbor().map_err(|e| e.to_string())?;
    let revoke_id = revoke.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &revoke_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        Ok(other) => return Err(format!("expected accepted revoke, got {other:?}")),
        Err(err) => return Err(format!("revoke ingest failed: {err:?}")),
    }

    let activate_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: ATLAS_IDENTITY_ACTIVATE.to_string(),
        auth: root_id,
        seq: 3,
        prev: Some(revoke_id),
        refs: vec![revoke_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let activate = AssertionPlaintext::sign(activate_header, Value::Map(vec![]), &root_sk)
        .map_err(|e| e.to_string())?;
    let activate_bytes = activate.to_cbor().map_err(|e| e.to_string())?;
    let _ = ingest_object(&store, &mut index, &activate_bytes, &mut keys);

    let status = identity::identity_status(store.env(), &subject).map_err(|e| e.to_string())?;
    if status != IdentityStatus::Revoked {
        return Err(format!("expected revoked status, got {status:?}"));
    }
    Ok(())
}

fn prop_identity_peer_verification(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    use dharma_core::net::peer::verify_peer_identity;

    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (root_sk, root_id) = crypto::generate_identity_keypair(rng);

    let genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: Some("identity genesis".to_string()),
        meta: add_signer_meta(None, &subject),
    };
    let genesis_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.verify".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(root_id.as_bytes().to_vec()),
        ),
    ]);
    let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let genesis_bytes = genesis.to_cbor().map_err(|e| e.to_string())?;
    let genesis_id = genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected accepted genesis, got {other:?}")),
    }

    let profile_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: "identity.profile".to_string(),
        auth: root_id,
        seq: 2,
        prev: Some(genesis_id),
        refs: vec![genesis_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let profile_body = Value::Map(vec![
        (
            Value::Text("alias".to_string()),
            Value::Text("alice".to_string()),
        ),
        (
            Value::Text("org".to_string()),
            Value::Text("acme".to_string()),
        ),
        (
            Value::Text("roles".to_string()),
            Value::Array(vec![Value::Text("Admin".to_string())]),
        ),
    ]);
    let profile = AssertionPlaintext::sign(profile_header, profile_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let profile_bytes = profile.to_cbor().map_err(|e| e.to_string())?;
    let profile_id = profile.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &profile_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected accepted profile, got {other:?}")),
    }

    let claims =
        verify_peer_identity(store.env(), &subject, &root_id).map_err(|e| e.to_string())?;
    if claims.is_none() {
        return Err("expected verified identity claims".to_string());
    }

    let suspend_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: ATLAS_IDENTITY_SUSPEND.to_string(),
        auth: root_id,
        seq: 3,
        prev: Some(profile_id),
        refs: vec![profile_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let suspend = AssertionPlaintext::sign(suspend_header, Value::Map(vec![]), &root_sk)
        .map_err(|e| e.to_string())?;
    let suspend_bytes = suspend.to_cbor().map_err(|e| e.to_string())?;
    let suspend_id = suspend.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &suspend_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected accepted suspend, got {other:?}")),
    }
    let claims =
        verify_peer_identity(store.env(), &subject, &root_id).map_err(|e| e.to_string())?;
    if claims.is_some() {
        return Err("expected suspended identity to be unverified".to_string());
    }

    let activate_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: ATLAS_IDENTITY_ACTIVATE.to_string(),
        auth: root_id,
        seq: 4,
        prev: Some(suspend_id),
        refs: vec![suspend_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let activate = AssertionPlaintext::sign(activate_header, Value::Map(vec![]), &root_sk)
        .map_err(|e| e.to_string())?;
    let activate_bytes = activate.to_cbor().map_err(|e| e.to_string())?;
    let activate_id = activate.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &activate_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected accepted activate, got {other:?}")),
    }
    let claims =
        verify_peer_identity(store.env(), &subject, &root_id).map_err(|e| e.to_string())?;
    if claims.is_none() {
        return Err("expected re-activated identity claims".to_string());
    }

    let revoke_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: ATLAS_IDENTITY_REVOKE.to_string(),
        auth: root_id,
        seq: 5,
        prev: Some(activate_id),
        refs: vec![activate_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let revoke = AssertionPlaintext::sign(revoke_header, Value::Map(vec![]), &root_sk)
        .map_err(|e| e.to_string())?;
    let revoke_bytes = revoke.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &revoke_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected accepted revoke, got {other:?}")),
    }
    let claims =
        verify_peer_identity(store.env(), &subject, &root_id).map_err(|e| e.to_string())?;
    if claims.is_some() {
        return Err("expected revoked identity to be unverified".to_string());
    }
    Ok(())
}

fn prop_identity_root_recovery(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let store = Store::new(&env);
    let (schema_id, contract_id) = store_cqrs_artifacts(&store)?;
    let (identity_schema, identity_contract) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;

    let identity_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (root_sk, root_id) = crypto::generate_identity_keypair(rng);
    let subject = SubjectId::from_bytes(rand_bytes32(rng));

    let action_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: "action.Set".to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let action_body = Value::Map(vec![(
        Value::Text("value".to_string()),
        Value::Integer(1.into()),
    )]);
    let action = AssertionPlaintext::sign(action_header, action_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let action_bytes = action.to_cbor().map_err(|e| e.to_string())?;
    let action_id = action.assertion_id().map_err(|e| e.to_string())?;

    let first = ingest_object(&store, &mut index, &action_bytes, &mut keys)
        .map_err(|e| format!("{e:?}"))?;
    match first {
        IngestStatus::Pending(_, reason) => {
            if !reason.contains("identity root") {
                return Err(format!("expected missing identity root, got {reason}"));
            }
        }
        other => return Err(format!("expected pending, got {other:?}")),
    }

    let genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: identity_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: identity_schema,
        contract: identity_contract,
        note: Some("identity genesis".to_string()),
        meta: add_signer_meta(None, &identity_subject),
    };
    let genesis_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.recovery".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(root_id.as_bytes().to_vec()),
        ),
    ]);
    let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let genesis_bytes = genesis.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected accepted genesis, got {other:?}")),
    }

    let accepted = dharma_core::net::ingest::retry_pending(&store, &mut index, &mut keys)
        .map_err(|e| format!("{e:?}"))?;
    if accepted == 0 {
        return Err("retry_pending did not accept any assertions".to_string());
    }
    let pending = pending::read_pending(store.env()).map_err(|e| e.to_string())?;
    if !pending.is_empty() {
        return Err("pending objects remain after identity recovery".to_string());
    }
    let stored = store.scan_subject(&subject).map_err(|e| e.to_string())?;
    if !stored.contains(&action_id) {
        return Err("action not accepted after identity recovery".to_string());
    }
    Ok(())
}

fn prop_domain_membership_ordering(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let owner_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (owner_sk, owner_id) = crypto::generate_identity_keypair(rng);
    let owner_genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: owner_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let owner_genesis_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.owner".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let owner_genesis =
        AssertionPlaintext::sign(owner_genesis_header, owner_genesis_body, &owner_sk)
            .map_err(|e| e.to_string())?;
    let owner_genesis_bytes = owner_genesis.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &owner_genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected owner genesis accepted, got {other:?}")),
    }

    let domain_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let domain_name = format!("corp.{}", rng.next_u32());
    let domain_genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.genesis".to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let domain_genesis_body = Value::Map(vec![
        (Value::Text("domain".to_string()), Value::Text(domain_name)),
        (
            Value::Text("owner".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let domain_genesis =
        AssertionPlaintext::sign(domain_genesis_header, domain_genesis_body, &owner_sk)
            .map_err(|e| e.to_string())?;
    let domain_genesis_bytes = domain_genesis.to_cbor().map_err(|e| e.to_string())?;
    let domain_genesis_id = domain_genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &domain_genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected domain genesis accepted, got {other:?}")),
    }

    let (_member_sk, member_id) = crypto::generate_identity_keypair(rng);
    let approve_first = rng.next_u32() % 2 == 0;
    let approve_body = Value::Map(vec![
        (
            Value::Text("target".to_string()),
            Value::Bytes(member_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("roles".to_string()),
            Value::Array(vec![Value::Text("member".to_string())]),
        ),
        (
            Value::Text("scopes".to_string()),
            Value::Array(vec![Value::Text("read".to_string())]),
        ),
        (Value::Text("expires".to_string()), Value::Integer(0.into())),
    ]);
    let revoke_body = Value::Map(vec![(
        Value::Text("target".to_string()),
        Value::Bytes(member_id.as_bytes().to_vec()),
    )]);

    let (first_typ, first_body, second_typ, second_body, expect_member) = if approve_first {
        (
            "atlas.domain.approve",
            approve_body.clone(),
            "atlas.domain.revoke",
            revoke_body.clone(),
            false,
        )
    } else {
        (
            "atlas.domain.revoke",
            revoke_body.clone(),
            "atlas.domain.approve",
            approve_body.clone(),
            true,
        )
    };

    let first_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: first_typ.to_string(),
        auth: owner_id,
        seq: 2,
        prev: Some(domain_genesis_id),
        refs: vec![domain_genesis_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let first =
        AssertionPlaintext::sign(first_header, first_body, &owner_sk).map_err(|e| e.to_string())?;
    let first_bytes = first.to_cbor().map_err(|e| e.to_string())?;
    let first_id = first.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &first_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected {first_typ} accepted, got {other:?}")),
    }

    let second_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: second_typ.to_string(),
        auth: owner_id,
        seq: 3,
        prev: Some(first_id),
        refs: vec![first_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let second = AssertionPlaintext::sign(second_header, second_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let second_bytes = second.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &second_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected {second_typ} accepted, got {other:?}")),
    }

    let state = DomainState::load(&store, &domain_subject).map_err(|e| e.to_string())?;
    let member = state.member(&member_id, 0);
    if expect_member && member.is_none() {
        return Err("expected member present after approve".to_string());
    }
    if !expect_member && member.is_some() {
        return Err("expected member removed after revoke".to_string());
    }
    if let Some(member) = member {
        if !member.roles.iter().any(|r| r == "member") {
            return Err("expected member role present".to_string());
        }
        if !member.scopes.iter().any(|s| s == "read") {
            return Err("expected member scope present".to_string());
        }
    }
    Ok(())
}

fn prop_domain_acting_context(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (note_schema, note_contract) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) = store_cqrs_artifacts(&store)?;

    let actor_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (actor_sk, actor_id) = crypto::generate_identity_keypair(rng);
    let actor_genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: actor_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: actor_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &actor_subject),
    };
    let actor_genesis_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.actor".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(actor_id.as_bytes().to_vec()),
        ),
    ]);
    let actor_genesis =
        AssertionPlaintext::sign(actor_genesis_header, actor_genesis_body, &actor_sk)
            .map_err(|e| e.to_string())?;
    let actor_genesis_bytes = actor_genesis.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &actor_genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected actor genesis accepted, got {other:?}")),
    }

    let owner_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (owner_sk, owner_id) = crypto::generate_identity_keypair(rng);
    let owner_genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: owner_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let owner_genesis_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.owner".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let owner_genesis =
        AssertionPlaintext::sign(owner_genesis_header, owner_genesis_body, &owner_sk)
            .map_err(|e| e.to_string())?;
    let owner_genesis_bytes = owner_genesis.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &owner_genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected owner genesis accepted, got {other:?}")),
    }

    let domain_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let domain_genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.genesis".to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let domain_genesis_body = Value::Map(vec![
        (
            Value::Text("domain".to_string()),
            Value::Text("corp.acme".to_string()),
        ),
        (
            Value::Text("owner".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let domain_genesis =
        AssertionPlaintext::sign(domain_genesis_header, domain_genesis_body, &owner_sk)
            .map_err(|e| e.to_string())?;
    let domain_genesis_bytes = domain_genesis.to_cbor().map_err(|e| e.to_string())?;
    let domain_genesis_id = domain_genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &domain_genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected domain genesis accepted, got {other:?}")),
    }

    let action_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let action_meta = Value::Map(vec![
        (
            Value::Text("acting_domain".to_string()),
            Value::Bytes(domain_subject.as_bytes().to_vec()),
        ),
        (
            Value::Text("acting_role".to_string()),
            Value::Text("member".to_string()),
        ),
    ]);
    let action_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: action_subject,
        typ: "action.Set".to_string(),
        auth: actor_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(Some(action_meta), &actor_subject),
    };
    let action_body = Value::Map(vec![(
        Value::Text("value".to_string()),
        Value::Integer(1.into()),
    )]);
    let action = AssertionPlaintext::sign(action_header, action_body, &actor_sk)
        .map_err(|e| e.to_string())?;
    let action_bytes = action.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &action_bytes, &mut keys) {
        Err(dharma_core::net::ingest::IngestError::Validation(reason)) => {
            if !reason.contains("domain member") {
                return Err(format!("expected domain member error, got {reason}"));
            }
        }
        other => return Err(format!("expected validation error, got {other:?}")),
    }

    let approve_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.approve".to_string(),
        auth: owner_id,
        seq: 2,
        prev: Some(domain_genesis_id),
        refs: vec![domain_genesis_id],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let approve_body = Value::Map(vec![
        (
            Value::Text("target".to_string()),
            Value::Bytes(actor_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("roles".to_string()),
            Value::Array(vec![Value::Text("member".to_string())]),
        ),
        (
            Value::Text("scopes".to_string()),
            Value::Array(vec![Value::Text("read".to_string())]),
        ),
        (Value::Text("expires".to_string()), Value::Integer(0.into())),
    ]);
    let approve = AssertionPlaintext::sign(approve_header, approve_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let _approve_id = approve.assertion_id().map_err(|e| e.to_string())?;
    let approve_bytes = approve.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &approve_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected approve accepted, got {other:?}")),
    }

    let action_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let action_meta = Value::Map(vec![
        (
            Value::Text("acting_domain".to_string()),
            Value::Bytes(domain_subject.as_bytes().to_vec()),
        ),
        (
            Value::Text("acting_role".to_string()),
            Value::Text("admin".to_string()),
        ),
    ]);
    let action_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: action_subject,
        typ: "action.Set".to_string(),
        auth: actor_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(Some(action_meta), &actor_subject),
    };
    let action_body = Value::Map(vec![(
        Value::Text("value".to_string()),
        Value::Integer(2.into()),
    )]);
    let action = AssertionPlaintext::sign(action_header, action_body, &actor_sk)
        .map_err(|e| e.to_string())?;
    let action_bytes = action.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &action_bytes, &mut keys) {
        Err(dharma_core::net::ingest::IngestError::Validation(reason)) => {
            if !reason.contains("role not granted") {
                return Err(format!("expected role not granted, got {reason}"));
            }
        }
        other => return Err(format!("expected role validation error, got {other:?}")),
    }

    let action_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let action_meta = Value::Map(vec![
        (
            Value::Text("acting_domain".to_string()),
            Value::Bytes(domain_subject.as_bytes().to_vec()),
        ),
        (
            Value::Text("acting_role".to_string()),
            Value::Text("member".to_string()),
        ),
    ]);
    let action_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: action_subject,
        typ: "action.Set".to_string(),
        auth: actor_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(Some(action_meta), &actor_subject),
    };
    let action_body = Value::Map(vec![(
        Value::Text("value".to_string()),
        Value::Integer(3.into()),
    )]);
    let action = AssertionPlaintext::sign(action_header, action_body, &actor_sk)
        .map_err(|e| e.to_string())?;
    let action_bytes = action.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &action_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected action accepted, got {other:?}")),
    }
    Ok(())
}

fn prop_directory_parent_authorization(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let (parent_sk, parent_id) = crypto::generate_identity_keypair(rng);
    let (child_sk, child_id) = crypto::generate_identity_keypair(rng);
    let parent_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let child_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let parent_domain = format!("corp{}", rng.next_u32());
    let child_domain = format!("{}.eng", parent_domain);

    let append_signed =
        |subject, seq, prev, typ: &str, auth, sk, body| -> Result<AssertionId, String> {
            let header = AssertionHeader {
                v: crypto::PROTOCOL_VERSION,
                ver: DEFAULT_DATA_VERSION,
                sub: subject,
                typ: typ.to_string(),
                auth,
                seq,
                prev,
                refs: prev.into_iter().collect(),
                ts: None,
                schema: schema_id,
                contract: contract_id,
                note: None,
                meta: None,
            };
            let assertion =
                AssertionPlaintext::sign(header, body, sk).map_err(|e| e.to_string())?;
            let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
            let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
            let envelope_id = crypto::envelope_id(&bytes);
            append_assertion(
                store.env(),
                &subject,
                seq,
                assertion_id,
                envelope_id,
                typ,
                &bytes,
            )
            .map_err(|e| e.to_string())?;
            Ok(assertion_id)
        };

    let _parent_genesis_id = append_signed(
        parent_subject,
        1,
        None,
        "atlas.domain.genesis",
        parent_id,
        &parent_sk,
        Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text(parent_domain.clone()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(parent_id.as_bytes().to_vec()),
            ),
        ]),
    )?;
    let _child_genesis_id = append_signed(
        child_subject,
        1,
        None,
        "atlas.domain.genesis",
        child_id,
        &child_sk,
        Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text(child_domain.clone()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(child_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("parent".to_string()),
                Value::Text(parent_domain.clone()),
            ),
        ]),
    )?;

    let dir_subject = DirectoryClient::default_subject();
    let mut dir_seq = 1;
    let mut dir_prev: Option<AssertionId> = None;
    let mut append_dir = |typ: &str, auth, sk, body| {
        let prev = dir_prev;
        let id = append_signed(dir_subject, dir_seq, prev, typ, auth, sk, body)?;
        dir_seq += 1;
        dir_prev = Some(id);
        Ok::<_, String>(id)
    };

    append_dir(
        "fabric.domain.register",
        parent_id,
        &parent_sk,
        Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text(parent_domain.clone()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(parent_id.as_bytes().to_vec()),
            ),
        ]),
    )?;
    append_dir(
        "fabric.domain.request",
        child_id,
        &child_sk,
        Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text(child_domain.clone()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(child_id.as_bytes().to_vec()),
            ),
        ]),
    )?;
    append_dir(
        "fabric.domain.register",
        child_id,
        &child_sk,
        Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text(child_domain.clone()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(child_id.as_bytes().to_vec()),
            ),
        ]),
    )?;

    let state = DirectoryState::load(&store, &dir_subject).map_err(|e| e.to_string())?;
    if state.owner_for_domain(&child_domain).is_some() {
        return Err("expected child domain unregistered without authorization".to_string());
    }

    append_dir(
        "fabric.domain.authorize",
        parent_id,
        &parent_sk,
        Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text(child_domain.clone()),
            ),
            (
                Value::Text("parent".to_string()),
                Value::Text(parent_domain.clone()),
            ),
            (
                Value::Text("authorized_owner".to_string()),
                Value::Bytes(child_id.as_bytes().to_vec()),
            ),
        ]),
    )?;
    append_dir(
        "fabric.domain.register",
        child_id,
        &child_sk,
        Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text(child_domain.clone()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(child_id.as_bytes().to_vec()),
            ),
        ]),
    )?;

    let state = DirectoryState::load(&store, &dir_subject).map_err(|e| e.to_string())?;
    let owner = state.owner_for_domain(&child_domain);
    if owner.is_none() {
        return Err("expected child domain registered after authorization".to_string());
    }
    if owner != Some(child_id) {
        return Err("expected child domain owner to match authorized owner".to_string());
    }

    append_dir(
        "fabric.domain.register",
        parent_id,
        &parent_sk,
        Value::Map(vec![
            (
                Value::Text("domain".to_string()),
                Value::Text(child_domain.clone()),
            ),
            (
                Value::Text("owner".to_string()),
                Value::Bytes(parent_id.as_bytes().to_vec()),
            ),
        ]),
    )?;

    let state = DirectoryState::load(&store, &dir_subject).map_err(|e| e.to_string())?;
    let owner = state.owner_for_domain(&child_domain);
    if owner != Some(child_id) {
        return Err("owner mismatch should not overwrite directory entry".to_string());
    }
    Ok(())
}

fn prop_key_epoch_monotonic(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let owner_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (owner_sk, owner_id) = crypto::generate_identity_keypair(rng);
    let owner_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: owner_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let owner_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.owner".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let owner_genesis =
        AssertionPlaintext::sign(owner_header, owner_body, &owner_sk).map_err(|e| e.to_string())?;
    let owner_bytes = owner_genesis.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &owner_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected owner genesis accepted, got {other:?}")),
    }

    let domain_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let domain_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.genesis".to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let domain_body = Value::Map(vec![
        (
            Value::Text("domain".to_string()),
            Value::Text(format!("corp.{}", rng.next_u32())),
        ),
        (
            Value::Text("owner".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let domain_genesis = AssertionPlaintext::sign(domain_header, domain_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let domain_bytes = domain_genesis.to_cbor().map_err(|e| e.to_string())?;
    let mut prev = Some(domain_genesis.assertion_id().map_err(|e| e.to_string())?);
    match ingest_object(&store, &mut index, &domain_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected domain genesis accepted, got {other:?}")),
    }

    let mut current_epoch = 0u64;
    for step in 1..=3u64 {
        let mut kek_key = [0u8; 32];
        rng.fill_bytes(&mut kek_key);
        let kek_id = key_id_for_key(&kek_key);
        let rotate_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: domain_subject,
            typ: "domain.key.rotate".to_string(),
            auth: owner_id,
            seq: step + 1,
            prev,
            refs: prev.into_iter().collect(),
            ts: None,
            schema: schema_id,
            contract: contract_id,
            note: None,
            meta: add_signer_meta(None, &owner_subject),
        };
        let rotate_body = Value::Map(vec![
            (
                Value::Text("epoch".to_string()),
                Value::Integer(step.into()),
            ),
            (
                Value::Text("kek_id".to_string()),
                Value::Bytes(kek_id.as_bytes().to_vec()),
            ),
        ]);
        let rotate = AssertionPlaintext::sign(rotate_header, rotate_body, &owner_sk)
            .map_err(|e| e.to_string())?;
        let rotate_bytes = rotate.to_cbor().map_err(|e| e.to_string())?;
        let rotate_id = rotate.assertion_id().map_err(|e| e.to_string())?;
        match ingest_object(&store, &mut index, &rotate_bytes, &mut keys) {
            Ok(IngestStatus::Accepted(_)) => {}
            other => return Err(format!("expected rotate accepted, got {other:?}")),
        }
        current_epoch = step;
        prev = Some(rotate_id);
    }

    let mut kek_key = [0u8; 32];
    rng.fill_bytes(&mut kek_key);
    let kek_id = key_id_for_key(&kek_key);
    let bad_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "domain.key.rotate".to_string(),
        auth: owner_id,
        seq: 5,
        prev,
        refs: prev.into_iter().collect(),
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let bad_body = Value::Map(vec![
        (
            Value::Text("epoch".to_string()),
            Value::Integer((current_epoch - 1).into()),
        ),
        (
            Value::Text("kek_id".to_string()),
            Value::Bytes(kek_id.as_bytes().to_vec()),
        ),
    ]);
    let bad =
        AssertionPlaintext::sign(bad_header, bad_body, &owner_sk).map_err(|e| e.to_string())?;
    let bad_bytes = bad.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &bad_bytes, &mut keys) {
        Err(dharma_core::net::ingest::IngestError::Validation(reason)) => {
            if !reason.contains("epoch not advanced") {
                return Err(format!("expected epoch not advanced, got {reason}"));
            }
        }
        other => return Err(format!("expected validation error, got {other:?}")),
    }

    let epoch = load_epoch(store.env(), &domain_subject).map_err(|e| e.to_string())?;
    if epoch != Some(current_epoch) {
        return Err(format!("expected epoch {current_epoch}, got {epoch:?}"));
    }
    Ok(())
}

fn prop_key_grant_requires_member(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let owner_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (owner_sk, owner_id) = crypto::generate_identity_keypair(rng);
    let owner_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: owner_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let owner_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.owner".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let owner_genesis =
        AssertionPlaintext::sign(owner_header, owner_body, &owner_sk).map_err(|e| e.to_string())?;
    let owner_bytes = owner_genesis.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &owner_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected owner genesis accepted, got {other:?}")),
    }

    let domain_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let domain_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.genesis".to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let domain_body = Value::Map(vec![
        (
            Value::Text("domain".to_string()),
            Value::Text("corp".to_string()),
        ),
        (
            Value::Text("owner".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let domain_genesis = AssertionPlaintext::sign(domain_header, domain_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let domain_bytes = domain_genesis.to_cbor().map_err(|e| e.to_string())?;
    let domain_genesis_id = domain_genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &domain_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected domain genesis accepted, got {other:?}")),
    }

    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let mut sdk = [0u8; 32];
    rng.fill_bytes(&mut sdk);
    let sdk_id = key_id_for_key(&sdk);
    let bind_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: "subject.key.bind".to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let bind_body = Value::Map(vec![
        (
            Value::Text("domain".to_string()),
            Value::Bytes(domain_subject.as_bytes().to_vec()),
        ),
        (Value::Text("epoch".to_string()), Value::Integer(0.into())),
        (
            Value::Text("sdk_id".to_string()),
            Value::Bytes(sdk_id.as_bytes().to_vec()),
        ),
    ]);
    let bind =
        AssertionPlaintext::sign(bind_header, bind_body, &owner_sk).map_err(|e| e.to_string())?;
    let bind_bytes = bind.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &bind_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected bind accepted, got {other:?}")),
    }

    let (_member_sk, member_id) = crypto::generate_identity_keypair(rng);
    let mut member_secret = [0u8; 32];
    rng.fill_bytes(&mut member_secret);
    let member_hpke_pk = hpke_public_key_from_secret(&member_secret);
    let sdk_env = hpke_seal(&member_hpke_pk, &sdk).map_err(|e| e.to_string())?;
    let sdk_bytes = sdk_env.to_cbor().map_err(|e| e.to_string())?;

    let grant_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "member.key.grant".to_string(),
        auth: owner_id,
        seq: 2,
        prev: Some(domain_genesis_id),
        refs: vec![domain_genesis_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let grant_body = Value::Map(vec![
        (
            Value::Text("member".to_string()),
            Value::Bytes(member_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("subject".to_string()),
            Value::Bytes(subject.as_bytes().to_vec()),
        ),
        (Value::Text("epoch".to_string()), Value::Integer(0.into())),
        (
            Value::Text("sdk_id".to_string()),
            Value::Bytes(sdk_id.as_bytes().to_vec()),
        ),
        (Value::Text("sdk".to_string()), Value::Bytes(sdk_bytes)),
    ]);
    let grant =
        AssertionPlaintext::sign(grant_header, grant_body, &owner_sk).map_err(|e| e.to_string())?;
    let grant_bytes = grant.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &grant_bytes, &mut keys) {
        Err(dharma_core::net::ingest::IngestError::Validation(reason)) => {
            if !reason.contains("member not active") {
                return Err(format!("expected member not active, got {reason}"));
            }
        }
        other => return Err(format!("expected validation error, got {other:?}")),
    }
    if keys.sdk_for_subject_epoch(&subject, 0).is_some() {
        return Err("unexpected sdk inserted for non-member".to_string());
    }
    Ok(())
}

fn prop_key_revoked_boundary(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let owner_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (owner_sk, owner_id) = crypto::generate_identity_keypair(rng);
    let owner_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: owner_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let owner_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.owner".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let owner_genesis =
        AssertionPlaintext::sign(owner_header, owner_body, &owner_sk).map_err(|e| e.to_string())?;
    let owner_bytes = owner_genesis.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &owner_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected owner genesis accepted, got {other:?}")),
    }

    let domain_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let domain_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.genesis".to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let domain_body = Value::Map(vec![
        (
            Value::Text("domain".to_string()),
            Value::Text("corp".to_string()),
        ),
        (
            Value::Text("owner".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let domain_genesis = AssertionPlaintext::sign(domain_header, domain_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let domain_bytes = domain_genesis.to_cbor().map_err(|e| e.to_string())?;
    let domain_genesis_id = domain_genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &domain_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected domain genesis accepted, got {other:?}")),
    }

    let (_member_sk, member_id) = crypto::generate_identity_keypair(rng);
    let mut member_secret = [0u8; 32];
    rng.fill_bytes(&mut member_secret);
    let member_hpke_pk = hpke_public_key_from_secret(&member_secret);

    let approve_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.approve".to_string(),
        auth: owner_id,
        seq: 2,
        prev: Some(domain_genesis_id),
        refs: vec![domain_genesis_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let approve_body = Value::Map(vec![
        (
            Value::Text("target".to_string()),
            Value::Bytes(member_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("roles".to_string()),
            Value::Array(vec![Value::Text("member".to_string())]),
        ),
        (
            Value::Text("scopes".to_string()),
            Value::Array(vec![Value::Text("all".to_string())]),
        ),
        (Value::Text("expires".to_string()), Value::Integer(0.into())),
    ]);
    let approve = AssertionPlaintext::sign(approve_header, approve_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let approve_id = approve.assertion_id().map_err(|e| e.to_string())?;
    let approve_bytes = approve.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &approve_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected approve accepted, got {other:?}")),
    }

    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let mut sdk0 = [0u8; 32];
    rng.fill_bytes(&mut sdk0);
    let sdk0_id = key_id_for_key(&sdk0);
    let bind0_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: "subject.key.bind".to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let bind0_body = Value::Map(vec![
        (
            Value::Text("domain".to_string()),
            Value::Bytes(domain_subject.as_bytes().to_vec()),
        ),
        (Value::Text("epoch".to_string()), Value::Integer(0.into())),
        (
            Value::Text("sdk_id".to_string()),
            Value::Bytes(sdk0_id.as_bytes().to_vec()),
        ),
    ]);
    let bind0 =
        AssertionPlaintext::sign(bind0_header, bind0_body, &owner_sk).map_err(|e| e.to_string())?;
    let bind0_bytes = bind0.to_cbor().map_err(|e| e.to_string())?;
    let bind0_id = bind0.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &bind0_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected bind0 accepted, got {other:?}")),
    }

    let sdk0_env = hpke_seal(&member_hpke_pk, &sdk0).map_err(|e| e.to_string())?;
    let sdk0_bytes = sdk0_env.to_cbor().map_err(|e| e.to_string())?;
    let grant0_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "member.key.grant".to_string(),
        auth: owner_id,
        seq: 3,
        prev: Some(approve_id),
        refs: vec![approve_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let grant0_body = Value::Map(vec![
        (
            Value::Text("member".to_string()),
            Value::Bytes(member_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("subject".to_string()),
            Value::Bytes(subject.as_bytes().to_vec()),
        ),
        (Value::Text("epoch".to_string()), Value::Integer(0.into())),
        (
            Value::Text("sdk_id".to_string()),
            Value::Bytes(sdk0_id.as_bytes().to_vec()),
        ),
        (Value::Text("sdk".to_string()), Value::Bytes(sdk0_bytes)),
    ]);
    let grant0 = AssertionPlaintext::sign(grant0_header, grant0_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let grant0_id = grant0.assertion_id().map_err(|e| e.to_string())?;
    let grant0_bytes = grant0.to_cbor().map_err(|e| e.to_string())?;
    keys.insert_hpke_secret(member_id, member_secret);
    match ingest_object(&store, &mut index, &grant0_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected grant0 accepted, got {other:?}")),
    }

    let revoke_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.revoke".to_string(),
        auth: owner_id,
        seq: 4,
        prev: Some(grant0_id),
        refs: vec![grant0_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let revoke_body = Value::Map(vec![
        (
            Value::Text("target".to_string()),
            Value::Bytes(member_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("reason".to_string()),
            Value::Text("rotate".to_string()),
        ),
    ]);
    let revoke = AssertionPlaintext::sign(revoke_header, revoke_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let revoke_bytes = revoke.to_cbor().map_err(|e| e.to_string())?;
    let revoke_id = revoke.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &revoke_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected revoke accepted, got {other:?}")),
    }

    let mut kek_key = [0u8; 32];
    rng.fill_bytes(&mut kek_key);
    let kek_id = key_id_for_key(&kek_key);
    let rotate_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "domain.key.rotate".to_string(),
        auth: owner_id,
        seq: 5,
        prev: Some(revoke_id),
        refs: vec![revoke_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let rotate_body = Value::Map(vec![
        (Value::Text("epoch".to_string()), Value::Integer(1.into())),
        (
            Value::Text("kek_id".to_string()),
            Value::Bytes(kek_id.as_bytes().to_vec()),
        ),
    ]);
    let rotate = AssertionPlaintext::sign(rotate_header, rotate_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let rotate_bytes = rotate.to_cbor().map_err(|e| e.to_string())?;
    let rotate_id = rotate.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &rotate_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected rotate accepted, got {other:?}")),
    }

    let mut sdk1 = [0u8; 32];
    rng.fill_bytes(&mut sdk1);
    let sdk1_id = key_id_for_key(&sdk1);
    let bind1_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: "subject.key.bind".to_string(),
        auth: owner_id,
        seq: 2,
        prev: Some(bind0_id),
        refs: vec![bind0_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let bind1_body = Value::Map(vec![
        (
            Value::Text("domain".to_string()),
            Value::Bytes(domain_subject.as_bytes().to_vec()),
        ),
        (Value::Text("epoch".to_string()), Value::Integer(1.into())),
        (
            Value::Text("sdk_id".to_string()),
            Value::Bytes(sdk1_id.as_bytes().to_vec()),
        ),
    ]);
    let bind1 =
        AssertionPlaintext::sign(bind1_header, bind1_body, &owner_sk).map_err(|e| e.to_string())?;
    let bind1_bytes = bind1.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &bind1_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected bind1 accepted, got {other:?}")),
    }

    let sdk1_env = hpke_seal(&member_hpke_pk, &sdk1).map_err(|e| e.to_string())?;
    let sdk1_bytes = sdk1_env.to_cbor().map_err(|e| e.to_string())?;
    let grant1_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "member.key.grant".to_string(),
        auth: owner_id,
        seq: 6,
        prev: Some(rotate_id),
        refs: vec![rotate_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let grant1_body = Value::Map(vec![
        (
            Value::Text("member".to_string()),
            Value::Bytes(member_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("subject".to_string()),
            Value::Bytes(subject.as_bytes().to_vec()),
        ),
        (Value::Text("epoch".to_string()), Value::Integer(1.into())),
        (
            Value::Text("sdk_id".to_string()),
            Value::Bytes(sdk1_id.as_bytes().to_vec()),
        ),
        (Value::Text("sdk".to_string()), Value::Bytes(sdk1_bytes)),
    ]);
    let grant1 = AssertionPlaintext::sign(grant1_header, grant1_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let grant1_bytes = grant1.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &grant1_bytes, &mut keys) {
        Err(dharma_core::net::ingest::IngestError::Validation(reason)) => {
            if !reason.contains("member") {
                return Err(format!("expected member validation, got {reason}"));
            }
        }
        other => return Err(format!("expected validation error, got {other:?}")),
    }

    if keys.sdk_for_subject_epoch(&subject, 0).is_none() {
        return Err("expected epoch 0 sdk retained".to_string());
    }
    if keys.sdk_for_subject_epoch(&subject, 1).is_some() {
        return Err("unexpected sdk for revoked member".to_string());
    }
    Ok(())
}

fn prop_key_grant_tampered_envelope(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let owner_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (owner_sk, owner_id) = crypto::generate_identity_keypair(rng);
    let owner_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: owner_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let owner_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.owner".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let owner_genesis =
        AssertionPlaintext::sign(owner_header, owner_body, &owner_sk).map_err(|e| e.to_string())?;
    let owner_bytes = owner_genesis.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &owner_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected owner genesis accepted, got {other:?}")),
    }

    let domain_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let domain_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.genesis".to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let domain_body = Value::Map(vec![
        (
            Value::Text("domain".to_string()),
            Value::Text("corp".to_string()),
        ),
        (
            Value::Text("owner".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let domain_genesis = AssertionPlaintext::sign(domain_header, domain_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let domain_bytes = domain_genesis.to_cbor().map_err(|e| e.to_string())?;
    let domain_genesis_id = domain_genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &domain_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected domain genesis accepted, got {other:?}")),
    }

    let (_member_sk, member_id) = crypto::generate_identity_keypair(rng);
    let mut member_secret = [0u8; 32];
    rng.fill_bytes(&mut member_secret);
    let member_hpke_pk = hpke_public_key_from_secret(&member_secret);

    let approve_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.approve".to_string(),
        auth: owner_id,
        seq: 2,
        prev: Some(domain_genesis_id),
        refs: vec![domain_genesis_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let approve_body = Value::Map(vec![
        (
            Value::Text("target".to_string()),
            Value::Bytes(member_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("roles".to_string()),
            Value::Array(vec![Value::Text("member".to_string())]),
        ),
        (
            Value::Text("scopes".to_string()),
            Value::Array(vec![Value::Text("all".to_string())]),
        ),
        (Value::Text("expires".to_string()), Value::Integer(0.into())),
    ]);
    let approve = AssertionPlaintext::sign(approve_header, approve_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let approve_id = approve.assertion_id().map_err(|e| e.to_string())?;
    let approve_bytes = approve.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &approve_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected approve accepted, got {other:?}")),
    }

    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let mut sdk = [0u8; 32];
    rng.fill_bytes(&mut sdk);
    let sdk_id = key_id_for_key(&sdk);
    let bind_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: subject,
        typ: "subject.key.bind".to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let bind_body = Value::Map(vec![
        (
            Value::Text("domain".to_string()),
            Value::Bytes(domain_subject.as_bytes().to_vec()),
        ),
        (Value::Text("epoch".to_string()), Value::Integer(0.into())),
        (
            Value::Text("sdk_id".to_string()),
            Value::Bytes(sdk_id.as_bytes().to_vec()),
        ),
    ]);
    let bind =
        AssertionPlaintext::sign(bind_header, bind_body, &owner_sk).map_err(|e| e.to_string())?;
    let bind_bytes = bind.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &bind_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected bind accepted, got {other:?}")),
    }

    let mut sdk_env = hpke_seal(&member_hpke_pk, &sdk).map_err(|e| e.to_string())?;
    if let Some(first) = sdk_env.ct.first_mut() {
        *first ^= 0x01;
    } else {
        sdk_env.ct.push(1);
    }
    let sdk_bytes = sdk_env.to_cbor().map_err(|e| e.to_string())?;

    keys.insert_hpke_secret(member_id, member_secret);
    let grant_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "member.key.grant".to_string(),
        auth: owner_id,
        seq: 3,
        prev: Some(approve_id),
        refs: vec![approve_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let grant_body = Value::Map(vec![
        (
            Value::Text("member".to_string()),
            Value::Bytes(member_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("subject".to_string()),
            Value::Bytes(subject.as_bytes().to_vec()),
        ),
        (Value::Text("epoch".to_string()), Value::Integer(0.into())),
        (
            Value::Text("sdk_id".to_string()),
            Value::Bytes(sdk_id.as_bytes().to_vec()),
        ),
        (Value::Text("sdk".to_string()), Value::Bytes(sdk_bytes)),
    ]);
    let grant =
        AssertionPlaintext::sign(grant_header, grant_body, &owner_sk).map_err(|e| e.to_string())?;
    let grant_bytes = grant.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &grant_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected grant accepted, got {other:?}")),
    }
    if keys.sdk_for_subject_epoch(&subject, 0).is_some() {
        return Err("tampered envelope inserted sdk unexpectedly".to_string());
    }
    Ok(())
}

fn prop_emergency_freeze_blocks(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let owner_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (owner_sk, owner_id) = crypto::generate_identity_keypair(rng);
    let owner_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: owner_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let owner_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.owner".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let owner_genesis =
        AssertionPlaintext::sign(owner_header, owner_body, &owner_sk).map_err(|e| e.to_string())?;
    let owner_bytes = owner_genesis.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &owner_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected owner genesis accepted, got {other:?}")),
    }

    let domain_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let domain_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.genesis".to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let domain_body = Value::Map(vec![
        (
            Value::Text("domain".to_string()),
            Value::Text("corp".to_string()),
        ),
        (
            Value::Text("owner".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let domain_genesis = AssertionPlaintext::sign(domain_header, domain_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let domain_bytes = domain_genesis.to_cbor().map_err(|e| e.to_string())?;
    let domain_genesis_id = domain_genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &domain_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected domain genesis accepted, got {other:?}")),
    }

    let freeze_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "domain.freeze".to_string(),
        auth: owner_id,
        seq: 2,
        prev: Some(domain_genesis_id),
        refs: vec![domain_genesis_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let freeze_body = Value::Map(vec![(
        Value::Text("reason".to_string()),
        Value::Text("incident".to_string()),
    )]);
    let freeze = AssertionPlaintext::sign(freeze_header, freeze_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let freeze_bytes = freeze.to_cbor().map_err(|e| e.to_string())?;
    let freeze_id = freeze.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &freeze_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected freeze accepted, got {other:?}")),
    }

    let invite_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.invite".to_string(),
        auth: owner_id,
        seq: 3,
        prev: Some(freeze_id),
        refs: vec![freeze_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let invite_body = Value::Map(vec![
        (
            Value::Text("target".to_string()),
            Value::Bytes([9u8; 32].to_vec()),
        ),
        (
            Value::Text("roles".to_string()),
            Value::Array(vec![Value::Text("member".to_string())]),
        ),
        (
            Value::Text("scopes".to_string()),
            Value::Array(vec![Value::Text("all".to_string())]),
        ),
    ]);
    let invite = AssertionPlaintext::sign(invite_header, invite_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let invite_bytes = invite.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &invite_bytes, &mut keys) {
        Err(dharma_core::net::ingest::IngestError::Validation(reason)) => {
            if !reason.contains("frozen") {
                return Err(format!("expected frozen rejection, got {reason}"));
            }
        }
        other => return Err(format!("expected frozen rejection, got {other:?}")),
    }
    Ok(())
}

fn prop_emergency_unfreeze_compromise(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let owner_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (owner_sk, owner_id) = crypto::generate_identity_keypair(rng);
    let owner_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: owner_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let owner_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.owner".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let owner_genesis =
        AssertionPlaintext::sign(owner_header, owner_body, &owner_sk).map_err(|e| e.to_string())?;
    let owner_bytes = owner_genesis.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &owner_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected owner genesis accepted, got {other:?}")),
    }

    let domain_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let domain_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.genesis".to_string(),
        auth: owner_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let domain_body = Value::Map(vec![
        (
            Value::Text("domain".to_string()),
            Value::Text("corp".to_string()),
        ),
        (
            Value::Text("owner".to_string()),
            Value::Bytes(owner_id.as_bytes().to_vec()),
        ),
    ]);
    let domain_genesis = AssertionPlaintext::sign(domain_header, domain_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let domain_bytes = domain_genesis.to_cbor().map_err(|e| e.to_string())?;
    let domain_genesis_id = domain_genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &domain_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected domain genesis accepted, got {other:?}")),
    }

    let freeze_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "domain.freeze".to_string(),
        auth: owner_id,
        seq: 2,
        prev: Some(domain_genesis_id),
        refs: vec![domain_genesis_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let freeze_body = Value::Map(vec![]);
    let freeze = AssertionPlaintext::sign(freeze_header, freeze_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let freeze_bytes = freeze.to_cbor().map_err(|e| e.to_string())?;
    let freeze_id = freeze.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &freeze_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected freeze accepted, got {other:?}")),
    }

    let unfreeze_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "domain.unfreeze".to_string(),
        auth: owner_id,
        seq: 3,
        prev: Some(freeze_id),
        refs: vec![freeze_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let unfreeze = AssertionPlaintext::sign(unfreeze_header, Value::Map(vec![]), &owner_sk)
        .map_err(|e| e.to_string())?;
    let unfreeze_bytes = unfreeze.to_cbor().map_err(|e| e.to_string())?;
    let unfreeze_id = unfreeze.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &unfreeze_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected unfreeze accepted, got {other:?}")),
    }

    let invite_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.invite".to_string(),
        auth: owner_id,
        seq: 4,
        prev: Some(unfreeze_id),
        refs: vec![unfreeze_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let invite_body = Value::Map(vec![
        (
            Value::Text("target".to_string()),
            Value::Bytes([8u8; 32].to_vec()),
        ),
        (
            Value::Text("roles".to_string()),
            Value::Array(vec![Value::Text("member".to_string())]),
        ),
        (
            Value::Text("scopes".to_string()),
            Value::Array(vec![Value::Text("all".to_string())]),
        ),
    ]);
    let invite = AssertionPlaintext::sign(invite_header, invite_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let invite_id = invite.assertion_id().map_err(|e| e.to_string())?;
    let invite_bytes = invite.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &invite_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => {
            return Err(format!(
                "expected invite accepted after unfreeze, got {other:?}"
            ))
        }
    }

    let compromised_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "domain.compromised".to_string(),
        auth: owner_id,
        seq: 5,
        prev: Some(invite_id),
        refs: vec![invite_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let compromised = AssertionPlaintext::sign(compromised_header, Value::Map(vec![]), &owner_sk)
        .map_err(|e| e.to_string())?;
    let compromised_bytes = compromised.to_cbor().map_err(|e| e.to_string())?;
    let compromised_id = compromised.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &compromised_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected compromised accepted, got {other:?}")),
    }

    let invite_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: domain_subject,
        typ: "atlas.domain.invite".to_string(),
        auth: owner_id,
        seq: 6,
        prev: Some(compromised_id),
        refs: vec![compromised_id],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &owner_subject),
    };
    let invite_body = Value::Map(vec![
        (
            Value::Text("target".to_string()),
            Value::Bytes([7u8; 32].to_vec()),
        ),
        (
            Value::Text("roles".to_string()),
            Value::Array(vec![Value::Text("member".to_string())]),
        ),
        (
            Value::Text("scopes".to_string()),
            Value::Array(vec![Value::Text("all".to_string())]),
        ),
    ]);
    let invite = AssertionPlaintext::sign(invite_header, invite_body, &owner_sk)
        .map_err(|e| e.to_string())?;
    let invite_bytes = invite.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &invite_bytes, &mut keys) {
        Err(dharma_core::net::ingest::IngestError::Validation(reason)) => {
            if !reason.contains("compromised") {
                return Err(format!("expected compromised rejection, got {reason}"));
            }
        }
        other => return Err(format!("expected compromised rejection, got {other:?}")),
    }
    Ok(())
}

fn prop_emergency_device_revoke(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (note_schema, note_contract) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;
    let (schema_id, contract_id) = store_cqrs_artifacts(&store)?;

    let identity_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (root_sk, root_id) = crypto::generate_identity_keypair(rng);
    let (device_sk, device_id) = crypto::generate_identity_keypair(rng);

    let genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: identity_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let genesis_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.device".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(root_id.as_bytes().to_vec()),
        ),
    ]);
    let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let genesis_bytes = genesis.to_cbor().map_err(|e| e.to_string())?;
    let genesis_id = genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected genesis accepted, got {other:?}")),
    }

    let delegate_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: identity_subject,
        typ: "iam.delegate".to_string(),
        auth: root_id,
        seq: 2,
        prev: Some(genesis_id),
        refs: vec![genesis_id],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let delegate_body = Value::Map(vec![
        (
            Value::Text("delegate".to_string()),
            Value::Bytes(device_id.as_bytes().to_vec()),
        ),
        (
            Value::Text("scope".to_string()),
            Value::Text("all".to_string()),
        ),
        (Value::Text("expires".to_string()), Value::Integer(0.into())),
    ]);
    let delegate = AssertionPlaintext::sign(delegate_header, delegate_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let delegate_bytes = delegate.to_cbor().map_err(|e| e.to_string())?;
    let delegate_id = delegate.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &delegate_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected delegate accepted, got {other:?}")),
    }

    let revoke_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: identity_subject,
        typ: "iam.delegate.revoke".to_string(),
        auth: root_id,
        seq: 3,
        prev: Some(delegate_id),
        refs: vec![delegate_id],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let revoke_body = Value::Map(vec![(
        Value::Text("delegate".to_string()),
        Value::Bytes(device_id.as_bytes().to_vec()),
    )]);
    let revoke = AssertionPlaintext::sign(revoke_header, revoke_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let revoke_bytes = revoke.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &revoke_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected revoke accepted, got {other:?}")),
    }

    let action_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let action_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: action_subject,
        typ: "action.Set".to_string(),
        auth: device_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let action_body = Value::Map(vec![(
        Value::Text("value".to_string()),
        Value::Integer(1.into()),
    )]);
    let action = AssertionPlaintext::sign(action_header, action_body, &device_sk)
        .map_err(|e| e.to_string())?;
    let action_bytes = action.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &action_bytes, &mut keys) {
        Err(dharma_core::net::ingest::IngestError::Validation(reason)) => {
            if !reason.contains("device revoked") {
                return Err(format!("expected device revoked, got {reason}"));
            }
        }
        other => return Err(format!("expected device revoked, got {other:?}")),
    }
    Ok(())
}

fn prop_permission_summary_denies_fast_reject(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (note_schema, note_contract) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let identity_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (root_sk, root_id) = crypto::generate_identity_keypair(rng);
    let genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: identity_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let genesis_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.fastreject".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(root_id.as_bytes().to_vec()),
        ),
    ]);
    let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let genesis_bytes = genesis.to_cbor().map_err(|e| e.to_string())?;
    let genesis_id = genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected genesis accepted, got {other:?}")),
    }

    let profile_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: identity_subject,
        typ: "identity.profile".to_string(),
        auth: root_id,
        seq: 2,
        prev: Some(genesis_id),
        refs: vec![genesis_id],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let profile_body = Value::Map(vec![
        (
            Value::Text("alias".to_string()),
            Value::Text("tester".to_string()),
        ),
        (
            Value::Text("roles".to_string()),
            Value::Array(vec![Value::Text("finance.approver".to_string())]),
        ),
    ]);
    let profile = AssertionPlaintext::sign(profile_header, profile_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let profile_bytes = profile.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &profile_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected profile accepted, got {other:?}")),
    }

    let (schema_id, contract_id) = store_cqrs_artifacts(&store)?;
    let mut rule_roles = BTreeSet::new();
    rule_roles.insert("finance.viewer".to_string());
    let mut actions = BTreeMap::new();
    actions.insert(
        "Set".to_string(),
        PermissionRule {
            roles: rule_roles,
            exhaustive: true,
        },
    );
    let summary = PermissionSummary {
        v: 1,
        contract: contract_id,
        ver: DEFAULT_DATA_VERSION,
        actions,
        queries: BTreeMap::new(),
        role_scopes: BTreeMap::new(),
        public: PublicPermissions::default(),
    };
    store
        .put_permission_summary(&summary)
        .map_err(|e| e.to_string())?;

    let action_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let action_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: action_subject,
        typ: "action.Set".to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let action_body = Value::Map(vec![(
        Value::Text("value".to_string()),
        Value::Integer(5.into()),
    )]);
    let action = AssertionPlaintext::sign(action_header, action_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let action_bytes = action.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &action_bytes, &mut keys) {
        Err(dharma_core::net::ingest::IngestError::Validation(reason)) => {
            if !reason.contains("summary denied") {
                return Err(format!("expected summary denied, got {reason}"));
            }
        }
        other => return Err(format!("expected summary deny, got {other:?}")),
    }
    Ok(())
}

fn prop_permission_summary_allows_contract_reject(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (note_schema, note_contract) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let identity_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (root_sk, root_id) = crypto::generate_identity_keypair(rng);
    let genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: identity_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let genesis_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.reject".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(root_id.as_bytes().to_vec()),
        ),
    ]);
    let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let genesis_bytes = genesis.to_cbor().map_err(|e| e.to_string())?;
    let genesis_id = genesis.assertion_id().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected genesis accepted, got {other:?}")),
    }

    let profile_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: identity_subject,
        typ: "identity.profile".to_string(),
        auth: root_id,
        seq: 2,
        prev: Some(genesis_id),
        refs: vec![genesis_id],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let profile_body = Value::Map(vec![
        (
            Value::Text("alias".to_string()),
            Value::Text("tester".to_string()),
        ),
        (
            Value::Text("roles".to_string()),
            Value::Array(vec![Value::Text("finance.approver".to_string())]),
        ),
    ]);
    let profile = AssertionPlaintext::sign(profile_header, profile_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let profile_bytes = profile.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &profile_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected profile accepted, got {other:?}")),
    }

    let (schema, _) = cqrs_schema_and_wasm();
    let schema_bytes = schema.to_cbor().map_err(|e| e.to_string())?;
    let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
    store
        .put_object(
            &EnvelopeId::from_bytes(*schema_id.as_bytes()),
            &schema_bytes,
        )
        .map_err(|e| e.to_string())?;
    let contract_bytes = reject_contract_bytes();
    let contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
    store
        .put_object(
            &EnvelopeId::from_bytes(*contract_id.as_bytes()),
            &contract_bytes,
        )
        .map_err(|e| e.to_string())?;

    let mut rule_roles = BTreeSet::new();
    rule_roles.insert("finance.approver".to_string());
    let mut actions = BTreeMap::new();
    actions.insert(
        "Set".to_string(),
        PermissionRule {
            roles: rule_roles,
            exhaustive: true,
        },
    );
    let summary = PermissionSummary {
        v: 1,
        contract: contract_id,
        ver: DEFAULT_DATA_VERSION,
        actions,
        queries: BTreeMap::new(),
        role_scopes: BTreeMap::new(),
        public: PublicPermissions::default(),
    };
    store
        .put_permission_summary(&summary)
        .map_err(|e| e.to_string())?;

    let action_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let action_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: action_subject,
        typ: "action.Set".to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let action_body = Value::Map(vec![(
        Value::Text("value".to_string()),
        Value::Integer(5.into()),
    )]);
    let action = AssertionPlaintext::sign(action_header, action_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let action_bytes = action.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &action_bytes, &mut keys) {
        Err(dharma_core::net::ingest::IngestError::Validation(reason)) => {
            if !reason.contains("contract rejected") {
                return Err(format!("expected contract rejected, got {reason}"));
            }
        }
        other => return Err(format!("expected contract rejection, got {other:?}")),
    }
    Ok(())
}

fn prop_permission_summary_corrupt_fallback(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (note_schema, note_contract) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let identity_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (root_sk, root_id) = crypto::generate_identity_keypair(rng);
    let genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: identity_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let genesis_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.corrupt".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(root_id.as_bytes().to_vec()),
        ),
    ]);
    let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let genesis_bytes = genesis.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected genesis accepted, got {other:?}")),
    }

    let (schema_id, contract_id) = store_cqrs_artifacts(&store)?;
    let dir = store.permission_summaries_dir();
    store
        .env()
        .create_dir_all(&dir)
        .map_err(|e| e.to_string())?;
    let path = dir.join(format!("{}.cbor", contract_id.to_hex()));
    store
        .env()
        .write(&path, b"not-cbor")
        .map_err(|e| e.to_string())?;

    let action_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let action_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: action_subject,
        typ: "action.Set".to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let action_body = Value::Map(vec![(
        Value::Text("value".to_string()),
        Value::Integer(1.into()),
    )]);
    let action = AssertionPlaintext::sign(action_header, action_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let action_bytes = action.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &action_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => {
            return Err(format!(
                "expected accepted with corrupt summary, got {other:?}"
            ))
        }
    }
    Ok(())
}

fn prop_permission_summary_version_mismatch(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let mut keys = Keyring::new();
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let (note_schema, note_contract) =
        builtins::ensure_note_artifacts(&store).map_err(|e| e.to_string())?;

    let identity_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (root_sk, root_id) = crypto::generate_identity_keypair(rng);
    let genesis_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: identity_subject,
        typ: ATLAS_IDENTITY_GENESIS.to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: note_schema,
        contract: note_contract,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let genesis_body = Value::Map(vec![
        (
            Value::Text("atlas_name".to_string()),
            Value::Text("person.local.version".to_string()),
        ),
        (
            Value::Text("owner_key".to_string()),
            Value::Bytes(root_id.as_bytes().to_vec()),
        ),
    ]);
    let genesis = AssertionPlaintext::sign(genesis_header, genesis_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let genesis_bytes = genesis.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &genesis_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => return Err(format!("expected genesis accepted, got {other:?}")),
    }

    let (schema_id, contract_id) = store_cqrs_artifacts(&store)?;
    let mut rule_roles = BTreeSet::new();
    rule_roles.insert("finance.viewer".to_string());
    let mut actions = BTreeMap::new();
    actions.insert(
        "Set".to_string(),
        PermissionRule {
            roles: rule_roles,
            exhaustive: true,
        },
    );
    let summary = PermissionSummary {
        v: 1,
        contract: contract_id,
        ver: DEFAULT_DATA_VERSION + 1,
        actions,
        queries: BTreeMap::new(),
        role_scopes: BTreeMap::new(),
        public: PublicPermissions::default(),
    };
    store
        .put_permission_summary(&summary)
        .map_err(|e| e.to_string())?;

    let action_subject = SubjectId::from_bytes(rand_bytes32(rng));
    let action_header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: action_subject,
        typ: "action.Set".to_string(),
        auth: root_id,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: schema_id,
        contract: contract_id,
        note: None,
        meta: add_signer_meta(None, &identity_subject),
    };
    let action_body = Value::Map(vec![(
        Value::Text("value".to_string()),
        Value::Integer(3.into()),
    )]);
    let action = AssertionPlaintext::sign(action_header, action_body, &root_sk)
        .map_err(|e| e.to_string())?;
    let action_bytes = action.to_cbor().map_err(|e| e.to_string())?;
    match ingest_object(&store, &mut index, &action_bytes, &mut keys) {
        Ok(IngestStatus::Accepted(_)) => {}
        other => {
            return Err(format!(
                "expected accepted with summary ver mismatch, got {other:?}"
            ))
        }
    }
    Ok(())
}

fn prop_fabric_ad_tamper(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let ad = Advertisement {
        v: 1,
        provider_id: pk,
        ts: 10,
        ttl: 30,
        endpoints: vec![Endpoint {
            protocol: "tcp".to_string(),
            address: "127.0.0.1:3000".to_string(),
        }],
        shards: vec![ShardAd {
            table: "invoice".to_string(),
            shard: 1,
            watermark: 9,
        }],
        load: 10,
        domain: "corp.example".to_string(),
        policy_hash: [7u8; 32],
        oracles: vec![],
        sig: vec![],
    };
    let payload = cbor::encode_canonical_value(&ad.signed_value()).map_err(|e| e.to_string())?;
    let sig = crypto::sign(&sk, &payload);
    let signed = Advertisement { sig, ..ad.clone() };
    let ok = signed.verify().map_err(|e| e.to_string())?;
    if !ok {
        return Err("expected signed ad to verify".to_string());
    }
    let mut tampered = signed.clone();
    tampered.domain = "corp.evil".to_string();
    let ok = tampered.verify().map_err(|e| e.to_string())?;
    if ok {
        return Err("tampered ad verified unexpectedly".to_string());
    }
    Ok(())
}

fn prop_fabric_token_scope(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    struct DummyDispatcher {
        now: u64,
    }
    impl FabricDispatcher for DummyDispatcher {
        fn now(&self) -> u64 {
            self.now
        }
        fn exec_action(
            &self,
            req: &FabricRequest,
            _subject: &SubjectId,
            _action: &str,
            _args: &Value,
        ) -> Result<FabricResponse, DharmaError> {
            Ok(FabricResponse {
                req_id: req.req_id,
                status: 200,
                watermark: 0,
                payload: Vec::new(),
                stats: ExecStats::default(),
                provenance: None,
            })
        }
        fn exec_query(
            &self,
            req: &FabricRequest,
            _subject: &SubjectId,
            _query: &str,
            _params: &Value,
            _predefined: bool,
        ) -> Result<FabricResponse, DharmaError> {
            Ok(FabricResponse {
                req_id: req.req_id,
                status: 200,
                watermark: 0,
                payload: Vec::new(),
                stats: ExecStats::default(),
                provenance: None,
            })
        }
        fn query_fast(
            &self,
            req: &FabricRequest,
            _table: &str,
            _key: &Value,
            _query: &str,
        ) -> Result<FabricResponse, DharmaError> {
            Ok(FabricResponse {
                req_id: req.req_id,
                status: 200,
                watermark: 0,
                payload: Vec::new(),
                stats: ExecStats::default(),
                provenance: None,
            })
        }
        fn query_wide(
            &self,
            req: &FabricRequest,
            _table: &str,
            _shard: u32,
            _query: &str,
        ) -> Result<FabricResponse, DharmaError> {
            Ok(FabricResponse {
                req_id: req.req_id,
                status: 200,
                watermark: 0,
                payload: Vec::new(),
                stats: ExecStats::default(),
                provenance: None,
            })
        }
        fn fetch(
            &self,
            req: &FabricRequest,
            _id: &EnvelopeId,
        ) -> Result<FabricResponse, DharmaError> {
            Ok(FabricResponse {
                req_id: req.req_id,
                status: 200,
                watermark: 0,
                payload: Vec::new(),
                stats: ExecStats::default(),
                provenance: None,
            })
        }
        fn oracle_invoke(
            &self,
            req: &FabricRequest,
            _name: &str,
            _mode: dharma_core::fabric::types::OracleMode,
            _timing: dharma_core::fabric::types::OracleTiming,
            _input: &Value,
        ) -> Result<FabricResponse, DharmaError> {
            Ok(FabricResponse {
                req_id: req.req_id,
                status: 200,
                watermark: 0,
                payload: Vec::new(),
                stats: ExecStats::default(),
                provenance: None,
            })
        }
    }

    let (_sk, issuer) = crypto::generate_identity_keypair(rng);
    let token = CapToken {
        v: 1,
        id: [1u8; 32],
        issuer,
        domain: "corp.example".to_string(),
        level: "member".to_string(),
        subject: None,
        scopes: vec![Scope::Table("invoice".to_string())],
        ops: vec![Op::Read],
        actions: vec![],
        queries: vec![],
        flags: vec![Flag::AllowCustomQuery],
        oracles: vec![],
        constraints: vec![],
        nbf: 0,
        exp: 100,
        sig: vec![],
    };
    let dispatcher = DummyDispatcher { now: 10 };
    let bad_req = FabricRequest {
        req_id: [9u8; 16],
        cap: token.clone(),
        op: FabricOp::QueryFast {
            table: "payments".to_string(),
            key: Value::Integer(1.into()),
            query: "by_id".to_string(),
        },
        deadline: 100,
    };
    match dispatch(&dispatcher, &bad_req) {
        Err(err) => {
            let msg = format!("{err}");
            if !msg.contains("scope not allowed") {
                return Err(format!("expected scope not allowed, got {msg}"));
            }
        }
        Ok(_) => return Err("expected scope rejection".to_string()),
    }

    let good_req = FabricRequest {
        req_id: [10u8; 16],
        cap: token,
        op: FabricOp::QueryFast {
            table: "invoice".to_string(),
            key: Value::Integer(1.into()),
            query: "by_id".to_string(),
        },
        deadline: 100,
    };
    match dispatch(&dispatcher, &good_req) {
        Ok(_) => {}
        Err(err) => return Err(format!("expected allow, got {err}")),
    }
    Ok(())
}

fn prop_fabric_directory_split_brain(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let temp_a = tempfile::tempdir().map_err(|e| e.to_string())?;
    let temp_b = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store_a = Store::from_root(temp_a.path());
    let store_b = Store::from_root(temp_b.path());
    let (signing_key, owner_id) = crypto::generate_identity_keypair(rng);
    let subject = DirectoryClient::default_subject();
    let domain = format!("corp.{}", rng.next_u32());
    let schema_id = SchemaId::from_bytes([9u8; 32]);
    let contract_id = ContractId::from_bytes([10u8; 32]);
    let hash_a = [3u8; 32];
    let hash_b = [9u8; 32];
    let hex_a = hex::encode(hash_a);
    let hex_b = hex::encode(hash_b);

    let append_policy =
        |store: &Store, seq: u64, policy_hex: &str| -> Result<AssertionId, String> {
            let header = AssertionHeader {
                v: crypto::PROTOCOL_VERSION,
                ver: DEFAULT_DATA_VERSION,
                sub: subject,
                typ: "fabric.domain.policy".to_string(),
                auth: owner_id,
                seq,
                prev: None,
                refs: Vec::new(),
                ts: None,
                schema: schema_id,
                contract: contract_id,
                note: None,
                meta: None,
            };
            let body = Value::Map(vec![
                (
                    Value::Text("domain".to_string()),
                    Value::Text(domain.clone()),
                ),
                (
                    Value::Text("policy_hash".to_string()),
                    Value::Text(policy_hex.to_string()),
                ),
            ]);
            let assertion =
                AssertionPlaintext::sign(header, body, &signing_key).map_err(|e| e.to_string())?;
            let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
            let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
            let envelope_id = crypto::envelope_id(&bytes);
            append_assertion(
                store.env(),
                &subject,
                seq,
                assertion_id,
                envelope_id,
                "fabric.domain.policy",
                &bytes,
            )
            .map_err(|e| e.to_string())?;
            Ok(assertion_id)
        };

    let id_a = append_policy(&store_a, 1, &hex_a)?;
    let id_b = append_policy(&store_a, 1, &hex_b)?;
    let _ = append_policy(&store_b, 1, &hex_b)?;
    let _ = append_policy(&store_b, 1, &hex_a)?;

    let state_a = DirectoryState::load(&store_a, &subject).map_err(|e| e.to_string())?;
    let state_b = DirectoryState::load(&store_b, &subject).map_err(|e| e.to_string())?;
    let policy_a = state_a.policy_hash_for_domain(&domain);
    let policy_b = state_b.policy_hash_for_domain(&domain);
    if policy_a != policy_b {
        return Err("directory state did not converge".to_string());
    }
    let expected = if id_a.as_bytes() > id_b.as_bytes() {
        hash_a
    } else {
        hash_b
    };
    if policy_a != Some(expected) {
        return Err("directory policy hash mismatch after convergence".to_string());
    }
    Ok(())
}

fn prop_fabric_ad_expiry_ttl(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let (_sk, pk) = crypto::generate_identity_keypair(rng);
    let ad = Advertisement {
        v: 1,
        provider_id: pk,
        ts: 10,
        ttl: 5,
        endpoints: vec![],
        shards: vec![ShardAd {
            table: "invoice".to_string(),
            shard: 0,
            watermark: 0,
        }],
        load: 0,
        domain: "corp.example".to_string(),
        policy_hash: [0u8; 32],
        oracles: vec![],
        sig: vec![1; 64],
    };
    let mut ads = AdStore::new();
    ads.insert(ad);
    ads.prune(12);
    if ads.get_providers_for_shard("invoice", 0).is_empty() {
        return Err("fresh ad was pruned unexpectedly".to_string());
    }
    ads.prune(20);
    if !ads.get_providers_for_shard("invoice", 0).is_empty() {
        return Err("stale ad was not pruned".to_string());
    }
    Ok(())
}

fn prop_fabric_token_expiry(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    struct DummyDispatcher {
        now: u64,
    }
    impl FabricDispatcher for DummyDispatcher {
        fn now(&self) -> u64 {
            self.now
        }
        fn exec_action(
            &self,
            req: &FabricRequest,
            _subject: &SubjectId,
            _action: &str,
            _args: &Value,
        ) -> Result<FabricResponse, DharmaError> {
            Ok(FabricResponse {
                req_id: req.req_id,
                status: 200,
                watermark: 0,
                payload: Vec::new(),
                stats: ExecStats::default(),
                provenance: None,
            })
        }
        fn exec_query(
            &self,
            req: &FabricRequest,
            _subject: &SubjectId,
            _query: &str,
            _params: &Value,
            _predefined: bool,
        ) -> Result<FabricResponse, DharmaError> {
            Ok(FabricResponse {
                req_id: req.req_id,
                status: 200,
                watermark: 0,
                payload: Vec::new(),
                stats: ExecStats::default(),
                provenance: None,
            })
        }
        fn query_fast(
            &self,
            req: &FabricRequest,
            _table: &str,
            _key: &Value,
            _query: &str,
        ) -> Result<FabricResponse, DharmaError> {
            Ok(FabricResponse {
                req_id: req.req_id,
                status: 200,
                watermark: 0,
                payload: Vec::new(),
                stats: ExecStats::default(),
                provenance: None,
            })
        }
        fn query_wide(
            &self,
            req: &FabricRequest,
            _table: &str,
            _shard: u32,
            _query: &str,
        ) -> Result<FabricResponse, DharmaError> {
            Ok(FabricResponse {
                req_id: req.req_id,
                status: 200,
                watermark: 0,
                payload: Vec::new(),
                stats: ExecStats::default(),
                provenance: None,
            })
        }
        fn fetch(
            &self,
            req: &FabricRequest,
            _id: &EnvelopeId,
        ) -> Result<FabricResponse, DharmaError> {
            Ok(FabricResponse {
                req_id: req.req_id,
                status: 200,
                watermark: 0,
                payload: Vec::new(),
                stats: ExecStats::default(),
                provenance: None,
            })
        }
        fn oracle_invoke(
            &self,
            req: &FabricRequest,
            _name: &str,
            _mode: dharma_core::fabric::types::OracleMode,
            _timing: dharma_core::fabric::types::OracleTiming,
            _input: &Value,
        ) -> Result<FabricResponse, DharmaError> {
            Ok(FabricResponse {
                req_id: req.req_id,
                status: 200,
                watermark: 0,
                payload: Vec::new(),
                stats: ExecStats::default(),
                provenance: None,
            })
        }
    }

    let (_sk, issuer) = crypto::generate_identity_keypair(rng);
    let token = CapToken {
        v: 1,
        id: [2u8; 32],
        issuer,
        domain: "corp.example".to_string(),
        level: "member".to_string(),
        subject: None,
        scopes: vec![Scope::Table("objects".to_string())],
        ops: vec![Op::Read],
        actions: vec![],
        queries: vec![],
        flags: vec![],
        oracles: vec![],
        constraints: vec![],
        nbf: 10,
        exp: 20,
        sig: vec![],
    };
    let req = FabricRequest {
        req_id: [7u8; 16],
        cap: token,
        op: FabricOp::Fetch {
            id: EnvelopeId::from_bytes(rand_bytes32(rng)),
        },
        deadline: 100,
    };

    let early = DummyDispatcher { now: 5 };
    match dispatch(&early, &req) {
        Err(err) => {
            let msg = format!("{err}");
            if !msg.contains("token not valid") {
                return Err(format!("expected token not valid, got {msg}"));
            }
        }
        Ok(_) => return Err("expected early token rejection".to_string()),
    }

    let ok = DummyDispatcher { now: 15 };
    match dispatch(&ok, &req) {
        Ok(_) => {}
        Err(err) => return Err(format!("expected token accepted, got {err}")),
    }

    let late = DummyDispatcher { now: 25 };
    match dispatch(&late, &req) {
        Err(err) => {
            let msg = format!("{err}");
            if !msg.contains("token not valid") {
                return Err(format!("expected token not valid, got {msg}"));
            }
        }
        Ok(_) => return Err("expected expired token rejection".to_string()),
    }
    Ok(())
}

const IAM_PRIVATE_FIELDS: [&str; 3] = ["display_name", "email", "phone"];

fn iam_state_value() -> Value {
    Value::Map(vec![
        (
            Value::Text("display_name".to_string()),
            Value::Text("Ada Lovelace".to_string()),
        ),
        (
            Value::Text("handle".to_string()),
            Value::Text("ada".to_string()),
        ),
        (
            Value::Text("email".to_string()),
            Value::Text("ada@example.com".to_string()),
        ),
        (
            Value::Text("phone".to_string()),
            Value::Text("+1-555-0100".to_string()),
        ),
        (
            Value::Text("profile".to_string()),
            Value::Map(vec![(
                Value::Text("title".to_string()),
                Value::Text("engineer".to_string()),
            )]),
        ),
    ])
}

fn iam_public_fields(state: &Value) -> Result<BTreeSet<String>, String> {
    let Value::Map(entries) = state else {
        return Err("expected IAM state map".to_string());
    };
    let mut fields = BTreeSet::new();
    for (k, _) in entries {
        if let Value::Text(name) = k {
            if IAM_PRIVATE_FIELDS
                .iter()
                .any(|private| *private == name.as_str())
            {
                continue;
            }
            fields.insert(name.clone());
        }
    }
    Ok(fields)
}

fn iam_field_access(
    owner: &IdentityKey,
    viewer: &IdentityKey,
    relation: ContactRelation,
    state: &Value,
) -> Result<FieldAccess, String> {
    if owner.as_bytes() == viewer.as_bytes() || relation == ContactRelation::Accepted {
        return Ok(FieldAccess::All);
    }
    let fields = iam_public_fields(state)?;
    Ok(FieldAccess::Fields(fields))
}

fn iam_field_access_from_store(
    store: &Store,
    owner: &IdentityKey,
    viewer: &IdentityKey,
    state: &Value,
) -> Result<FieldAccess, String> {
    let relation = contact_relation(store, owner, viewer).map_err(|e| e.to_string())?;
    iam_field_access(owner, viewer, relation, state)
}

fn value_map_get<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Map(entries) => entries.iter().find_map(|(k, v)| match k {
            Value::Text(name) if name == key => Some(v),
            _ => None,
        }),
        _ => None,
    }
}

fn ensure_visible(value: &Value, key: &str) -> Result<(), String> {
    match value_map_get(value, key) {
        Some(Value::Null) => Err(format!("{key} redacted unexpectedly")),
        Some(_) => Ok(()),
        None => Err(format!("{key} missing unexpectedly")),
    }
}

fn ensure_redacted(value: &Value, key: &str) -> Result<(), String> {
    match value_map_get(value, key) {
        Some(Value::Null) | None => Ok(()),
        Some(_) => Err(format!("{key} was not redacted")),
    }
}

fn append_contact_action(
    store: &Store,
    subject: &SubjectId,
    seq: u64,
    prev: Option<AssertionId>,
    signer: IdentityKey,
    schema: SchemaId,
    contract: ContractId,
    action: &str,
    body: Value,
    sign: impl FnOnce(AssertionHeader, Value) -> Result<AssertionPlaintext, String>,
) -> Result<AssertionId, String> {
    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: DEFAULT_DATA_VERSION,
        sub: *subject,
        typ: format!("action.{action}"),
        auth: signer,
        seq,
        prev,
        refs: prev.into_iter().collect(),
        ts: None,
        schema,
        contract,
        note: None,
        meta: None,
    };
    let assertion = sign(header, body)?;
    let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
    let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
    let envelope_id = crypto::envelope_id(&bytes);
    append_assertion(
        store.env(),
        subject,
        seq,
        assertion_id,
        envelope_id,
        &assertion.header.typ,
        &bytes,
    )
    .map_err(|e| e.to_string())?;
    Ok(assertion_id)
}

fn seed_contact_relation(
    store: &Store,
    owner: IdentityKey,
    contact: IdentityKey,
    relation: ContactRelation,
    sign_owner: &dyn Fn(AssertionHeader, Value) -> Result<AssertionPlaintext, String>,
    sign_contact: &dyn Fn(AssertionHeader, Value) -> Result<AssertionPlaintext, String>,
) -> Result<(), String> {
    let (schema_id, contract_id) =
        builtins::ensure_note_artifacts(store).map_err(|e| e.to_string())?;
    let subject = contact_subject_id(&owner, &contact);
    let mut seq = 1u64;
    let mut prev = None;

    let create_body = Value::Map(vec![
        (
            Value::Text("contact".to_string()),
            Value::Bytes(contact.as_bytes().to_vec()),
        ),
        (
            Value::Text("alias".to_string()),
            Value::Text("buddy".to_string()),
        ),
    ]);
    let create_id = append_contact_action(
        store,
        &subject,
        seq,
        prev,
        owner,
        schema_id,
        contract_id,
        "Create",
        create_body,
        |header, body| sign_owner(header, body),
    )?;
    prev = Some(create_id);
    seq += 1;

    match relation {
        ContactRelation::None => return Ok(()),
        ContactRelation::Pending | ContactRelation::Accepted | ContactRelation::Declined => {
            let request_body = Value::Map(vec![(
                Value::Text("other".to_string()),
                Value::Bytes(contact.as_bytes().to_vec()),
            )]);
            let request_id = append_contact_action(
                store,
                &subject,
                seq,
                prev,
                owner,
                schema_id,
                contract_id,
                "Request",
                request_body,
                |header, body| sign_owner(header, body),
            )?;
            prev = Some(request_id);
            seq += 1;
        }
        ContactRelation::Blocked => {}
    }

    match relation {
        ContactRelation::Accepted => {
            let _ = append_contact_action(
                store,
                &subject,
                seq,
                prev,
                contact,
                schema_id,
                contract_id,
                "Accept",
                Value::Map(vec![]),
                |header, body| sign_contact(header, body),
            )?;
        }
        ContactRelation::Declined => {
            let _ = append_contact_action(
                store,
                &subject,
                seq,
                prev,
                contact,
                schema_id,
                contract_id,
                "Decline",
                Value::Map(vec![]),
                |header, body| sign_contact(header, body),
            )?;
        }
        ContactRelation::Blocked => {
            let _ = append_contact_action(
                store,
                &subject,
                seq,
                prev,
                contact,
                schema_id,
                contract_id,
                "Block",
                Value::Map(vec![]),
                |header, body| sign_contact(header, body),
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn prop_iam_owner_visibility(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let (_sk, owner) = crypto::generate_identity_keypair(rng);
    let state = iam_state_value();
    let access = iam_field_access_from_store(&store, &owner, &owner, &state)?;
    if !matches!(access, FieldAccess::All) {
        return Err("owner access should be full".to_string());
    }
    let view = filter_state_value(state.clone(), &access);
    if view != state {
        return Err("owner view was redacted unexpectedly".to_string());
    }
    Ok(())
}

fn prop_iam_contact_visibility(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let (owner_sk, owner_id) = crypto::generate_identity_keypair(rng);
    let (viewer_sk, viewer_id) = crypto::generate_identity_keypair(rng);
    let sign_owner =
        |header, body| AssertionPlaintext::sign(header, body, &owner_sk).map_err(|e| e.to_string());
    let sign_viewer = |header, body| {
        AssertionPlaintext::sign(header, body, &viewer_sk).map_err(|e| e.to_string())
    };
    seed_contact_relation(
        &store,
        owner_id,
        viewer_id,
        ContactRelation::Accepted,
        &sign_owner,
        &sign_viewer,
    )?;
    let state = iam_state_value();
    let access = iam_field_access_from_store(&store, &owner_id, &viewer_id, &state)?;
    if !matches!(access, FieldAccess::All) {
        return Err("accepted contact should have full access".to_string());
    }
    let view = filter_state_value(state.clone(), &access);
    if view != state {
        return Err("accepted contact view was redacted unexpectedly".to_string());
    }
    Ok(())
}

fn prop_iam_non_contact_redaction(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let store = Store::from_root(temp.path());
    let (owner_sk, owner_id) = crypto::generate_identity_keypair(rng);
    let (viewer_sk, viewer_id) = crypto::generate_identity_keypair(rng);
    let state = iam_state_value();
    let relation = if rng.next_u32() % 2 == 0 {
        ContactRelation::None
    } else {
        ContactRelation::Pending
    };
    let sign_owner =
        |header, body| AssertionPlaintext::sign(header, body, &owner_sk).map_err(|e| e.to_string());
    let sign_viewer = |header, body| {
        AssertionPlaintext::sign(header, body, &viewer_sk).map_err(|e| e.to_string())
    };
    seed_contact_relation(
        &store,
        owner_id,
        viewer_id,
        relation,
        &sign_owner,
        &sign_viewer,
    )?;
    let access = iam_field_access_from_store(&store, &owner_id, &viewer_id, &state)?;
    if matches!(access, FieldAccess::All) {
        return Err("non-contact access should be redacted".to_string());
    }
    ensure_visible(&state, "display_name")?;
    ensure_visible(&state, "email")?;
    ensure_visible(&state, "phone")?;
    let view = filter_state_value(state, &access);
    ensure_visible(&view, "handle")?;
    ensure_redacted(&view, "display_name")?;
    ensure_redacted(&view, "email")?;
    ensure_redacted(&view, "phone")?;
    Ok(())
}

fn prop_iam_declined_blocked_redaction(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    for relation in [ContactRelation::Declined, ContactRelation::Blocked] {
        let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
        let store = Store::from_root(temp.path());
        let (owner_sk, owner_id) = crypto::generate_identity_keypair(rng);
        let (viewer_sk, viewer_id) = crypto::generate_identity_keypair(rng);
        let sign_owner = |header, body| {
            AssertionPlaintext::sign(header, body, &owner_sk).map_err(|e| e.to_string())
        };
        let sign_viewer = |header, body| {
            AssertionPlaintext::sign(header, body, &viewer_sk).map_err(|e| e.to_string())
        };
        seed_contact_relation(
            &store,
            owner_id,
            viewer_id,
            relation,
            &sign_owner,
            &sign_viewer,
        )?;
        let state = iam_state_value();
        let access = iam_field_access_from_store(&store, &owner_id, &viewer_id, &state)?;
        if matches!(access, FieldAccess::All) {
            return Err("declined/blocked should be redacted".to_string());
        }
        let view = filter_state_value(state.clone(), &access);
        ensure_visible(&view, "handle")?;
        ensure_redacted(&view, "display_name")?;
        ensure_redacted(&view, "email")?;
        ensure_redacted(&view, "phone")?;
    }
    Ok(())
}

fn prop_cqrs_replay_deterministic(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let (schema, wasm) = cqrs_schema_and_wasm();
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let mut prev = None;
    for seq in 1..=3 {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: pk,
            seq,
            prev,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("value".to_string()),
            ciborium::value::Value::Integer((seq as i64).into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
        let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
        let env_id = crypto::envelope_id(&bytes);
        append_assertion(
            &env,
            &subject,
            seq,
            assertion_id,
            env_id,
            "action.Set",
            &bytes,
        )
        .map_err(|e| e.to_string())?;
        prev = Some(assertion_id);
    }
    let state_a = load_state(&env, &subject, &schema, &wasm, 1).map_err(|e| e.to_string())?;
    let state_b = load_state(&env, &subject, &schema, &wasm, 1).map_err(|e| e.to_string())?;
    if state_a.memory != state_b.memory {
        return Err("state memory mismatch".to_string());
    }
    Ok(())
}

fn prop_cqrs_decode_deterministic(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let (schema, wasm) = cqrs_schema_and_wasm();
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: 1,
        sub: subject,
        typ: "action.Set".to_string(),
        auth: pk,
        seq: 1,
        prev: None,
        refs: vec![],
        ts: None,
        schema: SchemaId::from_bytes([1u8; 32]),
        contract: ContractId::from_bytes([2u8; 32]),
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let body = ciborium::value::Value::Map(vec![(
        ciborium::value::Value::Text("value".to_string()),
        ciborium::value::Value::Integer(5.into()),
    )]);
    let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
    let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
    let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
    let env_id = crypto::envelope_id(&bytes);
    append_assertion(
        &env,
        &subject,
        1,
        assertion_id,
        env_id,
        "action.Set",
        &bytes,
    )
    .map_err(|e| e.to_string())?;
    let state = load_state(&env, &subject, &schema, &wasm, 1).map_err(|e| e.to_string())?;
    let decoded_a = decode_state(&state.memory, &schema).map_err(|e| e.to_string())?;
    let decoded_b = decode_state(&state.memory, &schema).map_err(|e| e.to_string())?;
    if decoded_a != decoded_b {
        return Err("decode mismatch".to_string());
    }
    Ok(())
}

fn prop_dharmaq_rebuild_deterministic(
    rng: &mut ChaCha20Rng,
    _iterations: usize,
) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    for seq in 1..=3 {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Note".to_string(),
            auth: pk,
            seq,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("text".to_string()),
            ciborium::value::Value::Text("hello".to_string()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
        let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
        let env_id = crypto::envelope_id(&bytes);
        append_assertion(
            &env,
            &subject,
            seq,
            assertion_id,
            env_id,
            "action.Note",
            &bytes,
        )
        .map_err(|e| e.to_string())?;
    }
    dharmaq::rebuild_env(&env).map_err(|e| e.to_string())?;
    let results_a = dharmaq::search_env(&env, "hello", 10).map_err(|e| e.to_string())?;
    dharmaq::rebuild_env(&env).map_err(|e| e.to_string())?;
    let results_b = dharmaq::search_env(&env, "hello", 10).map_err(|e| e.to_string())?;
    if rows_key(&results_a) != rows_key(&results_b) {
        return Err("rebuild results mismatch".to_string());
    }
    Ok(())
}

fn prop_dharmaq_and_commutative(rng: &mut ChaCha20Rng, _iterations: usize) -> Result<(), String> {
    let env = SimEnv::new(rng.next_u64(), 0);
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    for seq in 1..=3 {
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: 1,
            sub: subject,
            typ: "action.Note".to_string(),
            auth: pk,
            seq,
            prev: None,
            refs: vec![],
            ts: None,
            schema: SchemaId::from_bytes([1u8; 32]),
            contract: ContractId::from_bytes([2u8; 32]),
            note: None,
            meta: None,
        };
        let body = ciborium::value::Value::Map(vec![]);
        let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
        let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
        let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
        let env_id = crypto::envelope_id(&bytes);
        append_assertion(
            &env,
            &subject,
            seq,
            assertion_id,
            env_id,
            "action.Note",
            &bytes,
        )
        .map_err(|e| e.to_string())?;
    }
    dharmaq::rebuild_env(&env).map_err(|e| e.to_string())?;
    let plan_a = QueryPlan {
        table: "assertions".to_string(),
        filter: Some(Filter::And(vec![
            Filter::Leaf(Predicate::Seq {
                op: CmpOp::Gte,
                value: 1,
            }),
            Filter::Leaf(Predicate::TypEq("action.Note".to_string())),
        ])),
        limit: 10,
    };
    let plan_b = QueryPlan {
        table: "assertions".to_string(),
        filter: Some(Filter::And(vec![
            Filter::Leaf(Predicate::TypEq("action.Note".to_string())),
            Filter::Leaf(Predicate::Seq {
                op: CmpOp::Gte,
                value: 1,
            }),
        ])),
        limit: 10,
    };
    let results_a = dharmaq::execute_env(&env, &plan_a).map_err(|e| e.to_string())?;
    let results_b = dharmaq::execute_env(&env, &plan_b).map_err(|e| e.to_string())?;
    if rows_key(&results_a) != rows_key(&results_b) {
        return Err("AND results differ".to_string());
    }
    Ok(())
}

fn write_ticket(failure: &TestFailure, opts: &TestOptions) -> Result<PathBuf, DharmaError> {
    let dir = PathBuf::from("tests").join("failures");
    fs::create_dir_all(&dir)?;
    let name = format!(
        "DHARMA-FAILURE-{}-{}.md",
        failure.seed,
        failure.property.replace(':', "_")
    );
    let path = dir.join(name);
    let contents = format_ticket(failure, opts);
    fs::write(&path, contents)?;
    Ok(path)
}

fn format_ticket(failure: &TestFailure, opts: &TestOptions) -> String {
    let mut contents = format!(
        "# DHARMA Failure\n\n- Property: {}\n- Seed: {}\n- Deep: {}\n- Chaos: {}\n- CI: {}\n- External: {}\n\n## Details\n{}\n",
        failure.property,
        failure.seed,
        opts.deep,
        opts.chaos,
        opts.ci,
        opts.external,
        failure.details
    );
    contents.push_str("\n## Trace\n");
    match &failure.trace {
        Some(trace) if !trace.is_empty() => {
            for line in trace {
                contents.push_str("- ");
                contents.push_str(line);
                contents.push('\n');
            }
        }
        Some(_) => {
            contents.push_str("- <empty>\n");
        }
        None => {
            contents.push_str("- <none>\n");
        }
    }
    contents
}

#[cfg(test)]
mod ticket_tests {
    use super::*;

    #[test]
    fn format_ticket_includes_trace_section() {
        let failure = TestFailure {
            property: "P-SIM-001",
            seed: 7,
            details: "boom".to_string(),
            trace: None,
        };
        let opts = TestOptions {
            deep: true,
            chaos: false,
            ci: true,
            replay_seed: None,
            relay_only: false,
            external: false,
        };
        let content = format_ticket(&failure, &opts);
        assert!(content.contains("## Trace"));
        assert!(content.contains("<none>"));
    }

    #[test]
    fn format_ticket_renders_trace_lines() {
        let failure = TestFailure {
            property: "P-SIM-001",
            seed: 9,
            details: "boom".to_string(),
            trace: Some(vec!["line-a".to_string(), "line-b".to_string()]),
        };
        let opts = TestOptions {
            deep: true,
            chaos: true,
            ci: false,
            replay_seed: None,
            relay_only: false,
            external: false,
        };
        let content = format_ticket(&failure, &opts);
        assert!(content.contains("- line-a"));
        assert!(content.contains("- line-b"));
    }
}

fn rand_bytes(rng: &mut ChaCha20Rng, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    rng.fill_bytes(&mut out);
    out
}

fn rand_nonce(rng: &mut ChaCha20Rng) -> [u8; 12] {
    let mut out = [0u8; 12];
    rng.fill_bytes(&mut out);
    out
}

fn rand_bytes32(rng: &mut ChaCha20Rng) -> [u8; 32] {
    let mut out = [0u8; 32];
    rng.fill_bytes(&mut out);
    out
}

fn rand_value(rng: &mut ChaCha20Rng, depth: usize) -> ciborium::value::Value {
    let choice = (rng.next_u32() % 5) as usize;
    if depth > 2 {
        return match choice % 3 {
            0 => ciborium::value::Value::Integer((rng.next_u32() as i64).into()),
            1 => ciborium::value::Value::Bool((rng.next_u32() % 2) == 0),
            _ => ciborium::value::Value::Text(rand_text(rng, 8)),
        };
    }
    match choice {
        0 => ciborium::value::Value::Integer((rng.next_u32() as i64).into()),
        1 => ciborium::value::Value::Bool((rng.next_u32() % 2) == 0),
        2 => ciborium::value::Value::Text(rand_text(rng, 8)),
        3 => {
            let len = (rng.next_u32() % 4) as usize;
            let mut items = Vec::new();
            for _ in 0..len {
                items.push(rand_value(rng, depth + 1));
            }
            ciborium::value::Value::Array(items)
        }
        _ => {
            let len = (rng.next_u32() % 3) as usize;
            let mut entries = BTreeMap::new();
            for _ in 0..len {
                entries.insert(rand_text(rng, 6), rand_value(rng, depth + 1));
            }
            let mut map = Vec::new();
            for (k, v) in entries {
                map.push((ciborium::value::Value::Text(k), v));
            }
            ciborium::value::Value::Map(map)
        }
    }
}

fn rand_text(rng: &mut ChaCha20Rng, max: usize) -> String {
    let len = (rng.next_u32() as usize % max).max(1);
    let mut s = String::new();
    for _ in 0..len {
        let c = b'a' + (rng.next_u32() % 26) as u8;
        s.push(c as char);
    }
    s
}

fn rows_key(rows: &[dharmaq::QueryRow]) -> Vec<(AssertionId, SubjectId, u64, u32)> {
    rows.iter()
        .map(|row| (row.assertion_id, row.subject, row.seq, row.score))
        .collect()
}

fn snapshot_env(env: &SimEnv) -> Result<Vec<(SubjectId, u64, AssertionId)>, String> {
    let store = Store::new(env);
    let mut out = Vec::new();
    let mut subjects = store.list_subjects().map_err(|e| e.to_string())?;
    subjects.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    for subject in subjects {
        for record in list_assertions(env, &subject).map_err(|e| e.to_string())? {
            out.push((subject, record.seq, record.assertion_id));
        }
    }
    out.sort_by(|a, b| {
        a.0.as_bytes()
            .cmp(b.0.as_bytes())
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.as_bytes().cmp(b.2.as_bytes()))
    });
    Ok(out)
}

fn diff_snapshots(
    expected: &[(SubjectId, u64, AssertionId)],
    actual: &[(SubjectId, u64, AssertionId)],
) -> String {
    use std::collections::BTreeSet;
    let exp: BTreeSet<_> = expected.iter().copied().collect();
    let act: BTreeSet<_> = actual.iter().copied().collect();
    let missing: Vec<_> = exp.difference(&act).cloned().collect();
    let extra: Vec<_> = act.difference(&exp).cloned().collect();
    let mut parts = Vec::new();
    if !missing.is_empty() {
        parts.push(format!(
            "missing {} entries (first {:?})",
            missing.len(),
            missing.first()
        ));
    }
    if !extra.is_empty() {
        parts.push(format!(
            "extra {} entries (first {:?})",
            extra.len(),
            extra.first()
        ));
    }
    if parts.is_empty() {
        "snapshots differ".to_string()
    } else {
        parts.join("; ")
    }
}

fn cqrs_schema_and_wasm() -> (CqrsSchema, Vec<u8>) {
    let mut fields = BTreeMap::new();
    fields.insert(
        "total".to_string(),
        FieldSchema {
            typ: TypeSpec::Int,
            default: Some(ciborium::value::Value::Integer(0.into())),
            visibility: Visibility::Public,
        },
    );
    let mut actions = BTreeMap::new();
    let mut args = BTreeMap::new();
    args.insert("value".to_string(), TypeSpec::Int);
    let mut arg_vis = BTreeMap::new();
    arg_vis.insert("value".to_string(), Visibility::Public);
    actions.insert(
        "Set".to_string(),
        ActionSchema {
            args,
            arg_vis,
            doc: None,
        },
    );
    let schema = CqrsSchema {
        namespace: "test".to_string(),
        version: "1.0.0".to_string(),
        aggregate: "Dummy".to_string(),
        extends: None,
        implements: Vec::new(),
        structs: BTreeMap::new(),
        fields,
        actions,
        queries: BTreeMap::new(),
        projections: BTreeMap::new(),
        concurrency: ConcurrencyMode::Strict,
    };
    let wasm = wat::parse_str(
        r#"(module
            (memory (export "memory") 1)
            (func (export "validate") (result i32)
              i32.const 0)
            (func (export "reduce") (result i32)
              (local $val i64)
              i32.const 0x2004
              i64.load
              local.set $val
              i32.const 0
              local.get $val
              i64.store
              i32.const 0)
          )"#,
    )
    .unwrap();
    (schema, wasm)
}

fn make_assertion(
    rng: &mut ChaCha20Rng,
    seq: u64,
    prev: Option<AssertionId>,
    schema: SchemaId,
    contract: ContractId,
) -> Result<(AssertionPlaintext, [u8; 32]), String> {
    let (sk, pk) = crypto::generate_identity_keypair(rng);
    let subject = SubjectId::from_bytes(rand_bytes32(rng));
    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: 1,
        sub: subject,
        typ: "action.Set".to_string(),
        auth: pk,
        seq,
        prev,
        refs: vec![],
        ts: None,
        schema,
        contract,
        note: None,
        meta: add_signer_meta(None, &subject),
    };
    let body = ciborium::value::Value::Map(vec![]);
    let assertion = AssertionPlaintext::sign(header, body, &sk).map_err(|e| e.to_string())?;
    let mut key = [0u8; 32];
    rng.fill_bytes(&mut key);
    Ok((assertion, key))
}

fn wrap_envelope(assertion: &AssertionPlaintext, key: &[u8; 32]) -> Result<Vec<u8>, String> {
    let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
    let kid = dharma_core::keys::key_id_for_key(key);
    let nonce = Nonce12::from_bytes([3u8; 12]);
    let env = envelope::encrypt_assertion(&bytes, kid, key, nonce).map_err(|e| e.to_string())?;
    env.to_cbor().map_err(|e| e.to_string())
}

fn store_cqrs_artifacts(store: &Store) -> Result<(SchemaId, ContractId), String> {
    let (schema, wasm) = cqrs_schema_and_wasm();
    let schema_bytes = schema.to_cbor().map_err(|e| e.to_string())?;
    let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
    let schema_obj = EnvelopeId::from_bytes(*schema_id.as_bytes());
    store
        .put_object(&schema_obj, &schema_bytes)
        .map_err(|e| e.to_string())?;
    let contract_id = ContractId::from_bytes(crypto::sha256(&wasm));
    let contract_obj = EnvelopeId::from_bytes(*contract_id.as_bytes());
    store
        .put_object(&contract_obj, &wasm)
        .map_err(|e| e.to_string())?;
    Ok((schema_id, contract_id))
}

fn reject_contract_bytes() -> Vec<u8> {
    wat::parse_str(
        r#"(module
            (memory (export "memory") 1)
            (func (export "validate") (result i32)
              i32.const 1)
            (func (export "reduce") (result i32)
              i32.const 0)
          )"#,
    )
    .unwrap()
}

struct TestNode {
    dir: TempDir,
    env: StdEnv,
    store: Store,
    identity: IdentityState,
    schema_id: SchemaId,
    contract_id: ContractId,
    subjects: HashMap<SubjectId, (u64, Option<AssertionId>)>,
}

impl TestNode {
    fn new(name: &str, rng: &mut ChaCha20Rng) -> Result<Self, String> {
        let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
        let env = StdEnv::new(dir.path());
        identity_store::init_identity(&env, name, "test-pass").map_err(|e| e.to_string())?;
        let identity =
            identity_store::load_identity(&env, "test-pass").map_err(|e| e.to_string())?;
        let store = Store::new(&env);
        let (schema_id, contract_id) = store_cqrs_artifacts(&store)?;
        let mut subjects = HashMap::new();
        // Reserve a deterministic subject for this node.
        let subject = SubjectId::from_bytes(rand_bytes32(rng));
        subjects.insert(subject, (0, None));
        Ok(Self {
            dir,
            env,
            store,
            identity,
            schema_id,
            contract_id,
            subjects,
        })
    }

    fn root(&self) -> &Path {
        self.dir.path()
    }

    fn new_subject(&mut self, rng: &mut ChaCha20Rng) -> SubjectId {
        let subject = SubjectId::from_bytes(rand_bytes32(rng));
        self.subjects.insert(subject, (0, None));
        subject
    }

    fn write_set(&mut self, subject: SubjectId, value: i64) -> Result<AssertionId, String> {
        let (seq, prev) = self.subjects.get(&subject).copied().unwrap_or((0, None));
        let next_seq = seq + 1;
        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: subject,
            typ: "action.Set".to_string(),
            auth: self.identity.root_public_key,
            seq: next_seq,
            prev,
            refs: vec![],
            ts: None,
            schema: self.schema_id,
            contract: self.contract_id,
            note: None,
            meta: add_signer_meta(None, &self.identity.subject_id),
        };
        let body = ciborium::value::Value::Map(vec![(
            ciborium::value::Value::Text("value".to_string()),
            ciborium::value::Value::Integer(value.into()),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &self.identity.root_signing_key)
            .map_err(|e| e.to_string())?;
        let bytes = assertion.to_cbor().map_err(|e| e.to_string())?;
        let assertion_id = assertion.assertion_id().map_err(|e| e.to_string())?;
        let envelope_id = crypto::envelope_id(&bytes);
        self.store
            .put_object(&envelope_id, &bytes)
            .map_err(|e| e.to_string())?;
        self.store
            .record_semantic(&assertion_id, &envelope_id)
            .map_err(|e| e.to_string())?;
        append_assertion(
            &self.env,
            &subject,
            next_seq,
            assertion_id,
            envelope_id,
            "Set",
            &bytes,
        )
        .map_err(|e| e.to_string())?;
        self.subjects
            .insert(subject, (next_seq, Some(assertion_id)));
        Ok(assertion_id)
    }

    fn start_relay(
        &self,
        port: u16,
        verbose: bool,
        trace: Option<Arc<Mutex<Vec<String>>>>,
        shutdown: Arc<AtomicBool>,
    ) -> thread::JoinHandle<()> {
        let addr = format!("127.0.0.1:{port}");
        let listener = TcpListener::bind(&addr).expect("bind relay");
        listener
            .set_nonblocking(true)
            .expect("set relay listener nonblocking");
        let identity = self.identity.clone();
        let store = self.store.clone();
        let options = server::ServerOptions {
            relay: true,
            verbose,
            trace,
            ..Default::default()
        };
        thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build relay runtime");
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            let _ = runtime.block_on(async {
                server::listen_with_shutdown(listener, identity, store, options, shutdown).await
            });
        })
    }

    fn connect_and_sync(
        &self,
        addr: &str,
        subject_override: Option<SubjectId>,
        trace: Option<Arc<Mutex<Vec<String>>>>,
    ) -> Result<(), String> {
        let identity = self.identity.clone();
        let store = self.store.clone();
        let addr = addr.to_string();
        let (tx, rx) = mpsc::channel();
        if let Some(trace) = &trace {
            if let Ok(mut guard) = trace.lock() {
                guard.push(format!("connect start addr={addr}"));
            }
        }
        let trace_for_thread = trace.clone();
        thread::spawn(move || {
            let result =
                connect_and_sync_once(identity, store, &addr, subject_override, trace_for_thread);
            let _ = tx.send(result);
        });
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(result) => result,
            Err(_) => {
                if let Some(trace) = &trace {
                    if let Ok(mut guard) = trace.lock() {
                        guard.push("sync timeout".to_string());
                    }
                }
                Err("sync timeout".to_string())
            }
        }
    }

    fn list_subjects(&self) -> Result<Vec<SubjectId>, String> {
        self.store.list_subjects().map_err(|e| e.to_string())
    }

    fn list_assertions(&self, subject: SubjectId) -> Result<Vec<AssertionId>, String> {
        self.store.scan_subject(&subject).map_err(|e| e.to_string())
    }

    fn frontiers(&self, subject: SubjectId) -> Result<Vec<AssertionId>, String> {
        let mut keys = Keyring::new();
        keys.insert_sdk(self.identity.subject_id, 0, self.identity.subject_key);
        let index = FrontierIndex::build(&self.store, &keys).map_err(|e| e.to_string())?;
        Ok(index.get_tips(&subject))
    }

    fn pending_log(&self) -> Result<Vec<AssertionId>, String> {
        let pending = pending::read_pending(self.store.env()).map_err(|e| e.to_string())?;
        Ok(pending.keys().copied().collect())
    }
}

fn connect_and_sync_once(
    identity: IdentityState,
    store: Store,
    addr: &str,
    subject_override: Option<SubjectId>,
    trace: Option<Arc<Mutex<Vec<String>>>>,
) -> Result<(), String> {
    let mut stream = TcpStream::connect(addr).map_err(|e| e.to_string())?;
    if let Some(trace) = &trace {
        if let Ok(mut guard) = trace.lock() {
            guard.push("handshake start".to_string());
        }
    }
    let session = client_handshake(&mut stream, &identity).map_err(|e| e.to_string())?;
    if let Some(trace) = &trace {
        if let Ok(mut guard) = trace.lock() {
            guard.push("handshake ok".to_string());
        }
    }
    let mut keys = Keyring::new();
    keys.insert_sdk(identity.subject_id, 0, identity.subject_key);
    let mut index = FrontierIndex::build(&store, &keys).map_err(|e| e.to_string())?;
    let policy = OverlayPolicy::load(store.root());
    let claims = PeerClaims::default();
    let access = OverlayAccess::new(&policy, None, false, &claims);
    let local_subs = subject_override.map(|subject| Subscriptions {
        all: false,
        subjects: vec![subject],
        namespaces: Vec::new(),
    });
    let options = net::sync::SyncOptions {
        relay: false,
        ad_store: None,
        local_subs,
        verbose: false,
        exit_on_idle: true,
        trace,
    };
    net::sync::sync_loop_with(
        &mut stream,
        session,
        &store,
        &mut index,
        &mut keys,
        &identity,
        &access,
        options,
    )
    .map_err(|e| e.to_string())
}

fn prop_relay_baseline(rng: &mut ChaCha20Rng, trace: &mut Vec<String>) -> Result<(), String> {
    let mut relay = TestNode::new("relay", rng)?;
    let node_b = TestNode::new("node-b", rng)?;
    let node_c = TestNode::new("node-c", rng)?;
    let sync_trace = Arc::new(Mutex::new(Vec::new()));
    let relay_trace = Arc::new(Mutex::new(Vec::new()));
    let flush_traces = |trace: &mut Vec<String>| {
        if let Ok(mut guard) = sync_trace.lock() {
            trace.extend(guard.drain(..));
        }
        if let Ok(mut guard) = relay_trace.lock() {
            for line in guard.drain(..) {
                trace.push(format!("relay: {line}"));
            }
        }
    };

    trace.push(format!("node_b_root {}", node_b.root().display()));
    trace.push(format!("node_c_root {}", node_c.root().display()));

    let port = free_port()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let handle = relay.start_relay(port, false, Some(relay_trace.clone()), shutdown.clone());
    let addr = format!("127.0.0.1:{port}");
    trace.push(format!("relay_started addr={addr}"));

    let result = (|| -> Result<(), String> {
        let subject = relay.new_subject(rng);
        trace.push(format!("subject {}", subject.to_hex()));
        relay.write_set(subject, 10)?;
        relay.write_set(subject, 11)?;
        relay.write_set(subject, 12)?;
        trace.push("relay_writes seq=3".to_string());

        node_b.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        node_c.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        trace.push("nodes_synced".to_string());

        assert_converged(&[&relay, &node_b, &node_c], trace)?;

        let subject_b = relay.new_subject(rng);
        relay.write_set(subject_b, 21)?;
        relay.write_set(subject_b, 22)?;
        trace.push(format!("second_subject {}", subject_b.to_hex()));
        node_b.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        node_c.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        assert_converged(&[&relay, &node_b, &node_c], trace)?;

        // Interleaved update
        relay.write_set(subject, 13)?;
        node_b.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        node_c.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        trace.push("nodes_resynced".to_string());
        assert_converged(&[&relay, &node_b, &node_c], trace)?;

        Ok(())
    })();

    flush_traces(trace);
    shutdown.store(true, Ordering::SeqCst);
    let _ = handle.join();
    result
}

fn prop_relay_identity_root(rng: &mut ChaCha20Rng, trace: &mut Vec<String>) -> Result<(), String> {
    let mut relay = TestNode::new("relay", rng)?;
    let node_b = TestNode::new("node-b", rng)?;
    let sync_trace = Arc::new(Mutex::new(Vec::new()));
    let relay_trace = Arc::new(Mutex::new(Vec::new()));
    let flush_traces = |trace: &mut Vec<String>| {
        if let Ok(mut guard) = sync_trace.lock() {
            trace.extend(guard.drain(..));
        }
        if let Ok(mut guard) = relay_trace.lock() {
            for line in guard.drain(..) {
                trace.push(format!("relay: {line}"));
            }
        }
    };

    let port = free_port()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let handle = relay.start_relay(port, false, Some(relay_trace.clone()), shutdown.clone());
    let addr = format!("127.0.0.1:{port}");
    trace.push(format!("relay_started addr={addr}"));

    let result = (|| -> Result<(), String> {
        let subject = relay.new_subject(rng);
        trace.push(format!("subject {}", subject.to_hex()));
        relay.write_set(subject, 42)?;

        node_b.connect_and_sync(&addr, Some(subject), Some(sync_trace.clone()))?;
        trace.push("subject_sync_done".to_string());
        let count = node_b.list_assertions(subject)?.len();
        let pending_after_subject = node_b.pending_log()?;
        trace.push(format!(
            "pending_after_subject_sync {} assertions={}",
            pending_after_subject.len(),
            count
        ));
        if count == 0 && pending_after_subject.is_empty() {
            return Err("subject sync yielded no assertions and no pending".to_string());
        }

        node_b.connect_and_sync(&addr, None, Some(sync_trace.clone()))?;
        trace.push("full_sync_done".to_string());
        let pending_after_full = node_b.pending_log()?;
        let count_after_full = node_b.list_assertions(subject)?.len();
        if count_after_full == 0 {
            trace.push("assertions_after_full_sync=0".to_string());
            return Err("full sync yielded no assertions".to_string());
        }
        if !pending_after_full.is_empty() {
            trace.push(format!(
                "pending_after_full_sync {}",
                pending_after_full.len()
            ));
            return Err("pending objects remain after full sync".to_string());
        }

        assert_converged(&[&relay, &node_b], trace)?;
        Ok(())
    })();

    flush_traces(trace);
    shutdown.store(true, Ordering::SeqCst);
    let _ = handle.join();
    result
}

fn assert_converged(nodes: &[&TestNode], trace: &mut Vec<String>) -> Result<(), String> {
    let Some(first) = nodes.first() else {
        return Ok(());
    };
    let mut baseline_subjects = first.list_subjects()?;
    baseline_subjects.retain(|subject| !is_identity_subject(&first.store, subject));
    let mut baseline_set: BTreeMap<SubjectId, Vec<AssertionId>> = BTreeMap::new();
    for subject in &baseline_subjects {
        let mut ids = first.list_assertions(*subject)?;
        ids.sort();
        baseline_set.insert(*subject, ids);
    }
    for node in nodes.iter().skip(1) {
        let mut subjects = node.list_subjects()?;
        subjects.retain(|subject| !is_identity_subject(&node.store, subject));
        if subjects.len() != baseline_subjects.len() {
            let baseline_list = baseline_subjects
                .iter()
                .map(|s| s.to_hex())
                .collect::<Vec<_>>()
                .join(",");
            let node_list = subjects
                .iter()
                .map(|s| s.to_hex())
                .collect::<Vec<_>>()
                .join(",");
            for missing in baseline_subjects.iter().filter(|s| !subjects.contains(s)) {
                trace.push(format!(
                    "missing_subject={} identity={}",
                    missing.to_hex(),
                    is_identity_subject(&first.store, missing)
                ));
            }
            trace.push(format!(
                "baseline_subjects={} [{}]",
                baseline_subjects.len(),
                baseline_list
            ));
            trace.push(format!("node_subjects={} [{}]", subjects.len(), node_list));
            return Err("subject count mismatch".to_string());
        }
        for subject in &baseline_subjects {
            let mut ids = node.list_assertions(*subject)?;
            ids.sort();
            let baseline = baseline_set.get(subject).cloned().unwrap_or_default();
            if ids != baseline {
                return Err(format!("assertion mismatch for {}", subject.to_hex()));
            }
            let mut tips = node.frontiers(*subject)?;
            tips.sort();
            let mut baseline_tips = first.frontiers(*subject)?;
            baseline_tips.sort();
            if tips != baseline_tips {
                return Err(format!("frontier mismatch for {}", subject.to_hex()));
            }
        }
        let pending = node.pending_log()?;
        if !pending.is_empty() {
            trace.push(format!("pending={}", pending.len()));
            return Err("pending objects present".to_string());
        }
    }
    Ok(())
}

fn is_identity_subject(store: &Store, subject: &SubjectId) -> bool {
    let Ok(records) = list_assertions(store.env(), subject) else {
        return false;
    };
    for record in records {
        let Ok(assertion) = AssertionPlaintext::from_cbor(&record.bytes) else {
            continue;
        };
        if assertion.header.typ == "core.genesis" {
            if let ciborium::value::Value::Map(entries) = &assertion.body {
                for (key, value) in entries {
                    if let ciborium::value::Value::Text(name) = key {
                        if name == "doc_type" {
                            if let ciborium::value::Value::Text(doc) = value {
                                if doc == "identity" {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
        if assertion.header.typ.starts_with("identity.") || assertion.header.typ.starts_with("iam.")
        {
            return true;
        }
    }
    false
}

fn free_port() -> Result<u16, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    Ok(port)
}
