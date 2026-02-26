use crate::cmd::action::{
    apply_action_prepared, load_contract_bytes, load_contract_ids_for_ver, load_schema_bytes,
};
use crate::dhlq;
use blake3;
use ciborium::value::Value;
use dharma::assertion::DEFAULT_DATA_VERSION;
use dharma::config::Config;
use dharma::env::Env;
use dharma::env::StdEnv;
use dharma::pdl::schema::CqrsSchema;
use dharma::store::consistency::{
    compare_configured_backends, validate_migrations_for_backends, CrossBackendReport,
    MigrationBackend, MigrationValidationReport,
};
use dharma::store::pending;
use dharma::store::state;
use dharma::types::{hex_decode, EnvelopeId, SubjectId};
use dharma::value::{expect_bytes, expect_int, expect_map, expect_text, map_get};
use dharma::{DharmaError, Store};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use fs2::available_space;
use std::collections::HashSet;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tar::{Archive, Builder, Header};
use tempfile::tempdir;

pub fn doctor() -> Result<(), DharmaError> {
    let root = std::env::current_dir()?;
    let config = Config::load(&root)?;
    let data_dir = config.storage_path(&root);
    let env = StdEnv::new(&data_dir);
    let store = Store::new(&env);

    let mut failures = Vec::new();
    let mut warnings = Vec::new();

    println!("DHARMA Doctor");

    match check_storage(&data_dir) {
        Ok(msg) => println!("[ok] storage: {msg}"),
        Err(err) => {
            failures.push(format!("storage: {err}"));
            println!("[fail] storage: {err}");
        }
    }

    match check_listen_port(config.network.listen_port) {
        Ok(msg) => println!("[ok] listen: {msg}"),
        Err(err) => {
            failures.push(format!("listen: {err}"));
            println!("[fail] listen: {err}");
        }
    }

    let peers = collect_peers(&data_dir, &config.network.peers);
    let (reachable, total, failed_peers) = check_peers(&peers, config.connect_timeout());
    if total == 0 {
        warnings.push("no peers configured".to_string());
        println!("[warn] peers: none configured");
    } else if failed_peers.is_empty() {
        println!("[ok] peers: {reachable}/{total} reachable");
    } else {
        warnings.push(format!("{} peers unreachable", failed_peers.len()));
        println!(
            "[warn] peers: {reachable}/{total} reachable ({} failed)",
            failed_peers.len()
        );
    }

    match check_logs(&env, &store) {
        Ok(count) => println!("[ok] integrity: {count} subjects checked"),
        Err(err) => {
            failures.push(format!("integrity: {err}"));
            println!("[fail] integrity: {err}");
        }
    }

    match check_clock() {
        Ok(msg) => println!("[ok] clock: {msg}"),
        Err(msg) => {
            warnings.push(msg.clone());
            println!("[warn] clock: {msg}");
        }
    }

    if !warnings.is_empty() {
        println!("Warnings:");
        for warn in &warnings {
            println!("  - {warn}");
        }
    }
    if !failures.is_empty() {
        println!("Failures:");
        for fail in &failures {
            println!("  - {fail}");
        }
        return Err(DharmaError::Validation("doctor found issues".to_string()));
    }
    Ok(())
}

pub fn gc(args: &[&str]) -> Result<(), DharmaError> {
    let mut dry_run = false;
    let mut prune_orphans = true;
    let mut rebuild_dharmaq = true;
    for arg in args {
        match *arg {
            "--dry-run" => dry_run = true,
            "--no-prune" => prune_orphans = false,
            "--no-dharmaq" => rebuild_dharmaq = false,
            _ => {}
        }
    }

    let root = std::env::current_dir()?;
    let config = Config::load(&root)?;
    let data_dir = config.storage_path(&root);
    let env = StdEnv::new(&data_dir);
    let store = Store::new(&env);

    let now = env.now();
    let cutoff = if now < 0 {
        0
    } else {
        (now as u64).saturating_sub(config.storage.prune_pending_hours.saturating_mul(3600))
    };
    let removed = pending::prune_pending(&env, cutoff)?;
    println!("gc: pending entries pruned: {removed}");

    if prune_orphans {
        let (count, bytes) = prune_orphan_objects(&root, &env, &store, dry_run)?;
        if dry_run {
            println!("gc: orphan objects identified: {count} ({} bytes)", bytes);
        } else {
            println!("gc: orphan objects removed: {count} ({} bytes)", bytes);
        }
    } else {
        println!("gc: orphan pruning skipped (--no-prune)");
    }

    if rebuild_dharmaq {
        dharma::dharmaq::rebuild(&data_dir)?;
        println!("gc: dharmaq tables rebuilt");
    } else {
        println!("gc: dharmaq rebuild skipped (--no-dharmaq)");
    }

    Ok(())
}

pub fn reserve_expire(args: &[&str]) -> Result<(), DharmaError> {
    let mut dry_run = false;
    for arg in args {
        if *arg == "--dry-run" {
            dry_run = true;
        }
    }

    let root = std::env::current_dir()?;
    let config = Config::load(&root)?;
    let data_dir = config.storage_path(&root);
    let env = StdEnv::new(&data_dir);
    crate::ensure_identity_present(&env)?;
    let identity = crate::load_identity(&env)?;

    let now = env.now() as i64;
    let expired = expire_reserve_holds(&data_dir, &identity, dry_run, now)?;
    if dry_run {
        println!("reserve: expired holds (dry-run): {expired}");
    } else {
        println!("reserve: expired holds: {expired}");
    }
    Ok(())
}

pub fn backup_export(path: &str) -> Result<(), DharmaError> {
    let root = std::env::current_dir()?;
    let config = Config::load(&root)?;
    let data_dir = config.storage_path(&root);
    let keystore_path = config.keystore_path_for(&root, &data_dir);
    let local_config = root.join("dharma.toml");

    let target = PathBuf::from(path);
    if target.exists() {
        return Err(DharmaError::Validation(format!(
            "backup target already exists: {}",
            target.display()
        )));
    }

    let file = fs::File::create(&target)?;
    let encoder = GzEncoder::new(file, Compression::default());
    let mut builder = Builder::new(encoder);

    builder.append_dir_all("data", &data_dir)?;

    if keystore_path.exists() {
        builder.append_path_with_name(&keystore_path, "keystore/identity.key")?;
    }

    if local_config.exists() {
        builder.append_path_with_name(&local_config, "config/dharma.toml")?;
    }

    let meta = backup_meta(&root, &data_dir, &keystore_path);
    let mut header = Header::new_gnu();
    header.set_size(meta.len() as u64);
    header.set_cksum();
    builder.append_data(&mut header, "meta.txt", meta.as_bytes())?;

    let encoder = builder.into_inner()?;
    encoder.finish()?;
    println!("backup exported to {}", target.display());
    Ok(())
}

pub fn backup_import(path: &str, force: bool) -> Result<(), DharmaError> {
    let root = std::env::current_dir()?;
    let config = Config::load(&root)?;
    let data_dir = config.storage_path(&root);
    let keystore_path = config.keystore_path_for(&root, &data_dir);
    let local_config = root.join("dharma.toml");

    if !force {
        if !dir_is_empty(&data_dir)? {
            return Err(DharmaError::Validation(
                "data directory is not empty (use --force)".to_string(),
            ));
        }
        if keystore_path.exists() {
            return Err(DharmaError::Validation(
                "keystore already exists (use --force)".to_string(),
            ));
        }
        if local_config.exists() {
            return Err(DharmaError::Validation(
                "dharma.toml already exists (use --force)".to_string(),
            ));
        }
    }

    if force {
        if data_dir.exists() {
            fs::remove_dir_all(&data_dir)?;
        }
        if keystore_path.exists() {
            fs::remove_file(&keystore_path)?;
        }
        if local_config.exists() {
            fs::remove_file(&local_config)?;
        }
    }

    let file = fs::File::open(path)?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);

    let temp = tempdir()?;
    for entry in archive.entries()? {
        let mut entry = entry?;
        let entry_path = entry.path()?.to_path_buf();
        if let Some(stripped) = strip_prefix(&entry_path, "data") {
            let target = temp.path().join("data").join(stripped);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            entry.unpack(&target)?;
            continue;
        }
        if let Some(stripped) = strip_prefix(&entry_path, "keystore") {
            let target = temp.path().join("keystore").join(stripped);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            entry.unpack(&target)?;
            continue;
        }
        if let Some(stripped) = strip_prefix(&entry_path, "config") {
            let target = temp.path().join("config").join(stripped);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            entry.unpack(&target)?;
            continue;
        }
    }

    let staged_data = temp.path().join("data");
    if staged_data.exists() {
        copy_dir_all(&staged_data, &data_dir)?;
    }

    let staged_key = temp.path().join("keystore").join("identity.key");
    if staged_key.exists() {
        if let Some(parent) = keystore_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(&staged_key, &keystore_path)?;
    }

    let staged_config = temp.path().join("config").join("dharma.toml");
    if staged_config.exists() {
        fs::copy(&staged_config, &local_config)?;
    }

    println!("backup imported from {}", path);
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
enum MigrationBackendTarget {
    #[default]
    All,
    Sqlite,
    Postgres,
    ClickHouse,
}

const ALL_MIGRATION_BACKENDS: [MigrationBackend; 3] = [
    MigrationBackend::Sqlite,
    MigrationBackend::Postgres,
    MigrationBackend::ClickHouse,
];
const SQLITE_MIGRATION_BACKEND: [MigrationBackend; 1] = [MigrationBackend::Sqlite];
const POSTGRES_MIGRATION_BACKEND: [MigrationBackend; 1] = [MigrationBackend::Postgres];
const CLICKHOUSE_MIGRATION_BACKEND: [MigrationBackend; 1] = [MigrationBackend::ClickHouse];

impl MigrationBackendTarget {
    fn parse(value: &str) -> Result<Self, DharmaError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "all" => Ok(MigrationBackendTarget::All),
            "sqlite" => Ok(MigrationBackendTarget::Sqlite),
            "postgres" | "postgresql" => Ok(MigrationBackendTarget::Postgres),
            "clickhouse" => Ok(MigrationBackendTarget::ClickHouse),
            other => Err(DharmaError::Validation(format!(
                "unknown backend `{other}` (expected sqlite|postgres|clickhouse|all)"
            ))),
        }
    }

    fn backends(&self) -> &'static [MigrationBackend] {
        match self {
            MigrationBackendTarget::All => &ALL_MIGRATION_BACKENDS,
            MigrationBackendTarget::Sqlite => &SQLITE_MIGRATION_BACKEND,
            MigrationBackendTarget::Postgres => &POSTGRES_MIGRATION_BACKEND,
            MigrationBackendTarget::ClickHouse => &CLICKHOUSE_MIGRATION_BACKEND,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
struct MigrateValidateOptions {
    backend: MigrationBackendTarget,
    strict: bool,
    json: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
struct MigrateParityOptions {
    strict: bool,
    json: bool,
}

pub fn migrate_validate(args: &[&str]) -> Result<(), DharmaError> {
    let opts = parse_migrate_validate_options(args)?;
    let root = std::env::current_dir()?;
    let config = Config::load(&root)?;
    let data_dir = config.storage_path(&root);
    let env = StdEnv::new(&data_dir);
    let store = Store::new(&env);

    let report = validate_migrations_for_backends(&root, &config, &store, opts.backend.backends())?;

    if opts.json {
        println!("{}", migration_report_json(&report));
    } else {
        print_migration_report(&report);
    }

    if opts.strict && migration_report_has_failures(&report) {
        return Err(DharmaError::Validation(
            "migration validation failed".to_string(),
        ));
    }
    Ok(())
}

pub fn migrate_parity(args: &[&str]) -> Result<(), DharmaError> {
    let opts = parse_migrate_parity_options(args)?;
    let root = std::env::current_dir()?;
    let config = Config::load(&root)?;
    let data_dir = config.storage_path(&root);
    let env = StdEnv::new(&data_dir);
    let store = Store::new(&env);
    let report = parity_report_from_result(compare_configured_backends(&root, &config, &store));

    if opts.json {
        println!("{}", parity_report_json(&report));
    } else {
        print_parity_report(&report);
    }

    if opts.strict && parity_report_has_failures(&report) {
        return Err(DharmaError::Validation(
            "cross-backend parity check failed".to_string(),
        ));
    }
    Ok(())
}

fn parity_report_from_result(
    result: Result<CrossBackendReport, DharmaError>,
) -> CrossBackendReport {
    match result {
        Ok(report) => report,
        Err(err) => CrossBackendReport {
            snapshots: Vec::new(),
            issues: vec![format!("backend access failed: {err}")],
        },
    }
}

fn parse_migrate_validate_options(args: &[&str]) -> Result<MigrateValidateOptions, DharmaError> {
    let mut opts = MigrateValidateOptions::default();
    let mut idx = 0usize;
    while idx < args.len() {
        match args[idx] {
            "--strict" => opts.strict = true,
            "--json" => opts.json = true,
            "--backend" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    DharmaError::Validation(
                        "missing backend value for --backend (expected sqlite|postgres|clickhouse|all)"
                            .to_string(),
                    )
                })?;
                opts.backend = MigrationBackendTarget::parse(value)?;
            }
            flag if flag.starts_with("--backend=") => {
                let value = flag.split_once('=').map(|(_, value)| value).unwrap_or("");
                opts.backend = MigrationBackendTarget::parse(value)?;
            }
            other => {
                return Err(DharmaError::Validation(format!(
                    "unknown migrate validate flag `{other}`"
                )));
            }
        }
        idx += 1;
    }
    Ok(opts)
}

fn parse_migrate_parity_options(args: &[&str]) -> Result<MigrateParityOptions, DharmaError> {
    let mut opts = MigrateParityOptions::default();
    for arg in args {
        match *arg {
            "--strict" => opts.strict = true,
            "--json" => opts.json = true,
            other => {
                return Err(DharmaError::Validation(format!(
                    "unknown migrate parity flag `{other}`"
                )));
            }
        }
    }
    Ok(opts)
}

fn migration_report_has_failures(report: &MigrationValidationReport) -> bool {
    if !report.issues.is_empty() {
        return true;
    }
    report.validations.iter().any(|v| !v.issues.is_empty())
}

fn parity_report_has_failures(report: &CrossBackendReport) -> bool {
    if !report.issues.is_empty() {
        return true;
    }
    report.snapshots.iter().any(|s| !s.issues.is_empty())
}

fn print_migration_report(report: &MigrationValidationReport) {
    if report.validations.is_empty() {
        println!("migrate validate: no backends selected");
        return;
    }
    for validation in &report.validations {
        let status = if validation.issues.is_empty() {
            "ok"
        } else {
            "fail"
        };
        println!(
            "[{status}] {} subjects={} assertions={} objects={} replay_hash={} frontier_hash={}",
            validation.backend,
            validation.subjects,
            validation.assertions,
            validation.objects,
            validation.replay_hash_hex,
            validation.frontier_hash_hex
        );
        for issue in &validation.issues {
            println!("  - {issue}");
        }
    }
}

fn print_parity_report(report: &CrossBackendReport) {
    for snapshot in &report.snapshots {
        let status = if snapshot.issues.is_empty() {
            "ok"
        } else {
            "fail"
        };
        println!(
            "[{status}] {} subjects={} assertions={} objects={} replay_hash={} frontier_hash={}",
            snapshot.backend,
            snapshot.subjects,
            snapshot.assertions,
            snapshot.objects,
            snapshot.replay_hash_hex,
            snapshot.frontier_hash_hex
        );
        for issue in &snapshot.issues {
            println!("  - {issue}");
        }
    }
    if !report.issues.is_empty() {
        println!("cross-backend issues:");
        for issue in &report.issues {
            println!("  - {issue}");
        }
    }
}

fn migration_report_json(report: &MigrationValidationReport) -> String {
    let mut out = String::from("{\"validations\":[");
    for (idx, validation) in report.validations.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "{{\"backend\":\"{}\",\"subjects\":{},\"assertions\":{},\"objects\":{},\"replay_hash_hex\":\"{}\",\"frontier_hash_hex\":\"{}\",\"issues\":[",
            json_escape(&validation.backend),
            validation.subjects,
            validation.assertions,
            validation.objects,
            json_escape(&validation.replay_hash_hex),
            json_escape(&validation.frontier_hash_hex),
        ));
        for (issue_idx, issue) in validation.issues.iter().enumerate() {
            if issue_idx > 0 {
                out.push(',');
            }
            out.push('"');
            out.push_str(&json_escape(issue));
            out.push('"');
        }
        out.push_str("]}");
    }
    out.push_str("],\"issues\":[");
    for (idx, issue) in report.issues.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push('"');
        out.push_str(&json_escape(issue));
        out.push('"');
    }
    out.push_str("]}");
    out
}

fn parity_report_json(report: &CrossBackendReport) -> String {
    let mut out = String::from("{\"snapshots\":[");
    for (idx, snapshot) in report.snapshots.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "{{\"backend\":\"{}\",\"subjects\":{},\"assertions\":{},\"objects\":{},\"replay_hash_hex\":\"{}\",\"frontier_hash_hex\":\"{}\",\"issues\":[",
            json_escape(&snapshot.backend),
            snapshot.subjects,
            snapshot.assertions,
            snapshot.objects,
            json_escape(&snapshot.replay_hash_hex),
            json_escape(&snapshot.frontier_hash_hex),
        ));
        for (issue_idx, issue) in snapshot.issues.iter().enumerate() {
            if issue_idx > 0 {
                out.push(',');
            }
            out.push('"');
            out.push_str(&json_escape(issue));
            out.push('"');
        }
        out.push_str("]}");
    }
    out.push_str("],\"issues\":[");
    for (idx, issue) in report.issues.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push('"');
        out.push_str(&json_escape(issue));
        out.push('"');
    }
    out.push_str("]}");
    out
}

fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out
}

fn backup_meta(root: &Path, data_dir: &Path, keystore: &Path) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!(
        "created_at={}\nroot={}\ndata_dir={}\nkeystore={}\nversion={}\n",
        now,
        root.display(),
        data_dir.display(),
        keystore.display(),
        crate::APP_VERSION,
    )
}

fn strip_prefix(path: &Path, prefix: &str) -> Option<PathBuf> {
    let mut components = path.components();
    let first = components.next()?;
    if first.as_os_str() != prefix {
        return None;
    }
    let mut clean = PathBuf::new();
    for component in components {
        match component {
            std::path::Component::Normal(part) => clean.push(part),
            _ => return None,
        }
    }
    Some(clean)
}

fn dir_is_empty(path: &Path) -> Result<bool, DharmaError> {
    if !path.exists() {
        return Ok(true);
    }
    let mut entries = fs::read_dir(path)?;
    Ok(entries.next().is_none())
}

fn copy_dir_all(src: &Path, dst: &Path) -> Result<(), DharmaError> {
    if !dst.exists() {
        fs::create_dir_all(dst)?;
    }
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if path.is_dir() {
            copy_dir_all(&path, &target)?;
        } else {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&path, &target)?;
        }
    }
    Ok(())
}

fn collect_peers(root: &Path, configured: &[String]) -> Vec<String> {
    let mut peers: Vec<String> = configured.iter().map(|addr| normalize_addr(addr)).collect();
    let path = root.join("peers.list");
    if let Ok(contents) = fs::read_to_string(&path) {
        for line in contents.lines() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let addr = line.split_whitespace().next().unwrap_or("");
            if !addr.is_empty() {
                peers.push(normalize_addr(addr));
            }
        }
    }
    peers.sort();
    peers.dedup();
    peers
}

fn normalize_addr(addr: &str) -> String {
    if let Some((_, rest)) = addr.split_once("://") {
        return rest.to_string();
    }
    addr.to_string()
}

fn check_storage(data_dir: &Path) -> Result<String, DharmaError> {
    if !data_dir.exists() {
        return Err(DharmaError::Validation(format!(
            "storage path missing: {}",
            data_dir.display()
        )));
    }
    let free = available_space(data_dir)?;
    let test_path = data_dir.join(".doctor_write_test");
    let test = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&test_path);
    match test {
        Ok(_) => {
            let _ = fs::remove_file(&test_path);
        }
        Err(err) => {
            return Err(DharmaError::Validation(format!(
                "storage not writable: {err}"
            )));
        }
    }
    Ok(format!("{} free", human_bytes(free)))
}

fn check_listen_port(port: u16) -> Result<String, String> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    match TcpListener::bind(addr) {
        Ok(listener) => {
            drop(listener);
            Ok(format!("port {port} available"))
        }
        Err(err) => match TcpStream::connect_timeout(&addr, Duration::from_secs(1)) {
            Ok(_) => Ok(format!("port {port} in use (server reachable)")),
            Err(_) => Err(format!("cannot bind or connect to port {port}: {err}")),
        },
    }
}

fn check_peers(peers: &[String], timeout: Duration) -> (usize, usize, Vec<String>) {
    let mut ok = 0usize;
    let mut failed = Vec::new();
    for addr in peers {
        match connect_with_timeout(addr, timeout) {
            Ok(_) => ok += 1,
            Err(_) => failed.push(addr.clone()),
        }
    }
    (ok, peers.len(), failed)
}

fn connect_with_timeout(addr: &str, timeout: Duration) -> io::Result<TcpStream> {
    let addrs: Vec<_> = addr.to_socket_addrs()?.collect();
    let mut last_err = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(stream) => return Ok(stream),
            Err(err) => last_err = Some(err),
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "connect failed")))
}

fn check_logs(env: &StdEnv, store: &Store) -> Result<usize, DharmaError> {
    let mut count = 0usize;
    for subject in store.list_subjects()? {
        state::read_assertion_log(env, &subject)?;
        state::read_overlay_log(env, &subject)?;
        count += 1;
    }
    Ok(count)
}

fn check_clock() -> Result<String, String> {
    let a = SystemTime::now();
    std::thread::sleep(Duration::from_millis(5));
    let b = SystemTime::now();
    if b < a {
        return Err("clock moved backwards".to_string());
    }
    Ok("monotonic (NTP not verified)".to_string())
}

fn prune_orphan_objects(
    root: &Path,
    env: &StdEnv,
    store: &Store,
    dry_run: bool,
) -> Result<(usize, u64), DharmaError> {
    let keep = collect_kept_envelopes(root, env, store)?;
    let mut removed = 0usize;
    let mut bytes = 0u64;
    for id in store.list_objects()? {
        if keep.contains(&id) {
            continue;
        }
        let path = store.objects_dir().join(format!("{}.obj", id.to_hex()));
        if let Ok(meta) = fs::metadata(&path) {
            bytes += meta.len();
        }
        if !dry_run {
            let _ = fs::remove_file(&path);
        }
        removed += 1;
    }
    Ok((removed, bytes))
}

fn collect_kept_envelopes(
    root: &Path,
    env: &StdEnv,
    store: &Store,
) -> Result<HashSet<EnvelopeId>, DharmaError> {
    let mut keep = HashSet::new();
    for subject in store.list_subjects()? {
        for entry in state::read_assertion_log(env, &subject)? {
            keep.insert(entry.envelope_id);
        }
        for entry in state::read_overlay_log(env, &subject)? {
            keep.insert(entry.envelope_id);
        }
    }
    for id in load_config_envelopes(root)? {
        keep.insert(id);
    }
    Ok(keep)
}

fn load_config_envelopes(root: &Path) -> Result<Vec<EnvelopeId>, DharmaError> {
    let path = root.join("dharma.toml");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let contents = fs::read_to_string(&path)?;
    let mut ids = Vec::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        if !(key.starts_with("schema") || key.starts_with("contract") || key.starts_with("reactor"))
        {
            continue;
        }
        let value = v.trim().trim_matches('"');
        if value.len() != 64 {
            continue;
        }
        let bytes = match hex_decode(value) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let id = match EnvelopeId::from_slice(&bytes) {
            Ok(id) => id,
            Err(_) => continue,
        };
        ids.push(id);
    }
    Ok(ids)
}

fn human_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

pub(crate) fn expire_reserve_holds(
    data_dir: &Path,
    identity: &dharma::IdentityState,
    dry_run: bool,
    now: i64,
) -> Result<usize, DharmaError> {
    let hold_query = r#"
std.commerce.inventory.ledger
| where entry_type == 'ReserveHold, expires_at != null, expires_at <= $1
| sel subject as hold_id,
      batch_id as batch_id,
      item_id as item_id,
      warehouse_id as warehouse_id,
      qty as qty,
      ref_line_id as ref_line_id,
      ref_event_id as ref_event_id
"#;
    let plan = dhlq::parse_plan(hold_query, 1)?;
    let params = Value::Array(vec![Value::Integer(now.into())]);
    let rows = dharma::dhlq::execute(data_dir, &plan, &params)?;

    let release_query = r#"
std.commerce.inventory.ledger
| where ref_line_id != null, ref_event_id != null,
        (entry_type == 'Release or entry_type == 'ExpireHold)
| sel ref_line_id as line_id, ref_event_id as event_id
"#;
    let release_plan = dhlq::parse_plan(release_query, 1)?;
    let release_rows = dharma::dhlq::execute(data_dir, &release_plan, &Value::Array(Vec::new()))?;
    let mut released: HashSet<(SubjectId, String)> = HashSet::new();
    for row in release_rows {
        let map = expect_map(&row)?;
        let Some(line_val) = map_get(map, "line_id") else {
            continue;
        };
        let Some(event_val) = map_get(map, "event_id") else {
            continue;
        };
        if matches!(line_val, Value::Null) || matches!(event_val, Value::Null) {
            continue;
        }
        let line_id = subject_from_value(line_val)?;
        let event_id = expect_text(event_val)?;
        released.insert((line_id, event_id));
    }

    let data_dir = data_dir.to_path_buf();
    let (schema_id, contract_id) = load_contract_ids_for_ver(&data_dir, DEFAULT_DATA_VERSION)?;
    let schema_bytes = load_schema_bytes(&data_dir, &schema_id)?;
    let contract_bytes = load_contract_bytes(&data_dir, &contract_id)?;
    let schema = CqrsSchema::from_cbor(&schema_bytes)?;

    let env = StdEnv::new(&data_dir);
    let store = Store::new(&env);
    let mut expired = 0usize;

    for row in rows {
        let map = expect_map(&row)?;
        let Some(hold_val) = map_get(map, "hold_id") else {
            continue;
        };
        let hold_id = subject_from_value(hold_val)?;

        let line_id = map_get(map, "ref_line_id")
            .filter(|v| !matches!(v, Value::Null))
            .map(subject_from_value)
            .transpose()?;
        let event_id = map_get(map, "ref_event_id")
            .filter(|v| !matches!(v, Value::Null))
            .map(expect_text)
            .transpose()?;
        if let (Some(line_id), Some(event_id)) = (&line_id, &event_id) {
            if released.contains(&(*line_id, event_id.clone())) {
                continue;
            }
        }

        let expire_subject = proj_id_subject(&[
            Value::Text("expire_hold".to_string()),
            Value::Bytes(hold_id.as_bytes().to_vec()),
        ])?;
        if store.subject_dir(&expire_subject).exists() {
            continue;
        }

        let batch_id = map_get(map, "batch_id").cloned().unwrap_or(Value::Null);
        let item_id = map_get(map, "item_id").cloned().unwrap_or(Value::Null);
        let warehouse_id = map_get(map, "warehouse_id").cloned().unwrap_or(Value::Null);
        let qty_val = map_get(map, "qty")
            .cloned()
            .unwrap_or(Value::Integer(0.into()));
        let qty = expect_int(&qty_val)?;
        let neg_qty = Value::Integer((-qty).into());
        let ref_line_val = line_id
            .map(|id| Value::Bytes(id.as_bytes().to_vec()))
            .unwrap_or(Value::Null);
        let ref_event_val = event_id.map(Value::Text).unwrap_or(Value::Null);

        if !dry_run {
            let args = Value::Map(vec![
                (Value::Text("batch_id".to_string()), batch_id),
                (Value::Text("item_id".to_string()), item_id),
                (Value::Text("warehouse_id".to_string()), warehouse_id),
                (
                    Value::Text("entry_type".to_string()),
                    Value::Text("ExpireHold".to_string()),
                ),
                (Value::Text("qty".to_string()), neg_qty),
                (Value::Text("expires_at".to_string()), Value::Null),
                (Value::Text("ref_line_id".to_string()), ref_line_val),
                (Value::Text("ref_event_id".to_string()), ref_event_val),
                (
                    Value::Text("reason".to_string()),
                    Value::Text("reserve_hold_expired".to_string()),
                ),
            ]);
            let _ = apply_action_prepared(
                &data_dir,
                identity,
                expire_subject,
                "Record",
                args,
                DEFAULT_DATA_VERSION,
                schema_id,
                contract_id,
                &schema,
                &contract_bytes,
                None,
            )?;
        }
        expired += 1;
    }

    Ok(expired)
}

fn proj_id_subject(items: &[Value]) -> Result<SubjectId, DharmaError> {
    let list = Value::Array(items.to_vec());
    let bytes = dharma::cbor::encode_canonical_value(&list)?;
    let hash = *blake3::hash(&bytes).as_bytes();
    Ok(SubjectId::from_bytes(hash))
}

fn subject_from_value(value: &Value) -> Result<SubjectId, DharmaError> {
    let bytes = expect_bytes(value)?;
    SubjectId::from_slice(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::action::{
        apply_action_prepared, load_contract_bytes, load_contract_ids_for_ver, load_schema_bytes,
    };
    use crate::cmd::project_runtime;
    use crate::compile_dhl;
    use dharma::assertion::{AssertionHeader, AssertionPlaintext, DEFAULT_DATA_VERSION};
    use dharma::crypto;
    use dharma::identity_store;
    use dharma::pdl::schema::CqrsSchema;
    use dharma::store::state::append_assertion;
    use dharma::types::{ContractId, SchemaId, SubjectId};
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::collections::HashMap;
    use std::sync::{Mutex, MutexGuard};
    use tempfile::TempDir;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    struct TempCtx {
        _guard: MutexGuard<'static, ()>,
        _dir: TempDir,
        prev_cwd: std::path::PathBuf,
        prev_home: Option<std::ffi::OsString>,
        prev_config: Option<std::ffi::OsString>,
    }

    impl Drop for TempCtx {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.prev_cwd);
            match &self.prev_home {
                Some(val) => std::env::set_var("HOME", val),
                None => std::env::remove_var("HOME"),
            }
            match &self.prev_config {
                Some(val) => std::env::set_var("DHARMA_CONFIG_PATH", val),
                None => std::env::remove_var("DHARMA_CONFIG_PATH"),
            }
        }
    }

    fn setup_temp_project() -> Result<(TempCtx, dharma::IdentityState), DharmaError> {
        let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new()?;
        let prev_cwd = std::env::current_dir()?;
        let prev_home = std::env::var_os("HOME");
        let prev_config = std::env::var_os("DHARMA_CONFIG_PATH");

        std::env::set_current_dir(dir.path())?;
        std::env::set_var("HOME", dir.path());
        let config_path = dir.path().join(".dharma").join("config.toml");
        std::env::set_var("DHARMA_CONFIG_PATH", &config_path);

        let local_config = r#"[storage]
path = "data"

[identity]
keystore_path = "keystore"
"#;
        std::fs::write(dir.path().join("dharma.toml"), local_config)?;

        let root = dir.path().to_path_buf();
        let config = Config::load(&root)?;
        let data_dir = config.storage_path(&root);
        std::fs::create_dir_all(&data_dir)?;
        let env = StdEnv::new(&data_dir);
        let passphrase = "test-pass";
        let _ = identity_store::init_identity(&env, "tester", passphrase)?;
        let identity = identity_store::load_identity(&env, passphrase)?;

        Ok((
            TempCtx {
                _guard: guard,
                _dir: dir,
                prev_cwd,
                prev_home,
                prev_config,
            },
            identity,
        ))
    }

    fn copy_contract(
        root: &std::path::Path,
        filename: &str,
    ) -> Result<std::path::PathBuf, DharmaError> {
        let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let source = repo_root.join("contracts").join("std").join(filename);
        let target = root.join("contracts").join("std").join(filename);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let contents = std::fs::read_to_string(&source)?;
        std::fs::write(&target, contents)?;
        Ok(target)
    }

    #[test]
    fn migrate_validate_parses_backend_and_flags() {
        let opts = parse_migrate_validate_options(&["--backend", "postgres", "--strict", "--json"])
            .unwrap();
        assert_eq!(opts.backend, MigrationBackendTarget::Postgres);
        assert!(opts.strict);
        assert!(opts.json);
    }

    #[test]
    fn migrate_validate_rejects_unknown_backend() {
        let err = parse_migrate_validate_options(&["--backend", "mysql"]).unwrap_err();
        match err {
            DharmaError::Validation(msg) => {
                assert!(msg.contains("unknown backend"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn migrate_validate_target_maps_to_requested_backends() {
        assert_eq!(
            MigrationBackendTarget::Sqlite.backends(),
            &[MigrationBackend::Sqlite]
        );
        assert_eq!(
            MigrationBackendTarget::Postgres.backends(),
            &[MigrationBackend::Postgres]
        );
        assert_eq!(
            MigrationBackendTarget::ClickHouse.backends(),
            &[MigrationBackend::ClickHouse]
        );
        assert_eq!(
            MigrationBackendTarget::All.backends(),
            &[
                MigrationBackend::Sqlite,
                MigrationBackend::Postgres,
                MigrationBackend::ClickHouse
            ]
        );
    }

    #[test]
    fn migrate_strict_failure_detection_uses_report_issues() {
        let parity = CrossBackendReport {
            snapshots: Vec::new(),
            issues: vec!["subjects mismatch".to_string()],
        };
        assert!(parity_report_has_failures(&parity));

        let migration = MigrationValidationReport {
            validations: vec![],
            issues: vec!["sqlite: missing table `objects`".to_string()],
        };
        assert!(migration_report_has_failures(&migration));
    }

    #[test]
    fn parity_report_from_result_wraps_error_as_issue() {
        let report = parity_report_from_result(Err(DharmaError::Validation(
            "connection refused".to_string(),
        )));
        assert!(report.snapshots.is_empty());
        assert_eq!(report.issues.len(), 1);
        assert!(report.issues[0].contains("backend access failed"));
        assert!(report.issues[0].contains("connection refused"));
    }

    #[test]
    fn parity_report_json_escapes_quote_characters_once() {
        let report = CrossBackendReport {
            snapshots: Vec::new(),
            issues: vec!["backend said: \"bad\"".to_string()],
        };
        assert_eq!(
            parity_report_json(&report),
            "{\"snapshots\":[],\"issues\":[\"backend said: \\\"bad\\\"\"]}"
        );
    }

    #[test]
    fn reserve_expire_creates_expire_hold_entry() -> Result<(), DharmaError> {
        let (_ctx, identity) = setup_temp_project()?;
        let ledger_path =
            copy_contract(&std::env::current_dir()?, "commerce_inventory_ledger.dhl")?;
        compile_dhl(ledger_path.to_str().unwrap(), None)?;
        let data_dir = crate::ensure_data_dir()?;
        let env = StdEnv::new(&data_dir);

        let mut rng = StdRng::seed_from_u64(42);
        let hold_subject = SubjectId::random(&mut rng);
        let item_id = SubjectId::random(&mut rng);
        let warehouse_id = SubjectId::random(&mut rng);
        let batch_id = SubjectId::random(&mut rng);
        let line_id = SubjectId::random(&mut rng);
        let allocation_id = "alloc-1".to_string();

        let (schema_id, contract_id) = load_contract_ids_for_ver(&data_dir, DEFAULT_DATA_VERSION)?;
        let schema_bytes = load_schema_bytes(&data_dir, &schema_id)?;
        let contract_bytes = load_contract_bytes(&data_dir, &contract_id)?;
        let schema = CqrsSchema::from_cbor(&schema_bytes)?;
        if let Some(field) = schema.fields.get("lines") {
            println!("lines field type: {:?}", field.typ);
        }
        println!(
            "credit_note struct keys: {:?}",
            schema.structs.keys().cloned().collect::<Vec<_>>()
        );
        if let Some(def) = schema.structs.get("CreditLine") {
            println!(
                "CreditLine field keys: {:?}",
                def.fields.keys().cloned().collect::<Vec<_>>()
            );
        }
        println!(
            "credit_note structs: {:?}",
            schema.structs.keys().cloned().collect::<Vec<_>>()
        );
        if let Some(action) = schema.actions.get("IssueCreditNoteForLine") {
            println!("credit_note action args: {:?}", action.args);
        }
        if let Some(def) = schema.structs.get("CreditLine") {
            println!(
                "CreditLine fields: {:?}",
                def.fields.keys().cloned().collect::<Vec<_>>()
            );
        }
        if let Some(action) = schema.actions.get("IssueCreditNoteForLine") {
            println!("schema action args: {:?}", action.args);
        }
        println!(
            "schema fields: {:?}",
            schema.fields.keys().cloned().collect::<Vec<_>>()
        );

        let expires_at = env.now() as i64 - 60;
        let args = Value::Map(vec![
            (
                Value::Text("batch_id".to_string()),
                Value::Bytes(batch_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("item_id".to_string()),
                Value::Bytes(item_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("warehouse_id".to_string()),
                Value::Bytes(warehouse_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("entry_type".to_string()),
                Value::Text("ReserveHold".to_string()),
            ),
            (
                Value::Text("qty".to_string()),
                Value::Integer(1_000_000i64.into()),
            ),
            (
                Value::Text("expires_at".to_string()),
                Value::Integer(expires_at.into()),
            ),
            (
                Value::Text("ref_line_id".to_string()),
                Value::Bytes(line_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("ref_event_id".to_string()),
                Value::Text(allocation_id.clone()),
            ),
            (
                Value::Text("reason".to_string()),
                Value::Text("reserve_hold_accepted".to_string()),
            ),
        ]);
        let _ = apply_action_prepared(
            &data_dir,
            &identity,
            hold_subject,
            "Record",
            args,
            DEFAULT_DATA_VERSION,
            schema_id,
            contract_id,
            &schema,
            &contract_bytes,
            None,
        )?;

        let expired = expire_reserve_holds(&data_dir, &identity, false, env.now() as i64)?;
        assert_eq!(expired, 1);

        let query = r#"
std.commerce.inventory.ledger
| where entry_type == 'ExpireHold, ref_event_id == $1
| sel subject as subject, qty as qty
"#;
        let plan = dhlq::parse_plan(query, 1)?;
        let rows = dharma::dhlq::execute(
            &data_dir,
            &plan,
            &Value::Array(vec![Value::Text(allocation_id)]),
        )?;
        assert_eq!(rows.len(), 1);
        let map = expect_map(&rows[0])?;
        let qty = expect_int(map_get(map, "qty").unwrap())?;
        assert_eq!(qty, -1_000_000);
        Ok(())
    }

    #[test]
    fn refund_payment_sets_final_state() -> Result<(), DharmaError> {
        let (_ctx, identity) = setup_temp_project()?;
        let payment_path = copy_contract(&std::env::current_dir()?, "commerce_payment.dhl")?;
        compile_dhl(payment_path.to_str().unwrap(), None)?;
        let data_dir = crate::ensure_data_dir()?;

        let mut rng = StdRng::seed_from_u64(7);
        let payment_subject = SubjectId::random(&mut rng);

        let (schema_id, contract_id) = load_contract_ids_for_ver(&data_dir, DEFAULT_DATA_VERSION)?;
        let schema_bytes = load_schema_bytes(&data_dir, &schema_id)?;
        let contract_bytes = load_contract_bytes(&data_dir, &contract_id)?;
        let schema = CqrsSchema::from_cbor(&schema_bytes)?;

        let auth_args = Value::Map(vec![
            (
                Value::Text("provider".to_string()),
                Value::Text("test".to_string()),
            ),
            (
                Value::Text("method".to_string()),
                Value::Text("card".to_string()),
            ),
            (
                Value::Text("currency".to_string()),
                Value::Text("EUR".to_string()),
            ),
            (
                Value::Text("authorized_amount_minor".to_string()),
                Value::Integer(1000i64.into()),
            ),
            (Value::Text("external_id".to_string()), Value::Null),
            (Value::Text("human_ref".to_string()), Value::Null),
        ]);
        let _ = apply_action_prepared(
            &data_dir,
            &identity,
            payment_subject,
            "AuthorizePayment",
            auth_args,
            DEFAULT_DATA_VERSION,
            schema_id,
            contract_id,
            &schema,
            &contract_bytes,
            None,
        )?;
        let capture_args = Value::Map(vec![(
            Value::Text("captured_amount_minor".to_string()),
            Value::Integer(1000i64.into()),
        )]);
        let _ = apply_action_prepared(
            &data_dir,
            &identity,
            payment_subject,
            "CapturePayment",
            capture_args,
            DEFAULT_DATA_VERSION,
            schema_id,
            contract_id,
            &schema,
            &contract_bytes,
            None,
        )?;
        let refund_args = Value::Map(vec![
            (
                Value::Text("refund_amount_minor".to_string()),
                Value::Integer(1000i64.into()),
            ),
            (
                Value::Text("final_state".to_string()),
                Value::Text("Refunded".to_string()),
            ),
        ]);
        let _ = apply_action_prepared(
            &data_dir,
            &identity,
            payment_subject,
            "RefundPayment",
            refund_args,
            DEFAULT_DATA_VERSION,
            schema_id,
            contract_id,
            &schema,
            &contract_bytes,
            None,
        )?;

        let query = r#"
std.commerce.payment
| where subject == $1
| sel state as state, refunded_amount_minor as refunded
| take 1
"#;
        let plan = dhlq::parse_plan(query, 1)?;
        let rows = dharma::dhlq::execute(
            &data_dir,
            &plan,
            &Value::Array(vec![Value::Bytes(payment_subject.as_bytes().to_vec())]),
        )?;
        assert_eq!(rows.len(), 1);
        let map = expect_map(&rows[0])?;
        let state = expect_text(map_get(map, "state").unwrap())?;
        let refunded = expect_int(map_get(map, "refunded").unwrap())?;
        assert_eq!(state, "Refunded");
        assert_eq!(refunded, 1000);
        Ok(())
    }

    #[test]
    fn issue_credit_note_for_line_populates_line() -> Result<(), DharmaError> {
        let (_ctx, identity) = setup_temp_project()?;
        let credit_path = copy_contract(&std::env::current_dir()?, "commerce_credit_note.dhl")?;
        let contents = std::fs::read_to_string(&credit_path)?;
        let ast = crate::pdl::parser::parse(&contents)?;
        if let Some(action) = ast
            .actions
            .iter()
            .find(|a| a.name == "IssueCreditNoteForLine")
        {
            for apply in &action.applies {
                if apply.value.target == vec!["state".to_string(), "lines".to_string()] {
                    if let crate::pdl::ast::Expr::Call(name, args) = &apply.value.value {
                        println!("apply call: {} args: {:?}", name, args);
                    }
                }
            }
        }
        compile_dhl(credit_path.to_str().unwrap(), None)?;
        let data_dir = crate::ensure_data_dir()?;

        let mut rng = StdRng::seed_from_u64(9);
        let credit_subject = SubjectId::random(&mut rng);
        let invoice_id = SubjectId::random(&mut rng);
        let line_id = SubjectId::random(&mut rng);

        let (schema_id, contract_id) = load_contract_ids_for_ver(&data_dir, DEFAULT_DATA_VERSION)?;
        let schema_bytes = load_schema_bytes(&data_dir, &schema_id)?;
        let contract_bytes = load_contract_bytes(&data_dir, &contract_id)?;
        let schema = CqrsSchema::from_cbor(&schema_bytes)?;

        let args = Value::Map(vec![
            (
                Value::Text("invoice_id".to_string()),
                Value::Bytes(invoice_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("currency".to_string()),
                Value::Text("EUR".to_string()),
            ),
            (
                Value::Text("credit_line_id".to_string()),
                Value::Text("line-1".to_string()),
            ),
            (
                Value::Text("line_id".to_string()),
                Value::Bytes(line_id.as_bytes().to_vec()),
            ),
            (Value::Text("fulfillment_id".to_string()), Value::Null),
            (
                Value::Text("description".to_string()),
                Value::Text("cancel".to_string()),
            ),
            (
                Value::Text("qty".to_string()),
                Value::Integer(2_000_000i64.into()),
            ),
            (
                Value::Text("unit_price_minor".to_string()),
                Value::Integer(500i64.into()),
            ),
            (Value::Text("external_id".to_string()), Value::Null),
            (Value::Text("human_ref".to_string()), Value::Null),
        ]);
        let action_schema = schema
            .action("IssueCreditNoteForLine")
            .ok_or_else(|| DharmaError::Schema("unknown action".to_string()))?;
        let action_idx = dharma::runtime::cqrs::action_index(&schema, "IssueCreditNoteForLine")?;
        let args_buffer = dharma::runtime::cqrs::encode_args_buffer(
            action_schema,
            &schema.structs,
            action_idx,
            &args,
            false,
        )?;
        let decoded_args = dharma::runtime::cqrs::decode_args_buffer(
            action_schema,
            &schema.structs,
            &args_buffer,
        )?;
        println!("decoded args: {:?}", decoded_args);
        let _ = apply_action_prepared(
            &data_dir,
            &identity,
            credit_subject,
            "IssueCreditNoteForLine",
            args,
            DEFAULT_DATA_VERSION,
            schema_id,
            contract_id,
            &schema,
            &contract_bytes,
            None,
        )?;
        println!("applied IssueCreditNoteForLine");

        let query = r#"
std.commerce.credit_note
| where subject == $1
| explode lines as line
| sel line as line
| take 1
"#;
        let plan = dhlq::parse_plan(query, 1)?;
        let rows = dharma::dhlq::execute(
            &data_dir,
            &plan,
            &Value::Array(vec![Value::Bytes(credit_subject.as_bytes().to_vec())]),
        )?;
        assert_eq!(rows.len(), 1);
        let map = expect_map(&rows[0])?;
        let line_val = map_get(map, "line").unwrap().clone();
        let line_map = expect_map(&line_val)?;
        let credit_line_id = expect_text(map_get(line_map, "credit_line_id").unwrap())?;
        let qty = expect_int(map_get(line_map, "qty").unwrap())?;
        let gross = expect_int(map_get(line_map, "gross_amount_minor").unwrap())?;
        println!("line_map: {:?}", line_map);
        assert_eq!(credit_line_id, "line-1");
        assert_eq!(qty, 2_000_000);
        assert_eq!(gross, 1000);
        Ok(())
    }

    #[test]
    fn variant_availability_status_transitions() -> Result<(), DharmaError> {
        let (_ctx, identity) = setup_temp_project()?;
        let item_bucket_path = copy_contract(
            &std::env::current_dir()?,
            "commerce_availability_item_bucket.dhl",
        )?;
        compile_dhl(item_bucket_path.to_str().unwrap(), None)?;
        let data_dir = crate::ensure_data_dir()?;

        let mut rng = StdRng::seed_from_u64(11);
        let bucket_subject = SubjectId::random(&mut rng);
        let item_id = SubjectId::random(&mut rng);
        let warehouse_id = SubjectId::random(&mut rng);

        let (schema_id, contract_id) = load_contract_ids_for_ver(&data_dir, DEFAULT_DATA_VERSION)?;
        let schema_bytes = load_schema_bytes(&data_dir, &schema_id)?;
        let contract_bytes = load_contract_bytes(&data_dir, &contract_id)?;
        let schema = CqrsSchema::from_cbor(&schema_bytes)?;

        let apply_upsert = |on_hand: i64, backorder: i64, preorder: i64, preorder_allowed: bool| {
            let args = Value::Map(vec![
                (
                    Value::Text("item_id".to_string()),
                    Value::Bytes(item_id.as_bytes().to_vec()),
                ),
                (
                    Value::Text("warehouse_id".to_string()),
                    Value::Bytes(warehouse_id.as_bytes().to_vec()),
                ),
                (Value::Text("channel_id".to_string()), Value::Null),
                (Value::Text("delivery_area".to_string()), Value::Null),
                (Value::Text("bucket_date".to_string()), Value::Null),
                (
                    Value::Text("on_hand_qty".to_string()),
                    Value::Integer(on_hand.into()),
                ),
                (
                    Value::Text("inbound_committed_qty".to_string()),
                    Value::Integer(0i64.into()),
                ),
                (
                    Value::Text("reserved_qty".to_string()),
                    Value::Integer(0i64.into()),
                ),
                (
                    Value::Text("available_on_hand_qty".to_string()),
                    Value::Integer(on_hand.into()),
                ),
                (
                    Value::Text("available_backorder_qty".to_string()),
                    Value::Integer(backorder.into()),
                ),
                (
                    Value::Text("available_preorder_qty".to_string()),
                    Value::Integer(preorder.into()),
                ),
                (
                    Value::Text("preorder_allowed".to_string()),
                    Value::Bool(preorder_allowed),
                ),
                (
                    Value::Text("preorder_capacity".to_string()),
                    Value::Integer(5_000_000i64.into()),
                ),
                (
                    Value::Text("preorder_unverified".to_string()),
                    Value::Bool(false),
                ),
                (
                    Value::Text("shelf_life_status".to_string()),
                    Value::Text("Unknown".to_string()),
                ),
                (
                    Value::Text("shelf_life_unverified".to_string()),
                    Value::Bool(true),
                ),
                (Value::Text("blocked_reason".to_string()), Value::Null),
                (Value::Text("as_of".to_string()), Value::Null),
            ]);
            apply_action_prepared(
                &data_dir,
                &identity,
                bucket_subject,
                "Upsert",
                args,
                DEFAULT_DATA_VERSION,
                schema_id,
                contract_id,
                &schema,
                &contract_bytes,
                None,
            )
        };

        let aggregate = || -> Result<(i64, i64, i64, bool), DharmaError> {
            let query = r#"
std.commerce.availability.item_bucket
| by item_id, delivery_area, bucket_date
| agg sum(available_backorder_qty) as available_backorder_qty, sum(available_on_hand_qty) as available_on_hand_qty, sum(available_preorder_qty) as available_preorder_qty, max(preorder_allowed) as preorder_allowed
| take 1
"#;
            let plan = dhlq::parse_plan(query, 0)?;
            let rows = dharma::dhlq::execute(&data_dir, &plan, &Value::Array(vec![]))?;
            let map = expect_map(&rows[0])?;
            let on_hand = expect_int(map_get(map, "available_on_hand_qty").unwrap())?;
            let backorder = expect_int(map_get(map, "available_backorder_qty").unwrap())?;
            let preorder = expect_int(map_get(map, "available_preorder_qty").unwrap())?;
            let preorder_allowed =
                dharma::value::expect_bool(map_get(map, "preorder_allowed").unwrap())?;
            Ok((on_hand, backorder, preorder, preorder_allowed))
        };

        let status_from = |on_hand: i64, backorder: i64, preorder: i64, preorder_allowed: bool| {
            if on_hand > 0 {
                "InStock"
            } else if backorder > 0 {
                "Backorder"
            } else if preorder_allowed && preorder > 0 {
                "Preorder"
            } else {
                "OutOfStock"
            }
        };

        apply_upsert(1_000_000, 1_000_000, 1_000_000, false)?;
        let (on_hand, backorder, preorder, preorder_allowed) = aggregate()?;
        assert_eq!(
            status_from(on_hand, backorder, preorder, preorder_allowed),
            "InStock"
        );

        apply_upsert(0, 2_000_000, 2_000_000, false)?;
        let (on_hand, backorder, preorder, preorder_allowed) = aggregate()?;
        assert_eq!(
            status_from(on_hand, backorder, preorder, preorder_allowed),
            "Backorder"
        );

        apply_upsert(0, 0, 3_000_000, true)?;
        let (on_hand, backorder, preorder, preorder_allowed) = aggregate()?;
        assert_eq!(
            status_from(on_hand, backorder, preorder, preorder_allowed),
            "Preorder"
        );

        apply_upsert(0, 0, 0, false)?;
        let (on_hand, backorder, preorder, preorder_allowed) = aggregate()?;
        assert_eq!(
            status_from(on_hand, backorder, preorder, preorder_allowed),
            "OutOfStock"
        );

        Ok(())
    }

    #[derive(Clone)]
    struct ContractArtifacts {
        schema_id: SchemaId,
        contract_id: ContractId,
        schema: CqrsSchema,
        contract_bytes: Vec<u8>,
    }

    #[derive(Clone)]
    struct CommerceFixture {
        data_dir: PathBuf,
        identity: dharma::IdentityState,
        category_id: String,
        variant_id: SubjectId,
        line_id: SubjectId,
        invoice_id: SubjectId,
        catalog_product: ContractArtifacts,
    }

    fn vmap(entries: Vec<(&str, Value)>) -> Value {
        Value::Map(
            entries
                .into_iter()
                .map(|(k, v)| (Value::Text(k.to_string()), v))
                .collect(),
        )
    }

    fn grant_projection_writer_role(
        data_dir: &Path,
        identity: &dharma::IdentityState,
    ) -> Result<(), DharmaError> {
        let env = StdEnv::new(data_dir);
        let _ = crate::mount_self(&env, identity)?;
        let records = state::list_assertions(&env, &identity.subject_id)?;
        let prev = records.last().map(|record| record.assertion_id);
        let seq = records.last().map(|record| record.seq + 1).unwrap_or(1);

        let header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: DEFAULT_DATA_VERSION,
            sub: identity.subject_id,
            typ: "identity.profile".to_string(),
            auth: identity.public_key,
            seq,
            prev,
            refs: prev.into_iter().collect(),
            ts: None,
            schema: SchemaId::from_bytes([9u8; 32]),
            contract: ContractId::from_bytes([7u8; 32]),
            note: None,
            meta: None,
        };
        let body = vmap(vec![(
            "roles",
            Value::Array(vec![Value::Text("projection.writer".to_string())]),
        )]);
        let assertion = AssertionPlaintext::sign(header, body, &identity.signing_key)?;
        let bytes = assertion.to_cbor()?;
        let assertion_id = assertion.assertion_id()?;
        let envelope_id = crypto::envelope_id(&bytes);
        append_assertion(
            &env,
            &identity.subject_id,
            seq,
            assertion_id,
            envelope_id,
            "identity.profile",
            &bytes,
        )?;
        Ok(())
    }

    fn compile_contract_artifacts(
        root: &Path,
        data_dir: &PathBuf,
        cache: &mut HashMap<String, ContractArtifacts>,
        filename: &str,
    ) -> Result<ContractArtifacts, DharmaError> {
        if let Some(found) = cache.get(filename) {
            return Ok(found.clone());
        }
        let path = copy_contract(root, filename)?;
        compile_dhl(path.to_str().unwrap(), None)?;
        let (schema_id, contract_id) = load_contract_ids_for_ver(data_dir, DEFAULT_DATA_VERSION)?;
        let schema_bytes = load_schema_bytes(data_dir, &schema_id)?;
        let contract_bytes = load_contract_bytes(data_dir, &contract_id)?;
        let schema = CqrsSchema::from_cbor(&schema_bytes)?;
        let artifacts = ContractArtifacts {
            schema_id,
            contract_id,
            schema,
            contract_bytes,
        };
        cache.insert(filename.to_string(), artifacts.clone());
        Ok(artifacts)
    }

    fn write_fixture_order_po_contract(root: &Path) -> Result<PathBuf, DharmaError> {
        let path = root
            .join("contracts")
            .join("std")
            .join("commerce_order_po.dhl");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = r#"---
namespace: std.commerce.order.po
version: 1.0.0
---

## Code

```dhl
aggregate PurchaseOrder
    state
        public customer_id: Ref<std.iam.Identity>
        public status: Enum(Draft, Posted) = 'Draft
        public created_at: Timestamp?

action Create(customer_id: Ref<std.iam.Identity>, status: Enum(Draft, Posted))
    validate
        state.created_at == null
    apply
        state.customer_id = customer_id
        state.status = status
        state.created_at = context.timestamp
```
"#;
        std::fs::write(&path, body)?;
        Ok(path)
    }

    fn write_fixture_catalog_product_contract(root: &Path) -> Result<PathBuf, DharmaError> {
        let path = root
            .join("contracts")
            .join("std")
            .join("commerce_catalog_product.dhl");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = r#"---
namespace: std.commerce.catalog.product
version: 1.0.0
---

## Code

```dhl
aggregate Product
    state
        public category_id: Text(len=64)?
        public taxonomy: Map<Text(len=64), Text(len=256)>
        public status: Enum(Draft, Published, Archived) = 'Draft
        public created_at: Timestamp?

action Create(category_id: Text(len=64)?, taxonomy: Map<Text(len=64), Text(len=256)>)
    validate
        state.created_at == null
    apply
        state.category_id = category_id
        state.taxonomy = taxonomy
        state.status = 'Draft
        state.created_at = context.timestamp

action Publish()
    validate
        state.status != 'Archived
    apply
        state.status = 'Published
```
"#;
        std::fs::write(&path, body)?;
        Ok(path)
    }

    fn write_fixture_order_line_contract(root: &Path) -> Result<PathBuf, DharmaError> {
        let path = root
            .join("contracts")
            .join("std")
            .join("commerce_order_line.dhl");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = r#"---
namespace: std.commerce.order.line
version: 1.0.0
---

## Code

```dhl
struct DeliveryWindow
    public start: Timestamp?
    public end: Timestamp?

aggregate Line
    state
        public po_id: Ref<std.commerce.order.po>
        public remaining_qty: Decimal(scale=6)
        public cancelled: Bool = false
        public requested_delivery_window: DeliveryWindow?
        public splits: List<Text(len=64)>
        public created_at: Timestamp?

action CreateFromSnapshot(
    po_id: Ref<std.commerce.order.po>,
    remaining_qty: Decimal(scale=6),
    requested_delivery_window: DeliveryWindow?
)
    validate
        state.created_at == null
    apply
        state.po_id = po_id
        state.remaining_qty = remaining_qty
        state.cancelled = false
        state.requested_delivery_window = requested_delivery_window
        state.created_at = context.timestamp

action SplitLine(split_ref: Text(len=64))
    validate
        state.created_at != null
    apply
        state.splits.push(split_ref)
```
"#;
        std::fs::write(&path, body)?;
        Ok(path)
    }

    fn write_fixture_catalog_variant_contract(root: &Path) -> Result<PathBuf, DharmaError> {
        let path = root
            .join("contracts")
            .join("std")
            .join("commerce_catalog_variant.dhl");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = r#"---
namespace: std.commerce.catalog.variant
version: 1.0.0
---

## Code

```dhl
aggregate Variant
    state
        public item_id: Ref<std.commerce.catalog.item>
        public status: Enum(Active, Archived) = 'Active
        public created_at: Timestamp?

action Create(item_id: Ref<std.commerce.catalog.item>)
    validate
        state.created_at == null
    apply
        state.item_id = item_id
        state.status = 'Active
        state.created_at = context.timestamp
```
"#;
        std::fs::write(&path, body)?;
        Ok(path)
    }

    fn write_fixture_logistics_batch_contract(root: &Path) -> Result<PathBuf, DharmaError> {
        let path = root
            .join("contracts")
            .join("std")
            .join("commerce_logistics_batch.dhl");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = r#"---
namespace: std.commerce.logistics.batch
version: 1.0.0
---

## Code

```dhl
struct BatchLineAttachment
    public line_id: Ref<std.commerce.order.line>
    public qty_planned: Decimal(scale=6)

aggregate LogisticsBatch
    state
        public attachments: List<BatchLineAttachment>
        public scheduled_for: Timestamp?
        public batch_state: Enum(Planning, Scheduled, InTransit, Delivered, Cancelled) = 'Planning
        public route_id: Text(len=64)?
        public route_sequence: Int?
        public created_at: Timestamp?

action CreateLogisticsBatch(scheduled_for: Timestamp?)
    validate
        state.created_at == null
    apply
        state.scheduled_for = scheduled_for
        state.batch_state = 'Planning
        state.created_at = context.timestamp

action AttachLineToBatch(line_id: Ref<std.commerce.order.line>, qty_planned: Decimal(scale=6))
    validate
        qty_planned >= 0
    apply
        state.attachments.push(BatchLineAttachment { line_id: line_id, qty_planned: qty_planned })

action AssignRoute(route_id: Text(len=64), sequence: Int?)
    apply
        state.route_id = route_id
        state.route_sequence = sequence
```
"#;
        std::fs::write(&path, body)?;
        Ok(path)
    }

    fn write_fixture_invoice_contract(root: &Path) -> Result<PathBuf, DharmaError> {
        let path = root
            .join("contracts")
            .join("std")
            .join("commerce_invoice.dhl");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = r#"---
namespace: std.commerce.invoice
version: 1.0.0
---

## Code

```dhl
struct InvoiceLine
    public invoice_line_id: Text(len=64)
    public line_id: Ref<std.commerce.order.line>
    public fulfillment_id: Text(len=64)
    public description: Text(len=256)
    public qty: Decimal(scale=6)
    public unit_price_minor: Int
    public tax_amount_minor: Int
    public net_amount_minor: Int
    public gross_amount_minor: Int

aggregate Invoice
    state
        public po_id: Ref<std.commerce.order.po>
        public issued_at: Timestamp?
        public posted_at: Timestamp?
        public state: Enum(Draft, Posted, Void) = 'Draft
        public lines: List<InvoiceLine>

action IssueInvoiceFromBatch(po_id: Ref<std.commerce.order.po>, lines: List<InvoiceLine>)
    validate
        state.issued_at == null
    apply
        state.po_id = po_id
        state.lines = lines
        state.issued_at = context.timestamp
        state.state = 'Draft

action PostInvoice()
    validate
        state.state == 'Draft
    apply
        state.state = 'Posted
        state.posted_at = context.timestamp
```
"#;
        std::fs::write(&path, body)?;
        Ok(path)
    }

    fn write_fixture_credit_note_contract(root: &Path) -> Result<PathBuf, DharmaError> {
        let path = root
            .join("contracts")
            .join("std")
            .join("commerce_credit_note.dhl");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = r#"---
namespace: std.commerce.credit_note
version: 1.0.0
---

## Code

```dhl
struct CreditLine
    public credit_line_id: Text(len=64)
    public invoice_line_id: Text(len=64)
    public line_id: Ref<std.commerce.order.line>
    public fulfillment_id: Text(len=64)
    public description: Text(len=256)
    public qty: Decimal(scale=6)
    public unit_price_minor: Int
    public tax_amount_minor: Int
    public net_amount_minor: Int
    public gross_amount_minor: Int

aggregate CreditNote
    state
        public invoice_id: Ref<std.commerce.invoice>
        public issued_at: Timestamp?
        public posted_at: Timestamp?
        public state: Enum(Draft, Posted, Void) = 'Draft
        public lines: List<CreditLine>

action IssueCreditNoteForLine(
    invoice_id: Ref<std.commerce.invoice>,
    credit_line_id: Text(len=64),
    line_id: Ref<std.commerce.order.line>,
    fulfillment_id: Text(len=64),
    description: Text(len=256),
    qty: Decimal(scale=6),
    unit_price_minor: Int,
    net_amount_minor: Int,
    gross_amount_minor: Int
)
    validate
        state.issued_at == null
    apply
        state.invoice_id = invoice_id
        state.lines.push(CreditLine { credit_line_id: credit_line_id, invoice_line_id: "inv-line", line_id: line_id, fulfillment_id: fulfillment_id, description: description, qty: qty, unit_price_minor: unit_price_minor, tax_amount_minor: 0, net_amount_minor: net_amount_minor, gross_amount_minor: gross_amount_minor })
        state.issued_at = context.timestamp
        state.state = 'Draft

action PostCreditNote()
    validate
        state.state == 'Draft
    apply
        state.state = 'Posted
        state.posted_at = context.timestamp
```
"#;
        std::fs::write(&path, body)?;
        Ok(path)
    }

    fn compile_written_contract(
        data_dir: &PathBuf,
        path: &Path,
    ) -> Result<ContractArtifacts, DharmaError> {
        compile_dhl(path.to_str().unwrap(), None)?;
        let (schema_id, contract_id) = load_contract_ids_for_ver(data_dir, DEFAULT_DATA_VERSION)?;
        let schema_bytes = load_schema_bytes(data_dir, &schema_id)?;
        let contract_bytes = load_contract_bytes(data_dir, &contract_id)?;
        Ok(ContractArtifacts {
            schema_id,
            contract_id,
            schema: CqrsSchema::from_cbor(&schema_bytes)?,
            contract_bytes,
        })
    }

    fn apply_contract_action(
        data_dir: &PathBuf,
        identity: &dharma::IdentityState,
        contract: &ContractArtifacts,
        subject: SubjectId,
        action: &str,
        args: Value,
    ) -> Result<(), DharmaError> {
        let _ = apply_action_prepared(
            data_dir,
            identity,
            subject,
            action,
            args,
            DEFAULT_DATA_VERSION,
            contract.schema_id,
            contract.contract_id,
            &contract.schema,
            &contract.contract_bytes,
            None,
        )?;
        Ok(())
    }

    fn run_query(
        data_dir: &PathBuf,
        query: &str,
        params: Value,
    ) -> Result<Vec<Value>, DharmaError> {
        let plan = dhlq::parse_plan(query, 1)?;
        dharma::dhlq::execute(data_dir, &plan, &params)
    }

    fn run_query_from_contract_source(
        data_dir: &PathBuf,
        contract_filename: &str,
        query_name: &str,
        params: Value,
    ) -> Result<Vec<Value>, DharmaError> {
        let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let contract_path = repo_root
            .join("contracts")
            .join("std")
            .join(contract_filename);
        let contents = std::fs::read_to_string(&contract_path)?;
        let ast = crate::pdl::parser::parse(&contents)?;
        let query = ast
            .queries
            .iter()
            .find(|q| q.name == query_name)
            .ok_or_else(|| {
                DharmaError::Validation(format!(
                    "query '{}' not found in {}",
                    query_name,
                    contract_path.display()
                ))
            })?;
        let query_text = query.body.join("\n");
        if query_text.trim().is_empty() {
            return Err(DharmaError::Validation(format!(
                "query '{}' has empty body in {}",
                query_name,
                contract_path.display()
            )));
        }
        let plan = dhlq::parse_plan(&query_text, query.start_line)?;
        dharma::dhlq::execute(data_dir, &plan, &params)
    }

    fn table_count(data_dir: &PathBuf, table: &str) -> Result<i64, DharmaError> {
        let query = format!("{table}\n| agg count()");
        let rows = run_query(data_dir, &query, Value::Array(vec![]))?;
        if rows.is_empty() {
            return Ok(0);
        }
        let row = rows
            .first()
            .ok_or_else(|| DharmaError::Validation("missing count row".to_string()))?;
        let map = expect_map(row)?;
        let count = map_get(map, "count")
            .ok_or_else(|| DharmaError::Validation("missing count".to_string()))?;
        expect_int(count)
    }

    fn build_commerce_fixture() -> Result<(TempCtx, CommerceFixture), DharmaError> {
        let (ctx, identity) = setup_temp_project()?;
        std::env::set_var("DHARMA_PASSPHRASE", "test-pass");
        let root = std::env::current_dir()?;
        let data_dir = crate::ensure_data_dir()?;
        grant_projection_writer_role(&data_dir, &identity)?;

        let mut cache: HashMap<String, ContractArtifacts> = HashMap::new();
        let po_contract =
            compile_written_contract(&data_dir, &write_fixture_order_po_contract(&root)?)?;
        let product_contract =
            compile_written_contract(&data_dir, &write_fixture_catalog_product_contract(&root)?)?;
        let variant_contract =
            compile_written_contract(&data_dir, &write_fixture_catalog_variant_contract(&root)?)?;
        let batch_contract =
            compile_written_contract(&data_dir, &write_fixture_logistics_batch_contract(&root)?)?;
        let invoice_contract =
            compile_written_contract(&data_dir, &write_fixture_invoice_contract(&root)?)?;
        let credit_note_contract =
            compile_written_contract(&data_dir, &write_fixture_credit_note_contract(&root)?)?;
        let line_contract =
            compile_written_contract(&data_dir, &write_fixture_order_line_contract(&root)?)?;
        let bucket_contract = compile_contract_artifacts(
            &root,
            &data_dir,
            &mut cache,
            "commerce_availability_item_bucket.dhl",
        )?;
        for filename in [
            "commerce_catalog_product_facet.dhl",
            "commerce_catalog_variant_availability.dhl",
            "commerce_order_line_stats.dhl",
            "commerce_order_po_action_queue.dhl",
            "commerce_logistics_batch_line.dhl",
            "commerce_logistics_batch_route.dhl",
            "commerce_invoice_line.dhl",
            "commerce_credit_note_line.dhl",
            "commerce_inventory_supplier.dhl",
            "commerce_logistics_warehouse.dhl",
        ] {
            let _ = compile_contract_artifacts(&root, &data_dir, &mut cache, filename)?;
        }

        let mut rng = StdRng::seed_from_u64(66_300_001);
        let po_id = SubjectId::random(&mut rng);
        let warehouse_id = SubjectId::random(&mut rng);
        let product_id = SubjectId::random(&mut rng);
        let variant_id = SubjectId::random(&mut rng);
        let item_id = SubjectId::random(&mut rng);
        let bucket_id = SubjectId::random(&mut rng);
        let line_id = SubjectId::random(&mut rng);
        let batch_id = SubjectId::random(&mut rng);
        let invoice_id = SubjectId::random(&mut rng);
        let credit_note_id = SubjectId::random(&mut rng);
        let category_id = "cat-1".to_string();
        let t0 = 1_700_000_000i64;

        apply_contract_action(
            &data_dir,
            &identity,
            &po_contract,
            po_id,
            "Create",
            vmap(vec![
                (
                    "customer_id",
                    Value::Bytes(identity.subject_id.as_bytes().to_vec()),
                ),
                ("status", Value::Text("Draft".to_string())),
            ]),
        )?;
        apply_contract_action(
            &data_dir,
            &identity,
            &product_contract,
            product_id,
            "Create",
            vmap(vec![
                ("category_id", Value::Text(category_id.clone())),
                (
                    "taxonomy",
                    Value::Map(vec![(
                        Value::Text("origin".to_string()),
                        Value::Text("FR".to_string()),
                    )]),
                ),
            ]),
        )?;
        apply_contract_action(
            &data_dir,
            &identity,
            &product_contract,
            product_id,
            "Publish",
            Value::Map(vec![]),
        )?;

        apply_contract_action(
            &data_dir,
            &identity,
            &variant_contract,
            variant_id,
            "Create",
            vmap(vec![("item_id", Value::Bytes(item_id.as_bytes().to_vec()))]),
        )?;

        apply_contract_action(
            &data_dir,
            &identity,
            &bucket_contract,
            bucket_id,
            "Upsert",
            vmap(vec![
                ("item_id", Value::Bytes(item_id.as_bytes().to_vec())),
                (
                    "warehouse_id",
                    Value::Bytes(warehouse_id.as_bytes().to_vec()),
                ),
                ("channel_id", Value::Null),
                ("delivery_area", Value::Null),
                ("bucket_date", Value::Null),
                ("on_hand_qty", Value::Integer(2_000_000.into())),
                ("inbound_committed_qty", Value::Integer(0.into())),
                ("reserved_qty", Value::Integer(0.into())),
                ("available_on_hand_qty", Value::Integer(2_000_000.into())),
                ("available_backorder_qty", Value::Integer(0.into())),
                ("available_preorder_qty", Value::Integer(0.into())),
                ("preorder_allowed", Value::Bool(false)),
                ("preorder_capacity", Value::Integer(0.into())),
                ("preorder_unverified", Value::Bool(false)),
                ("shelf_life_status", Value::Text("Ok".to_string())),
                ("shelf_life_unverified", Value::Bool(false)),
                ("blocked_reason", Value::Null),
                ("as_of", Value::Null),
            ]),
        )?;

        apply_contract_action(
            &data_dir,
            &identity,
            &line_contract,
            line_id,
            "CreateFromSnapshot",
            vmap(vec![
                ("po_id", Value::Bytes(po_id.as_bytes().to_vec())),
                ("remaining_qty", Value::Integer(2_000_000.into())),
                (
                    "requested_delivery_window",
                    vmap(vec![
                        ("start", Value::Integer((t0 + 3_600).into())),
                        ("end", Value::Integer((t0 + 7_200).into())),
                    ]),
                ),
            ]),
        )?;

        apply_contract_action(
            &data_dir,
            &identity,
            &batch_contract,
            batch_id,
            "CreateLogisticsBatch",
            vmap(vec![("scheduled_for", Value::Integer((t0 + 5_400).into()))]),
        )?;
        apply_contract_action(
            &data_dir,
            &identity,
            &batch_contract,
            batch_id,
            "AttachLineToBatch",
            vmap(vec![
                ("line_id", Value::Bytes(line_id.as_bytes().to_vec())),
                ("qty_planned", Value::Integer(2_000_000.into())),
            ]),
        )?;
        apply_contract_action(
            &data_dir,
            &identity,
            &batch_contract,
            batch_id,
            "AssignRoute",
            vmap(vec![
                ("route_id", Value::Text("route-1".to_string())),
                ("sequence", Value::Integer(1.into())),
            ]),
        )?;

        apply_contract_action(
            &data_dir,
            &identity,
            &invoice_contract,
            invoice_id,
            "IssueInvoiceFromBatch",
            vmap(vec![
                ("po_id", Value::Bytes(po_id.as_bytes().to_vec())),
                (
                    "lines",
                    Value::Array(vec![vmap(vec![
                        ("invoice_line_id", Value::Text("inv-line-1".to_string())),
                        ("line_id", Value::Bytes(line_id.as_bytes().to_vec())),
                        ("fulfillment_id", Value::Text("ful-1".to_string())),
                        ("description", Value::Text("line item".to_string())),
                        ("qty", Value::Integer(2_000_000.into())),
                        ("unit_price_minor", Value::Integer(1200.into())),
                        ("tax_amount_minor", Value::Integer(0.into())),
                        ("net_amount_minor", Value::Integer(2400.into())),
                        ("gross_amount_minor", Value::Integer(2400.into())),
                    ])]),
                ),
            ]),
        )?;
        apply_contract_action(
            &data_dir,
            &identity,
            &invoice_contract,
            invoice_id,
            "PostInvoice",
            Value::Map(vec![]),
        )?;

        apply_contract_action(
            &data_dir,
            &identity,
            &credit_note_contract,
            credit_note_id,
            "IssueCreditNoteForLine",
            vmap(vec![
                ("invoice_id", Value::Bytes(invoice_id.as_bytes().to_vec())),
                ("credit_line_id", Value::Text("cn-line-1".to_string())),
                ("line_id", Value::Bytes(line_id.as_bytes().to_vec())),
                ("fulfillment_id", Value::Text("ful-1".to_string())),
                ("description", Value::Text("return".to_string())),
                ("qty", Value::Integer(1_000_000.into())),
                ("unit_price_minor", Value::Integer(1200.into())),
                ("net_amount_minor", Value::Integer(1200.into())),
                ("gross_amount_minor", Value::Integer(1200.into())),
            ]),
        )?;
        apply_contract_action(
            &data_dir,
            &identity,
            &credit_note_contract,
            credit_note_id,
            "PostCreditNote",
            Value::Map(vec![]),
        )?;

        Ok((
            ctx,
            CommerceFixture {
                data_dir,
                identity: identity.clone(),
                category_id,
                variant_id,
                line_id,
                invoice_id,
                catalog_product: product_contract,
            },
        ))
    }

    #[test]
    fn project_rebuild_populates_commerce_projections() -> Result<(), DharmaError> {
        let (_ctx, fixture) = build_commerce_fixture()?;
        let tables = [
            "std.commerce.catalog.product_facet",
            "std.commerce.catalog.variant_availability",
            "std.commerce.order.line_stats",
            "std.commerce.order.po_action_queue",
            "std.commerce.logistics.batch_line",
            "std.commerce.logistics.batch_route",
            "std.commerce.invoice_line",
            "std.commerce.credit_note_line",
        ];

        for table in tables.iter().copied() {
            let before = table_count(&fixture.data_dir, table)?;
            assert_eq!(
                before, 0,
                "expected empty projection table before rebuild: {table}"
            );
        }

        project_runtime::rebuild("std.commerce")?;

        for table in tables.iter().copied() {
            let after = table_count(&fixture.data_dir, table)?;
            assert!(
                after > 0,
                "expected rebuild writes for projection table: {table}"
            );
        }

        Ok(())
    }

    #[test]
    fn ecommerce_key_queries_return_expected_rows() -> Result<(), DharmaError> {
        let (_ctx, fixture) = build_commerce_fixture()?;
        project_runtime::rebuild("std.commerce")?;

        let query_from = 0i64;
        let query_to = 4_102_444_800i64;

        let facets = run_query_from_contract_source(
            &fixture.data_dir,
            "commerce_catalog_product.dhl",
            "GetProductFacets",
            Value::Array(vec![Value::Text(fixture.category_id.clone()), Value::Null]),
        )?;
        assert_eq!(facets.len(), 1);
        let facet_row = expect_map(&facets[0])?;
        assert_eq!(
            map_get(facet_row, "facet_value")
                .ok_or_else(|| DharmaError::Validation("missing facet_value".to_string()))?,
            &Value::Text("FR".to_string())
        );

        let availability = run_query_from_contract_source(
            &fixture.data_dir,
            "commerce_catalog_variant.dhl",
            "GetVariantAvailabilityHint",
            Value::Array(vec![
                Value::Bytes(fixture.variant_id.as_bytes().to_vec()),
                Value::Integer(1.into()),
                Value::Null,
                Value::Null,
            ]),
        )?;
        assert_eq!(availability.len(), 1);
        let availability_row = expect_map(&availability[0])?;
        assert_eq!(
            map_get(availability_row, "variant_id")
                .ok_or_else(|| DharmaError::Validation("missing variant_id".to_string()))?,
            &Value::Bytes(fixture.variant_id.as_bytes().to_vec())
        );

        let lines = run_query_from_contract_source(
            &fixture.data_dir,
            "commerce_order_line.dhl",
            "LinesNeedingAllocation",
            Value::Array(vec![
                Value::Integer(query_from.into()),
                Value::Integer(query_to.into()),
                Value::Integer(0.into()),
                Value::Integer(10.into()),
            ]),
        )?;
        let mut fixture_line_found = false;
        for row in &lines {
            let line_subject = map_get(expect_map(row)?, "subject")
                .ok_or_else(|| DharmaError::Validation("missing line subject".to_string()))?;
            if expect_bytes(line_subject)? == fixture.line_id.as_bytes().to_vec() {
                fixture_line_found = true;
                break;
            }
        }
        assert!(
            fixture_line_found,
            "expected LinesNeedingAllocation to include fixture line {:?}",
            fixture.line_id
        );

        let invoices = run_query_from_contract_source(
            &fixture.data_dir,
            "commerce_invoice.dhl",
            "ListMyInvoices",
            Value::Array(vec![
                Value::Bytes(fixture.identity.subject_id.as_bytes().to_vec()),
                Value::Integer(query_from.into()),
                Value::Integer(query_to.into()),
                Value::Integer(0.into()),
                Value::Integer(10.into()),
            ]),
        )?;
        assert_eq!(invoices.len(), 1);
        let invoice_subject = map_get(expect_map(&invoices[0])?, "subject")
            .ok_or_else(|| DharmaError::Validation("missing invoice subject".to_string()))?;
        assert_eq!(
            expect_bytes(invoice_subject)?,
            fixture.invoice_id.as_bytes().to_vec()
        );

        let credits = run_query_from_contract_source(
            &fixture.data_dir,
            "commerce_credit_note.dhl",
            "ReturnsAndCreditsSummary",
            Value::Array(vec![
                Value::Integer(query_from.into()),
                Value::Integer(query_to.into()),
            ]),
        )?;
        assert_eq!(credits.len(), 1);
        let total = expect_int(
            map_get(expect_map(&credits[0])?, "sum")
                .ok_or_else(|| DharmaError::Validation("missing sum".to_string()))?,
        )?;
        assert_eq!(total, 1200);

        Ok(())
    }

    #[test]
    fn project_watch_applies_incremental_update() -> Result<(), DharmaError> {
        let (_ctx, fixture) = build_commerce_fixture()?;
        project_runtime::rebuild("std.commerce")?;

        let count_rows = run_query(
            &fixture.data_dir,
            "std.commerce.catalog.product_facet\n| agg count()",
            Value::Array(vec![]),
        )?;
        let before = expect_int(
            map_get(expect_map(&count_rows[0])?, "count")
                .ok_or_else(|| DharmaError::Validation("missing count".to_string()))?,
        )?;
        let before_seq_rows = run_query(
            &fixture.data_dir,
            "std.commerce.catalog.product_facet\n| agg max(seq) as max_seq",
            Value::Array(vec![]),
        )?;
        let before_seq = expect_int(
            map_get(expect_map(&before_seq_rows[0])?, "max_seq")
                .ok_or_else(|| DharmaError::Validation("missing max_seq".to_string()))?,
        )?;

        let mut rng = StdRng::seed_from_u64(66_300_002);
        let product2 = SubjectId::random(&mut rng);
        apply_contract_action(
            &fixture.data_dir,
            &fixture.identity,
            &fixture.catalog_product,
            product2,
            "Create",
            vmap(vec![
                ("category_id", Value::Text(fixture.category_id.clone())),
                (
                    "taxonomy",
                    Value::Map(vec![(
                        Value::Text("origin".to_string()),
                        Value::Text("ES".to_string()),
                    )]),
                ),
            ]),
        )?;
        apply_contract_action(
            &fixture.data_dir,
            &fixture.identity,
            &fixture.catalog_product,
            product2,
            "Publish",
            Value::Map(vec![]),
        )?;

        project_runtime::watch_with_limit("std.commerce", Duration::from_millis(5), Some(1))?;

        let count_rows = run_query(
            &fixture.data_dir,
            "std.commerce.catalog.product_facet\n| agg count()",
            Value::Array(vec![]),
        )?;
        let after = expect_int(
            map_get(expect_map(&count_rows[0])?, "count")
                .ok_or_else(|| DharmaError::Validation("missing count".to_string()))?,
        )?;
        assert!(after >= before);

        let after_seq_rows = run_query(
            &fixture.data_dir,
            "std.commerce.catalog.product_facet\n| agg max(seq) as max_seq",
            Value::Array(vec![]),
        )?;
        let after_seq = expect_int(
            map_get(expect_map(&after_seq_rows[0])?, "max_seq")
                .ok_or_else(|| DharmaError::Validation("missing max_seq".to_string()))?,
        )?;
        assert!(after_seq > before_seq);

        Ok(())
    }
}
