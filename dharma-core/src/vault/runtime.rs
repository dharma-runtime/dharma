use crate::backup::{backup_policy_status, BackupPolicyStatus};
use crate::cbor;
use crate::config::{Config, VaultArchiveMode};
use crate::error::DharmaError;
use crate::identity::IdentityState;
use crate::store::Store;
use crate::types::{ContractId, SchemaId, SubjectId};
use crate::value::{expect_bytes, expect_map, expect_uint, map_get};
use crate::vault::drivers::LocalDriver;
use crate::vault::drivers::PeerDriver;
use crate::vault::{
    archive_subject, VaultArchiveInput, VaultConfig, VaultDictionaryRef, VaultDriver,
};
use ciborium::value::Value;
use fs2::{available_space, total_space};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::warn;

#[cfg(feature = "vault-s3")]
use crate::vault::drivers::s3::S3Options;
#[cfg(feature = "vault-arweave")]
use crate::vault::drivers::ArweaveDriver;
#[cfg(feature = "vault-s3")]
use crate::vault::drivers::S3Driver;

pub enum VaultArchiveOutcome {
    Deferred,
    Skipped,
    Archived(usize),
    Alert(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VaultArchiveJob {
    pub subject: SubjectId,
    pub ver: u64,
    pub schema_id: SchemaId,
    pub contract_id: ContractId,
}

pub fn enqueue_archive_job(root: &Path, job: VaultArchiveJob) -> Result<(), DharmaError> {
    let path = vault_queue_path(root);
    let mut queue = VaultArchiveQueue::load(&path);
    if !queue.jobs.iter().any(|existing| *existing == job) {
        queue.jobs.push(job);
        queue.save(&path)?;
    }
    Ok(())
}

pub fn drain_archive_queue(
    store: &Store,
    config: &Config,
    identity: &IdentityState,
) -> Result<usize, DharmaError> {
    let path = vault_queue_path(store.root());
    let queue = VaultArchiveQueue::load(&path);
    if queue.jobs.is_empty() {
        return Ok(0);
    }
    let mut remaining = Vec::new();
    let mut processed = 0usize;
    for job in queue.jobs {
        processed = processed.saturating_add(1);
        if job.subject != identity.subject_id {
            warn!(
                subject_id = %job.subject.to_hex(),
                "vault archive skipping unsupported subject"
            );
            continue;
        }
        match maybe_archive_subject_with_config(
            store,
            config,
            identity,
            job.subject,
            identity.subject_key,
            job.ver,
            job.schema_id,
            job.contract_id,
        ) {
            Ok(VaultArchiveOutcome::Archived(_)) => {}
            Ok(VaultArchiveOutcome::Alert(msg)) => {
                warn!(message = %msg, "vault archive alert");
                remaining.push(job);
            }
            Ok(VaultArchiveOutcome::Deferred) => {
                remaining.push(job);
            }
            Ok(VaultArchiveOutcome::Skipped) => {}
            Err(err) => {
                warn!(error = %err, "vault archive failed");
                remaining.push(job);
            }
        }
    }
    let mut next = VaultArchiveQueue { jobs: remaining };
    next.save(&path)?;
    Ok(processed)
}

pub fn maybe_archive_subject_with_config(
    store: &Store,
    config: &Config,
    identity: &IdentityState,
    subject: SubjectId,
    subject_key: [u8; 32],
    ver: u64,
    schema_id: SchemaId,
    contract_id: ContractId,
) -> Result<VaultArchiveOutcome, DharmaError> {
    if !config.vault.enabled {
        return Ok(VaultArchiveOutcome::Skipped);
    }

    let now = store.env().now().max(0) as u64;
    let monitor_path = vault_monitor_path(store.root());
    let mut monitor = VaultMonitorState::load(&monitor_path);
    if !monitor.should_check(now, config.vault.check_interval_secs) {
        return Ok(VaultArchiveOutcome::Deferred);
    }

    match backup_policy_status(store, &subject)? {
        BackupPolicyStatus::Defined { .. } => {}
        status => {
            let message = match status {
                BackupPolicyStatus::MissingDomainPolicy { domain } => {
                    format!("domain {domain} has no backup policy")
                }
                BackupPolicyStatus::MissingRelayDomain {
                    domain,
                    relay_domain,
                } => format!("domain {domain} relay {relay_domain} missing"),
                BackupPolicyStatus::MissingRelayPlan {
                    domain,
                    relay_domain,
                    plan,
                } => format!("domain {domain} relay {relay_domain} missing plan {plan}"),
                BackupPolicyStatus::MissingRelayGrant {
                    domain,
                    relay_domain,
                    plan,
                } => format!("domain {domain} has no grant for plan {plan} on {relay_domain}"),
                BackupPolicyStatus::OwnerIdentity => {
                    "subject owner is identity; no domain backup policy".to_string()
                }
                BackupPolicyStatus::MissingOwnership => {
                    "subject ownership missing; backup policy undefined".to_string()
                }
                BackupPolicyStatus::MissingDomainName => {
                    "domain subject missing domain name".to_string()
                }
                BackupPolicyStatus::Defined { .. } => String::new(),
            };
            if monitor.should_alert(now, config.vault.alert_interval_secs) {
                monitor.last_alert_ts = now;
                monitor.last_check_ts = now;
                monitor.save(&monitor_path)?;
                return Ok(VaultArchiveOutcome::Alert(message));
            }
            monitor.last_check_ts = now;
            monitor.save(&monitor_path)?;
            return Ok(VaultArchiveOutcome::Deferred);
        }
    }

    let metrics = gather_metrics(store.root(), config.vault.max_local_storage_mb)?;
    let max_storage = metrics.max_storage_bytes;
    let pressure_storage = metrics.storage_bytes >= max_storage;
    let pressure_disk = metrics.disk_used_pct >= config.vault.disk_pressure_pct;
    let approaching_storage = metrics.storage_bytes
        >= max_storage.saturating_mul(u64::from(config.vault.alert_threshold_pct)) / 100;
    let approaching_disk = metrics.disk_used_pct >= config.vault.alert_threshold_pct;

    let driver = build_driver(config, store.root())?;
    let has_driver = driver.is_some();

    if !has_driver {
        if (approaching_storage || approaching_disk)
            && monitor.should_alert(now, config.vault.alert_interval_secs)
        {
            monitor.last_alert_ts = now;
            monitor.last_check_ts = now;
            monitor.save(&monitor_path)?;
            return Ok(VaultArchiveOutcome::Alert(format!(
                "vault backup not configured; storage {}MB/{}MB, disk {}% used",
                metrics.storage_bytes / (1024 * 1024),
                max_storage / (1024 * 1024),
                metrics.disk_used_pct
            )));
        }
        monitor.last_check_ts = now;
        monitor.save(&monitor_path)?;
        return Ok(VaultArchiveOutcome::Deferred);
    }

    let should_archive = match config.vault.mode {
        VaultArchiveMode::SafeStorage => true,
        VaultArchiveMode::InfiniteStorage => pressure_storage || pressure_disk,
    };
    if !should_archive {
        monitor.last_check_ts = now;
        monitor.save(&monitor_path)?;
        return Ok(VaultArchiveOutcome::Deferred);
    }

    let checkpoint_schema_hex = match config.vault.checkpoint_schema.as_ref() {
        Some(value) if !value.trim().is_empty() => value,
        _ => {
            if monitor.should_alert(now, config.vault.alert_interval_secs) {
                monitor.last_alert_ts = now;
                monitor.last_check_ts = now;
                monitor.save(&monitor_path)?;
                return Ok(VaultArchiveOutcome::Alert(
                    "vault checkpoint schema not configured".to_string(),
                ));
            }
            monitor.last_check_ts = now;
            monitor.save(&monitor_path)?;
            return Ok(VaultArchiveOutcome::Deferred);
        }
    };
    let checkpoint_contract_hex = match config.vault.checkpoint_contract.as_ref() {
        Some(value) if !value.trim().is_empty() => value,
        _ => {
            if monitor.should_alert(now, config.vault.alert_interval_secs) {
                monitor.last_alert_ts = now;
                monitor.last_check_ts = now;
                monitor.save(&monitor_path)?;
                return Ok(VaultArchiveOutcome::Alert(
                    "vault checkpoint contract not configured".to_string(),
                ));
            }
            monitor.last_check_ts = now;
            monitor.save(&monitor_path)?;
            return Ok(VaultArchiveOutcome::Deferred);
        }
    };
    let checkpoint_schema = SchemaId::from_hex(checkpoint_schema_hex)?;
    let checkpoint_contract = ContractId::from_hex(checkpoint_contract_hex)?;

    let driver = driver.expect("driver checked");
    let input = VaultArchiveInput {
        subject,
        schema_id,
        contract_id,
        checkpoint_schema,
        checkpoint_contract,
        ver,
        signer_subject: identity.subject_id,
        signer_key: identity.public_key,
        signing_key: &identity.signing_key,
        svk: subject_key,
        dict: VaultDictionaryRef::None,
        driver: driver.as_ref(),
        config: VaultConfig::default(),
    };
    let results = archive_subject(store, input)?;
    monitor.last_check_ts = now;
    monitor.save(&monitor_path)?;
    if results.is_empty() {
        Ok(VaultArchiveOutcome::Skipped)
    } else {
        Ok(VaultArchiveOutcome::Archived(results.len()))
    }
}

struct VaultMetrics {
    storage_bytes: u64,
    disk_used_pct: u8,
    max_storage_bytes: u64,
}

fn gather_metrics(root: &Path, max_local_storage_mb: u64) -> Result<VaultMetrics, DharmaError> {
    let storage_bytes = dir_size(root)?;
    let max_storage_bytes = max_local_storage_mb.saturating_mul(1024 * 1024);
    let disk_used_pct = disk_usage_percent(root)?;
    Ok(VaultMetrics {
        storage_bytes,
        disk_used_pct,
        max_storage_bytes,
    })
}

fn disk_usage_percent(path: &Path) -> Result<u8, DharmaError> {
    let total = total_space(path)?;
    if total == 0 {
        return Ok(0);
    }
    let available = available_space(path)?;
    let used = total.saturating_sub(available);
    let pct = ((used.saturating_mul(100)) / total).min(100);
    Ok(pct as u8)
}

fn dir_size(path: &Path) -> Result<u64, DharmaError> {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(next) = stack.pop() {
        let entries = match fs::read_dir(&next) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        for entry in entries {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_dir() {
                stack.push(entry.path());
            } else if meta.is_file() {
                total = total.saturating_add(meta.len());
            }
        }
    }
    Ok(total)
}

fn build_driver(
    config: &Config,
    data_root: &Path,
) -> Result<Option<Box<dyn VaultDriver>>, DharmaError> {
    let Some(driver_name) = config.vault.driver.as_ref() else {
        return Ok(None);
    };
    let name = driver_name.trim().to_lowercase();
    if name.is_empty() {
        return Ok(None);
    }
    match name.as_str() {
        "local" => {
            let root = config
                .vault
                .local
                .path
                .as_ref()
                .map(PathBuf::from)
                .unwrap_or_else(|| data_root.join("vault").join("local"));
            Ok(Some(Box::new(LocalDriver::new(root))))
        }
        "peer" => {
            let peer_id = config
                .vault
                .peer
                .peer_id
                .as_ref()
                .ok_or_else(|| DharmaError::Config("vault peer_id missing".to_string()))?;
            let root = config
                .vault
                .peer
                .root
                .as_ref()
                .map(PathBuf::from)
                .unwrap_or_else(|| data_root.join("vault").join("peers"));
            Ok(Some(Box::new(PeerDriver::new(root, peer_id.clone()))))
        }
        "s3" => {
            #[cfg(feature = "vault-s3")]
            {
                let bucket =
                    config.vault.s3.bucket.as_ref().ok_or_else(|| {
                        DharmaError::Config("vault s3.bucket missing".to_string())
                    })?;
                let prefix = config.vault.s3.prefix.clone().unwrap_or_default();
                let mut opts = S3Options::default();
                opts.endpoint_url = config.vault.s3.endpoint_url.clone();
                opts.region = config.vault.s3.region.clone();
                opts.force_path_style = config.vault.s3.force_path_style;
                let driver = S3Driver::new_with_options(bucket.clone(), prefix, opts)?;
                Ok(Some(Box::new(driver)))
            }
            #[cfg(not(feature = "vault-s3"))]
            {
                Err(DharmaError::Config(
                    "vault-s3 feature not enabled".to_string(),
                ))
            }
        }
        "arweave" => {
            #[cfg(feature = "vault-arweave")]
            {
                let driver = if config.vault.arweave.arlocal {
                    let endpoint = config.vault.arweave.gateway_url.as_ref().ok_or_else(|| {
                        DharmaError::Config("vault arweave.gateway_url missing".to_string())
                    })?;
                    ArweaveDriver::new_arlocal(endpoint.clone())?
                } else {
                    let upload_url = config.vault.arweave.upload_url.as_ref().ok_or_else(|| {
                        DharmaError::Config("vault arweave.upload_url missing".to_string())
                    })?;
                    let gateway_url =
                        config.vault.arweave.gateway_url.as_ref().ok_or_else(|| {
                            DharmaError::Config("vault arweave.gateway_url missing".to_string())
                        })?;
                    ArweaveDriver::new(
                        upload_url.clone(),
                        gateway_url.clone(),
                        config.vault.arweave.token.clone(),
                    )?
                };
                Ok(Some(Box::new(driver)))
            }
            #[cfg(not(feature = "vault-arweave"))]
            {
                Err(DharmaError::Config(
                    "vault-arweave feature not enabled".to_string(),
                ))
            }
        }
        other => Err(DharmaError::Config(format!("unknown vault driver {other}"))),
    }
}

#[derive(Clone, Copy, Debug)]
struct VaultMonitorState {
    last_check_ts: u64,
    last_alert_ts: u64,
}

impl Default for VaultMonitorState {
    fn default() -> Self {
        Self {
            last_check_ts: 0,
            last_alert_ts: 0,
        }
    }
}

impl VaultMonitorState {
    fn load(path: &Path) -> Self {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(_) => return Self::default(),
        };
        let value = match cbor::decode_value(&bytes) {
            Ok(value) => value,
            Err(_) => return Self::default(),
        };
        let map = match value {
            Value::Map(map) => map,
            _ => return Self::default(),
        };
        let mut state = Self::default();
        for (key, val) in map {
            let key = match key {
                Value::Text(text) => text,
                _ => continue,
            };
            let num = match val {
                Value::Integer(num) => num_to_u64(num),
                _ => None,
            };
            if let Some(num) = num {
                match key.as_str() {
                    "last_check_ts" => state.last_check_ts = num,
                    "last_alert_ts" => state.last_alert_ts = num,
                    _ => {}
                }
            }
        }
        state
    }

    fn save(&self, path: &Path) -> Result<(), DharmaError> {
        let mut entries = Vec::new();
        entries.push((
            Value::Text("last_check_ts".to_string()),
            Value::Integer(self.last_check_ts.into()),
        ));
        entries.push((
            Value::Text("last_alert_ts".to_string()),
            Value::Integer(self.last_alert_ts.into()),
        ));
        let value = Value::Map(entries);
        let bytes = cbor::encode_canonical_value(&value)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, bytes)?;
        Ok(())
    }

    fn should_check(&self, now: u64, interval_secs: u64) -> bool {
        if interval_secs == 0 {
            return true;
        }
        now.saturating_sub(self.last_check_ts) >= interval_secs
    }

    fn should_alert(&self, now: u64, interval_secs: u64) -> bool {
        if interval_secs == 0 {
            return true;
        }
        now.saturating_sub(self.last_alert_ts) >= interval_secs
    }
}

fn num_to_u64(num: ciborium::value::Integer) -> Option<u64> {
    num.try_into().ok()
}

fn vault_monitor_path(root: &Path) -> PathBuf {
    root.join("vault").join("monitor.cbor")
}

fn vault_queue_path(root: &Path) -> PathBuf {
    root.join("vault").join("queue.cbor")
}

#[derive(Clone, Debug, Default)]
struct VaultArchiveQueue {
    jobs: Vec<VaultArchiveJob>,
}

impl VaultArchiveQueue {
    fn load(path: &Path) -> Self {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(_) => return Self::default(),
        };
        let value = match cbor::decode_value(&bytes) {
            Ok(value) => value,
            Err(_) => return Self::default(),
        };
        let map = match value {
            Value::Map(map) => map,
            _ => return Self::default(),
        };
        let list_val = map_get(&map, "jobs");
        let mut jobs = Vec::new();
        if let Some(Value::Array(items)) = list_val {
            for item in items {
                if let Some(job) = VaultArchiveJob::from_value(item) {
                    jobs.push(job);
                }
            }
        }
        Self { jobs }
    }

    fn save(&mut self, path: &Path) -> Result<(), DharmaError> {
        let jobs = Value::Array(self.jobs.iter().map(|j| j.to_value()).collect());
        let value = Value::Map(vec![(Value::Text("jobs".to_string()), jobs)]);
        let bytes = cbor::encode_canonical_value(&value)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, bytes)?;
        Ok(())
    }
}

impl VaultArchiveJob {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (
                Value::Text("subject".to_string()),
                Value::Bytes(self.subject.as_bytes().to_vec()),
            ),
            (
                Value::Text("ver".to_string()),
                Value::Integer(self.ver.into()),
            ),
            (
                Value::Text("schema".to_string()),
                Value::Bytes(self.schema_id.as_bytes().to_vec()),
            ),
            (
                Value::Text("contract".to_string()),
                Value::Bytes(self.contract_id.as_bytes().to_vec()),
            ),
        ])
    }

    fn from_value(value: &Value) -> Option<Self> {
        let map = expect_map(value).ok()?;
        let subject_val = map_get(map, "subject")?;
        let schema_val = map_get(map, "schema")?;
        let contract_val = map_get(map, "contract")?;
        let ver_val = map_get(map, "ver")?;
        let subject_bytes = expect_bytes(subject_val).ok()?;
        let schema_bytes = expect_bytes(schema_val).ok()?;
        let contract_bytes = expect_bytes(contract_val).ok()?;
        let ver = expect_uint(ver_val).ok()?;
        let subject = SubjectId::from_slice(&subject_bytes).ok()?;
        let schema_id = SchemaId::from_slice(&schema_bytes).ok()?;
        let contract_id = ContractId::from_slice(&contract_bytes).ok()?;
        Some(Self {
            subject,
            ver,
            schema_id,
            contract_id,
        })
    }
}
