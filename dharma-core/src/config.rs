use crate::error::DharmaError;
use crate::net::codec;
use crate::runtime::vm::{set_default_limits, VmLimits};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEFAULT_LISTEN_PORT: u16 = 3000;
const DEFAULT_MAX_PEERS: usize = 50;
const DEFAULT_MAX_CONNECTIONS: usize = 256;
const DEFAULT_MAX_FRAME_SIZE: usize = 1_048_576;
const DEFAULT_SYNC_OBJ_CHUNK_BYTES: usize = 256 * 1024;
const DEFAULT_SYNC_OBJ_BUFFER_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5000;
const DEFAULT_READ_TIMEOUT_MS: u64 = 5000;
const DEFAULT_WRITE_TIMEOUT_MS: u64 = 5000;
const DEFAULT_STORAGE_PATH: &str = "~/.dharma/data";
const DEFAULT_KEYSTORE_PATH: &str = "keystore";
const DEFAULT_SNAPSHOT_INTERVAL: u64 = 1000;
const DEFAULT_PRUNE_PENDING_HOURS: u64 = 24;
const DEFAULT_STORAGE_POSTGRES_URL: &str = "postgres://postgres:postgres@127.0.0.1:5432/dharma";
const DEFAULT_STORAGE_POSTGRES_SCHEMA: &str = "public";
const DEFAULT_STORAGE_POSTGRES_POOL_MAX_SIZE: u32 = 16;
const DEFAULT_STORAGE_POSTGRES_CONNECT_TIMEOUT_MS: u64 = 5000;
const DEFAULT_STORAGE_POSTGRES_ACQUIRE_TIMEOUT_MS: u64 = 5000;
const DEFAULT_STORAGE_POSTGRES_STATEMENT_TIMEOUT_MS: u64 = 5000;
const DEFAULT_STORAGE_POSTGRES_RETRY_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_STORAGE_POSTGRES_RETRY_BACKOFF_MS: u64 = 50;
const DEFAULT_STORAGE_CLICKHOUSE_ENABLED: bool = true;
const DEFAULT_STORAGE_CLICKHOUSE_URL: &str = "http://127.0.0.1:8123";
const DEFAULT_STORAGE_CLICKHOUSE_DATABASE: &str = "default";
const DEFAULT_STORAGE_CLICKHOUSE_TABLE_PREFIX: &str = "dharma_analytics";
const DEFAULT_STORAGE_CLICKHOUSE_CONNECT_TIMEOUT_MS: u64 = 5000;
const DEFAULT_STORAGE_CLICKHOUSE_RETRY_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_STORAGE_CLICKHOUSE_RETRY_BACKOFF_MS: u64 = 50;
const DEFAULT_PROFILE_MODE: &str = "embedded";
const DEFAULT_REGISTRY_URL: &str = "https://registry.dharma.systems";
const DEFAULT_VM_FUEL: u64 = 1_000_000;
const DEFAULT_VM_MEMORY_BYTES: usize = 640 * 1024;
const DEFAULT_COMPILER_OUT_DIR: &str = ".dharma/contracts";
const DEFAULT_VAULT_MODE: &str = "safe_storage";
const DEFAULT_VAULT_MAX_LOCAL_STORAGE_MB: u64 = 1024;
const DEFAULT_VAULT_DISK_PRESSURE_PCT: u8 = 90;
const DEFAULT_VAULT_ALERT_THRESHOLD_PCT: u8 = 80;
const DEFAULT_VAULT_CHECK_INTERVAL_SECS: u64 = 60;
const DEFAULT_VAULT_ALERT_INTERVAL_SECS: u64 = 3600;

#[derive(Clone, Debug)]
pub struct Config {
    pub identity: IdentityConfig,
    pub network: NetworkConfig,
    pub storage: StorageConfig,
    pub compiler: CompilerConfig,
    pub profile: ProfileConfig,
    pub registry: RegistryConfig,
    pub vm: VmConfig,
    pub vault: VaultPolicyConfig,
}

#[derive(Clone, Debug)]
pub struct IdentityConfig {
    pub default_key: Option<String>,
    pub keystore_path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NetworkConfig {
    pub listen_port: u16,
    pub peers: Vec<String>,
    pub max_peers: usize,
    pub max_connections: usize,
    pub max_frame_size: usize,
    pub sync_obj_chunk_bytes: usize,
    pub sync_obj_buffer_bytes: usize,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub write_timeout_ms: u64,
}

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub path: String,
    pub snapshot_interval: u64,
    pub prune_pending_hours: u64,
    pub postgres: StoragePostgresConfig,
    pub clickhouse: StorageClickHouseConfig,
}

#[derive(Clone, Debug)]
pub struct StoragePostgresConfig {
    pub url: String,
    pub schema: String,
    pub pool_max_size: u32,
    pub connect_timeout_ms: u64,
    pub acquire_timeout_ms: u64,
    pub statement_timeout_ms: u64,
    pub retry_max_attempts: u32,
    pub retry_backoff_ms: u64,
}

#[derive(Clone, Debug)]
pub struct StorageClickHouseConfig {
    pub enabled: bool,
    pub url: String,
    pub database: String,
    pub table_prefix: String,
    pub connect_timeout_ms: u64,
    pub retry_max_attempts: u32,
    pub retry_backoff_ms: u64,
}

#[derive(Clone, Debug)]
pub struct CompilerConfig {
    pub out_dir: String,
}

#[derive(Clone, Debug)]
pub struct ProfileConfig {
    pub mode: String,
}

#[derive(Clone, Debug)]
pub struct RegistryConfig {
    pub url: String,
    pub pins: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct VmConfig {
    pub fuel: u64,
    pub memory_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VaultArchiveMode {
    SafeStorage,
    InfiniteStorage,
}

impl VaultArchiveMode {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_lowercase().as_str() {
            "safe_storage" | "safe-storage" | "safe" => Some(VaultArchiveMode::SafeStorage),
            "infinite_storage" | "infinite-storage" | "infinite" => {
                Some(VaultArchiveMode::InfiniteStorage)
            }
            _ => None,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            VaultArchiveMode::SafeStorage => "safe_storage",
            VaultArchiveMode::InfiniteStorage => "infinite_storage",
        }
    }
}

#[derive(Clone, Debug)]
pub struct VaultPolicyConfig {
    pub enabled: bool,
    pub mode: VaultArchiveMode,
    pub driver: Option<String>,
    pub max_local_storage_mb: u64,
    pub disk_pressure_pct: u8,
    pub alert_threshold_pct: u8,
    pub check_interval_secs: u64,
    pub alert_interval_secs: u64,
    pub checkpoint_schema: Option<String>,
    pub checkpoint_contract: Option<String>,
    pub local: VaultLocalConfig,
    pub peer: VaultPeerConfig,
    pub s3: VaultS3Config,
    pub arweave: VaultArweaveConfig,
}

#[derive(Clone, Debug, Default)]
pub struct VaultLocalConfig {
    pub path: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct VaultPeerConfig {
    pub peer_id: Option<String>,
    pub root: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct VaultS3Config {
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
    pub force_path_style: bool,
}

#[derive(Clone, Debug, Default)]
pub struct VaultArweaveConfig {
    pub upload_url: Option<String>,
    pub gateway_url: Option<String>,
    pub token: Option<String>,
    pub arlocal: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            identity: IdentityConfig {
                default_key: None,
                keystore_path: Some(DEFAULT_KEYSTORE_PATH.to_string()),
            },
            network: NetworkConfig {
                listen_port: DEFAULT_LISTEN_PORT,
                peers: Vec::new(),
                max_peers: DEFAULT_MAX_PEERS,
                max_connections: DEFAULT_MAX_CONNECTIONS,
                max_frame_size: DEFAULT_MAX_FRAME_SIZE,
                sync_obj_chunk_bytes: DEFAULT_SYNC_OBJ_CHUNK_BYTES,
                sync_obj_buffer_bytes: DEFAULT_SYNC_OBJ_BUFFER_BYTES,
                connect_timeout_ms: DEFAULT_CONNECT_TIMEOUT_MS,
                read_timeout_ms: DEFAULT_READ_TIMEOUT_MS,
                write_timeout_ms: DEFAULT_WRITE_TIMEOUT_MS,
            },
            storage: StorageConfig {
                path: DEFAULT_STORAGE_PATH.to_string(),
                snapshot_interval: DEFAULT_SNAPSHOT_INTERVAL,
                prune_pending_hours: DEFAULT_PRUNE_PENDING_HOURS,
                postgres: StoragePostgresConfig {
                    url: DEFAULT_STORAGE_POSTGRES_URL.to_string(),
                    schema: DEFAULT_STORAGE_POSTGRES_SCHEMA.to_string(),
                    pool_max_size: DEFAULT_STORAGE_POSTGRES_POOL_MAX_SIZE,
                    connect_timeout_ms: DEFAULT_STORAGE_POSTGRES_CONNECT_TIMEOUT_MS,
                    acquire_timeout_ms: DEFAULT_STORAGE_POSTGRES_ACQUIRE_TIMEOUT_MS,
                    statement_timeout_ms: DEFAULT_STORAGE_POSTGRES_STATEMENT_TIMEOUT_MS,
                    retry_max_attempts: DEFAULT_STORAGE_POSTGRES_RETRY_MAX_ATTEMPTS,
                    retry_backoff_ms: DEFAULT_STORAGE_POSTGRES_RETRY_BACKOFF_MS,
                },
                clickhouse: StorageClickHouseConfig {
                    enabled: DEFAULT_STORAGE_CLICKHOUSE_ENABLED,
                    url: DEFAULT_STORAGE_CLICKHOUSE_URL.to_string(),
                    database: DEFAULT_STORAGE_CLICKHOUSE_DATABASE.to_string(),
                    table_prefix: DEFAULT_STORAGE_CLICKHOUSE_TABLE_PREFIX.to_string(),
                    connect_timeout_ms: DEFAULT_STORAGE_CLICKHOUSE_CONNECT_TIMEOUT_MS,
                    retry_max_attempts: DEFAULT_STORAGE_CLICKHOUSE_RETRY_MAX_ATTEMPTS,
                    retry_backoff_ms: DEFAULT_STORAGE_CLICKHOUSE_RETRY_BACKOFF_MS,
                },
            },
            compiler: CompilerConfig {
                out_dir: DEFAULT_COMPILER_OUT_DIR.to_string(),
            },
            profile: ProfileConfig {
                mode: DEFAULT_PROFILE_MODE.to_string(),
            },
            registry: RegistryConfig {
                url: DEFAULT_REGISTRY_URL.to_string(),
                pins: HashMap::new(),
            },
            vm: VmConfig {
                fuel: DEFAULT_VM_FUEL,
                memory_bytes: DEFAULT_VM_MEMORY_BYTES,
            },
            vault: VaultPolicyConfig {
                enabled: true,
                mode: VaultArchiveMode::SafeStorage,
                driver: None,
                max_local_storage_mb: DEFAULT_VAULT_MAX_LOCAL_STORAGE_MB,
                disk_pressure_pct: DEFAULT_VAULT_DISK_PRESSURE_PCT,
                alert_threshold_pct: DEFAULT_VAULT_ALERT_THRESHOLD_PCT,
                check_interval_secs: DEFAULT_VAULT_CHECK_INTERVAL_SECS,
                alert_interval_secs: DEFAULT_VAULT_ALERT_INTERVAL_SECS,
                checkpoint_schema: None,
                checkpoint_contract: None,
                local: VaultLocalConfig::default(),
                peer: VaultPeerConfig::default(),
                s3: VaultS3Config::default(),
                arweave: VaultArweaveConfig::default(),
            },
        }
    }
}

impl Config {
    pub fn load(root: &Path) -> Result<Self, DharmaError> {
        let mut cfg = Config::default();

        if let Some(legacy) = secondary_global_path()? {
            if legacy.exists() {
                cfg.apply_file(&legacy)?;
            }
        }
        let global = ensure_global_config_file()?;
        if global.exists() {
            cfg.apply_file(&global)?;
        }
        let local = root.join("dharma.toml");
        if local.exists() {
            cfg.apply_file(&local)?;
        }

        cfg.apply_runtime_defaults();
        Ok(cfg)
    }

    pub fn apply_runtime_defaults(&self) {
        codec::set_max_frame_size(self.network.max_frame_size);
        crate::net::sync::set_sync_obj_chunk_bytes(self.network.sync_obj_chunk_bytes);
        crate::net::sync::set_sync_obj_buffer_bytes(self.network.sync_obj_buffer_bytes);
        set_default_limits(VmLimits {
            fuel: self.vm.fuel,
            memory_bytes: self.vm.memory_bytes,
        });
    }

    pub fn to_toml_string(&self) -> String {
        let mut out = Vec::new();
        out.push("[identity]".to_string());
        out.push(format!(
            "default_key = \"{}\"",
            self.identity.default_key.clone().unwrap_or_default()
        ));
        out.push(format!(
            "keystore_path = \"{}\"",
            self.identity
                .keystore_path
                .clone()
                .unwrap_or_else(|| "".to_string())
        ));
        out.push(String::new());

        out.push("[network]".to_string());
        out.push(format!("listen_port = {}", self.network.listen_port));
        out.push(format!(
            "peers = [{}]",
            format_string_array(&self.network.peers)
        ));
        out.push(format!("max_peers = {}", self.network.max_peers));
        out.push(format!(
            "max_connections = {}",
            self.network.max_connections
        ));
        out.push(format!("max_frame_size = {}", self.network.max_frame_size));
        out.push(format!(
            "sync_obj_chunk_bytes = {}",
            self.network.sync_obj_chunk_bytes
        ));
        out.push(format!(
            "sync_obj_buffer_bytes = {}",
            self.network.sync_obj_buffer_bytes
        ));
        out.push(format!(
            "connect_timeout_ms = {}",
            self.network.connect_timeout_ms
        ));
        out.push(format!(
            "read_timeout_ms = {}",
            self.network.read_timeout_ms
        ));
        out.push(format!(
            "write_timeout_ms = {}",
            self.network.write_timeout_ms
        ));
        out.push(String::new());

        out.push("[storage]".to_string());
        out.push(format!("path = \"{}\"", self.storage.path));
        out.push(format!(
            "snapshot_interval = {}",
            self.storage.snapshot_interval
        ));
        out.push(format!(
            "prune_pending_hours = {}",
            self.storage.prune_pending_hours
        ));
        out.push(String::new());
        out.push("[storage.postgres]".to_string());
        out.push(format!("url = \"{}\"", self.storage.postgres.url));
        out.push(format!("schema = \"{}\"", self.storage.postgres.schema));
        out.push(format!(
            "pool_max_size = {}",
            self.storage.postgres.pool_max_size
        ));
        out.push(format!(
            "connect_timeout_ms = {}",
            self.storage.postgres.connect_timeout_ms
        ));
        out.push(format!(
            "acquire_timeout_ms = {}",
            self.storage.postgres.acquire_timeout_ms
        ));
        out.push(format!(
            "statement_timeout_ms = {}",
            self.storage.postgres.statement_timeout_ms
        ));
        out.push(format!(
            "retry_max_attempts = {}",
            self.storage.postgres.retry_max_attempts
        ));
        out.push(format!(
            "retry_backoff_ms = {}",
            self.storage.postgres.retry_backoff_ms
        ));
        out.push(String::new());
        out.push("[storage.clickhouse]".to_string());
        out.push(format!("enabled = {}", self.storage.clickhouse.enabled));
        out.push(format!("url = \"{}\"", self.storage.clickhouse.url));
        out.push(format!(
            "database = \"{}\"",
            self.storage.clickhouse.database
        ));
        out.push(format!(
            "table_prefix = \"{}\"",
            self.storage.clickhouse.table_prefix
        ));
        out.push(format!(
            "connect_timeout_ms = {}",
            self.storage.clickhouse.connect_timeout_ms
        ));
        out.push(format!(
            "retry_max_attempts = {}",
            self.storage.clickhouse.retry_max_attempts
        ));
        out.push(format!(
            "retry_backoff_ms = {}",
            self.storage.clickhouse.retry_backoff_ms
        ));
        out.push(String::new());

        out.push("[compiler]".to_string());
        out.push(format!("out_dir = \"{}\"", self.compiler.out_dir));
        out.push(String::new());

        out.push("[profile]".to_string());
        out.push(format!("mode = \"{}\"", self.profile.mode));
        out.push(String::new());

        out.push("[registry]".to_string());
        out.push(format!("url = \"{}\"", self.registry.url));
        out.push(String::new());

        out.push("[registry.pins]".to_string());
        let mut pins: Vec<_> = self.registry.pins.iter().collect();
        pins.sort_by(|a, b| a.0.cmp(b.0));
        for (name, version) in pins {
            out.push(format!("\"{}\" = \"{}\"", name, version));
        }
        out.push(String::new());

        out.push("[vm]".to_string());
        out.push(format!("fuel = {}", self.vm.fuel));
        out.push(format!("memory_bytes = {}", self.vm.memory_bytes));
        out.push(String::new());

        out.push("[vault]".to_string());
        out.push(format!("enabled = {}", self.vault.enabled));
        out.push(format!("mode = \"{}\"", self.vault.mode.as_str()));
        out.push(format!(
            "driver = \"{}\"",
            self.vault.driver.clone().unwrap_or_default()
        ));
        out.push(format!(
            "max_local_storage_mb = {}",
            self.vault.max_local_storage_mb
        ));
        out.push(format!(
            "disk_pressure_pct = {}",
            self.vault.disk_pressure_pct
        ));
        out.push(format!(
            "alert_threshold_pct = {}",
            self.vault.alert_threshold_pct
        ));
        out.push(format!(
            "check_interval_secs = {}",
            self.vault.check_interval_secs
        ));
        out.push(format!(
            "alert_interval_secs = {}",
            self.vault.alert_interval_secs
        ));
        out.push(format!(
            "checkpoint_schema = \"{}\"",
            self.vault.checkpoint_schema.clone().unwrap_or_default()
        ));
        out.push(format!(
            "checkpoint_contract = \"{}\"",
            self.vault.checkpoint_contract.clone().unwrap_or_default()
        ));
        out.push(String::new());

        out.push("[vault.local]".to_string());
        out.push(format!(
            "path = \"{}\"",
            self.vault.local.path.clone().unwrap_or_default()
        ));
        out.push(String::new());

        out.push("[vault.peer]".to_string());
        out.push(format!(
            "peer_id = \"{}\"",
            self.vault.peer.peer_id.clone().unwrap_or_default()
        ));
        out.push(format!(
            "root = \"{}\"",
            self.vault.peer.root.clone().unwrap_or_default()
        ));
        out.push(String::new());

        out.push("[vault.s3]".to_string());
        out.push(format!(
            "bucket = \"{}\"",
            self.vault.s3.bucket.clone().unwrap_or_default()
        ));
        out.push(format!(
            "prefix = \"{}\"",
            self.vault.s3.prefix.clone().unwrap_or_default()
        ));
        out.push(format!(
            "endpoint_url = \"{}\"",
            self.vault.s3.endpoint_url.clone().unwrap_or_default()
        ));
        out.push(format!(
            "region = \"{}\"",
            self.vault.s3.region.clone().unwrap_or_default()
        ));
        out.push(format!(
            "force_path_style = {}",
            self.vault.s3.force_path_style
        ));
        out.push(String::new());

        out.push("[vault.arweave]".to_string());
        out.push(format!(
            "upload_url = \"{}\"",
            self.vault.arweave.upload_url.clone().unwrap_or_default()
        ));
        out.push(format!(
            "gateway_url = \"{}\"",
            self.vault.arweave.gateway_url.clone().unwrap_or_default()
        ));
        out.push(format!(
            "token = \"{}\"",
            self.vault.arweave.token.clone().unwrap_or_default()
        ));
        out.push(format!("arlocal = {}", self.vault.arweave.arlocal));
        out.push(String::new());

        out.join("\n")
    }

    pub fn storage_path(&self, root: &Path) -> PathBuf {
        resolve_path(root, &self.storage.path)
    }

    pub fn keystore_path_for(&self, project_root: &Path, storage_root: &Path) -> PathBuf {
        let Some(path) = &self.identity.keystore_path else {
            return storage_root.join("identity.key");
        };
        let resolved = resolve_keystore_path(project_root, storage_root, path);
        if resolved.extension().is_some() {
            resolved
        } else {
            resolved.join("identity.key")
        }
    }

    pub fn apply_timeouts(&self, stream: &TcpStream) {
        let read = Duration::from_millis(self.network.read_timeout_ms);
        let write = Duration::from_millis(self.network.write_timeout_ms);
        let _ = stream.set_read_timeout(Some(read));
        let _ = stream.set_write_timeout(Some(write));
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.network.connect_timeout_ms)
    }

    fn apply_file(&mut self, path: &Path) -> Result<(), DharmaError> {
        let contents = fs::read_to_string(path)?;
        let pairs = parse_pairs(&contents)?;
        for (key, value) in pairs {
            self.apply_pair(&key, value)?;
        }
        Ok(())
    }

    fn apply_pair(&mut self, key: &str, value: ConfigValue) -> Result<(), DharmaError> {
        match key {
            "identity.default_key" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.identity.default_key = None;
                    } else {
                        self.identity.default_key = Some(val);
                    }
                }
            }
            "identity.keystore_path" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.identity.keystore_path = None;
                    } else {
                        self.identity.keystore_path = Some(val);
                    }
                }
            }
            "network.listen_port" => {
                if let ConfigValue::Int(val) = value {
                    if val > 0 && val <= u16::MAX as i64 {
                        self.network.listen_port = val as u16;
                    }
                }
            }
            "network.peers" => {
                if let ConfigValue::Array(items) = value {
                    self.network.peers = items
                        .into_iter()
                        .filter_map(|item| match item {
                            ConfigValue::Str(val) => Some(val),
                            _ => None,
                        })
                        .collect();
                }
            }
            "network.max_peers" => {
                if let ConfigValue::Int(val) = value {
                    if val > 0 {
                        self.network.max_peers = val as usize;
                    }
                }
            }
            "network.max_connections" => match value {
                ConfigValue::Int(val) => {
                    if val <= 0 {
                        return Err(DharmaError::Config(
                            "network.max_connections must be a positive integer".to_string(),
                        ));
                    }
                    let parsed = usize::try_from(val).map_err(|_| {
                        DharmaError::Config(
                            "network.max_connections is too large for this platform".to_string(),
                        )
                    })?;
                    self.network.max_connections = parsed;
                }
                _ => {
                    return Err(DharmaError::Config(
                        "network.max_connections must be a positive integer".to_string(),
                    ));
                }
            },
            "network.max_frame_size" => {
                if let ConfigValue::Int(val) = value {
                    if val > 0 {
                        self.network.max_frame_size = val as usize;
                    }
                }
            }
            "network.sync_obj_chunk_bytes" => {
                if let ConfigValue::Int(val) = value {
                    if val > 0 {
                        self.network.sync_obj_chunk_bytes = val as usize;
                    }
                }
            }
            "network.sync_obj_buffer_bytes" => {
                if let ConfigValue::Int(val) = value {
                    if val > 0 {
                        self.network.sync_obj_buffer_bytes = val as usize;
                    }
                }
            }
            "network.connect_timeout_ms" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 {
                        self.network.connect_timeout_ms = val as u64;
                    }
                }
            }
            "network.read_timeout_ms" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 {
                        self.network.read_timeout_ms = val as u64;
                    }
                }
            }
            "network.write_timeout_ms" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 {
                        self.network.write_timeout_ms = val as u64;
                    }
                }
            }
            "storage.path" => {
                if let ConfigValue::Str(val) = value {
                    self.storage.path = val;
                }
            }
            "storage.snapshot_interval" => {
                if let ConfigValue::Int(val) = value {
                    if val > 0 {
                        self.storage.snapshot_interval = val as u64;
                    }
                }
            }
            "storage.prune_pending_hours" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 {
                        self.storage.prune_pending_hours = val as u64;
                    }
                }
            }
            "storage.postgres.url" => {
                if let ConfigValue::Str(val) = value {
                    if !val.is_empty() {
                        self.storage.postgres.url = val;
                    }
                }
            }
            "storage.postgres.schema" => {
                if let ConfigValue::Str(val) = value {
                    if !val.is_empty() {
                        self.storage.postgres.schema = val;
                    }
                }
            }
            "storage.postgres.pool_max_size" => {
                if let ConfigValue::Int(val) = value {
                    if val > 0 && val <= u32::MAX as i64 {
                        self.storage.postgres.pool_max_size = val as u32;
                    }
                }
            }
            "storage.postgres.connect_timeout_ms" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 {
                        self.storage.postgres.connect_timeout_ms = val as u64;
                    }
                }
            }
            "storage.postgres.acquire_timeout_ms" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 {
                        self.storage.postgres.acquire_timeout_ms = val as u64;
                    }
                }
            }
            "storage.postgres.statement_timeout_ms" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 {
                        self.storage.postgres.statement_timeout_ms = val as u64;
                    }
                }
            }
            "storage.postgres.retry_max_attempts" => {
                if let ConfigValue::Int(val) = value {
                    if val > 0 && val <= u32::MAX as i64 {
                        self.storage.postgres.retry_max_attempts = val as u32;
                    }
                }
            }
            "storage.postgres.retry_backoff_ms" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 {
                        self.storage.postgres.retry_backoff_ms = val as u64;
                    }
                }
            }
            "storage.clickhouse.enabled" => {
                if let ConfigValue::Bool(val) = value {
                    self.storage.clickhouse.enabled = val;
                }
            }
            "storage.clickhouse.url" => {
                if let ConfigValue::Str(val) = value {
                    if !val.is_empty() {
                        self.storage.clickhouse.url = val;
                    }
                }
            }
            "storage.clickhouse.database" => {
                if let ConfigValue::Str(val) = value {
                    if !val.is_empty() {
                        self.storage.clickhouse.database = val;
                    }
                }
            }
            "storage.clickhouse.table_prefix" => {
                if let ConfigValue::Str(val) = value {
                    if !val.is_empty() {
                        self.storage.clickhouse.table_prefix = val;
                    }
                }
            }
            "storage.clickhouse.connect_timeout_ms" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 {
                        self.storage.clickhouse.connect_timeout_ms = val as u64;
                    }
                }
            }
            "storage.clickhouse.retry_max_attempts" => {
                if let ConfigValue::Int(val) = value {
                    if val > 0 && val <= u32::MAX as i64 {
                        self.storage.clickhouse.retry_max_attempts = val as u32;
                    }
                }
            }
            "storage.clickhouse.retry_backoff_ms" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 {
                        self.storage.clickhouse.retry_backoff_ms = val as u64;
                    }
                }
            }
            "profile.mode" => {
                if let ConfigValue::Str(val) = value {
                    self.profile.mode = val;
                }
            }
            "compiler.out_dir" => {
                if let ConfigValue::Str(val) = value {
                    if !val.is_empty() {
                        self.compiler.out_dir = val;
                    }
                }
            }
            "registry.url" => {
                if let ConfigValue::Str(val) = value {
                    self.registry.url = val;
                }
            }
            key if key.starts_with("registry.pins.") => {
                if let ConfigValue::Str(val) = value {
                    let name = key.trim_start_matches("registry.pins.").to_string();
                    if !name.is_empty() {
                        self.registry.pins.insert(name, val);
                    }
                }
            }
            "vm.fuel" => {
                if let ConfigValue::Int(val) = value {
                    if val > 0 {
                        self.vm.fuel = val as u64;
                    }
                }
            }
            "vm.memory_bytes" => {
                if let ConfigValue::Int(val) = value {
                    if val > 0 {
                        self.vm.memory_bytes = val as usize;
                    }
                }
            }
            "vault.enabled" => {
                if let ConfigValue::Bool(val) = value {
                    self.vault.enabled = val;
                }
            }
            "vault.mode" => {
                if let ConfigValue::Str(val) = value {
                    if let Some(mode) = VaultArchiveMode::parse(&val) {
                        self.vault.mode = mode;
                    }
                }
            }
            "vault.driver" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.driver = None;
                    } else {
                        self.vault.driver = Some(val);
                    }
                }
            }
            "vault.max_local_storage_mb" => {
                if let ConfigValue::Int(val) = value {
                    if val > 0 {
                        self.vault.max_local_storage_mb = val as u64;
                    }
                }
            }
            "vault.disk_pressure_pct" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 && val <= 100 {
                        self.vault.disk_pressure_pct = val as u8;
                    }
                }
            }
            "vault.alert_threshold_pct" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 && val <= 100 {
                        self.vault.alert_threshold_pct = val as u8;
                    }
                }
            }
            "vault.check_interval_secs" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 {
                        self.vault.check_interval_secs = val as u64;
                    }
                }
            }
            "vault.alert_interval_secs" => {
                if let ConfigValue::Int(val) = value {
                    if val >= 0 {
                        self.vault.alert_interval_secs = val as u64;
                    }
                }
            }
            "vault.checkpoint_schema" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.checkpoint_schema = None;
                    } else {
                        self.vault.checkpoint_schema = Some(val);
                    }
                }
            }
            "vault.checkpoint_contract" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.checkpoint_contract = None;
                    } else {
                        self.vault.checkpoint_contract = Some(val);
                    }
                }
            }
            "vault.local.path" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.local.path = None;
                    } else {
                        self.vault.local.path = Some(val);
                    }
                }
            }
            "vault.peer.peer_id" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.peer.peer_id = None;
                    } else {
                        self.vault.peer.peer_id = Some(val);
                    }
                }
            }
            "vault.peer.root" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.peer.root = None;
                    } else {
                        self.vault.peer.root = Some(val);
                    }
                }
            }
            "vault.s3.bucket" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.s3.bucket = None;
                    } else {
                        self.vault.s3.bucket = Some(val);
                    }
                }
            }
            "vault.s3.prefix" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.s3.prefix = None;
                    } else {
                        self.vault.s3.prefix = Some(val);
                    }
                }
            }
            "vault.s3.endpoint_url" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.s3.endpoint_url = None;
                    } else {
                        self.vault.s3.endpoint_url = Some(val);
                    }
                }
            }
            "vault.s3.region" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.s3.region = None;
                    } else {
                        self.vault.s3.region = Some(val);
                    }
                }
            }
            "vault.s3.force_path_style" => {
                if let ConfigValue::Bool(val) = value {
                    self.vault.s3.force_path_style = val;
                }
            }
            "vault.arweave.upload_url" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.arweave.upload_url = None;
                    } else {
                        self.vault.arweave.upload_url = Some(val);
                    }
                }
            }
            "vault.arweave.gateway_url" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.arweave.gateway_url = None;
                    } else {
                        self.vault.arweave.gateway_url = Some(val);
                    }
                }
            }
            "vault.arweave.token" => {
                if let ConfigValue::Str(val) = value {
                    if val.is_empty() {
                        self.vault.arweave.token = None;
                    } else {
                        self.vault.arweave.token = Some(val);
                    }
                }
            }
            "vault.arweave.arlocal" => {
                if let ConfigValue::Bool(val) = value {
                    self.vault.arweave.arlocal = val;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn resolve_path(root: &Path, value: &str) -> PathBuf {
    if let Some(rest) = value.strip_prefix("~/") {
        if let Some(home) = home_dir() {
            return home.join(rest);
        }
    }
    let candidate = PathBuf::from(value);
    if candidate.is_absolute() {
        return candidate;
    }
    root.join(value)
}

fn resolve_keystore_path(project_root: &Path, storage_root: &Path, value: &str) -> PathBuf {
    if value.starts_with("~/") || Path::new(value).is_absolute() {
        return resolve_path(project_root, value);
    }
    if value.starts_with("./") || value.starts_with("../") {
        return project_root.join(value);
    }
    storage_root.join(value)
}

fn ensure_global_config_file() -> Result<PathBuf, DharmaError> {
    ensure_home_layout()?;
    let path = global_config_path()?;
    if !path.exists() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, default_config_template())?;
    }
    Ok(path)
}

fn ensure_home_layout() -> Result<(), DharmaError> {
    let Some(root) = home_layout_root() else {
        return Ok(());
    };
    fs::create_dir_all(&root)?;
    for dir in ["data", "contracts"] {
        fs::create_dir_all(root.join(dir))?;
    }
    Ok(())
}

fn home_layout_root() -> Option<PathBuf> {
    if let Ok(path) = env::var("DHARMA_CONFIG_PATH") {
        let path = PathBuf::from(path);
        if path.file_name().and_then(|s| s.to_str()) == Some("config.toml") {
            if let Some(parent) = path.parent() {
                if parent.file_name().and_then(|s| s.to_str()) == Some(".dharma") {
                    return Some(parent.to_path_buf());
                }
            }
        }
        return None;
    }
    home_dir().map(|home| home.join(".dharma"))
}

fn global_config_path() -> Result<PathBuf, DharmaError> {
    if let Ok(path) = env::var("DHARMA_CONFIG_PATH") {
        return Ok(PathBuf::from(path));
    }
    let home = home_dir().ok_or_else(|| {
        DharmaError::Config("unable to locate home directory for config".to_string())
    })?;
    Ok(home.join(".dharma").join("config.toml"))
}

fn secondary_global_path() -> Result<Option<PathBuf>, DharmaError> {
    let home = match home_dir() {
        Some(home) => home,
        None => return Ok(None),
    };
    Ok(Some(
        home.join(".config").join("dharma").join("dharma.toml"),
    ))
}

fn home_dir() -> Option<PathBuf> {
    if let Ok(path) = env::var("HOME") {
        if !path.is_empty() {
            return Some(PathBuf::from(path));
        }
    }
    if let Ok(path) = env::var("USERPROFILE") {
        if !path.is_empty() {
            return Some(PathBuf::from(path));
        }
    }
    if let (Ok(drive), Ok(path)) = (env::var("HOMEDRIVE"), env::var("HOMEPATH")) {
        if !drive.is_empty() && !path.is_empty() {
            return Some(PathBuf::from(format!("{}{}", drive, path)));
        }
    }
    None
}

fn default_config_template() -> String {
    [
        "[identity]",
        "default_key = \"\"",
        &format!("keystore_path = \"{}\"", DEFAULT_KEYSTORE_PATH),
        "",
        "[network]",
        &format!("listen_port = {}", DEFAULT_LISTEN_PORT),
        "peers = []",
        &format!("max_peers = {}", DEFAULT_MAX_PEERS),
        &format!("max_connections = {}", DEFAULT_MAX_CONNECTIONS),
        &format!("max_frame_size = {}", DEFAULT_MAX_FRAME_SIZE),
        &format!("sync_obj_chunk_bytes = {}", DEFAULT_SYNC_OBJ_CHUNK_BYTES),
        &format!("sync_obj_buffer_bytes = {}", DEFAULT_SYNC_OBJ_BUFFER_BYTES),
        &format!("connect_timeout_ms = {}", DEFAULT_CONNECT_TIMEOUT_MS),
        &format!("read_timeout_ms = {}", DEFAULT_READ_TIMEOUT_MS),
        &format!("write_timeout_ms = {}", DEFAULT_WRITE_TIMEOUT_MS),
        "",
        "[storage]",
        &format!("path = \"{}\"", DEFAULT_STORAGE_PATH),
        &format!("snapshot_interval = {}", DEFAULT_SNAPSHOT_INTERVAL),
        &format!("prune_pending_hours = {}", DEFAULT_PRUNE_PENDING_HOURS),
        "",
        "[storage.postgres]",
        &format!("url = \"{}\"", DEFAULT_STORAGE_POSTGRES_URL),
        &format!("schema = \"{}\"", DEFAULT_STORAGE_POSTGRES_SCHEMA),
        &format!("pool_max_size = {}", DEFAULT_STORAGE_POSTGRES_POOL_MAX_SIZE),
        &format!(
            "connect_timeout_ms = {}",
            DEFAULT_STORAGE_POSTGRES_CONNECT_TIMEOUT_MS
        ),
        &format!(
            "acquire_timeout_ms = {}",
            DEFAULT_STORAGE_POSTGRES_ACQUIRE_TIMEOUT_MS
        ),
        &format!(
            "statement_timeout_ms = {}",
            DEFAULT_STORAGE_POSTGRES_STATEMENT_TIMEOUT_MS
        ),
        &format!(
            "retry_max_attempts = {}",
            DEFAULT_STORAGE_POSTGRES_RETRY_MAX_ATTEMPTS
        ),
        &format!(
            "retry_backoff_ms = {}",
            DEFAULT_STORAGE_POSTGRES_RETRY_BACKOFF_MS
        ),
        "",
        "[storage.clickhouse]",
        &format!("enabled = {}", DEFAULT_STORAGE_CLICKHOUSE_ENABLED),
        &format!("url = \"{}\"", DEFAULT_STORAGE_CLICKHOUSE_URL),
        &format!("database = \"{}\"", DEFAULT_STORAGE_CLICKHOUSE_DATABASE),
        &format!(
            "table_prefix = \"{}\"",
            DEFAULT_STORAGE_CLICKHOUSE_TABLE_PREFIX
        ),
        &format!(
            "connect_timeout_ms = {}",
            DEFAULT_STORAGE_CLICKHOUSE_CONNECT_TIMEOUT_MS
        ),
        &format!(
            "retry_max_attempts = {}",
            DEFAULT_STORAGE_CLICKHOUSE_RETRY_MAX_ATTEMPTS
        ),
        &format!(
            "retry_backoff_ms = {}",
            DEFAULT_STORAGE_CLICKHOUSE_RETRY_BACKOFF_MS
        ),
        "",
        "[compiler]",
        &format!("out_dir = \"{}\"", DEFAULT_COMPILER_OUT_DIR),
        "",
        "[profile]",
        &format!("mode = \"{}\"", DEFAULT_PROFILE_MODE),
        "",
        "[registry]",
        &format!("url = \"{}\"", DEFAULT_REGISTRY_URL),
        "",
        "[registry.pins]",
        "",
        "[vm]",
        &format!("fuel = {}", DEFAULT_VM_FUEL),
        &format!("memory_bytes = {}", DEFAULT_VM_MEMORY_BYTES),
        "",
        "[vault]",
        "enabled = true",
        &format!("mode = \"{}\"", DEFAULT_VAULT_MODE),
        "driver = \"\"",
        &format!(
            "max_local_storage_mb = {}",
            DEFAULT_VAULT_MAX_LOCAL_STORAGE_MB
        ),
        &format!("disk_pressure_pct = {}", DEFAULT_VAULT_DISK_PRESSURE_PCT),
        &format!(
            "alert_threshold_pct = {}",
            DEFAULT_VAULT_ALERT_THRESHOLD_PCT
        ),
        &format!(
            "check_interval_secs = {}",
            DEFAULT_VAULT_CHECK_INTERVAL_SECS
        ),
        &format!(
            "alert_interval_secs = {}",
            DEFAULT_VAULT_ALERT_INTERVAL_SECS
        ),
        "checkpoint_schema = \"\"",
        "checkpoint_contract = \"\"",
        "",
        "[vault.local]",
        "path = \"\"",
        "",
        "[vault.peer]",
        "peer_id = \"\"",
        "root = \"\"",
        "",
        "[vault.s3]",
        "bucket = \"\"",
        "prefix = \"\"",
        "endpoint_url = \"\"",
        "region = \"\"",
        "force_path_style = false",
        "",
        "[vault.arweave]",
        "upload_url = \"\"",
        "gateway_url = \"\"",
        "token = \"\"",
        "arlocal = false",
        "",
    ]
    .join("\n")
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
enum ConfigValue {
    Str(String),
    Int(i64),
    Bool(bool),
    Array(Vec<ConfigValue>),
}

fn parse_pairs(contents: &str) -> Result<Vec<(String, ConfigValue)>, DharmaError> {
    let mut pairs = Vec::new();
    let mut section: Vec<String> = Vec::new();
    for raw_line in contents.lines() {
        let line = strip_comment(raw_line).trim().to_string();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            let inner = line.trim_matches(&['[', ']'][..]).trim();
            section = if inner.is_empty() {
                Vec::new()
            } else {
                inner.split('.').map(|s| s.trim().to_string()).collect()
            };
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let mut key = key.trim().to_string();
        if let Some(stripped) = key.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
            key = unescape(stripped);
        }
        let value = value.trim();
        let value = parse_value(value)?;
        let full_key = if section.is_empty() {
            key
        } else {
            format!("{}.{}", section.join("."), key)
        };
        pairs.push((full_key, value));
    }
    Ok(pairs)
}

fn strip_comment(line: &str) -> String {
    let mut out = String::new();
    let mut in_string = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                out.push(ch);
                in_string = !in_string;
            }
            '#' if !in_string => break,
            _ => out.push(ch),
        }
    }
    out
}

fn parse_value(raw: &str) -> Result<ConfigValue, DharmaError> {
    let raw = raw.trim();
    if raw.starts_with('[') && raw.ends_with(']') {
        return parse_array(raw);
    }
    if raw.eq_ignore_ascii_case("true") {
        return Ok(ConfigValue::Bool(true));
    }
    if raw.eq_ignore_ascii_case("false") {
        return Ok(ConfigValue::Bool(false));
    }
    if let Some(stripped) = raw.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        return Ok(ConfigValue::Str(unescape(stripped)));
    }
    if let Ok(num) = raw.parse::<i64>() {
        return Ok(ConfigValue::Int(num));
    }
    Err(DharmaError::Config(format!("invalid config value: {raw}")))
}

fn parse_array(raw: &str) -> Result<ConfigValue, DharmaError> {
    let inner = raw.trim_matches(&['[', ']'][..]).trim();
    if inner.is_empty() {
        return Ok(ConfigValue::Array(Vec::new()));
    }
    let mut items = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    for ch in inner.chars() {
        match ch {
            '"' => {
                current.push(ch);
                in_string = !in_string;
            }
            ',' if !in_string => {
                let item = current.trim();
                if !item.is_empty() {
                    items.push(parse_value(item)?);
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        items.push(parse_value(current.trim())?);
    }
    Ok(ConfigValue::Array(items))
}

fn unescape(value: &str) -> String {
    let mut out = String::new();
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(next) = chars.next() {
                match next {
                    '"' => out.push('"'),
                    '\\' => out.push('\\'),
                    'n' => out.push('\n'),
                    't' => out.push('\t'),
                    other => out.push(other),
                }
            }
        } else {
            out.push(ch);
        }
    }
    out
}

fn format_string_array(items: &[String]) -> String {
    items
        .iter()
        .map(|item| format!("\"{}\"", item.replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn parse_pairs_applies_sections() {
        let text = r#"
[network]
listen_port = 4040
max_connections = 77
sync_obj_chunk_bytes = 12345
sync_obj_buffer_bytes = 67890
peers = ["tcp://a:1", "tcp://b:2"]

[storage.postgres]
url = "postgres://test:test@127.0.0.1:5432/test"
schema = "dharma_test"
pool_max_size = 8
connect_timeout_ms = 1234
acquire_timeout_ms = 2345
statement_timeout_ms = 3456
retry_max_attempts = 4
retry_backoff_ms = 15

[storage.clickhouse]
enabled = true
url = "http://127.0.0.1:8123"
database = "analytics"
table_prefix = "analytics_v1"
connect_timeout_ms = 3456
retry_max_attempts = 7
retry_backoff_ms = 12

[compiler]
out_dir = "build/contracts"

[registry.pins]
"std.finance" = "1.2.0"
"#;
        let pairs = parse_pairs(text).unwrap();
        let mut cfg = Config::default();
        for (key, value) in pairs {
            cfg.apply_pair(&key, value).unwrap();
        }
        assert_eq!(cfg.network.listen_port, 4040);
        assert_eq!(cfg.network.max_connections, 77);
        assert_eq!(cfg.network.sync_obj_chunk_bytes, 12345);
        assert_eq!(cfg.network.sync_obj_buffer_bytes, 67890);
        assert_eq!(cfg.network.peers.len(), 2);
        assert_eq!(
            cfg.storage.postgres.url,
            "postgres://test:test@127.0.0.1:5432/test".to_string()
        );
        assert_eq!(cfg.storage.postgres.schema, "dharma_test".to_string());
        assert_eq!(cfg.storage.postgres.pool_max_size, 8);
        assert_eq!(cfg.storage.postgres.connect_timeout_ms, 1234);
        assert_eq!(cfg.storage.postgres.acquire_timeout_ms, 2345);
        assert_eq!(cfg.storage.postgres.statement_timeout_ms, 3456);
        assert_eq!(cfg.storage.postgres.retry_max_attempts, 4);
        assert_eq!(cfg.storage.postgres.retry_backoff_ms, 15);
        assert!(cfg.storage.clickhouse.enabled);
        assert_eq!(
            cfg.storage.clickhouse.url,
            "http://127.0.0.1:8123".to_string()
        );
        assert_eq!(cfg.storage.clickhouse.database, "analytics".to_string());
        assert_eq!(
            cfg.storage.clickhouse.table_prefix,
            "analytics_v1".to_string()
        );
        assert_eq!(cfg.storage.clickhouse.connect_timeout_ms, 3456);
        assert_eq!(cfg.storage.clickhouse.retry_max_attempts, 7);
        assert_eq!(cfg.storage.clickhouse.retry_backoff_ms, 12);
        assert_eq!(cfg.compiler.out_dir, "build/contracts");
        assert_eq!(
            cfg.registry.pins.get("std.finance").cloned(),
            Some("1.2.0".to_string())
        );
        let rendered = cfg.to_toml_string();
        assert!(rendered.contains("[network]"));
        assert!(rendered.contains("max_connections"));
        assert!(rendered.contains("max_frame_size"));
        assert!(rendered.contains("sync_obj_chunk_bytes"));
        assert!(rendered.contains("sync_obj_buffer_bytes"));
        assert!(rendered.contains("[storage.postgres]"));
        assert!(rendered.contains("[storage.clickhouse]"));
        assert!(rendered.contains("[compiler]"));
        assert!(rendered.contains("out_dir = \"build/contracts\""));
    }

    #[test]
    fn ensure_global_config_creates_file() {
        let temp = tempfile::tempdir().unwrap();
        env::set_var("DHARMA_CONFIG_PATH", temp.path().join("config.toml"));
        let path = ensure_global_config_file().unwrap();
        assert!(path.exists());
        let contents = fs::read_to_string(path).unwrap();
        assert!(contents.contains("[storage]"));
        env::remove_var("DHARMA_CONFIG_PATH");
    }

    #[test]
    fn max_connections_rejects_non_positive_value() {
        let mut cfg = Config::default();
        let err = cfg
            .apply_pair("network.max_connections", ConfigValue::Int(0))
            .unwrap_err();
        assert!(matches!(err, DharmaError::Config(_)));
    }

    #[test]
    fn max_connections_rejects_non_integer_value() {
        let mut cfg = Config::default();
        let err = cfg
            .apply_pair(
                "network.max_connections",
                ConfigValue::Str("invalid".to_string()),
            )
            .unwrap_err();
        assert!(matches!(err, DharmaError::Config(_)));
    }
}
