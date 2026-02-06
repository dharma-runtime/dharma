use crate::error::DharmaError;
use crate::types::SubjectId;
use crate::vault::{DhboxChunk, VaultDriver, VaultLocation, VaultMeta};

#[cfg(feature = "vault-arweave")]
use reqwest::blocking::Client;
#[cfg(feature = "vault-arweave")]
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
#[cfg(feature = "vault-arweave")]
use serde_json::Value as JsonValue;
#[cfg(feature = "vault-arweave")]
use std::process::{Command, Stdio};
#[cfg(feature = "vault-arweave")]
use std::path::{Path, PathBuf};
#[cfg(feature = "vault-arweave")]
use std::sync::Mutex;
#[cfg(feature = "vault-arweave")]
use std::{env, fs};
#[cfg(feature = "vault-arweave")]
use rand_core::{OsRng, RngCore};

#[cfg(feature = "vault-arweave")]
#[derive(Clone, Debug)]
pub enum ArweaveMode {
    Bundlr,
    Arlocal,
}

#[cfg(feature = "vault-arweave")]
#[derive(Debug)]
pub struct ArweaveDriver {
    upload_url: String,
    gateway_url: String,
    token: Option<String>,
    client: Client,
    mode: ArweaveMode,
    wallet_path: Mutex<Option<PathBuf>>,
}

#[cfg(not(feature = "vault-arweave"))]
#[derive(Clone, Debug)]
pub struct ArweaveDriver;

impl ArweaveDriver {
    #[cfg(feature = "vault-arweave")]
    pub fn new(
        upload_url: impl Into<String>,
        gateway_url: impl Into<String>,
        token: Option<String>,
    ) -> Result<Self, DharmaError> {
        Ok(Self {
            upload_url: upload_url.into(),
            gateway_url: gateway_url.into(),
            token,
            client: Client::new(),
            mode: ArweaveMode::Bundlr,
            wallet_path: Mutex::new(None),
        })
    }

    #[cfg(feature = "vault-arweave")]
    pub fn new_arlocal(endpoint: impl Into<String>) -> Result<Self, DharmaError> {
        let endpoint = endpoint.into();
        Ok(Self {
            upload_url: endpoint.clone(),
            gateway_url: endpoint,
            token: None,
            client: Client::new(),
            mode: ArweaveMode::Arlocal,
            wallet_path: Mutex::new(None),
        })
    }

    #[cfg(not(feature = "vault-arweave"))]
    pub fn new(
        _upload_url: impl Into<String>,
        _gateway_url: impl Into<String>,
        _token: Option<String>,
    ) -> Result<Self, DharmaError> {
        Err(DharmaError::Config(
            "vault-arweave feature not enabled".to_string(),
        ))
    }
}

#[cfg(feature = "vault-arweave")]
impl ArweaveDriver {
    fn with_auth(&self, headers: &mut HeaderMap) -> Result<(), DharmaError> {
        if let Some(token) = &self.token {
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| DharmaError::Validation("invalid arweave token".to_string()))?;
            headers.insert(AUTHORIZATION, value);
        }
        Ok(())
    }

    fn extract_location(&self, headers: &HeaderMap, body: &str) -> Result<String, DharmaError> {
        for key in ["x-irys-id", "x-bundlr-id", "x-transaction-id", "x-arweave-tx"] {
            if let Some(value) = headers.get(key) {
                if let Ok(text) = value.to_str() {
                    if !text.trim().is_empty() {
                        return Ok(text.trim().to_string());
                    }
                }
            }
        }
        let trimmed = body.trim();
        if trimmed.is_empty() {
            return Err(DharmaError::Network("empty arweave response".to_string()));
        }
        if trimmed.starts_with('{') {
            if let Ok(json) = serde_json::from_str::<JsonValue>(trimmed) {
                for key in ["id", "tx_id", "transaction_id"] {
                    if let Some(value) = json.get(key) {
                        if let Some(text) = value.as_str() {
                            if !text.trim().is_empty() {
                                return Ok(text.trim().to_string());
                            }
                        }
                    }
                }
            }
        }
        Ok(trimmed.to_string())
    }

    fn download_url(&self, location: &str) -> String {
        let base = self.gateway_url.trim_end_matches('/');
        format!("{base}/{location}")
    }

    fn arlocal_endpoint(&self) -> &str {
        self.gateway_url.as_str()
    }

    fn ensure_wallet_path(&self) -> Result<PathBuf, DharmaError> {
        if let Ok(path) = env::var("ARLOCAL_WALLET") {
            let path = PathBuf::from(path);
            if path.exists() {
                return Ok(path);
            }
        }
        let mut guard = self.wallet_path.lock().map_err(|_| {
            DharmaError::Config("wallet lock poisoned".to_string())
        })?;
        if let Some(path) = guard.as_ref() {
            if path.exists() {
                return Ok(path.clone());
            }
        }
        let path = temp_path("dharma-arlocal-wallet", "json")?;
        run_arweave_script(
            self.arlocal_endpoint(),
            &path,
            None,
            ARLOCAL_WALLET_SCRIPT,
        )?;
        *guard = Some(path.clone());
        Ok(path)
    }

    fn put_chunk_arlocal(&self, chunk: &DhboxChunk) -> Result<VaultLocation, DharmaError> {
        let wallet_path = self.ensure_wallet_path()?;
        let chunk_path = temp_path("dharma-arlocal-chunk", "dhbox")?;
        fs::write(&chunk_path, chunk.to_bytes())?;
        let tx_id = run_arweave_script(
            self.arlocal_endpoint(),
            &wallet_path,
            Some(&chunk_path),
            ARLOCAL_UPLOAD_SCRIPT,
        )?;
        let _ = fs::remove_file(&chunk_path);
        Ok(VaultLocation {
            driver: "arweave".to_string(),
            path: tx_id.trim().to_string(),
        })
    }
}

#[cfg(feature = "vault-arweave")]
impl VaultDriver for ArweaveDriver {
    fn put_chunk(&self, chunk: &DhboxChunk) -> Result<VaultLocation, DharmaError> {
        match self.mode {
            ArweaveMode::Bundlr => {
                let bytes = chunk.to_bytes();
                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));
                self.with_auth(&mut headers)?;
                let resp = self
                    .client
                    .post(&self.upload_url)
                    .headers(headers)
                    .body(bytes)
                    .send()
                    .map_err(|e| DharmaError::Network(format!("arweave upload failed: {e}")))?;
                let status = resp.status();
                let headers = resp.headers().clone();
                let body = resp
                    .text()
                    .map_err(|e| DharmaError::Network(format!("arweave response read failed: {e}")))?;
                if !status.is_success() {
                    return Err(DharmaError::Network(format!(
                        "arweave upload failed: {status} {body}"
                    )));
                }
                let tx_id = self.extract_location(&headers, &body)?;
                Ok(VaultLocation {
                    driver: "arweave".to_string(),
                    path: tx_id,
                })
            }
            ArweaveMode::Arlocal => self.put_chunk_arlocal(chunk),
        }
    }

    fn get_chunk(&self, location: &VaultLocation) -> Result<Vec<u8>, DharmaError> {
        if location.driver != "arweave" {
            return Err(DharmaError::Validation("invalid driver for arweave".to_string()));
        }
        let url = self.download_url(&location.path);
        let resp = self
            .client
            .get(url)
            .send()
            .map_err(|e| DharmaError::Network(format!("arweave download failed: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(DharmaError::Network(format!(
                "arweave download failed: {status}"
            )));
        }
        resp.bytes()
            .map(|b| b.to_vec())
            .map_err(|e| DharmaError::Network(format!("arweave read failed: {e}")))
    }

    fn head_chunk(&self, location: &VaultLocation) -> Result<VaultMeta, DharmaError> {
        let bytes = self.get_chunk(location)?;
        let chunk = DhboxChunk::from_bytes(&bytes)?;
        Ok(VaultMeta {
            size: bytes.len() as u64,
            hash: chunk.ciphertext_hash(),
        })
    }

    fn list_chunks(&self, _subject: &SubjectId) -> Result<Vec<VaultLocation>, DharmaError> {
        Err(DharmaError::Config(
            "arweave does not support listing without index".to_string(),
        ))
    }
}

#[cfg(not(feature = "vault-arweave"))]
impl VaultDriver for ArweaveDriver {
    fn put_chunk(&self, _chunk: &DhboxChunk) -> Result<VaultLocation, DharmaError> {
        Err(DharmaError::Config(
            "vault-arweave feature not enabled".to_string(),
        ))
    }

    fn get_chunk(&self, _location: &VaultLocation) -> Result<Vec<u8>, DharmaError> {
        Err(DharmaError::Config(
            "vault-arweave feature not enabled".to_string(),
        ))
    }

    fn head_chunk(&self, _location: &VaultLocation) -> Result<VaultMeta, DharmaError> {
        Err(DharmaError::Config(
            "vault-arweave feature not enabled".to_string(),
        ))
    }

    fn list_chunks(&self, _subject: &SubjectId) -> Result<Vec<VaultLocation>, DharmaError> {
        Err(DharmaError::Config(
            "vault-arweave feature not enabled".to_string(),
        ))
    }
}

#[cfg(feature = "vault-arweave")]
const ARLOCAL_WALLET_SCRIPT: &str = r#"
const fs = require('fs');
const path = require('path');
const os = require('os');
function loadArweave() {
  try {
    return require('arweave');
  } catch (err) {
    const cache = process.env.npm_config_cache || path.join(os.homedir(), '.npm');
    const npxRoot = path.join(cache, '_npx');
    if (fs.existsSync(npxRoot)) {
      const dirs = fs.readdirSync(npxRoot);
      dirs.sort((a, b) => {
        const aStat = fs.statSync(path.join(npxRoot, a)).mtimeMs;
        const bStat = fs.statSync(path.join(npxRoot, b)).mtimeMs;
        return bStat - aStat;
      });
      for (const dir of dirs) {
        const candidate = path.join(npxRoot, dir, 'node_modules', 'arweave');
        if (fs.existsSync(candidate)) {
          return require(candidate);
        }
      }
    }
    throw err;
  }
}
const Arweave = loadArweave();
(async () => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  async function waitReady(arweave) {
    for (let attempt = 0; attempt < 30; attempt += 1) {
      try {
        const res = await arweave.api.get('/info');
        if (res.status === 200) {
          return;
        }
      } catch (_) {}
      await sleep(250);
    }
  }
  async function mintAndConfirm(arweave, address) {
    const base = (process.env.ARLOCAL_ENDPOINT || 'http://127.0.0.1:1984').replace(/\/$/, '');
    const target = 1000000000000000;
    const last = { mint: null, mine: null, balance: null, create: null, patch: null };
    for (let attempt = 0; attempt < 30; attempt += 1) {
      try {
        const res = await fetch(base + '/mint/' + address + '/' + target);
        last.mint = { status: res.status };
      } catch (err) { last.mint = { error: err && err.message ? err.message : String(err) }; }
      try {
        const res = await fetch(base + '/mine');
        last.mine = { status: res.status };
      } catch (err) { last.mine = { error: err && err.message ? err.message : String(err) }; }
      try {
        const res = await fetch(base + '/wallet/' + address + '/balance');
        if (res.ok) {
          const text = (await res.text()).trim();
          if (text !== "0" && text !== "") {
            return text;
          }
        }
        last.balance = { status: res.status, text: res.ok ? (await res.text()).trim() : null };
      } catch (err) { last.balance = { error: err && err.message ? err.message : String(err) }; }
      try {
        const res = await fetch(base + '/wallet', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ address, balance: target }),
        });
        last.create = { status: res.status };
      } catch (err) { last.create = { error: err && err.message ? err.message : String(err) }; }
      try {
        const res = await fetch(base + '/wallet/' + address + '/balance', {
          method: 'PATCH',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ balance: target }),
        });
        last.patch = { status: res.status };
      } catch (err) { last.patch = { error: err && err.message ? err.message : String(err) }; }
      await sleep(250);
    }
    throw new Error('arlocal mint failed: ' + JSON.stringify(last));
  }
  const endpoint = process.env.ARLOCAL_ENDPOINT || 'http://127.0.0.1:1984';
  const url = new URL(endpoint);
  const arweave = Arweave.init({ host: url.hostname, port: url.port, protocol: url.protocol.replace(':','') });
  await waitReady(arweave);
  const walletPath = process.env.ARWEAVE_WALLET;
  const wallet = await arweave.wallets.generate();
  fs.writeFileSync(walletPath, JSON.stringify(wallet));
  const address = await arweave.wallets.jwkToAddress(wallet);
  await mintAndConfirm(arweave, address);
  console.log(address);
})().catch(err => { console.error(err); process.exit(1); });
"#;

#[cfg(feature = "vault-arweave")]
const ARLOCAL_UPLOAD_SCRIPT: &str = r#"
const fs = require('fs');
const path = require('path');
const os = require('os');
function loadArweave() {
  try {
    return require('arweave');
  } catch (err) {
    const cache = process.env.npm_config_cache || path.join(os.homedir(), '.npm');
    const npxRoot = path.join(cache, '_npx');
    if (fs.existsSync(npxRoot)) {
      const dirs = fs.readdirSync(npxRoot);
      dirs.sort((a, b) => {
        const aStat = fs.statSync(path.join(npxRoot, a)).mtimeMs;
        const bStat = fs.statSync(path.join(npxRoot, b)).mtimeMs;
        return bStat - aStat;
      });
      for (const dir of dirs) {
        const candidate = path.join(npxRoot, dir, 'node_modules', 'arweave');
        if (fs.existsSync(candidate)) {
          return require(candidate);
        }
      }
    }
    throw err;
  }
}
const Arweave = loadArweave();
(async () => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  async function waitReady(arweave) {
    for (let attempt = 0; attempt < 30; attempt += 1) {
      try {
        const res = await arweave.api.get('/info');
        if (res.status === 200) {
          return;
        }
      } catch (_) {}
      await sleep(250);
    }
  }
  async function mintAndConfirm(arweave, address) {
    const base = (process.env.ARLOCAL_ENDPOINT || 'http://127.0.0.1:1984').replace(/\/$/, '');
    const target = 1000000000000000;
    const last = { mint: null, mine: null, balance: null, create: null, patch: null };
    for (let attempt = 0; attempt < 30; attempt += 1) {
      try {
        const res = await fetch(base + '/mint/' + address + '/' + target);
        last.mint = { status: res.status };
      } catch (err) { last.mint = { error: err && err.message ? err.message : String(err) }; }
      try {
        const res = await fetch(base + '/mine');
        last.mine = { status: res.status };
      } catch (err) { last.mine = { error: err && err.message ? err.message : String(err) }; }
      try {
        const res = await fetch(base + '/wallet/' + address + '/balance');
        if (res.ok) {
          const text = (await res.text()).trim();
          if (text !== "0" && text !== "") {
            return text;
          }
        }
        last.balance = { status: res.status, text: res.ok ? (await res.text()).trim() : null };
      } catch (err) { last.balance = { error: err && err.message ? err.message : String(err) }; }
      try {
        const res = await fetch(base + '/wallet', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ address, balance: target }),
        });
        last.create = { status: res.status };
      } catch (err) { last.create = { error: err && err.message ? err.message : String(err) }; }
      try {
        const res = await fetch(base + '/wallet/' + address + '/balance', {
          method: 'PATCH',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ balance: target }),
        });
        last.patch = { status: res.status };
      } catch (err) { last.patch = { error: err && err.message ? err.message : String(err) }; }
      await sleep(250);
    }
    throw new Error('arlocal mint failed: ' + JSON.stringify(last));
  }
  async function postTxWithRetry(arweave, tx) {
    let last = null;
    for (let attempt = 0; attempt < 5; attempt += 1) {
      try {
        const res = await arweave.transactions.post(tx);
        if (res.status === 200 || res.status === 202) {
          return res;
        }
        last = res;
      } catch (err) {
        last = { status: 0, data: err && err.message ? err.message : err };
      }
      await sleep(250);
    }
    return last;
  }
  const endpoint = process.env.ARLOCAL_ENDPOINT || 'http://127.0.0.1:1984';
  const url = new URL(endpoint);
  const arweave = Arweave.init({ host: url.hostname, port: url.port, protocol: url.protocol.replace(':','') });
  await waitReady(arweave);
  const walletPath = process.env.ARWEAVE_WALLET;
  const chunkPath = process.env.ARWEAVE_CHUNK;
  const wallet = JSON.parse(fs.readFileSync(walletPath));
  const address = await arweave.wallets.jwkToAddress(wallet);
  await mintAndConfirm(arweave, address);
  const data = fs.readFileSync(chunkPath);
  const tx = await arweave.createTransaction({ data }, wallet);
  await arweave.transactions.sign(tx, wallet);
  const res = await postTxWithRetry(arweave, tx);
  if (!(res && (res.status === 200 || res.status === 202))) {
    console.error('POST failed', res.status, res.data);
    process.exit(2);
  }
  await arweave.api.get('/mine');
  console.log(tx.id);
})().catch(err => { console.error(err); process.exit(1); });
"#;

#[cfg(feature = "vault-arweave")]
fn run_arweave_script(
    endpoint: &str,
    wallet_path: &Path,
    chunk_path: Option<&Path>,
    script: &str,
) -> Result<String, DharmaError> {
    let script_path = temp_path("dharma-arlocal-script", "js")?;
    fs::write(&script_path, script)?;
    let cmd = arweave_cmd()?;
    let mut command = Command::new(&cmd);
    command
        .arg("--yes")
        .arg("-p")
        .arg("arweave")
        .arg("-c")
        .arg(format!("node {}", shell_escape(&script_path)))
        .env("ARLOCAL_ENDPOINT", endpoint)
        .env("ARWEAVE_WALLET", wallet_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(chunk) = chunk_path {
        command.env("ARWEAVE_CHUNK", chunk);
    }
    let output = command.output().map_err(|e| {
        DharmaError::Config(format!("arweave helper failed: {e}"))
    })?;
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(DharmaError::Network(format!(
            "arweave helper failed: {stdout} {stderr}"
        )));
    }
    let _ = fs::remove_file(&script_path);
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(feature = "vault-arweave")]
fn arweave_cmd() -> Result<&'static str, DharmaError> {
    if command_exists("npx") {
        return Ok("npx");
    }
    Err(DharmaError::Config(
        "npx required for arlocal (install npm/node)".to_string(),
    ))
}

#[cfg(feature = "vault-arweave")]
fn command_exists(cmd: &str) -> bool {
    Command::new(cmd)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

#[cfg(feature = "vault-arweave")]
fn temp_path(prefix: &str, ext: &str) -> Result<PathBuf, DharmaError> {
    let mut buf = [0u8; 12];
    let mut rng = OsRng;
    rng.fill_bytes(&mut buf);
    let name = format!("{prefix}-{}.{}", hex::encode(buf), ext);
    Ok(env::temp_dir().join(name))
}

#[cfg(feature = "vault-arweave")]
fn shell_escape(path: &Path) -> String {
    let text = path.to_string_lossy();
    format!("\"{}\"", text.replace('\"', "\\\""))
}
