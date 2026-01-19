use dharma_core::env::{Env, Fs, MappedBytes};
use dharma_core::error::DharmaError;
use rand_chacha::ChaCha20Rng;
use rand_chacha::rand_core::{RngCore, SeedableRng};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub mod net;
pub mod scheduler;
pub mod timeline;

pub use net::{FaultConfig, NodeId, SimHub, SimNet, SimStream, SimStreamControl};
pub use scheduler::{SimScheduler, ScheduledEvent};
pub use timeline::{FaultEvent, FaultTimeline};

pub struct TraceSink {
    lines: Mutex<VecDeque<String>>,
    seq: AtomicU64,
    max: usize,
}

impl TraceSink {
    pub fn new(max: usize) -> Self {
        Self {
            lines: Mutex::new(VecDeque::new()),
            seq: AtomicU64::new(0),
            max,
        }
    }

    pub fn record(&self, line: String) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut lines) = self.lines.lock() {
            if lines.len() >= self.max {
                lines.pop_front();
            }
            lines.push_back(format!("{:08} {}", seq, line));
        }
    }

    pub fn snapshot(&self) -> Vec<String> {
        if let Ok(lines) = self.lines.lock() {
            return lines.iter().cloned().collect();
        }
        Vec::new()
    }
}

#[derive(Clone, Debug)]
pub struct ClockFaultConfig {
    pub drift_per_call: i64,
    pub jump_every: Option<u64>,
    pub jump_amount: i64,
    pub monotonic_violation_every: Option<u64>,
    pub monotonic_backstep: i64,
}

impl Default for ClockFaultConfig {
    fn default() -> Self {
        Self {
            drift_per_call: 0,
            jump_every: None,
            jump_amount: 0,
            monotonic_violation_every: None,
            monotonic_backstep: 0,
        }
    }
}

#[derive(Clone, Debug)]
struct ClockState {
    now: i64,
    calls: u64,
    last_returned: i64,
    faults: ClockFaultConfig,
}

impl ClockState {
    fn new(now: i64) -> Self {
        Self {
            now,
            calls: 0,
            last_returned: now,
            faults: ClockFaultConfig::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FsFaultConfig {
    pub enospc_after: Option<u64>,
    pub torn_every: Option<u64>,
    pub torn_max: usize,
    pub fsync_lie_every: Option<u64>,
    pub read_corrupt_every: Option<u64>,
    pub read_corrupt_bits: u8,
}

impl Default for FsFaultConfig {
    fn default() -> Self {
        Self {
            enospc_after: None,
            torn_every: None,
            torn_max: 0,
            fsync_lie_every: None,
            read_corrupt_every: None,
            read_corrupt_bits: 0,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct FsFaultState {
    config: FsFaultConfig,
    write_count: u64,
    bytes_written: u64,
    fsync_count: u64,
    read_count: u64,
}

#[derive(Clone, Debug)]
struct FaultPlan {
    write_len: usize,
    error: bool,
}

impl FaultPlan {
    fn ok(len: usize) -> Self {
        Self {
            write_len: len,
            error: false,
        }
    }
}

#[derive(Clone)]
pub struct SimEnv {
    rng: ChaCha20Rng,
    root: PathBuf,
    fs: Arc<SimFs>,
    clock: Arc<Mutex<ClockState>>,
    trace: Option<Arc<TraceSink>>,
}

impl SimEnv {
    pub fn new(seed: u64, start: i64) -> Self {
        Self::with_root(seed, start, PathBuf::from("sim"))
    }

    pub fn with_root<P: Into<PathBuf>>(seed: u64, start: i64, root: P) -> Self {
        let root = root.into();
        let fs = Arc::new(SimFs::new());
        let _ = fs.create_dir_all(&root);
        let clock = Arc::new(Mutex::new(ClockState::new(start)));
        Self {
            rng: ChaCha20Rng::seed_from_u64(seed),
            root,
            fs,
            clock,
            trace: None,
        }
    }

    pub fn enable_trace(&mut self, trace: Arc<TraceSink>) {
        self.trace = Some(trace);
    }

    fn trace_line(&self, line: String) {
        if let Some(trace) = &self.trace {
            trace.record(line);
        }
    }

    pub fn advance(&mut self, seconds: i64) {
        if let Ok(mut clock) = self.clock.lock() {
            clock.now = clock.now.saturating_add(seconds);
        }
    }

    pub fn set_fs_faults(&self, faults: FsFaultConfig) {
        self.fs.set_faults(faults);
    }

    pub fn set_clock_faults(&self, faults: ClockFaultConfig) {
        if let Ok(mut clock) = self.clock.lock() {
            clock.faults = faults;
            clock.calls = 0;
            clock.last_returned = clock.now;
        }
    }
}

impl Env for SimEnv {
    fn now(&self) -> i64 {
        let mut clock = self
            .clock
            .lock()
            .expect("clock lock poisoned");
        clock.calls = clock.calls.wrapping_add(1);
        let mut t = clock
            .now
            .saturating_add(clock.faults.drift_per_call.saturating_mul(clock.calls as i64));
        if let Some(freq) = clock.faults.jump_every {
            if freq > 0 && clock.calls % freq == 0 {
                t = t.saturating_add(clock.faults.jump_amount);
            }
        }
        if let Some(freq) = clock.faults.monotonic_violation_every {
            if freq > 0 && clock.calls % freq == 0 && clock.faults.monotonic_backstep != 0 {
                let back = clock.faults.monotonic_backstep.abs();
                t = clock.last_returned.saturating_sub(back);
            }
        }
        clock.last_returned = t;
        t
    }

    fn random_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    fn root(&self) -> &Path {
        &self.root
    }
}

impl Fs for SimEnv {
    fn read(&self, path: &Path) -> Result<Vec<u8>, DharmaError> {
        let result = self.fs.read(path);
        match &result {
            Ok(bytes) => {
                self.trace_line(format!("fs.read path={} len={}", path.display(), bytes.len()));
            }
            Err(err) => {
                self.trace_line(format!("fs.read path={} err={}", path.display(), err));
            }
        }
        result
    }

    fn read_mmap(&self, path: &Path) -> Result<MappedBytes, DharmaError> {
        match self.fs.read(path) {
            Ok(bytes) => {
                self.trace_line(format!(
                    "fs.read_mmap path={} len={}",
                    path.display(),
                    bytes.len()
                ));
                Ok(MappedBytes::Owned(bytes))
            }
            Err(err) => {
                self.trace_line(format!("fs.read_mmap path={} err={}", path.display(), err));
                Err(err)
            }
        }
    }

    fn file_len(&self, path: &Path) -> Result<u64, DharmaError> {
        let result = self.fs.file_len(path);
        match &result {
            Ok(len) => {
                self.trace_line(format!("fs.file_len path={} len={}", path.display(), len));
            }
            Err(err) => {
                self.trace_line(format!("fs.file_len path={} err={}", path.display(), err));
            }
        }
        result
    }

    fn write(&self, path: &Path, data: &[u8]) -> Result<(), DharmaError> {
        let result = self.fs.write(path, data);
        match &result {
            Ok(()) => {
                self.trace_line(format!(
                    "fs.write path={} len={} ok",
                    path.display(),
                    data.len()
                ));
            }
            Err(err) => {
                self.trace_line(format!(
                    "fs.write path={} len={} err={}",
                    path.display(),
                    data.len(),
                    err
                ));
            }
        }
        result
    }

    fn append(&self, path: &Path, data: &[u8]) -> Result<(), DharmaError> {
        let result = self.fs.append(path, data);
        match &result {
            Ok(()) => {
                self.trace_line(format!(
                    "fs.append path={} len={} ok",
                    path.display(),
                    data.len()
                ));
            }
            Err(err) => {
                self.trace_line(format!(
                    "fs.append path={} len={} err={}",
                    path.display(),
                    data.len(),
                    err
                ));
            }
        }
        result
    }

    fn remove_dir_all(&self, path: &Path) -> Result<(), DharmaError> {
        let result = self.fs.remove_dir_all(path);
        match &result {
            Ok(()) => self.trace_line(format!("fs.remove_dir_all path={} ok", path.display())),
            Err(err) => self.trace_line(format!("fs.remove_dir_all path={} err={}", path.display(), err)),
        }
        result
    }

    fn remove_file(&self, path: &Path) -> Result<(), DharmaError> {
        let result = self.fs.remove_file(path);
        match &result {
            Ok(()) => self.trace_line(format!("fs.remove_file path={} ok", path.display())),
            Err(err) => self.trace_line(format!("fs.remove_file path={} err={}", path.display(), err)),
        }
        result
    }

    fn fsync(&self, path: &Path) -> Result<(), DharmaError> {
        let result = self.fs.fsync(path);
        match &result {
            Ok(()) => self.trace_line(format!("fs.fsync path={} ok", path.display())),
            Err(err) => self.trace_line(format!("fs.fsync path={} err={}", path.display(), err)),
        }
        result
    }

    fn exists(&self, path: &Path) -> bool {
        let exists = self.fs.exists(path);
        self.trace_line(format!("fs.exists path={} {}", path.display(), exists));
        exists
    }

    fn is_dir(&self, path: &Path) -> bool {
        let is_dir = self.fs.is_dir(path);
        self.trace_line(format!("fs.is_dir path={} {}", path.display(), is_dir));
        is_dir
    }

    fn is_file(&self, path: &Path) -> bool {
        let is_file = self.fs.is_file(path);
        self.trace_line(format!("fs.is_file path={} {}", path.display(), is_file));
        is_file
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), DharmaError> {
        let result = self.fs.create_dir_all(path);
        match &result {
            Ok(()) => self.trace_line(format!("fs.create_dir_all path={} ok", path.display())),
            Err(err) => self.trace_line(format!("fs.create_dir_all path={} err={}", path.display(), err)),
        }
        result
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>, DharmaError> {
        let result = self.fs.list_dir(path);
        match &result {
            Ok(entries) => {
                self.trace_line(format!(
                    "fs.list_dir path={} count={}",
                    path.display(),
                    entries.len()
                ));
            }
            Err(err) => {
                self.trace_line(format!("fs.list_dir path={} err={}", path.display(), err));
            }
        }
        result
    }
}

#[derive(Default)]
pub struct SimFs {
    files: Mutex<HashMap<PathBuf, Vec<u8>>>,
    dirs: Mutex<HashSet<PathBuf>>,
    faults: Mutex<FsFaultState>,
}

impl SimFs {
    pub fn new() -> Self {
        Self {
            files: Mutex::new(HashMap::new()),
            dirs: Mutex::new(HashSet::new()),
            faults: Mutex::new(FsFaultState::default()),
        }
    }

    pub fn set_faults(&self, faults: FsFaultConfig) {
        if let Ok(mut state) = self.faults.lock() {
            state.config = faults;
            state.write_count = 0;
            state.bytes_written = 0;
            state.fsync_count = 0;
            state.read_count = 0;
        }
    }

    fn fault_plan(&self, len: usize) -> Result<FaultPlan, DharmaError> {
        if len == 0 {
            return Ok(FaultPlan::ok(0));
        }
        let mut state = self
            .faults
            .lock()
            .map_err(|_| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fs lock poisoned")))?;
        if let Some(limit) = state.config.enospc_after {
            if state.bytes_written.saturating_add(len as u64) > limit {
                return Err(DharmaError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "ENOSPC",
                )));
            }
        }
        state.write_count = state.write_count.wrapping_add(1);
        if let Some(freq) = state.config.torn_every {
            if freq > 0 && state.write_count % freq == 0 {
                let max = state.config.torn_max.min(len.saturating_sub(1));
                let partial = if max == 0 { 0 } else { max };
                state.bytes_written = state.bytes_written.saturating_add(partial as u64);
                return Ok(FaultPlan {
                    write_len: partial,
                    error: true,
                });
            }
        }
        state.bytes_written = state.bytes_written.saturating_add(len as u64);
        Ok(FaultPlan::ok(len))
    }

    fn insert_dir_chain(&self, path: &Path) -> Result<(), DharmaError> {
        let mut dirs = self
            .dirs
            .lock()
            .map_err(|_| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fs lock poisoned")))?;
        for ancestor in path.ancestors() {
            if ancestor.as_os_str().is_empty() {
                continue;
            }
            dirs.insert(ancestor.to_path_buf());
        }
        Ok(())
    }
}

impl Fs for SimFs {
    fn read(&self, path: &Path) -> Result<Vec<u8>, DharmaError> {
        let data = {
            let files = self
                .files
                .lock()
                .map_err(|_| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fs lock poisoned")))?;
            files
                .get(path)
                .cloned()
                .ok_or_else(|| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "file not found")))?
        };
        let mut data = data;
        if let Ok(mut state) = self.faults.lock() {
            state.read_count = state.read_count.wrapping_add(1);
            if let Some(freq) = state.config.read_corrupt_every {
                if freq > 0 && state.read_count % freq == 0 {
                    let mask = state.config.read_corrupt_bits;
                    if mask != 0 {
                        for byte in &mut data {
                            *byte ^= mask;
                        }
                    }
                }
            }
        }
        Ok(data)
    }

    fn read_mmap(&self, path: &Path) -> Result<MappedBytes, DharmaError> {
        Ok(MappedBytes::Owned(self.read(path)?))
    }

    fn file_len(&self, path: &Path) -> Result<u64, DharmaError> {
        let files = self
            .files
            .lock()
            .map_err(|_| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fs lock poisoned")))?;
        let len = files
            .get(path)
            .map(|data| data.len() as u64)
            .ok_or_else(|| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "file not found")))?;
        Ok(len)
    }

    fn write(&self, path: &Path, data: &[u8]) -> Result<(), DharmaError> {
        if let Some(parent) = path.parent() {
            self.insert_dir_chain(parent)?;
        }
        let plan = self.fault_plan(data.len())?;
        let mut files = self
            .files
            .lock()
            .map_err(|_| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fs lock poisoned")))?;
        let bytes = data.get(..plan.write_len).unwrap_or(&[]);
        files.insert(path.to_path_buf(), bytes.to_vec());
        if plan.error {
            return Err(DharmaError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "torn write",
            )));
        }
        Ok(())
    }

    fn append(&self, path: &Path, data: &[u8]) -> Result<(), DharmaError> {
        if let Some(parent) = path.parent() {
            self.insert_dir_chain(parent)?;
        }
        let plan = self.fault_plan(data.len())?;
        let mut files = self
            .files
            .lock()
            .map_err(|_| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fs lock poisoned")))?;
        let bytes = data.get(..plan.write_len).unwrap_or(&[]);
        files.entry(path.to_path_buf()).or_default().extend_from_slice(bytes);
        if plan.error {
            return Err(DharmaError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "torn write",
            )));
        }
        Ok(())
    }

    fn remove_dir_all(&self, path: &Path) -> Result<(), DharmaError> {
        let mut files = self
            .files
            .lock()
            .map_err(|_| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fs lock poisoned")))?;
        files.retain(|p, _| !p.starts_with(path));
        let mut dirs = self
            .dirs
            .lock()
            .map_err(|_| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fs lock poisoned")))?;
        dirs.retain(|p| !p.starts_with(path));
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> Result<(), DharmaError> {
        let mut files = self
            .files
            .lock()
            .map_err(|_| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fs lock poisoned")))?;
        files.remove(path);
        Ok(())
    }

    fn fsync(&self, _path: &Path) -> Result<(), DharmaError> {
        if let Ok(mut state) = self.faults.lock() {
            state.fsync_count = state.fsync_count.wrapping_add(1);
            if let Some(freq) = state.config.fsync_lie_every {
                if freq > 0 && state.fsync_count % freq == 0 {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    fn exists(&self, path: &Path) -> bool {
        let files = self.files.lock();
        let dirs = self.dirs.lock();
        match (files, dirs) {
            (Ok(files), Ok(dirs)) => files.contains_key(path) || dirs.contains(path),
            _ => false,
        }
    }

    fn is_dir(&self, path: &Path) -> bool {
        self.dirs
            .lock()
            .map(|dirs| dirs.contains(path))
            .unwrap_or(false)
    }

    fn is_file(&self, path: &Path) -> bool {
        self.files
            .lock()
            .map(|files| files.contains_key(path))
            .unwrap_or(false)
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), DharmaError> {
        self.insert_dir_chain(path)
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>, DharmaError> {
        let mut out = HashSet::new();
        let files = self
            .files
            .lock()
            .map_err(|_| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fs lock poisoned")))?;
        for file in files.keys() {
            if let Some(parent) = file.parent() {
                if parent == path {
                    out.insert(file.to_path_buf());
                }
            }
        }
        let dirs = self
            .dirs
            .lock()
            .map_err(|_| DharmaError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fs lock poisoned")))?;
        for dir in dirs.iter() {
            if let Some(parent) = dir.parent() {
                if parent == path {
                    out.insert(dir.to_path_buf());
                }
            }
        }
        Ok(out.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sim_env_is_deterministic() {
        let mut a = SimEnv::new(42, 100);
        let mut b = SimEnv::new(42, 100);
        assert_eq!(a.now(), b.now());
        assert_eq!(a.random_u64(), b.random_u64());
        a.advance(5);
        b.advance(5);
        assert_eq!(a.now(), b.now());
    }

    #[test]
    fn sim_clock_drift_and_jump() {
        let env = SimEnv::new(1, 100);
        env.set_clock_faults(ClockFaultConfig {
            drift_per_call: 1,
            jump_every: Some(2),
            jump_amount: 10,
            monotonic_violation_every: None,
            monotonic_backstep: 0,
        });
        let t1 = env.now();
        let t2 = env.now();
        assert_eq!(t1, 101);
        assert_eq!(t2, 112);
    }

    #[test]
    fn sim_clock_monotonic_violation() {
        let env = SimEnv::new(1, 200);
        env.set_clock_faults(ClockFaultConfig {
            drift_per_call: 0,
            jump_every: None,
            jump_amount: 0,
            monotonic_violation_every: Some(2),
            monotonic_backstep: 5,
        });
        let t1 = env.now();
        let t2 = env.now();
        assert_eq!(t1, 200);
        assert_eq!(t2, 195);
    }

    #[test]
    fn simfs_enospc_blocks_write() {
        let fs = SimFs::new();
        fs.set_faults(FsFaultConfig {
            enospc_after: Some(2),
            ..FsFaultConfig::default()
        });
        let path = PathBuf::from("data.bin");
        assert!(fs.write(&path, b"abcd").is_err());
        assert!(!fs.exists(&path));
    }

    #[test]
    fn simfs_torn_write_persists_partial() {
        let fs = SimFs::new();
        fs.set_faults(FsFaultConfig {
            torn_every: Some(1),
            torn_max: 2,
            ..FsFaultConfig::default()
        });
        let path = PathBuf::from("data.bin");
        assert!(fs.write(&path, b"abcdef").is_err());
        let data = fs.read(&path).unwrap();
        assert!(data.len() > 0);
        assert!(data.len() < 6);
    }

    #[test]
    fn simfs_read_corrupts_on_schedule() {
        let fs = SimFs::new();
        fs.set_faults(FsFaultConfig {
            read_corrupt_every: Some(2),
            read_corrupt_bits: 0x01,
            ..FsFaultConfig::default()
        });
        let path = PathBuf::from("data.bin");
        fs.write(&path, b"\x10\x20\x30").unwrap();
        let first = fs.read(&path).unwrap();
        assert_eq!(first, b"\x10\x20\x30");
        let second = fs.read(&path).unwrap();
        assert_eq!(second, b"\x11\x21\x31");
    }

    #[test]
    fn simfs_fsync_lie_is_ok() {
        let fs = SimFs::new();
        fs.set_faults(FsFaultConfig {
            fsync_lie_every: Some(1),
            ..FsFaultConfig::default()
        });
        let path = PathBuf::from("data.bin");
        fs.write(&path, b"abcd").unwrap();
        assert!(fs.fsync(&path).is_ok());
    }
}
