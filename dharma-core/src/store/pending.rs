use crate::env::Env;
use crate::error::DharmaError;
use crate::types::AssertionId;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const ENTRY_LEN: usize = 1 + 8 + 32;

#[derive(Clone, Copy, Debug)]
pub enum PendingOp {
    Add,
    Remove,
}

fn pending_path(env: &dyn Env) -> PathBuf {
    env.root().join("indexes").join("pending.log")
}

fn write_with_retry(env: &dyn Env, path: &Path, data: &[u8]) -> Result<(), DharmaError> {
    match env.write(path, data) {
        Ok(()) => Ok(()),
        Err(err) => Err(err),
    }
}

fn repair_pending_log(env: &dyn Env, path: &Path) -> Result<(), DharmaError> {
    if !env.exists(path) {
        return Ok(());
    }
    let buf = env.read(path)?;
    let usable = (buf.len() / ENTRY_LEN) * ENTRY_LEN;
    if usable == buf.len() {
        return Ok(());
    }
    if usable == 0 {
        env.remove_file(path)?;
        return Ok(());
    }
    write_with_retry(env, path, &buf[..usable])
}

fn env_now(env: &dyn Env) -> u64 {
    let now = env.now();
    if now < 0 {
        0
    } else {
        now as u64
    }
}

pub fn append_pending(
    env: &dyn Env,
    assertion_id: AssertionId,
    op: PendingOp,
) -> Result<(), DharmaError> {
    if env.root().as_os_str().is_empty() {
        return Ok(());
    }
    let path = pending_path(env);
    if let Some(parent) = path.parent() {
        env.create_dir_all(parent)?;
    }
    let op_code = match op {
        PendingOp::Add => 1u8,
        PendingOp::Remove => 2u8,
    };
    let mut buf = Vec::with_capacity(ENTRY_LEN);
    buf.push(op_code);
    buf.extend_from_slice(&env_now(env).to_le_bytes());
    buf.extend_from_slice(assertion_id.as_bytes());
    match env.append(&path, &buf) {
        Ok(()) => {}
        Err(err) => {
            repair_pending_log(env, &path)?;
            env.append(&path, &buf)?;
            if matches!(err, DharmaError::Io(_)) {
                // ignore; repaired + append succeeded
            }
        }
    }
    Ok(())
}

pub fn read_pending(env: &dyn Env) -> Result<HashMap<AssertionId, u64>, DharmaError> {
    let path = pending_path(env);
    if !env.exists(&path) {
        return Ok(HashMap::new());
    }
    let buf = env.read(&path)?;
    let usable = (buf.len() / ENTRY_LEN) * ENTRY_LEN;
    if usable == 0 {
        return Ok(HashMap::new());
    }
    let mut out: HashMap<AssertionId, u64> = HashMap::new();
    let mut offset = 0usize;
    let slice = &buf[..usable];
    while offset < slice.len() {
        let op = slice[offset];
        offset += 1;
        let ts = u64::from_le_bytes(slice[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let assertion_id = AssertionId::from_slice(&slice[offset..offset + 32])?;
        offset += 32;
        match op {
            1 => {
                out.insert(assertion_id, ts);
            }
            2 => {
                out.remove(&assertion_id);
            }
            _ => {
                // Unknown op: stop parsing to avoid compounding errors.
                break;
            }
        }
    }
    Ok(out)
}

fn compact_pending(env: &dyn Env, pending: &HashMap<AssertionId, u64>) -> Result<(), DharmaError> {
    let path = pending_path(env);
    if pending.is_empty() {
        if env.exists(&path) {
            env.remove_file(&path)?;
        }
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        env.create_dir_all(parent)?;
    }
    let mut buf = Vec::with_capacity(pending.len() * ENTRY_LEN);
    for (id, ts) in pending {
        buf.push(1u8);
        buf.extend_from_slice(&ts.to_le_bytes());
        buf.extend_from_slice(id.as_bytes());
    }
    write_with_retry(env, &path, &buf)
}

pub fn prune_pending(env: &dyn Env, cutoff_ts: u64) -> Result<usize, DharmaError> {
    let mut pending = read_pending(env)?;
    if pending.is_empty() {
        return Ok(0);
    }
    let before = pending.len();
    pending.retain(|_, ts| *ts >= cutoff_ts);
    let removed = before.saturating_sub(pending.len());
    if removed > 0 {
        compact_pending(env, &pending)?;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::StdEnv;
    use crate::types::AssertionId;
    use tempfile::tempdir;

    #[test]
    fn pending_roundtrip_add_remove() {
        let temp = tempdir().unwrap();
        let env = StdEnv::new(temp.path());
        let id = AssertionId::from_bytes([7u8; 32]);
        append_pending(&env, id, PendingOp::Add).unwrap();
        let pending = read_pending(&env).unwrap();
        assert!(pending.contains_key(&id));
        append_pending(&env, id, PendingOp::Remove).unwrap();
        let pending = read_pending(&env).unwrap();
        assert!(!pending.contains_key(&id));
    }

    #[test]
    fn prune_pending_removes_old_entries() {
        let temp = tempdir().unwrap();
        let env = StdEnv::new(temp.path());
        let id = AssertionId::from_bytes([9u8; 32]);
        append_pending(&env, id, PendingOp::Add).unwrap();
        let removed = prune_pending(&env, env_now(&env) + 1).unwrap();
        assert_eq!(removed, 1);
        let pending = read_pending(&env).unwrap();
        assert!(pending.is_empty());
    }
}
