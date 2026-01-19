use crate::error::DharmaError;
use fs2::FileExt;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct LockHandle {
    #[allow(dead_code)]
    file: std::fs::File,
    #[allow(dead_code)]
    path: PathBuf,
}

impl LockHandle {
    pub fn acquire(path: &Path) -> Result<Self, DharmaError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        if let Err(err) = file.try_lock_exclusive() {
            if err.kind() == std::io::ErrorKind::WouldBlock {
                return Err(DharmaError::LockBusy);
            }
            return Err(DharmaError::Io(err));
        }
        file.set_len(0)?;
        writeln!(&file, "pid={}", std::process::id())?;
        file.sync_data()?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_writes_pid() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("dharma.lock");
        let _lock = LockHandle::acquire(&path).unwrap();
        let contents = std::fs::read_to_string(path).unwrap();
        assert!(contents.starts_with("pid="));
    }
}
