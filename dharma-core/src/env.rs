use crate::error::DharmaError;
use crate::net::codec;
use rand_core::RngCore;
use std::fs;
use std::io::Write;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "dharmaq")]
use memmap2::Mmap;

pub trait Fs {
    fn read(&self, path: &Path) -> Result<Vec<u8>, DharmaError>;
    fn read_mmap(&self, path: &Path) -> Result<MappedBytes, DharmaError>;
    fn file_len(&self, path: &Path) -> Result<u64, DharmaError>;
    fn write(&self, path: &Path, data: &[u8]) -> Result<(), DharmaError>;
    fn append(&self, path: &Path, data: &[u8]) -> Result<(), DharmaError>;
    fn remove_dir_all(&self, path: &Path) -> Result<(), DharmaError>;
    fn remove_file(&self, path: &Path) -> Result<(), DharmaError>;
    fn fsync(&self, path: &Path) -> Result<(), DharmaError>;
    fn exists(&self, path: &Path) -> bool;
    fn is_dir(&self, path: &Path) -> bool;
    fn is_file(&self, path: &Path) -> bool;
    fn create_dir_all(&self, path: &Path) -> Result<(), DharmaError>;
    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>, DharmaError>;
}

pub enum MappedBytes {
    #[cfg(feature = "dharmaq")]
    Mmap(Mmap),
    Owned(Vec<u8>),
}

impl AsRef<[u8]> for MappedBytes {
    fn as_ref(&self) -> &[u8] {
        match self {
            #[cfg(feature = "dharmaq")]
            MappedBytes::Mmap(mmap) => mmap.as_ref(),
            MappedBytes::Owned(bytes) => bytes.as_slice(),
        }
    }
}

impl std::ops::Deref for MappedBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

pub trait Net {
    fn send_frame(&mut self, bytes: &[u8]) -> Result<(), DharmaError>;
    fn recv_frame(&mut self) -> Result<Option<Vec<u8>>, DharmaError>;
}

pub trait Env: Fs {
    fn now(&self) -> i64;
    fn random_u64(&mut self) -> u64;
    fn root(&self) -> &Path;
}

#[derive(Clone, Debug)]
pub struct StdEnv {
    root: PathBuf,
}

impl StdEnv {
    pub fn new<P: Into<PathBuf>>(root: P) -> Self {
        Self { root: root.into() }
    }
}

impl Env for StdEnv {
    fn now(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    fn random_u64(&mut self) -> u64 {
        let mut buf = [0u8; 8];
        rand_core::OsRng.fill_bytes(&mut buf);
        u64::from_le_bytes(buf)
    }

    fn root(&self) -> &Path {
        &self.root
    }
}

impl Fs for StdEnv {
    fn read(&self, path: &Path) -> Result<Vec<u8>, DharmaError> {
        fs::read(path).map_err(Into::into)
    }

    #[allow(unsafe_code)]
    fn read_mmap(&self, path: &Path) -> Result<MappedBytes, DharmaError> {
        #[cfg(feature = "dharmaq")]
        {
            let file = fs::File::open(path)?;
            let len = file.metadata()?.len();
            if len == 0 {
                return Ok(MappedBytes::Owned(Vec::new()));
            }
            let map = unsafe { Mmap::map(&file) }?;
            Ok(MappedBytes::Mmap(map))
        }
        #[cfg(not(feature = "dharmaq"))]
        {
            Ok(MappedBytes::Owned(self.read(path)?))
        }
    }

    fn file_len(&self, path: &Path) -> Result<u64, DharmaError> {
        Ok(fs::metadata(path)?.len())
    }

    fn write(&self, path: &Path, data: &[u8]) -> Result<(), DharmaError> {
        fs::write(path, data).map_err(Into::into)
    }

    fn append(&self, path: &Path, data: &[u8]) -> Result<(), DharmaError> {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        file.write_all(data)?;
        file.flush()?;
        Ok(())
    }

    fn remove_dir_all(&self, path: &Path) -> Result<(), DharmaError> {
        fs::remove_dir_all(path).map_err(Into::into)
    }

    fn remove_file(&self, path: &Path) -> Result<(), DharmaError> {
        fs::remove_file(path).map_err(Into::into)
    }

    fn fsync(&self, path: &Path) -> Result<(), DharmaError> {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        file.sync_all().map_err(Into::into)
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn is_dir(&self, path: &Path) -> bool {
        path.is_dir()
    }

    fn is_file(&self, path: &Path) -> bool {
        path.is_file()
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), DharmaError> {
        fs::create_dir_all(path).map_err(Into::into)
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>, DharmaError> {
        let mut out = Vec::new();
        for entry in fs::read_dir(path)? {
            out.push(entry?.path());
        }
        Ok(out)
    }
}

impl Net for TcpStream {
    fn send_frame(&mut self, bytes: &[u8]) -> Result<(), DharmaError> {
        codec::write_frame(self, bytes)
    }

    fn recv_frame(&mut self) -> Result<Option<Vec<u8>>, DharmaError> {
        codec::read_frame_optional(self)
    }
}
