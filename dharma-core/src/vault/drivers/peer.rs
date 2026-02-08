use crate::error::DharmaError;
use crate::types::SubjectId;
use crate::vault::{DhboxChunk, VaultDriver, VaultLocation, VaultMeta};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct PeerDriver {
    root: PathBuf,
    peer_id: String,
}

impl PeerDriver {
    pub fn new<P: AsRef<Path>>(root: P, peer_id: impl Into<String>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            peer_id: peer_id.into(),
        }
    }

    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    fn peer_root(&self) -> PathBuf {
        self.root.join(&self.peer_id)
    }

    fn subject_dir(&self, subject: &SubjectId) -> PathBuf {
        self.peer_root().join(subject.to_hex())
    }

    fn chunk_path(&self, chunk: &DhboxChunk) -> PathBuf {
        let dir = self.subject_dir(&chunk.header.subject_id);
        let filename = format!("{}_{}.dhbox", chunk.header.seq_start, chunk.header.seq_end);
        dir.join(filename)
    }

    fn location_for_path(&self, path: &Path) -> VaultLocation {
        VaultLocation {
            driver: "peer".to_string(),
            path: path.to_string_lossy().to_string(),
        }
    }
}

impl VaultDriver for PeerDriver {
    fn put_chunk(&self, chunk: &DhboxChunk) -> Result<VaultLocation, DharmaError> {
        let path = self.chunk_path(chunk);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, chunk.to_bytes())?;
        fs::rename(&tmp, &path)?;
        Ok(self.location_for_path(&path))
    }

    fn get_chunk(&self, location: &VaultLocation) -> Result<Vec<u8>, DharmaError> {
        if location.driver != "peer" {
            return Err(DharmaError::Validation(
                "invalid driver for peer".to_string(),
            ));
        }
        Ok(fs::read(&location.path)?)
    }

    fn head_chunk(&self, location: &VaultLocation) -> Result<VaultMeta, DharmaError> {
        let bytes = self.get_chunk(location)?;
        let chunk = DhboxChunk::from_bytes(&bytes)?;
        Ok(VaultMeta {
            size: bytes.len() as u64,
            hash: chunk.ciphertext_hash(),
        })
    }

    fn list_chunks(&self, subject: &SubjectId) -> Result<Vec<VaultLocation>, DharmaError> {
        let dir = self.subject_dir(subject);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "dhbox" {
                        out.push(self.location_for_path(&path));
                    }
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ContractId, SchemaId};
    use crate::vault::{VaultDictionaryRef, VaultItem, VaultSegment};
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use tempfile::tempdir;

    #[test]
    fn peer_driver_put_get_roundtrip() {
        let dir = tempdir().unwrap();
        let driver = PeerDriver::new(dir.path(), "peer-1");
        let subject = SubjectId::from_bytes([1u8; 32]);
        let schema = SchemaId::from_bytes([2u8; 32]);
        let contract = ContractId::from_bytes([3u8; 32]);
        let assertions = vec![VaultItem {
            seq: 1,
            bytes: b"abc".to_vec(),
        }];
        let segment =
            VaultSegment::new(subject, schema, contract, assertions, b"snap".to_vec()).unwrap();
        let svk = [9u8; 32];
        let mut rng = StdRng::seed_from_u64(7);
        let chunk = segment
            .seal(&svk, VaultDictionaryRef::None, &mut rng)
            .unwrap();
        let loc = driver.put_chunk(&chunk).unwrap();
        let bytes = driver.get_chunk(&loc).unwrap();
        let roundtrip = DhboxChunk::from_bytes(&bytes).unwrap();
        assert_eq!(roundtrip.ciphertext, chunk.ciphertext);
    }
}
