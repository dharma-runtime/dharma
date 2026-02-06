use crate::error::DharmaError;
use crate::types::SubjectId;
use crate::vault::{DhboxChunk, VaultDriver, VaultLocation, VaultMeta};

#[cfg(feature = "vault-s3")]
use crate::types::{hex_decode, hex_encode};
#[cfg(feature = "vault-s3")]
use aws_sdk_s3::primitives::ByteStream;
#[cfg(feature = "vault-s3")]
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
#[cfg(feature = "vault-s3")]
use aws_sdk_s3::Client;
#[cfg(feature = "vault-s3")]
use aws_types::region::Region;
#[cfg(feature = "vault-s3")]
use tokio::runtime::Runtime;

#[cfg(feature = "vault-s3")]
#[derive(Debug)]
pub struct S3Driver {
    client: Client,
    bucket: String,
    prefix: String,
    rt: Runtime,
}

#[cfg(not(feature = "vault-s3"))]
#[derive(Debug)]
pub struct S3Driver;

#[cfg(feature = "vault-s3")]
#[derive(Clone, Debug, Default)]
pub struct S3Options {
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub region: Option<String>,
}

impl S3Driver {
    #[cfg(feature = "vault-s3")]
    pub fn new(bucket: impl Into<String>, prefix: impl Into<String>) -> Result<Self, DharmaError> {
        Self::new_with_options(bucket, prefix, S3Options::default())
    }

    #[cfg(feature = "vault-s3")]
    pub fn new_with_options(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        options: S3Options,
    ) -> Result<Self, DharmaError> {
        let bucket = bucket.into();
        let prefix = prefix.into();
        let rt = Runtime::new()?;
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(endpoint) = options.endpoint_url {
            loader = loader.endpoint_url(endpoint);
        }
        if let Some(region) = options.region {
            loader = loader.region(Region::new(region));
        }
        let shared = rt.block_on(async { loader.load().await });
        let mut s3_builder = S3ConfigBuilder::from(&shared);
        if options.force_path_style {
            s3_builder = s3_builder.force_path_style(true);
        }
        let client = Client::from_conf(s3_builder.build());
        Ok(Self {
            client,
            bucket,
            prefix,
            rt,
        })
    }

    #[cfg(not(feature = "vault-s3"))]
    pub fn new(_bucket: impl Into<String>, _prefix: impl Into<String>) -> Result<Self, DharmaError> {
        Err(DharmaError::Config(
            "vault-s3 feature not enabled".to_string(),
        ))
    }
}

#[cfg(feature = "vault-s3")]
impl S3Driver {
    fn key_for_chunk(&self, chunk: &DhboxChunk) -> String {
        let subject = chunk.header.subject_id.to_hex();
        let filename = format!("{}_{}.dhbox", chunk.header.seq_start, chunk.header.seq_end);
        if self.prefix.is_empty() {
            format!("{subject}/{filename}")
        } else {
            format!("{}/{subject}/{filename}", self.prefix.trim_end_matches('/'))
        }
    }

    fn block_on<F: std::future::Future>(&self, fut: F) -> Result<F::Output, DharmaError> {
        Ok(self.rt.block_on(fut))
    }

    pub fn ensure_bucket(&self) -> Result<(), DharmaError> {
        self.block_on(async {
            let head = self.client.head_bucket().bucket(&self.bucket).send().await;
            if head.is_ok() {
                return Ok(());
            }
            let result = self.client.create_bucket().bucket(&self.bucket).send().await;
            match result {
                Ok(_) => Ok(()),
                Err(err) => {
                    let msg = err.to_string();
                    if msg.contains("BucketAlreadyOwnedByYou")
                        || msg.contains("BucketAlreadyExists")
                        || msg.contains("BucketAlreadyOwnedBy")
                    {
                        Ok(())
                    } else {
                        Err(DharmaError::Network(format!(
                            "s3 create bucket failed: {err}"
                        )))
                    }
                }
            }
        })?
    }
}

#[cfg(feature = "vault-s3")]
impl VaultDriver for S3Driver {
    fn put_chunk(&self, chunk: &DhboxChunk) -> Result<VaultLocation, DharmaError> {
        let key = self.key_for_chunk(chunk);
        let bytes = chunk.to_bytes();
        let hash_hex = hex_encode(chunk.ciphertext_hash());
        self.block_on(async {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .metadata("vault-hash", hash_hex)
                .body(ByteStream::from(bytes))
                .send()
                .await
                .map_err(|e| DharmaError::Network(format!("s3 put failed: {e}")))?;
            Ok::<(), DharmaError>(())
        })??;
        Ok(VaultLocation {
            driver: "s3".to_string(),
            path: key,
        })
    }

    fn get_chunk(&self, location: &VaultLocation) -> Result<Vec<u8>, DharmaError> {
        if location.driver != "s3" {
            return Err(DharmaError::Validation("invalid driver for s3".to_string()));
        }
        let key = location.path.clone();
        self.block_on(async {
            let output = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| DharmaError::Network(format!("s3 get failed: {e}")))?;
            let data = output
                .body
                .collect()
                .await
                .map_err(|e| DharmaError::Network(format!("s3 read failed: {e}")))?;
            Ok::<Vec<u8>, DharmaError>(data.into_bytes().to_vec())
        })?
    }

    fn head_chunk(&self, location: &VaultLocation) -> Result<VaultMeta, DharmaError> {
        if location.driver != "s3" {
            return Err(DharmaError::Validation("invalid driver for s3".to_string()));
        }
        let key = location.path.clone();
        self.block_on(async {
            let output = self
                .client
                .head_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| DharmaError::Network(format!("s3 head failed: {e}")))?;
            let size = output.content_length().unwrap_or(0) as u64;
            let hash = if let Some(meta) = output.metadata() {
                if let Some(value) = meta.get("vault-hash") {
                    let bytes = hex_decode(value)
                        .map_err(|e| DharmaError::Validation(format!("hash decode failed: {e}")))?;
                    if bytes.len() != 32 {
                        return Err(DharmaError::Validation(
                            "vault-hash metadata length".to_string(),
                        ));
                    }
                    let mut out = [0u8; 32];
                    out.copy_from_slice(&bytes);
                    out
                } else {
                    return Err(DharmaError::Validation(
                        "missing vault-hash metadata".to_string(),
                    ));
                }
            } else {
                return Err(DharmaError::Validation(
                    "missing vault metadata".to_string(),
                ));
            };
            Ok::<VaultMeta, DharmaError>(VaultMeta { size, hash })
        })?
    }

    fn list_chunks(&self, subject: &SubjectId) -> Result<Vec<VaultLocation>, DharmaError> {
        let subject_prefix = if self.prefix.is_empty() {
            subject.to_hex()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), subject.to_hex())
        };
        self.block_on(async {
            let mut out = Vec::new();
            let mut token = None;
            loop {
                let mut req = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(&subject_prefix);
                if let Some(t) = &token {
                    req = req.continuation_token(t);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| DharmaError::Network(format!("s3 list failed: {e}")))?;
                for obj in resp.contents() {
                    if let Some(key) = obj.key() {
                        out.push(VaultLocation {
                            driver: "s3".to_string(),
                            path: key.to_string(),
                        });
                    }
                }
                if resp.is_truncated().unwrap_or(false) {
                    token = resp.next_continuation_token().map(|s| s.to_string());
                } else {
                    break;
                }
            }
            Ok::<Vec<VaultLocation>, DharmaError>(out)
        })?
    }
}

#[cfg(not(feature = "vault-s3"))]
impl VaultDriver for S3Driver {
    fn put_chunk(&self, _chunk: &DhboxChunk) -> Result<VaultLocation, DharmaError> {
        Err(DharmaError::Config(
            "vault-s3 feature not enabled".to_string(),
        ))
    }

    fn get_chunk(&self, _location: &VaultLocation) -> Result<Vec<u8>, DharmaError> {
        Err(DharmaError::Config(
            "vault-s3 feature not enabled".to_string(),
        ))
    }

    fn head_chunk(&self, _location: &VaultLocation) -> Result<VaultMeta, DharmaError> {
        Err(DharmaError::Config(
            "vault-s3 feature not enabled".to_string(),
        ))
    }

    fn list_chunks(&self, _subject: &SubjectId) -> Result<Vec<VaultLocation>, DharmaError> {
        Err(DharmaError::Config(
            "vault-s3 feature not enabled".to_string(),
        ))
    }
}
