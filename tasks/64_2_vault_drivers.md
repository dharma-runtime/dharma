# Task 64.2: Vault Drivers (Local/S3/Arweave/Peer)

## Goal
Implement secure storage drivers behind a `VaultDriver` trait.

## Why
Cold storage is only as strong as the driver contract. We need consistent APIs, integrity checks, and predictable behavior across providers.

## Scope
- `VaultDriver` trait with minimal, stable operations.
- `LocalDriver`, `S3Driver`, `ArweaveDriver`, `PeerDriver`.
- Optional erasure coding wrapper (k-of-n shards).

## Out of Scope
- Vault core format (Task 64.1).
- Contract + checkpoint rules (Task 64.3).
- Runtime archival scheduling (Task 64.4).

## Specification

### 1) Trait
```
trait VaultDriver {
  fn put_chunk(&self, chunk: &DhboxChunk) -> Result<VaultRef>;
  fn get_chunk(&self, location: &VaultLocation) -> Result<Vec<u8>>;
  fn head_chunk(&self, location: &VaultLocation) -> Result<VaultMeta>;
  fn list_chunks(&self, subject: SubjectId) -> Result<Vec<VaultRef>>;
}
```

### 2) LocalDriver
- Store in `~/.dharma/vault/local/<subject>/<seq_start>_<seq_end>.dhbox`.
- Should allow read-only mode for air-gapped restore.

### 3) S3Driver
- Uses AWS SDK.
- Require:
  - Object Lock (compliance mode).
  - Block public access.
  - Versioning enabled.
  - IAM policy Put/Get only (no delete).
- `head_chunk` should verify object metadata for immutability.

### 4) ArweaveDriver
- Store via Irys/Bundlr (fast upload).
- Public store, ciphertext only.
- `location` is tx id.

### 5) PeerDriver
- Transfer via Dharma Noise protocol.
- Store at `~/.dharma/vault/peers/<peer_id>/<subject>/`.
- Peer never receives keys.

### 6) Erasure Coding (optional)
- Wrapper driver that splits ciphertext into shards.
- k-of-n recovery (default 3-of-5).
- Each shard stores a `VaultShard` entry.

## Implementation Steps
1. Define `VaultDriver`, `VaultLocation`, `VaultMeta`, `DriverCaps`.
2. Implement `LocalDriver` (baseline).
3. Implement `S3Driver` with policy/lock checks.
4. Implement `ArweaveDriver` using Irys/Bundlr.
5. Implement `PeerDriver` over existing sync transport.
6. Implement optional erasure coding wrapper.

## Test Plan (Detailed)

### Unit Tests
- `local_driver_put_get_roundtrip`:
  - Put chunk -> get returns identical bytes.
- `local_driver_head_meta`:
  - `head_chunk` returns size + hash.

### Integration Tests
- `s3_driver_put_get_minio` (if minio available):
  - Upload + download matches hash.
- `arweave_driver_upload_download` (feature flag):
  - Upload -> retrieve -> verify hash.
- `peer_driver_roundtrip`:
  - Simulate two peers, transfer chunk, verify hash.

### Security/Policy Tests
- `s3_driver_rejects_non_compliant_bucket`:
  - Missing lock or public access enabled => error.
- `erasure_code_reconstructs`:
  - Any k shards reconstruct original ciphertext.
- `erasure_code_insufficient_shards_fails`:
  - <k shards => error.

## Acceptance Criteria
- All drivers conform to `VaultDriver`.
- Local driver passes tests without network.
- S3/Arweave/Peer drivers gated by feature flags and documented in tests.
