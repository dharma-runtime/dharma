# Task 64.1: Vault Core + .dhbox Format

## Goal
Implement the core vault primitives: chunking, .dhbox serialization, compression, encryption, hashing, and merkle proofs.

## Why
This is the cryptographic foundation for cold storage. Every driver, CLI, and restore path depends on a stable, verifiable format.

## Scope
- Vault core module in `dharma-core`.
- .dhbox v1 header/body format.
- Chunk sizing and segmentation logic.
- Zstd dictionary training and dictionary handling.
- Merkle root computation for partial proofs.

## Out of Scope
- Storage drivers (Task 64.2).
- Contract and checkpoint enforcement (Task 64.3).
- Runtime archival scheduling (Task 64.4).
- CLI UX (Task 64.5).

## Specification

### 1) Module Layout
- Add `dharma-core/src/vault/mod.rs` with:
  - `DhboxHeaderV1`
  - `DhboxChunk`
  - `VaultSegment`
  - `VaultConfig`
  - `VaultCrypto` (key derivation + encrypt/decrypt)
  - `VaultMerkle` (root + proof)

### 2) Chunking
- Chunk boundary triggers on:
  - `chunk_assertions` OR
  - `chunk_size_mb` (post-serialize, pre-compress size)
- Defaults in `VaultConfig`:
  - `chunk_size_mb = 10`
  - `chunk_assertions = 10_000`
- Always allow override via config for mobile/enterprise.

### 3) Key Derivation (CIA‑grade)
- `VMK = HKDF(root_key, salt=identity_id, info="dharma:vault:master")`
- `SVK = HKDF(VMK, "dharma:vault:subject" || subject_id || epoch)`
- `CK  = HKDF(SVK, "dharma:vault:chunk" || seq_start || seq_end || chunk_salt)`
- Use XChaCha20-Poly1305 with:
  - random 24-byte nonce
  - random 32-byte `chunk_salt` (stored in header)
  - AAD = `subject_id || seq_start || seq_end || schema_id || contract_id`

### 4) .dhbox v1 Format
Header fields:
- magic = "DHBOX"
- version = 1
- subject_id (32 bytes)
- seq_start, seq_end (u64)
- assertion_count (u32)
- schema_id (32 bytes)
- contract_id (32 bytes)
- snapshot_hash (32 bytes)
- merkle_root (32 bytes)
- chunk_salt (32 bytes)
- dict_hash (32 bytes, optional)
- dict_len (u32, optional)
- dict_inline (bytes, optional)
- compression = enum (zstd-19)
- encryption = enum (xchacha20-poly1305)
- nonce (24 bytes)

Body:
- ciphertext = Encrypt(Compress(CBOR({ assertions[], snapshot })))
- hash = BLAKE3(ciphertext)

### 5) Zstd Dictionary
- Train a 32KB dictionary using sampled assertions:
  - Require a training flow from sample assertions in local store.
- Dictionary usage:
  - Either inline in header or referenced by hash.
  - If referenced by hash, lookup in local dictionary cache.

### 6) Merkle Tree
- Leaves are `EnvelopeId` (or canonical assertion hash) in chunk order.
- Root stored in header.
- Proof format: `Vec<Hash>` of sibling hashes.

## Implementation Steps
1. Add vault module + config defaults.
2. Implement chunking and segmentation builder.
3. Implement dictionary training + cache.
4. Implement .dhbox serialize/deserialize for v1.
5. Implement encryption/decryption with AAD binding.
6. Implement merkle root + proof generation/verification.

## Test Plan (Detailed)

### Unit Tests
- `dhbox_header_roundtrip_v1`:
  - Serialize header -> parse -> identical fields.
- `dhbox_ciphertext_hash_matches`:
  - Hash from header matches recomputed hash.
- `dhbox_decrypt_fails_on_wrong_aad`:
  - Mutate subject_id or seq range -> decrypt fails.
- `chunking_respects_size_and_count`:
  - Assert N assertions per chunk or size limit triggers new chunk.
- `dict_hash_mismatch_rejects_decode`:
  - Tamper dict hash -> decode error.
- `merkle_root_matches_leaves`:
  - Root computed from leaves equals stored root.
- `merkle_proof_verifies_leaf`:
  - Proof validates leaf -> root.

### Property Tests
- Randomize assertions and verify:
  - roundtrip encode/decode preserves content
  - decrypt( encrypt(x) ) == x
  - merkle proofs always validate for included leaf

### Negative Tests
- Corrupt header fields -> parse error.
- Corrupt ciphertext -> hash mismatch, decrypt error.
- Wrong dict -> decrypt/deserialize error.

## Acceptance Criteria
- .dhbox v1 roundtrip works with dictionary and without.
- Merkle root/proof verify for all assertions in a chunk.
- Decrypt fails on any AAD mismatch or ciphertext mutation.
