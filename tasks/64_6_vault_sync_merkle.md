# Task 64.6: Vault Sync + Merkle Proofs

## Goal
Add bandwidth-efficient vault sync and merkle proof APIs for partial verification.

## Why
We must avoid re-transmitting entire archives and allow "prove this exists" without full download.

## Scope
- Range-based checkpoint diff for vault sync.
- Merkle proof generation/verification APIs.
- Protocol hooks for proof requests.

## Out of Scope
- Core .dhbox format (Task 64.1).
- Storage drivers (Task 64.2).

## Specification

### 1) Range-based Sync
- Exchange checkpoint ranges per subject.
- Request missing chunks by hash or seq range.
- Avoid redundant transfer when ranges overlap.

### 2) Merkle Proof API
- `get_merkle_proof(chunk_id, leaf_index)` returns proof.
- `verify_merkle_proof(leaf_hash, proof, root)` returns bool.
- Proofs must be verifiable without decrypting entire chunk.

### 3) Protocol Surface
Add optional sync messages or RPC:
- `vault.inv` (ranges + hashes)
- `vault.get` (request by range/hash)
- `vault.proof` (merkle proof)

## Implementation Steps
1. Add range diff logic for checkpoints.
2. Add proof API in vault core.
3. Wire proof request to storage driver.
4. Integrate into sync flow (if enabled).

## Test Plan (Detailed)

### Unit Tests
- `range_diff_returns_missing_only`:
  - Overlapping ranges should produce minimal diff.
- `merkle_proof_verifies`:
  - Proof validates leaf -> root.
- `merkle_proof_rejects_wrong_leaf`:
  - Wrong leaf hash => verify fails.

### Integration Tests
- `vault_sync_skips_existing_chunks`:
  - Node B already has chunk -> not transferred.
- `proof_request_roundtrip`:
  - Request proof -> verify without downloading chunk.

### Security Tests
- Malicious proof payload => reject.
- Chunk hash mismatch => no sync acceptance.

## Acceptance Criteria
- Sync transfers only missing chunks.
- Proofs verify correctly and cheaply.
