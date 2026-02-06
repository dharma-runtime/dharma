# Task 64.3: sys.vault Contract + Checkpoint Rules

## Goal
Define and enforce the `sys.vault.Checkpoint` assertion that anchors cold chunks to the hot log.

## Why
Checkpoints are the integrity bridge between hot log and cold storage. Without strict rules, tampering or rollback is possible.

## Scope
- Add `contracts/std/sys_vault.dhl`.
- Wire contract validation in ingest.
- Enforce checkpoint monotonicity and header consistency.

## Out of Scope
- Driver implementation (Task 64.2).
- Vault core format details (Task 64.1).
- Archival pipeline scheduling (Task 64.4).

## Specification

### 1) Contract (DHL)
Define:
- `StorageDriver` enum.
- `CompressionAlg`, `EncryptionAlg`.
- `VaultShard` and `VaultRef`.
- `Checkpoint(start_seq, end_seq, state_root, vault)`.

### 2) Validation Rules
- `start_seq >= last_checkpoint.end_seq`.
- `end_seq > start_seq`.
- `vault.subject == subject_id`.
- `vault.seq_start == start_seq`, `vault.seq_end == end_seq`.
- `vault.hash` must match `.dhbox` ciphertext hash.
- `vault.merkle_root` must match computed root.
- `vault.snapshot_hash` must match snapshot referenced in chunk.
- `vault.format_version` == supported version.

### 3) Acceptance Rules
Reject:
- Missing or non-monotonic checkpoints.
- Checkpoint that references unknown chunk.
- Checkpoint with mismatched header fields or hash.

## Implementation Steps
1. Add `contracts/std/sys_vault.dhl` and include in std lib build.
2. Update contract validation pipeline to understand `sys.vault.Checkpoint`.
3. Add monotonic checkpoint tracking per subject.
4. Reject mismatched or replayed checkpoints.

## Test Plan (Detailed)

### Unit Tests
- `checkpoint_monotonicity_enforced`:
  - start_seq less than last end_seq => reject.
- `checkpoint_range_progression_required`:
  - start_seq == end_seq => reject.
- `checkpoint_hash_mismatch_rejected`:
  - vault.hash != computed hash => reject.
- `checkpoint_merkle_mismatch_rejected`.
- `checkpoint_snapshot_mismatch_rejected`.

### Integration Tests
- Archive chunk -> commit checkpoint -> verify acceptance.
- Replay old checkpoint after newer => reject.

### Negative/Security Tests
- Mismatched subject or seq range => reject.
- Fake driver type or invalid location => reject.

## Acceptance Criteria
- Checkpoints accepted only when all rules pass.
- Any mismatch prevents prune and does not alter state.
