# Task 64.7: Vault Test Plan + Harness

## Goal
Deliver a deterministic test harness that validates vault integrity end-to-end.

## Why
Vault is critical infrastructure. We need confidence under corruption, replay, and restore scenarios.

## Scope
- Add test fixtures for assertions, chunks, and checkpoints.
- Extend `dharma-test` with vault scenarios.
- Add CI-friendly tests and optional long-running stress tests.

## Out of Scope
- Driver implementation (Task 64.2).
- CLI flows (Task 64.5).

## Specification

### 1) Test Fixtures
- Generate sample subjects with:
  - 10k assertions
  - multiple schemas/contracts
  - multiple snapshots
- Include golden `.dhbox` fixtures for v1 header parsing.

### 2) Deterministic Harness
- Add a vault test suite in `dharma-test`:
  - Fixed RNG seed for reproducibility.
  - Deterministic chunk boundaries.

### 3) Failure Injection
- Corrupt ciphertext.
- Truncate chunk.
- Mismatched dict hash.
- Checkpoint rollback attempts.
- Partial chunk loss.

## Test Plan (Detailed)

### Unit Tests
- Header parse/serialize golden vectors.
- Dict training ratio check (>= 20% on fixture set).
- Merkle proof verification.

### Integration Tests
- `vault_archive_restore_roundtrip`:
  - Archive -> restore -> identical state root.
- `vault_checkpoint_enforced`:
  - Missing checkpoint => prune forbidden.
- `vault_mixed_restore`:
  - Cold + hot log merge deterministic.

### Chaos/Stress Tests (optional, nightly)
- Random corruption injection across 100 chunks.
- Simulated storage outages during upload.
- Large subject (1M assertions) archival and restore.

### CI Coverage Gates
- Minimum coverage of:
  - encryption/decryption
  - checkpoint verification
  - restore path

## Acceptance Criteria
- Deterministic vault suite runs in CI.
- All core failure cases are detected.
