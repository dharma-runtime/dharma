# Task 64.4: Runtime Archival + Restore Pipeline

## Goal
Integrate vault archival and restore into the runtime with a safe state machine.

## Why
Cold storage must be automatic, reliable, and never risk data loss.

## Scope
- Background archival job in runtime/store.
- Checkpoint commit after successful upload + verification.
- Restore flow from cold-only chunks.
- Safe prune policy.

## Out of Scope
- Driver implementation (Task 64.2).
- CLI UX (Task 64.5).
- Vault sync optimization (Task 64.6).

## Specification

### 1) State Machine
```
Pending -> Uploading -> Uploaded -> Verified -> Pruned
```
- Upload failure => retry, no prune.
- Verify failure => abort prune, mark corrupt.
- Checkpoint failure => keep chunk locally.

### 2) Archival Trigger
- When hot log exceeds:
  - `chunk_size_mb` OR `chunk_assertions`.
- Each chunk must include:
  - Assertions + snapshot
  - Snapshot hash in header
  - Merkle root in header

### 3) Verification Before Prune
- Must match:
  - header fields
  - ciphertext hash
  - merkle root
  - checkpoint accepted by contract

### 4) Restore
- Cold-only restore path:
  - Fetch all checkpoints
  - Download missing chunks
  - Replay chunks to rebuild subject state
- Mixed restore path:
  - Merge cold + hot log
  - Ensure deterministic frontier

## Implementation Steps
1. Add archival scheduler in runtime/store.
2. Implement chunk creation and enqueue.
3. Upload via selected driver.
4. Verify chunk, then commit checkpoint.
5. Prune hot log only after checkpoint accepted.
6. Implement restore pipeline and replay logic.

## Test Plan (Detailed)

### Unit Tests
- `archival_state_machine_transitions`:
  - Pending -> Uploading -> Uploaded -> Verified -> Pruned.
- `prune_requires_checkpoint`:
  - No checkpoint => no prune.

### Integration Tests
- `archive_and_restore_roundtrip`:
  - Create assertions -> archive -> restore -> identical state root.
- `cold_only_restore`:
  - Delete hot log -> restore from chunks -> state matches.
- `mixed_restore_merge`:
  - Some chunks + hot log -> converge to same frontier.

### Failure Injection
- Upload failure mid-flight => retry, no checkpoint.
- Corrupt chunk on remote => verify fails, no prune.
- Crash after upload but before checkpoint => recover and retry.

## Acceptance Criteria
- No prune occurs without verified checkpoint.
- Restore reproduces identical state root and frontier.
