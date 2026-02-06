# Task 61.1: Permission Summary Artifact

## Goal
Define and emit permission summary artifacts for each contract version.

## Dependencies
- Task 61 (spec)
- Task 59.1 (domain contract schema)

## Scope
- Summary schema + encoding.
- Emit summaries during contract build.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/contract.rs`
  - Define summary schema + encoder/decoder.
- `dharma-core/src/runtime/compiler` (or build pipeline)
  - Emit summary artifact alongside contract.
- `dharma-core/src/store.rs`
  - Store/retrieve summary artifacts by contract id.

## Test Plan (Detailed)
### Unit Tests
- `summary_format_roundtrip`:
  - Encode/decode summary artifact.

