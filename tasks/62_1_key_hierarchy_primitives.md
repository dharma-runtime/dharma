# Task 62.1: Key Hierarchy Primitives

## Goal
Implement domain-rooted key hierarchy primitives and key envelope types.

## Dependencies
- Task 62 (spec)
- Task 59.1 (domain schema)

## Scope
- Domain Root Key, KEK, SDK primitives.
- Key envelope assertions and metadata.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/keys.rs` (new)
  - Derivation helpers for KEK/SDK.
- `dharma-core/src/builtins.rs`
  - Add schema types for key envelope actions (`domain.key.rotate`, `subject.key.bind`, `member.key.grant`).
- `dharma-core/src/envelope.rs`
  - Add epoch metadata to envelopes.

## Test Plan (Detailed)
### Unit Tests
- `key_derivation_stable`:
  - Same inputs -> same key; different epoch -> different key.
- `epoch_metadata_roundtrip`:
  - Encode/decode epoch from envelope.

