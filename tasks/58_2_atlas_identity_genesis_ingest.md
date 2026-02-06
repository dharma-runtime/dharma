# Task 58.2: Genesis Phase Enforcement

## Goal
Enforce the Genesis Phase rule for Atlas identities at ingest (kernel level).

## Dependencies
- Task 58.1 (schema/types)
- Task 58 (spec)

## Scope
- Hard rule: genesis only when subject has no prior accepted facts.
- Non-repeatable, seq==1, type must be `atlas.identity.genesis`.
- Implemented before contract validation.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/net/ingest.rs`
  - Add genesis gate before contract validation:
    - If subject has no assertions, only allow `atlas.identity.genesis` with seq=1.
    - If subject already has assertions, reject `atlas.identity.genesis`.
- `dharma-core/src/store/state.rs`
  - Add helper `subject_has_facts(env, subject) -> bool` (fast check).
- `dharma-core/src/identity_store.rs`
  - Update identity init to emit `atlas.identity.genesis` (seq=1).

## Test Plan (Detailed)
### Unit Tests
- `atlas_genesis_only_once`:
  - First genesis accepted, second rejected.
- `atlas_genesis_requires_seq1`:
  - seq!=1 => rejected.
- `atlas_genesis_requires_empty_subject`:
  - Pre-existing assertion => genesis rejected.

### Integration Tests
- `identity_init_emits_atlas_genesis`:
  - `dh identity init` creates atlas genesis with correct fields.

### Negative/Security Tests
- Wrong type at seq=1 on empty subject rejected.
- Malformed atlas genesis rejected.

