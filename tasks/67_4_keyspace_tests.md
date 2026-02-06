# Task 67.4: Keyspace + Embedding Tests

## Goal
Guarantee determinism, hierarchy, and historical read integrity.

## Scope
- Property tests for deterministic payload + key generation.
- Hierarchy parent prefix validation.
- Version bump behavior (keys must change when spec changes).
- Canonicalization checks (map/list order invariance).
- Performance regression for membership projection.

## Acceptance Criteria
- All tests pass in CI.
- Replay produces identical keys and membership rows.
