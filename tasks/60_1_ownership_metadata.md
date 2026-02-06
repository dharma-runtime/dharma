# Task 60.1: Ownership & Attribution Metadata

## Goal
Add explicit owner/creator/acting context metadata to subjects and enforce defaults.

## Dependencies
- Task 59.3 (acting context)

## Scope
- Record ownership + creator attribution for every subject.
- Default owner = domain if omitted.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/ownership.rs` (new)
  - Define `OwnershipRecord { owner, creator, acting_domain, role }`.
  - Helpers to compute defaults.
- `dharma-core/src/net/ingest.rs`
  - When creating subject, attach ownership metadata and validate exclusivity.
- `dharma-core/src/store/state.rs`
  - Persist ownership metadata with subject logs or indexes.

## Test Plan (Detailed)
### Unit Tests
- `owner_default_to_domain`:
  - No explicit owner => owner = acting domain.
- `creator_attribution_recorded`:
  - Creator fields present on subject creation.
- `ownership_exclusive`:
  - Multiple owners => reject.

