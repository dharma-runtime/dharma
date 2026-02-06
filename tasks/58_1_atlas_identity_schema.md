# Task 58.1: Atlas Identity Schema & Types

## Goal
Define the Atlas identity assertion types and schema entries required by PRD v1.

## Dependencies
- Task 58: Atlas Identity + Genesis Phase + Lifecycle (spec)

## Scope
- Add schema entries for `atlas.identity.*` types.
- Add constants/type helpers where needed.
- No ingest behavior changes (handled in Task 58.2/58.3).

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/builtins.rs`
  - Add schema types:
    - `atlas.identity.genesis`
    - `atlas.identity.activate`
    - `atlas.identity.suspend`
    - `atlas.identity.revoke`
  - Required fields:
    - genesis: `atlas_name`, `owner_key` (or `root_key`), optional `note`
    - lifecycle: `reason` (optional), `ts` (optional)
- `dharma-core/src/identity.rs`
  - Add string constants for atlas identity types to avoid typos.
- `docs/` (optional)
  - Add short schema reference entry (if schema docs exist).

## Test Plan (Detailed)
### Unit Tests
- `atlas_identity_schema_roundtrip`:
  - Encode schema -> decode -> equality.
- `atlas_identity_schema_required_fields`:
  - Missing required fields => schema validation failure.

### Negative Tests
- Unknown `atlas.identity.*` type rejected by schema lookup.

