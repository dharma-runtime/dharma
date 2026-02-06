# Task 59.1: std.atlas.domain Schema + Contract Types

## Goal
Define the domain contract schema and assertion types required for membership, hierarchy, and ownership defaults.

## Dependencies
- Task 59 (spec)
- Task 58.1 (Atlas identity types)

## Scope
- Schema types for `atlas.domain.*` assertions.
- Contract metadata for ownership defaults.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/builtins.rs`
  - Add schema types:
    - `atlas.domain.genesis`
    - `atlas.domain.invite`
    - `atlas.domain.request`
    - `atlas.domain.approve`
    - `atlas.domain.revoke`
    - `atlas.domain.leave`
- `contracts/` (new)
  - Add contract skeleton `std.atlas.domain` (DSL/WASM).
  - Export summary metadata (permissions, defaults).

## Test Plan (Detailed)
### Unit Tests
- `atlas_domain_schema_roundtrip`:
  - Encode/decode domain schema.
- `atlas_domain_required_fields`:
  - Missing fields => schema validation fails.

### Negative Tests
- Unknown `atlas.domain.*` type rejected.

