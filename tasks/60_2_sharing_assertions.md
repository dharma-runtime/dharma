# Task 60.2: Sharing & Revocation Assertions

## Goal
Implement explicit share/revoke/public assertions and a deterministic share-state evaluator.

## Dependencies
- Task 60.1 (ownership metadata)
- Task 59.2 (membership)

## Scope
- `share.grant`, `share.revoke`, `share.public` assertions.
- Access scopes: fields/actions/queries.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/builtins.rs`
  - Add schema types for share actions.
- `dharma-core/src/share.rs` (new)
  - Build share state from assertions.
- `dharma-core/src/net/ingest.rs`
  - Validate share assertions (owner-only by default).

## Test Plan (Detailed)
### Unit Tests
- `share_grant_direct_identity`:
  - Granted identity can read/query.
- `share_revoke_blocks_future`:
  - After revoke, access denied.
- `share_public_explicit`:
  - Public toggle is explicit and auditable.

