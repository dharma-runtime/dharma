# Task 60.3: Fabric Enforcement of Ownership/Sharing

## Goal
Enforce ownership and sharing rules in Fabric read/query/execute paths.

## Dependencies
- Task 60.2 (sharing state)
- Task 60.1 (ownership metadata)
- Task 61.2 (permission summaries) [soft dependency]

## Scope
- Enforce access on ExecQuery/QueryFast/QueryWide/ExecAction.
- Redact or deny based on share state + scopes.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/fabric/router.rs`
  - Enforce share checks for subject/table queries.
- `dharma-core/src/fabric/protocol.rs`
  - Apply checks for Execute/Read operations.
- `dharma-core/src/runtime/cqrs.rs`
  - Ensure query results are filtered per field scope.

## Test Plan (Detailed)
### Integration Tests
- `owner_can_read_write`:
  - Owner allowed all scopes.
- `shared_identity_can_query_scoped_fields`:
  - Only allowed fields present.
- `unshared_identity_denied`:
  - Read/query denied or redacted.

