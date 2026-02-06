# Task 59.4: Directory Integration for Domains

## Goal
Bind domain contract ownership to directory registration and parent authorization.

## Dependencies
- Task 59.1 (domain schema)
- Task 59.2 (membership state)
- Task 25 (Directory & Relay System)

## Scope
- Ensure directory registration reflects domain contract owner.
- Enforce parent authorization for nested domains.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/fabric/directory.rs`
  - Validate `fabric.domain.register` against domain contract owner.
  - Enforce parent authorization (existing logic to be aligned with contract).
- `dharma-core/src/domain.rs`
  - Add `owner_for_domain(subject)` helper.

## Test Plan (Detailed)
### Unit Tests
- `domain_register_requires_owner_match`:
  - Directory register fails if owner != contract owner.
- `nested_domain_requires_parent_auth`:
  - Parent authorization missing => reject.

### Integration Tests
- End-to-end flow: request -> authorize -> register -> sync -> owner recognized.

