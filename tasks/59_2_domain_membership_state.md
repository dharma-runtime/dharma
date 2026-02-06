# Task 59.2: Domain Membership State Evaluation

## Goal
Compute deterministic membership state (roles/scopes/expiry) for a domain subject.

## Dependencies
- Task 59.1 (domain schema)
- Task 58.3 (identity verification)

## Scope
- Membership state derivation from domain assertions.
- Role/scope resolution and expiration.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/domain.rs` (new)
  - Implement `DomainState::load(store, subject)`.
  - Compute members, roles, scopes, expiry, revocations.
- `dharma-core/src/identity.rs`
  - Add helper to check domain membership for an identity.

## Test Plan (Detailed)
### Unit Tests
- `membership_invite_approve_flow`:
  - Invite -> approve => active membership.
- `membership_revoke_removes_access`:
  - Revoke => membership removed.
- `membership_expiry`:
  - Expired membership => denied.

### Determinism Tests
- Replay membership chain => same `DomainState`.

