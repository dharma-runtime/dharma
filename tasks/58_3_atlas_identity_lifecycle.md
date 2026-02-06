# Task 58.3: Atlas Identity Lifecycle + Verification

## Goal
Add lifecycle evaluation (active/suspended/revoked) and verification semantics for Atlas identities.

## Dependencies
- Task 58.1 (schema/types)
- Task 58.2 (genesis enforcement)

## Scope
- Evaluate identity status from the subject chain.
- Update peer verification to consider lifecycle state.
- Ensure revoked/suspended identities fail verification.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/identity.rs`
  - Add `IdentityStatus` enum: `Active | Suspended | Revoked`.
  - Add `identity_status(env, subject) -> Status`.
  - Add `is_verified_identity(env, subject) -> bool` with PRD rules.
- `dharma-core/src/net/peer.rs`
  - Replace legacy verification logic with `is_verified_identity`.
  - Ensure lifecycle assertions are signed by root key only.
- `dharma-core/src/net/server.rs`
  - Ensure `Handshake complete. Auth verified.` only when status is Active.

## Test Plan (Detailed)
### Unit Tests
- `atlas_status_transitions`:
  - Active -> suspend -> active -> revoke -> (no re-activate).
- `atlas_verified_only_if_active`:
  - Active => verified; Suspended/Revoked => not verified.

### Integration Tests
- `handshake_with_suspended_identity`:
  - Peer is suspended => handshake unverified.

### Negative/Security Tests
- Lifecycle actions signed by non-root key rejected.

