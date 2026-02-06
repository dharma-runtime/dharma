# Task 62.3: Revocation + Key Distribution

## Goal
Prevent revoked identities from receiving new epoch keys and enforce distribution via domain membership only.

## Dependencies
- Task 62.2 (epoch rotation)
- Task 59.2 (membership state)

## Scope
- Key grant assertions for members.
- Revocation removes future key grants.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/domain.rs`
  - Track member eligibility for key grants.
- `dharma-core/src/net/ingest.rs`
  - Enforce that key grants are only to active members.
- `dharma-core/src/keys.rs`
  - Envelop SDK with recipient identity key.

## Test Plan (Detailed)
### Integration Tests
- `revoked_identity_cannot_decrypt_new_epoch`:
  - Revoked member denied new epoch keys.
- `active_member_receives_key_grant`:
  - Active member gets SDK envelope.

