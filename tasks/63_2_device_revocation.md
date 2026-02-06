# Task 63.2: Device Key Revocation Enforcement

## Goal
Ensure revoked device keys cannot sign accepted actions.

## Dependencies
- Task 63 (spec)
- Task 58.3 (identity lifecycle)

## Scope
- Enforce device revocation in signer checks.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/identity.rs`
  - Add device key revocation check helper.
- `dharma-core/src/net/ingest.rs`
  - Reject actions signed by revoked device keys.

## Test Plan (Detailed)
### Unit Tests
- `device_revoke_blocks_signer`:
  - Revoked device cannot sign accepted action.

