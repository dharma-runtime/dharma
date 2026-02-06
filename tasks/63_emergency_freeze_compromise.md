# Task 63: Emergency Freeze + Compromise Handling (PRD v1)

## Goal
Add a domain emergency freeze mechanism and codify compromise handling in v1.

## Why
- PRD requires explicit freeze with no new facts accepted.
- Device revocation must be enforced.
- Domain admin compromise must mark the domain as unsafe (no recovery in v1).

## Scope
- `domain.freeze` and (optional) `domain.unfreeze` actions.
- Enforcement in ingest.
- Device key revoke enforcement.
- Domain compromise flag.

## Specification

### 1) Emergency Freeze
- Assertion type: `domain.freeze`.
- Effects:
  - No new facts accepted for the frozen domain.
  - Read access remains unchanged.
  - Freeze is explicit and logged.
- Optional `domain.unfreeze` (if allowed by contract).

### 2) Device Compromise
- Device keys can be revoked via identity chain.
- Revoked device keys cannot sign accepted actions.

### 3) Domain Admin Compromise
- Assertion type: `domain.compromised`.
- Once asserted, domain is considered lost.
- No recovery in v1; all new writes are rejected.

## Implementation Steps
1. Add schema/contract actions for freeze/compromise.
2. Enforce freeze in ingest for domain actions.
3. Enforce device key revocation in signer checks.
4. Add domain compromise guard (reject new actions).

## Test Plan (Detailed)

### Unit Tests
- `freeze_blocks_new_facts`:
  - After freeze, action acceptance fails.
- `freeze_does_not_block_reads`:
  - Query/read still works.
- `unfreeze_restores_acceptance` (if enabled).
- `device_revoke_blocks_signer`:
  - Revoked device key => action rejected.
- `domain_compromised_is_terminal`:
  - No actions accepted after compromise.

### Integration Tests
- Multi-node sync propagates freeze -> all nodes enforce.
- Relay continues to store but clients reject actions.

### Negative/Security Tests
- Non-owner attempts freeze/compromise => reject.
- Stale freeze events ignored if not properly signed.

### Determinism/Replay Tests
- Replay freeze + actions => deterministic rejection.
- Snapshot rebuild => consistent frozen state.

