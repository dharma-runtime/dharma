# Task 60: Ownership, Attribution, Sharing & Transfer (PRD v1)

## Goal
Implement explicit ownership, creator attribution, sharing, and revocation as auditable facts with contract enforcement.

## Why
- PRD requires ownership semantics and explicit, auditable sharing/revocation.
- Access must be cryptographically enforceable and deterministic.

## Scope
- Ownership metadata on subjects.
- Creator attribution.
- Sharing and revocation assertions (direct + role-based).
- Transfer rules (contract-defined).

## Specification

### 1) Ownership Metadata (Required)
- Each subject must record:
  - `owner` (identity or domain)
  - `creator_identity`
  - `acting_domain`
  - `role` (if applicable)
- Owner is **exclusive**.
- Default owner = domain (if not explicitly set).

### 2) Ownership Transfer
- Assertion type: `subject.transfer` (immediate) or `subject.transfer.propose` + `subject.transfer.accept`.
- Contract defines whether transfer is allowed, and which mode.

### 3) Sharing Model
- Assertions:
  - `share.grant` (direct identity or role-based)
  - `share.revoke`
  - `share.public` (explicit public read)
- Sharing scopes:
  - fields
  - actions
  - queries
- Access defaults are contract-defined.

### 4) Revocation Semantics
- Revocation prevents future access via Dharma.
- Previously decrypted data may persist outside the system (UI disclaimer required).

### 5) Enforcement Points
- Ingest: validate share/transfer permissions.
- Fabric router/runtime: enforce access for read/execute/query.
- Sync remains blind; enforcement happens at execution/read.

### 6) Scope Boundaries (v1)
- No subject-level crypto sharing independent of domains.
- Sharing is enforced via domain membership + grants.

## Implementation Steps
1. Extend assertion metadata for owner/creator/acting domain.
2. Add ownership enforcement to ingest pipeline.
3. Implement share/revoke assertions and state evaluation.
4. Add public subject toggle as explicit fact.
5. Enforce access in Fabric (read/query/execute).

## Test Plan (Detailed)

### Unit Tests
- `owner_default_to_domain`:
  - No explicit owner => owner = domain.
- `owner_exclusive_no_joint`:
  - Attempt to set multiple owners => reject.
- `creator_attribution_recorded`:
  - On subject creation, creator fields are present.
- `transfer_forbidden_by_default`:
  - Transfer attempt without contract rule => reject.
- `transfer_allowed_flow`:
  - Propose -> accept => owner changes.

### Sharing Tests
- `share_grant_direct_identity`:
  - Granted identity can read/query; others denied.
- `share_grant_role_based`:
  - Role member gains access; non-member denied.
- `share_revoke_blocks_future`:
  - After revoke, access denied (new epoch).
- `share_public_explicit`:
  - Public toggle makes read world-readable; off restores protection.

### Integration Tests
- Fabric query path enforces field/action/query scopes.
- Execution path denies access when sharing rules not satisfied.

### Negative/Security Tests
- Non-owner attempts to share/revoke => reject.
- Stale or revoked role attempting access => denied.
- Invalid scope keys => reject.

### Determinism/Replay Tests
- Replay ownership + sharing assertions => deterministic access state.
- Snapshot rebuild => identical access map.

