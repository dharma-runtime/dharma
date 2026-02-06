# Task 59: std.atlas.domain Contract + Membership + Hierarchy (PRD v1)

## Goal
Define and implement the domain contract (`std.atlas.domain`) that governs membership, roles/scopes, ownership defaults, and domain hierarchy.

## Why
- The PRD requires authority to be defined by domains, not infrastructure.
- Membership rules must be contract-defined and auditable.
- Domain hierarchy must be explicit and verifiable.

## Scope
- Contract schema + logic for domain membership and hierarchy.
- Integration with directory (domain registry) and relay validation.
- Enforcement hooks for membership checks (no simultaneous self+domain acting).

## Specification

### 1) Domain Subject + Contract
- Each Atlas identity is a **domain subject** governed by `std.atlas.domain`.
- Domain subject contains domain name and policy state (membership, roles, scopes).
- Domain hierarchy is derived from dotted names (parent is prefix).

### 2) Domain Genesis
- Assertion type: `atlas.domain.genesis`.
- Required fields:
  - `domain` (string)
  - `owner` (identity key)
  - `parent` (optional, for nested domains)
- Validation:
  - Parent domain must authorize child creation if `parent` is set.

### 3) Membership Actions
- `atlas.domain.invite`:
  - target identity, role(s), scope(s), expiration.
- `atlas.domain.request`:
  - target identity requests membership.
- `atlas.domain.approve`:
  - domain owner approves membership.
- `atlas.domain.revoke`:
  - remove membership or role.
- `atlas.domain.leave`:
  - member leaves domain.

### 4) Membership Properties
- Role(s), scope(s), time bounds, revocation conditions.
- No fixed global roles; contract defines meaning.

### 5) Acting Context Rule
- Assertion must not act simultaneously as user and domain.
- All actions must specify acting context:
  - `acting_identity`
  - `acting_domain`
  - `role` (optional)
- Validation ensures context is consistent with membership.

### 6) Domain Registry Integration
- `fabric.domain.register` must reference the current domain owner.
- For nested domains, parent authorization is mandatory and auditable.

## Implementation Steps
1. Define `std.atlas.domain` schema and contract rules.
2. Add new assertion types to builtins and runtime validation.
3. Implement membership state computation (roles/scopes/time).
4. Enforce acting context constraints in ingest/contract.
5. Integrate with DirectoryState validation (domain register + policy).

## Test Plan (Detailed)

### Unit Tests
- `domain_genesis_requires_owner`:
  - Missing owner => reject.
- `domain_hierarchy_requires_parent_auth`:
  - child without parent authorization => reject.
- `membership_invite_approve_flow`:
  - invite -> approve -> member active.
- `membership_revoke`:
  - revoke removes role + access.
- `membership_expiry`:
  - expired membership => denied.

### Integration Tests
- `domain_register_matches_owner`:
  - Directory register must match domain contract owner.
- `acting_context_enforced`:
  - user action with domain context but no membership => reject.
  - action with both self+domain => reject.

### Negative/Security Tests
- Non-owner attempts approve/revoke => reject.
- Stale parent authorization => reject.
- Invalid role/scopes outside contract => reject.

### Determinism/Replay Tests
- Replay membership log -> deterministic state.
- Snapshot rebuild -> identical membership state.

