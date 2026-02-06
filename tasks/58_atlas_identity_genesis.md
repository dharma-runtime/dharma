# Task 58: Atlas Identity + Genesis Phase + Lifecycle (PRD v1)

## Goal
Implement the PRD v1 Atlas identity model with a strict Genesis Phase, a verifiable lifecycle (active/suspended/revoked), and local-only handles.

## Why
- The PRD requires a first-class Atlas identity with explicit lifecycle semantics.
- Genesis must be enforced by the kernel (not by contracts) to avoid bootstrap paradoxes.
- Verification must be deterministic and auditable.

## Scope
- Atlas identity creation, verification, and lifecycle enforcement.
- Local handle stays local and never syncs unless explicitly linked.
- Genesis rules are kernel-level and non-upgradeable.

## Specification

### 1) Atlas Identity Naming
- Atlas identity namespace: `person.<tld>.<handle>_<pubkey_suffix>`.
- The namespace string is a **display/claim** in the identity chain, not the SubjectId.
- SubjectId is still cryptographic; name is validated as a field in assertions.

### 2) Genesis Phase (Kernel Rule)
- New assertion type: `atlas.identity.genesis` (reserved, kernel-only).
- Allowed only when:
  - The subject has **no prior assertions** (seq must be 1).
  - The assertion type is `atlas.identity.genesis`.
- Genesis assertions:
  - Are **non-repeatable** for a subject.
  - Are **not contract-upgradeable**.
  - Are accepted only if the subject has no prior accepted facts.

### 3) Atlas Identity Lifecycle
- New lifecycle assertions:
  - `atlas.identity.activate`
  - `atlas.identity.suspend`
  - `atlas.identity.revoke`
- Rules:
  - `revoke` is terminal (no further activation allowed).
  - `suspend` may be lifted by `activate`.
  - Only the identity root key may change lifecycle state.

### 4) Verification (v1)
- Identity is **verified** iff:
  - genesis exists and is valid
  - status is **active**
  - not revoked
  - not suspended

### 5) Local Handle
- Local handle is stored only in local config/keystore.
- It never appears on the network unless explicitly linked by a signed assertion.

### 6) One Atlas Identity per Local User
- `dh identity init` must refuse if a local Atlas identity already exists.
- The rule is local (not global): remote identities may exist in the store.

## Implementation Steps
1. Add new assertion types and schemas for `atlas.identity.*`.
2. Enforce Genesis Phase rules in ingest (kernel-level, before contract).
3. Add lifecycle evaluation helpers in `identity.rs` (active/suspended/revoked).
4. Update handshake verification to reflect lifecycle status.
5. Update `identity_store::init_identity` to emit `atlas.identity.genesis`.
6. Add local config guard: exactly one local Atlas identity.

## Test Plan (Detailed)

### Unit Tests
- `atlas_genesis_only_once`:
  - Create genesis for subject -> accepted.
  - Attempt second genesis -> rejected (Validation).
- `atlas_genesis_requires_empty_subject`:
  - Write a non-genesis assertion first -> genesis rejected.
- `atlas_genesis_requires_seq1`:
  - Genesis with seq != 1 -> rejected.
- `atlas_status_transitions`:
  - Active -> suspend -> active -> revoke -> active (rejected).
- `atlas_verified_only_if_active`:
  - Active => verified.
  - Suspended => not verified.
  - Revoked => not verified.

### Integration Tests
- `identity_init_singleton_local`:
  - `dh identity init` once -> ok.
  - second init -> error.
- `handshake_with_suspended_identity`:
  - Peer suspends identity -> handshake shows unverified; overlay access rules apply.
- `sync_identity_chain_validates`:
  - Sync remote identity chain; verify status computed from chain.

### Negative/Security Tests
- Invalid signature on genesis => reject.
- Genesis with invalid namespace format => reject.
- Lifecycle action signed by non-root key => reject.

### Determinism/Replay Tests
- Replay genesis + lifecycle assertions => deterministic status outcome.
- Snapshot rebuild => same lifecycle status.

