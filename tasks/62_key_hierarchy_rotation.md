# Task 62: Domain Key Hierarchy, Subject Keys, Rotation & Epochs (PRD v1)

## Goal
Implement the PRD v1 hierarchical key model with rotation and epoch semantics, without rewriting subject data.

## Why
- PRD requires domain-rooted keys and rotation without changing subject identity.
- Revocation must prevent future access without retroactive erasure.

## Scope
- Domain root key, KEK, subject data keys.
- Key envelope assertions and epoch metadata.
- Rotation and revocation behavior.

## Specification

### 1) Key Hierarchy
- Domain Root Key (DRK)
  - top-level authority per domain.
- Domain Key Encryption Key (KEK)
  - derived/enveloped from DRK.
  - rotatable without changing subject identity.
- Subject Data Key (SDK)
  - derived/enveloped from KEK.
  - per-subject, per-epoch.

### 2) Epochs
- Each subject has an encryption epoch identifier.
- New facts use the latest epoch.
- Old epochs remain readable for those who already have keys.

### 3) Rotation
- `domain.key.rotate` creates a new KEK epoch.
- Does NOT rewrite existing subject data.
- New SDKs are derived/enveloped for the new epoch.

### 4) Revocation
- Revoked identities do not receive new epoch keys.
- Revocation prevents future access via Dharma.
- Prior decrypted copies may exist outside the system.

### 5) Scope Boundary (v1)
- No subject-level crypto sharing independent of domains.
- SDKs are distributed only via domain membership.

## Implementation Steps
1. Define key envelope assertion types (`domain.key.rotate`, `subject.key.bind`, `member.key.grant`).
2. Add epoch metadata to encrypted assertions.
3. Implement key derivation + envelope encryption.
4. Enforce revocation by withholding new epoch grants.
5. Add key cache and lookup paths for decrypting envelopes.

## Test Plan (Detailed)

### Unit Tests
- `key_derivation_stable`:
  - Same inputs -> same key; different epoch -> different key.
- `epoch_metadata_roundtrip`:
  - Encode/decode epoch in envelope metadata.

### Rotation Tests
- `rotation_does_not_rewrite_data`:
  - After rotate, old objects unchanged (count/hash stable).
- `new_epoch_used_for_new_facts`:
  - New assertions after rotate use new epoch id.

### Revocation Tests
- `revoked_identity_cannot_decrypt_new_epoch`:
  - Access denied to new epoch data.
- `revoked_identity_can_read_old_epoch`:
  - If old key cached, old data readable.

### Integration Tests
- Domain member joins -> receives SDK for current epoch.
- Member revoked -> does not receive new epoch keys.

### Negative/Security Tests
- Invalid key envelope => reject.
- Attempt to share SDK outside domain membership => reject.

