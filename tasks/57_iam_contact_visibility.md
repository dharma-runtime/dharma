# Task 57: IAM Contact-Gated Visibility (Fabric Testbed)

## Goal
Make **private IAM fields** visible **only** to Accepted contacts (and the owner),
using `std.io.contacts` as the authorization source. This is the first
end-to-end testbed for Fabric enforcement.

## Why
- IAM is the **source of truth** for contact info.
- Contacts should automatically receive updates to identity data.
- Fabric needs a concrete, real-world enforcement rule to validate the
  access-control pipeline.

## Scope (V1)
- Enforce visibility **at Fabric execution time** (query/action responses).
- Do **not** change low-level sync behavior yet (no access-controlled replication).
- Redaction is **field-level**: private fields are omitted unless authorized.

## Specification

### 1. IAM Fields
- Treat `display_name`, `email`, and `phone` as **private** fields.
- Public IAM fields remain visible to everyone.
- If the requester is **not authorized**, private fields are omitted or set to `null`.

### 2. Authorization Rule
A requester `R` can see private IAM fields of identity `I` if:

1. `R == I` (owner), OR
2. There exists a **Contact** between `R` and `I` with `relation == Accepted`.

### 3. Contact Resolution
- The Contact subject for a pair is deterministic:
  ```
  subject_id = hash("contact" + min(a,b) + max(a,b))
  ```
- Use `std.io.contacts.Contact` state to check `relation`.

### 4. Enforcement Points
- **ExecQuery / QueryFast / QueryWide** for IAM subjects must apply field filtering.
- **ExecAction** responses that return state must apply the same filter.
- **Fetch** of IAM objects via Fabric should apply redaction (unless authorized).

### 5. Redaction Strategy
- Default: omit private fields from the response payload.
- If schema requires presence, set to `null` (explicit redaction).

## Implementation Steps
1. Update IAM schema/contract so `display_name`, `email`, `phone` are **not public**.
2. Implement `contacts::is_accepted(a, b) -> bool` using deterministic subject id.
3. Add a Fabric visibility filter for IAM responses:
   - owner sees all
   - accepted contact sees all
   - others see only public fields
4. Add tests:
   - Owner can see private fields
   - Accepted contact can see private fields
   - Non-contact sees redacted fields
   - Declined/Blocked does not grant access

## Tests
- Unit tests for deterministic contact subject id.
- Integration tests in Fabric query path (mock tokens + identities + contacts).

## Future (Out of Scope)
- Access-controlled replication (sync-level filtering).
- Fine-grained policy rules (per-field or per-role privacy).
