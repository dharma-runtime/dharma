# Task 59.3: Acting Context Enforcement

## Goal
Enforce the PRD rule that actions must specify a single acting context (identity or domain) and that context must be valid.

## Dependencies
- Task 59.2 (membership state)

## Scope
- Add required metadata fields for acting context.
- Enforce at ingest time.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/assertion.rs`
  - Define standard meta keys:
    - `acting_identity`
    - `acting_domain`
    - `acting_role` (optional)
- `dharma-core/src/net/ingest.rs`
  - Enforce exactly one acting context.
  - If acting as domain, verify membership + role.

## Test Plan (Detailed)
### Unit Tests
- `acting_context_requires_single`:
  - Both identity+domain => reject.
  - Neither specified => reject.
- `acting_domain_requires_membership`:
  - Non-member => reject.
- `acting_role_requires_role`:
  - Missing role in membership => reject.

