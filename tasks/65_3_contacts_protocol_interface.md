# Task 65.3: Contacts Protocol Interface + Resolver

## Goal
Implement a core, deterministic resolver for `std.protocol.contacts@1`, and use it for IAM visibility gating.

## Dependencies
- Task 57 (IAM contact-gated visibility)
- Task 65.1 (protocol registry)
- Task 65.2 (implements validation)

## Scope
- Add `contacts` module to `dharma-core`:
  - `contact_subject_id(a, b)` (deterministic)
  - `relation(store, a, b)` (fold assertions using protocol semantics)
  - `is_accepted(a, b)` convenience helper
- Update IAM visibility enforcement to use the resolver.
- Update `std.io.contacts.dhl` to declare `implements: std.protocol.contacts@1`.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/contacts.rs` (new)
  - Deterministic subject id + reducer.
- `dharma-core/src/fabric/router.rs` (or IAM query path)
  - Apply contact gating using `contacts::relation`.
- `contracts/std/io_contacts.dhl`
  - Add `implements` frontmatter.

## Test Plan (Detailed)
### Unit Tests
- `contact_subject_id_is_deterministic`
- `contact_relation_request_accept`
- `contact_relation_block_unblock`
- `contact_relation_decline`

### Integration Tests
- IAM visibility: owner vs accepted contact vs non-contact (Fabric query path).

