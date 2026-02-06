# Task 65.6: Atlas Domain Protocol Interface

## Goal
Define a protocol interface for Atlas domain contracts and enforce compatibility.

## Dependencies
- Task 59 (Atlas domain)
- Task 65.1 (protocol registry)
- Task 65.2 (implements validation)

## Scope
- Define `std.protocol.atlas.domain@1`:
  - Required fields: `domain`, `owner`, `parent`, membership collections.
  - Required actions: `atlas.domain.genesis`, `invite`, `request`, `approve`, `revoke`, `leave`.
  - Required semantics: parent authorization, ownership default, transfer policy default.
- Update `std.atlas.domain.dhl` to declare `implements: std.protocol.atlas.domain@1`.
- Align domain state evaluation helpers to the protocol interface.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/protocols/atlas_domain.rs`
  - Interface spec + membership semantics helpers.
- `dharma-core/src/domain.rs`
  - Ensure state fold aligns with protocol semantics.
- `contracts/std/atlas_domain.dhl`
  - Add `implements` frontmatter.

## Test Plan (Detailed)
### Unit Tests
- `atlas_domain_protocol_required_actions`
- `atlas_domain_protocol_parent_authorization`
- `atlas_domain_protocol_membership_chain_validity`

