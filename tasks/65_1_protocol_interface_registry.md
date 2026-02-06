# Task 65.1: Protocol Interface Registry (Core)

## Goal
Define a core registry of protocol interfaces with versioned IDs and compatibility rules.

## Scope
- Add a new module (e.g. `dharma-core/src/protocols/`) containing:
  - `ProtocolId` (name + version)
  - `ProtocolInterface` (required fields, actions, enums, semantics)
  - `ProtocolCompatibility` helpers
- Provide interfaces for contacts, IAM, Atlas identity, Atlas domain.
- Expose an API to retrieve interface specs by ID.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/protocols/mod.rs`
  - Define `ProtocolId` + registry.
- `dharma-core/src/protocols/contacts.rs`
  - Required fields/actions + relation enum semantics.
- `dharma-core/src/protocols/iam.rs`
  - Required fields/actions + privacy field list.
- `dharma-core/src/protocols/atlas_identity.rs`
  - Required assertions + lifecycle semantics.
- `dharma-core/src/protocols/atlas_domain.rs`
  - Required actions/fields + membership semantics.

## Test Plan (Detailed)
### Unit Tests
- `protocol_id_parse_roundtrip`
- `contacts_interface_minimal_schema_ok`
- `iam_interface_minimal_schema_ok`
- `atlas_identity_protocol_invariants_basic`
- `atlas_domain_protocol_invariants_basic`

