# Task 65.5: Atlas Identity Protocol Interface

## Goal
Define a protocol interface for Atlas identity lifecycle and expose it as a first-class, versioned spec.

## Dependencies
- Task 58 (Atlas identity)
- Task 65.1 (protocol registry)

## Scope
- Define `std.protocol.atlas.identity@1`:
  - Required assertion types: `atlas.identity.genesis`, `atlas.identity.activate`, `atlas.identity.suspend`, `atlas.identity.revoke`.
  - Required fields in genesis (owner key, atlas name, schema/contract ids).
  - Lifecycle semantics: Revoked is terminal; suspended blocks verification.
- Expose helper APIs to query identity status via the protocol interface.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/protocols/atlas_identity.rs`
  - Interface spec + lifecycle helpers.
- `dharma-core/src/identity.rs`
  - Align existing logic to protocol interface, expose `identity_status_v1` helper.
- `dharma-core/src/builtins.rs`
  - Mark built-in identity schema as implementing `std.protocol.atlas.identity@1`.

## Test Plan (Detailed)
### Unit Tests
- `atlas_identity_protocol_genesis_fields_required`
- `atlas_identity_protocol_revoked_terminal`
- `atlas_identity_protocol_suspend_unverified`

