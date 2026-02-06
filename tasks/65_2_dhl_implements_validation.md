# Task 65.2: DHL `implements` Metadata + Compiler Validation

## Goal
Allow DHL contracts to declare protocol implementations and enforce compatibility at compile time.

## Scope
- Parse `implements` list from DHL frontmatter.
- Extend AST + schema compiler to carry protocol IDs.
- Validate required fields/actions/enums against protocol interfaces.
- Emit protocol list into compiled artifacts for runtime inspection.

## File-level TODOs (Implementation Tickets)
- `dharma-cli/src/pdl/parser.rs`
  - Parse `implements` frontmatter key.
- `dharma-cli/src/pdl/ast.rs`
  - Store `implements: Vec<ProtocolId>`.
- `dharma-cli/src/pdl/typecheck.rs`
  - Validate interface compatibility.
- `dharma-cli/src/pdl/codegen/schema.rs`
  - Emit protocol list in schema metadata (or sidecar manifest).
- `dharma-core/src/schema.rs` (or new metadata struct)
  - Decode/encode protocol list.

## Test Plan (Detailed)
### Unit Tests
- `dhl_parses_implements_list`
- `compile_rejects_missing_required_action`
- `compile_rejects_missing_required_field`
- `compile_allows_extra_fields_and_actions`

