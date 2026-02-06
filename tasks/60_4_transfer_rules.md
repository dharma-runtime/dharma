# Task 60.4: Ownership Transfer Rules

## Goal
Implement ownership transfer (immediate or propose/accept) as contract-defined behavior.

## Dependencies
- Task 60.1 (ownership metadata)
- Task 59.1 (domain contract schema)

## Scope
- `subject.transfer` and `subject.transfer.propose/accept` assertions.
- Contract-defined rules for allowed transfer modes.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/builtins.rs`
  - Add schema types for transfer actions.
- `dharma-core/src/ownership.rs`
  - Implement transfer state + validation helpers.
- `dharma-core/src/net/ingest.rs`
  - Enforce transfer rules at ingest.

## Test Plan (Detailed)
### Unit Tests
- `transfer_forbidden_by_default`:
  - Without contract rule => reject.
- `transfer_immediate`:
  - Owner changes on single action.
- `transfer_propose_accept`:
  - Ownership changes only after accept.

