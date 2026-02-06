# Task 63.1: Emergency Freeze

## Goal
Implement domain emergency freeze that blocks new facts while allowing reads.

## Dependencies
- Task 63 (spec)
- Task 59.1 (domain contract)

## Scope
- `domain.freeze` assertion.
- Enforcement in ingest.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/builtins.rs`
  - Add `domain.freeze` schema type.
- `dharma-core/src/domain.rs`
  - Track freeze state.
- `dharma-core/src/net/ingest.rs`
  - Reject new actions when domain frozen.

## Test Plan (Detailed)
### Unit Tests
- `freeze_blocks_new_facts`:
  - After freeze, acceptance fails.
- `freeze_allows_read`:
  - Reads still succeed.

