# Task 63.3: Domain Compromise Handling

## Goal
Mark a domain as compromised and reject all new actions thereafter (no recovery in v1).

## Dependencies
- Task 63 (spec)
- Task 59.1 (domain contract)
- Task 62.1 (domain key hierarchy) [soft]

## Scope
- `domain.compromised` assertion.
- Enforcement in ingest and router.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/builtins.rs`
  - Add `domain.compromised` schema type.
- `dharma-core/src/domain.rs`
  - Track compromised state.
- `dharma-core/src/net/ingest.rs`
  - Reject new actions for compromised domains.

## Test Plan (Detailed)
### Unit Tests
- `domain_compromised_terminal`:
  - Actions rejected after compromise.

