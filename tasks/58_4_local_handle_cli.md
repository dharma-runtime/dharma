# Task 58.4: Local Handle Persistence & UX Guardrails

## Goal
Enforce the PRD rule that local handles never leave the device and remain a UX-only concept.

## Dependencies
- Task 58 (spec)

## Scope
- Store local handle in local config only.
- Prevent accidental sync of local handle.
- Surface in CLI/REPL as a local alias.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/identity_store.rs`
  - Store local handle in config (e.g. `dharma.toml`).
- `dharma-cli/src/repl/core.rs`
  - Display local handle in `whoami`/`identity status`.
- `dharma-core/src/net/ingest.rs`
  - Explicitly disallow any assertion field that attempts to set local handle (if present).

## Test Plan (Detailed)
### Unit Tests
- `local_handle_persisted_in_config`:
  - init identity stores handle locally.

### Negative Tests
- `local_handle_never_in_assertions`:
  - Assertions containing `local_handle` field rejected.

