# Task 61.2: Router Cache + Fast Reject

## Goal
Add summary-based early reject in router/ingest without bypassing full validation.

## Dependencies
- Task 61.1 (summary artifact)

## Scope
- Cache summaries by (contract version, role, action).
- Reject early when summary denies.

## File-level TODOs (Implementation Tickets)
- `dharma-core/src/fabric/router.rs`
  - Add summary cache and fast reject path.
- `dharma-core/src/net/ingest.rs`
  - Add summary checks before contract guard evaluation.

## Test Plan (Detailed)
### Integration Tests
- `summary_denies_fast_reject`:
  - Router rejects without WASM.
- `summary_allows_but_contract_denies`:
  - Final rejection after full validation.

