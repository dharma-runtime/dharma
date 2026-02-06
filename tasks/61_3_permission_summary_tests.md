# Task 61.3: Permission Summary Tests & Benchmarks

## Goal
Add deterministic tests and perf checks for summary-based permission evaluation.

## Dependencies
- Task 61.2 (router cache)

## Scope
- Unit + integration tests.
- Microbench for router throughput.

## File-level TODOs (Implementation Tickets)
- `dharma-test/src/lib.rs`
  - Add integration tests for summary behavior.
- `dharma-test/` (new bench harness)
  - Add microbench for router summary checks.

## Test Plan (Detailed)
### Integration Tests
- `summary_cache_keying`:
  - Contract version/role/action changes invalidate cache.

### Performance Tests
- `router_summary_bench`:
  - Compare summary vs full validation throughput.

