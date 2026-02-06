# Task 68.1: Forecast Pipeline DSL

## Goal
Implement `forecast_pipeline` DSL, parser, and compiler artifact `.forecast_pipeline`.

## Scope
- DSL parsing for target/covariates/features/hierarchy/model/calibration/backtest/emit.
- Type validation of joins and field references.
- Compiler emits resolved plan with config_hash.

## Test Plan
- Parse/compile positive and negative fixtures.
- Deterministic compilation output.
- Reject invalid missing policies or join keys.

## Acceptance Criteria
- `forecast_pipeline` blocks compile with clear errors.
- Compiled plan is versioned and deterministic.
