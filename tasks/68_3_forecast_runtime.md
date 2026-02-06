# Task 68.3: Forecast Runtime Engine

## Goal
Implement runtime execution for compiled forecast pipelines.

## Scope
- Deterministic joins and missing-data policies.
- Feature builder (lags, rolling stats, onehot, interactions, scaling).
- Hierarchy fallback logic (DemandKey levels).
- Run orchestration (batch runs, run_id, lineage).
- Feature storage strategy (on-demand in v1; cache projection optional v2).

## Test Plan
- Deterministic run with fixed seed/data.
- Hierarchy fallback correctness.
- Feature correctness vs expected vectors.

## Acceptance Criteria
- Pipeline runs end-to-end on synthetic dataset.
- Outputs are reproducible across runs.
