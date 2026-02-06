# Task 68: Forecast Pipeline Epic

## Goal
Deliver a declarative, versioned forecasting pipeline DSL and runtime capable of producing quantile forecasts with hierarchical fallback.

## Why
Commerce forecasting must be reproducible, explainable, and cold-start resilient. It also becomes a core input to availability and planning.

## Scope
- Task 68.1: Forecast pipeline DSL + compiler artifact.
- Task 68.2: Base projections (observed_demand_bucket, calendar, pricing, etc.).
- Task 68.3: Runtime engine (joins, features, hierarchy).
- Task 68.4: Model backends + calibration + backtest.
- Task 68.5: Forecast output projections + queries.
- Task 68.6: Serving API + error handling.

## Out of Scope
- Recommendation engine.

## Dependencies
- Epic 67 (Keyspace + Embedding) for keyspace runtime and membership projections.

## Acceptance Criteria
- Forecast pipeline DSL compiles to a plan.
- Forecast outputs emitted for a sample dataset.
- Backtest + coverage metrics available.
