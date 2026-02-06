# Task 68.5: Forecast Output Projections + Queries

## Goal
Emit forecast buckets and expose internal queries.

## Scope
- Contract: `std.commerce.forecast.demand_forecast_bucket`.
- Queries: GetForecast, GetForecastForItem, ExplainForecast, CoverageReport.
- Linkage to keyspace versions and pipeline versions.
- Serving API schema (request/response + error codes).

## Test Plan
- Projection row shape tests.
- Query correctness on sample run.

## Acceptance Criteria
- Forecast outputs stored with run_id + lineage.
- Queries return expected buckets and quantiles.
- Serving API defines explicit error responses for missing/invalid keys.
