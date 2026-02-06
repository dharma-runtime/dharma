# Task 68.2: Forecast Input Projections

## Goal
Provide deterministic data sources required by the forecast pipeline.

## Scope
- `std.commerce.forecast.observed_demand_bucket` projection.
- `std.commerce.calendar.bucket` projection (holiday + payday).
- `std.commerce.pricing.bucket` projection.
- `std.commerce.availability.purchasable_bucket` projection (optional v1).
- Optional: weather + marketing buckets stubbed.

## Dependencies
- Task 67.2: keyspace runtime built-ins (`key_from_subject`).
- Task 67.3: key membership projection for DemandKey joins.

## Test Plan
- Projection correctness tests on sample data.
- Censoring/blocked reason propagation.
- Coverage of required regions.

## Acceptance Criteria
- Input contracts populate deterministically.
- Missing data handling documented and enforced.
