# Task 68.6: Forecast Serving API + Error Handling

## Goal
Expose forecast outputs via a stable API with explicit error handling and caching guidance.

## Scope
- Define REST/gRPC endpoints for forecast retrieval.
- Request/response schemas for date range + quantiles.
- Error codes: KEY_UNKNOWN, INSUFFICIENT_DATA, VERSION_DEPRECATED.
- Caching guidance (by key + date range + pipeline version).

## Out of Scope
- Model training and forecast generation.

## Test Plan
- API schema contract tests.
- Error handling tests on missing key/version.
- Cache key correctness tests.

## Acceptance Criteria
- Serving API endpoints are documented and versioned.
- Errors are explicit and deterministic.
