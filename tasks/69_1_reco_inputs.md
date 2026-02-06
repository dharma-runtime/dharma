# Task 69.1: Reco Input Contracts + Projections

## Goal
Provide deterministic input data for recommendation pipelines.

## Scope
- `std.reco.event` (interaction log)
- `std.reco.basket`
- `std.reco.item_features`
- `std.reco.item_servability` (availability-aware filter)
- `std.reco.trending_by_key` (cold start prior)

## Dependencies
- Task 67.2: keyspace runtime built-ins (`key_from_subject`).
- Task 67.3: key membership projection for DemandKey features/prior.

## Test Plan
- Projection determinism on sample sessions.
- Correct basket/session grouping.
- Availability and servability coverage.

## Acceptance Criteria
- Input contracts populate deterministically.
