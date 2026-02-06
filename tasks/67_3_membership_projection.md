# Task 67.3: Key Membership Projection

## Goal
Materialize key membership rows so downstream systems can join without recomputing embeddings.

## Scope
- Contract: `std.commerce.demand.membership` (or domain-agnostic equivalent).
- Projection from item snapshots -> membership rows.
- Rebuild/backfill on version upgrade.
- Queries: `GetItemKey`, `ListItemsInKey`, `KeyPopulationStats`.

## Out of Scope
- Forecasting or recommendation outputs.

## Test Plan
- Projection determinism test (rebuild twice = identical results).
- Membership correctness on sample snapshot set.
- Version upgrade rebuild produces new space without overwriting old.

## Acceptance Criteria
- Membership projection emits rows for each snapshot.
- Keys usable as join keys in DHARMA-Q.
