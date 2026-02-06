# Task 66: ECommerce Epic

## Goal
Ship the missing infrastructure that turns the commerce DHL suite into a usable product surface.

## Why
We now have comprehensive commerce contracts + DHARMA‑Q queries, but they depend on:
- Projection pipelines to populate derived tables.
- Core entity contracts (supplier/warehouse) referenced by inventory.
- Tests that validate projections and query behavior end‑to‑end.

## Scope
- Task 66.1: Projection writers + rebuild pipelines.
- Task 66.2: Core entity contracts (supplier + warehouse) or mapping to existing std.biz.*.
- Task 66.3: Projection + query correctness tests.

## Out of Scope
- UI implementation.
- Billing/plan enforcement.

## Acceptance Criteria
- All three tasks are implemented and verified.
- Commerce DHL suite compiles and projection tables are populated by pipelines.
- Tests exist for projection correctness and key queries.
