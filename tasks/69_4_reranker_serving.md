# Task 69.4: Reranker + Serving Filters

## Goal
Apply business rules, diversity, and availability to candidate sets.

## Scope
- Reranker (GBM or linear) with features: margin, availability, diversity.
- Filters: exclude in cart, require available, temp-chain compatibility.
- Output contract: `std.reco.item_to_item` (optional) + serving API.
- Serving API schema (request/response + error codes) and caching notes.

## Test Plan
- Rerank output determinism.
- Filter correctness for availability/constraints.

## Acceptance Criteria
- Serving output honors filters and diversity constraints.
- Serving API defines explicit error responses for missing keys and unavailable items.
