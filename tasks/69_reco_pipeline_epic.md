# Task 69: Recommendation Pipeline Epic

## Goal
Ship a versioned, availability-aware two-tower recommendation system with DHL governance.

## Why
Commerce productization needs relevance + cold start handling + availability awareness under strict determinism/lineage.

## Scope
- Task 69.1: Reco input contracts + projections.
- Task 69.2: reco_pipeline DSL + compiler artifacts.
- Task 69.3: Two-tower training + ANN candidate retrieval.
- Task 69.4: Reranker + business rules + serving filters.
- Task 69.5: Evaluation + metrics + tests.
- Task 69.6: ANN index lifecycle management.

## Out of Scope
- Forecasting system.

## Dependencies
- Epic 67 (Keyspace + Embedding) for DemandKey membership and item features.

## Acceptance Criteria
- DSL compiles to plan.
- End-to-end retrieval + rerank works on sample data.
- Outputs are versioned and availability-aware.
