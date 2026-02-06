# Task 67: Keyspace + Embedding Layer Epic

## Goal
Ship a deterministic, versioned **Keyspace + Embedding** subsystem that produces stable hierarchical keys from immutable snapshots and makes them queryable via projections.

## Why
Forecasting, recommendations, and analytics need a **shared, stable join key**. Without a deterministic keyspace, cross-subject pipelines drift and cold start fails.

## Scope
- Task 67.1: EmbeddingModel + Keyspace DSL + compiler artifacts.
- Task 67.2: Runtime payload construction + keyspace ops + quantizer interface.
- Task 67.3: Membership projections + queries.
- Task 67.4: Determinism + historical read tests.
- Task 67.5: ML service interface (encode/train/predict).

## Out of Scope
- Forecasting or recommendation pipelines.
- Model training pipelines.

## Acceptance Criteria
- Embedding payload generation is deterministic and versioned.
- Keyspace keys are stable across replays.
- Membership projection populates keys for snapshots.
- Tests cover determinism, hierarchy, versioning, canonicalization, and historical reads.
