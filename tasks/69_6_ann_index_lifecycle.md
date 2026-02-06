# Task 69.6: ANN Index Lifecycle Management

## Goal
Define storage, versioning, and update policy for ANN indexes used by reco retrieval.

## Scope
- Index storage backend options (local HNSW files or external service).
- Versioning: one index per task_type + model_version.
- Rebuild policy: full rebuild on new model version (v1).
- Index metadata registry (location, build time, corpus size, hash).

## Out of Scope
- Two-tower model training.
- Serving layer implementation.

## Test Plan
- Index registry validation.
- Deterministic rebuild on fixed embeddings.
- Version isolation (queries do not mix index versions).

## Acceptance Criteria
- Index lifecycle policy documented and enforceable.
- Runtime can resolve the correct index for a model version.
