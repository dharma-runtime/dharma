# Task 69.3: Two-Tower Training + ANN Retrieval

## Goal
Provide training + retrieval infrastructure for two-tower recommendations.

## Scope
- External ML interface for training (InfoNCE / sampled softmax).
- Item + user embedding projection outputs.
- ANN index build per task/model version.
- Candidate retrieval API for pipelines.

## Dependencies
- Task 67.5: ML service interface (train/predict + artifact resolution).
- Task 69.6: ANN index lifecycle (storage/versioning/rebuild policy).

## Test Plan
- Retrieval returns stable top-K for fixed embeddings.
- ANN index rebuild deterministic for same inputs.

## Acceptance Criteria
- Candidate generation works end-to-end for a sample dataset.
