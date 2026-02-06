# Task 67.5: ML Service Interface (Encode / Train / Predict)

## Goal
Define the external ML service interface used by embedding, forecasting, and recommendation runtimes.

## Scope
- HTTP or gRPC API definitions for:
  - encode(payload_bytes) -> embedding vector
  - train(model_spec, training_dataset_ref) -> model_artifact_id
  - predict(model_artifact_id, feature_matrix) -> predictions
- Request/response schemas and error codes.
- Authentication and timeout/retry policy.
- Model artifact storage contract (format + location + version tags).

## Out of Scope
- ML service implementation.
- Model training data preparation logic.

## Test Plan
- Contract tests for request/response schemas.
- Timeout/retry behavior tests with mocked service.
- Artifact resolution tests (model id -> artifact fetch).

## Acceptance Criteria
- ML service API is documented and versioned.
- Runtime code can call encode/predict with deterministic inputs.
- Artifacts are resolved by id with verifiable hashes.
