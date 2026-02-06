# Task 67.2: Keyspace Runtime + Quantizer Interface

## Goal
Implement deterministic payload construction, keyspace operations, and quantizer versioning in runtime.

## Scope
- Canonical payload generator (token lines, stable ordering).
- Built-ins: `key_from_subject`, `key`, `key_full`, `parent`, `levels`, `same_space`, `prefix`.
- Quantizer interface and versioned codebooks.
- Embedding inference boundary (external service interface).
- Quantizer storage format, versioning, and loading.

## Out of Scope
- Forecasting / recommendation pipelines.

## Quantizer Details
- Codebooks stored at `vault/quantizers/<version>/`.
- Format: JSON metadata + binary centroid/codebook blob.
- Code format: `u64` with `max_bits` in low bits (per keyspace levels).
- Training is manual in v1 via CLI; automated retraining is v2.
- Determinism: same training data + seed => identical codebook.

## Test Plan
- Determinism tests: same snapshot + model version -> same payload bytes.
- Quantizer version tests: different version -> different code.
- Parent prefix tests: hierarchy correctness.
- Historical read tests for pinned seq.

## Acceptance Criteria
- Runtime can compute key from subject snapshot at pinned seq.
- Keys are stable across runs.
- Quantizer versioning enforced.
