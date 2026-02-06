# Task 67.1: EmbeddingModel + Keyspace DSL

## Goal
Add `embedding_model` and `keyspace` top-level blocks in DHL and compile them into artifacts (`.embedding`, `.keyspace`).

## Scope
- Grammar + parser support in DHL.
- Type-checking for field paths and input specs.
- Emission of compiler artifacts with config hash.
- Schema validation for versioning rules.
- Privacy validation: reject fields marked sensitive/PII in schema metadata.
- Documented DSL surface in `dev_docs/keyspace_embedding_spec.md`.

## Out of Scope
- Runtime embedding inference.
- Quantizer implementation.

## Test Plan
- Unit tests for DSL parsing and error cases.
- Golden tests for compiled artifact output.
- Recompile unchanged DSL -> identical artifacts.

## Acceptance Criteria
- `embedding_model` and `keyspace` blocks compile.
- Artifact fields match spec: versions, dependencies, config_hash.
- Validation errors are clear for missing/invalid fields.
- Compiler rejects any embedding_model input fields flagged as sensitive/PII.
