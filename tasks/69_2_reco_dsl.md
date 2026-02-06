# Task 69.2: reco_pipeline DSL

## Goal
Implement `reco_pipeline` DSL parsing + compiler artifact.

## Scope
- Parsing task, positives/negatives, model, candidates, rerank, serve_filters.
- Validation of required fields and task types.
- Compiler emits `.reco_pipeline` plan + config_hash.

## Test Plan
- Parse/compile fixtures.
- Negative tests for missing blocks.

## Acceptance Criteria
- reco_pipeline blocks compile with deterministic artifacts.
