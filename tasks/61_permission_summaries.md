# Task 61: Permission Summaries & Fast Reject (PRD v1 Guardrail)

## Goal
Implement layered permission evaluation with declarative summaries to avoid full WASM execution for every check, while preserving strict acceptance rules.

## Why
- PRD guardrail: MUST NOT require full WASM execution for every permission check.
- Router/ingest should reject early when possible.

## Scope
- Permission summary artifact format and caching.
- Router/ingest early rejection path.
- Full validation remains mandatory for acceptance.

## Specification

### 1) Permission Summary Artifact
- Generated per contract version.
- Contains:
  - allowed actions by role
  - allowed read/query scopes by role
  - public access flags
- Stored as a signed artifact alongside contract.

### 2) Evaluation Layers
1. Summary check (fast, cacheable)
2. Contract guard evaluation (bounded)
3. Full validation (required for acceptance)

### 3) Rules
- Router MAY reject early based on summary.
- Router MUST NOT accept without full contract validation.
- Summary cache key = (contract_id, version, role, action).

## Implementation Steps
1. Define summary schema and encoding (CBOR).
2. Extend contract compiler to emit summaries.
3. Add summary cache in router/ingest.
4. Wire summary checks into execution paths.

## Test Plan (Detailed)

### Unit Tests
- `summary_format_roundtrip`:
  - Encode/decode summary artifact.
- `summary_cache_keying`:
  - Ensure role/action/version changes invalidate cache.

### Integration Tests
- `early_reject_on_summary`:
  - Summary denies action -> router rejects without WASM.
- `must_not_accept_without_full_validation`:
  - Summary allows action but contract denies -> reject after full validation.

### Negative/Security Tests
- Corrupt summary artifact => ignore and fallback to full validation.
- Summary missing required fields => treat as deny.

### Performance Tests
- Benchmark router with summary vs full validation (expect significant reduction in WASM calls).

