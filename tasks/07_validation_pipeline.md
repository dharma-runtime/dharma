# Task: Data Versioning + Validation Pipeline

## Objective
Match README requirements for versioned assertions, deterministic validation, and replay ordering.

## Requirements
- Add header field `ver` (data version) to all assertions.
- `ver` is signed as part of the canonical header+body payload.
- Route validation/execution based on `ver` (lens).
- Deterministic ingest pipeline:
  1) canonical CBOR decode
  2) signature verification
  3) schema validation for typ
  4) contract validate
  5) accept/reject/pending
- Deterministic replay ordering with dependency graph and lexicographic tie-break.
- Missing deps/artifacts => PENDING, never guess state.

## Implementation Details
- Update AssertionHeader with `ver: Option<u64>` or `ver: u64`.
- Update cbor encode/decode and signing to include `ver` when present/required.
- Update schema/contract lookup to be keyed by typ + ver (lens).
- Implement topological sort ordering for per-subject replay.
- Extend validation/reporting for PENDING reasons (missing prev, refs, schema, contract).

## Acceptance Criteria
- New assertions include `ver` and validate with correct schema/contract versions.
- Replay produces identical state for the same object set + lens.
- Missing prerequisites yield PENDING not REJECT.
