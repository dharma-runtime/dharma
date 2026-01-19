# Execution Semantics (Current)

This document clarifies how assertions are ordered, validated, and applied today.

---

## 1) Ordering Rules

Assertions are ordered by:

1) **DAG dependencies** (`prev` + `refs`)
2) **Deterministic tie-break** (lexicographic by assertion ID)

If a cycle exists, replay fails with `DependencyCycle`.

---

## 2) Structural Validation

Structural validation checks:

- Protocol version matches.
- `seq` and `prev` consistency.
- Signature validity.
- `core.merge` rules (must reference >= 2 refs).

Failures are rejected; missing deps are pending.

---

## 3) Concurrency Modes

### Strict
- If multiple tips exist, new assertions are **pending** until merged.
- Implemented by `FrontierIndex` checks.

### Allow
- Concurrent branches are allowed.
- Replay uses deterministic topological order.

---

## 4) Contract Execution

Contracts run in Wasm with a fixed memory layout:

- `STATE_BASE = 0x0000`
- `OVERLAY_BASE = 0x1000`
- `ARGS_BASE = 0x2000`
- `CONTEXT_BASE = 0x3000`

`validate()` must return `0` for accept. `reduce()` applies changes and must preserve invariants.

---

## 5) Pending vs Rejected

- **Pending:** missing dependency (prev, refs, schema, contract).
- **Rejected:** invalid data, signature, or contract validation failure.

---

## 6) Overlays

Overlays are stored separately and merged during replay if available.

- Overlay must reference exactly one base assertion.
- Overlay payload is applied on top of base arguments.

---

## 7) Known Gaps

- Deterministic time injection (Task 12).
- Reactor execution daemon.
- Schema registry for artifact discovery.

