# The DHARMA Lifecycle (Current Implementation)

How a user intention becomes durable truth.

---

## 1) Intent
A user initiates an action in the CLI/REPL. The client loads the schema + contract for the active lens.

## 2) Assertion Construction
The client builds:
- `AssertionHeader`
- `AssertionBody` (CBOR)
- `sig` (Ed25519)

In REPL, assertions are **not encrypted** by default; envelopes are used for sync.

## 3) Ingest (Gatekeeper)
On ingest, the kernel checks:

1) Canonical CBOR
2) Signature validity
3) Structural invariants (seq/prev/refs)
4) Schema validation
5) Contract validation (Wasm)

If dependencies are missing, the assertion is **Pending**.

## 4) Acceptance + Storage
Accepted assertions are appended to:
- `subjects/<id>/assertions/log.bin` (or `overlays/log.bin`)
- `indexes/global.idx`

Frontier tips are updated.

## 5) Derivation
The runtime replays accepted actions and applies:
- `apply` logic
- invariants (post-apply checks)

Snapshots are periodically saved to speed up replay.

## 6) Sync
Peers exchange inventory and missing objects through DHARMA-SYNC.

---

## Current Gaps

- `header.ts` is often unset in REPL commits.
- Deterministic time injection is planned.
- Reactor execution is not yet implemented.

