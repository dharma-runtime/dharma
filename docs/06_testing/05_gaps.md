# Coverage Gaps (dh test)

This file lists **what `dh test` does NOT currently cover** based on the
actual test runner in `dharma-test/src/lib.rs`. It is intentionally explicit.

---

## Not Covered by `dh test`

### CLI / UX / End-to-End
- No tests for `dh`, `dhd`, or REPL commands (`identity`, `serve`, `connect`,
  `write`, `overlay`, `q`, `find`, etc.).
- No end-to-end tests across **real TCP** sockets (all network tests use
  `SimHub` / `SimStream`).

### Identity & Keystore
- No tests for keystore encryption/decryption (Argon2/PBKDF2), unlock flows, or
  failure paths.
- No tests for identity export, delegate creation, or delegate revocation.

### Sync & Handshake Hardening
- No negative tests for handshake failures (bad keys, bad signatures,
  mismatched peer IDs, replayed handshakes).
- No tests for malformed frames or partial frame reads/writes.
- No tests for subscription allow/deny policies impacting inventory exchange.

### Overlay / Privacy
- No tests for overlay sidecar logs, overlay ACL enforcement, or base/overlay
  merge semantics in sync and replay.

### Schema / Contract Execution
- No direct tests that schema validation rejects bad types or unexpected fields
  during ingest.
- No tests that contract `validate`/`reduce` failures surface correctly in
  ingest or sync.
- No tests for `ver` routing or schema version compatibility.

### Storage Durability & Recovery
- Chaos injects faults, but there are **no assertions** verifying recovery
  invariants (fsync semantics, torn write repair).
- No tests for migration or upgrade scenarios.

### DHARMA-Q Depth
- Property tests only cover rebuild determinism and AND commutativity.
- No tests for OR / NOT / UNION, trigram accuracy, or query planner
  cost/ordering.

### Scale & Performance
- No tests for large payloads, large numbers of subjects, or boundary sizes.
- No binary size regression tests or runtime performance budgets.

---

## What This Implies

`dh test` is currently strong on **core correctness invariants** (CBOR,
signatures, basic DAG rules, deterministic replay), but it is **not yet a full
end-to-end validation** of the CLI, identity lifecycle, overlay privacy, or
network resilience.

If we want those guarantees, we should add targeted integration tests or extend
the simulation suite.
