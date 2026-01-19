# Testing Philosophy (DHARMA)

In DHARMA, testing is a correctness invariant. If the kernel lies, the system fails its purpose.

---

## 1) Determinism First

Every failure must be reproducible from a single seed. If it isn't, the testing system itself is broken.

---

## 2) Layers of Confidence

- **Unit Tests**: fast, local correctness (CBOR, crypto, parser).
- **Property Tests**: generated invariants (idempotency, replay determinism).
- **Simulation**: deterministic multi-node convergence.
- **Chaos**: fault injection (network + disk + crash).

---

## 3) Performance as Correctness

Binary size, latency, and throughput regressions are treated as failures.

---

## 4) Where the Full Spec Lives

The authoritative testing spec is in `docs/testing.md`.

