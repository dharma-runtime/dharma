# dhd: The Core Runtime (Current)

`dhd` is the reference implementation of the DHARMA kernel. Its responsibility is to ingest, validate, and persist assertions safely.

---

## 1) Minimal Kernel Principle

The kernel is deliberately small. It understands:

- **Signatures** (Ed25519)
- **Envelopes** (ChaCha20-Poly1305)
- **Ordering** (prev/refs DAG)
- **Artifacts** (schema + contract objects)

Domain logic lives in DHL contracts compiled to Wasm.

---

## 2) Storage Model (Envelope-First)

DHARMA stores all objects in a flat, content-addressed store:

```
<storage_root>/
  objects/<envelope_id>.obj
  subjects/<subject_id>/assertions/log.bin
  subjects/<subject_id>/overlays/log.bin
  subjects/<subject_id>/snapshots/
  indexes/embedded.sqlite
  indexes/global.idx
```

- **Objects** are immutable and deduplicated by hash.
- **Subject logs** are derived views.
- **Snapshots** accelerate replay but are discardable.
- In `embedded` profile mode, `indexes/embedded.sqlite` is initialized on first use and mirrors object/semantic/query paths.
- Embedded SQLite keeps custom lookup indexes (`idx_semantic_assertion`, `idx_cqrs_envelope`, `idx_cqrs_assertion`, `idx_subject_assertions_subject_seq`) so replay and query lookups avoid full scans.
- If SQLite rows are missing, runtime backfills from legacy file artifacts; `rebuild_subject_views` remains file-log driven and then rebuilds SQLite indexes.

---

## 3) Ingest Pipeline (What Actually Runs)

At ingest, the kernel performs:

1) **Canonical CBOR check**
2) **Signature verification**
3) **Structural validation** (`seq`, `prev`, `refs`)
4) **Schema validation**
5) **Contract validation** (Wasm)
6) **Append to subject log**
7) **Frontier index update**

If any dependency is missing, the assertion is **Pending** rather than guessed.

---

## 4) Concurrency Model

### Strict Mode (default)

If the schema concurrency mode is `strict`, forks are not accepted. The kernel returns **Pending** until a `core.merge` resolves the fork.

### Allow Mode

Parallel assertions are allowed and ordered deterministically by DAG traversal.

> Current status: strict concurrency is enforced in ingest; merge semantics exist but are still evolving.

---

## 5) Determinism Guarantees (Current vs Planned)

**Current:**
- No floating point math in kernel code.
- Wasm contracts run in a fixed memory layout.
- `now()` reads the **context clock**.

**Limitations:**
- REPL uses **local system time** for `now()` and does not set `header.ts`.
- Fuel metering and deterministic host env are **planned**, not implemented.

---

## 6) Overlays (Private Data)

Overlays are private sidecar assertions:

- Stored under `subjects/<id>/overlays/`.
- Require a base assertion reference.
- Controlled by overlay policies (`overlays.policy`).

---

## 7) Sync (DHARMA-SYNC/1)

Sync is a peer-to-peer exchange of inventory and objects over a Noise-encrypted channel.

- **Inv**: send frontier tips
- **Get**: request missing envelopes
- **Obj**: transmit object bytes

Sync is currently **blocking** and synchronous.

---

## Planned Enhancements

- Deterministic `Env` injection (Task 12)
- Wasm fuel + memory limits
- Async sync engine
- Registry integration for artifact discovery
