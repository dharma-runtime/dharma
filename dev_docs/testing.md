# DHARMA Testing System Specification

**Version:** 1.1
**Status:** Normative (Design + Implementation Plan)
**Scope:** Core Kernel, Sync Protocol, Logic Engine, DHARMA-Q, CLI/REPL

---

## 0) Read This First

DHARMA is a truth kernel. If it corrupts data or diverges between peers, the system fails its purpose. The testing system is therefore not optional and not an afterthought. It is part of the protocol.

This document defines:

- The **testing architecture** (how we test).
- The **property catalog** (what must always be true).
- The **conformance vectors** (fixed inputs with fixed outputs).
- The **fault model** (what must not break us).
- The **developer loop** (how to run and reproduce tests).
- The **implementation roadmap** (how we get there from today).

Where relevant, this document distinguishes **Current State** vs **Target State**.

---

## 1) Purpose & Philosophy

**Core Mandate**
> "A failure without reproduction is a bug in the testing system."

Every test failure MUST be reproducible from a single seed and produce a standalone failure ticket.

DHARMA's testing system is not about finding bugs; it is about **making lies impossible**. We test for correctness, determinism, convergence, and durability under faults.

---

## 2) Current State vs Target State (Informative)

### Current State (as of this repo)

- Unit tests exist in core modules:
  - CBOR canonicalization (`dharma-core/src/cbor.rs`)
  - Crypto (sign/verify, AEAD) (`dharma-core/src/crypto.rs`)
  - Envelope encode/decode (`dharma-core/src/envelope.rs`)
  - Runtime VM smoke test (`dharma-core/src/runtime/vm.rs`)
- Conformance vectors exist under `tests/vectors/` (cbor, assertion, envelope, schema, contract).
- `dharma-test` implements property testing + vector execution; `dh test` is wired in the CLI.
- Failure tickets are written to `tests/failures/DHARMA-FAILURE-<seed>-<property>.md`.
- No deterministic simulation harness yet.
- Sync is blocking and uses direct `std` IO without harness abstraction.
- `--chaos`/`--ci` flags are accepted but do not change behavior yet.

### Target State (per Task 12)

- Deterministic simulation harness with fault injection.
- Conformance vectors for CBOR, assertions, envelopes, sync, and contracts.
- Property-based testing integrated for deterministic invariants.
- A single `dh test` entrypoint with tiers and replay.

---

## 3) Unified Test Entry Point

### Current CLI

```
dh test
dh test --deep
dh test --chaos
dh test --ci
dh test --replay SEED=<seed>
dh test --replay <seed>
```

### Target CLI (Planned)

```
dh test              # fast gate (PR)
dh test --deep       # deterministic simulation suite (nightly)
dh test --cluster    # black-box multi-process tests
dh test --chaos      # fault injection + soak
dh test --upgrade    # version-matrix upgrade tests
dh test --perf       # performance regressions
dh test --replay SEED=<seed>  # reproduce failure
dh test --ci         # headless CI output
```

---

## 4) Test Harness Architecture (Target)

### 4.1 DharmaEnv Abstractions (Task 12)

We introduce explicit environment traits so all nondeterminism can be controlled:

```
trait Env {
    fn now(&self) -> Timestamp;
    fn random_u64(&self) -> u64;
}

trait Fs {
    fn read(&self, path: &str) -> Result<Vec<u8>>;
    fn write(&self, path: &str, data: &[u8]) -> Result<()>;
    fn fsync(&self, path: &str) -> Result<()>;
    ...
}

trait Net {
    fn send(&self, to: NodeId, msg: Bytes) -> Result<()>;
    fn recv(&self) -> Option<Bytes>;
    ...
}
```

- **StdEnv**: uses `std::time`, `std::fs`, `std::net` (production).
- **SimEnv**: uses deterministic in-memory time, filesystem, and network (tests).

### 4.2 Simulation Engine

The simulation engine provides:

- A **deterministic scheduler** (fixed seed, deterministic interleavings).
- A **virtual clock** (time jumps, drift, ordering).
- A **virtual network** (drop/delay/reorder/duplicate, partitions).
- A **virtual disk** (torn writes, corruption, ENOSPC).

### 4.3 Test Runner & UI

The test runner orchestrates seeds, fault schedules, and property checks. It provides:

- **TUI dashboard** for local runs.
- **Headless JSON/log output** for CI.
- Automatic **failure ticketing**.

---

## 5) Property-Based Testing (Required)

Property testing is mandatory for deterministic components. It is not a substitute for simulation; it is the fast gate that proves core invariants.

### Principles

- Every property test must be **seeded** and **reproducible**.
- All randomness must flow through `DharmaEnv`.
- Property tests must generate **valid inputs** unless explicitly testing invalid paths.

### Recommended Tooling

- `proptest` for generators and shrinking.
- `quickcheck` acceptable for lightweight cases.

### Property Testing Targets (Concrete)

#### Encoding & Canonicalization
- **P-CBOR-001**: encode(decode(bytes)) == bytes for canonical CBOR.
- **P-CBOR-002**: non-canonical CBOR is rejected.

#### Assertions
- **P-ASSERT-001**: signature verifies for valid assertions.
- **P-ASSERT-002**: any bit flip in signed payload fails verification.
- **P-ASSERT-003**: invalid `seq/prev` rules are rejected.

#### Envelope
- **P-ENV-001**: encrypt/decrypt roundtrip.
- **P-ENV-002**: wrong key fails.
- **P-ENV-003**: envelope id = sha256(cbor(envelope)).

#### DAG Ordering
- **P-DAG-001**: topo order respects all deps.
- **P-DAG-002**: cycles are rejected.

#### Storage + Ingest
- **P-STORE-001**: ingesting same envelope twice is idempotent.
- **P-STORE-002**: append-only logs replay deterministically.

#### CQRS State
- **P-CQRS-001**: replay of assertions yields identical state memory.
- **P-CQRS-002**: state decode is deterministic for same memory.

#### DHARMA-Q
- **P-Q-001**: rebuilding projections twice yields identical results.
- **P-Q-002**: filters are commutative for AND groups (bitset logic).

---

## 6) Conformance Vectors (Required)

Conformance vectors are golden inputs with fixed expected outcomes. These are required for protocol stability and versioning.

### Directory Layout

```
tests/
  vectors/
    cbor/
    assertion/
    envelope/
    schema/
    contract/
    sync/
    dharmaq/
```

### Vector Formats

- **CBOR**: raw bytes + expected canonical status.
- **Assertion**: `assertion.cbor` + expected status (accept/reject/pending).
- **Envelope**: `envelope.cbor` + expected decryptability.
- **Schema**: schema manifest + expected validation outcomes.
- **Contract**: wasm + assertion/context + expected result.
- **Sync**: inv/get/obj frames + expected responses.

### Example Vector Metadata

```
# tests/vectors/assertion/valid-001.meta
expect = "accept"
ver = 1
schema = "std.task@1.0.0"
```

---

## 7) Fault Model (Required)

The system MUST tolerate and correctly surface:

- **Network**: drop, delay, reorder, duplicate, partition (symmetric/asymmetric).
- **Node**: crash-stop, crash-restart, partial startup.
- **Disk**: torn writes, fsync lies, corruption, ENOSPC.
- **Clock**: drift, jumps, monotonic violations.

Faults must never lead to silent corruption. Failures must be explicit and reproducible.

---

## 8) Test Taxonomy (Tiers)

| Tier | Description | Mode |
| :--- | :--- | :--- |
| **T0** | Unit + property tests | `dh test` |
| **T1** | Single-node property + replay | `dh test` |
| **T2** | Deterministic multi-node simulation | `dh test --deep` |
| **T3** | Black-box multi-process cluster | `dh test --cluster` |
| **T4** | Chaos + soak | `dh test --chaos` |
| **T5** | Upgrade matrix (mixed versions) | `dh test --upgrade` |
| **T6** | Performance regression gates | `dh test --perf` |

---

## 9) Performance as Correctness

Performance regressions are correctness failures. The test system must enforce:

- **Binary size budgets** (dharma-runtime < 1.05 MB).
- **Ingest latency budgets** (configurable p95/p99).
- **Sync roundtrip budgets** (p95 for inv/get/obj).

Thresholds are versioned and stored in `tests/perf/budgets.toml`.

---

## 10) Failure Handling & Ticketing

Every failure generates a standalone Markdown ticket.

### Ticket Naming

`DHARMA-FAILURE-<ID>-<PROPERTY>.md`

### Required Fields

- Seed and replay command
- Git commit, platform, CPU
- Trace (event + net + fs)
- Minimal counterexample (assertions)
- State hash diff (if applicable)

> Current tickets only include property, seed, deep/chaos/ci flags, and details. The rest are target requirements.

Example header:

```
Reproduce: dh test --replay SEED=123456
Property: P-DET-001
Commit: <hash>
Platform: macOS arm64
```

---

## 11) Developer Loop

### Quick Checks

```
dh test
```

### Deep Simulation

```
dh test --deep
```

### Reproduce Failure

```
dh test --replay SEED=123456
```

Current artifacts are written under `tests/failures/` as Markdown tickets.

---

## 12) Coverage Matrix (Target)

| Feature | Unit | Property | Sim | Cluster | Conformance |
| --- | --- | --- | --- | --- | --- |
| CBOR | Yes | Yes | - | - | Yes |
| Assertions | Yes | Yes | Yes | - | Yes |
| Envelopes | Yes | Yes | - | - | Yes |
| Store/Logs | Yes | Yes | Yes | Yes | - |
| CQRS | Yes | Yes | Yes | - | - |
| Sync | - | Yes | Yes | Yes | Yes |
| Overlays | Yes | Yes | Yes | - | - |
| Identity | Yes | Yes | - | - | Yes |
| DHARMA-Q | Yes | Yes | - | - | - |

---

## 13) Implementation Roadmap (Task 12)

### Phase 1: Abstraction (Harness)
- Add `dharma-core/src/env.rs` with `Env`, `Fs`, `Net` traits.
- Refactor kernel to use env instead of `std` directly.
- Implement `StdEnv` for runtime.
- Create `dharma-sim` crate and `SimEnv`.

### Phase 2: Properties
- Implement property tests for CBOR, assertions, envelopes, DAG, CQRS.
- Add property tests for DHARMA-Q rebuild determinism.

### Phase 3: CLI + Dashboard
- Add `dharma-test` crate.
- Implement TUI dashboard with `ratatui`.
- Wire `dh test` CLI.

### Phase 4: Ticketing + Replay
- Automatic ticket generation on failure.
- Replay by seed and deterministic scheduler.
- Store traces and snapshots for debugging.

---

## 14) Acceptance Criteria

The testing system is complete only when:

1) `dh test` runs with zero configuration.
2) Deterministic simulation exists and is reproducible.
3) Faults are injectable and correlated with failures.
4) Failures generate Markdown tickets and reproduce via seed.
5) Conformance vectors gate protocol changes.

---

## 15) References

- `docs/test-philosophy.md`
- `tasks/12_testing_conformance.md`
- `dharma-core/src/*` (canonical CBOR, assertions, envelope, sync, runtime)
