# What Is DHARMA?

**DHARMA is a Sovereign Truth Kernel.**

It is a protocol and runtime for establishing shared facts between distrusting parties, without requiring a central server.

---

## The 5 Non-Negotiable Ideas

### 1) Assertions, Not Mutations
You never "update a database." You **sign an assertion**.

### 2) Humans Commit, Machines Propose
Only a human (private key holder) can create binding history.

### 3) Append-Only Truth
History is immutable. Corrections are new assertions, not edits.

### 4) Deterministic Replay
Given the same assertions and the same contract, every node derives the same state.

### 5) Versioned Meaning
Data outlives code. Lenses allow old and new logic to coexist.

---

## What Exists Today

- A Rust kernel that can ingest, validate, and store assertions.
- A REPL with inspection, actions, and sync.
- A minimal DHL compiler that emits schemas + Wasm.
- A draft query engine (DHARMA-Q).

## What Is Planned

- Deterministic simulation harness.
- Verified package registry.
- Capability tokens and richer access control.
- GUI Workspace.

---

## What DHARMA Is Not

- **Not a database:** DHARMA-Q is the projection DB.
- **Not a blockchain:** no global consensus.
- **Not a framework:** it's a kernel for durable truth.
