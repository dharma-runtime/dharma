# DHARMA: Codebase & Architecture Analysis

**Date:** January 17, 2026
**Project:** DHARMA (Peer Assertions for Commitments & Truth)
**Version:** v1 (In Development)

## 1. Project Scope & Vision

DHARMA is an ambitious attempt to rebuild the fundamental layer of digital collaboration. It rejects the current model of "Client-Server + Database" in favor of "Peer-to-Peer + Cryptographic Ledger".

*   **Core Premise:** "Truth" should not live in a vendor's database (SaaS). It should live in signed, portable, immutable assertions controlled by the users (Peers).
*   **Target Audience:** Spans from personal use ("Grandmother" - recipes, chores) to high-assurance sectors ("CIA" - chain of custody, classified logs).
*   **Mechanism:** A local-first, offline-capable protocol where state is derived deterministically by replaying signed events (Assertions) against a logic kernel (Contracts).

## 2. Architecture & Mechanics

The system is a "Sovereign Truth Kernel". It combines concepts from Git, Blockchains, and Event Sourcing, but avoids global consensus (no Proof-of-Work/Stake).

### 2.1 The Kernel (`dharma-core`)
The heart of the system, written in Rust.
*   **Storage:** Append-only filesystem logs (`data/objects/*.obj`). Objects are content-addressed (SHA-256).
*   **Encryption:** Everything is encrypted at rest and in transit.
    *   **Subject Keys:** Symmetric keys (ChaCha20-Poly1305) rotated via "Epochs".
    *   **Identity:** Ed25519 for signing assertions.
*   **Networking:** Custom TCP protocol with Noise-based handshake (for mutual auth and encryption). Sync is graph-based (fetching missing DAG nodes).

### 2.2 The Logic Layer (DHL & Wasm)
DHARMA is not just a storage engine; it's a compute engine.
*   **DHL (Literate DHARMA Domain Law):** A domain-specific language embedded in Markdown. It defines:
    *   **Schemas:** Data types.
    *   **Contracts:** State transition logic.
    *   **Workflows:** Finite State Machines (e.g., `Open -> InProgress -> Done`).
*   **Compilation:** DHL compiles to **WebAssembly (Wasm)**.
*   **Execution:** The runtime uses `wasmi` to execute these Wasm contracts deterministically. This ensures that any peer, anywhere, at any time, derives the exact same state from the same history.

### 2.3 User Interaction (`dharma-cli`)
*   Current interaction is CLI-driven.
*   Handles identity management (init, export).
*   Compiles DHL files.
*   Runs the node (server/peer).

## 3. Code Quality & Patterns

The codebase demonstrates high engineering standards:

*   **Idiomatic Rust:** Strong usage of Rust's type system (Enums for state, Result for errors).
*   **Modularity:** Clean separation between `core`, `runtime`, and `cli`.
*   **Safety:** Explicit `deny(unsafe_code)` (mostly) and careful handling of crypto primitives.
*   **Zero-Copy Intent:** Use of `ciborium` and byte slices suggests attention to performance.
*   **Wasm Integration:** The `ContractEngine` in `dharma-core/src/contract.rs` is a clean abstraction over the raw Wasm runtime, managing memory marshaling effectively.

## 4. Strengths

1.  **First-Principles Design:** The "Grandmother + CIA" principle forces a design that is both simple (files, keys) and robust (crypto-enforced).
2.  **Deterministic State:** By relying on Wasm for contracts, DHARMA solves the "business logic" problem in distributed systems. You don't just sync data; you sync the *rules* of the data.
3.  **Privacy by Design:** Encryption is not an add-on; it's the default state. The "Subject" model creates natural sharding and privacy boundaries.
4.  **Local-First:** The system functions perfectly offline. Sync is just an optimization for availability, not a requirement for operation.

## 5. Weaknesses & Pitfalls

1.  **Concurrency & Conflict:**
    *   **Pitfall:** Without a central sequencer, concurrent edits (forks) are inevitable. DHARMA relies on DAGs and merge strategies, but "User A changes Title" vs "User B changes Title" is a UX nightmare to resolve cleanly.
    *   **Risk:** Users might end up with "split brain" states that are hard to understand.
2.  **Key Management:**
    *   **Pitfall:** "Not your keys, not your data" is great until you lose your keys. Recovery mechanisms (social recovery, paper keys) are critical and hard to get right for non-technical users.
3.  **Performance at Scale:**
    *   **Risk:** Replaying history to derive state is O(n). As a subject grows (e.g., a chat log with 1M messages), startup time could become prohibitive without aggressive snapshotting (which is planned but complex).
4.  **Adoption Barrier:**
    *   **Weakness:** DHL is a new language. Asking developers to learn a new DSL *and* a new mental model (Event Sourcing) is a high friction point.

## 6. Value Proposition vs. Shortcomings

*   **Value:** DHARMA offers **Digital Sovereignty**. It is one of the few viable paths out of the "Feudal Internet" (where users are serfs on Big Tech land). It is particularly valuable for cross-organization workflows where no single admin is trusted.
*   **Shortcoming:** It lacks the "Instant Gratification" of centralized apps. Setting up a DHARMA node is harder than signing up for Trello. The CLI-first approach limits the current audience to developers.

## 7. Improvements Needed

1.  **Developer Experience (DX):**
    *   **Debugger:** A tool to step through DHL contract execution (replay assertions one by one) is essential for contract authors.
    *   **LSP:** Language Server Protocol support for DHL (syntax highlighting, errors in VS Code).
2.  **Client Libraries:**
    *   Currently, everything is Rust. A **Wasm-based JS client** is needed to build web UIs that talk to DHARMA nodes or run DHARMA logic directly in the browser.
3.  **Snapshotting & Indexing:**
    *   Implement the snapshotting logic mentioned in docs to ensure O(1) read times for long histories.
    *   Build `DHARMA-Q` (the query engine) to allow searching *across* subjects (e.g., "Find all tasks assigned to me").
4.  **Conflict UX:**
    *   Develop standard patterns/UI components for visualizing and resolving merge conflicts.

## 8. Conclusion

DHARMA is a serious, well-architected system solving a profound problem. The foundation (`dharma-core`) is solid. The next phase must focus on **Usability** (UI, client libs) and **Scalability** (snapshots, indexing) to bridge the gap from "Protocol" to "Product".
