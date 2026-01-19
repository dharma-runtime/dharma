# Task 12: Testing & Conformance (Hardening)

## Goal
Implement the **DHARMA Testing System** as defined in `docs/testing.md`.
Transition the codebase from "Prototype" to "Production-Grade" by enforcing deterministic simulation.

## Reference Specification
See **[docs/testing.md](../docs/testing.md)** for the normative requirements, property catalog, and fault models.

## Implementation Steps

### Phase 1: Abstraction (The Harness)
1.  **Define `DharmaEnv` Trait:** Create `dharma-core/src/env.rs`.
    -   `trait Env { fn now(&self) -> Timestamp; fn random_u64(&self) -> u64; }`
    -   `trait Fs { ... }`
    -   `trait Net { ... }`
2.  **Refactor Core:** Replace `std::time::SystemTime`, `std::fs`, and `std::net` usage in `dharma-core` with `DharmaEnv`.
    -   *Strategy:* Pass `&impl Env` to all kernel functions.
3.  **Real Implementation:** Implement `StdEnv` (using `std`) for the production binary (`dharma-runtime`).
4.  **Sim Implementation:** Create `dharma-sim` crate. Implement `SimEnv` (using in-memory structs and a priority queue for events).

### Phase 2: The Properties (The Tests)
1.  **Replay Determinism (P-DET-001):**
    -   Write a test that generates random assertions.
    -   Ingest them into `Node A`.
    -   Replay `Node A`'s log into `Node B`.
    -   Assert `Node A.state_hash == Node B.state_hash`.
2.  **Convergence (P-CONV-001):**
    -   Spin up 3 nodes in `SimEnv`.
    -   Connect them.
    -   Inject random assertions to all nodes concurrently.
    -   Run simulation until quiet.
    -   Assert all State Hashes match.

### Phase 3: The CLI & Dashboard (The Interface)
1.  **Add `dharma-test` crate:** A library for the test runner.
2.  **Implement Dashboard:** Create a TUI (using `ratatui`) that visualizes the simulation state, fault timeline, and property status board.
3.  **Implement `dh test`:**
    -   Parses flags (`--deep`, `--chaos`, `--ci`).
    -   Configures `SimEnv`.
    -   **UI Selection:** Selects `Ratatui` (default) or `Headless` (CI) renderer.
    -   Drives the Dashboard UI (or CI logger).
    -   Runs the Property Checkers.

### Phase 4: Ticketing & Repro (The Proof)
1.  **Automatic Ticketing:** Implement logic to dump a standard `DHARMA-FAILURE-*.md` on test failure.
2.  **Repro Command:** Implement `--replay seed=<SEED>` to force the RNG and scheduler to match the failed run.
3.  **Artifact Collection:** Capture event traces and store snapshots into the ticket bundle.

## Success Criteria
-   `cargo test` runs unit tests.
-   `./dh test` runs the Simulation Suite with a live dashboard.
-   Failures generate reproducible Markdown tickets.
-   `./dh test --deep` runs 1000 seeds and finds no nondeterminism.