# dh test CLI (Current)

`dh test` is the single entry point for correctness testing. It runs the property suite and the vector conformance checks.

```
dh test
dh test --deep
dh test --chaos
dh test --ci
dh test --replay SEED=<seed>
dh test --replay <seed>
```

---

## What It Runs

- **Property suite** (every seed): randomized, seed-reproducible checks (see `03_properties.md`).
- **Conformance vectors** (once per run): fixed inputs under `tests/vectors/*` (if present).
- **Simulation** (only with `--deep`): deterministic multi-node sync using the in-process `SimHub` and the real handshake + sync loop (see `04_simulation.md`).

---

## Seeds & Iterations

Current defaults from the test runner:

- **Normal**: 1 seed, 25 iterations per property.
- **Deep** (`--deep`, no replay): 1000 seeds, 200 iterations per property.
- **Replay** (`--replay`): forces a single seed even with `--deep`.

Notes:

- The **simulation** phase always runs **once per run** (base seed), not once per seed.
- Properties that ignore the `iterations` parameter still run once per seed (see `03_properties.md`).

---

## Flags (Current Behavior)

- `--deep`: increases seeds + iterations and enables the simulation phase.
- `--replay`: pins the RNG seed for reproduction.
- `--chaos`: enables **fault injection** in the simulation phase (only runs when `--deep`).
- `--ci`: forces headless output (no TUI), non-interactive failure handling.

---

## Output & Failure Tickets

On success:

```
dh test passed (<n> run, seed <seed>).
```

On failure:

- Prints `dh test failed (seed <seed>).`
- Writes a ticket under `tests/failures/DHARMA-FAILURE-<seed>-<property>.md`.

The ticket includes the property ID, seed, and details to reproduce. For simulation
failures, the runner automatically **replays with deep trace** and attaches the
trace to the ticket.
