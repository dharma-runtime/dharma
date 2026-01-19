# Scheduler & Time (Planned)

DHARMA does not yet have a full scheduler. Today, contracts run synchronously in the REPL and ingest pipeline.

## Current Behavior

- `now()` returns `context.clock.time`.
- REPL supplies local system time.
- Ingest validation uses `header.ts` (or 0 if unset).

## Planned Behavior (Task 12)

- All time and randomness will flow through `Env` abstractions.
- Deterministic simulation will drive time.
- Contracts will be pure and deterministic under the same input history.

