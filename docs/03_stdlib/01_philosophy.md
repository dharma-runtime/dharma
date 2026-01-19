# Standard Library Philosophy

The DHARMA standard library ("stdlib") is a set of **canonical, interoperable contracts** that define common business objects (notes, tasks, identities, registries).

## Goals

- **Interoperability:** Two systems using `std.task` should agree on fields and semantics.
- **Minimalism:** Contracts remain small, auditable, and deterministic.
- **Versioned Evolution:** Semantics evolve via versioned lenses, not breaking changes.

## Current Status

- The stdlib DHL sources live in `contracts/std/`.
- Some stdlib contracts are **drafts** and may not compile yet.
- Registry-based package distribution is **planned**, not implemented.

## How to Use Today

- Treat `contracts/std` as **reference designs**.
- Compile the ones that work in your environment:
  ```bash
  dh compile contracts/std/task.dhl
  ```
- Use REPL actions with the resulting schema/contract IDs.

## Design Rules

- All stdlib contracts must be deterministic and explicit.
- Fields should prefer `Enum` over free text for status.
- Avoid unbounded types; respect fixed-size constraints.

