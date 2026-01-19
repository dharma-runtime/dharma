# Task: Compiler + Runtime Enhancements

## Objective
Bring the DHL compiler and runtime to the Phase 4+ requirements in README.

## Requirements
- DHL v2 features: has_role, concat, lists, ACLs
- Deterministic contract execution limits (fuel + memory)
- Reactor daemon: subscribe to ingest, run reactor wasm, emit signed assertions

## Implementation Details
- Extend parser + AST for new language features.
- Extend wasm codegen for strings/lists/role checks (stub if needed).
- Add wasmi fuel + memory limits.
- Reactor daemon runs as background thread in serve/repl.

## Acceptance Criteria
- Actions with role checks and list handling compile and execute.
- Reactor can observe an accepted event and emit a signed assertion.
