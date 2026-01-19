# Task 27: Workspace Refactoring (Kernel vs CLI)

## Goal
Split the monolithic crate into a workspace to strictly enforce the 1MB limit for the Runtime while allowing the CLI to be rich.

## Structure

```
/dh (Workspace)
  /Cargo.toml           <-- Workspace definition
  
  /dharma-core (Lib)      <-- The <1MB Kernel
    - Dependencies: wasmi, ed25519-dalek, chacha20poly1305, fs2
    - Modules: assertion, contract, crypto, envelope, error, identity, net, store, runtime
    - NO: rustyline, nom, pulldown-cmark
    
  /dharma-runtime (Bin)   <-- The "dhd" daemon
    - Dependencies: dharma-core
    - Logic: main.rs that calls dharma_core::net::server::listen
    
  /dharma-cli (Bin)       <-- The "dh" developer tool
    - Dependencies: dharma-core, rustyline, nom, pulldown-cmark
    - Modules: repl, pdl (compiler), cmd
    - Logic: REPL, Compiler, Identity Tools
```

## Implementation Steps
1.  **Create Directories:** `mkdir dharma-core dharma-runtime dharma-cli`.
2.  **Move Code:**
    - Move `src/lib.rs` and core modules to `dharma-core/src/`.
    - Move `src/repl/` and `src/pdl/` (compiler parts) to `dharma-cli/src/`.
    - Move `src/main.rs` logic to `dharma-cli/src/main.rs`.
3.  **Update Cargo.toml:**
    - Root `Cargo.toml` becomes workspace.
    - `dharma-core/Cargo.toml` has minimal deps.
    - `dharma-cli/Cargo.toml` has full deps.
4.  **Fix Imports:** Update all `use crate::` to `use dharma_core::`.

## Outcome
- `dharma-runtime` build size: <1MB (Verified).
- `dharma-cli` build size: ~3-5MB (Allowed).

## Status
- Completed. Workspace split is in place, binaries are `dh` and `dhd`, tests pass.
