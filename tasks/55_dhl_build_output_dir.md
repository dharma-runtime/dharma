# Task 55: DHL Build Output Directory (No Source Pollution)

## Goal
Stop emitting `.schema`, `.contract`, `.reactor` alongside `.dhl` sources.  
All compiled artifacts should go to a dedicated project build directory (e.g. `.dharma/contracts/`).

## Motivation
Compiling DHL files currently pollutes the source tree and creates noisy diffs.
We want a clean source tree and a predictable, ignorable build output path.

## Current Behavior (Code Pointers)
- `dharma-cli/src/lib.rs::compile_dhl` writes:
  - `<stem>.schema`, `<stem>.contract`, `<stem>.reactor` next to the source file.
- REPL `collect_local_contracts` scans `./contracts/**.schema` and pairs them with `.contract`.

## Requirements
1. **Default build output directory**
   - If a project root is found (nearest `dharma.toml`):
     - `./.dharma/contracts/<relative_path>/<name>.(schema|contract|reactor)`
   - Preserve subdirectory structure to avoid collisions.
   - If no project root exists, fall back to local `_build/`.

2. **Configurable output**
   - Add `dharma.toml` config, e.g.:
     ```toml
     [compiler]
     out_dir = ".dharma/contracts"
     ```
   - CLI override (optional but preferred): `dh compile --out <dir> <file.dhl>`

3. **REPL discovery**
   - `collect_local_contracts` must scan the new build directory.
   - Keep **backward compatibility** by also checking legacy locations
     (`contracts/_build` and adjacent `_build`) if the new directory does not exist.

4. **No behavior regressions**
   - The object store writes and `dharma.toml` ID updates remain unchanged.
   - `dh compile` still prints schema/contract/reactor IDs as before.

5. **Git hygiene**
   - Add `.dharma/` (or configured output dir) to `.gitignore`.
   - Do not require users to commit compiled artifacts.

## Implementation Sketch
- Add a helper to compute build root:
  - Detect project root by nearest `dharma.toml` (if present).
  - Emit into `.dharma/contracts/<relative_path>/...`.
  - If no project root exists, fall back to local `_build/...`.
- Replace direct `stem.with_extension` writes with computed output paths.
- Update REPL `collect_local_contracts` to look in `.dharma/contracts` first.
- Optionally add `dh compile --out` to override location.

## Success Criteria
- Compiling a directory of `.dhl` files creates no `.schema/.contract/.reactor`
  beside source files.
- REPL still lists local contracts correctly.
- Output tree is predictable and ignorable.
