# Task 45: REPL Contract Discovery + New Subject

## Goal
Improve the REPL UX for creating subjects and discovering available contracts.

## Commands to Add

### 1) `new <contract>`
- Generate a new subject id.
- Set the active schema/contract in `dharma.toml` for the current lens.
- `use` the new subject automatically.
- Print the new subject id.
- Example:
  ```
  > new std.wrk.task
  11de2b...
  > commit action Create title=Buy_milk description=Fresh_whole_milk
  ```

### 2) `contracts`
- List known contracts (from compiled artifacts in the object store and/or local stdlib sources).
- Show at least: namespace, version, schema id, contract id.

### 3) `contracts schema <contract>`
- Resolve `<contract>` by name.
- Print the schema fields (public/private), types, and defaults.

### 4) `contracts actions <contract>`
- Resolve `<contract>` by name.
- Print available actions and their args (name, type, visibility).

## Notes
- The contract resolver should prefer installed artifacts; fallback to stdlib sources if present.
- Preserve current behavior for users who only rely on `dharma.toml`.

## Success Criteria
- REPL can create a new subject in one command and auto-select the contract.
- Users can discover available contracts and inspect schema/actions without leaving the REPL.
