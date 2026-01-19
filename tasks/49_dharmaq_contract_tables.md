# Task 49: DHARMA-Q Contract Tables (Live State Index)

## Goal
Provide one DHARMA-Q table per contract (per lens) containing the **current state** for each subject,
auto-created and auto-updated, and queryable as the default “NOW” view.

## Requirements
- **One table per contract per lens** (e.g. `std.wrk.task@v1`, `std.wrk.task@v2`).
- **One row per subject** representing the latest derived state for that contract.
- **Auto-create** tables on demand.
- **Lazy rebuild**: build on first access if missing or stale.
- **Auto-update** incrementally after new assertions.
- **Searchable** via `find` / `q`, defaulting to latest lens.
- **Overlay-aware**: private fields should be visible only when caller has access.

## Proposed Behavior
- Table naming uses raw contract name + `@v<lens>` suffix for lens versions.
- Table contains:
  - Core columns: `subject`, `seq`, `typ`, `assertion_id`, `ts`, `signer` (as available).
  - **State columns**: each schema field becomes a column (stored from latest derived state).
- Query defaults:
  - `table <contract>` => latest lens table.
  - `q` / `find` in subject context => use active contract table if known.
  - `q --lens <ver>` => uses `<contract>@v<ver>`.

## Privacy & Overlays
- Store full state in table rows.
- At query time:
  - If overlay is **not** allowed, mask private fields in output.
  - Search indexes should only be built for public fields unless overlay access is available.

## Lazy Build / Incremental Update
- Each contract table has a small manifest recording:
  - lens version
  - last processed assertion per subject (or highest seq)
  - schema/contract IDs
- On query:
  - If table missing or manifest stale, rebuild from subject assertions.
  - Otherwise, process only new assertions since last checkpoint.

## Implementation Outline
1. Add contract-table registry under `data/dharmaq/tables/`.
2. Build per-contract table from:
   - subject list for contract
   - replay assertions to latest state
3. Maintain per-table manifest for incremental updates.
4. Update DHARMA-Q query planner to route to contract tables by default.
5. Update `tables` / `table` commands to show contract tables and columns.

## Success Criteria
- Querying a contract table returns one row per subject (latest state).
- Private fields are visible only to authorized identities.
- Tables are created on first query and kept in sync incrementally.
