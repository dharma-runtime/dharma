# Task 54: DHARMA-Q Dual Store + DHL Indexes + Consistency Planner

## Goal
Provide a **transparent dual-path query engine** that automatically chooses between:
- **Row store (transactional, strongly consistent)** for OLTP-style queries.
- **Column store (analytical, eventually consistent)** for scans, aggregates, and analytics.

## ADR Dependency (DHA-55)
- Reference: `dev_docs/adr/ADR-0071-runtime-storage-migration.md`
- Risk register: `dev_docs/adr/ADR-0071-risk-register.md`
- This task is the primary implementation path for ADR consistency/storage decisions and must follow ADR rollout/rollback constraints.

Indexes are **declared in DHL** and drive both storage layout and query planning.

## Motivation
We want ecommerce-grade transactional UX without exposing storage complexity to users.
Users should only define contracts + indexes in DHL and get optimal behavior automatically.

## Scope
- DHL index declaration syntax and compilation into contract metadata.
- Row-store tables per contract view (per lens) with indexes.
- Column-store tables per contract view (existing Task 49 + assertions tables).
- Query planner heuristics that route to row or column storage automatically.
- Strong consistency for row store; eventual consistency for column store.

## Definitions
- **Row store**: one row per subject, optimized for point reads + indexed filters.
- **Column store**: columnar partitions optimized for scans + aggregates.
- **Lens**: schema/contract version (`@vN`).
- **View**: a specific contract + lens + index definition set.
- **Consistency**:
  - **Strong**: read sees latest committed state for the subject.
  - **Eventual**: read may lag behind newest commits.

## DHL Index Syntax (Proposed)
Add index declarations to state fields. Examples:
```dhl
state Task {
  id: Id
  owner: Ref<Identity>
  status: Text
  priority: Text
  updated_at: Time
  total: Number

  @unique(id)
  @index(owner)
  @index(status)
  @index(updated_at)
  @index(owner, status)        # compound
  @index(total, kind="range")  # range-friendly index
}
```

### Rules
- `@unique(field)` implies `@index(field)` + uniqueness constraint.
- `@index(a, b, ...)` defines a compound index in that order.
- Indexes on **private fields** are only honored in row store when overlay access is granted.
- If no index declared: still build a row store with **primary key on subject** only.

## Storage Model

### Column Store (Existing + Extend)
- Contract state tables: `<contract>@v<lens>` (Task 49).
- Assertion tables: `<contract>@v<lens>.assertions` (Task 49 add-on).
- Eventually consistent: updated async or on-demand.

### Row Store (New)
- Table name: `<contract>@v<lens>.row`
- One row per subject (current state).
- Required columns:
  - `subject`, `seq`, `assertion_id`, `ts`, `signer` (as available)
  - All schema fields (current state)
- Backed by:
  - Primary key: `subject`
  - Secondary indexes from DHL
- Stored under: `data/dharmaq/rows/<contract>@v<lens>/`
  - `rows.kv` or `rows/` shard files
  - `indexes/<index_name>/` for secondary index segments
  - `table.meta` with schema hash, contract id, lens, index list, last_seq

## Consistency Model
- **Row store** updates are part of the commit path. If row store update fails, commit fails.
- **Column store** updates are asynchronous (eventual). Missing updates should not block commits.
- Reads default to **strong** when:
  - Query is point lookup by subject
  - Query uses only indexed filters (no aggregate)
  - Query has small `limit` and high selectivity
- Reads default to **eventual** when:
  - Query includes aggregates, group-by, or wide scans
  - Query touches non-indexed fields

## Query Planner (Automatic Routing)

### Classification
1. **Transactional**:
   - point lookup by subject
   - filters only on indexed fields
   - no group-by, no aggregate, no join
2. **Analytical**:
   - group-by / aggregates
   - large scans or wide filters
   - any non-indexed filters

### Planner Decision Rules
- Prefer **row store** if all predicates can be satisfied by indexes and estimated rows < threshold.
- Prefer **column store** if:
  - aggregate/group-by present
  - estimated rows >= threshold
  - query requests full row projection of many columns
- Allow fallback:
  - If row store missing/stale => fall back to column store unless strong consistency required.
  - If column store missing => fall back to row store if possible, else error.

### Stats & Estimation
Row store keeps lightweight stats:
- row count per contract view
- per-index cardinality and selectivity sketches (optional)
- last rebuild seq

### Storage & Technology
- **Engine:** Use **Redb** (Pure Rust, embedded-friendly) for the Row Store. Avoid RocksDB to keep binary size small.

## Write Path (Commit)
To ensure durability and prevent phantom reads: **The Log is the WAL.**

1.  **Validate:** Check signature and contract logic.
2.  **Commit Log:** Append assertion to `log.bin` (Fsync). *This is the Point of No Return.*
3.  **Update Row Store:**
    -   Open Redb transaction.
    -   Compute new state (Apply).
    -   Upsert row & update secondary indexes.
    -   Commit Redb transaction.
4.  **Recovery:** On startup, check if `RowStore.last_seq < Log.head`. If so, replay the gap.
5.  **Async:** Enqueue column-store update.

## Read Path
- `state`, `why`, `prove`, and `do/try` remain source-of-truth from assertions.
- `q` / `find` / `table` use planner to select row or column.
- Row store always enforces overlay visibility on output.

## Schema & Index Changes
- **New lens** => new row + column tables.
- **Index set changed** (DHL update):
  - create new row-store view for that lens
  - rebuild indexes from assertions or from column store snapshot
- **Field removed/renamed**:
  - row store rebuild required
  - old tables remain read-only for historical use

## Overlay & Privacy
- Row store stores **full state** (including private fields).
- At query time, private fields are masked unless overlay allowed.
- Indexing private fields:
  - if overlay access is not present, queries cannot use private indexes
  - private indexes may exist but are gated by access control

## REPL / UX
- `tables` shows row/column variants, but default remains transparent.
- `table <contract>` shows column store (analytical) by default.
- `table <contract> --tx` shows row store explicitly (debugging only).
- `q` and `find` do not require hints; planner decides.

## Failure & Recovery
- Row-store WAL allows crash recovery.
- Full rebuild available from assertions if corruption detected.
- Column store can be fully rebuilt from assertions at any time.

## Success Criteria
- No user-facing storage decisions required.
- Indexes declared in DHL control transactional performance.
- Point lookups and indexed filters are strongly consistent.
- Aggregates and scans use column store and remain eventually consistent.
- Planner reliably routes queries without explicit hints.

## Open Questions
- Do we allow explicit query hints for testing (`--row` / `--column`)?
- What index types do we support first (hash vs btree vs bitmap)?
- How to keep row store small on very large datasets (TTL/archival)?
