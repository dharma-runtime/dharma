# ADR-0071: Runtime + Storage Migration Architecture

- Status: Proposed
- Date: 2026-02-06
- Owner: Runtime + Storage Working Group
- Related: DHA-55, `tasks/06_storage_layout.md`, `tasks/17_concurrency_control.md`, `tasks/54_dharmaq_dual_store_planner.md`

## Non-Negotiables

- DHL syntax remains unchanged.
- DharmaQ syntax remains unchanged.
- Existing serialized artifacts remain readable.
- Deterministic replay semantics are preserved.

## Scope

- This ADR defines migration architecture and rollout/rollback only.
- Implementation work is out of scope for this document.

## 1. Context & Problem Statement

### Current runtime/storage stack

- Runtime server loop is currently blocking/threaded (`dharma-runtime` + `dharma_core::net::server`), with manual worker pools.
- Compiler, DSL parsing, and replay-critical logic are synchronous and deterministic (`dharma-core` + CLI parser paths).
- Storage uses an append-only object store plus derived subject views (`objects/`, `subjects/<id>/assertions|overlays|snapshots|indexes`).
- DHARMA-Q direction requires dual-path querying (strong OLTP path + eventual analytics path), but backend boundaries are not yet formally locked.

### What is breaking today

- Performance: threaded networking + mixed responsibilities in one path limits concurrency scaling for multi-peer/server workloads.
- Reliability: storage behaviors are spread across direct filesystem flows without a formal adapter boundary for embedded vs server operation.
- Maintainability: runtime and storage responsibilities are coupled, making staged migration and rollback hard to reason about.

## 2. Decision Drivers

1. Determinism and replay safety.
2. Stability under migration with reversible rollout.
3. Local-first operation for embedded/single-node use.
4. Multi-process/server scalability for hosted deployments.
5. Mobile/edge compatibility for constrained environments.
6. Clear operational envelope (backup, monitoring, DR).

## 3. Options Considered

### 3.1 OLTP storage options

| Option | Strengths | Weaknesses | Outcome |
| --- | --- | --- | --- |
| SQLite | Mature, ubiquitous, simple ops | Coarser concurrency model, weaker fit for high-write multi-worker server mode | Rejected as primary |
| Redb | Pure Rust, embedded-friendly, small footprint | Younger ecosystem, less battle-tested in distributed server ops | Selected for embedded |
| RocksDB | High write throughput, proven at scale | Native dependency complexity, larger operational surface | Rejected |
| Postgres | Strong multi-process semantics, mature tooling/HA | Heavier for embedded/offline footprint | Selected for server |

### 3.2 Analytics/query backend options

| Option | Strengths | Weaknesses | Outcome |
| --- | --- | --- | --- |
| SQLite | Simple deployment | Limited analytical performance for wide scans/group-by | Rejected |
| DuckDB | Excellent analytical scans/aggregates, embeddable, columnar | Eventual-sync orchestration required | Selected |
| CozoDB | Graph/relational flexibility | Smaller ecosystem, higher migration risk here | Rejected |

### 3.3 Tokio adoption boundary options

| Option | Strengths | Weaknesses | Outcome |
| --- | --- | --- | --- |
| Keep runtime fully blocking | Lowest migration complexity | Limits server scalability, harder to manage backpressure/retries | Rejected |
| Make core fully async | Uniform async model | High determinism risk in replay/compiler internals | Rejected |
| Boundary async only (runtime edges) | Scales network/daemon loops while isolating deterministic core | Requires strict sync/async contracts | Selected |

## 4. Decision

Selected architecture is **dual-profile (embedded + server)** with a shared Storage SPI and explicit runtime boundaries:

1. Tokio boundaries:
- Async components: daemon lifecycle, REPL remote loop integration, network accept/sync workers, outbound replication, retry schedulers.
- Sync components: compiler pipeline, DHL parsing, DharmaQ parsing, deterministic replay/validation core.
- Rule: `dharma-core` deterministic modules remain runtime-agnostic; async orchestration lives at boundaries.

2. Storage architecture:
- Embedded OLTP adapter: Redb.
- Server OLTP adapter: Postgres.
- Canonical assertion/object artifacts remain append-only and readable; adapters index/project canonical data, not replace it.

3. Analytics backend:
- DuckDB for analytical/eventual paths (aggregates, wide scans, group-by).
- OLTP path remains source for strong reads and command-side correctness.

4. Consistency model:
- OLTP: strong + snapshot isolation.
- Analytics: eventual with explicit watermark lag and staleness reporting.

5. Rollback commitment:
- Any phase rollback must complete in < 1 hour with no data loss using feature flags + mirrored write journal.

## 5. Architecture

### 5.1 Storage SPI + adapter contract

```rust
trait StorageSpi {
    type Txn: StorageTxn;

    fn put_object_if_absent(&self, envelope_id: EnvelopeId, bytes: &[u8]) -> Result<(), StoreErr>;
    fn get_object(&self, envelope_id: EnvelopeId) -> Result<Option<Vec<u8>>, StoreErr>;
    fn delete_object(&self, envelope_id: EnvelopeId) -> Result<(), StoreErr>;

    fn begin_txn(&self, mode: TxnMode) -> Result<Self::Txn, StoreErr>;
    fn scan_subject_log(&self, subject: SubjectId, from_seq: u64) -> Result<Box<dyn Iterator<Item = LogEntry>>, StoreErr>;
    fn range_scan(&self, table: TableId, range: KeyRange) -> Result<Box<dyn Iterator<Item = KvRow>>, StoreErr>;
    fn iter_table(&self, table: TableId) -> Result<Box<dyn Iterator<Item = KvRow>>, StoreErr>;
    fn snapshot(&self) -> Result<SnapshotToken, StoreErr>;
}

trait StorageTxn {
    fn append_assertion(&mut self, entry: LogEntry, bytes: &[u8]) -> Result<(), StoreErr>;
    fn upsert_state_row(&mut self, table: TableId, key: Key, row: Row) -> Result<(), StoreErr>;
    fn delete_state_row(&mut self, table: TableId, key: Key) -> Result<(), StoreErr>;
    fn commit(self) -> Result<CommitToken, StoreErr>;
    fn rollback(self) -> Result<(), StoreErr>;
}
```

Snapshot isolation requirements:

- `snapshot()` defines a stable read boundary across all tables touched by one query.
- Readers never observe partial commits.
- Writers commit atomically for: object presence + assertion log append + OLTP row/index updates.

### 5.2 DharmaQ backend strategy

- Strong path (OLTP): keyed lookups and indexed filters from OLTP adapter.
- Eventual path (analytics): DuckDB projections fed by commit/changefeed with watermark.
- Planner behavior:
- point reads/indexed filters -> OLTP default.
- aggregates/wide scans/non-indexed filters -> analytics default.
- If strong consistency is requested and analytics is stale, route to OLTP.

### 5.3 Consistency model (OLTP vs analytics)

- Source of truth: append-only assertions/object artifacts.
- OLTP freshness: synchronous on commit path (read-your-write guaranteed on same node).
- Analytics freshness: eventual; target p95 lag <= 30s, hard alert at 120s.
- Cross-store invariant: `analytics_watermark_seq <= oltp_committed_seq`, never ahead.

### 5.4 Error model + retry semantics

Classification:

- Retryable: timeouts, temporary network failures, deadlocks, busy/backpressure.
- Fatal: schema/contract validation errors, signature failures, deterministic replay mismatch.
- Compensating-action required: dual-write partial success, adapter unavailable during commit stage.

Retry policy:

- Exponential backoff with jitter, bounded attempts.
- Idempotency key: `(envelope_id, target_adapter, operation_kind)`.
- Retries allowed only for idempotent operations.
- On retry budget exhaustion: move to durable retry queue + alert.

## 6. Migration Plan

### 6.1 Dependency-safe sequence

1. Feature flags + SPI scaffolding must ship before any dual-write.
2. Dual-write must prove parity before any production read switch.
3. Read switch canary must pass before full switch-over.
4. Rollback drill on staging is mandatory before production cutover.

### 6.2 Phase 0: Preparation (Week 0-1)

Scope:

- Add feature flags:
- `runtime.async_daemon`
- `runtime.async_network`
- `store.spi_v2`
- `store.dual_write`
- `store.shadow_read`
- `query.analytics_backend`
- Add adapter scaffolding for Redb/Postgres/DuckDB and mirrored write journal.
- Baseline metrics and capture golden deterministic replay dataset.
- Run first staging migration rehearsal.

Rollback (Phase 0):

- Disable all new flags and redeploy.
- No data rollback required (no authoritative path changed).
- Target rollback time: < 15 minutes.

Validation checkpoint (end of Week 1):

- Staging replay determinism: 100% pass.
- Adapter smoke tests green.
- Observability dashboards operational.

### 6.3 Phase 1: Dual-write + shadow-read (Week 2-4)

Scope:

- Writes continue on legacy path and mirror to SPI adapters using idempotency keys.
- Reads remain legacy for users; shadow reads execute on new adapters and diff results.
- Start analytics projection from mirrored commit feed; track watermark lag.

Rollback (Phase 1):

1. Flip `store.dual_write=false`, `store.shadow_read=false`.
2. Keep legacy as sole read/write source.
3. Drain retry queue and archive divergence reports.
4. Validate no seq gap on legacy.

Target rollback time: < 30 minutes.

Validation checkpoints:

- +48h: mismatch rate < 0.1%, no replay drift.
- +1 week: mismatch rate < 0.01%, p95 write overhead < 15%.
- Phase exit gate: zero unresolved critical diffs for 5 consecutive days.

### 6.4 Phase 2: Switch-over (Week 5-6)

Scope:

- Canary read/write switch to SPI primary: 5% -> 25% -> 100%.
- Keep reverse mirroring to legacy for 7 days after 100% cutover.
- Enable async runtime boundaries for daemon/network workers in server profile.
- Keep compiler/parser/replay paths synchronous.

Rollback (Phase 2, < 1 hour, no data loss):

1. Freeze write admission for max 10 minutes.
2. Flip read/write primary back to legacy.
3. Replay mirrored delta from cutover checkpoint to legacy until seq parity.
4. Unfreeze writes and verify parity checks.
5. Re-run deterministic replay sample and frontier parity checks.

Target rollback time: 45 minutes.

### 6.5 Data migration & verification plan

- No rewrite of canonical serialized assertion artifacts.
- Adapter state is rebuilt/backfilled from canonical object/log stream.
- Verification uses:
- assertion count parity
- per-subject frontier parity
- deterministic replay hash parity
- OLTP vs analytics watermark and aggregate parity checks

### 6.6 Tests & Validation (required)

1. Replay determinism test (seeded):
- Replay identical seeded datasets on legacy and new stack; final state hash must match.
2. Cross-store consistency check:
- OLTP rows vs analytics projections must match within configured lag window.
3. Staging migration rehearsal:
- Full phase 0->2 dry run on staging dataset, including rollback drill.
4. Load test:
- Concurrent read/write workload on canary profile; measure p95/p99 and error budgets.
5. Failure injection:
- Inject I/O errors, timeouts, adapter outages, deadlocks; verify retry + recovery behavior.

## 7. Risks & Mitigations

Top migration risks and mitigations are tracked in `dev_docs/adr/ADR-0071-risk-register.md`.

Immediate hard controls:

- Blocking release gate on determinism drift.
- Blocking release gate on rollback rehearsal failure.
- Blocking release gate on unresolved dual-write divergence.

## 8. Operational Plan

### Backups

- Canonical object/log snapshots every 15 minutes (incremental) + daily full.
- Postgres PITR enabled in server mode.
- DuckDB projection snapshots daily; always rebuildable from canonical log.

### Monitoring

- Core metrics: commit latency, dual-write divergence, retry queue depth, analytics lag, replay hash drift, pending object backlog.
- SLO alerts:
- analytics lag > 120s
- divergence > 0.01%
- rollback drill older than 14 days

### Disaster recovery

- RPO target: 0 for canonical log (mirrored + frequent snapshots).
- RTO target: < 1 hour for runtime/storage service restoration.
- Quarterly DR drill includes replay-from-backup and parity verification.

## 9. Appendix

### 9.1 Schema sketches

```text
canonical_log(
  seq BIGINT PK,
  envelope_id BYTEA UNIQUE,
  subject_id BYTEA,
  assertion_id BYTEA,
  ts_committed BIGINT
)

dual_write_journal(
  seq BIGINT PK,
  envelope_id BYTEA,
  target_adapter TEXT,
  op_kind TEXT,
  status TEXT,
  last_error TEXT,
  updated_at BIGINT
)

analytics_watermark(
  pipeline TEXT PK,
  committed_seq BIGINT,
  updated_at BIGINT
)
```

### 9.2 Performance assumptions

- OLTP point read p95 <= 20ms (embedded) and <= 30ms (server).
- OLTP write commit p95 <= 40ms under target concurrency.
- Analytics query p95 <= 300ms for standard dashboard aggregates on warm cache.
- Dual-write overhead budget during phase 1: <= 15% p95 write latency increase.
