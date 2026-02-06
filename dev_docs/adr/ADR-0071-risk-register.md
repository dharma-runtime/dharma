# ADR-0071 Risk Register: Runtime + Storage Migration

- ADR: `dev_docs/adr/ADR-0071-runtime-storage-migration.md`
- Date: 2026-02-06
- Scope: Migration phases 0-2 (prep, dual-write, switch-over, rollback)

| ID | Risk | Impact | Likelihood | Mitigation | Owner |
| --- | --- | --- | --- | --- | --- |
| R1 | Replay determinism drift after async boundary changes | Critical | Medium | Keep replay/compiler/DSL paths sync-only; block release on seeded replay hash mismatch; add determinism CI gate | Runtime Lead |
| R2 | Dual-write divergence between legacy and SPI adapters | High | Medium | Idempotent write keys, per-commit diff checks, divergence alerts, auto-stop on threshold breach | Storage Lead |
| R3 | Snapshot isolation mismatch across Redb and Postgres adapters | High | Medium | Explicit SPI snapshot contract, adapter conformance tests, canary read parity checks | Storage Lead |
| R4 | Retry storms amplify outages and increase tail latency | High | Medium | Bounded retries with jitter, per-target circuit breakers, durable retry queue, backpressure metrics/alerts | Runtime Lead |
| R5 | Analytics lag exceeds freshness SLO causing stale decisions | Medium | Medium | Watermark monitoring, lag alerts at 120s, planner fallback to OLTP for strong reads | DharmaQ Lead |
| R6 | Rollback cannot be executed in < 1 hour under load | Critical | Low | Mandatory staging rollback rehearsal, mirrored delta journal, documented runbook, quarterly drills | Release Manager |
| R7 | Existing serialized artifacts fail to decode in new adapters | Critical | Low | Compatibility corpus from production-like samples, decode regression tests, no canonical format rewrite | Serialization Owner |
| R8 | Observability gaps hide migration regressions | High | Medium | Define migration dashboard before phase 1, enforce alert coverage for divergence/lag/retry/backlog | SRE Lead |
| R9 | Write-path partial failure creates commit ambiguity | Critical | Medium | Atomic commit ordering, commit tokens, compensating queue, hard fail on non-idempotent partial writes | Storage Lead |
| R10 | Capacity shortfall in Postgres or DuckDB during switch-over | High | Medium | Pre-cutover load tests, capacity headroom policy, auto-scaling/partition tuning, canary ramp gates | Platform Lead |

## Phase Gates

- Gate A (Phase 0 -> 1): R1/R7 mitigations must be green in staging.
- Gate B (Phase 1 -> 2): R2 divergence below 0.01% for 5 consecutive days.
- Gate C (Production cutover): R6 rollback drill completed successfully within 45 minutes.

## Escalation Thresholds

- Immediate stop and rollback if:
- Determinism mismatch detected (R1).
- Divergence exceeds 0.01% sustained for 15 minutes (R2).
- Inability to prove seq parity during cutover/rollback (R6/R9).
