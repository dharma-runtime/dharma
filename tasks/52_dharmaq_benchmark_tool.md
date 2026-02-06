# Task 52: DHARMA-Q Benchmark Tool

## Goal
Create a benchmark tool to generate ~100M rows and run 3–4 representative queries.

## ADR Dependency (DHA-55)
- Reference: `dev_docs/adr/ADR-0071-runtime-storage-migration.md`
- Risk register: `dev_docs/adr/ADR-0071-risk-register.md`
- Benchmarks must measure ADR-relevant migration metrics (OLTP latency, analytics lag, and dual-path behavior where applicable).

## Requirements
- Deterministic data generator with configurable distributions.
- Generates datasets for:
  - contract state table (1 row per subject)
  - contract assertion table (many rows)
- 3–4 benchmark queries:
  - Filter + group-by + count
  - Filter + group-by + sum/avg
  - Multi-predicate filter + min/max
  - (Optional) high-cardinality group-by
- Report metrics: build time, query time, rows scanned, output rows.

## Implementation Notes
- Add a CLI under `dharma-cli` or a small crate in `dharma-test`.
- Generate data directly into dh storage and build DHARMA-Q tables.
- Seeded RNG for repeatability.

## Success Criteria
- One command generates data and runs benchmarks.
- Outputs stable, comparable timings across runs.
