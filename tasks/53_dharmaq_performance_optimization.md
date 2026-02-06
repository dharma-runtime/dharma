# Task 53: DHARMA-Q Performance Optimization

## Goal
Optimize DHARMA-Q to reach the target performance on the benchmark dataset.

## ADR Dependency (DHA-55)
- Reference: `dev_docs/adr/ADR-0071-runtime-storage-migration.md`
- Risk register: `dev_docs/adr/ADR-0071-risk-register.md`
- Optimization work must preserve ADR guarantees for determinism and strong-vs-eventual read behavior.

## Targets (TBD)
- Agree on “satisfactory performance” thresholds:
  - max query latency for 100M rows
  - acceptable memory use
  - build time

## Focus Areas
- SIMD/vectorized predicate evaluation
- Partition pruning (time/key-based)
- Compression + dictionary encoding
- Parallel scan + aggregation
- Spill-to-disk for large group-by

## Steps
1. Baseline with Task 52 benchmarks.
2. Add SIMD filters where hot.
3. Add partition pruning via metadata.
4. Implement compression/dict encoding for hot columns.
5. Parallelize scans + aggregations.
6. Re-run benchmarks and iterate.

## Success Criteria
- Meets or exceeds agreed performance targets.
