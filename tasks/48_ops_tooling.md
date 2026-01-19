# Task 48: Operations Tooling (Day 2 Ops)

## Goal
Provide essential tooling for running DHARMA in production environments (Servers, Long-running nodes).
Focus on Observability, Maintenance, and Disaster Recovery.

## 1. Health & Diagnostics (`dh doctor`)
A command to verify the runtime environment.
-   **Checks:**
    -   [ ] Connectivity: Can bind to port? Can reach peers?
    -   [ ] Storage: Disk space available? Permissions correct?
    -   [ ] Time: Is system clock monotonic? Is it synced (NTP)?
    -   [ ] Integrity: Are there corrupted logs? (Quick CRC check).

## 2. Maintenance (`dh gc`)
A command to reclaim space.
-   **Targets:**
    -   [ ] **Pending Queue:** Drop assertions pending > X hours.
    -   [ ] **Blobs:** Prune local blobs not referenced by any active state.
    -   [ ] **Indexes:** Vacuum/Rebuild DHARMA-Q columnar files.

## 3. Backup & Restore (`dh backup`)
Portable archives of the node state.
-   `dh backup export <path>`: Dumps `log.bin`, `keystore`, and `config` to a `.tar.gz`.
-   `dh backup import <path>`: Restores state (safety checks for overwriting).

## 4. Metrics (Server Profile)
If compiled with `feature = "server"`, expose a `/metrics` endpoint (Prometheus format) on the HTTP port.
-   **Gauges:** `dharma_peers_connected`, `dharma_subject_count`.
-   **Counters:** `dharma_assertions_ingested`, `dharma_wasm_executions`.
-   **Histograms:** `dharma_ingest_latency`, `dharma_sync_latency`.

## Success Criteria
-   `dh doctor` reports "All Systems Operational" on a fresh install.
-   `dh gc` reduces disk usage.
-   Prometheus can scrape a running `dhd`.
