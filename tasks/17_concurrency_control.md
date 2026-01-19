# Task 17: Concurrency Control (LockManager)

## Goal
Prevent data corruption when multiple DHARMA processes (e.g., REPL + Sync Daemon) access the same data directory.

## Why
- **Safety:** `fs::write` is not atomic across processes for complex updates (index + object).
- **UX:** Users might run `dh repl` while `dhd` is running.

## Specification

### 1. PID File / Lock File
- **Location:** `data/dharma.lock`
- **Mechanism:** `flock` (Unix) or `LockFile` (Windows).
- **Behavior:**
  - If lock exists and process is alive: Fail start (or wait).
  - If lock exists but process dead: Steal lock (carefully).

### 2. Lock Manager (Internal)
- If we move to a daemon architecture:
  - The **Daemon** holds the write lock on the DB.
  - **Clients** (REPL) talk to Daemon via IPC (Unix Socket / Named Pipe).
  - **Read-Only Access:** Clients *might* read `data/` directly if careful (mmap is often safe for multiple readers), but writing must be serialized.

### Implementation Steps (V1 - Process Exclusion)
1.  On startup, try to acquire `data/dharma.lock` (exclusive).
2.  If failed, print friendly error: "Another DHARMA instance is running. Please stop it or use --readonly."
3.  Implement `ReadonlyStore` mode that skips writing but allows `state` inspection.
