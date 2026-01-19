# Task 15: Persistent Frontier Index (Fix In-Memory Bloat)

## Goal
Replace the pure in-memory `FrontierIndex` (HashMap) with a disk-backed or memory-mapped solution.
This ensures the runtime uses constant RAM regardless of the number of subjects/objects.

## Why
- **Memory Pressure:** Storing millions of object IDs in RAM is wasteful.
- **Fast Startup:** Loading a hashmap from disk is slower than mmapping a sorted file or using a KV store.

## Specification

### Option A: `sled` (Embedded KV)
- **Pros:** Fast, crash-safe, rust-native.
- **Cons:** Adds a dependency (might break 1MB limit check).
- **Usage:** Map `subject_id -> frontier_set_bytes`.

### Option B: Hand-rolled Memory Mapped Index (Recommended for 1MB constraint)
- Create `data/indexes/frontier.bin`.
- **Layout:** Sorted list of `(subject_id, tip_object_id)`.
- **Lookup:** Binary search.
- **Update:** Append-only log + periodic compaction, or a simple WAL.

### Design Choice for V1
Stick to **Option B (Append-Only Log)** for simplicity and zero-dep.
- File: `data/frontier.log`
- Entry: `[subject_id(32) | op_code(1) | object_id(32)]`
  - OpCode 0x01: Add Tip
  - OpCode 0x02: Remove Tip (consumed)
- **Startup:** Read log, build in-memory map (much faster than parsing full objects).
- **Compaction:** Rewrite log when it gets too large (snapshot current tips).

## Implementation Steps
1.  Define `FrontierLog` struct handling file I/O.
2.  Modify `FrontierIndex` to wrap this log.
3.  On `update(new_tip, prev_tips)`:
    - Write `Remove` entries for `prev_tips`.
    - Write `Add` entry for `new_tip`.
4.  On startup, replay the log to restore state.
5.  Implement `compact()`: write current live set to new file, swap.
