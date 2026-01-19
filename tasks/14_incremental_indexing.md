# Task 14: Incremental Indexing & Manifests (Fix O(N) Startup)

## Goal
Eliminate the O(N) startup cost caused by `Store::rebuild_subject_views` scanning every object.
Implement an **incremental indexing** strategy where subject logs are updated atomically at write time, or efficiently repaired if missing.

## Why
- **Performance:** Startup must be constant time O(1) or proportional to *new* data only.
- **Scalability:** The current `list_objects` scan dies at >100k objects.
- **Reliability:** We need a source of truth for "what belongs to this subject" without scanning the universe.

## Specification

### 1. The Global Manifest (Optional but recommended)
Create a `data/manifest.log` (append-only) or `data/indexes/global.idx`.
- Maps `object_id -> subject_id`.
- Used to quickly identify orphan objects or rebuild subject specific indexes.

### 2. Atomic Write Path
Modify `Store::put_assertion` (and overlay equivalent):
- **Current:** Writes object file -> Returns.
- **New:**
  1. Write object file (content-addressed).
  2. **Append** entry to `data/subjects/<sub_id>/assertions/log.idx` (or similar).
  3. Update `FrontierIndex` in memory/disk.

### 3. Persistent Subject Log
Instead of just `data/subjects/<sub_id>/assertions/0001_....dharma` files:
- Maintain a binary index file `data/subjects/<sub_id>/log.bin`.
- Format: `[seq(8) | object_id(32) | offset(8) | len(4)]`.
- This allows O(1) lookups of "last sequence" and fast loading of history without directory listing (which is slow on some FS).

### 4. Incremental Repair
- On startup, read the **tail** of the subject logs.
- Compare with `FrontierIndex` (if persisted) or re-hydrate `FrontierIndex` from these logs (fast).
- Only scan raw objects directory if explicitly asked (`dh doctor` or `--rebuild`).

## Implementation Steps
1.  Define the binary format for `log.bin`.
2.  Update `Store` to append to `log.bin` inside `append_assertion`.
3.  Rewrite `FrontierIndex::new` to load from these logs instead of scanning all objects.
4.  Remove `rebuild_subject_views` from the default boot path.
