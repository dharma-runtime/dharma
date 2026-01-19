# DHARMA Review - Jeff Dean

## Executive Summary

DHARMA is a highly ambitious and philosophically sound attempt to build a **distributed, local-first truth machine**. It correctly identifies that "truth" in a distributed system must be cryptographic, append-only, and deterministic. The architecture avoids the common trap of "database-first" thinking, opting instead for a "log-first" approach that aligns perfectly with distributed system principles.

The codebase is clean, well-structured, and demonstrates a strong grasp of the problem domain. However, as it scales from prototype to production, the current **storage indexing** and **network handshake** implementation will likely become bottlenecks or security liabilities.

## What is Great

### 1. The "Truth Plane" Architecture
The decision to separate the **Truth Plane** (assertions, signatures, content-addressing) from the **Availability Plane** (sync, storage) is the correct architectural split. By making `Assertion` the atomic unit of truth—signed, typed, and immutable—you ensure that the system is audit-native.

### 2. Determinism by Design
The rigorous enforcement of **Canonical CBOR** for all hashing and signing inputs (`cbor::ensure_canonical`) is excellent. Combined with a **Wasm-based runtime** (`runtime/vm.rs`) that strictly limits host imports, you have achieved a level of determinism that many blockchain projects struggle with. This makes "replay" a reliable mechanism for state derivation.

### 3. Literate Domain Law (DHL)
Embedding the schema and logic directly into Markdown documents (`pdl/parser.rs`) is a brilliant usability choice. It bridges the gap between *legal text* and *smart contract code*, making the "rules of reality" readable by humans and machines. The parser implementation using `pulldown-cmark` + `nom` is robust.

### 4. Privacy-First Overlays
The native support for **Overlays** (encrypted extensions to public subjects) in the sync protocol (`net/sync.rs`) is a standout feature. Most systems bolt this on later; DHARMA makes it a first-class citizen, enabling "public header, private body" workflows essential for enterprise adoption.

### 5. Storage Simplicity
The filesystem layout (`src/store.rs`) is pragmatic.
- `data/objects/<hash>.obj`: Flat, content-addressed blob store.
- `data/subjects/<id>/assertions/`: Log of pointers.
This makes the system inspectable with standard CLI tools (`ls`, `cat`), which is invaluable for debugging and trust.

## What is Horrible (Risks)

### 1. The "Rebuild World" Problem
The `rebuild_subject_views` function in `src/store.rs` iterates over **every single object** in the store to reconstruct subject logs.
- **Complexity:** O(N) where N is total objects in the node.
- **Impact:** Startup time will degrade linearly. At 100k objects, this will be noticeable. At 10M, it will be unusable.
- **Fix:** You need an incremental index or a persistent "manifest" file that tracks which objects belong to which subject, updated at write time.

### 2. Roll-Your-Own Handshake
`src/net/handshake.rs` implements a custom authenticated key exchange using X25519 + HKDF + HMAC.
- **Risk:** While the primitives are standard, the *protocol composition* is custom. This is prone to subtle replay, reflection, or identity-misbinding attacks.
- **Fix:** Replace this with **Noise Protocol Framework** (e.g., `snow` crate) or standard TLS 1.3. Do not maintain custom handshake logic in production code.

### 3. In-Memory Indexing
The `FrontierIndex` (`src/store/index.rs`) loads all frontier tips into `HashMap`s in memory.
- **Impact:** Memory usage grows with the number of subjects.
- **Fix:** Use a lightweight embedded KV store (like `sled` or even `sqlite`) for the index, or use an append-only index file on disk that can be memory-mapped.

## What Needs Improvement

### 1. Sync Protocol Robustness
The current sync (`src/net/sync.rs`) is a basic "exchange tips -> fetch missing" loop.
- **Issue:** It relies on `Inv` messages listing *all* tips. If a subject has a wide frontier (many concurrent edits), this message becomes large.
- **Suggestion:** Implement a **range-based reconciliation** or a **Merkle Search Tree** (like Merkle-CRDTs) to efficiently find differences without exchanging full tip sets.

### 2. Concurrency Control
The code currently assumes a single-process lock (via filesystem creation flags).
- **Issue:** `fs::OpenOptions::create_new(true)` is good for atomic writes, but there is no overarching lock manager. If the REPL and a background sync daemon run simultaneously, they might fight over index updates or cache coherence.
- **Suggestion:** Introduce a dedicated `LockManager` or use a singleton `Store` actor channel if moving to an async runtime.

### 3. Error Granularity
`DharmaError` is a bit monolithic.
- **Suggestion:** Split errors into `Transient` (network blips, lock contention) vs `Permanent` (corrupt data, invalid signature). This helps the sync loop decide whether to retry or ban a peer.

## Simplification Opportunities

### 1. Unified Indexing
Currently, you have `FrontierIndex` (in-memory) and filesystem logs (`list_assertions`).
- **Simplify:** Make the filesystem log the *source of truth* for the index. A memory-mapped file of `(seq, object_id)` tuples is faster to read than scanning a directory of files and uses less RAM than a HashMap.

### 2. PDL Parsing
The `nom` parser is powerful but complex.
- **Simplify:** For v1, if the syntax is strict, a simpler recursive descent parser might be easier to maintain and debug than the combinator-heavy approach. However, if the language grows, `nom` is the right choice.

## Final Verdict

**A+** for architecture and vision.
**B-** for scalability (indexing/sync).
**C** for crypto-protocol hygiene (handshake).

Fix the handshake and the O(N) rebuild, and you have a world-class foundation.

-- Jeff Dean
