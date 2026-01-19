# Task 19: DHARMA-Q (Embedded Query Engine) Spec

## Architecture
DHARMA-Q is an **embedded module** split into two layers to maintain the <1MB Kernel constraint.

1.  **Engine (`dharma-core`):** Storage, Indexing, Execution. (Binary safe).
2.  **Language (`dharma-cli`):** Text Parsing, Planning. (String heavy).

## 1. Storage Layout (Columnar)
- **Root:** `data/dharmaq/`
- **Tables:** Time-partitioned directories.
- **Columns:** Typed binary files (`.bin`) + Validity Bitmap (`.valid`).
- **Compression:** None for V1 (rely on OS page cache).
- **Symbol Dictionaries:** **Per-Table**. No global dict.
- **Row Identity:** `RowId` is physical offset (unstable). `(oid, seq)` is stable logical key.

## 2. Ingest Pipeline (Kernel)
- **Source:** Follows `data/subjects/*/assertions/log.bin` (from Task 14).
- **Atomicity:** **Checksummed WAL**.
- **Trigger:** Synchronous call or channel notification from `Store` when `query` feature is active.
- **Dependencies:** `memmap2`, `byteorder`, `crc32fast`. **NO `serde`, `nom`, `regex`.**

## 3. Query Engine (Vectorized)
- **Interface:** Struct-based API (`QueryPlan`).
- **Filter Model:** **Recursive Boolean Tree.**
    ```rust
    enum Filter {
        Leaf(Predicate),
        And(Vec<Filter>),
        Or(Vec<Filter>),
        Not(Box<Filter>),
    }
    ```
- **Execution Model:** **BitSet-Driven.**
    - `Leaf` -> Produces initial `BitSet` via vectorized kernel.
    - `And` -> `mask.and_inplace(next_mask)`.
    - `Or` -> `mask.or_inplace(next_mask)`.
    - `Not` -> `mask.negate()`.
    - Materialization happens only at the end.
- **Predicates:** **Vectorized Kernels.** No row-by-row interpretation.
    - `fn filter_gt_i64(col: &[i64], val: i64, out: &mut BitSet)`
- **Floats:** **Strictly Forbidden** for logic. `f16` allowed only for vector storage.
- **Decimals:** Fixed scale per column (defined in schema). Store raw `i64` mantissa.

## 4. Query Language (CLI)
- **Location:** `dharma-cli`.
- **Dependencies:** `nom`.
- **Function:** Parses `query "invoice | where amount > 1000"` into `QueryPlan`.
- **Execution:** Passes `QueryPlan` to `dharma-core` for execution.

## 5. Geo Engine (Simplified)
- **Types:** `lat_e7`, `lon_e7` (i32).
- **Index:** Memory-mapped sorted arrays of `(cell_id, row_id)`.
- **Algorithm:** BBox Filter + Manual Ray Casting -> `BitSet`.

## 6. Text Search (Simplified)
- **Tokenization:** Lowercase, simple split on whitespace/punctuation.
- **Index:** Inverted Index `Trigram (u32) -> [RowId]`.
- **Query:** Jaccard Similarity -> `BitSet`.

## 7. Vector Search (Brute Force)
- **Storage:** Contiguous `[i8; N]` or `[f16; N]` arrays.
- **Query:** Brute-force scan + SIMD Dot Product -> `BitSet` (Top-K mask).

## Implementation Roadmap
1.  **Refactor Workspace:** Create `dharma-core` and `dharma-cli` (Task 27).
2.  **Engine:** Implement `Table`, `Column` (fixed scale), `SymbolDict` (local).
3.  **Kernels:** Implement vectorized predicate functions.
4.  **Parser:** Implement `nom` parser in `dharma-cli`.
5.  **Wire:** Connect REPL `query` command to Parser -> Engine pipeline.