# Task 20: Bloat Removal & Float Ban

## Goal
Enforce the 1MB limit by removing heavy dependencies and auditing for floating point usage.

## Why
- **Size:** `wasmi` defaults, `serde`, and `std` bloat add up.
- **Determinism:** Floating point math is non-deterministic across architectures. DHARMA must be deterministic.

## Specification

### 1. Dependency Audit
- **`wasmi`:** Ensure `default-features = false`. Explicitly enable `std` (required for WASI/host) but check if we can disable `f32`/`f64` instructions support (likely hard, but we can lint against their usage in *contracts*).
- **`serde`:** Ensure `serde` is **not** present in the dependency tree of `dh` (core). It is only allowed if `query` feature is on (and even then, discouraged).
- **`rand`:** Ensure we only use `rand_core` and `os_rng`. No `rand` (distributions).

### 2. Float Ban (Lints)
- Add `#![deny(clippy::float_arithmetic)]` to `src/lib.rs`.
- This ensures the Kernel never accidentally uses `f32` or `f64` for consensus logic.
- Exception: `src/query/` may use floats for *ranking scores* (dot product), but never for *truth*.

### 3. Generics Refactor
- Identify functions with many type parameters in `src/store.rs` and `src/net/`.
- Convert to `&dyn Trait` where I/O is the bottleneck.
- Example: `fn sync<S: Stream>(...)` -> `fn sync(s: &mut dyn Stream, ...)`.

### 4. String Optimization
- Audit `format!` and `panic!` strings.
- Replace dynamic error strings with `static str` variants in `DharmaError`.

## Implementation Steps
1.  Update `Cargo.toml` to trim features.
2.  Add clippy lints to `src/lib.rs`.
3.  Run `cargo bloat` (if installed) or `ls -lh target/release/dh` to measure.
4.  Refactor generics in `net` and `store` modules.

## Status
- Done: `wasmi` default features disabled in `dharma-core`, clippy float lint added, net I/O generics changed to trait objects.
- Done: Release size check — `target/release/dhd` is 866K (Jan 16, 2026).
- Remaining: error string compaction, dependency tree audit for serde/rand in core.
