# DHARMA Test Philosophy: "Trust but Verify"

## Core Principle
DHARMA is a "Truth Machine." If it corrupts data, it fails its primary purpose.
Therefore, testing must go beyond "unit tests passed." We must aggressively hunt for edge cases, non-determinism, and protocol divergence.

**We do not prove correctness (yet). We simulate failure until we are bored.**

---

## 1. The Hierarchy of Confidence

### Level 1: Unit Tests (The Baseline)
*   **Scope:** Individual functions (`parse_header`, `verify_signature`).
*   **Tool:** `cargo test`.
*   **Mandate:** 100% coverage of error paths. Every `Err(...)` must be triggered by a test.

### Level 2: Property-Based Testing (The Fuzzer)
*   **Scope:** Encoders, Decoders, Mergers.
*   **Tool:** `proptest` or `quickcheck`.
*   **Philosophy:** "Don't test `1 + 1 = 2`. Test `a + b = b + a` for random `a, b`."
*   **Targets:**
    *   `Canonical CBOR`: Encode(Decode(bytes)) == bytes.
    *   `DHL Parser`: Parse(Print(ast)) == ast.
    *   `Merge Logic`: `merge(A, B)` must equal `merge(B, A)` (commutativity).

### Level 3: Deterministic Simulation (The Time Machine)
*   **Scope:** Concurrency, Networking, Sync.
*   **Tool:** `shuttle` or `turmoil`.
*   **Philosophy:** "If it bugs once, it must bug every time."
*   **Method:**
    *   Run the Sync Loop in a simulated network.
    *   Inject delays, packet drops, and reorderings.
    *   Run 10,000 permutations with a fixed seed.
    *   Assert that all nodes converge to the same `Frontier`.

### Level 4: The "Chaos" Integration (The Real World)
*   **Scope:** `dhd` binary, Filesystem I/O.
*   **Tool:** Docker / Shell scripts (`tests/conformance/`).
*   **Method:**
    *   Spin up 5 nodes.
    *   `kill -9` a node during a write.
    *   Corrupt a `log.bin` file (flip a bit).
    *   Restart and ensure `dhd` recovers or fails safely (no silent corruption).

---

## 2. Specific Testing Tactics

### A. The "1MB" Constraint Check
*   **CI Step:** Fail the build if `target/release/dh-runtime` > 1.05 MB.
*   **Why:** Performance regression is a bug. Size regression is a bug.

### B. The "DHL" Fuzz
*   Generate random valid DHL contracts.
*   Compile them to Wasm.
*   Execute them with random Inputs.
*   **Invariant:** The VM must *never* panic. It must return `Ok` or `Err`. If `wasmi` panics, we lose.

### C. The "Protocol" Compat
*   **Golden Vectors:** Store raw hex files of "Valid V1 Assertions" in `tests/vectors/`.
*   **Rule:** A new version of DHARMA *must* be able to ingest these vectors. Breaking this requires a protocol version bump.

---

## 3. What we DO NOT do

*   **Mocking the World:** Do not mock the Filesystem or Network logic inside the Kernel. Use trait abstraction (`Store`, `Transport`) and implement **In-Memory** backends for testing. This is better than mocking.
*   **Formal Verification (Yet):** We rely on Rust's type system + aggressive fuzzing. Formal proofs are for V2.

---

## 4. Summary Checklist for PRs

1.  [ ] **Unit:** Did you test the error case?
2.  [ ] **Prop:** Did you fuzz the parser?
3.  [ ] **Sim:** Did you run the sync loop with `shuttle` (if touching net)?
4.  [ ] **Size:** Did you blow the binary size?

**"If it isn't tested, it's broken. If it's tested once, it's lucky. If it's fuzzed, it's DHARMA."**
