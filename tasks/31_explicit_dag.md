# Task 31: Explicit DAG & Merge Rules (Ordering Hardening)

## Goal
Eliminate implicit topological sorting instability by enforcing an explicit DAG (Directed Acyclic Graph) in the kernel.

## Specification

### 1. Graph Constraints
*   **Genesis:** `seq=1`, `prev=null`.
*   **Linear Chain:** `seq=N`, `prev=AssertionID(seq=N-1)`.
*   **Fork:** Two assertions with same `prev`.
    *   This is **allowed** at the storage layer.
    *   This represents concurrency (two devices, or two users).

### 2. Contract Enforcement
The DHL Contract defines how to handle the Graph.
*   **Mode A: Strict (Default):**
    *   `validate` checks: `is_linear(history)`.
    *   If fork exists, the contract **Rejects** (or Pending) until a `Merge` assertion arrives.
*   **Mode B: CRDT / Commutative:**
    *   `reduce` iterates over the DAG in a deterministic order (e.g., `AssertionID` sort) but acknowledges branches.
    *   State is the result of `fold(branches)`.

### 3. Merge Assertion (`core.merge`)
A standard primitive to resolve forks.
*   **Type:** `core.merge`.
*   **Header:** `refs = [BranchTipA, BranchTipB]`.
*   **Body:** (Optional) Conflict resolution data.
*   **Function:** Joins two branches back into one.

## Implementation Steps
1.  **Frontier Logic:** Update `FrontierIndex` to track the full DAG tips, not just "latest".
2.  **Validator:** Enforce `prev` existence strictly.
3.  **DHL:** Add `concurrency: strict | allow` to DHL header.
