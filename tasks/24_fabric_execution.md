# Task 24: Fabric Execution Protocol (Wire Layer)

## Goal
Define the request/response structs and the execution dispatcher for Fast Path and Wide Path, including **domain-scoped authorization** and **oracle invocation**.

## Why
- We need a standard way to send a query/action to a shard and get a result.
- Predefined queries must be enforced by policy; custom queries require explicit token flags.

## Specification

### 1. Request Object
```rust
struct FabricRequest {
    req_id: [u8; 16],       // UUID
    cap: CapToken,          // Authorization
    op: FabricOp,           // Operation
    deadline: u64,          // Absolute timeout
}

enum FabricOp {
    ExecAction { subject: SubjectId, action: String, args: Value },
    ExecQuery { subject: SubjectId, query: String, params: Value, predefined: bool },
    QueryFast { table: String, key: Value, query: String },
    QueryWide { table: String, shard: u32, query: String },
    Fetch { oid: ObjectId },
    OracleInvoke { name: String, mode: OracleMode, timing: OracleTiming, input: Value },
}
```

### 2. Response Object
```rust
struct FabricResponse {
    req_id: [u8; 16],
    status: u8,             // 200 OK, 4xx, 5xx
    watermark: u64,         // Data freshness
    payload: Vec<u8>,       // Result (CBOR table, Object bytes)
    stats: ExecStats,       // Time taken, rows scanned
    provenance: Option<Vec<ObjectId>>,
}
```

### 3. The Dispatcher (Server Side)
- Receives `FabricRequest`.
- Verifies `cap`.
- Checks `deadline` (reject if expired).
- Routes to local engine:
  - `ExecAction` -> `dharma::runtime::apply_action`
  - `ExecQuery` -> `dharma::query::execute` (predefined only unless token flag allows custom)
  - `QueryFast/Wide` -> `dharma::query::execute`
  - `Fetch` -> `dharma::store::get`
  - `OracleInvoke` -> oracle handler (bridge)
- Returns `FabricResponse`.

### 4. Wide Path: Scatter/Gather (Client Side)
- **Scatter:**
  - Decompose query into shards.
  - Send `QueryWide` to all shards in parallel.
- **Gather:**
  - Wait for results.
  - Merge Monoids (Sum, Count, etc.).
  - Handle failures (partial result if allowed, or fail).

## Implementation Steps
1.  Define types in `src/fabric/protocol.rs`.
2.  Implement `Dispatcher` trait (interface to core/query).
3.  Implement `ScatterGather` logic (simple parallel loop).
4.  Enforce token checks for `predefined` queries vs custom queries.
5.  Implement oracle invocation hook for `OracleInvoke`.
6.  For `OracleInvoke` with `timing=Async`, enqueue into the oracle job queue (see dev_docs/oracles.md).
