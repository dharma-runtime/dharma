# Task 21: Fabric Types & Ads (Discovery & Sharding)

## Goal
Implement the core data structures for DHARMA-FABRIC: ShardMaps (static topology), Advertisements (dynamic liveness), and **Domain Sovereignty metadata** (ownership + policy hash + oracle ads).

## Why
- **ShardMap:** Clients need to know *which* shard holds the data (key -> shard).
- **Ads:** Clients need to know *who* hosts that shard right now (shard -> provider).
- **Sovereignty:** Domain owners (e.g. `corp.ph.cmdv`) must control *who* can execute *what* and *which* oracles are offered.

## Specification

### 1. ShardMap (Hard State)
A ShardMap defines the sharding topology for a table or subject space.
- **Location:** Stored as an assertion in a config subject (e.g., `sys.config`).
- **Structure (Rust Struct):**
  ```rust
  struct ShardMap {
      table: String,           // e.g., "invoice"
      strategy: ShardingStrategy, // Hash(key) or Time(col) + Hash(key)
      key_col: String,         // e.g., "id"
      shard_count: u32,        // N (e.g., 64)
      replication_factor: u8,  // R (e.g., 3)
      // Optional: Explicit replica sets (if static)
      // replica_sets: Map<u32, Vec<PeerId>> 
  }

  enum ShardingStrategy {
      Hash,        // CRC32(key) % N
      TimeAndHash, // Partition by Day/Month, then Hash
  }
  ```

### 2. Advertisement (Soft State)
A signed, ephemeral statement by a Provider declaring what they serve.
- **Wire Format:** Canonical CBOR, signed by Provider Identity Key.
- **Fields:**
  - `v`: 1
  - `provider_id`: 32-byte PubKey
  - `ts`: Timestamp (u64)
  - `ttl`: Seconds (u32)
  - `endpoints`: List of `(Protocol, Address)`
  - `shards`: List of `(Table, ShardId, Watermark)`
  - `load`: u8 (0-255, where 255 = overloaded)
  - `domain`: String (e.g. `corp.ph.cmdv`)
  - `policy_hash`: Bytes32 (hash of Domain Policy from Atlas)
  - `oracles`: List of Oracle Ads (below)
  - `sig`: Signature

### 2.1 Oracle Advertisement (Bridge Ads)
Each Provider can advertise **oracles/bridges** scoped to a domain.
```rust
struct OracleAd {
    name: String,            // e.g. "email.send" or "maps.matrix"
    domain: String,          // e.g. "corp.ph.cmdv"
    mode: OracleMode,        // InputOnly | RequestReply | OutputOnly
    timing: OracleTiming,    // Sync | Async
    input_schema: SchemaId,  // expected input payload
    output_schema: Option<SchemaId>, // response payload (if RequestReply)
    max_inflight: Option<u32>,
    timeout_ms: Option<u64>,
}

enum OracleMode {
    InputOnly,     // emits actions/inputs
    RequestReply,  // API-like
    OutputOnly,    // emits externally, no reply required
}

enum OracleTiming {
    Sync,          // request waits for immediate response
    Async,         // request is queued, response arrives later
}
```

### 3. Ad Store (In-Memory)
A simple registry to hold active ads.
- **Map:** `ProviderId -> Advertisement`
- **Index:** `(Table, ShardId) -> Vec<ProviderId>` (sorted by freshness/load)
- **GC:** Remove ads where `ts + ttl < now`.

## Implementation Steps
1.  Define structs in `src/fabric/types.rs`.
2.  Implement `ShardMap::resolve(key) -> ShardId`.
3.  Implement `Advertisement::verify(&self) -> bool`.
4.  Create `AdStore` with `insert`, `get_providers_for_shard`, and `prune`.
5.  Add `OracleAd`, `OracleMode`, and `OracleTiming` types, include them in Ads.
6.  Add `domain` + `policy_hash` fields and include in verification.
7.  **Testbed:** implement IAM contact-gated visibility (Accepted contacts can see private IAM fields). This is the initial Fabric enforcement testbed. See `tasks/57_iam_contact_visibility.md`.
