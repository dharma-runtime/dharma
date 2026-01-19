# Task 23: Fabric Router (Client Logic)

## Goal
Implement the logic that decides *where* to send a request (including oracle routing).
This is the "Smart Client" that executes the Fabric routing algorithm.

## Why
- **Decentralization:** No load balancer. The client balances load.
- **Reliability:** Hedging reduces tail latency.
- **Sovereignty:** Only route requests that comply with domain policy + capability tokens and oracle claims.

## Specification

### 1. The `Router` Struct
Holds:
- `shard_maps`: Cache of `ShardMap` configs.
- `ads`: Reference to `AdStore`.
- `config`: Timeout settings, hedging policy.
- `policy`: Domain policy cache (from Atlas/Directory).

### 2. Route Resolution
`resolve(table, key, cap) -> Vec<Endpoint>`
1.  **Shard Selection:**
    - `map = shard_maps.get(table)`
    - `shard_id = map.resolve(key)`
2.  **Provider Discovery:**
    - `providers = ads.get_providers_for_shard(table, shard_id)`
3.  **Ranking:**
    - Score providers based on:
      - `freshness` (Watermark close to now?)
      - `load` (Reported load)
      - `rtt` (Local observation)
    - Sort `providers` by score.
4.  **Selection:** Return top N (usually 2 for hedging).
5.  **Policy Filter:** drop providers whose `policy_hash` or domain mismatches.
6.  **Oracle Filter:** only providers advertising required oracle `name/mode/timing`.

### 3. Hedging Logic (The "Jeff Dean" Special)
- Send Request to Provider A.
- Wait `HEDGE_DELAY` (e.g., 10ms).
- If no response, Send Request to Provider B.
- First response wins. Cancel the other.

## Implementation Steps
1.  Create `src/fabric/router.rs`.
2.  Implement ranking logic.
3.  Implement `resolve` function.
4.  (Optional for V1) Implement the async `hedged_request` wrapper (requires async or threads). For V1 sync, maybe just "try A, then B on timeout".
5.  Enforce token flags: block custom query if token lacks `AllowCustomQuery`.
6.  Enforce allowlists: if token has `actions/queries`, only route those.
