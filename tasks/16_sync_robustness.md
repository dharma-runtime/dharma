# Task 16: Sync Robustness (Range/Merkle Sync)

## Goal
Improve the synchronization protocol to handle large frontiers efficiently.
Avoid sending the entire list of tips in the `Inv` message.

## Why
- **Bandwidth:** Sending 1000 tips for a hot subject is wasteful.
- **Latency:** Large messages block the connection.
- **Privacy:** Leaking *all* tips might reveal more than intended (though privacy overlays help).

## Specification

### 1. Merkle Frontier (Ideal)
- Maintain a Merkle Tree (or Merkle Mountain Range) of the subject's history.
- **Sync:** Exchange Root Hash. If match, done. If diff, descend tree.
- **Complexity:** High. Requires maintaining the tree.

### 2. Range-Based / Timestamp Sync (Pragmatic V1)
- **Current:** `Inv { subject, tips: [...] }`
- **New:** `Inv { subject, time_range: (start, end), summary_hash: ... }`
- OR simply **Time-Window Sync**:
  - "Give me everything since T".
  - Most syncs are "catch up".
  - If T is too old, fall back to "Full Scan".

### Specification (Range Reconciliation)
1.  **Inv Message Update:**
    - Add `since_seq: u64` or `since_ts: u64`.
    - Peers only reply with tips/objects newer than that mark.
2.  **Bloom Filter (Optional):**
    - Send a Bloom filter of known tips.
    - Peer sends back only objects *not* in filter.

## Implementation Steps
1.  Update `SyncMessage` CBOR schema (backward compatible if possible, or bump version).
2.  In `FrontierIndex`, track `max_seq` per subject.
3.  Modify `handle_inv` to support `since` parameters.
4.  Implement the logic to fetch "missing" items based on the optimized exchange.
