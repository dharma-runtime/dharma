# Task: DHARMA-SYNC/1 Protocol Completion

## Objective
Align sync behavior and peer handling with README DHARMA-SYNC/1.

## Requirements
- Hello/inv/get/obj/err frames include capabilities + suite info.
- Subscription/interest filtering (avoid gossiping everything).
- Peer trust/ban enforcement in sync loop.
- Bind peer SubjectId to verified identity assertion (beyond signature proof).
- Overlay disclosure by org/role ACLs (not just subject/namespace policy).

## Implementation Details
- Extend hello payload with suite + capabilities.
- Add per-peer subscription state + subject filters.
- Add trust store for peers; apply allow/deny to requests.
- Add identity assertion verification path and pin identity -> pubkey mapping.
- Extend overlay policy to evaluate org/role claims (contract-driven).

## Acceptance Criteria
- Sync respects subscription filters and peer trust rules.
- Handshake identity verification uses on-chain identity assertions.
- Overlay disclosure matches org/role policy.
