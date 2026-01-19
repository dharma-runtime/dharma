# Task 25: Directory & Relay System (V1 Discovery)

## Goal
Implement the "Control Plane" for DHARMA-FABRIC: how nodes discover Relays and how Relays discover each other,
and how **domains are owned and governed** (Atlas + Directory).
We replace the complex DHT/Atlas with a simpler **Directory Subject** mechanism for V1.

## Why
- **Bootstrapping:** Nodes need a way to find their first peer.
- **Topology:** Relays need to know which shards exist (`ShardMap`).
- **Liveness:** Clients need to know which Relays are online (`Ads`).
- **Sovereignty:** Domain owners must publish policy, levels, and delegates.

## Specification

### 1. The Directory Subject (`sys.directory`)
This is a standard DHARMA Subject managed by the organization (or user) running the mesh.
- **Type:** `sys.directory` (Kernel reserved).
- **Content:**
  - `fabric.shardmap.define`: Hard-state Sharding Configuration (Task 21).
  - `fabric.provider.register`: Permanent registration of a Relay/Provider identity (Public Key + Static Endpoint).
  - `fabric.provider.revoke`: Removal of a provider.
  - `fabric.domain.request`: Child-domain registration request (signed by requester).
  - `fabric.domain.authorize`: Parent-domain approval for a child (signed by parent owner).
  - `fabric.domain.register`: Domain ownership record (root key, requires parent authorization if nested).
  - `fabric.domain.policy`: Domain policy and authorization levels.

### 2. Relay "Blind Mode"
Relays must store and forward encrypted envelopes without keys.
- **Config:** `dhd --relay --storage ./data --public-addr 1.2.3.4:3000`
- **Behavior:**
  - Accepts `INV/OBJ` for *any* subject (possibly subject to rate limits/allowlists).
  - Does *not* validate semantic rules (Schema/Contract).
  - Validates **Structural Integrity** (Hash match, Sig valid) only.
  - Stores blobs in `data/objects/`.
  - Maintains `FrontierIndex` for all seen subjects.

### 3. The "Seed" Mechanism
How do we find `sys.directory`?
- **Client Config:** `seeds = ["relay.dharma.org:3000", "backup.dharma.org:3000"]`
- **Boot Sequence:**
  1.  Connect to random Seed.
  2.  Sync `sys.directory` (Subject ID derived from Org Key or hardcoded).
  3.  Load `ShardMap` from `sys.directory`.
  4.  Fetch **Ads** (Liveness) from the Seed (Soft-state, not in ledger).
  5.  Now the client has the full map. It can route traffic via Fabric (Task 23).

### 4. Liveness Propagation (Gossip Lite)
Since Ads are soft-state (not in the ledger), they must be propagated differently.
- **Mechanism:** `AD_BROADCAST` message (over the wire, transient).
- **Rule:**
  - Relay receives `Ad` from Provider P.
  - Checks signature.
  - Caches in memory (`AdStore`).
  - Forwards to connected peers (with simple hop limit/dedup).
  - Client connects to Relay -> Asks `GET /ads` -> Relay dumps `AdStore`.

## Implementation Steps
1.  **Schema:** Define `sys.directory` schema (Task 21 types wrapped in assertions).
2.  **Relay Logic:** Add `--relay` flag to `dh`. Implement "Blind Ingest" path in `store` (skip contract/schema checks).
3.  **Seed Client:** Implement `DirectoryClient` that connects to seeds and syncs the directory subject on startup.
4.  **Ad Gossip:** Add `TYPE_AD` to the wire protocol (Task 24 context) and a simple broadcast loop.
5.  **Domain Policy:** Add `fabric.domain.register` + `fabric.domain.policy` assertions, enforce parent authorization for nested domains, and validate ad `policy_hash`.

### 1.1 Domain Ownership (Atlas-lite)
Each domain (e.g. `corp.ph.cmdv`) publishes:
- `owner_key`: IdentityKey (root authority)
- `policy`: levels + feature flags + allowed oracles
- `delegates`: keys allowed to issue CapTokens
Clients validate `policy_hash` in Ads against the current policy record.

### 1.2 Parent Authorization (Hierarchical Domains)
Domains are hierarchical (`corp.ph.cmdv` is a child of `corp.ph`).
Child domains **must** obtain explicit approval from the parent domain owner.

- **Request:** `fabric.domain.request`
  - `domain`: child domain (e.g. `corp.ph.cmdv`)
  - `parent`: parent domain (e.g. `corp.ph`)
  - `requester_key`: proposed owner key for the child
  - `note` (optional): reason/metadata
  - Signed by the requester.

- **Authorize:** `fabric.domain.authorize`
  - `domain`: child domain
  - `parent`: parent domain
  - `request_id`: hash/id of the request being approved
  - `authorized_owner`: owner key granted for the child domain
  - Signed by the **parent** domain owner key.

- **Register:** `fabric.domain.register`
  - `domain`: child domain
  - `owner_key`: must match `authorized_owner`
  - `parent_auth`: reference to `fabric.domain.authorize` (hash/id)

**Validation rule:** a `fabric.domain.register` for a nested domain is only valid
if a corresponding `fabric.domain.authorize` exists and is signed by the parent’s
current owner key.

### 1.3 Journey Reference
End-to-end flow (create parent, request child, authorize, register, onboard members)
is documented in `docs/02_tutorial/04_domain_journey.md`.
