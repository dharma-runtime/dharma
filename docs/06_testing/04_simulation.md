# Simulation Phase (dh test --deep)

The simulation phase runs **only** when `--deep` is supplied. It exercises the
real handshake + sync loop using the deterministic in-process `SimHub`.

Source of truth: `dharma-test/src/lib.rs::sim_convergence`.

---

## High-Level Flow

1. **Seeded RNG**: `ChaCha20Rng::seed_from_u64(seed ^ 0x5a5a_1234)`.
2. **Network Hub**: `SimHub::new(rng.next_u64())`.
3. **Create 3 nodes**:
   - `SimEnv`, `Store`, `FrontierIndex` per node.
   - Identity created via `make_identity`.
4. **Seed identity** (per node):
   - Write `core.genesis` assertion for the node’s identity subject.
   - Uses **note** schema/contract (`builtins::ensure_note_artifacts`).
   - Updates frontier index + semantic index.
5. **Create a local chain** (per node):
   - Subject = node’s **identity subject**.
   - 3 assertions: `note.text`, seq **2..4**, `prev = genesis`.
   - Body: `{ "text": "value <seq>" }`.
   - Ingested via `ingest_object`.
6. **Start hub runner** thread:
   - `hub.step()` in a loop.
   - Applies chaos timeline if enabled.
7. **Sync pairs**:
   - `(1,2)`, `(2,3)`, `(1,3)` via `sync_pair_real`.
   - If `--chaos`, an extra round of the same pairs runs after the first pass.
8. **Stop hub runner**, snapshot node 0, compare all other nodes.
   - Snapshot entries are `(subject, seq, assertion_id)`.
   - Any mismatch fails the simulation.

On failure, the runner **replays with deep trace** and attaches it to the ticket.

---

## What `sync_pair_real` Does

Each pair runs the **real handshake + sync loop**:

- `server_handshake` on one side, `client_handshake` on the other.
- `sync_loop` exchanges inventory, GET, OBJ frames.
- Overlay access uses default `OverlayPolicy` / `OverlayAccess`.
- Subscriptions are loaded from `subscriptions.*` files (defaults to allow all).

Termination logic:

- Waits until both sides are “ready”.
- Then waits for **200 idle cycles** (no pending deliveries).
- Aborts with **error if no activity for 5 seconds** (global log-stale timer).

---

## Chaos Mode (dh test --deep --chaos)

Chaos is **only** applied during simulation. Property + vector phases are
unchanged.

### Network Faults (SimHub)

At time `t = 0`:
- `drop_rate = 0.1`
- `dup_rate = 0.05`
- `min_delay = 0`
- `max_delay = 5`

At time `t = clear_at`:
- Faults reset to `drop=0, dup=0, min_delay=0, max_delay=0`.
- `clear_at = 25 + (rng % 25)` → range **25..49**.

### Filesystem Faults (per node)

Applied at `t = 0`:

- `enospc_after = 65536`
- `torn_every = 17`, `torn_max = 32`
- `fsync_lie_every = 19`
- `read_corrupt_every = 23`, `read_corrupt_bits = 0x01`

### Clock Faults (per node)

Applied at `t = 0`:

- `drift_per_call = rng % 3`
- `jump_every = 5 + rng % 5`
- `jump_amount = rng % 20`
- `monotonic_violation_every = 7 + rng % 5`
- `monotonic_backstep = 1 + rng % 3`

### Crash / Restart

Single-node outage on node 2 (`nodes[1]`):

- `crash_at = 5 + rng % 5`
- `restart_at = crash_at + 5 + rng % 10`

### Network Partition

Between node 1 and node 3 (`nodes[0]` <-> `nodes[2]`):

- `part_at = 3 + rng % 5`
- `heal_at = part_at + 4 + rng % 6`

