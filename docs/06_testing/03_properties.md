# Property Catalog (Current)

This is the **exact set of properties executed** by `dh test` today.
The source of truth is `dharma-test/src/lib.rs`.

---

## Execution Notes

- **RNG**: `ChaCha20Rng` seeded with the run seed; the same RNG instance is
  reused across properties **in order**.
- **Iterations**:
  - Normal: 25
  - Deep: 200
- **Per-iteration vs single-run**: Properties marked **(per-iteration)** loop
  for `iterations`. The rest ignore the `iterations` parameter and run once per
  seed.

---

## Encoding & Crypto

### P-CBOR-001 (per-iteration)
**Canonical CBOR roundtrip determinism.**

For each iteration:
1. Generate a random `Value` (`rand_value`):
   - depth <= 2: `int | bool | text | array | map`
   - depth > 2: only `int | bool | text`
   - arrays: length 0..3
   - maps: 0..2 entries with **text keys**
2. Encode with `encode_canonical_value`.
3. Decode with `ensure_canonical`.
4. Re-encode and require **byte-for-byte equality**.

### P-CBOR-002 (single-run)
**Reject a known non-canonical byte sequence.**

`ensure_canonical([0xbf, 0x61, 0x61, 0x01, 0xff])` must **fail**.

### P-ASSERT-001 (per-iteration)
**Signature verifies for valid payloads.**

For each iteration:
1. Generate keypair + random subject.
2. Build header:
   - `typ = "action.Set"`, `seq = 1`, `prev = None`
   - random `schema` and `contract` IDs
3. Body: `{ "value": <random i64> }`
4. Sign and require `verify_signature == true`.

### P-ASSERT-002 (per-iteration)
**Signature rejects tampered payloads.**

Same setup as P-ASSERT-001, but mutate body from `{value: 1}` to `{value: 2}`.
`verify_signature` must **fail**.

### P-ASSERT-003 (per-iteration)
**Structural rules reject invalid `seq/prev`.**

For each iteration:
1. Create an assertion with `seq = 2`, `prev = None`.
2. `structural_validate(.., None)` must return **Reject**.

### P-ENV-001 (per-iteration)
**Envelope encrypt/decrypt roundtrip.**

For each iteration:
1. Random 32-byte key, random `KeyId`, random nonce.
2. Random 32-byte plaintext.
3. Encrypt then decrypt; bytes must match.

### P-ENV-002 (per-iteration)
**Envelope decryption fails with wrong key.**

Same as P-ENV-001, but decrypt with a different random key; must **fail**.

### P-ENV-003 (per-iteration)
**Envelope ID changes on mutation.**

For each iteration:
1. Encrypt a random plaintext (16 bytes) into an envelope.
2. Compute `envelope_id` from CBOR bytes.
3. Flip the first byte of the CBOR bytes.
4. Recompute the ID and require **ID != original**.

---

## Ordering & DAG

### P-DAG-001 (single-run)
**Topological ordering respects dependencies.**

Build a linear chain of 3 assertions (`prev` links) and require:
`order_assertions` returns exactly 3 items.

### P-DAG-002 (single-run)
**Cycles are detected.**

Create `A -> B`, then mutate `B.prev = B` (self-cycle). `order_assertions` must
return **error**.

---

## Storage & Indexing

### P-STORE-001 (single-run)
**Ingest is idempotent for identical envelopes.**

1. Create CQRS artifacts in the store (schema + contract objects).
2. Build a signed assertion, wrap in an envelope with a subject key.
3. Ingest the same envelope twice.
4. Both ingests must be `Accepted` or `Pending` (never error).

### P-STORE-002 (single-run)
**Frontier index rebuild is deterministic.**

1. Ingest one assertion.
2. Read frontier tips from the in-memory index.
3. Rebuild a new `FrontierIndex` from disk and compare tips.

---

## Determinism & Convergence

### P-DET-001 (single-run)
**Replay determinism across stores.**

1. Build two `SimEnv`s.
2. Write identical assertion logs to both.
3. `load_state` must return identical memory.

### P-CONV-001 (single-run)
**File-level convergence without network.**

1. Create 3 nodes, each with its own subject and 3 assertions.
2. Copy all subjects’ assertion logs to every node.
3. `snapshot_env` must match across all nodes.

---

## CQRS Runtime

### P-CQRS-001 (single-run)
**Replay is deterministic.**

1. Append 3 `action.Set` assertions.
2. Call `load_state` twice; memory must be identical.

### P-CQRS-002 (single-run)
**Decode is deterministic.**

1. Append a single `action.Set`.
2. `decode_state` on the same memory twice must match exactly.

---

## DHARMA-Q

### P-Q-001 (single-run)
**Index rebuild is deterministic.**

1. Append 3 `action.Note` assertions containing `"hello"`.
2. `rebuild_env` + `search_env("hello")` twice must return identical rows.

### P-Q-002 (single-run)
**AND filter commutativity.**

Execute two logically equivalent `AND` filters with reversed order and require
identical result sets.

---

## Conformance Vectors (Executed After Properties)

Vectors live under `tests/vectors/*` and are executed once per run (not per
seed). Each `.meta` file provides an `expect` outcome and optional `key_hex`.

- **cbor**:
  - `expect: canonical` -> `ensure_canonical` must pass
  - `expect: reject` -> `ensure_canonical` must fail
- **assertion**:
  - `expect: accept` -> `AssertionPlaintext::from_cbor` + `verify_signature` must pass
  - `expect: reject` -> parsing or signature must fail
- **envelope**:
  - `expect: decrypt` -> decrypt with `key_hex` must succeed
  - `expect: reject` -> decrypt must fail
- **schema**:
  - `expect: accept` -> `parse_schema` must pass
  - `expect: reject` -> `parse_schema` must fail
- **contract**:
  - load `.wasm` or compile `.wat`
  - run `validate` with empty args
  - `expect: accept` -> validate succeeds
  - `expect: reject` -> validate fails
