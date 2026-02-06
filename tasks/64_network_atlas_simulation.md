# Simulation Test Plan: PRD v1 (Identity, Domains, Keys, Emergency)

This document outlines the comprehensive test cases required for the Simulation Automated Testing Suite (SATS). These tests go beyond simple unit tests by verifying system invariants under randomized conditions, concurrency, partitions, and adversarial inputs.

## Global Test Harness Requirements
- **Seeded determinism:** Every scenario must be reproducible by seed. Record seed + failure trace.
- **Fault injection:** Use network partitions, latency, message loss, torn writes, and clock skew.
- **Replay:** Any failure must be replayable with the same seed until fixed.
- **Traceability:** Each scenario logs the causal ordering of assertions, sync exchanges, and accept/reject decisions.
- **Baseline invariants:** No panics, no deadlocks, no sync loops that exceed timeout unless explicitly testing timeout recovery.
- **Topology matrix:** Run scenarios in direct (peer-to-peer), relay, and mixed topologies.

## 1. Ops, Recovery & Tooling Simulation (Task 48)

### Invariants
- **Backup integrity:** Restored node converges to the same frontiers and assertions as source.
- **GC safety:** `dh gc` never deletes referenced blobs or frontier tips.
- **Doctor accuracy:** `dh doctor` detects broken prerequisites (clock skew, disk full, peer unreachable).
- **Metrics sanity:** Counters monotonic, gauges non-negative, histogram buckets sum to total count.

### Scenarios
1.  **Hot Backup:**
    -   **Setup:** Node is actively syncing while `dh backup export` runs.
    -   **Expectation:** Backup completes; restore produces identical state after replay.
    -   **Property:** `state_hash(source) == state_hash(restored)`.

2.  **Restore + Catch-up:**
    -   **Setup:** Restore a backup on a new node, then connect to relay with newer data.
    -   **Expectation:** Node catches up without reintroducing old data.
    -   **Property:** `frontier(restored)` matches peers after sync.

3.  **GC Under Load:**
    -   **Setup:** Large pending queue + active subjects; run `dh gc`.
    -   **Expectation:** Stale pending pruned; active data preserved; sync still resolves new deps.
    -   **Property:** `no_missing_refs` for active subjects.

4.  **Doctor Failure Modes:**
    -   **Setup:** Simulate clock skew, disk full, missing permissions, peer unreachable.
    -   **Expectation:** `dh doctor` flags each issue with non-zero exit.
    -   **Property:** `doctor.failures == expected`.

5.  **Metrics Consistency:**
    -   **Setup:** Run N writes + M syncs.
    -   **Expectation:** `assertions_ingested == N`, `sync_latency` recorded.
    -   **Property:** counters/gauges match observed trace.

## 2. Identity (Atlas) Simulation

### Invariants
- **Genesis Singularity:** A subject has exactly one genesis assertion (seq: 1). No branches or forks can create a second genesis for the same subject ID.
- **Lifecycle Monotonicity:** `Active` -> `Suspended` -> `Active` is allowed. `Revoked` is terminal. No path exists from `Revoked` to `Active`.
- **Handle Uniqueness (Local):** A node never manages two active Atlas identities simultaneously.
- **Local Handle Isolation:** `local_handle` never appears in assertions or sync artifacts.

### Scenarios
1.  **The "Genesis Race":**
    -   **Setup:** Multiple concurrent actors attempt to initialize the same Subject ID with different `atlas.identity.genesis` payloads.
    -   **Expectation:** Only one wins (first valid write). All others are rejected as "Sequence Mismatch" or "Genesis Exists".
    -   **Property:** `count(genesis_assertions) == 1`.

2.  **The "Zombie" Lifecycle:**
    -   **Setup:** An identity is revoked. Concurrent actors attempt to sign `activate`, `suspend`, or new `genesis` assertions for it.
    -   **Expectation:** All are rejected.
    -   **Property:** `status(subject) == Revoked` invariant.

3.  **The "Imposter" Handshake:**
    -   **Setup:** Peer A performs a handshake using a valid Subject ID but signs the handshake with a `suspended` or `revoked` key.
    -   **Expectation:** Handshake completes but `verification_status` is `Unverified` or `Failed`.
    -   **Property:** No `Verified` session with non-active identity.

4.  **Local Handle Leak Test:**
    -   **Setup:** Set `local_handle` in config; attempt to insert or sync an assertion containing `local_handle`.
    -   **Expectation:** Assertion rejected; no remote visibility.
    -   **Property:** `local_handle` never appears in remote state.

## 3. Domain & Membership Simulation

### Invariants
- **Hierarchy Integrity:** A child domain `a.b.c` cannot exist without `a.b`'s authorization.
- **Membership Consistency:** `is_member(user, domain)` implies a valid, unbroken chain of `Invite` -> `Approve` (or `Request` -> `Approve`) and no subsequent `Revoke` or `Leave`.
- **Acting Context:** No action is accepted where `acting_domain` is set but the signer is not a valid member with the required role.
- **Directory Ownership Binding:** Directory registration owner must equal domain contract owner.

### Scenarios
1.  **Recursive Domain Genesis:**
    -   **Setup:** Create `root`, `root.sub`, `root.sub.child` in random order across partitioned nodes.
    -   **Expectation:** `root.sub` only valid after `root` authorizes it. `root.sub.child` only valid after `root.sub` exists.
    -   **Property:** `exists(child) -> exists(parent) AND authorized(child)`.

2.  **Concurrent Membership Flux:**
    -   **Setup:** Admin A invites User X. Admin B revokes User X. User X attempts to `Accept`. User X attempts to `Act` as domain.
    -   **Expectation:** Outcomes depend on causal ordering. If Revoke < Accept, Accept fails. If Accept < Revoke, Act fails.
    -   **Property:** `can_act(X, Domain)` is strictly consistent with the causal history of membership events.

3.  **The "Double Agent" (Acting Context):**
    -   **Setup:** User X is a member of Domain A and Domain B. User X attempts to sign an action claiming `acting_domain: A` but using Domain B's logic/keys, or acting as both.
    -   **Expectation:** Rejection.
    -   **Property:** `action.accepted -> context.valid`.

4.  **Directory Owner Mismatch:**
    -   **Setup:** Attempt `fabric.domain.register` with an owner that differs from the domain contract owner.
    -   **Expectation:** Registration rejected.
    -   **Property:** `register.owner == domain.owner`.

## 4. Ownership & Sharing Simulation

### Invariants
- **Exclusive Ownership:** At any logic time, `owner` resolves to exactly one entity.
- **Access Determinism:** For any Subject S and User U, `can_read(U, S)` is deterministic based on the graph of `share.grant`, `share.revoke`, and `share.public`.
- **Revocation Safety:** Once `share.revoke(U)` is causally committed, no new reads by U are permitted (key epoch rotation).
- **Transfer Rule Compliance:** Ownership transfer obeys contract-defined rules (immediate or propose/accept).

### Scenarios
1.  **Ownership Hot Potato:**
    -   **Setup:** User A transfers to B. B transfers to C. C transfers back to A. Concurrent writes occur during transfers.
    -   **Expectation:** Ownership follows the causal chain. Writes by "former" owners rejected immediately after transfer.
    -   **Property:** `writer == current_owner`.

2.  **The "Leaky" Public Toggle:**
    -   **Setup:** Owner toggles `public: true`, then `public: false` repeatedly while random peers attempt to sync/read.
    -   **Expectation:** Peers only access data when `public: true` (or if they have keys).
    -   **Property:** `read_access == (public OR has_key)`.

3.  **Role-Based Sharing:**
    -   **Setup:** Share granted to Role `viewer` in Domain D. User U is added/removed from Role `viewer`.
    -   **Expectation:** Access fluctuates with role membership.
    -   **Property:** `can_read(U) == (U in Role)`.

4.  **Transfer Propose/Accept:**
    -   **Setup:** `subject.transfer.propose` then `subject.transfer.accept` with concurrent writes.
    -   **Expectation:** Owner changes only after accept; writes by non-owner rejected.
    -   **Property:** `owner` changes exactly once on accept.

5.  **Transfer Forbidden:**
    -   **Setup:** Attempt transfer without contract rule permitting it.
    -   **Expectation:** Reject.
    -   **Property:** `transfer_forbidden_by_default`.

## 5. Keys & Rotation Simulation

### Invariants
- **Epoch Monotonicity:** Epoch IDs only increase.
- **Data Stability:** Key rotation never corrupts or renders inaccessible previously written data (assuming old keys are retained).
- **Forward Secrecy (Revocation):** Revoked users never receive keys for Epoch N+1.
- **Grant Eligibility:** Key grants only to active members.

### Scenarios
1.  **The "Key Storm":**
    -   **Setup:** Trigger `domain.key.rotate` 100 times in rapid succession while writing data.
    -   **Expectation:** Data is written to various epochs. All remains readable by valid members.
    -   **Property:** `decryptable(data) == true` for all valid members.

2.  **Revocation Boundary:**
    -   **Setup:** User X is revoked. Domain rotates keys. New data is written.
    -   **Expectation:** User X cannot decrypt new data.
    -   **Property:** `decrypt(X, new_data) == error`.

3.  **Grant to Non-Member:**
    -   **Setup:** Issue `member.key.grant` for a non-member.
    -   **Expectation:** Rejected.
    -   **Property:** `grant -> member_active`.

4.  **Tampered SDK Envelope:**
    -   **Setup:** Corrupt an SDK envelope during sync.
    -   **Expectation:** Recipient rejects and does not update keyring.
    -   **Property:** `verify(envelope) == false -> reject`.

5.  **Offline Member Catch-up:**
    -   **Setup:** Member offline during multiple rotations; rejoins later.
    -   **Expectation:** Receives latest epoch grant; can decrypt new data but not retroactively without old keys.
    -   **Property:** `decrypt(latest) == true`, `decrypt(missed_epochs) == depends_on_keys`.

## 6. Emergency & Compromise Simulation

### Invariants
- **Freeze Immutability:** If `domain.freeze` is active, the set of accepted facts for that domain is immutable (count constant).
- **Compromise Terminality:** `domain.compromised` is the final event. Nothing follows.
- **Unfreeze Restores Writes:** `domain.unfreeze` lifts freeze unless compromised.

### Scenarios
1.  **The "Frozen" Writer:**
    -   **Setup:** Domain is frozen. 100 concurrent actors attempt to write new assertions (tasks, docs, membership).
    -   **Expectation:** 0 acceptances.
    -   **Property:** `count(assertions) @ t_freeze == count(assertions) @ t_end`.

2.  **Device Revocation Replay:**
    -   **Setup:** Revoke device key K. Attempt to replay an old signature from K, or a new signature from K.
    -   **Expectation:** Replay rejected (by nonce/seq). New signature rejected (by revocation).
    -   **Property:** `valid_sig(K) == false`.

3.  **Unfreeze + Compromise:**
    -   **Setup:** Freeze domain, unfreeze, then mark compromised.
    -   **Expectation:** Unfreeze restores writes only until compromised; after compromise, nothing accepted.
    -   **Property:** `compromised` dominates all subsequent actions.

## 7. Permissions & Fast Reject Simulation

### Invariants
- **Safety Fallback:** `Router Reject` <= `Contract Reject`. The router never rejects something the contract would allow (false positive), AND CRITICALLY, the Router never accepts something the contract would reject (false negative - though router "accept" just means "pass to contract").
- **Consistency:** Summary artifact matches actual contract logic.

### Scenarios
1.  **Summary Desync:**
    -   **Setup:** Intentionally publish a Permission Summary that allows *more* than the contract (malicious/buggy).
    -   **Expectation:** Router passes it through (Fast Accept), but Contract Validation rejects it.
    -   **Property:** `final_state` reflects Contract Logic, not Summary Logic.

2.  **Role Masquerade:**
    -   **Setup:** Send action with `acting_role: admin` while actually being `guest`.
    -   **Expectation:** Router (checking summary) sees "Admin allowed", passes to Contract. Contract (checking membership) sees "Not Admin", rejects.
    -   **Property:** No privilege escalation.

3.  **Corrupt Summary Artifact:**
    -   **Setup:** Corrupt or truncate summary artifact bytes.
    -   **Expectation:** Ignore summary; fallback to full validation.
    -   **Property:** `corrupt_summary -> full_validation`.

4.  **Cache Invalidation:**
    -   **Setup:** Change contract version or role; reuse cached summary.
    -   **Expectation:** Cache miss; new summary loaded.
    -   **Property:** `cache_key(contract, version, role, action)` correct.

## 8. Fabric Router, Directory, Ads & Cap Tokens Simulation

### Invariants
- **Ad Integrity:** Advertisements must be signed by their publishers and rejected if tampered.
- **Token Scope:** Capability tokens never grant access beyond their declared scope.
- **Directory Consistency:** Directory state converges across nodes after sync.
- **Directory Auth:** Domain registrations require owner match + parent authorization.

### Scenarios
1.  **Ad Tamper:**
    -   **Setup:** Modify ad payload bytes in transit.
    -   **Expectation:** Receiver rejects the ad.
    -   **Property:** `verify(ad) == false -> reject`.

2.  **Token Scope Escalation:**
    -   **Setup:** Present a token scoped to table A to access table B.
    -   **Expectation:** Router rejects.
    -   **Property:** `token.scope != request.scope -> reject`.

3.  **Directory Split-Brain:**
    -   **Setup:** Two partitions modify directory policies concurrently.
    -   **Expectation:** On merge, directory converges to a single state consistent with causal history.
    -   **Property:** `dir_state(A) == dir_state(B)` after convergence.

4.  **Ad Expiry / TTL:**
    -   **Setup:** Ads include TTL; peers keep stale ads past TTL.
    -   **Expectation:** Stale ads ignored; fresh ads used.
    -   **Property:** `now > ad.ttl -> ignore`.

5.  **Token Revocation / Expiry:**
    -   **Setup:** Use a token after revocation or expiry.
    -   **Expectation:** Reject.
    -   **Property:** `token.valid == false -> reject`.

## 9. IAM Contact-Gated Visibility (Fabric) Simulation

### Invariants
- **Field-level privacy:** Private IAM fields visible only to owner or accepted contacts.
- **No sync filtering:** Sync layer remains unchanged; redaction happens at execution/read time.

### Scenarios
1.  **Owner Visibility:**
    -   **Setup:** Owner queries IAM subject.
    -   **Expectation:** Sees full fields.
    -   **Property:** `owner -> full_fields`.

2.  **Accepted Contact Visibility:**
    -   **Setup:** Contact relation == `Accepted`.
    -   **Expectation:** Sees private fields.
    -   **Property:** `accepted -> full_fields`.

3.  **Non-Contact Redaction:**
    -   **Setup:** No contact or relation != Accepted.
    -   **Expectation:** Private fields omitted or null.
    -   **Property:** `non_contact -> redacted`.

4.  **Declined/Blocked:**
    -   **Setup:** Contact relation == Declined/Blocked.
    -   **Expectation:** Redaction enforced.
    -   **Property:** `declined/blocked -> redacted`.

## 10. Sync, Replay, and Determinism Simulation

### Invariants
- **Replay determinism:** Replaying assertions yields identical frontier tips and state.
- **No ghost tips:** Tips in frontier must always refer to existing assertions.
- **No missing refs:** Overlay or merge refs must exist or be pending until found.

### Scenarios
1.  **Partitioned Replay:**
    -   **Setup:** Partition nodes; each writes actions; merge and sync.
    -   **Expectation:** Same final state regardless of merge order.
    -   **Property:** `state(hash)` identical across nodes post-convergence.

2.  **Overlay Missing Base:**
    -   **Setup:** Send overlay action before base action.
    -   **Expectation:** Pending until base arrives, then accept.
    -   **Property:** `pending -> accepted` when base is present.

3.  **Fork Resolution (Strict vs Allow):**
    -   **Setup:** Induce forked sequences for strict and allow concurrency modes.
    -   **Expectation:** Strict: pending/reject; Allow: accept both.
    -   **Property:** `strict rejects forks`, `allow accepts forks`.

4.  **Missing Identity Root:**
    -   **Setup:** Sync subject whose identity root is missing; deliver identity later.
    -   **Expectation:** Subject pending until identity arrives; then resumes.
    -   **Property:** `pending(identity_root) -> accepted`.

## 11. Relay Behavior Simulation

### Invariants
- **Relay Opacity:** Relay never needs decryption to propagate objects.
- **No identity leakage:** Relay must not be able to decrypt subject data.
- **Pending propagation:** Relay propagates missing dependencies and resumes when they arrive.

### Scenarios
1.  **Opaque Relay Forwarding:**
    -   **Setup:** Relay receives encrypted objects without keys.
    -   **Expectation:** Relay stores and forwards objects; client decrypts.
    -   **Property:** `relay accepts opaque -> client accepts/decrypts`.

2.  **Relay Missing Dependency:**
    -   **Setup:** Relay receives an assertion referencing missing prev.
    -   **Expectation:** Relay requests missing; once received, forwards.
    -   **Property:** `pending(missing) -> resolved`.

3.  **Relay Under Load:**
    -   **Setup:** High fan-in and fan-out with ads and sync at once.
    -   **Expectation:** No deadlocks; throughput remains non-zero.
    -   **Property:** `sync completes within timeout`.

4.  **Reconnect After Reset:**
    -   **Setup:** Client disconnects mid-sync, reconnects.
    -   **Expectation:** Sync resumes without duplication or timeout loops.
    -   **Property:** `reconnect -> completes`.

## 12. Device Revocation & Delegation Simulation

### Invariants
- **Delegate chain integrity:** Delegates only valid if authorized by root key.
- **Revocation dominance:** Any revoke dominates prior delegate.

### Scenarios
1.  **Delegation Churn:**
    -   **Setup:** Rapidly add/revoke delegate keys while actions are emitted.
    -   **Expectation:** Only actions signed by active delegates are accepted.
    -   **Property:** `accepted -> signer active at time`.

2.  **Revocation Replay:**
    -   **Setup:** Replays of revoked delegate signatures after revocation.
    -   **Expectation:** Rejected.
    -   **Property:** `revoked => reject`.

3.  **Revocation Distribution Lag:**
    -   **Setup:** Member revoked while offline; reconnects later.
    -   **Expectation:** Revoked member does not receive new epoch grants.
    -   **Property:** `revoked -> no_grant`.

## 13. Query/Indexing & Planner Simulation (Tasks 49-54)

### Invariants
- **Contract table correctness:** Contract tables reflect latest derived state per subject.
- **Overlay privacy:** Private fields never appear in query results without access.
- **Planner consistency:** Row store returns strong reads; column store may lag but converges.
- **Index gating:** Private-field indexes are not used without overlay access.

### Scenarios
1.  **Lazy Build + Replay:**
    -   **Setup:** Query a contract table that does not exist.
    -   **Expectation:** Table builds from log; rows match latest state.
    -   **Property:** `row_count == subjects_with_contract`.

2.  **Incremental Update Under Sync:**
    -   **Setup:** Run sync while querying; new assertions arrive.
    -   **Expectation:** Row store updates transactionally; column store catches up eventually.
    -   **Property:** `row_store == latest`, `column_store eventually == latest`.

3.  **Overlay Redaction in Queries:**
    -   **Setup:** Query as non-authorized user.
    -   **Expectation:** Private fields masked; public fields intact.
    -   **Property:** `private_fields -> redacted`.

4.  **Planner Routing:**
    -   **Setup:** Execute point lookup vs group-by query.
    -   **Expectation:** Point lookup uses row store; group-by uses column store.
    -   **Property:** `planner(path) == expected`.

5.  **Corrupt Column Store:**
    -   **Setup:** Corrupt or delete column store segments.
    -   **Expectation:** Rebuild from assertions; queries recover.
    -   **Property:** `rebuild -> consistent`.

## 14. Performance & Load Simulation (Tasks 52-53)

### Invariants
- **Deterministic benchmarks:** Same seed yields comparable timings and outputs.
- **Resource ceilings:** Memory and disk remain within agreed thresholds.

### Scenarios
1.  **100M Row Benchmark:**
    -   **Setup:** Generate benchmark dataset; run representative queries.
    -   **Expectation:** Meets agreed latency/throughput targets.
    -   **Property:** `latency <= target`, `rows_scanned` reported.

2.  **Mixed Workload:**
    -   **Setup:** Concurrent sync + queries + writes.
    -   **Expectation:** No pathological slowdowns or OOM.
    -   **Property:** `p99_latency` within budget.

## 15. Negative/Adversarial Network Scenarios

### Invariants
- **Byzantine tolerance:** Corrupted or malformed objects never crash the system.
- **Validation integrity:** Reject malformed CBOR, invalid signatures, and invalid schema types.

### Scenarios
1.  **Malformed Object Flood:**
    -   **Setup:** Random bytes injected as objects during sync.
    -   **Expectation:** All rejected; no crashes.
    -   **Property:** `reject && continue`.

2.  **Schema/Contract Swap:**
    -   **Setup:** Mismatched schema/contract IDs sent with assertions.
    -   **Expectation:** Pending or rejected.
    -   **Property:** `contract mismatch -> reject or pending`.

3.  **Replay/Spam Flood:**
    -   **Setup:** Duplicate object IDs, replay old objects, or spam ads.
    -   **Expectation:** De-dup, rate-limit, or ignore without starvation.
    -   **Property:** `no_double_apply`.

4.  **Forged Token / Ad:**
    -   **Setup:** Token or ad signed by invalid key.
    -   **Expectation:** Reject.
    -   **Property:** `verify == false -> reject`.

## 16. Coverage Map (Tasks 48-63)

- **Task 48 (Ops tooling):** Section 1.
- **Task 49 (Contract tables):** Section 13.
- **Task 50 (Dharma web):** UI tests (out of SATS scope).
- **Task 51 (Group-by/aggregates):** Sections 13–14.
- **Task 52 (Bench tool):** Section 14.
- **Task 53 (Performance optimization):** Section 14 (perf baselines).
- **Task 54 (Dual store + planner):** Section 13.
- **Task 55 (Build output dir):** Build/CI checks (out of SATS scope).
- **Task 56 (Rename dharma):** Build/packaging checks (out of SATS scope).
- **Task 57 (Relay harness + IAM visibility):** Sections 9–11.
- **Task 58 (Identity):** Section 2.
- **Task 59 (Domains + directory integration):** Section 3 + Section 8.
- **Task 60 (Ownership/sharing/transfer + Fabric enforcement):** Sections 4 + 8 + 9.
- **Task 61 (Permission summaries/fast reject):** Section 7.
- **Task 62 (Key hierarchy/rotation/revocation):** Sections 5 + 12.
- **Task 63 (Freeze/compromise):** Section 6.
