# DHARMA Codebase Analysis

This analysis is based on the repository state under `/Users/julienmarie/Code/dharma`, including the top-level docs, task roadmap, and the Rust implementation in `dharma-core`, `dharma-cli`, and `dharma-runtime`. The project is explicitly "under extremely heavy development", so this document focuses on what exists today, where the gaps are, and how the design intent maps (or fails to map) to the current code.

---

## 1) Scope of the Project

DHARMA is aiming to be a local-first, peer-to-peer "truth kernel" for signed, typed, append-only assertions about real-world commitments. The core promises are:

- No central server required.
- Deterministic replay from immutable logs.
- Cryptographic auditability (signatures, content addressing).
- Extensible meaning through schema + contract code (DHL -> schema + wasm).
- Human-scale workflows (tasks, approvals, invoices, identity, etc).

The repository contains:

- **Protocol and product vision docs**: `README.md`, `protocol.md`, `docs/overview.md`, `docs/language.md`, `docs/workspace.md`, `docs/test-philosophy.md`, and domain explainers in `docs/`.
- **A runnable kernel and CLI**: `dharma-core` (kernel), `dharma-cli` (CLI + REPL), `dharma-runtime` (server).
- **A nascent query engine**: DHARMA-Q (spec in `dharma-q.md`, partial implementation in `dharma-core/src/dharmaq`).
- **A roadmap and task tracking**: `todo.md` and detailed design tasks in `tasks/`.
- **Contracts (DHL)**: a standard library of example contracts under `contracts/std/`.

The scope is unusually broad: it includes protocol design, storage, encryption, contract execution, sync, query/indexing, and even a future GUI (DHARMA Workspace).

---

## 2) How It Works (Current Implementation)

This section is a map of the current code paths and data flows. It blends design intent (docs) with actual mechanics (code).

### 2.1 Core Data Model

- **Subject**: 32-byte random ID (no global registry required). `dharma-core/src/types.rs`.
- **Assertion**: Signed, typed, append-only statement with:
  - `header` (protocol version, subject, type, author, sequence, dependencies, schema/contract refs, etc).
  - `body` (typed CBOR).
  - `sig` (Ed25519).
  - `ver` field for "lens" / data versioning.
  - `meta` field used for overlays.
  - Code: `dharma-core/src/assertion.rs`.
- **Envelope**: Encrypted wrapper around assertion or artifact. Uses:
  - ChaCha20-Poly1305 for AEAD.
  - Canonical CBOR for AAD.
  - Content addressing (SHA-256).
  - Code: `dharma-core/src/envelope.rs`, `dharma-core/src/crypto.rs`.

### 2.2 Storage Layout (Kernel)

The kernel is a file-based store. Objects are content-addressed and stored independently of subjects.

- Global object store: `data/objects/<envelope_id>.obj`
- Per-subject logs:
  - `data/subjects/<subject>/assertions/log.bin`
  - `data/subjects/<subject>/overlays/log.bin`
  - `data/subjects/<subject>/snapshots/` (state snapshots)
  - `data/subjects/<subject>/indexes/`
- Global index: `data/indexes/global.idx`

Implementation lives in:
- `dharma-core/src/store.rs`
- `dharma-core/src/store/state.rs`
- `dharma-core/src/store/index.rs`

Notable behaviors:

- The object store is authoritative; subject logs can be rebuilt from objects (via `rebuild_subject_views`).
- Logs are append-only, binary-encoded (no checksums yet).
- Snapshots are per subject and per `ver` (lens).
- Frontier index tracks "tip" assertions to optimize sync.

### 2.3 Validation and Ingest Pipeline

The ingest path is the heart of the kernel. It does the following:

1. **Decode envelope**, decrypt assertion (if needed).
2. **Canonical CBOR check** (`cbor::ensure_canonical`).
3. **Structural validation**:
   - protocol version matches.
   - sequence and prev are consistent.
   - signature is valid.
4. **Schema validation** of the assertion body.
5. **Contract validation** through wasm.
6. **Append to subject log**, update frontier index.

Key code:

- Structural validation and ordering: `dharma-core/src/validation.rs`
- Ingest logic: `dharma-core/src/net/ingest.rs`
- Contract execution: `dharma-core/src/contract.rs`

Pending assertions are tracked if dependencies or referenced artifacts are missing.

### 2.4 Contracts, Schemas, and CQRS State

DHARMA treats domain logic as code/data, not kernel behavior:

- **DHL** (Literate DHARMA Domain Law) is embedded in Markdown.
  - Parsed by `dharma-cli/src/pdl/parser.rs`.
  - AST + type-checking exists but is still evolving.
- **Schema artifacts**: Serialized CQRS schemas (CBOR).
  - `dharma-core/src/pdl/schema.rs` (CQRS schema, type specs, layout).
  - `dharma-core/src/schema.rs` (generic schema validation for assertion bodies).
- **Contracts**: compiled to wasm.
  - Validation returns `Accept`, `Reject`, `Pending`.
  - Reduction runs against accepted actions to mutate state.

State is computed via a fixed memory layout:

- `STATE_BASE = 0x0000`
- `OVERLAY_BASE = 0x1000` (private overlay data)
- `ARGS_BASE = 0x2000`
- `CONTEXT_BASE = 0x3000`

Code:

- `dharma-core/src/runtime/vm.rs` (low-level wasm execution)
- `dharma-core/src/runtime/cqrs.rs` (state loading, decoding, encoding, replay)

### 2.5 Overlays (Private Data and Policy)

Overlays allow private, sidecar assertions tied to a base action. They are stored and replicated separately:

- An overlay assertion is identified by `meta.overlay = true` (`dharma-core/src/assertion.rs`).
- Overlay assertions reference exactly one base assertion in `refs`.
- Overlay logs are stored under `data/subjects/<subject>/overlays/`.
- Sync can be gated by overlay policies (`overlays.policy`).

Code paths:

- `dharma-core/src/net/ingest.rs` (overlay handling and validation)
- `dharma-core/src/net/policy.rs` (policy loading and checks)
- `dharma-core/src/store/state.rs` (overlay log storage)

### 2.6 Sync Protocol

Sync is a peer-to-peer, Noise-encrypted exchange:

- Noise_XX handshake implemented manually.
- Sync messages: `Hello`, `Inv`, `Get`, `Obj`, `Err` (`dharma-core/src/sync.rs`).
- A frontier index is exchanged to identify missing objects.
- Missing objects are requested by `Get`.
- Overlay access is filtered by peer policy and subscriptions.

Implementation:

- Noise handshake: `dharma-core/src/net/noise.rs`, `dharma-core/src/net/handshake.rs`
- Sync loop: `dharma-core/src/net/sync.rs`
- Frame codec: `dharma-core/src/net/codec.rs`

The current sync loop is blocking and synchronous, designed for simplicity over throughput.

### 2.7 Identity and Key Management

Identity is a subject with its own assertion history.

- Encrypted keystore using Argon2: `dharma-core/src/keystore.rs`, `dharma-core/src/identity_store.rs`.
- Delegation rules and revocations: `dharma-core/src/identity.rs`.
- Device key delegation and root keys are partially implemented.

### 2.8 CLI and REPL

The CLI is the primary user interface today:

- `dh` binary (CLI + REPL).
- Commands: identity init/export, connect, compile, action, write, repl, serve.
- REPL supports subject navigation, state inspection, actions, overlays, and sync tools.

Code:

- CLI entry: `dharma-cli/src/lib.rs`
- REPL: `dharma-cli/src/repl/`
- Commands: `dharma-cli/src/cmd/`

### 2.9 DHARMA-Q (Query Engine)

DHARMA-Q is designed as a disposable, columnar projection store.

- Spec: `dharma-q.md`
- Implementation: `dharma-core/src/dharmaq/`
- Current implementation includes:
  - Columnar partitions
  - WAL for hot partitions
  - Bitset filtering and simple text search
  - A query plan with `where`, `search`, `take`

It is feature-flagged in `dharma-core` and used by the CLI parser (`dharma-cli/src/dharmaq.rs`).

---

## 3) Strengths (Code and Architecture)

1. **Deterministic, canonical encoding**  
   Canonical CBOR is enforced on decode and encode, which is crucial for reproducible signatures and deterministic replay. `dharma-core/src/cbor.rs`.

2. **Clear separation between kernel and higher layers**  
   `dharma-core` is small and focused, while CLI and compiler live in `dharma-cli`. This is consistent with the "minimal kernel" principle in the docs.

3. **Content addressing and immutable logs**  
   Envelope IDs and assertion IDs are SHA-256 hashes. This yields integrity guarantees and easy deduplication.

4. **Explicit validation pipeline**  
   The ingest path is strict: canonical CBOR, signature, schema, contract. This matches the protocol invariants in the README and `docs/test-philosophy.md`.

5. **Deterministic ordering of DAG assertions**  
   Assertions are ordered via a dependency-aware topological sort. This is a solid foundation for eventual consistency.

6. **Overlay mechanism for privacy**  
   The overlay feature provides a real privacy primitive beyond "everything encrypted". It allows sidecar private data with explicit replication policy.

7. **Practical CQRS runtime**  
   The CQRS memory layout is simple, fixed, and designed for wasm execution. This makes the runtime predictable and small.

8. **Thoughtful roadmap and documentation**  
   The docs and `tasks/` are unusually explicit. They highlight intended behavior, missing pieces, and invariants. That reduces ambiguity for contributors.

---

## 4) Weaknesses (Current Code and Maturity)

These are based on actual code and the roadmap. Many are expected given heavy development, but they are real risks.

1. **Tests are sparse relative to the ambition**  
   The test philosophy is strong, but the codebase mostly contains unit tests embedded in modules. There is no visible integration test suite or conformance vectors yet (`tests/` is empty).

2. **Blocking network and sync implementation**  
   Sync uses blocking IO and manual retry. It is fine for early prototypes but will be fragile under latency, large payloads, or many peers.

3. **No robust corruption detection in logs**  
   The binary log format (`log.bin`) has no CRC or checksum. A single corrupted byte can break parsing.

4. **Storage scalability limits**  
   One file per object plus frequent directory scans will become slow at scale. There is no GC, compaction, or bloom filtering.

5. **Crypto story is incomplete**  
   HPKE and capability tokens are in the spec, but not yet implemented. `Hello` includes `hpke_pk` but the current handshake and session are noise-based only.

6. **DHL implementation is partial**  
   The parser and compiler exist, but the DHL feature set is not yet aligned with the full `docs/language.md` spec. Many language features in docs are not implemented yet.

7. **Contract runtime has no resource limits**  
   wasm is executed without explicit gas or memory bounds in the kernel. A contract could cause denial-of-service or unpredictable latency if not constrained.

8. **Strict correctness depends on correct schema/contract artifacts**  
   If a peer lacks the schema or contract artifact, assertions become pending. This is correct per spec, but operationally it will be a common failure mode.

9. **Repo hygiene and build artifacts**  
   `target/` is present in the repo. This can bloat the working tree and obscure real changes.

---

## 5) Pitfalls and Edge Cases

These are areas that are likely to surprise new contributors or cause subtle bugs:

1. **Data versioning (`ver`) and lenses**  
   The same subject can have multiple versions of schema/contract. If a peer defaults to the wrong `ver`, it can accept or reject assertions incorrectly.

2. **Merge assertions (`core.merge`) and concurrency**  
   Forked histories are allowed, but contract logic must explicitly reconcile them. The system is correct only if merge rules are consistently enforced.

3. **Overlay semantics**  
   Overlays must reference exactly one base assertion. They are stored separately and gated by policy. Missing base assertions will cause overlays to pend.

4. **Canonical CBOR is non-negotiable**  
   If any client produces non-canonical CBOR, signatures will fail and assertions will be rejected. This is a correctness landmine for external integrations.

5. **Identity delegation rules are partial**  
   Delegation currently uses a simple scope matching (e.g. "all", "chat"). It is not yet a full capability system.

6. **State snapshots vs replay**  
   Snapshots are used to avoid full replay, but the snapshot format is not versioned beyond `ver`. If layout changes, snapshots can become invalid.

7. **Query engine results are derived, not authoritative**  
   DHARMA-Q is disposable. If clients treat its results as authoritative truth without linking back to assertions, audit guarantees erode.

---

## 6) Value and Shortcomings

### Value

- **Trustable collaboration substrate**: It formalizes commitments as signed, typed assertions.
- **Local-first, no server dependency**: Works offline and does not require centralized infra.
- **Extensible semantics**: Domain logic can evolve via schema/contract artifacts rather than kernel changes.
- **Auditability by construction**: History is immutable and content-addressed.

### Shortcomings (today)

- **Not production ready**: Many critical components are still in roadmap form (capabilities, registry, robust sync).
- **Operational ergonomics are immature**: Missing tooling for diagnostics, migrations, and large-scale replication.
- **Spec vs implementation mismatch**: The docs are ahead of the code in several areas (DHL, DHARMA-Q, Fabric).

---

## 7) Improvements Needed (Prioritized)

This is a practical, engineering-oriented list rather than a full roadmap rewrite.

### Top Priority (Stability and Correctness)

1. **Conformance and regression tests**  
   Implement the conformance vectors described in `docs/test-philosophy.md` and `tasks/12_testing_conformance.md`. This is essential to prevent protocol drift.

2. **Corruption detection for logs**  
   Add checksums (CRC or hash) to `log.bin` entries and snapshot records. Detect and fail safely.

3. **Deterministic resource limits for contracts**  
   Introduce wasm execution limits (fuel, memory). This is necessary for safety and predictability.

4. **Schema/contract artifact availability**  
   Implement registry and artifact fetch so peers can retrieve missing schema/contract artifacts and resolve pending assertions.

### High Priority (Security and Access Control)

5. **Capability tokens**  
   Implement and enforce capabilities at the network and contract layers (see `tasks/22_capability_tokens.md`).

6. **HPKE integration**  
   Move beyond placeholder `hpke_pk` in hello and integrate real key wrapping for subject keys.

7. **Identity and delegation hardening**  
   Expand delegation scopes and implement device key revocation workflows (Task 32).

### Medium Priority (Performance and Scale)

8. **Async or streaming sync**  
   Replace blocking sync with async IO and support large object streaming with backpressure.

9. **Store compaction and GC**  
   Provide object GC, snapshot pruning, and index compaction to avoid unbounded growth.

10. **DHARMA-Q robustness**  
   Harden WAL recovery, add partition pruning and more query operators, ensure determinism for search ranking.

### Medium Priority (Developer Experience)

11. **DHL feature parity**  
   Align `dharma-cli` parser/codegen with the language spec (`docs/language.md`) and add CEL expression coverage.

12. **Better diagnostics and "why" tooling**  
   Expand REPL tooling for pending/rejected explanations, missing dependency discovery, and provenance inspection.

13. **Remove build artifacts from repo**  
   Keep `target/` out of version control to improve review clarity and reduce clutter.

---

## 8) Overall Assessment

DHARMA is a bold attempt to build a "truth kernel" for collaborative work. The project has a strong conceptual foundation and a clear architectural direction. The kernel already implements core primitives (assertions, encryption, storage, validation, sync) in a clean and auditable way.

However, the current state is not yet fully aligned with the expansive specification. Many of the most important operational and security features (capabilities, registry, robust sync, conformance tests) are still incomplete. The existing code is enough to validate the kernel philosophy and explore workflows, but it is not yet hardened for production or adversarial environments.

The roadmap in `todo.md` and `tasks/` matches the major missing pieces. The biggest risks are not conceptual; they are practical: test coverage, protocol conformance, and operational hardening.

If those are addressed, the architecture is strong enough to support the long-term ambition described in the docs.

