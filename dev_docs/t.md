DHARMA v1

Peer Assertions for Commitments & Truth

A local-first, peer-to-peer protocol for creating, sharing, validating, and auditing signed typed assertions about work — without servers, without central authority, and without operational admin.

⸻

Abstract

DHARMA is a protocol for shared truth: a way for independent parties to coordinate, commit, and audit work over time using signed, typed, append-only assertions replicated peer-to-peer. DHARMA treats collaboration as a cryptographic ledger of human commitments rather than a mutable database owned by vendors.

DHARMA is designed to be universal: easy enough to manage home finances, and strong enough for high-assurance environments (healthcare, nuclear, financial, military, government). This is achieved by freezing a small “kernel” of deterministic rules and expressing nearly everything else as data: schemas, contracts, automation, permissions, and domain logic.

⸻

1. Problem & Motivation

Modern work is fragmented:
	•	Chat systems optimize for conversation, not commitment.
	•	Task systems optimize for management, not truth.
	•	Knowledge bases optimize for storage, not provenance.
	•	Financial systems optimize for centralized control, not shared trust.

Consequences:
	•	Decisions disappear or become disputable.
	•	Tasks rot, duplicate, or become invisible.
	•	Audit trails are reconstructed after the fact.
	•	Trust depends on vendors and admins rather than proofs.

DHARMA addresses the root problem:
There is no durable, verifiable, low-friction way for peers to agree on what is true, pending, and committed — without central infrastructure.

⸻

2. One Sentence Vision

A peer-to-peer protocol for creating, sharing, validating, and auditing signed typed assertions about shared reality — offline-first, encrypted by default, and deterministically replayable.

⸻

3. Design Goals

Functional goals
	•	Record decisions, approvals, responsibilities, obligations, and evidence as durable truth.
	•	Allow deterministic reconstruction of current state by replay.
	•	Support cross-organization collaboration without vendor lock-in.
	•	Support assisted workflows (automation proposes; peers commit).

Non-functional goals
	•	Local-first: works offline indefinitely.
	•	Crypto-enforced trust: no privileged server role.
	•	Deterministic convergence: same data + same code ⇒ same state.
	•	Auditability by construction: the ledger is the audit trail.
	•	Minimal kernel: small enough to audit/certify; stable enough to freeze.

⸻

4. The “Grandmother + CIA” Principle

DHARMA must serve:
	•	Home: budgeting, receipts, chores, family planning (zero ceremony).
	•	High assurance: classification, separation-of-duties, formal change control, long-lived evidence, air-gapped workflows.

This is achieved through:
	•	a universal kernel (frozen, boring),
	•	a standard library of domain packages,
	•	multiple UI “shells”/profiles with different defaults,
	•	explicit trust governance (who can publish what rules).

⸻

5. Core Laws (Protocol Invariants)

Law 1 — No hidden state
All meaningful state MUST be reconstructible by replaying accepted assertions the peer possesses and can decrypt. Caches MUST be disposable.

Law 2 — No silent mutation
Derived state MUST NOT change unless a new assertion becomes accepted (or a pending assertion becomes accepted after prerequisites arrive).

Law 3 — Compute never commits
Only signed assertions can change truth. Automation may propose, but proposals must be represented as assertions that require endorsement to take effect.

Law 4 — Trust is cryptographic
Authorization MUST reduce to signature verification + deterministic rules over prior assertions.

Law 5 — Local-first
Peers MUST be able to create, validate, and derive state offline.

Law 6 — Deterministic convergence
For a given Subject, any two peers with the same decryptable assertion set and the same governing code MUST derive identical state.

Law 7 — Typed meaning, free text allowed
Assertions MUST have explicit types; meaning-bearing fields MUST be typed. Free text is allowed but MUST NOT be required for interpreting state transitions.

⸻

6. Architecture Overview

DHARMA is a forest of sovereign ledgers (Subjects). There is no global “world computer.”

6.1 Layers
	•	Kernel (DHARMA Runtime): verify, decrypt, validate schema, run contract, store objects, sync.
	•	Semantics (Schemas + Contracts): typed structure + deterministic laws.
	•	Automation (Reactors): event handlers that emit signed follow-up assertions.
	•	Distribution (Registries): publish and verify domain packages (schemas/contracts/reactors/docs).
	•	Discovery & Naming (optional): Atlas-like naming + rendezvous/relays for global reach.
	•	UX shells: Weaver-like client(s) rendering subjects as notes, ledgers, workflows, etc.

6.2 Minimal Runtime Principle

The runtime is deliberately small:
	•	no heavy databases,
	•	no “business logic” baked into kernel,
	•	no mutable permission tables,
	•	no required online services.

⸻

7. Terminology
	•	Peer: a participant identified by a public key.
	•	Identity key: peer signing key (Ed25519).
	•	Subject: a 32-byte identifier for a “thing” assertions are about (invoice, contract, task list, identity).
	•	Assertion: signed, typed, append-only statement.
	•	Envelope: encrypted, content-addressed container used for replication.
	•	Artifact: large blobs referenced by assertions (docs, images, wasm binaries, DHL sources).
	•	Schema: typed description of allowed assertion bodies.
	•	Contract: deterministic program that validates assertions and derives state (CQRS aggregate rules).
	•	Reactor: deterministic automation program that proposes/emits follow-up assertions.
	•	Epoch key: symmetric subject encryption key for a period; rotated for revocation/compartment changes.

⸻

8. Cryptographic Suite (DHARMA Suite 1)

DHARMA is crypto-agile via suite_id. Suite 1 is mandatory for v1 compliance.
	•	Hash: SHA-256
	•	Signatures: Ed25519
	•	Symmetric encryption: ChaCha20-Poly1305 (96-bit nonce)
	•	Key exchange / wrapping: HPKE (X25519 + HKDF-SHA256 + ChaCha20-Poly1305)
(You can ship without HPKE initially, but the protocol target includes it.)

Transport encryption is optional (payloads are E2E encrypted), but recommended to reduce metadata leakage.

⸻

9. Identifiers & Content Addressing

9.1 Object ID

Every replicated object is content-addressed:
	•	object_id = SHA-256(object_bytes) (32 bytes)

Guarantees:
	•	deduplication
	•	integrity/tamper-evidence
	•	immutable history

9.2 Subject ID

A Subject ID is 32 random bytes generated by the creator. There is no registry requirement for existence; a subject “exists” if assertions referencing it exist.

⸻

10. Canonical Encoding

All structured data MUST be encoded as Canonical CBOR (deterministic encoding).
Signature inputs MUST be canonical CBOR. Contract inputs MUST be canonical CBOR.

(Implementations should enforce canonical constraints strictly; map-key ordering alone is not sufficient long term.)

⸻

11. The Assertion (Atomic Semantic Unit)

11.1 Plaintext structure

An assertion is a CBOR map:
	•	h: header (normative fields)
	•	b: body (typed, schema-validated)
	•	sig: signature

Header MUST include
	•	v: protocol version (1)
	•	sub: subject id (32 bytes)
	•	typ: type identifier (namespaced string)
	•	auth: author public key (32 bytes)
	•	seq: author sequence number within subject
	•	prev: prior assertion object_id in the author chain (or null if seq=1)
	•	refs: array of referenced assertion object_ids (dependencies)
	•	ts: optional authored timestamp claim (UI context; not trusted for ordering)
	•	schema: schema artifact object_id
	•	contract: contract artifact object_id
	•	ver: data version tag (see Versioning section; required once enabled)

Header MAY include
	•	note: free-text intent
	•	meta: additional typed metadata (must be schema-defined if meaningful)

Signature
sig = Ed25519.Sign(author_sk, CanonicalCBOR({h,b}))

11.2 Type naming

typ is namespaced:
	•	reserved: core.* (protocol types)
	•	domain: std.invoice.issue, com.ph.cmdv.invoice.po_link, etc.

⸻

12. Encryption Model

12.1 Subject keys and epochs

Each subject uses symmetric keys by epoch:
	•	epoch key: 32 bytes K_epoch
	•	key id: kid = SHA-256(K_epoch) (32 bytes)

Assertions and artifacts are encrypted under a kid.

12.2 Assertion envelope (replicated object)

Envelope fields:
	•	v: 1
	•	suite: 1
	•	kid: 32 bytes
	•	nonce: 12 bytes
	•	ct: ciphertext

AAD is canonical CBOR of {v,suite,kid,nonce}.

ct = AEAD_Encrypt(K_epoch, nonce, plaintext_assertion_bytes, aad)

12.3 Access control = key distribution

If you can obtain K_epoch, you can decrypt epoch objects. Revocation is achieved by rotating to a new epoch key and not wrapping it for revoked peers.

⸻

13. Storage Model (Filesystem-Only, Append-Only)

Each peer maintains an append-only object store:

/data
  /objects/
     <object_id>.obj        (raw envelope bytes for assertions/artifacts/packages)
  /subjects/
     <subject_id>/
        /assertions/         (optional convenience; can be derived from object store)
        /snapshots/          (versioned snapshots)
        /indexes/            (derived, disposable)

Rule: object bytes are immutable. Indexes/snapshots are derived and can be regenerated.

13.1 Snapshots (derived but useful)

Snapshots speed replay and replication:
	•	snapshot file contains:
	•	subject_id
	•	contract_id
	•	schema_id
	•	data version
	•	last_accepted_object_id (tip)
	•	state blob

Snapshots MUST be disposable and reproducible.

⸻

14. Replication (DHARMA-SYNC/1)

Transport-agnostic message frames over any channel (TCP, QUIC, WebRTC, USB drop, relays).

Frames are canonical CBOR maps:
	•	t: type
	•	p: payload

Message types:
	•	hello: capabilities, peer identity keys, supported suites
	•	inv: inventory summary (subject frontiers or object sets)
	•	get: request missing object IDs
	•	obj: send object bytes
	•	err: non-fatal errors

14.1 Frontier concept

For each subject, maintain a set of “tips” (frontier):
	•	an assertion is a tip if no other known assertion references it in prev or refs.

14.2 Sync behavior
	1.	exchange hello
	2.	exchange inv (subject + frontier tips)
	3.	request missing tips
	4.	recursively request missing dependencies (prev/refs)
	5.	ingest/validate/store objects
	6.	repeat until convergence or policy limits

14.3 Partial replication

Peers may:
	•	sync only some subjects
	•	cap ancestry depth
	•	refuse to serve some objects
	•	prune old objects locally

Missing objects MUST not produce “guessed truth.” They only produce PENDING states.

⸻

15. Deterministic Validation Pipeline

For a subject:

15.1 Candidate set

Decryptable envelopes whose plaintext header sub matches the subject.

15.2 Structural validation
	•	parse canonical CBOR
	•	verify header fields
	•	verify signature
	•	verify author chain (seq, prev) when possible
	•	if prev missing: mark PENDING (not rejected)

15.3 Deterministic replay ordering

Build dependency graph using:
	•	deps(A) = refs ∪ {prev} intersecting available assertions

Topologically sort with deterministic tie-break:
	•	Kahn’s algorithm
	•	tie-break by lexicographic object_id

15.4 Schema validation

Validate b against schema for typ.

Fail schema ⇒ REJECT.

15.5 Contract validation + state reduction (CQRS)

Run contract validate() in deterministic VM using:
	•	current derived state (for that version)
	•	accepted history so far
	•	referenced assertions available

Outcomes:
	•	ACCEPT
	•	REJECT
	•	PENDING (missing prerequisites, missing code, missing cross-subject refs)

Accepted assertions are reduced via contract reduce() to produce derived state.

⸻

16. Contracts (Deterministic CQRS Engine)

DHARMA’s semantics layer is a decentralized CQRS / event-sourced engine.
	•	Subject = aggregate root
	•	Assertions = events (or command-intent assertions treated as events once accepted)
	•	Contract.validate = guardrails (“is this allowed now?”)
	•	Contract.reduce = projection (“what is the new state?”)

Contracts are:
	•	deterministic
	•	pure (no time/random/network/filesystem)
	•	replayable

16.1 Contract format: Wasm Contract v1

A contract artifact is a wasm32 module exporting:
	•	validate(assertion_bytes, context_bytes) -> result_bytes
	•	reduce(accepted_assertions_bytes) -> state_bytes

Inputs/outputs are canonical CBOR bytes.

Host MUST enforce:
	•	instruction limits / fuel
	•	memory limits
	•	no imports (or a tightly controlled import surface)

⸻

17. Literate Domain Law (DHL)

DHARMA introduces DHL: Markdown-based “literate law” documents where business-readable prose and machine-compilable blocks live together.
	•	prose is displayed by clients (human contract)
	•	code blocks define:
	•	schemas (types/fields/constraints)
	•	contracts (actions/validation/state transitions)
	•	reactors (event handlers)

The compiler produces artifacts:
	•	schema
	•	contract wasm
	•	reactor wasm
	•	source artifact (original DHL text)

The runtime never compiles DHL; it only executes artifacts.

⸻

18. Automation: Reactors (Event Handlers)

Reactors are deterministic programs that:
	•	observe accepted assertions (events)
	•	emit proposed follow-up assertions (commands/events)
	•	must be signed by an explicit peer identity (bot key)

This makes automation auditable and non-magical:
	•	“why did this happen?” ⇒ because reactor X signed assertion Y after seeing event Z.

Reactors MUST not bypass contract rules. Their outputs are validated like any other assertion.

⸻

19. Data-Driven Versioning (Critical)

Key rule: code versions can coexist; data carries the version and is routed to the correct interpreter.
	•	The assertion header includes ver (a version tag).
	•	A node may have multiple contract/schema versions installed for a given package.
	•	Derived state is versioned:
	•	you can derive state as interpreted by v1, v2, v2-beta, etc.
	•	A/B testing is achieved by emitting assertions with different ver.

If a node receives an assertion whose version it cannot interpret:
	•	store object
	•	mark UNPROCESSABLE_VERSION
	•	do not guess or auto-downgrade unless explicitly configured and safe.

⸻

20. Inheritance + Field Ownership + Privacy Overlays

To be universal, DHARMA must support:
	•	shared public standards (std.invoice)
	•	organization-specific extensions (com.ph.cmdv.invoice)
	•	private internal fields that should not leak to third parties

20.1 Ownership model

Fields are owned by namespaces:
	•	std.invoice.lineitems[] are public base fields
	•	com.ph.cmdv.invoice.po_id is private extension field

20.2 Overlay requirement

To allow sharing only the base without breaking signatures, extensions MUST use an overlay model:
	•	Base chain: complete, valid, shareable standalone
	•	Overlay chain: separate log, encrypted, references base objects

Decision: overlays are a sidecar log (separate directory / chain).
This preserves strict validity for public peers who never see overlay objects.

20.3 Merge behavior

Authorized peers merge base + overlay into a unified virtual state space for contracts/UI, while outsiders see only base.

20.4 “Public vs private sync”

Sync inventory and serving MUST be ACL-aware:
	•	if peer lacks overlay keys, do not advertise overlay objects
	•	base is still shareable if allowed by base subject membership/policy

⸻

21. Security & Access Control (Universal IAM)

High-assurance universality requires two complementary models:

21.1 Capability-based authorization (recommended core)

Assertions can carry or reference capabilities:
	•	“Key X may perform action A on subject S until time T”
	•	delegable with scope limitations
	•	revocable by epoch rotation + contract rules

21.2 Role-based access (RBAC) as a derived view

Roles are assertions:
	•	role assignments
	•	role revocations
	•	role scopes (per subject/package/namespace)
	•	separation of duties and quorum requirements

The kernel enforces only signatures and deterministic execution. Authorization semantics live in contracts and standard libraries.

⸻

22. Universal Types (Safety-Critical Completeness)

To truly span home → nuclear, DHARMA needs standard typed primitives:
	•	Duration / interval / recurrence
	•	Units & dimensional analysis (mass, volume, temperature, concentration, dosage)
	•	Geo (coordinate systems, geofences, facilities, jurisdictions)
	•	Identity assurance (attestation fields for hardware-backed keys, clearance, etc.)
	•	Money: fixed-point amounts, currency codes, exchange-rate assertions

These live in the std.* library and are referenced by schemas/contracts.

⸻

23. Search, Vector, and Derived Indexes (Not Truth)

DHARMA treats indexes as derived, disposable artifacts, never the source of truth:
	•	full-text index
	•	vector index
	•	graph/provenance index

Indexes may be:
	•	local
	•	relay-provided
	•	enterprise-hosted

But results must be explainable:
	•	“show me why this result matches”
	•	“show the assertion chain that produced this state”

⸻

24. Currency / Stablecoin / Ledger Layer (Optional but Universal)

DHARMA can support “money” without forcing a single coin:
	•	Standard double-entry ledger subjects
	•	Receipts and settlement assertions
	•	Optional integration with external rails (banks, Stripe, on-chain stablecoins)

If you include a native stablecoin later, it should remain:
	•	a package/contract set
	•	not a kernel feature

⸻

25. Registries, Packages, Publishing, Verification

To distribute “laws” safely, DHARMA treats code as content-addressed artifacts published via registry subjects.

25.1 Registry scopes
	•	Global std registry: public packages managed by a conservative governance process
	•	Company registry: encrypted internal packages; only employees/authorized nodes can access
	•	Personal registry: private packages for personal workflows/devices

25.2 Publish flow

Publishing produces:
	•	DHL source artifact
	•	schema artifact(s)
	•	contract wasm artifact(s)
	•	reactor wasm artifact(s)
	•	test artifact(s) (optional but strongly recommended)

Then appends a registry assertion (e.g., sys.package.add) containing:
	•	package name
	•	mapping of supported data versions → artifact hashes
	•	dependencies (hash-pinned)
	•	authorized publisher identity
	•	signatures

25.3 Verification chain

A node trusts a package only if:
	•	the publisher key is authorized to publish for that registry scope
	•	the artifact hashes match downloaded content
	•	dependency hashes resolve and verify
	•	optional: tests pass (required in some profiles)

⸻

26. Testing & Certification Readiness

To be credible in nuclear/medical/finance/military contexts, DHARMA needs:
	•	conformance test vectors for canonical encoding, signature, envelope, sync, replay
	•	contract execution limits and deterministic VM behavior tests
	•	standard library compatibility tests (Liskov-style tests for extensions)
	•	reproducible builds (supply-chain integrity)
	•	“profiles” defining required constraints (e.g., High-Security profile requires:
	•	hardware-key attestations
	•	quorum for specific actions
	•	strict retention
	•	compartment semantics)

DHARMA’s minimal kernel is what makes this realistic: you certify the kernel once, then certify packages/profiles separately.

⸻

27. Conformance Requirements (What a v1 Node MUST Do)

A compliant node MUST:
	1.	operate offline indefinitely
	2.	store and replicate content-addressed objects
	3.	verify signatures deterministically
	4.	enforce canonical encoding for signature inputs
	5.	decrypt and validate assertions when keys exist
	6.	mark missing prerequisites as PENDING (never guess)
	7.	deterministically replay accepted assertions to derive state
	8.	converge with other peers given same decryptable object set + same contracts
	9.	maintain an explainable audit trail (replay chain)

⸻

28. Minimal End-to-End Flows

28.1 Create subject (offline)
	•	generate subject_id
	•	choose schema + contract artifacts
	•	create genesis assertion
	•	encrypt into envelope
	•	store locally

28.2 Invite peer
	•	wrap epoch key to recipient (HPKE)
	•	send key grant package (any transport)
	•	optionally record membership assertion in-subject

28.3 Make assertion
	•	derive state by replay
	•	build assertion with refs + prev
	•	sign, encrypt, store
	•	sync later

28.4 Automation
	•	reactor sees accepted event
	•	emits signed follow-up assertion
	•	follow-up validated like any other

⸻

29. Why This Can Be Easy and High-Assurance

Because the kernel is tiny and stable, and everything else is policy/data:
	•	Home UI hides complexity and provides “packs.”
	•	High-security environments enable stricter profiles (capabilities, compartments, quorum).
	•	The underlying truth model never changes: assertions + replay + proof.

The UX is just a lens. The truth is portable and vendor-independent.

⸻

30. The World It Enables

A future where:
	•	“Work” is not trapped in SaaS silos.
	•	Audit trails are automatic.
	•	Trust is mathematical, not administrative.
	•	UIs are disposable; history is permanent.
	•	Automation is powerful but accountable.
	•	Collaboration works offline and across orgs without a shared server.

DHARMA is not “a better productivity app.”
DHARMA is a new primitive: sovereign truth you can share.

DHARMA-FABRIC v1 

Deterministic Sharding + Soft-State Discovery + Targeted Execution

Goal

Make DHARMA “just run” with:
	•	p99-first latency for OLTP queries
	•	predictable scatter/gather for OLAP
	•	safe, capability-gated access
	•	location-agnostic UX (clients never choose servers)
	•	provable correctness (watermarks + provenance + optional receipts)

This spec applies to:
	•	object fetch / subject sync
	•	DHARMA-Q queries
	•	search (text/vector/geo)
	•	compute jobs

⸻

0. Definitions
	•	ShardMap: deterministic function mapping (table,key) or (subject_id) to a shard.
	•	ReplicaSet: set of providers responsible for a shard.
	•	Soft-state Ads: TTL advertisements of endpoints, health, load, watermark.
	•	Watermark: “how fresh” a replica is for a shard + lens.
	•	Capability Token: signed authorization for scope + ops + constraints.
	•	Fast Path: single-shard (fanout=1) point query / small scan.
	•	Wide Path: scatter/gather across many shards (fanout>1), reduce monoids.

⸻

1. Core principles 

1.1 Deterministic placement, soft-state liveness
	•	Placement and replica membership are hard-state within an org (config subject or Raft).
	•	Liveness/load/freshness are soft-state via ads (TTL).

1.2 OLTP defaults to single-shard

The planner MUST attempt to answer a request with one shard:
	•	by primary key lookup
	•	by time-partition + shard range narrowing
	•	by precomputed keyed dimensions

No distributed joins by default.

1.3 Distribute aggregates, not joins

Wide path SHOULD be:
	•	partitioned scans producing mergeable monoids
	•	reduce by associative merge

Distributed joins are allowed only:
	•	when explicitly requested
	•	or when a materialized view exists

1.4 Tail latency is a first-class constraint
	•	hedged requests
	•	strict budgets/timeouts
	•	bounded work per query
	•	backpressure when wide queries are too expensive

1.5 Provenance & watermarks

Every response includes:
	•	watermark (which shard state/tip it reflects)
	•	provenance pointers (oid / oids[]) for rows (configurable)
Optional: signed receipts.

⸻

2. Sharding model (hard-state)

2.1 Shard types

DHARMA-FABRIC supports sharding for:
	•	DHARMA-Q tables (projection data)
	•	object storage (optional, if you run vault clusters)
	•	event streams (optional)

v1 focuses on DHARMA-Q tables.

2.2 Partition keys

Each table declares a partition strategy:
	1.	Key-hash sharding

	•	shard = hash(key) mod N
	•	used for keyed lookups (invoice_id, order_id, sku)

	2.	Time partitioning

	•	partition = day/month bucket on ts
	•	within partition: either key-hash shards or single shard

	3.	Geo partitioning (optional v1)

	•	partition by cell_prefix for geo tables
	•	then key-hash inside

2.3 ShardMap definition object

Hard-state config stored in an Org ShardMap Subject:
org.<org>.shardmap

ShardMap entry:
	•	table: sym
	•	strategy: enum(hash, time+hash, geo+hash)
	•	key: column name for hashing (e.g., invoice_id)
	•	time_col: optional (ts)
	•	N: number of shards
	•	replication: R replicas per shard
	•	replica_sets: mapping shard_id → list(provider_ids)
	•	lens: supported lenses/data_ver (optional)
	•	policy: constraints (max rows, max scan)

Clients MUST be able to compute shard_id locally using this map.

⸻

3. Provider ads (soft-state)

Providers publish TTL advertisements:
	•	endpoints
	•	services offered
	•	shard coverage
	•	watermarks
	•	load signals

These can be distributed via:
	•	org directory subject (preferred)
	•	LAN beacon
	•	optional DHT

3.1 Minimum ad fields (for routing)
	•	provider_id
	•	endpoints (proto + addr)
	•	services: query/search/compute/store/event
	•	shards: list of (table, shard_id, lens) served
	•	watermark: per shard/lens
	•	health: ok/degraded
	•	load: qps, queue depth (rough)
	•	ttl_s
	•	signature

⸻

4. Capability model (hard requirement)

Every request MUST include a capability token that grants:
	•	operations allowed: query.execute, search.execute, compute.execute, fetch.object
	•	scopes allowed: namespaces/tables/compartments/subjects
	•	constraints: row filters, time windows, max rows/bytes, require provenance

Providers MUST enforce capabilities before execution.

⸻

5. Execution modes

5.1 Fast path (fanout=1)

Used for:
	•	table@key point queries
	•	small filtered queries that hit one shard/partition
	•	most ERP screens

Planner rule:
	•	If query can be satisfied by one shard, MUST choose fast path.

5.2 Wide path (scatter/gather)

Used for:
	•	large scans across shards
	•	aggregates across many partitions/shards
	•	full-text / vector searches when not pre-indexed centrally

Planner rule:
	•	wide path must be bounded:
	•	max shards
	•	max partitions
	•	max time window
	•	max work budget
	•	max result size

⸻

6. Query planning (minimal but real)

6.1 Query IR

Queries compile to a small operator pipeline:
	•	scan (partitioned)
	•	filter
	•	project
	•	group/agg (monoid)
	•	sort
	•	take
	•	join (keyed dims only in v1)

6.2 Key decision: single shard test

Planner MUST attempt:
	•	can predicates constrain to a single shard?
	•	does query use keyed lookup?
	•	does it specify a key range?

If yes → fast path.

6.3 Partition pruning

If query includes time constraints and table is time partitioned:
	•	only relevant partitions are touched.

6.4 Join policy

v1 join rules:
	•	joins allowed only if:
	•	right side is keyed dim table replicated everywhere OR
	•	right side is in same shard mapping
Otherwise planner must refuse or require a materialized view.

⸻

7. Routing and replica selection

Given a target shard, the router chooses a replica.

7.1 Eligibility filters
	•	provider serves required service
	•	provider is in replica set for shard
	•	watermark meets freshness requirement
	•	provider is trusted per profile
	•	capability accepted (audience constraints)

7.2 Ranking function

Score = weighted sum:
	•	trust (allowlist/attestation)
	•	freshness distance (how behind watermark is)
	•	RTT
	•	load

7.3 Hedged requests (tail latency)

For fast path, clients SHOULD hedge:
	•	send to best replica
	•	if no response within hedge_delay_ms (e.g., 20ms), send to second-best
	•	take first successful response, cancel the other

Hedging disabled in highsec if policy forbids redundant disclosure; otherwise allowed.

⸻

8. Time budgets and backpressure (p99 discipline)

Every request has a strict budget:
	•	parse+plan: 2ms
	•	route: 2ms
	•	execute: 20–50ms (fast path)
	•	result marshal: 2–5ms

Wide path budgets are larger but capped:
	•	250ms default
	•	2s max unless explicitly allowed

Providers MUST apply backpressure:
	•	reject wide queries when overloaded (E_OVERLOADED)
	•	expose retry-after hints

⸻

9. Wide path: scatter/gather protocol

9.1 Map task

A wide query decomposes into map tasks per shard/partition:
	•	task.map includes:
	•	query fragment (scan+filter+partial agg)
	•	shard/partition identity
	•	capability
	•	deadline
	•	desired partial result format (monoid state)

9.2 Reduce topology

Reduce SHOULD be tree-based (fan-in):
	•	local reduce near data center
	•	final reduce near client

9.3 Mergeable monoids (required)

Aggregates must be representable as mergeable states:
	•	sum: (sum)
	•	count: (count)
	•	avg: (sum,count)
	•	topK: (heapK) (mergeable by heap merge)
	•	histograms: (bins[])

This guarantees associative reduce correctness.

9.4 Failure handling
	•	map tasks have deadlines
	•	stragglers can be hedged
	•	if some shards fail:
	•	either fail closed (highsec)
	•	or return partial with explicit completeness metadata (org/home profile)

Completeness metadata MUST be explicit:
	•	shards_expected
	•	shards_completed
	•	shards_failed

⸻

10. Results: watermarks, provenance, receipts

10.1 Watermark required

Every result includes:
	•	per-shard watermark used
	•	overall watermark summary (“at least up to …”)

10.2 Provenance modes

Configurable:
	•	none (fastest)
	•	oid per row
	•	oids[] for derived rows
	•	proof_pointer (compact reference to provenance table)

10.3 Receipts (optional)

Highsec profile may require signed receipt:
	•	request hash
	•	capability id
	•	provider id
	•	result hash
	•	watermark summary
	•	signature

⸻

11. Applying the same fabric to compute

Compute execution uses identical flow:
	•	determine scope + basis tip(s)
	•	shard selection (data locality)
	•	route to best executor that has data access
	•	run compute
	•	output is a proposal assertion or proposal payload + provenance
	•	endorsement required to apply

Compute can be:
	•	local
	•	single executor
	•	distributed map/reduce (training, big forecasts)

Same scheduling and budgets.

⸻

12. “Where is the data?” is always irrelevant to users

This is achieved because:
	•	placement is deterministic (ShardMap)
	•	liveness is soft-state ads
	•	authorization is capability
	•	routing is automatic
	•	results are verifiable

Users only specify:
	•	what they want
	•	what lens
	•	what freshness
	•	constraints (optional)

⸻

13. Minimal v1 deliverables (ship this)
	1.	Org ShardMap subject format + client cache
	2.	Provider ads with shard coverage + watermark + load
	3.	Capability token issuance + enforcement
	4.	Fast path:
	•	point query protocol
	•	replica selection + hedging
	5.	Wide path:
	•	map task format
	•	monoid reduce
	•	completeness metadata
	6.	Watermark inclusion + optional receipts
	7.	Backpressure + budgets

This will give you a system that feels like a “global computer” while remaining fully decentralized and safe.

Below is a complete, implementable specification for a q/kdb-inspired projection database + query/search/vector engine for DHARMA.

This is DHARMA-Q v1: a projection-only system (derived, rebuildable) that consumes accepted DHARMA assertions and provides blazing fast ERP-style queries, geo, typo-resistant search, and vector search—with no SQL and a terse q-like language.

⸻

DHARMA-Q v1

A q/kdb-inspired Projection Database + Query/Search/Vector Engine for DHARMA

Status

Draft v1 — implementable, deterministic in semantics (not necessarily deterministic in ranking for fuzzy search unless configured).

⸻

0) Core principles

0.1 Truth vs projection
	•	Truth remains the DHARMA append-only object store (assertion envelopes + artifacts).
	•	DHARMA-Q stores only derived projections/indexes.
	•	DHARMA-Q MUST be disposable: delete it, rebuild it from the DHARMA event log.

0.2 Performance goals
	•	Optimize for 99.9% simple queries:
	•	filter, select, sort, limit
	•	group & aggregates
	•	small joins against keyed dimensions
	•	time bucketing
	•	geo within/near
	•	full-text search (typo tolerant)
	•	vector similarity search
	•	Predictable latency:
	•	interactive: <10–50 ms typical for common queries on warm cache
	•	dashboard: <100–300 ms typical (aggregations)
	•	heavy scans: acceptable but visible (explain must reveal cost)

0.3 Small kernel separation

DHARMA-Q MUST be behind:
	•	a feature flag, or preferably
	•	a separate binary (dharmaq / dharma-queryd) to keep DHARMA runtime certifiable and tiny.

0.4 Provenance is first-class

Every row in DHARMA-Q MUST carry:
	•	oid (source assertion object id) and/or oids (set of contributing assertion ids)
	•	sub (subject id)
This enables WHY / audit from query results back to DHARMA truth.

⸻

1) High-level architecture

DHARMA-Q is composed of:
	1.	Ingestor

	•	subscribes to accepted assertions (from local store or network)
	•	extracts facts/rows
	•	appends to hot partitions
	•	maintains indexes incrementally

	2.	Columnar Store

	•	kdb-style tables
	•	time partitioned
	•	column files + symbol dictionary
	•	hot partition mutable, cold partitions immutable/mmapped

	3.	Query Engine

	•	q-like terse language
	•	vectorized operators over columns
	•	partition pruning + predicate pushdown
	•	joins optimized for “fact ↔ dim” patterns

	4.	Search Engine

	•	tokenization + normalization
	•	inverted index
	•	typo tolerant retrieval (fast candidate generation + edit distance)
	•	scoring (BM25 or simpler TF-IDF)

	5.	Vector Engine

	•	ANN index (HNSW) per field/table/partition
	•	hybrid vector + filters + geo support
	•	optional reranking

	6.	Geo Engine

	•	fixed-point coordinates
	•	cell indexing (S2 / geohash bits)
	•	bbox + exact geometry checks
	•	near/within/intersects operators

	7.	Explain/Why

	•	query plan inspection
	•	provenance extraction for rows

⸻

2) Projection store: directory layout

All data lives under a DHARMA-Q root, e.g. data/dharmaq/.

dharmaq/
  meta/
    config.toml
    schema_catalog.cbor
    watermark.cbor
  sym/
    sym.dict        # global symbol dictionary
    sym.index       # reverse lookup (optional)
  tables/
    <table>/
      meta.cbor
      partitions/
        p=YYYY.MM.DD/
          cols/
            <col>.bin
            <col>.idx      # optional per-column index
          rowid.bin
          provenance.bin   # optional (oids lists)
          text/            # per-table text index segments (optional)
          vec/             # per-table vector index segments (optional)
          geo/             # per-table geo index segments (optional)
      hot/
        wal.bin            # crash safety for hot partition
        cols/
        indexes/
  indexes/
    text/...
    vector/...
    geo/...

Requirements
	•	Cold partitions MUST be immutable.
	•	Hot partition MAY be mutable but MUST be recoverable from WAL.
	•	A partition is “sealed” by writing a partition.seal marker and compacting/optimizing.

⸻

3) Data types (complete v1)

DHARMA-Q uses typed, columnar vectors. No floats are required for core business logic; floats MAY exist for embeddings only.

3.1 Scalar types
	•	b1  : bool (1 bit logical, stored as u8 or bitset)
	•	i32 : signed 32-bit
	•	i64 : signed 64-bit
	•	u32 : unsigned 32-bit
	•	u64 : unsigned 64-bit
	•	dec : fixed-point decimal {mantissa:i64, scale:u8}
	•	e.g., money in cents: scale=2
	•	time : i64 microseconds since epoch (UTC)
	•	dur  : i64 microseconds (duration)
	•	sym  : symbol (interned string id, u32)
	•	str  : UTF-8 string (rare; prefer sym)
	•	bytes: byte slice (artifact refs, etc.)
	•	id32 : 32-byte id (object_id, subject_id) stored as 32 bytes

3.2 Composite types
	•	list<T>: variable length list (encoded in two columns: offsets + values)
	•	dict<K,V>: map (stored as two lists; v1 discourages for hot queries)

3.3 Geo types (fixed point, deterministic)

All geo types avoid floats.
	•	geopoint
	•	lat_e7: i32 (lat * 1e7)
	•	lon_e7: i32 (lon * 1e7)
	•	optional alt_mm: i32
	•	optional acc_mm: i32
	•	geocell
	•	cell: u64 (S2 cell id or geohash bits at configured precision)
	•	geobox
	•	min/max lat/lon e7
	•	geocircle
	•	center geopoint + radius_m: u32
	•	geopoly
	•	points list geopoint + bbox
	•	holes NOT in v1 (add later)

3.4 Vector types
	•	vec_f16[n] or vec_i8[n] (preferred)
	•	vec_f32[n] allowed (but larger)
Vectors stored columnar; ANN index stores graph.

⸻

4) Tables, keys, and joins

4.1 Table kinds
	•	Fact tables: high volume, time partitioned (orders, postings, events)
	•	Dim tables: lower volume, keyed (customers, products, vendors)
	•	Index tables: inverted index postings, vector node metadata, geo cell maps

4.2 Keyed tables (kdb-inspired)

A table MAY be “keyed” by one or more columns:
	•	customer keyed by customer_id
	•	product keyed by sku or product_id

Keyed lookup must be O(1) or O(log n) depending on index type.

4.3 Join support (v1)

DHARMA-Q supports joins optimized for ERP:
	•	lj left join
	•	ij inner join
	•	aj as-of join for time-series (optional v1)
	•	join condition limited to equality on key columns (v1)

⸻

5) Column encoding and storage

5.1 Column files

Each column is stored as:
	•	header (type, count, encoding)
	•	data blocks

Encodings:
	•	plain (fixed-width)
	•	dictionary (for sym and low cardinality)
	•	RLE (run-length encoding for repeated values)
	•	delta encoding for monotonic numbers (timestamps)
	•	bitset encoding for bools and some categorical filters
	•	optional compression: LZ4/Zstd (feature flag; Zstd for cold partitions)

5.2 Nullability

Nulls are supported via:
	•	a bitmap column <col>.null OR
	•	sentinel values for some types (discouraged)

5.3 Hot partition write path

Hot partitions append to column append buffers + WAL:
	•	wal.bin records row batches (row-oriented) for recovery
	•	on flush/compaction, WAL is folded into columnar blocks

Cold partition read path is memory-mapped where possible.

⸻

6) Ingestion from DHARMA (projection pipeline)

6.1 Input stream

DHARMA-Q ingests only ACCEPTED assertions (per lens):
	•	from local DHARMA store tailing
	•	or via subscription API from a gateway/node

It must track a watermark:
	•	last processed object_id or per-subject frontier tip, per lens

6.2 Fact extraction rules

Extraction is defined by packages (stdlib or company):
	•	mapping from assertion type → rows in one or more tables
	•	must be versioned with the same data lens model

Example mapping:
	•	std.order.create → row in order + N rows in order_line
	•	std.ledger.post → N rows in ledger_posting

6.3 Idempotency

Every ingested row MUST include:
	•	oid (source assertion id)
	•	sub (subject)
Rows MUST be deduplicable by (table, oid, row_ordinal).

6.4 Provenance fields (required)

At minimum:
	•	oid: id32
	•	sub: id32
	•	ts: time (best available timestamp claim or derived ordering)

Derived rows (aggregates) SHOULD store:
	•	oids: list<id32> or a compact provenance pointer.

⸻

7) Query language (q-inspired, no SQL)

7.1 Overview

A query is an expression producing:
	•	a scalar
	•	a vector
	•	a table

The dominant form is pipeline:

<table_expr> | <op> | <op> | ...

7.2 Lexical conventions
	•	identifiers: [a-zA-Z_][a-zA-Z0-9_.]*
	•	symbols: 'foo or "foo" depending on preference; pick one
	•	time literals: 2026.01.15, now(), today()
	•	duration literals: 5s, 10m, 2h, 7d, 30d

7.3 Core operators (v1)

Source
	•	t table reference
	•	t[p=2026.01.15] partition
	•	t@key keyed lookup (if applicable)

Filter
	•	where <pred>[, <pred>...]
Predicates:
	•	= != < <= > >=
	•	in
	•	between
	•	like (prefix/suffix/contains on sym and str)
	•	isnull, notnull

Projection
	•	sel col1,col2,...
	•	sel expr as name, ...

Sort / limit
	•	sort col (asc)
	•	sort -col (desc)
	•	take n
	•	drop n

Group + aggregate
	•	by col[,col...] | agg sum(x), count(), min(x), max(x), avg(x)
	•	bucket ts 1d (adds a bucket column)
	•	by bucket ts 1d | agg ... allowed

Joins
	•	lj <table> on <a>=<b> (or on key)
	•	ij ...
	•	aj ... (optional)

Search
	•	search "query" in <table>.<field>[,<field>...] [opts...]

Vector
	•	vsearch "query" in <table>.<vecfield> k=50 [opts...]
	•	vnear <vector_literal> in ... (optional)

Geo
	•	near (lat=…,lon=…) within 5000m
	•	within zone <place_id_or_sym>
	•	within circle (...)
	•	within box (...)
	•	intersects ... (v2)

Explain/why
	•	explain <query>
	•	why row <n> or why oid <id>

⸻

8) Query engine execution model (vectorized)

8.1 Execution pipeline

Queries compile to a physical plan of operators:
	•	partition pruning
	•	column selection
	•	predicate evaluation producing a boolean mask
	•	mask application producing filtered vectors
	•	group-by via hash maps / sort-group
	•	join via keyed lookup or hash join
	•	sorting via indices
	•	take/drop by slicing

8.2 Partition pruning (mandatory)

If the query includes a time predicate on a partitioned table, engine MUST:
	•	select only relevant partitions first

8.3 Predicate pushdown (mandatory)

Filters must evaluate using only referenced columns; avoid materializing full rows.

8.4 Join strategies (v1)
	•	keyed lookup join: O(n) for left side, fast
	•	hash join: for non-keyed dims (optional)
	•	join must preserve provenance: row provenance is union of both sides

8.5 Determinism

Numeric query semantics are deterministic.
Search ranking may be deterministic if:
	•	tokenization and candidate ordering are fixed
	•	ties broken by oid ascending

Vector ANN results are not strictly deterministic (graph traversal); if determinism needed, add deterministic=true mode that forces full scan or exact kNN for small sets.

⸻

9) Full-text search (typo-resistant)

9.1 Goals
	•	fast keyword search
	•	typo tolerance (misspellings)
	•	phrase-ish behavior optional
	•	field weighting
	•	results joinable back to domain tables

9.2 Normalization pipeline

For each indexed field:
	1.	Unicode normalize (NFKC)
	2.	casefold
	3.	remove diacritics (configurable)
	4.	tokenize (unicode word boundaries)
	5.	optional stemming (off by default; ERP often wants literal)
	6.	stopwords optional (off by default for names/SKUs)

9.3 Inverted index layout

Per partition (or global for dims):
	•	term -> postings
Postings contain:
	•	doc_id (rowid) OR oid directly
	•	optional positions for phrase support
	•	per-field weights

Store postings compressed:
	•	delta-encoded docids
	•	varints
	•	optional roaring bitmap for high-frequency terms

9.4 Typo tolerance design (fast)

Do two-stage retrieval:

Stage A: Candidate generation
	•	Build an n-gram index (recommended trigram) over terms or over field text.
	•	Query trigrams of the input token to get candidate terms quickly.

Stage B: Edit-distance filtering
	•	Use a bounded Levenshtein distance (e.g., ≤1 or ≤2 depending on token length).
	•	Filter candidate terms by edit distance.
	•	Deterministically sort candidates by:
	1.	edit distance
	2.	term frequency / idf
	3.	lexicographic term

Stage C: Scoring
	•	BM25 or TF-IDF variant.
	•	Score per document = sum(term scores * field weight).
	•	tie-break by oid or rowid.

9.5 Query syntax

Examples:

search "foie gras" in product.name,product.desc
search "andouillete troyes" in product.name fuzz=2
search "saaf" in vendor.name fuzz=1
search "INV-2026" in invoice.id exact=true

Options (v1):
	•	fuzz=<0..2> default 1 for tokens length ≥5
	•	prefix=true|false
	•	fields weights: w(name)=3,w(desc)=1
	•	limit=n

9.6 Output table

Search returns a result table:
	•	oid (or doc rowid + join key)
	•	score
	•	field
	•	snippet (optional)
	•	provenance pointer to source assertion(s)

⸻

10) Vector search (ANN)

10.1 Goals
	•	semantic retrieval
	•	hybrid filtering (status, warehouse, price range)
	•	reranking optional
	•	joinable results

10.2 Vector storage

Vectors stored columnar in the table:
	•	embed: vec_i8[256] or vec_f16[384]
	•	metadata: oid, join keys

10.3 ANN index: HNSW (v1)

Per table.field per partition (or global for small dims):
	•	HNSW graph persisted in vec/
	•	node id corresponds to rowid
	•	store:
	•	level
	•	neighbor lists per level
	•	entry point
	•	vector norms if needed

10.4 Hybrid query execution

Execution order:
	1.	apply structured filters first (mask)
	2.	ANN search over candidates:
	•	either build per-partition HNSW and query those partitions
	•	or global HNSW + filter during traversal (less efficient)
	3.	optional rerank on exact similarity for top K*R (R=2..5)
	4.	return top K

10.5 Similarity metrics
	•	cosine similarity (recommended)
	•	dot product
Use fixed behavior.

10.6 Query syntax

vsearch "luxury cheese gift" in product.embed k=50
| lj product on oid=product.oid
| where price<5000
| sort -score
| take 10


⸻

11) Geo engine (fast + deterministic)

11.1 Core idea

Never scan polygons blindly. Use:
	•	cell index (geohash bits / S2 cell id)
	•	bbox reject
	•	exact check on survivors

11.2 Geo indexing

For any geo point event table:
	•	store lat_e7, lon_e7
	•	store derived cell at configured precision
Index:
	•	(cell, ts, rowid) → fast region queries

For zones/places:
	•	store polygon/circle + bbox + covering cells
	•	index zone coverage cells → candidate zones

11.3 Operators

Near

ship_evt | near (lat=14.5547, lon=121.0244) within 5000m

Execution:
	•	compute set of cells covering radius (approx)
	•	fetch candidates via cell index
	•	exact distance check using fixed-point approximation

Within

evt | within zone 'ncr.delivery

Execution:
	•	resolve zone polygon
	•	candidate points via zone cell covering
	•	exact point-in-polygon with deterministic ray casting on int coords

11.4 Deterministic geometry rules
	•	Points on boundary count as inside (recommended)
	•	Polygon rings must be canonical (fixed winding order)
	•	Max polygon vertices configured (caps worst-case)

⸻

12) Search + vector + geo combined (“universal queries”)

DHARMA-Q supports hybrid queries by pipeline:

Example: “recommended products near delivery zone, typo tolerant search”

search "andouillete" in product.name fuzz=2
| lj product on oid=product.oid
| where in_stock=true
| vsearch "classic french charcuterie" in product.embed k=200
| where price between (1500,5000)
| sort -score
| take 20

Geo + vector:

warehouse_evt | within zone 'ncr
| vsearch "cold chain risk" in incident.embed k=50


⸻

13) Explain and provenance (“WHY” for queries)

13.1 explain <query>

Must print:
	•	partitions scanned
	•	columns read
	•	indexes used (cell/text/vector)
	•	join strategy
	•	estimated cost
	•	actual runtime (if executed)

13.2 why row <n>

Returns:
	•	oid (source assertion)
	•	if derived: oids[] (or provenance pointer)
	•	optional: link to DHARMA prove output

Rule: any projection row must be traceable back to DHARMA truth.

⸻

14) API surface (for ERP apps)

Even if you’re not doing GraphQL yet, you’ll want:
	•	a query endpoint that takes a DHARMA-Q expression and returns rows
	•	a subscription endpoint for incremental updates (optional)

14.1 Query API

POST /query
	•	input: query string + parameters
	•	output: table (columnar JSON or row JSON)
	•	optional: include provenance

14.2 Prepared queries (optional v1)

Allow parameterized queries:

q("invoice | where status=$1, ts>= $2 | sort -ts | take $3", ["open", today()-30d, 50])


⸻

15) Operational behavior

15.1 Rebuild

Two modes:
	•	full rebuild: wipe DHARMA-Q store, replay all accepted assertions
	•	incremental: tail from watermark

15.2 Compaction & sealing

Hot partition:
	•	accepts writes
	•	WAL ensures crash safety
Periodic:
	•	seal partition
	•	compress columns
	•	build/optimize indexes
	•	move to cold partitions

15.3 Feature flags

Recommended:
	•	query (engine)
	•	text (inverted + fuzzy)
	•	vector (ANN)
	•	geo (cell + geometry)
	•	compression_zstd
	•	deterministic_search (tie-breaking & stable ranking)

⸻

16) Minimal v1 roadmap (what to implement first)

Phase Q1 (core ERP speed)
	•	columnar store
	•	partitions
	•	where/sel/sort/take/by/agg
	•	keyed dims + lj/ij
	•	provenance via oid

Phase Q2 (geo)
	•	geopoint + cell index
	•	near/within operators

Phase Q3 (text search)
	•	tokenization + inverted index
	•	trigram candidate generator + edit distance fuzz=1/2
	•	scoring + stable tie-break

Phase Q4 (vector)
	•	store vectors
	•	HNSW per table.field
	•	hybrid filters + rerank

Phase Q5 (why/explain complete)
	•	full explain plans
	•	provenance on derived results (oids pointers)

⸻

17) Example “99.9% ERP queries” in DHARMA-Q

Open invoices:

invoice | where status='open | sort -ts | take 50

AP aging:

invoice | where status='open
| sel id,vendor,total,due,age=(now()-due)
| by bucket age 30d
| agg sum(total),count()

Inventory by warehouse:

inventory | by wh | agg qty=sum(qty) | sort -qty

Top customers last 30 days:

order | where ts>=today()-30d
| by customer
| agg rev=sum(total)
| sort -rev
| take 20

Near deliveries:

delivery_evt | near (lat=14.55,lon=121.02) within 3000m | take 100

Typo tolerant product search:

search "comte chees" in product.name fuzz=2 | take 20

Semantic search:

vsearch "luxury french cheese board" in product.embed k=50
| lj product on oid=product.oid
| sel product.id, product.name, score
| sort -score
| take 10


⸻

Final note (important)

This design is coherent with DHARMA because:
	•	DHARMA remains the immutable truth ledger.
	•	DHARMA-Q is a fast, disposable, rebuildable projection engine.
	•	It gives you q/kdb-style speed without dragging SQL complexity in.
	•	It supports geo + typo-resistant text + vector in a unified, terse query language.
	•	It retains DHARMA’s superpower: explainability and provenance.

Intent

To define a peer-to-peer protocol that allows independent parties to create, share, validate, and audit truth about work without relying on central servers, administrators, or vendors.

The protocol exists to make commitments, decisions, responsibilities, and facts durable, verifiable, and trustworthy — even in the absence of continuous connectivity or centralized control.

At its core, the protocol treats all collaborative work as signed, typed assertions about shared reality, ensuring that meaning is explicit, authority is provable, and history is replayable.

⸻

Use

The protocol is designed to be used wherever people must coordinate, decide, and commit over time, especially when:
	•	Trust must not depend on a central service
	•	Auditability matters
	•	Work spans devices, organizations, or long time horizons
	•	Connectivity is unreliable or intermittent
	•	Administrative overhead is undesirable

Typical uses include:
	•	Recording decisions and approvals
	•	Delegating and tracking responsibilities
	•	Managing obligations, invoices, and payments
	•	Maintaining shared knowledge and context
	•	Coordinating work asynchronously across peers

The protocol does not replace communication tools; it complements them by serving as the system of record for what is true, agreed, and committed.

⸻

Core Use Cases

1. Decision Recording & Accountability

Problem:
Decisions are made in meetings, chats, or emails, then lost or disputed.

Use case:
Peers record decisions as typed assertions:
	•	what was decided
	•	by whom
	•	under what authority
	•	at what time

Later, anyone can replay the decision history and verify signatures without relying on meeting minutes or chat logs.

⸻

2. Task Delegation Without Project Management Overhead

Problem:
Delegating small tasks requires heavy tools or informal messages that decay.

Use case:
A peer asserts:
	•	a task exists
	•	responsibility is delegated
	•	completion is acknowledged

Tasks remain lightweight, text-first, and auditable, without requiring projects, boards, or centralized task systems.

⸻

3. Approvals and Reviews

Problem:
Approvals (documents, invoices, changes) are scattered across tools and hard to audit.

Use case:
Approvals are explicit assertions:
	•	“Document X approved by Peer Y”
	•	“Invoice Z rejected with reason”

Approval rules may be validated by contracts, but the approval itself is a signed human act with permanent provenance.

⸻

4. Financial Obligations and Payments

Problem:
Financial commitments are fragmented across accounting systems, emails, and spreadsheets.

Use case:
Peers assert:
	•	obligations (who owes whom, how much)
	•	approvals
	•	payment confirmations

These assertions form a shared, auditable financial ledger that can be reconciled across organizations without a central authority.

⸻

5. Document Provenance and Evidence Tracking

Problem:
Documents lose context: where they came from, who validated them, and why they matter.

Use case:
Documents are referenced as artifacts, and assertions record:
	•	classification (e.g., “this is an invoice”)
	•	extraction results
	•	human validation

This creates an immutable provenance trail linking evidence to decisions and outcomes.

⸻

6. Knowledge Accumulation Without a Central Knowledge Base

Problem:
Knowledge bases require structure, curation, and administration to stay useful.

Use case:
Knowledge emerges organically as assertions that remain relevant over time:
	•	explanations
	•	policies
	•	decisions
	•	rationales

Search and replay allow peers to rediscover knowledge without enforcing rigid taxonomy or ownership.

⸻

7. Cross-Organization Collaboration

Problem:
Collaboration across companies requires trust in shared platforms or duplicated systems.

Use case:
Independent peers exchange encrypted assertions:
	•	each retains local control of data
	•	shared truth is cryptographically verifiable
	•	no party owns the infrastructure

This enables joint projects, contracts, and financial coordination without vendor lock-in.

⸻

8. Offline-First and Intermittent Environments

Problem:
Most collaboration tools fail without continuous connectivity.

Use case:
Peers continue asserting, reviewing, and committing work offline.
When connectivity resumes, assertions replicate and converge deterministically.

This supports remote work, field work, and long-lived projects.

⸻

9. Assisted Workflows via Stateless Compute

Problem:
Automation is either opaque or tightly coupled to centralized systems.

Use case:
Stateless compute services:
	•	analyze documents
	•	propose structured assertions
	•	flag anomalies

Humans review and accept or reject proposals, preserving trust while reducing manual effort.

⸻

10. Long-Term Institutional Memory

Problem:
Organizations lose history as tools change, vendors disappear, or staff turnover occurs.

Use case:
The protocol serves as a durable memory of:
	•	what happened
	•	why it happened
	•	who authorized it

This memory remains usable decades later, independent of any specific UI or vendor.

⸻

Mission

To provide a minimal, durable, and humane foundation for shared work that:
	•	Is local-first and works offline
	•	Is end-to-end encrypted by default
	•	Requires no central authority to function
	•	Enforces trust through cryptography, not administration
	•	Makes auditability an emergent property, not an afterthought

The mission is to remove the hidden costs of modern collaboration — lost context, decaying tasks, broken trust, and bureaucratic overhead — by ensuring that shared truth is explicit, typed, and verifiable.

⸻

Ambition

The ambition is to create a protocol that becomes a long-lived substrate for collaborative systems, comparable in durability and conceptual clarity to:
	•	Email (for communication)
	•	Git (for distributed truth)
	•	Double-entry accounting (for financial correctness)

Over time, this protocol should enable an ecosystem of tools — notes, task systems, financial ledgers, knowledge bases, and decision logs — that interoperate through shared assertions rather than proprietary backends.

Ultimately, the ambition is not to build another productivity product, but to establish a new primitive for shared truth: one that allows people and organizations to coordinate with confidence, autonomy, and longevity, without surrendering control to centralized platforms.



Project Vision Protocol (PVP) v1

A local-first, peer-to-peer protocol for creating, sharing, validating, and auditing signed typed assertions about work — without servers, without central authority, and without operational admin.

This document is written like an RFC: it is implementable, deterministic, and deliberately boring.

⸻

1. Conformance language

The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be interpreted as normative requirements.

⸻

2. What this protocol is

PVP defines:
	1.	The only semantic primitive: the Assertion
A signed, typed, append-only statement by a peer about a subject.
	2.	How assertions are stored and exchanged:
	•	content-addressed objects
	•	replicated peer-to-peer
	•	offline-capable
	3.	How truth is derived:
	•	deterministic validation (schema + contract)
	•	deterministic convergence (same assertions ⇒ same derived state)
	•	auditability by replay

PVP does not define:
	•	user interfaces
	•	workflows
	•	chat semantics
	•	centralized identity
	•	global consensus

⸻

3. Core invariants (protocol laws turned into requirements)

Law 1 — No hidden state
	•	Any client MUST be able to reconstruct all meaningful state by replaying assertions it possesses and can decrypt.
	•	Implementations MAY cache derived state, but caches MUST be disposable and recomputable.

Law 2 — No silent mutation
	•	Derived state MUST NOT change unless a new assertion becomes present and accepted (or a previously pending assertion becomes accepted due to newly received prerequisites).

Law 3 — Compute never commits
	•	The protocol recognizes only signed assertions as candidates for truth.
	•	“Compute services” MAY generate candidate assertions, but unless they hold an explicitly trusted signing key (i.e., they are a peer), their output is non-authoritative.
	•	If you want “proposals,” you represent them as assertions whose contract-defined effect is “no state transition until endorsed.”

Law 4 — Trust is cryptographic
	•	All authorization decisions MUST reduce to verifying signatures and evaluating deterministic rules over prior assertions.
	•	There is no privileged server or admin role at the protocol layer.

Law 5 — Local first
	•	A peer MUST be able to create, store, validate, and derive state while offline.

Law 6 — Deterministic convergence
	•	For a given subject, for any two peers with the same decryptable assertion set and the same contract code, derived state MUST be identical.

Law 7 — Typed meaning, free text
	•	Assertions MUST have explicit types.
	•	Business-relevant fields MUST be typed and validated.
	•	Free text is permitted, but it MUST NOT be required to interpret state transitions.

⸻

4. Terminology
	•	Peer: a participant identified by a cryptographic public key (identity key).
	•	Identity key: the public key that identifies a peer (for signatures).
	•	Subject: a 32-byte identifier representing the “thing” assertions are about (document).
	•	Assertion: the atomic semantic unit; signed, typed, append-only.
	•	Envelope: the encrypted, content-addressed container used to replicate an assertion.
	•	Artifact: any large blob referenced by assertions (attachments), also encrypted and content-addressed.
	•	Schema: a content-addressed description of allowed assertion types and typed fields.
	•	Contract: a deterministic program used to validate assertions and derive state.
	•	Key epoch: a subject-level encryption key generation (for rotation/revocation).

⸻

5. Cryptographic suite (PVP Suite 1)

PVP is crypto-agile via a suite_id field. This spec defines Suite 1, which implementations MUST support to claim PVP v1 compliance.

Suite 1 algorithms
	•	Hash: SHA-256
	•	Signatures: Ed25519 (RFC 8032)
	•	Public-key encryption for key wrapping: HPKE (RFC 9180) with:
	•	KEM: X25519
	•	KDF: HKDF-SHA256
	•	AEAD: ChaCha20-Poly1305
	•	Symmetric encryption for assertion/artifact payloads: ChaCha20-Poly1305 (RFC 8439) with 96-bit nonce

Note: Transport encryption is optional because payloads are end-to-end encrypted, but encrypted transport is still recommended to reduce metadata leakage.

⸻

6. Identifiers and content-addressing

6.1 Object ID

Every replicated object (assertion envelope, artifact envelope, invite package) is content-addressed:
	•	object_id = SHA-256(object_bytes)
	•	object_id is 32 bytes

This guarantees:
	•	deduplication
	•	integrity (tamper-evidence)
	•	immutable history

6.2 Subject ID

A Subject ID is exactly 32 bytes.
	•	When creating a new subject, the creator MUST generate 32 random bytes (cryptographically secure RNG).
	•	A subject “exists” if and only if assertions referencing that subject exist.

There is no global registry. Collisions are treated as cryptographic impossibility.

⸻

7. Canonical encoding

All structured data in PVP v1 MUST be encoded as Canonical CBOR (deterministic encoding per RFC 8949).
	•	Signature input bytes MUST be canonical CBOR.
	•	Contract input bytes MUST be canonical CBOR.
	•	Schema artifacts MUST be canonical CBOR (or canonical JSON if explicitly declared; v1 recommends CBOR only).

This avoids “same meaning, different bytes” ambiguity.

⸻

8. Assertion: the only semantic primitive

8.1 Assertion plaintext structure (inside encryption)

An assertion is a CBOR map with two top-level keys: h (header) and b (body).

Header fields (normative)
The header MUST include:
	•	v (uint): protocol version (v1 = 1)
	•	sub (bytes, 32): subject id
	•	typ (tstr): assertion type identifier (namespaced string)
	•	auth (bytes, 32): author identity public key (Ed25519)
	•	seq (uint): author sequence number within this subject
	•	prev (bytes, 32) or null: object_id of the author’s previous assertion within this subject (null if seq=1)
	•	refs (array of bytes(32)): zero or more referenced assertion object_ids (dependencies/causal basis)
	•	ts (int or null): authored timestamp claim (e.g., Unix microseconds). For UI/audit context; not trusted for ordering.
	•	schema (bytes, 32): object_id of the schema artifact governing this subject
	•	contract (bytes, 32): object_id of the contract artifact governing this subject

The header MAY include:
	•	note (tstr): free-text note about intent
	•	meta (map): additional typed metadata (must be schema-defined if used for meaning)

Body fields (normative)
The body is an arbitrary CBOR value that MUST be validated by the schema for typ.
	•	b is typically a CBOR map with typed fields.
	•	Free-text is allowed in body fields of type text or tstr, but meaningful fields must be typed.

Signature (normative)
The assertion also includes:
	•	sig (bytes): Ed25519 signature over canonical CBOR of {h, b} excluding sig itself.

Formally:
	•	sig = Ed25519.Sign(author_sk, CanonicalCBOR({ "h": header, "b": body }))

The assertion plaintext structure is:

{
  "h": { ...header fields... },
  "b": ...body...,
  "sig": <bytes>
}

8.2 Assertion type naming

typ is a namespaced string.
	•	Reserved prefix: core. for protocol-defined types.
	•	All other types are application/document-type specific.

Examples:
	•	core.genesis
	•	core.key.rotate
	•	task.create
	•	invoice.issue
	•	approval.grant

⸻

9. Encryption model

9.1 Subject encryption keys and epochs

Each subject uses symmetric encryption keys, rotated over time.
	•	Each epoch has a 32-byte symmetric key K_epoch.
	•	Each epoch has a key identifier kid = SHA-256(K_epoch) (32 bytes)

Assertions and artifacts are encrypted under a specific kid.

9.2 Assertion envelope (replicated object)

The envelope is what peers replicate. It is not the assertion itself; it is the encrypted container.

Envelope fields:
	•	v (uint): envelope version (1)
	•	suite (uint): crypto suite id (1)
	•	kid (bytes, 32): key id used for encryption
	•	nonce (bytes, 12): ChaCha20-Poly1305 nonce
	•	ct (bytes): ciphertext (AEAD output)
	•	aad is implicit: canonical CBOR of {v, suite, kid, nonce} (or equivalent fixed ordering)

Encryption:
	•	ct = AEAD_Encrypt(K_epoch, nonce, plaintext_assertion_bytes, aad)

Decryption:
	•	plaintext = AEAD_Decrypt(K_epoch, nonce, ct, aad)

No plaintext assertion fields transit or rest outside encryption.
The envelope reveals only {suite, kid, nonce, ct length}.

9.3 Artifact envelopes

Artifacts use the same envelope format, but plaintext is arbitrary bytes (or a small CBOR wrapper containing mime type + bytes). Artifacts are referenced by object_id.

9.4 Key distribution is access control

Access control is defined by who can obtain K_epoch.
	•	If you have the subject key for an epoch, you can decrypt assertions in that epoch.
	•	Revocation is achieved by rotating keys and refusing to wrap new keys for revoked peers.

⸻

10. Core subject bootstrap and key rotation

PVP needs a minimal set of core assertion types to make subject encryption workable and auditable.

10.1 core.genesis

The first assertion for a subject MUST be core.genesis.

Contract rule (minimum):
	•	exactly one core.genesis is accepted per subject
	•	any other assertion for the subject is invalid unless a genesis is present

Body (recommended fields; schema/contract may extend):
	•	doc_type (tstr): human-readable document type name
	•	schema (bytes32): schema object_id (MUST match header.schema)
	•	contract (bytes32): contract object_id (MUST match header.contract)
	•	title (tstr or text): free-text label
	•	members (array of bytes32): initial authorized identity keys (optional but common)
	•	key_policy (map): optional governance hints (thresholds etc) — only meaningful if contract interprets it

This is the cryptographic “constitution” of a subject.

10.2 core.key.rotate

A key rotation is represented as an assertion about the subject.

Body fields (normative for interoperability):
	•	from_kid (bytes32): current/previous epoch key id
	•	to_kid (bytes32): new epoch key id
	•	wraps (array): list of wrapped key entries:
	•	each entry: { "rcpt": <bytes32 identity pk>, "hpke_ct": <bytes> }
	•	hpke_ct decrypts to the raw 32-byte K_new_epoch
	•	reason (tstr or null): optional free text

Semantics:
	•	Peers who can decrypt the core.key.rotate assertion (using from_kid) can attempt to unwrap K_new_epoch using their HPKE private key.
	•	A peer is revoked if it does not receive a wrapper for to_kid (and thus cannot decrypt new epoch assertions).

Important: Whether a rotation is accepted (and therefore whether later assertions “should” use to_kid) is determined by the subject’s contract. The crypto mechanism only makes rotation possible.

10.3 Direct key grants (out-of-ledger delivery object)

To add a peer who does not yet have any subject key, you need an out-of-ledger delivery mechanism that is still encrypted and auditable.

PVP defines a Key Grant Package object (replicated like other objects, but not a subject assertion):

Fields:
	•	v (uint): 1
	•	suite (uint): 1
	•	grantor (bytes32): grantor identity pk
	•	rcpt (bytes32): recipient identity pk
	•	hpke_ct (bytes): HPKE ciphertext to recipient that decrypts to:
	•	sub (bytes32): subject id
	•	kid (bytes32): epoch key id
	•	K_epoch (bytes32): epoch key
	•	genesis_id (bytes32): object_id of core.genesis
	•	optional: pointers to schema/contract artifacts
	•	sig (bytes): Ed25519 signature by grantor over canonical CBOR of the package fields (excluding sig)

This package is end-to-end encrypted to the recipient and provides the minimum data to join.

Audit linkage: The subject ledger SHOULD also contain an assertion (e.g., core.member.add) recording that the peer is considered a member, so other members can audit authorization decisions. The exact membership model is contract-defined.

⸻

11. Replication model (peer-to-peer)

PVP replication is transport-agnostic. It defines message frames that can run over:
	•	QUIC
	•	TCP + TLS
	•	WebRTC data channels
	•	local transports (USB, file drop)
	•	store-and-forward relays

Because assertions are already encrypted, replication correctness does not depend on transport security.

11.1 Local object store

Every peer maintains:
	•	an object store: object_id -> object_bytes (append-only)
	•	optional derived indexes (subject catalogs, frontiers, etc), which are recomputable

11.2 Sync session goals

Given two peers A and B, sync attempts to:
	•	discover which objects the other has
	•	transfer missing objects
	•	do so incrementally
	•	allow partial replication

11.3 Minimal sync protocol (PVP-SYNC/1)

Message types
All messages are canonical CBOR maps with t (type) and p (payload).
	•	hello: capabilities + peer descriptor
	•	inv: inventory summary
	•	get: request object ids
	•	obj: send object bytes
	•	err: error message (non-fatal)

hello
Payload:
	•	v: protocol version (1)
	•	peer_id: identity pk (Ed25519)
	•	hpke_pk: HPKE public key (X25519)
	•	suites: array of supported suite ids
	•	note: optional free text

No global registry. Trust of peer_id is local.

inv (inventory)
Two modes (both allowed; implementation MAY support either or both):

Mode A: Subject-aware inventory (used when peers share subject keys)
Payload:
	•	subjects: array of entries:
	•	sub: subject id
	•	frontier: array of assertion object_ids (see below)

Mode B: Blind inventory (for store-and-forward / relays)
Payload:
	•	objects: array of object_ids (possibly truncated or batched)

Mode A is preferred for peers collaborating on shared subjects. Mode B is simpler but can be large.

Frontier definition (subject-aware)
For a subject, a peer’s frontier is the set of assertion object_ids that the peer believes are “tips” of the known dependency graph. Minimum workable definition:
	•	An assertion is in the frontier if:
	•	it belongs to the subject, and
	•	no other locally-known assertion references it in refs or prev.

Frontier is derived state and recomputable.

get
Payload:
	•	ids: array of object_ids to fetch

obj
Payload:
	•	id: object_id
	•	bytes: exact object bytes (assertion envelope, artifact envelope, or key grant package)

Sync behavior (baseline)
	1.	A and B exchange hello.
	2.	A sends inv (subjects it wants to sync + its frontiers) or requests B’s inv.
	3.	Both sides compute missing objects by graph traversal:
	•	request frontier objects you lack
	•	for each received assertion, parse (decrypt if possible) and request any referenced ids you lack
	4.	Continue until no new ids are discovered or policy limits reached.

11.4 Partial replication

A peer MAY:
	•	sync only some subjects
	•	cap recursion depth when fetching ancestors
	•	refuse to serve certain objects
	•	garbage-collect old objects locally

This may reduce completeness, but must not create incorrect accepted state: missing assertions simply cannot be used.

⸻

12. Deterministic validation pipeline

For a given subject, a peer computes derived state via a deterministic pipeline.

12.1 Candidate set

Let E_sub be the set of envelopes in the local store with a kid that the peer can decrypt and whose plaintext header sub matches the subject.

A peer MAY also store envelopes it cannot decrypt; they are ignored for derived state.

12.2 Structural validation (deterministic)

For each decrypted plaintext assertion:
	1.	Parse canonical CBOR into {h, b, sig}.
	2.	Verify h.v == 1.
	3.	Verify h.sub is 32 bytes.
	4.	Verify h.typ is a string.
	5.	Verify h.auth is 32 bytes.
	6.	Verify sig is valid Ed25519 signature of canonical CBOR of {h,b} under h.auth.
	7.	Verify author chain fields:
	•	h.seq is integer ≥ 1
	•	if h.seq == 1, h.prev MUST be null
	•	if h.seq > 1, h.prev MUST be a valid object_id (32 bytes)
	•	if the prev assertion is available and decryptable, its header MUST match:
	•	same sub
	•	same auth
	•	prev.seq == h.seq - 1

If (7) cannot be verified because prev is missing, the assertion is PENDING (not rejected). This preserves offline/partial replication behavior.

12.3 Canonical dependency ordering

To replay deterministically, construct a dependency graph among available assertions for the subject:
	•	For each assertion A, define dependencies as:
	•	deps(A) = set(A.h.refs) ∪ {A.h.prev if not null} restricted to ids that are present and decryptable.

Compute a deterministic topological order using:
	•	Kahn’s algorithm
	•	tie-break by lexicographic ascending object_id

This yields a canonical replay order ORDER_sub.

12.4 Schema validation (typed meaning)

Each subject has a schema object_id declared in every assertion header (and typically anchored by core.genesis).

A schema defines:
	•	allowed typ values
	•	the required/optional fields for b
	•	types for each field
	•	any additional structural constraints

An assertion that fails schema validation is REJECTED.

12.5 Contract validation (truth rules)

Each subject has a contract object_id declared in every assertion header (and typically anchored by core.genesis).

A contract defines:
	•	whether an assertion is acceptable given the already-accepted history
	•	invariants (e.g., “cannot approve without prior request”)
	•	authorization rules (e.g., “only members can assign responsibility”)
	•	how derived state is computed

Contract execution is deterministic and stateless (see §13).

An assertion that fails contract validation is REJECTED.

If contract validation cannot complete due to missing referenced assertions, missing schema/contract artifacts, or missing cross-subject dependencies explicitly referenced by id, it is PENDING.

12.6 Accepted set and derived state

Let:
	•	ACCEPTED_sub = assertions accepted by schema + contract during replay in ORDER_sub
	•	Derived state STATE_sub = Reduce(contract, ACCEPTED_sub) deterministically

⸻

13. Contracts: deterministic, stateless, replayable

13.1 Contract object

A contract is a content-addressed artifact referenced by contract (bytes32).

PVP v1 defines the standard contract format as Wasm Contract v1.

13.2 Wasm Contract v1 requirements

A contract module:
	•	MUST be a WebAssembly module targeting wasm32
	•	MUST be deterministic:
	•	MUST NOT access time, randomness, network, filesystem
	•	MUST NOT depend on floating-point non-determinism (recommended: avoid floats entirely)
	•	MUST be pure with respect to inputs: output depends only on provided bytes

13.3 Host interface (normative)

Contracts interact with the host only via two exported functions:
	1.	validate(assertion_bytes, context_bytes) -> result_bytes
	2.	reduce(accepted_assertions_bytes) -> state_bytes

Where:
	•	inputs are canonical CBOR bytes
	•	outputs are canonical CBOR bytes

validate input
assertion_bytes is canonical CBOR of the plaintext assertion {h,b,sig} (or {h,b} + separate sig info, implementation choice but must be consistent).

context_bytes is canonical CBOR containing:
	•	subject: subject id
	•	accepted: list of accepted assertion headers (or full assertions) so far in replay
	•	lookup: a map of referenced assertion ids to their decoded assertions (only for refs present)
	•	optional: external for explicitly referenced cross-subject assertions (if the contract requires and the host provides)

validate output
CBOR map:
	•	ok (bool)
	•	status (tstr): "accept" | "reject" | "pending"
	•	reason (tstr or null): optional human-readable reason code (for UI/debug; not used for determinism)

reduce input
CBOR array of accepted assertions (canonical bytes), in canonical replay order.

reduce output
CBOR bytes representing derived state (contract-defined shape).

13.4 Determinism rule for iteration

If a contract iterates over a set/map of assertions, it MUST iterate in a deterministic order.

PVP supplies deterministic order by:
	•	providing accepted assertions already ordered (ORDER_sub)
	•	requiring maps to be canonical CBOR (deterministic key ordering in encoding)

⸻

14. Schemas: typed reality, not inferred

14.1 Schema object

A schema is a content-addressed artifact referenced by schema (bytes32).

PVP v1 defines Schema Manifest v1, a declarative schema format that is sufficient to type-check assertions.

14.2 Schema Manifest v1 (normative minimal)

Schema manifest fields:
	•	v (uint): 1
	•	name (tstr)
	•	types (map):
	•	keys are assertion type strings (typ)
	•	values define body structure:
	•	body (map): field -> type descriptor
	•	required (array of tstr): required field names
	•	allow_extra (bool): default false

Type descriptors (minimal set):
	•	bool, int, bytes, text, id32 (bytes32), pubkey32 (bytes32), list(T), map(K,V), enum([...])

This schema format does not define business semantics. It defines only typed structure. Semantics live in contracts.

⸻

15. Audit model: replayability, attribution, causality, non-repudiation

PVP is auditable because:
	1.	Replayability: all accepted state is derived by replaying assertions.
	2.	Attribution: every assertion is signed; author key is explicit.
	3.	Causality: refs and prev encode explicit dependency and author chains.
	4.	Non-repudiation: signatures + immutable content addressing.

15.1 Required audit queries (deterministic answers)

For any accepted assertion A, an implementation MUST be able to answer:
	•	Who asserted this?
A.h.auth
	•	When?
A.h.ts is the author’s claim.
If stronger guarantees are needed, they must be represented as additional assertions (e.g., time attestation assertions from trusted peers), not hidden logs.
	•	Under which rules?
A.h.schema and A.h.contract (plus the content-addressed bytes they refer to)
	•	Based on which prior assertions?
A.h.refs and A.h.prev (and the referenced assertion contents)
	•	Was it authorized at the time?
Determined by contract validation during replay, using only the assertion set.
“At the time” is represented structurally (via causal refs and key epochs), not by wallclock authority.

15.2 No logging layer

There is no separate audit log. The ledger is the audit trail.

⸻

16. How revocation actually works (and why it remains deterministic)

Revocation is achieved by the combination of:
	1.	Key rotation: revoked peer cannot decrypt future epoch assertions.
	2.	Contract rules: contract can require that valid assertions must reference current membership/epoch assertions (via refs) and/or must be encrypted under currently accepted kid epochs.

Mechanically:
	•	A revoked peer might still possess old keys and old assertions.
	•	They can produce new assertions under old kid, but:
	•	remaining peers’ contracts can reject assertions that do not build on the latest accepted context (which the revoked peer cannot see if context is in new epoch)
	•	peers can treat assertions under obsolete epochs as non-authoritative once a rotation is accepted

This is not “admin enforcement.” It is deterministic cryptographic and contractual enforcement.

⸻

17. Extensibility rules (do not add new primitives)

PVP is extended by:
	•	new assertion types (typ)
	•	new schemas
	•	new contracts

PVP is not extended by adding new semantic object categories.

Transport messages may evolve, but they must not become a second semantic layer.

⸻

18. Minimal end-to-end flows (normative behavior, non-normative examples)

18.1 Create a subject (offline)
	1.	Generate subject_id (32 random bytes).
	2.	Choose schema artifact bytes (canonical CBOR), compute schema_id.
	3.	Choose contract artifact bytes, compute contract_id.
	4.	Generate subject epoch key K0, compute kid0.
	5.	Create core.genesis assertion plaintext with header {sub, typ=core.genesis, schema_id, contract_id, ...}, sign it.
	6.	Encrypt it under K0 into an assertion envelope (kid=kid0), compute its object_id.
	7.	Store envelope locally.

18.2 Invite another peer
	1.	Obtain recipient’s peer_id and hpke_pk (out-of-band or prior contact exchange).
	2.	Create Key Grant Package containing {subject_id, kid0, K0, genesis_id} encrypted to recipient via HPKE, signed by grantor.
	3.	Send it over any transport or store-and-forward.
	4.	Recipient stores it, decrypts, imports K0, fetches or receives genesis envelope, decrypts, replays.

18.3 Make an assertion (offline)
	1.	Derive current subject state by replaying accepted assertions.
	2.	Create new assertion with explicit typ and typed b.
	3.	Include refs to the assertions you are basing this on (and prev for your per-subject author chain).
	4.	Sign, encrypt under latest usable subject key epoch, store locally.
	5.	Sync later.

18.4 Rotate keys (revocation)
	1.	Generate new epoch key K1, compute kid1.
	2.	For each remaining member, HPKE-encrypt K1 to them.
	3.	Create and sign core.key.rotate assertion with wraps.
	4.	Encrypt it under K0 (kid0), store and sync.
	5.	New assertions SHOULD be encrypted under K1 once rotation is accepted by contract.

⸻

19. Security considerations (minimum)

19.1 Malicious peers
	•	Peers may publish invalid assertions.
	•	Deterministic validation + signatures ensure invalid assertions are rejected.
	•	DoS (flooding envelopes) is possible; mitigations are implementation-level (rate limits, storage quotas), but MUST NOT affect truth semantics.

19.2 Metadata leakage

Even with end-to-end encrypted payloads, envelopes reveal:
	•	kid (key id)
	•	object sizes
	•	exchange patterns

Mitigations:
	•	encrypted transport
	•	padding/batching (optional, outside truth semantics)
	•	store-and-forward relays that do not learn plaintext

19.3 Key compromise

If a peer’s identity key is compromised:
	•	attackers can sign assertions as them until key rotation is recognized by collaborators (represented as assertions in subjects where it matters)
	•	remediation is explicit: key rotation assertions, membership changes, and subject key rotation

⸻

Appendix A: CBOR/CDDL-style sketches (implementer aid)

These are sketches, not formal CDDL, but directly implementable.

A.1 Assertion plaintext

AssertionPlain = {
  "h": {
    "v": 1,
    "sub": bytes(32),
    "typ": tstr,
    "auth": bytes(32),
    "seq": uint,
    "prev": bytes(32) / null,
    "refs": [* bytes(32)],
    "ts": int / null,
    "schema": bytes(32),
    "contract": bytes(32),
    ? "note": tstr,
    ? "meta": map
  },
  "b": any,
  "sig": bytes
}

A.2 Assertion envelope

AssertionEnvelope = {
  "v": 1,
  "suite": 1,
  "kid": bytes(32),
  "nonce": bytes(12),
  "ct": bytes
}
object_id = sha256(canonical_cbor(AssertionEnvelope))

A.3 Key rotation body (inside plaintext assertion)

KeyRotateBody = {
  "from_kid": bytes(32),
  "to_kid": bytes(32),
  "wraps": [
    * { "rcpt": bytes(32), "hpke_ct": bytes }
  ],
  ? "reason": tstr / null
}


⸻

Appendix B: What an implementation MUST do to pass the “test of correctness”

A compliant peer MUST be able to:
	1.	Disconnect from the network indefinitely.
	2.	Replay decryptable assertions locally.
	3.	Produce an explanation of current derived state (contract-defined state + the accepted assertions that caused it).
	4.	Reconnect later, sync objects, and converge deterministically with other peers who have the same assertion set.

⸻

DHARMA REPL v1 — User Guide

A practical operator manual for home → enterprise → high assurance

1) The mental model (read this once)

DHARMA is not a server you query. It’s a truth machine you replay.
	•	Subjects are independent “truth spaces” (an invoice, a ledger, a case file).
	•	Assertions are append-only signed events: “I assert this happened.”
	•	Derived State is what you see: computed deterministically by replaying accepted assertions through a contract lens.
	•	Lenses (data_ver) are interpreters: the same subject may have multiple parallel interpretations (v1 vs v2).
	•	Pending means: “I can’t decide yet because something is missing.”
	•	Rejected means: “This is invalid; it will never be accepted under this lens.”

If you remember one thing:

Nothing is true unless it’s an accepted assertion under a chosen lens.

⸻

2) First launch: identity, safety, and readiness

2.1 Start the REPL

dh

You’ll see a banner. There are only three possible starting states:

A) UNINITIALIZED (fresh install)
You have no identity. You can browse public subjects if you have keys/links, but you can’t sign.

Do:

identity init julien

B) LOCKED (identity exists, key encrypted)
You can inspect some local decrypted subjects (if keys are stored), but you can’t sign new assertions.

Do:

identity unlock

C) UNLOCKED (ready)
You can sign actions and participate normally.

Check:

identity whoami
status

2.2 High-assurance default (recommended)

If you want the “CIA-grade” behavior even at home (safer):

:set profile highsec

This makes:
	•	dry-run the default
	•	commit requires explicit confirmation
	•	more verbose authority explanations
	•	fewer “helpful guesses”

⸻

3) Your first 10 commands (the core loop)

These ten are the heart of daily use:
	1.	subjects — see what you have locally
	2.	use <subject> — select the thing you’re working on
	3.	lens — see the active interpreter/version
	4.	state — show current derived truth
	5.	tail 20 — show recent accepted assertions
	6.	pending — show what can’t be decided yet
	7.	rejected — show what was invalid
	8.	why <field> — explain why a state value is true
	9.	dryrun action ... — simulate a mutation
	10.	commit action ... — actually append truth

If you only ever learn these, you’re functional.

⸻

4) Navigating subjects like a filesystem

4.1 List subjects

subjects
subjects recent
subjects mine

	•	subjects shows everything in your local store.
	•	recent is the default most people want.
	•	mine filters to subjects you authored or own.

4.2 Use a subject

use 7f3a...c012
pwd

pwd prints your current context (subject + lens + overlays).

4.3 Aliases (so normal humans don’t paste hex)

alias set home.ledger 7f3a...c012
alias set cmdv.ap.inbox 9b11...aa20
alias list
use home.ledger


⸻

5) Lenses (versioned interpreters)

5.1 What is a lens?

A lens is “which contract version interprets the data right now.”

Even if you don’t care about versioning today, a lens is how DHARMA stays stable for decades.

5.2 View and set lens

lens
lens list
lens set 1

5.3 When you change lens

You are not changing history. You are changing the interpretation.

Example:
	•	Lens 1 might compute a ledger total one way.
	•	Lens 2 might include a tax or new rules.

Use:

diff --lens 1 --lens 2


⸻

6) Inspecting truth

6.1 Derived state

state
state --json
state --at <tip_object_id>

	•	state uses snapshot + replay.
	•	--at time-travels (perfect for audits).

6.2 Timeline

tail 10
log 50
show <object_id>

	•	tail is compact summaries.
	•	log is verbose.
	•	show prints the full decoded assertion (header/body/signature status).

6.3 Status dashboard

status
status --verbose

Verbose should show:
	•	accepted/pending/rejected counts
	•	missing dependencies/artifacts
	•	missing lens versions
	•	frontier tips
	•	snapshot position

⸻

7) The “Explain” superpower (auditing & safety)

7.1 Why is this field true?

why status
why balance["Food"]
why invoice.total

A good why output includes:
	•	current value
	•	minimal proof chain of assertions
	•	authors + types
	•	links to inspect each assertion

7.2 Prove an assertion

prove <object_id>

This is your “truth debugger.” It must say:
	•	canonical CBOR OK?
	•	signature OK?
	•	dependencies present?
	•	schema validation OK?
	•	contract validation OK?
	•	accepted/pending/rejected and why

7.3 Diff state

diff --since "2026-01-01"
diff --at <tipA> <tipB>

Perfect for “what changed since last week?”

⸻

8) Acting safely: dry-run before commit

8.1 Dry-run an action

dryrun action Spend amount=4500 category="Groceries"

Dry-run MUST show:
	•	whether you’re authorized
	•	validate result (pass/fail + reason)
	•	a state diff preview
	•	any implied events (if the contract generates them)

8.2 Commit an action

commit action Spend amount=4500 category="Groceries"

In highsec profile you should see a transaction card:
	•	Subject
	•	Lens
	•	Action + args
	•	Authority proof (why you’re allowed)
	•	Expected state diff
	•	“Will write N assertions”
	•	Confirm: type yes

8.3 Emergency “I know what I’m doing”

commit --force action ...

Only available in pro profile.

⸻

9) Pending and rejected: what to do when things don’t work

9.1 Pending means “missing prerequisites”

Common causes:
	•	missing parent assertion (prev)
	•	missing referenced assertion (refs)
	•	missing schema artifact
	•	missing contract artifact
	•	missing lens version installed

Commands:

pending
prove <pending_id>
sync now

Workflow:
	1.	pending
	2.	prove <id> tells you what is missing
	3.	sync now tries to fetch it
	4.	pending again — it should resolve

9.2 Rejected means “never accepted under this lens”

Common causes:
	•	invalid signature
	•	schema mismatch (wrong field type)
	•	contract rule violated (e.g., paying invoice twice)

Commands:

rejected
prove <rejected_id>

Remedy depends:
	•	signature invalid: data is garbage/malicious/corrupt
	•	schema mismatch: you used wrong action args or wrong lens
	•	contract reject: you need a different sequence of actions

⸻

10) Overlays: public base + private extensions

This is how you can send a standard invoice to a third party without leaking internal PO IDs.

10.1 Overlay status

overlay status
overlay list

You might see:
	•	base-only: ✅
	•	overlays available but locked: 🔒
	•	overlays enabled: ✅ merged

10.2 A common workflow
	•	Outsiders see only: std.invoice.*
	•	Employees see: std.invoice.* + com.cmdv.invoice.*

10.3 Explaining overlay-derived state

why should annotate:
	•	value from base
	•	value from overlay
	•	merge rule

⸻

11) Peers and sync (day-to-day)

11.1 See peers

peers
peers --verbose

11.2 Sync now

sync now
sync subject
sync subject <id>

11.3 Discovery

discover status
discover on
discover off

Home profile: discovery ON by default.
Highsec: discovery OFF by default; manual connect or approved rendezvous only.

⸻

12) Packages (code) and installing the “rules of reality”

This matters once you start distributing stdlib and company logic.

12.1 List packages installed

pkg list
pkg show std.invoice

12.2 Install a package

pkg install std.invoice
pkg install com.ph.cmdv.invoice

12.3 Verify provenance

pkg verify std.invoice

This should show:
	•	publisher identity
	•	signature chain / trusted registry
	•	artifact hashes match

⸻

13) Search (once indexing exists)

Remember: indexes are derived, disposable.

13.1 Build index

index status
index build text
index build vector
index build graph

13.2 Search

find "invoice paid"
vfind "late deliveries last month"
gfind refs <object_id>

Every result should support:
	•	open
	•	why

⸻

14) Real scenario walkthroughs

Scenario A — Home finance: groceries + audit

use home.ledger
state
dryrun action Spend amount=4500 category="Groceries" note="S&R"
commit action Spend amount=4500 category="Groceries" note="S&R"
state
why balance["Groceries"]

Scenario B — Business: approve an invoice with strict rules

use cmdv.ap.invoice.2026.001
state
authority Approve
dryrun action Approve reason="Goods received"
commit action Approve reason="Goods received"
tail 10
why status

Scenario C — Incident response: why is something stuck pending?

use cmdv.case.incident.77
pending
prove <pending_id>
sync now
pending
status --verbose

Scenario D — Compare interpretations (lens 1 vs lens 2)

use home.ledger
diff --lens 1 --lens 2
lens set 2
state


⸻

15) High-assurance operating mode (CIA-style)

Turn it on:

:set profile highsec
:set confirmations on

Guidelines:
	•	Always dryrun before commit
	•	Use authority before actions
	•	Require dual control via contract (if enabled)
	•	Keep discovery off unless approved
	•	Use prove and why as standard steps

⸻

16) Help system

16.1 Built-in help

help
help action
help why
help profile

16.2 Command discovery

help should group commands by category and show examples.

⸻

17) Recommended onboarding path for users

For non-technical home users

Teach:
	•	use, state, dryrun action, commit action, why

Everything else is hidden behind menus/help.

For operators (enterprise)

Teach:
	•	plus prove, pending, diff, authority, pkg verify, sync subject


Appendix A — Full Command Reference (DHARMA REPL v1)

This appendix is the complete command reference for the interactive dh. Commands are grouped by category. For each command you’ll find:
	•	Syntax (with optional flags)
	•	Description
	•	Outputs (what it prints)
	•	Exit / error codes (where relevant)
	•	Examples

Conventions:
	•	<…> = required argument
	•	[…] = optional argument
	•	k=v = key/value argument (strings can be quoted)
	•	--json = machine output (canonical JSON)
	•	--raw = raw CBOR bytes shown as hex/base64 (implementation choice; must be stable)
	•	Object IDs / Subject IDs may be abbreviated (prefix), but REPL MUST disambiguate.

⸻

A.0 Global Meta-Commands

help

Syntax:
help
help <command>
help <category>

Description: Shows command list or detailed help.

Output (pretty):
	•	categories + brief summaries
	•	with <command>: syntax + examples

Examples:

help
help state
help identity


⸻

version

Syntax: version

Description: Prints REPL/runtime build info.

Output:
	•	REPL version
	•	runtime protocol version
	•	enabled features (compiler/repl/indexing)
	•	build hash

⸻

clear

Syntax: clear
Clears the screen.

⸻

exit / quit

Syntax: exit | quit
Exits REPL. If identity is unlocked, SHOULD lock/zeroize secrets.

⸻

:set

Syntax:
:set
:set <key> <value>

Keys (normative):
	•	profile = home|pro|highsec
	•	json = on|off
	•	color = on|off
	•	confirmations = on|off
	•	autosnapshot.every = <N> (integer)
	•	lens.default = <ver>
	•	pager = on|off
	•	time.format = iso|unix|human

Output: Current settings or “OK”.

Examples:

:set profile highsec
:set json on
:set autosnapshot.every 50


⸻

A.1 Identity Commands

identity status

Syntax: identity status [--json]

Description: Shows identity lifecycle status.

Output fields (normative):
	•	state: UNINITIALIZED|LOCKED|UNLOCKED|READONLY
	•	alias (if known)
	•	pubkey (hex)
	•	identity_subject (subject id if any)
	•	key_store_path

Example:

identity status


⸻

identity init

Syntax: identity init <alias> [--force]

Description: Creates a new identity keypair + identity subject; stores encrypted private key.

Interactive behavior (normative):
	•	prompt for passphrase twice
	•	confirm overwrite if identity exists unless --force

Output:
	•	new public key
	•	identity subject id
	•	storage locations

Errors:
	•	E_IDENTITY_EXISTS (unless --force)
	•	E_WEAK_PASSPHRASE (optional policy)

⸻

identity unlock

Syntax: identity unlock [--timeout <secs>] [--json]

Description: Unlocks identity private key into memory.

Interactive:
	•	prompts passphrase if not provided by env/agent

Output:
	•	OK + alias/pubkey
	•	optional timeout

Errors:
	•	E_BAD_PASSPHRASE
	•	E_UNINITIALIZED

⸻

identity lock

Syntax: identity lock

Description: Zeroizes and unloads private key.

Output: OK

⸻

identity whoami

Syntax: identity whoami [--json]

Description: Prints active identity details.

Output:
	•	alias
	•	pubkey
	•	identity subject id
	•	assurance level (optional): vouched|sovereign|hsm

⸻

identity export

Syntax: identity export [--format hex|mnemonic]

Description: Exports private key material (dangerous). MUST require:
	•	unlocked identity
	•	explicit confirmation prompt
	•	highsec profile MAY disable entirely

Output: Secret in selected format.

Errors: E_DISABLED_BY_POLICY

⸻

A.2 Subject Navigation

subjects

Syntax:
subjects [--json] [--limit <n>] [--sort recent|name|id]
subjects recent [--limit <n>]
subjects mine [--limit <n>]

Description: Lists known subjects in local store.

Output rows (pretty):
	•	subject id (short)
	•	alias (if any)
	•	last activity time
	•	accepted/pending counts (optional)
	•	keys available (yes/no)

⸻

use

Syntax: use <subject_id_or_alias> [--json]

Description: Sets current subject context.

Output: prints new context line (subject + lens).

Errors: E_SUBJECT_NOT_FOUND

⸻

pwd

Syntax: pwd [--json]

Description: Prints current context:
	•	subject id
	•	alias
	•	lens
	•	overlay status
	•	active package bindings

⸻

alias

Syntax:
alias set <name> <subject_id>
alias rm <name>
alias list [--json]

Output: OK or list.

⸻

subject create

Syntax: subject create [--type <typ>] [--title <text>] [--json]

Description: Creates a new subject with core.genesis using current default schema/contract (or a chosen pack). If your system requires explicit package choice, REPL SHOULD prompt.

Output:
	•	new subject id
	•	genesis object id

Errors: E_LOCKED if needs signing.

⸻

A.3 Lens / Versioning

lens

Syntax: lens [--json]

Description: Shows current lens settings for current subject.

Output fields:
	•	current lens id (data_ver)
	•	installed lenses available
	•	missing lenses referenced by data (if any)

⸻

lens list

Syntax: lens list [--json]

Description: Lists installed lenses for current subject/package.

⸻

lens set

Syntax: lens set <data_ver> [--json]

Description: Sets current lens for state/validation preview. MUST NOT rewrite history.

Errors:
	•	E_LENS_NOT_INSTALLED

⸻

A.4 State & History

state

Syntax:
state [--json] [--raw]
state --at <assertion_id>
state --lens <data_ver>

Description: Computes and displays derived state (snapshot + replay).

Output:
	•	pretty: formatted view of contract-defined state
	•	json: canonical JSON of state CBOR

Errors:
	•	E_NO_KEYS (cannot decrypt)
	•	E_LENS_NOT_INSTALLED
	•	E_CONTRACT_MISSING
	•	E_SCHEMA_MISSING

⸻

tail

Syntax: tail [n] [--json] [--accepted|--pending|--rejected]

Default: n=20, --accepted

Output columns (accepted):
	•	seq (author or logical)
	•	typ
	•	author (short)
	•	time claim
	•	object id (short)
	•	summary (contract-provided optional)

⸻

log

Syntax:
log [n] [--json] [--accepted|--pending|--rejected] [--since <time>] [--until <time>]

Verbose history with more header details.

⸻

show

Syntax: show <object_id> [--json] [--raw]

Displays decoded assertion (if decryptable):
	•	header fields
	•	body
	•	signature bytes (optional)
	•	schema/contract ids

If not decryptable:
	•	envelope metadata only.

Errors: E_OBJECT_NOT_FOUND

⸻

status

Syntax: status [--json] [--verbose]

Description: Subject health report.

Verbose includes:
	•	frontier tips
	•	snapshot status per lens
	•	missing deps/artifacts
	•	counts accepted/pending/rejected
	•	overlay status

⸻

A.5 Explain / Audit

why

Syntax: why <state_path> [--json]

Description: Explains why a state field has its value.

Output (normative):
	•	current value
	•	minimal proof chain: list of assertions that caused it
	•	each includes: object id, typ, author, refs
	•	optionally: derived rule (contract explanation string)

Errors: E_PATH_NOT_FOUND

Examples:

why status
why balance["Food"]


⸻

prove

Syntax: prove <object_id> [--json]

Description: Full validation report.

Output fields (normative):
	•	canonical decode: ok/fail
	•	signature verify: ok/fail
	•	deps present/missing
	•	schema validation: ok/fail + errors
	•	contract validation: accept/reject/pending + reason
	•	final status: ACCEPTED/PENDING/REJECTED

⸻

authority

Syntax:
authority <ActionName> [k=v ...] [--json]
authority typ <typ> [--json]

Description: Explains whether current identity is authorized, and why.

Output:
	•	allowed: true/false
	•	required capability/role/quorum
	•	evidence assertions (ids) proving grant/delegation
	•	failure reason if denied

⸻

diff

Syntax:
diff --at <tipA> <tipB> [--json]
diff --since <time> [--until <time>]
diff --lens <verA> --lens <verB>

Description: Shows state differences:
	•	between two tips
	•	between time ranges
	•	between lens interpretations

Output:
	•	pretty: added/removed/changed fields
	•	json: structured diff

⸻

A.6 Acting (Dry-run & Commit)

dryrun action

Syntax: dryrun action <ActionName> [k=v ...] [--json]

Description: Simulates validate+reduce without writing.

Output (normative):
	•	authority result
	•	validation result (pass/fail + reason)
	•	preview diff
	•	would-write count (0 or N if split assertions)
	•	notes about overlays/lens routing

Errors:
	•	E_UNAUTHORIZED
	•	E_SCHEMA_MISMATCH
	•	E_CONTRACT_REJECT

⸻

commit action

Syntax: commit action <ActionName> [k=v ...] [--json] [--force]

Description: Executes action and writes resulting assertion(s). --force bypasses confirmation (disabled in highsec).

Output:
	•	committed object_id(s)
	•	updated frontier
	•	snapshot update note (if created)

Errors:
	•	same as dryrun, plus storage errors:
	•	E_IO
	•	E_FSYNC (if policy requires)

⸻

dryrun emit

Syntax: dryrun emit <typ> <json_or_cbor> [--json]

Low-level simulation: bypasses action mapping and emits raw typ/body.

⸻

commit emit

Syntax: commit emit <typ> <json_or_cbor> [--json] [--force]

Writes a raw assertion.

⸻

tx

Syntax:
tx begin
tx show
tx commit
tx abort

Description: Optional transaction staging in REPL:
	•	stage multiple actions/emit
	•	then commit as a batch (still append-only, but can emit a “batch envelope” or sequential assertions)

Highsec profile MAY require tx for certain operations.

⸻

A.7 Overlays (Public/Base + Private Extensions)

overlay status

Syntax: overlay status [--json]

Shows:
	•	overlays present
	•	overlays enabled/disabled
	•	key availability (decryptable?)

⸻

overlay list

Syntax: overlay list [--json]

Lists overlay namespaces for current subject.

⸻

overlay enable

Syntax: overlay enable <namespace>

Enables merge of overlay state into derived state view.

Errors: E_NO_KEYS

⸻

overlay disable

Syntax: overlay disable <namespace>

⸻

overlay show

Syntax: overlay show <namespace> [--tail <n>]

Shows overlay assertion history for that namespace.

⸻

A.8 Peers & Sync

peers

Syntax: peers [--json] [--verbose]

Output fields:
	•	peer id (pubkey/subject)
	•	addr
	•	status (connected/disconnected)
	•	last_seen
	•	trust level (optional)
	•	relay role (optional)

⸻

peer trust

Syntax: peer trust <peer_id> <low|normal|high>

Stores local trust preference.

⸻

peer ban

Syntax: peer ban <peer_id> [--duration <secs>]

Client-side ban (stop syncing / drop connections).

⸻

sync now

Syntax: sync now [--json]

Triggers immediate INV exchange and sync cycle.

⸻

sync subject

Syntax: sync subject [<subject_id>]

If subject omitted, sync current subject only.

⸻

sync pause / sync resume

Pauses/resumes background sync loop (if daemon embedded).

⸻

connect

Syntax: connect <ip:port>

Manual connection from REPL (if not using dh connect outside).

⸻

listen

Syntax: listen [--port <p>]

Starts listening server (REPL embedded mode). Usually dh serve handles this; REPL may delegate.

⸻

discover

Syntax:
discover status
discover on
discover off

Controls UDP beacon discovery (LAN).

⸻

A.9 Packages / Code / Registry

pkg list

Syntax: pkg list [--json]

Lists installed packages:
	•	name
	•	available data versions
	•	artifact hashes
	•	trust status

⸻

pkg show

Syntax: pkg show <name> [--json]

Shows:
	•	all installed lenses (data_ver → schema+contract)
	•	dependencies pinned
	•	publishers/trust chain

⸻

pkg verify

Syntax: pkg verify <name> [--json]

Verifies:
	•	hashes
	•	publisher signatures
	•	dependency integrity

⸻

pkg install

Syntax: pkg install <name> [--from <registry_subject>]

Fetches from configured registry subject(s).

⸻

pkg pin

Syntax: pkg pin <name> <artifact_hash>

Pins to exact artifact hash (high assurance).

⸻

pkg remove

Syntax: pkg remove <name> [--keep-cache]

Removes active mapping; may keep artifacts cached.

⸻

A.10 Indexing & Search (Derived Views)

index status

Syntax: index status [--json]

Shows:
	•	enabled indexes (text/vector/graph)
	•	last build time
	•	size
	•	coverage (subjects indexed)

⸻

index build

Syntax:
index build text [--scope current|all]
index build vector [--scope current|all] [--model <name>]
index build graph [--scope current|all]

Builds derived indexes.

⸻

index drop

Syntax: index drop <text|vector|graph> [--scope current|all]

Drops derived index.

⸻

find

Syntax: find "<query>" [--limit <n>] [--json]

Full-text search across indexed data.

⸻

vfind

Syntax: vfind "<query>" [--limit <n>] [--json]

Vector semantic search.

⸻

gfind

Syntax:
gfind refs <object_id>
gfind deps <object_id>
gfind path <a> <b>

Graph/provenance navigation.

⸻

open

Syntax: open <result_id_or_object_id>

Opens item in a detailed view (like show, but friendlier).

⸻

A.11 Diagnostics & Maintenance

check

Syntax: check [--deep] [--json]

Runs local consistency checks:
	•	canonical CBOR checks (deep)
	•	missing objects referenced
	•	frontier correctness
	•	snapshot consistency

⸻

gc

Syntax: gc [--policy <name>] [--dryrun]

Garbage collection of derived artifacts and optionally old objects under policy constraints. MUST NOT delete required objects silently; highsec profile typically disables destructive GC unless retention allows.

⸻

snapshot

Syntax:
snapshot list [--lens <ver>]
snapshot make [--lens <ver>]
snapshot prune [--keep <n>]

⸻

export

Syntax:
export subject <subject_id> --out <file>
export proof <object_id> --out <file>

Exports portable bundles (useful for air-gapped transfer).

⸻

import

Syntax: import <file>

Imports object bundles.

⸻

A.12 Output formats (normative)

Every command that supports --json MUST output a single JSON object with:
	•	ok: boolean
	•	result: payload (object/array)
	•	warnings: optional array
	•	error: optional object { code, message, detail }

Example (prove):

{
  "ok": true,
  "result": {
    "object_id": "9af3...",
    "canonical": "ok",
    "signature": "ok",
    "deps_missing": [],
    "schema": { "status": "ok" },
    "contract": { "status": "accept", "reason": null },
    "final": "ACCEPTED"
  }
}

Pretty output may be richer but MUST be derivable from the JSON payload.

⸻

A.13 Standard error codes (minimum set)
	•	E_UNINITIALIZED
	•	E_LOCKED
	•	E_BAD_PASSPHRASE
	•	E_NO_KEYS
	•	E_SUBJECT_NOT_FOUND
	•	E_OBJECT_NOT_FOUND
	•	E_CANONICAL_FAIL
	•	E_SIG_INVALID
	•	E_MISSING_DEP
	•	E_SCHEMA_MISSING
	•	E_CONTRACT_MISSING
	•	E_VERSION_UNSUPPORTED
	•	E_UNAUTHORIZED
	•	E_CONTRACT_REJECT
	•	E_IO
	•	E_POLICY_DENY
	•	E_DISABLED_BY_PROFILE


# Golden Transcripts

Golden Transcript 01 — First Run + Identity Init + First Subject + First Action (Home)

$ dh

DHARMA REPL v1.0
Kernel: DHARMA Runtime v1  Suite: 1  Storage: filesystem (append-only)
Data dir: ./data
Profile: home  JSON: off  Color: on
Status: UNINITIALIZED (no identity found)
Tip: run `identity init <alias>` to create your identity.

anon@dh [no-subject] [lens:?] [peers:0] >

help
Commands (home):
  identity init|unlock|whoami|status
  subjects|use|alias
  state|tail|pending|rejected|show
  dryrun action ... | commit action ...
  why|prove|diff
  peers|sync now
  :set profile|json|color
Type: help <command> for details.

anon@dh [no-subject] [lens:?] [peers:0] >

identity init julien
Creating identity:
  alias: julien
  key type: Ed25519 (Suite 1)
Enter passphrase: ********
Confirm passphrase: ********

✅ Identity created.
  alias: julien
  pubkey: ed25519: 8b4d9c2f0f9a1d7e6a19e7b3b5f3c8d4c2a1e9f0b7d2a3c4e5f6a7b8c9d0e1f2
  identity subject: 3f6a4b1c9d0e2f3a4b5c6d7e8f90123456789abcdeffedcba9876543210a1b2c3
Stored:
  ./data/identity.key (encrypted)
  ./data/subjects/3f6a.../objects (created)

julien@dh [no-subject] [lens:1] [peers:0] >

identity whoami
Identity:
  alias: julien
  pubkey: 8b4d…e1f2
  identity subject: 3f6a…b2c3
  state: UNLOCKED
  assurance: sovereign (local key)

julien@dh [no-subject] [lens:1] [peers:0] >

subjects
No subjects found (besides identity subject).
Tip: create one with `subject create` (or install a pack).

julien@dh [no-subject] [lens:1] [peers:0] >

subject create --type std.home.ledger --title "Home Ledger"
Creating subject...
  subject_id: 9a7c1e2f3b4c5d6e7f80112233445566778899aabbccddeeff00112233445566
  genesis: core.genesis
  schema: std.home.ledger@lens=1 (installed)
  contract: std.home.ledger@lens=1 (installed)

✅ Subject created.
  subject: 9a7c…5566
  genesis object: 0c12a7…9f88

Context switched.
julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:0] >

state
Home Ledger (lens 1)
--------------------------------
currency: PHP
accounts:
  Cash:            0.00
  Bank:            0.00
  Food:            0.00
  Utilities:       0.00
  Entertainment:   0.00
recent:
  (none)

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:0] >

dryrun action Income amount=250000 account="Bank" note="Salary"
Dry-run: Income
  subject: 9a7c…5566
  lens: 1
  signer: julien (8b4d…e1f2)
  authorization: ✅ allowed (owner)

Schema check: ✅ ok
Contract validate: ✅ accept
Would write: 1 assertion
Preview state diff:
  accounts.Bank: 0.00  -> 250000.00
  recent += Income(amount=250000, account=Bank)

No changes committed.

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:0] >

commit action Income amount=250000 account="Bank" note="Salary"
Commit: Income
✅ Committed.
  object: 14be33…c901
  typ: std.home.ledger.income
  refs: []
  ts: 1736923105000000 (claim)

Snapshot policy: every 50 commits → not due (seq=1).
Broadcast: peers=0 (no sync)

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:0] >

state
Home Ledger (lens 1)
--------------------------------
currency: PHP
accounts:
  Cash:            0.00
  Bank:       250000.00
  Food:            0.00
  Utilities:       0.00
  Entertainment:   0.00
recent:
  1) Income  PHP 250000.00  -> Bank  ("Salary")

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:0] >

why accounts.Bank
WHY accounts.Bank = 250000.00
Proof chain (minimal):
  1) 14be33…c901  std.home.ledger.income
     author: julien (8b4d…e1f2)
     body: { amount: 250000, account: "Bank", note: "Salary" }
     accepted by: contract std.home.ledger@lens=1
To inspect: show 14be33…c901


⸻

Golden Transcript 02 — Two Devices Sync + Seeing Replication Happen

# Terminal A (Laptop)
$ dh
DHARMA REPL v1.0  Profile: home
Status: UNLOCKED (julien)
Discovery: ON (LAN beacon)
Listening: tcp://0.0.0.0:3000

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:0] >

peers
No peers connected.

# Terminal B (Desktop)
$ dh
DHARMA REPL v1.0  Profile: home
Status: UNLOCKED (julien)
Discovery: ON (LAN beacon)
Listening: tcp://0.0.0.0:3000

julien@dh [no-subject] [lens:1] [peers:0] >

discover status
Discovery: ON
Beacon: udp://0.0.0.0:3001 broadcast every 3s
Known LAN peers: 0

# A few seconds pass…

[DISCOVER] Found peer signal:
  addr: 192.168.1.23:3000
  peer_id: 8b4d…e1f2 (julien)
  note: same identity detected; treating as “my device”
[SYNC] Auto-connecting to 192.168.1.23:3000 …

[NET] Handshake: OK (suite 1)
[SYNC] Exchanging INV/GET/OBJ …
[SYNC] Completed:
  objects received: 148
  subjects updated: 2
  pending resolved: 0

julien@dh [no-subject] [lens:1] [peers:1] >

subjects recent
1) home.ledger        9a7c…5566   updated: 10s ago
2) identity.julien    3f6a…b2c3   updated: 1m ago

use home.ledger
Context switched: sub 9a7c…5566

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:1] >

state
Home Ledger (lens 1)
--------------------------------
currency: PHP
accounts:
  Bank: 250000.00
recent:
  1) Income  PHP 250000.00  -> Bank  ("Salary")

tail 5
ACCEPTED (last 1)
  #1  14be33…c901  std.home.ledger.income   author: julien  note:"Salary"


⸻

Golden Transcript 03 — Pending Dependencies + Auto-Resolution via Sync

$ dh
DHARMA REPL v1.0  Profile: pro
Status: UNLOCKED (julien)
Listening: tcp://0.0.0.0:3000

julien@dh [sub:cmdv.case.77=aa11…ff90] [lens:1] [peers:2] >

status --verbose
Subject: cmdv.case.77 (aa11…ff90)
Lens: 1
Accepted: 42
Pending: 3
Rejected: 1
Frontier tips: 2
Snapshots: present (lens 1 at seq=40 tip=bb02…19a0)
Missing deps:
  - 77c9…e012 (prev missing for 9f10…aa33)
  - schema artifact: 55aa…beef (needed by 1c2d…3e4f)

pending
PENDING assertions (3)
1) 9f10…aa33  typ=std.case.note.add
   reason: E_MISSING_DEP prev=77c9…e012
2) 1c2d…3e4f  typ=std.case.evidence.link
   reason: E_SCHEMA_MISSING schema=55aa…beef
3) 44dd…1122  typ=std.case.task.assign
   reason: E_MISSING_DEP ref=0ab1…cafe

prove 1c2d…3e4f
PROVE 1c2d…3e4f
  canonical: ✅ ok
  signature: ✅ ok (auth=1b22…c0de)
  deps: ✅ ok
  schema: ❌ missing artifact 55aa…beef
  contract: ⏸ pending (schema missing)
  final: PENDING
Next steps:
  - sync now
  - or install package containing schema 55aa…beef

sync now
[SYNC] Starting…
[SYNC] INV received from peer 71.56.10.9:3000
[SYNC] GET queued: 77c9…e012, 55aa…beef, 0ab1…cafe
[SYNC] OBJ received: 3
[INGEST] 77c9…e012 → committed
[INGEST] 55aa…beef (artifact) → stored
[INGEST] 0ab1…cafe → committed
[REPLAY] Re-evaluating pending set…
  1c2d…3e4f: schema now present → ACCEPTED
  9f10…aa33: prev now present → ACCEPTED
  44dd…1122: ref now present → ACCEPTED
[SYNC] Completed.

pending
No pending assertions.

status
Subject: cmdv.case.77
Accepted: 45  Pending: 0  Rejected: 1  Tips: 1


⸻

Golden Transcript 04 — Highsec Workflow: Dry-run → Transaction Card → Commit → Audit Proof

$ dh
DHARMA REPL v1.0  Profile: highsec
Status: LOCKED (identity present)
Policy: fail-closed, confirmations required, discovery OFF
Tip: run `identity unlock`

anon@dh [no-subject] [lens:1] [peers:0] >

identity unlock
Enter passphrase: ********
✅ Identity unlocked: julien (8b4d…e1f2)

julien@dh [no-subject] [lens:1] [peers:0] >

use cmdv.ap.invoice.2026.001
Context switched: sub 0f0f…d00d (alias: cmdv.ap.invoice.2026.001)

julien@dh [sub:cmdv.ap.invoice.2026.001=0f0f…d00d] [lens:1] [peers:0] >

state
Invoice (lens 1)
--------------------------------
id: INV-2026-001
vendor: SAFF
amount: PHP 889.00
status: PendingApproval
approvals:
  - requested_by: Alfred
  - required: Accountant + OpsManager
evidence:
  - pdf: present
  - delivery_receipt: present

authority Approve role="Accountant"
Authority check: Approve(role=Accountant)
  allowed: ✅ yes
  basis:
    - role.assign(Accountant) -> julien (assertion 77aa…1001)
    - subject policy: requires Accountant + OpsManager (contract rule)
  note: this action will still require OpsManager co-approval for final status change.

dryrun action Approve role="Accountant" reason="Verified vendor billing"
Dry-run: Approve
  schema: ✅ ok
  contract.validate: ✅ accept
  effect: “Accountant approval recorded”
Preview diff:
  approvals += { role: Accountant, by: julien, reason: "Verified vendor billing" }
  status remains: PendingApproval (awaiting OpsManager)

No changes committed.

commit action Approve role="Accountant" reason="Verified vendor billing"
────────────────────────────────────────────────────────
HIGHSEC TRANSACTION CARD (REVIEW REQUIRED)
Subject:    cmdv.ap.invoice.2026.001 (0f0f…d00d)
Lens:       1
Signer:     julien (8b4d…e1f2)
Action:     Approve(role=Accountant)
Args:       reason="Verified vendor billing"
Authority:  ✅ role.assign(Accountant) (77aa…1001)
Policy:     Requires co-approval OpsManager to finalize
Writes:     1 assertion
Preview:
  approvals += Accountant(julien)
  status unchanged (awaiting OpsManager)
Type `yes` to commit, anything else to abort:
────────────────────────────────────────────────────────
> yes

✅ Committed.
  object: a1b2…c3d4
  typ: cmdv.invoice.approval.add
  refs: [invoice.request…]
Snapshot: not due (seq=17)

prove a1b2…c3d4
PROVE a1b2…c3d4
  canonical: ✅ ok
  signature: ✅ ok (auth=julien)
  deps: ✅ ok
  schema: ✅ ok
  contract: ✅ accept
  final: ACCEPTED

why status
WHY status = PendingApproval
Proof chain (minimal):
  1) invoice.create … (accepted)
  2) approval.request … (accepted)
  3) approval.add(role=Accountant) a1b2…c3d4 (accepted)
Contract explanation:
  “Final status becomes Approved only after roles Accountant + OpsManager both present.”


⸻

Golden Transcript 05 — Overlays: Sending Standard Invoice to 3rd Party Without Private Fields

This shows the base+overlay concept and how the REPL makes it obvious.

$ dh
DHARMA REPL v1.0  Profile: pro
Status: UNLOCKED (julien)

julien@dh [sub:cmdv.invoice.2026.001=0f0f…d00d] [lens:1] [peers:1] >

overlay status
Overlay model: ENABLED
Base namespace: std.invoice
Overlays present (1):
  - com.ph.cmdv.invoice   (decryptable ✅)  chain: 5 assertions
Sharing policy:
  - base assertions MAY be shared to external peers if allowed
  - overlay assertions NEVER shared unless peer has overlay keys

state
Invoice (merged view)
--------------------------------
std.invoice:
  id: INV-2026-001
  vendor: SAFF
  amount: PHP 889.00
  status: PendingApproval
com.ph.cmdv.invoice (private overlay):
  internal_po_id: PO-8137-2026
  department: OPS
  margin_bucket: “thin”
  notes_internal: “match clearing fees”

# External peer (supplier) connects, they only subscribe to std.invoice
peers --verbose
Peers (1)
1) peer: saff.fr (pubkey 99aa…77cc)
   addr: 81.12.4.9:3000
   permissions: base-only (std.invoice)  overlay: denied (no keys)
   sync mode: active

sync subject
[SYNC] Subject cmdv.invoice.2026.001
[SYNC] Applying disclosure policy:
  sending base: 18 objects
  sending overlays: 0 objects (peer lacks com.ph.cmdv.invoice keys)
[SYNC] Completed.

# Now we inspect what the external peer would see (REPL simulation)
export proof subject 0f0f…d00d --as external --out /tmp/inv_export.dharmabundle
Export created:
  mode: external(base-only)
  objects: 18
  overlays: 0
  file: /tmp/inv_export.dharmabundle

# Sanity: show that PO id is not in exported view
open /tmp/inv_export.dharmabundle
Bundle view (external/base-only):
  std.invoice fields present ✅
  com.ph.cmdv.invoice fields present ❌
  integrity: ✅ signatures intact


⸻

Golden Transcript 06 — Multi-Lens Versioning: Same Subject, Two Interpretations + A/B Emissions

$ dh
DHARMA REPL v1.0  Profile: pro
Status: UNLOCKED (julien)

julien@dh [sub:home.ledger=9a7c…5566] [lens:1] [peers:1] >

pkg list
Installed packages:
  std.home.ledger
    lenses: 1, 2
    note: lens 2 introduces “tax buckets” and auto-rounding rules

lens
Current lens: 1
Available: [1,2]

state
Home Ledger (lens 1)
accounts:
  Bank: 250000.00
  Food: 0.00
recent:
  Income Bank 250000.00

# Switch interpretation without changing history
lens set 2
Lens set: 2

state
Home Ledger (lens 2)
accounts:
  Bank: 250000.00
  Food: 0.00
tax_buckets:
  VAT_estimate: 0.00
rounding:
  policy: “cash rounding”
recent:
  Income Bank 250000.00

diff --lens 1 --lens 2
State diff (lens 1 → lens 2)
+ tax_buckets.VAT_estimate = 0.00
+ rounding.policy = "cash rounding"
(no changes to balances)

# Emit a v2 event (data carries version)
dryrun action Spend amount=999.99 category="Food" --data_ver 2
Dry-run: Spend (data_ver=2)
Contract validate: ✅ accept
Preview:
  Food: 0.00 -> 999.99
  VAT_estimate: 0.00 -> 119.99
  rounding applied: (none)
Would write: 1 assertion ver=2

commit action Spend amount=999.99 category="Food" --data_ver 2
✅ Committed object: 55ff…aa11  ver=2

# Now compare how lens 1 sees it
lens set 1
state
Home Ledger (lens 1)
accounts:
  Bank: 250000.00
  Food: 999.99
recent:
  Income …
  Spend Food 999.99

lens set 2
state
Home Ledger (lens 2)
accounts:
  Bank: 250000.00
  Food: 999.99
tax_buckets:
  VAT_estimate: 119.99
recent:
  Income …
  Spend Food 999.99 (vat tracked)


⸻

Golden Transcript 07 — Search + Vector Search + Proofable Results (Derived Indexes)

$ dh
DHARMA REPL v1.0
Status: UNLOCKED (julien)

julien@dh [no-subject] [lens:1] [peers:2] >

index status
Indexes:
  text:   ❌ not built
  vector: ❌ not built
  graph:  ✅ built (refs/deps)

index build text --scope all
Building text index…
  subjects scanned: 48
  decryptable assertions: 12,440
  artifacts included: 220
✅ text index built.
  size: 83.2MB
  build time: 12.4s

index build vector --scope all --model mini-embeddings-v1
Building vector index…
  fields embedded: note.text, invoice.memo, case.summary
  subjects scanned: 48
✅ vector index built.
  vectors: 120,110
  size: 312MB
  build time: 38.9s

find "underpayment 9 pesos"
Results (text) [3]
1) subject: cmdv.ap.invoice.2026.001  obj: a1b2…c3d4  snippet: "remaining 9 pesos..."
2) subject: home.ledger               obj: 91aa…ee02  snippet: "paid later..."
3) subject: case.payments             obj: 11bb…77aa  snippet: "underpayment…"

open 1
Object a1b2…c3d4
typ: cmdv.invoice.approval.add
body: { reason: "Verified vendor billing", note: "remaining 9 pesos ..." }

why reason
WHY reason = "Verified vendor billing"
Proof chain:
  a1b2…c3d4 (this assertion)
(Direct field from body; accepted by contract.)

vfind "payments that were short and later corrected"
Results (vector) [2]
1) subject: case.payments  obj: 11bb…77aa  score: 0.83  summary: "short payment corrected later"
2) subject: home.ledger    obj: 91aa…ee02  score: 0.78  summary: "settled difference afterward"

open 1
show 11bb…77aa
... (prints decoded assertion) ...


⸻

Golden Transcript 08 — Air-Gapped Export/Import + Verify (High Assurance)

$ dh
DHARMA REPL v1.0  Profile: highsec
Status: UNLOCKED (julien)
Discovery: OFF
Network: OFF (airgap mode)

julien@dh [sub:cmdv.case.incident.77=aa11…ff90] [lens:1] [peers:0] >

status
Subject: cmdv.case.incident.77
Accepted: 45  Pending: 0  Rejected: 1
Note: airgap mode → no sync

export subject aa11…ff90 --out /media/usb/incident77.dharmabundle
Preparing export bundle…
Policy: highsec
  include: base assertions + required artifacts + latest snapshot
  exclude: overlays not requested
  include proofs: YES
✅ Export written:
  file: /media/usb/incident77.dharmabundle
  objects: 2,140
  snapshot: lens=1 seq=40
  manifest signature: ✅ (julien)

# Move USB to another machine…

$ dh
DHARMA REPL v1.0  Profile: highsec
Status: UNINITIALIZED (read-only inspection allowed)
Network: OFF

anon@dh [no-subject] [lens:1] [peers:0] >

import /media/usb/incident77.dharmabundle
Importing…
  verifying manifest signature… ✅ ok
  checking object hashes… ✅ ok
  writing objects… ✅ ok
✅ Imported:
  objects added: 2,140
  subjects added: 1

subjects
1) cmdv.case.incident.77  aa11…ff90  updated: just now

use aa11…ff90
state
Incident Case (lens 1)
--------------------------------
severity: HIGH
status: CONTAINED
timeline:
  - initial report …
  - mitigation …
  - closure pending …

prove <some_object_id>
PROVE …
  signature: ✅ ok
  final: ACCEPTED


# DHARMA v1: The Sovereign Truth Kernel

**DHARMA (Peer Assertions for Commitments & Truth)** is a protocol and runtime for creating, sharing, and auditing **signed, typed, immutable facts** without central servers.

It solves the problem of **"Who agreed to what, when, and under what rules?"** in a distributed, offline-first world.

---

## 1. The Core Problem

Modern collaboration is broken because **Truth is trapped in Silos**.
*   Tasks live in project management tools owned by vendors.
*   Financial records live in banking databases owned by institutions.
*   Decisions live in chat logs owned by platforms.

If the server goes down, work stops. If the admin account is compromised, history can be rewritten. Integrating a Task in one system with a Payment in another requires fragile API glue.

**DHARMA replaces "Database Rows" with "Cryptographic Assertions".**
Instead of updating a mutable row in a database, a user signs a statement: *"I assert that Task 123 is Done."*
This statement is append-only, portable, and verifiable by anyone possessing the corresponding public key.

---

## 2. What DHARMA Is

### A. The "Git for Truth"
Just as Git tracks source code history, DHARMA tracks **State History**.
*   **Subject:** A document (Invoice, Task, Identity), identified by a stable random ID.
*   **Assertion:** An atomic update (Created, Approved, Paid), signed by an identity.
*   **Head:** The current state, derived deterministically by replaying valid assertions in order.

### B. A "Local-First" Database
*   **Offline by Default:** You can read and write assertions anywhere, regardless of connectivity.
*   **Sync:** When peers (or relays) connect, they exchange assertions. The state converges mathematically without a central coordinator.
*   **Zero Admin:** No table creation, no permission grants. Access control is cryptographic.

### C. A "Contract" Engine
*   **DHL (Literate DHARMA Domain Law):** Logic is defined in readable Markdown files.
    *   *Rule:* "Only the Assignee can mark a Task as Done."
    *   *Rule:* "An Invoice cannot be paid twice."
*   **Enforcement:** The DHARMA Runtime rejects any assertion that violates the contract. The contract itself is content-addressed and immutable.

---

## 3. Key Use Cases

### 1. The Sovereign Professional
*   **Scenario:** Billing clients, tracking hours, and managing projects independently.
*   **With DHARMA:** Create an `Invoice` subject. Assert `LineItem`. Send the **Subject ID** to the client. The client asserts `Approved`. You assert `Paid`.
*   **Value:** Elimination of SaaS subscriptions. A perfect, portable audit trail. Complete data ownership.

### 2. Secure "Darknet" Collaboration
*   **Scenario:** A remote team requires secure collaboration (Tasks, Wikis, Decisions) without metadata leakage to cloud providers.
*   **With DHARMA:** The team runs a private **Relay**. Employees connect via the DHARMA-FABRIC protocol. All data is end-to-end encrypted.
*   **Value:** "Signal-level" privacy applied to business logic and state.

### 3. Supply Chain Provenance
*   **Scenario:** Tracking a product from origin to consumer.
*   **With DHARMA:** The Producer asserts `Created`. The Logistics Provider asserts `Transported`. The Retailer asserts `Received`.
*   **Value:** Consumers scan a QR code to replay the history. The provenance is mathematically unforgeable and independent of any single vendor's database.

---

## 4. What DHARMA is NOT

*   **It is NOT a Blockchain.**
    *   There is no global consensus (PoW/PoS).
    *   There is no global ledger state.
    *   Agreement is scoped to the participants of a Subject.
    *   *Subjectivity:* Different peers can hold different subsets of truth until they sync.

*   **It is NOT a Chat App.**
    *   While possible, DHARMA is heavyweight for ephemeral messages ("Hey, how are you?").
    *   DHARMA is designed for **Commitments**: durable state that must be preserved, audited, and acted upon.

*   **It is NOT a File System.**
    *   Assertions store metadata and state, not large binary blobs.
    *   Large assets (images, videos) should be stored in content-addressed stores (IPFS, S3) and referenced by hash within DHARMA assertions.

---

## 5. The Architecture (V1)

1.  **Truth Plane (The Kernel):**
    *   Handles Signatures (Ed25519), Encryption (ChaCha20), and Storage (`log.bin`).
    *   Designed to be a <1MB binary that runs anywhere.

2.  **Availability Plane (The Fabric):**
    *   **Relays:** Simple servers that store and forward encrypted envelopes without seeing the content.
    *   **Discovery:** A directory system to locate where data is hosted.

3.  **Query Plane (DHARMA-Q):**
    *   A high-speed columnar engine designed to answer complex questions ("Show me all unpaid invoices due next week").
    *   It rebuilds its state entirely from the Truth Plane, making it disposable and cacheable.

---

## 6. Design Philosophy

*   **Simplicity:** Avoiding complex consensus algorithms like Raft or Paxos in favor of append-only logs and Merkle reconciliation.
*   **Scalability:** Sharding is natural and inherent, as every Subject is an independent ledger.
*   **Safety:** "Compute Never Commits." Only signed facts are committed. Time, randomness, and external data are treated as inputs to the log, not side effects of replay.

**DHARMA is the missing layer of the internet: The Protocol for Shared Reality.**

# DHARMA Language Specification: The Unified Logic Layer

This document specifies the three interlocking languages of the DHARMA ecosystem:
1.  **DHARMA CEL (Common Expression Language):** The shared core for logic, types, and values.
2.  **DHL (Literate DHARMA Domain Law):** The structure definition language (Contracts).
3.  **DHARMA-Q (Query Language):** The data projection and retrieval language.

---

## 1. DHARMA CEL (Common Expression Language)

CEL is the safe, deterministic, side-effect-free expression language used inside both DHL (validation/assignments) and DHARMA-Q (filters/projections).

### 1.1 Principles
-   **No Floats:** Only `Int` (i64), `Decimal` (fixed-point), or `Ratio`.
-   **No Loops:** Guaranteed termination.
-   **Strict Typing:** No implicit coercion.

### 1.2 Types
| Type | Keyword | Example |
| :--- | :--- | :--- |
| Integer | `Int` | `42`, `-100` |
| Boolean | `Bool` | `true`, `false` |
| Text | `Text` | `"hello world"` |
| Identity | `Identity` | `id(0xabc...)` |
| Reference | `Ref<T>` | `ref(0x123...)` |
| List | `List<T>` | `[1, 2, 3]` |
| Map | `Map<K,V>` | `{"a": 1, "b": 2}` |
| Enum | `Enum` | `'Pending`, `'Approved` |

### 1.3 Operators
| Category | Ops | Usage |
| :--- | :--- | :--- |
| Arithmetic | `+`, `-`, `*`, `/`, `%` | `a + b` |
| Comparison | `==`, `!=`, `>`, `<`, `>=`, `<=` | `a >= 10` |
| Logic | `and`, `or`, `not` | `a and (b or not c)` |
| Set | `in` | `'Pending in status_list` |
| Path | `.` | `invoice.lines[0].amount` |

### 1.4 Built-in Functions
-   `len(List|Text|Map) -> Int`
-   `contains(List|Map, Item) -> Bool`
-   `has_role(Identity, Text) -> Bool` (RBAC check)
-   `now() -> Int` (Context-dependent: Block time in DHL, Query time in DHARMA-Q)
-   `sum(List<Int>) -> Int`

---

## 2. DHL (Literate DHARMA Domain Law)

DHL defines **State**, **Transitions**, and **Invariants**. It is embedded in Markdown code blocks.

### 2.1 Structure
An DHL block can define:
-   `aggregate`: The data model.
-   `action`: A transaction type.
-   `flow`: (Optional) State machine visualizer and validator.
-   `reactor`: An event handler.

### 2.2 Syntax Guide

#### Aggregate
```dhl
aggregate Invoice extends std.finance.Base
    state
        public amount: Int
        public status: Enum(Draft, Sent, Paid) = 'Draft
        private internal_notes: Text
```

#### Flow (BPM)
```dhl
flow Lifecycle
    'Draft -> [Send] -> 'Sent
    'Sent -> [Approve] -> 'Paid
    'Sent -> [Reject] -> 'Draft
```
*Note: A flow block automatically generates the corresponding `validate` and `apply` logic for the referenced actions.*

#### Action
```dhl
action Send(recipient: Identity)
    validate
        state.status == 'Draft
        state.amount > 0
        has_role(context.signer, "Sales")
    
    apply
        state.status = 'Sent
        state.recipient = recipient
        state.sent_at = now()
```

#### Reactor
```dhl
reactor OnPayment
    trigger action.Payment.Receive
    validate trigger.state.invoice_id == state.id
    emit action.Invoice.MarkPaid()
```

---

## 3. DHARMA-Q (Query Language)

DHARMA-Q is a **Pipeline Language**. It starts with a source (table) and flows data through operators (`|`).

### 3.1 Syntax Guide

**Basic Pipeline:**
```dhlq
invoice | where status == 'Paid' | sort -amount | take 10
```

**Joins:**
```dhlq
invoice 
| where status == 'Open'
| join customer on invoice.cust_id == customer.id
| select invoice.id, customer.name, invoice.amount
```

**Aggregations:**
```dhlq
invoice
| where date > '2023-01-01'
| by customer_id
| agg total = sum(amount), count = count()
| sort -total
```

**Search:**
```dhlq
search "cheese" in product.name fuzz=1 | take 5
```

---

## 4. Formal Grammar (EBNF)

This grammar unifies CEL, DHL, and DHARMA-Q.

```ebnf
/* --- Top Level --- */
LpdlBlock   ::= (AggregateDef | ActionDef | FlowDef | ReactorDef)*
Query       ::= TableSource ( "|" PipeOp )*

/* --- DHARMA CEL (Common Expression Language) --- */
Expr        ::= LogicOr
LogicOr     ::= LogicAnd ( "or" LogicAnd )*
LogicAnd    ::= Equality ( "and" Equality )*
Equality    ::= Comparison ( ( "==" | "!=" | "in" ) Comparison )*
Comparison  ::= Term ( ( ">" | "<" | ">=" | "<=" ) Term )*
Term        ::= Factor ( ( "+" | "-" ) Factor )*
Factor      ::= Unary ( ( "*" | "/" | "%" ) Unary )*
Unary       ::= ( "-" | "not" )? Atom
Atom        ::= Literal | Path | FunctionCall | "(" Expr ")"

Path        ::= Identifier ( "." Identifier | "[" Expr "]" )*
FunctionCall::= Identifier "(" ( Expr ( "," Expr )* )? ")"
Literal     ::= IntLit | StringLit | BoolLit | EnumLit | ListLit | MapLit

/* --- DHL Definitions --- */
AggregateDef::= "aggregate" Identifier ( "extends" Path )? 
                "state" ( FieldDef )*
FieldDef    ::= ( "public" | "private" )? Identifier ":" TypeSpec ( "=" Literal )?

FlowDef     ::= "flow" Identifier ( Transition )*
Transition  ::= EnumLit "->" "[" Identifier "]" "->" EnumLit

ActionDef   ::= "action" Identifier "(" ArgList? ")" 
                ( "validate" Expr* )? 
                ( "apply" Assignment* )?
ArgList     ::= ArgDef ( "," ArgDef )*
ArgDef      ::= Identifier ":" TypeSpec
Assignment  ::= Path "=" Expr

ReactorDef  ::= "reactor" Identifier 
                "trigger" Path 
                ( "validate" Expr* )?
                "emit" Path "(" AssignmentList? ")"

TypeSpec    ::= "Int" | "Bool" | "Text" | "Identity" 
              | "List" "<" TypeSpec ">" 
              | "Map" "<" TypeSpec "," TypeSpec ">"
              | "Ref" "<" Identifier ">"
              | "Enum" "(" Identifier ( "," Identifier )* ")"

/* --- DHARMA-Q Operators --- */
TableSource ::= Identifier
PipeOp      ::= WhereOp | SelectOp | SortOp | TakeOp | JoinOp | AggOp | SearchOp

WhereOp     ::= "where" Expr
SelectOp    ::= "select" ( Path ( "as" Identifier )? )+
SortOp      ::= "sort" ( "-"? Path )+
TakeOp      ::= "take" IntLit
JoinOp      ::= ( "join" | "lj" | "ij" ) Identifier "on" Expr
AggOp       ::= "by" ( Path )+ "agg" ( Identifier "=" FunctionCall )+
SearchOp    ::= "search" StringLit "in" Path ( "fuzz=" IntLit )?

/* --- Lexical --- */
Identifier  ::= [a-zA-Z_][a-zA-Z0-9_]*
IntLit      ::= [0-9]+
StringLit   ::= '"' [^"]* '"'
EnumLit     ::= "'" Identifier
```

---

## 5. Implementation Guide

### 5.1 The `dharma-expr` Crate
Create a shared crate that implements:
1.  **AST:** The `Expr` enum and `TypeSpec` enum.
2.  **Parser:** A `nom` implementation of the `Expr` grammar rules.
3.  **Evaluator:** A trait `EvalContext` that resolves `Path` lookups, and an implementation `eval(Expr, &Context) -> Value`.

### 5.2 DHL Integration
-   Use `dharma_expr::parser::parse_expr` inside `validate` blocks.
-   Compile `Expr` AST to Wasm instructions (Task 26).

### 5.3 DHARMA-Q Integration
-   Use `dharma_expr::parser::parse_expr` inside `where` clauses.
-   Use the `Expr` AST to drive the Columnar Scan loop (Predicate Pushdown).

This unification ensures that **Logic is Logic**, everywhere in DHARMA.

# DHARMA for Commerce

**Frictionless B2B. Self-Driving Contracts.**
DHARMA automates the "Boring Backend" of the global economy.

## 1. The "Live Invoice"
*   **Problem:** Invoices are dead PDFs. You email them. You wait. You chase.
*   **DHARMA Solution:** The Invoice is a Shared Subject.
    *   Vendor asserts: `Invoice.Issue(items=[...])`.
    *   Client asserts: `Invoice.Approve`.
    *   Bank (Reactor) sees Approval -> Releases Payment -> Asserts `Invoice.Paid`.
    *   **Result:** Days Sales Outstanding (DSO) drops from 45 days to minutes.

## 2. Supply Chain Visibility
*   **Scenario:** Just-in-Time Manufacturing.
*   **DHARMA Solution:** Shared State.
    *   Supplier updates `Inventory` subject.
    *   Manufacturer's system subscribes to it.
    *   When `Inventory < Threshold`, Manufacturer automatically issues a `PurchaseOrder`.
    *   **Result:** No emails. No phone calls. The factories talk to each other.

## 3. Reputation & Credit
*   **Problem:** Getting a loan requires faxing 3 years of statements.
*   **DHARMA Solution:** Cryptographic Credit History.
    *   A business can prove: "I have paid 500 invoices on time."
    *   They share the **Read Key** to their `Invoices` subject with the Bank.
    *   The Bank verifies the signatures of the suppliers.
    *   **Result:** Instant, algorithmic credit scoring based on real trade data.

## 4. Gig Economy / Freelancers
*   **Scenario:** Getting paid for work.
*   **DHARMA Solution:** Escrow Contracts.
    *   Client deposits funds into a DHARMA Escrow subject.
    *   Freelancer delivers work.
    *   Client signs `Accept`.
    *   Escrow releases funds.
    *   **Safety:** If Client disappears, an Arbiter (defined in DHL) can resolve the dispute.

**DHARMA removes the "Trust Tax" from business.**
You don't need to trust your partner to pay. You trust the contract.

# DHARMA for Governance

**Governance is the management of Shared Truth.**
DHARMA creates a tamper-proof, transparent (or private), and auditable record of decisions.

## 1. The "Unforgeable Ballot"
*   **Problem:** Paper ballots can be lost. Electronic voting machines are black boxes.
*   **DHARMA Solution:** Every vote is a signed assertion.
    *   `action.Vote(candidate="Alice")`.
    *   The "Ballot Box" is a DHARMA Subject.
    *   **Audit:** Anyone can sync the subject and count the signatures. The math proves the count.

## 2. Transparent Treasury
*   **Problem:** "Where did the tax money go?"
*   **DHARMA Solution:** The Treasury is a DHARMA Ledger.
    *   Income: `action.Tax.Receive`.
    *   Expense: `action.Grant.Release`.
    *   **Traceability:** Every expense is linked to a specific `Project` subject. You can click "School Construction" and see exactly who signed for the cement.

## 3. "Liquid" Democracy
*   **Problem:** rigid 4-year election cycles.
*   **DHARMA Solution:** Real-time delegation.
    *   `action.Delegate(scope="Environment", target="Expert_Bob")`.
    *   You can revoke this delegation instantly if Bob betrays your trust.
    *   Governance becomes a fluid, living stream of trust.

## 4. Multi-Sig Administration
*   **Scenario:** Nuclear codes or Reserve Bank keys.
*   **DHARMA Solution:** `M-of-N` Contracts.
    *   DHL rule: `validate count(signatures) >= 3`.
    *   No single person can act alone. The protocol enforces the quorum.

**DHARMA turns "Bureaucracy" into "Code".**
It makes corruption mathematically impossible (you cannot fake a signature) and makes incompetence visible (the audit trail never forgets).

# DHARMA for Healthcare

**Patient Sovereignty. Interoperability. Privacy.**
DHARMA solves the "Siloed Patient Data" crisis by giving the patient the keys.

## 1. The "Portable Record"
*   **Problem:** Your X-Ray is at Hospital A. Your Blood Test is at Lab B. Your Prescription is at Clinic C.
*   **DHARMA Solution:** The Patient Identity is the "Root".
    *   Subject: `Patient.HealthRecord`.
    *   Lab B asserts `BloodTestResult` to the Patient's subject.
    *   Hospital A asserts `Diagnosis`.
    *   **Result:** The patient has the *only* complete copy of their history.

## 2. Consent & Sharing
*   **Scenario:** Visiting a Specialist.
*   **DHARMA Solution:** Granting Access.
    *   Patient generates a **Read Token** (or wraps the Subject Key) for the Specialist.
    *   Specialist syncs the record instantly.
    *   Patient revokes the key after the visit.

## 3. Clinical Trials & Research
*   **Scenario:** Proving a drug works without leaking patient names.
*   **DHARMA Solution:** Zero-Knowledge Proofs (Future) or Pseudonymous Data.
    *   Patients submit data to a `Research` subject using a random one-time key.
    *   The data is signed and valid, but the identity is protected.

## 4. Supply Chain Safety (Pharma)
*   **Problem:** Counterfeit drugs.
*   **DHARMA Solution:** Track and Trace.
    *   Manufacturer signs `BatchCreated`.
    *   Distributor signs `Received`.
    *   Pharmacist signs `Dispensed`.
    *   Patient scans the box: "This path is unbroken."

**DHARMA restores the Hippocratic Oath to data:**
"First, do no harm." (By not leaking it).

# DHARMA: The Protocol for Shared Reality

Imagine a world where your digital life isn't just files in a folder, but a **Living, Unstoppable Truth**.

## The Problem: Dead Data in Walled Gardens

Right now, your digital world is **passive** and **trapped**.
- Your tasks sit dead in Asana until a human moves them.
- Your money sits dead in a bank DB until a server approves it.
- Your rules ("Don't spend more than $500") are just text in a wiki that everyone ignores.

You don't own this. You rent access to it. And it's dumb.

---

## The Solution: DHARMA (The Living Ledger)

DHARMA is a new layer of the internet. It turns "Dead Data" into **Active, Sovereign Laws**.

### 1. It is Law, Not Just Text.
In DHARMA, you don't just write data. You write the **Rules of Reality** (DHL).
- *Rule:* "This invoice cannot be paid twice."
- *Rule:* "Only Mom can approve expense requests over $100."
- *Rule:* "If the Task is Done, the Payment is Released."

Once these rules are set, **they cannot be broken.** Not by a hacker, not by a bug, not even by you. The system itself rejects any lie. It is **Digital Constitution**.

### 2. It is Alive (Automation).
DHARMA documents are not static paper. They are **active agents**.
- You sign "Job Done".
- The Document **wakes up**. It checks the rules. It sees the job is verified. It **automatically** signs "Release Payment".
- No servers. No middleman. The logic lives in the data itself.

### 3. It Lives Everywhere and Nowhere.
DHARMA data doesn't live on a "server."
It lives in the **Swarm**.
- It is on your phone.
- It is on your laptop.
- It is encrypted in the cloud (blindly).
- It is on your friend's backup drive.

If Google shuts down, your DHARMA lives on.
If you drop your phone in the ocean, your DHARMA lives on.
You just grab a new device, type your key, and the **Truth re-assembles itself** from the air.

---

## What does this feel like?

### The "Self-Driving" Business
You are a freelancer. You define a Contract: *"I get paid when I commit code."*
You push code. DHARMA sees the commit. DHARMA checks the contract. DHARMA generates the invoice. DHARMA sends it to the client.
**The business runs itself based on the Laws you wrote.**

### The Unbreakable Promise
You bet your friend $50 on a game. You lock the money in a DHARMA subject.
The game result comes in (via a trusted oracle).
The DHARMA subject **releases the money to the winner automatically**.
You don't need to trust your friend to pay up. You trust the DHARMA.

---

## The Big Idea

The old internet was about **Connecting Computers**.
The current internet is about **Connecting Apps**.
DHARMA is about **Connecting Truth**.

It is a shared, indestructible, self-executing reality that we own together.

# DHARMA for Logistics

**Truth in Motion. The Unbroken Chain.**
Logistics is about "Who has the package?" DHARMA answers this mathematically.

## 1. The "Hot Potato" Handshake
*   **Scenario:** Passing a container from Truck to Ship.
*   **Problem:** "I delivered it!" "No you didn't!"
*   **DHARMA Solution:** The Digital Handshake.
    *   Truck Driver and Ship Captain tap devices (NFC/QR).
    *   They both sign a `Transfer` assertion.
    *   **Result:** Indisputable proof of custody change. Time and Geo-stamped.

## 2. Bill of Lading (BoL)
*   **Problem:** Paper BoLs are slow, forgeable, and expensive to courier.
*   **DHARMA Solution:** The e-BoL.
    *   The BoL is a DHARMA Subject.
    *   Ownership is a `State` field.
    *   Transferring the BoL is an `Action`.
    *   **Speed:** Ownership moves at the speed of the internet, not the speed of FedEx.

## 3. Condition Monitoring (IoT)
*   **Scenario:** Cold Chain (Vaccines/Food).
*   **DHARMA Solution:** Sensor Assertions.
    *   A temperature sensor wakes up every hour.
    *   It signs: `Temp: 4C`.
    *   If `Temp > 8C`, the `Contract` automatically flags the shipment as `Spoiled`.
    *   **Result:** Automatic insurance claims. No human argument.

## 4. Multi-Party Coordination
*   **Scenario:** A port with Customs, Truckers, Cranes, and Agents.
*   **DHARMA Solution:** The Port Data Mesh.
    *   Everyone syncs the relevant subjects.
    *   Trucker sees: "Customs Cleared" (Signed by Customs).
    *   Trucker enters gate.
    *   Crane sees: "Truck at Dock 4".
    *   **Result:** Seamless choreography without a central "Port Operating System" that crashes.

**DHARMA makes Logistics invisible.**
It turns physical movement into digital certainty.

# DHARMA for National Security

**High Assurance. Compartmentalization. Resilience.**
DHARMA is designed to operate in "Denied Environments" where the internet is untrusted or unavailable.

## 1. The "Disconnected Field" Problem
*   **Scenario:** A submarine or a forward operating base. Satellite link is down.
*   **DHARMA Solution:** Local-First.
    *   The commander issues orders (`action.Order`).
    *   The team executes (`action.Report`).
    *   The local mesh syncs via radio/LAN.
    *   When the satellite connects, the history bursts to HQ. Nothing is lost.

## 2. Compartmentalization (Need-to-Know)
*   **Problem:** Preventing leaks (Snowden/Manning).
*   **DHARMA Solution:** Encryption Layers.
    *   Every "Mission" is a separate Subject with unique keys.
    *   Access is granted via **Capability Tokens** (Task 22).
    *   Revocation is cryptographic (`Key Rotation`). Once a key is rotated, a compromised actor cannot decrypt future messages.

## 3. Provenance & Chain of Custody
*   **Problem:** Intelligence requires verifying the source. "Who took this photo? When?"
*   **DHARMA Solution:** Immutable Lineage.
    *   The camera signs the photo (Hardware Key).
    *   The analyst signs the report.
    *   The commander signs the decision.
    *   **Result:** A perfect, unbroken chain of evidence from sensor to shooter.

## 4. "Darknet" Operations
*   **Scenario:** Covert communication.
*   **DHARMA Solution:** Relays.
    *   DHARMA traffic looks like random encrypted noise (`Noise_XX`).
    *   It runs over any transport (TCP, UDP, USB stick).
    *   It leaves no metadata on central servers (because there are no central servers).

**DHARMA is the "Digital Backbone" for sovereign operations.**
It provides the durability of paper with the speed of a network.

# DHARMA vs Ethereum

**Ethereum is a World Computer (Global Singleton).**
**DHARMA is a Fleet of Sovereign Computers (Sharded).**

## 1. The "Global State" Fallacy
*   **Ethereum:** There is **One State**.
    *   Everyone agrees on everything. Every transaction competes for the same block space.
    *   This forces global consensus, which is slow (12s blocks) and expensive (Gas fees).
    *   It is a "Global Singleton."

*   **DHARMA:** There is **No Global State**.
    *   Your invoice is *your* state. It lives only on your device and your client's device.
    *   My family budget is *my* state.
    *   They never intersect. They don't block each other.
    *   **Infinite Scalability:** Because they are independent shards.

## 2. The Consensus Model
*   **Ethereum:** **Global Consensus (PoS).**
    *   10,000 nodes must agree that you bought coffee.

*   **DHARMA:** **Local Consensus.**
    *   Only the participants of the Subject (You + Client) need to agree.
    *   If I double-spend in DHARMA, I only fork *my* reality. The global economy doesn't care. The person I tried to cheat just rejects my fork.

## 3. Privacy
*   **Ethereum:** **Public by Default.**
    *   Everyone sees every transaction. (Zero-Knowledge adds patches, but the base is public).

*   **DHARMA:** **Private by Default.**
    *   Data is encrypted end-to-end.
    *   Only people with the key can see the ledger.
    *   It is a "Dark Forest" of millions of private ledgers.

## 4. Interoperability
*   **Ethereum:** **Atomic Composability.**
    *   Smart Contracts call each other instantly ("Flash Loans").

*   **DHARMA:** **Async Messaging.**
    *   Contracts talk via messages (like Email). You cannot do a Flash Loan. You can only do business workflows (Request -> Approval -> Payment).

**Analogy:**
*   **Ethereum** is the Federal Reserve Settlement System.
*   **DHARMA** is Cash in your Pocket.

# DHARMA vs IPFS

**IPFS is a Hard Drive.**
**DHARMA is a CPU + Database.**

While both systems rely on **Content Addressing** (hashes) and **Peer-to-Peer** distribution, they solve fundamentally different layers of the stack.

## 1. Static vs. Dynamic
*   **IPFS (InterPlanetary File System)** is designed for **Static Blobs**.
    *   You upload a PDF. You get a hash (`Qm...`). That PDF never changes.
    *   If you change one byte, you get a new hash. It is a new object.
    *   IPFS has no concept of "Time" or "Evolution" built-in (IPNS is a mutable pointer, but primitive).

*   **DHARMA** is designed for **State History**.
    *   It tracks a **Subject** (an Identity).
    *   It records a sequence of **Assertions** (Updates).
    *   "The Ledger started empty. Alice added X. Bob changed X to Y."
    *   DHARMA tracks the *evolution* of truth over time.

## 2. Bytes vs. Semantics
*   **IPFS** is agnostic to content.
    *   It stores bytes. A cat photo and a smart contract are treated equally.
    *   It does not validate data.

*   **DHARMA** enforces **Law (DHL)**.
    *   Every assertion is typed and validated against a Contract.
    *   If you try to sign an invalid transaction (e.g., negative balance), the DHARMA kernel rejects it.
    *   DHARMA is a **Compliance Engine**, not just storage.

## 3. The Stack Integration
They are complementary.
*   **Layer 1 (Storage):** DHARMA uses IPFS-style content addressing for its low-level object store (`data/objects/`).
*   **Layer 2 (Logic):** DHARMA adds Identity, Signatures, Causality, and Business Logic on top.

**Analogy:**
*   **IPFS** is the file system (EXT4/NTFS).
*   **DHARMA** is the database and application server running on top of it.

# DHARMA vs SAP

**SAP forces you to model your business *their* way.**
**DHARMA allows you to model your business *your* way.**

## 1. The Centralization Trap
*   **SAP (and Oracle, Salesforce):**
    *   Huge central database.
    *   If the server goes down, the factory stops.
    *   If you stop paying the license, you lose access to your history.
    *   Customization requires expensive consultants.

*   **DHARMA:**
    *   **Local-First:** The data lives on the warehouse tablets, the sales laptops, and the backup drives.
    *   **Resilient:** If the internet cuts out, the factory keeps running. The data syncs when connectivity returns.
    *   **Sovereign:** You own the data. You own the schema. No one can revoke your access.

## 2. The Logic Model
*   **SAP:** "Business Logic" is hidden in millions of lines of proprietary ABAP code on a mainframe.
*   **DHARMA:** "Business Logic" is defined in **DHL Contracts** that you write.
    *   *Rule:* "Order Created -> Check Inventory -> Deduct Stock -> Ship".
    *   These rules are enforced cryptographically by the network.

## 3. The "Edge" Advantage
*   **Scenario:** A remote mining site with bad internet.
*   **SAP:** Painful. VPNs drop. Latency makes the UI unusable.
*   **DHARMA:** Native. The site operates as a local DHARMA cluster. It syncs with Headquarters only when the satellite link is up.

## 4. Cost
*   **SAP:** Millions of dollars per year.
*   **DHARMA:** The cost of commodity hardware and electricity.

**Conclusion:**
DHARMA is the **Operating System** for the sovereign enterprise. It replaces the "ERP Monolith" with a "Swarm of Synchronized Processes."

# What DHARMA Cannot Do

Knowing what *not* to build is as important as knowing what to build. DHARMA is optimized for **Human-Speed Commitments**, not machine-speed streams.

## 1. High-Frequency Trading (HFT)
*   **Why:** DHARMA is **Async**. It relies on network propagation and signature verification.
*   **Constraint:** It cannot achieve microsecond latency. It cannot guarantee atomic arbitrage across global markets.
*   **Use:** Centralized matching engines. Use DHARMA for the *settlement*, not the *trade*.

## 2. Real-Time Multiplayer Games (FPS)
*   **Why:** DHARMA has encryption and signature overhead on every message.
*   **Constraint:** It is too heavy for 60Hz state updates (Call of Duty).
*   **Use:** UDP state compression. Use DHARMA for the *inventory* (skins, loot), not the *bullets*.

## 3. "The World's Truth" (Global Singleton)
*   **Why:** DHARMA does not enforce a single global ordering of events for 8 billion people.
*   **Constraint:** You cannot build a "Global DNS" that is instantly consistent for everyone. You will have forks and eventual consistency.
*   **Use:** Blockchains (Ethereum) if you need absolute global scarcity (e.g., a single unique NFT art piece).

## 4. Ephemeral Streaming (Netflix/Zoom)
*   **Why:** DHARMA stores history.
*   **Constraint:** You do not want to "Sign and Store" every video frame of a call. That creates petabytes of garbage.
*   **Use:** WebRTC/RTMP. Use DHARMA for the *signaling* ("Call started", "Call ended"), not the *media*.

## 5. Big Data Processing (Snowflake)
*   **Why:** DHARMA is row-oriented and cryptographically heavy.
*   **Constraint:** It is designed for "Business Data" (Millions of rows), not "Telemetry" (Trillions of rows).
*   **Use:** Parquet/Arrow on S3. Use DHARMA to track the *metadata* and *provenance* of those datasets.

## Summary
**DHARMA is for:** Contracts, Tasks, Invoices, Votes, decisions.
**DHARMA is not for:** Physics simulations, raw streams, or sub-millisecond race conditions.

# DHARMA Test Philosophy: "Trust but Verify"

## Core Principle
DHARMA is a "Truth Machine." If it corrupts data, it fails its primary purpose.
Therefore, testing must go beyond "unit tests passed." We must aggressively hunt for edge cases, non-determinism, and protocol divergence.

**We do not prove correctness (yet). We simulate failure until we are bored.**

---

## 1. The Hierarchy of Confidence

### Level 1: Unit Tests (The Baseline)
*   **Scope:** Individual functions (`parse_header`, `verify_signature`).
*   **Tool:** `cargo test`.
*   **Mandate:** 100% coverage of error paths. Every `Err(...)` must be triggered by a test.

### Level 2: Property-Based Testing (The Fuzzer)
*   **Scope:** Encoders, Decoders, Mergers.
*   **Tool:** `proptest` or `quickcheck`.
*   **Philosophy:** "Don't test `1 + 1 = 2`. Test `a + b = b + a` for random `a, b`."
*   **Targets:**
    *   `Canonical CBOR`: Encode(Decode(bytes)) == bytes.
    *   `DHL Parser`: Parse(Print(ast)) == ast.
    *   `Merge Logic`: `merge(A, B)` must equal `merge(B, A)` (commutativity).

### Level 3: Deterministic Simulation (The Time Machine)
*   **Scope:** Concurrency, Networking, Sync.
*   **Tool:** `shuttle` or `turmoil`.
*   **Philosophy:** "If it bugs once, it must bug every time."
*   **Method:**
    *   Run the Sync Loop in a simulated network.
    *   Inject delays, packet drops, and reorderings.
    *   Run 10,000 permutations with a fixed seed.
    *   Assert that all nodes converge to the same `Frontier`.

### Level 4: The "Chaos" Integration (The Real World)
*   **Scope:** `dhd` binary, Filesystem I/O.
*   **Tool:** Docker / Shell scripts (`tests/conformance/`).
*   **Method:**
    *   Spin up 5 nodes.
    *   `kill -9` a node during a write.
    *   Corrupt a `log.bin` file (flip a bit).
    *   Restart and ensure `dhd` recovers or fails safely (no silent corruption).

---

## 2. Specific Testing Tactics

### A. The "1MB" Constraint Check
*   **CI Step:** Fail the build if `target/release/dh-runtime` > 1.05 MB.
*   **Why:** Performance regression is a bug. Size regression is a bug.

### B. The "DHL" Fuzz
*   Generate random valid DHL contracts.
*   Compile them to Wasm.
*   Execute them with random Inputs.
*   **Invariant:** The VM must *never* panic. It must return `Ok` or `Err`. If `wasmi` panics, we lose.

### C. The "Protocol" Compat
*   **Golden Vectors:** Store raw hex files of "Valid V1 Assertions" in `tests/vectors/`.
*   **Rule:** A new version of DHARMA *must* be able to ingest these vectors. Breaking this requires a protocol version bump.

---

## 3. What we DO NOT do

*   **Mocking the World:** Do not mock the Filesystem or Network logic inside the Kernel. Use trait abstraction (`Store`, `Transport`) and implement **In-Memory** backends for testing. This is better than mocking.
*   **Formal Verification (Yet):** We rely on Rust's type system + aggressive fuzzing. Formal proofs are for V2.

---

## 4. Summary Checklist for PRs

1.  [ ] **Unit:** Did you test the error case?
2.  [ ] **Prop:** Did you fuzz the parser?
3.  [ ] **Sim:** Did you run the sync loop with `shuttle` (if touching net)?
4.  [ ] **Size:** Did you blow the binary size?

**"If it isn't tested, it's broken. If it's tested once, it's lucky. If it's fuzzed, it's DHARMA."**