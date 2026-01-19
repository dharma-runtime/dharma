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
