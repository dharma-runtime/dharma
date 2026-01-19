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
