Dharma PRD — Identity, Domains, Ownership, Sharing & Revocation (v1)

Status

Approved – Ready for Technical Specification & Implementation

Audience
	•	Engineering
	•	Architecture
	•	Security
	•	UX

⸻

1. Purpose & Goals

Dharma provides a federated, sovereign runtime where systems are built from facts and intrinsic rules, not mutable state.

This PRD defines the identity, discovery, permission, ownership, sharing, and revocation model for Dharma v1.

Primary goals
	•	Local-first by default
	•	Explicit sovereignty and ownership
	•	Contract-defined authority and permissions
	•	Cryptographically enforceable sharing and revocation
	•	Deterministic and auditable behavior
	•	No deletion of data, ever

Non-goals
	•	Anonymous public sharing
	•	DRM-like enforcement
	•	Content deletion or “right to be forgotten”
	•	Centralized identity authority

⸻

2. Core Principles
	1.	Sovereignty is explicit
Authority is defined by domains, not infrastructure.
	2.	Facts are immutable
Nothing is deleted or overwritten.
	3.	Rules are intrinsic
Behavior emerges from contracts, not from ad-hoc code.
	4.	Local-first is the default
Federation is optional and explicit.
	5.	Security is enforceable, not advisory
Access is cryptographically enforced.

⸻

3. Identity Model

3.1 Local Handle
	•	A local handle (e.g. julienmarie) exists only on the user’s device.
	•	It never leaves the machine unless explicitly linked.
	•	It is purely a UX convenience.
	•	It is not globally unique and not verifiable.

Users can operate indefinitely with only a local handle.

⸻

3.2 Atlas Identity
	•	An Atlas identity is a globally unique identity under an Atlas namespace:

person.fr.julienmarie_<public_key_suffix>


	•	Each user may have exactly one Atlas identity.
	•	Atlas identities are sovereign domains.
	•	Atlas identity creation is open to anyone.

Verification (v1)
	•	Verification is binary.
	•	An identity is considered verified if it is:
	•	active
	•	not revoked
	•	not suspended

Domains may impose their own acceptance rules beyond this.

⸻

3.3 Identity Lifecycle

Atlas identities can be:
	•	active
	•	suspended
	•	revoked

Bad actors may be suspended or revoked.

Consequences of identity loss
	•	The identity loses the ability to:
	•	decrypt
	•	propose
	•	accept
	•	administer
	•	Subjects owned by the identity remain in existence.
	•	Shared domains continue to operate according to their own contracts.

Identity loss is irreversible in v1.

⸻

4. Sovereign Domains

4.1 Definition
	•	Every Atlas identity is a sovereign domain.
	•	Domains are implemented as contracts conforming to std.atlas.domain.
	•	Domains define:
	•	membership rules
	•	permissions
	•	sharing policies
	•	subject ownership defaults

4.2 Domain Hierarchy
	•	Domains can own other domains.
	•	Example:

person.fr.julienmarie
  └── com.ph.cmdv


	•	Ownership is verifiable and auditable.

⸻

4.3 Domain Membership
	•	Membership is contract-defined.
	•	There is no fixed global role model.
	•	Membership properties may include:
	•	roles
	•	scopes
	•	time bounds
	•	revocation conditions

Users cannot act simultaneously as themselves and as a domain in a single action.

⸻

4.4 Operating Context
	•	Once an Atlas identity exists, the user always has an active domain context.
	•	The active context is either:
	•	their own domain
	•	a domain they are acting within
	•	Domain context cannot change mid-action.
	•	The UI must always clearly show:
	•	current domain
	•	acting identity

⸻

5. Subjects & Ownership

5.1 Subject Definition
	•	A subject is governed by a contract.
	•	The subject’s domain is inferred from the contract.
	•	Subjects are immutable in identity.

⸻

5.2 Ownership Semantics

Ownership is exclusive and grants the full rights bundle:

Right	Granted
Read	Yes
Propose / Write	Yes
Accept / Reject	Yes
Share	Yes
Revoke access	Yes
Transfer ownership	Yes, if contract allows

Joint ownership is not supported in v1.

⸻

5.3 Creator Attribution
	•	Creator attribution is always recorded, regardless of ownership.
	•	Attribution includes:
	•	creator identity
	•	acting domain
	•	role (if applicable)

Creator attribution does not grant residual rights by default.

⸻

5.4 Ownership Defaults
	•	Ownership rules are defaulted if omitted.
	•	Default:
	•	owner = domain
	•	ownership transfer = forbidden

⸻

5.5 Ownership Transfer
	•	Ownership transfer is contract-defined.
	•	Transfers may be:
	•	immediate
	•	multi-step (propose + accept)
	•	Transfers can be fully forbidden.

⸻

6. Contracts

6.1 Contract Ownership
	•	Contracts are owned by domains.
	•	Individuals are domains.
	•	Contract owners may delegate:
	•	deployment
	•	upgrade
	•	deprecation

⸻

6.2 Contract Versioning
	•	Contracts are versioned.
	•	Subjects may continue indefinitely under old contract versions.
	•	Versioning is part of the subject’s meaning and audit trail.

⸻

6.3 Contract-Enforced Rules

Contracts define:
	•	subject ownership rules
	•	transfer rules
	•	sharing permissions
	•	revocation behavior
	•	domain membership semantics

Ownership rules:
	•	are defaulted if omitted
	•	may change over time via contract updates

⸻

7. Sharing & Permissions

7.1 Sharing Model

All sharing is:
	•	explicit
	•	logged as a fact
	•	auditable

Sharing modes:
	•	direct (identity → identity)
	•	role-based (domain role)

Group-based sharing is not supported in v1 (roles cover this).

⸻

7.2 Access Scope

Access can be scoped by:
	•	fields
	•	actions
	•	queries

Access levels are contract-defined, not fixed globally.

⸻

7.3 Delegation
	•	Delegation may be allowed or forbidden by contract.
	•	Delegated rights may or may not be further delegable.

⸻

7.4 Public Subjects
	•	Subjects may be public if contract allows.
	•	Public subjects are world-readable.
	•	Public status is explicit and auditable.

⸻

8. Encryption & Key Lifecycle

8.1 Key Scope (v1 decision)

Dharma v1 uses a hierarchical key model:
	•	Domain keys form the root.
	•	Subject keys are derived/enveloped from domain keys.

⸻

8.2 Key Rotation
	•	Domains can rotate keys without changing subject identity.
	•	Subject IDs, links, and references remain stable.
	•	New encryption epoch prevents access for revoked parties.

⸻

8.3 Revocation Semantics

Revocation guarantees:
	•	No future access via Dharma.
	•	No ability to decrypt new epochs.

Revocation does not guarantee:
	•	Erasure of previously decrypted copies.

The UI must state this explicitly.

Revocation can be:
	•	immediate
	•	scheduled
	•	conditional
(depending on contract)

⸻

9. Emergency & Compromise Handling

9.1 Emergency Freeze (v1)
	•	Domains may be frozen.
	•	While frozen:
	•	no new facts can be accepted
	•	read access remains unchanged
	•	Freeze is explicit and logged.

⸻

9.2 Device Compromise
	•	Device keys can be revoked.
	•	User continues to operate with remaining devices.

⸻

9.3 Admin / Domain Key Compromise
	•	Domain admin key compromise results in:
	•	identity/domain considered lost
	•	no recovery in v1
	•	This is an explicit, harsh security posture.

⸻

10. Federation & Participation

10.1 Joining Domains

Domains may allow:
	•	invite-only
	•	request + approval
	•	public join

All behavior is contract-defined.

Invitations:
	•	single-use
	•	revocable
	•	may embed role assignment

⸻

10.2 Leaving Domains

On leave:
	•	access revocation timing is contract-defined
	•	read-only grace periods may exist
	•	subject fate depends on ownership policy

⸻

11. UX Requirements

11.1 Transparency

The UI must always show:
	•	current domain
	•	subject owner
	•	acting identity

⸻

11.2 Sensitive Actions

Actions affecting:
	•	ownership
	•	sharing
	•	revocation
require:
	•	explicit confirmation
	•	explanation of consequences

⸻

11.3 Failure Feedback

Denied actions must:
	•	fail explicitly
	•	include explanation
	•	cite the rule or contract that forbids the action

Simulation of ownership/sharing changes is nice-to-have in v1.

⸻

12. Explicit Non-Goals
	•	Deletion of subjects or history
	•	Anonymous identity sharing
	•	DRM-like control
	•	Implicit trust via infrastructure

⸻

13. Acceptance Criteria

This PRD is satisfied when:
	•	Users can operate fully locally without Atlas.
	•	Atlas identity creation enables federation.
	•	Domains enforce ownership and membership via contracts.
	•	Sharing and revocation are explicit, auditable, and enforceable.
	•	Key rotation revokes access without changing subject identity.
	•	Emergency freeze exists and is enforced.
	•	Nothing can be deleted.

⸻

14. Closing Statement

Dharma treats authority as data, rules as structure, and facts as the only source of truth.

This model favors clarity, sovereignty, and correctness over convenience.


Dharma v1 — Architectural Clarifications & Guardrails

(Companion to PRD)

Purpose

This document clarifies how to interpret the PRD for v1, especially where the PRD defines semantic guarantees that must be preserved while allowing practical, scalable implementations.

The goal is to prevent:
	•	over-literal implementations that break at scale
	•	accidental weakening of core invariants
	•	v2 rewrites caused by missing early abstractions

This document is normative where marked MUST, and guidance elsewhere.

⸻

1. Semantic Immutability vs Physical Storage

Clarification

When the PRD states:

“Facts are immutable. Nothing is deleted.”

This refers to semantic immutability, not physical storage behavior.

What MUST hold
	•	Accepted facts are never silently rewritten or removed.
	•	Any derived state must be explainable from accepted facts + rules.
	•	The meaning of history must remain auditable.

What is ALLOWED
	•	Log segmentation
	•	Snapshots / checkpoints
	•	Compaction
	•	Archival of old segments
	•	Cryptographic erasure (key destruction)

What is FORBIDDEN
	•	Silent removal of accepted facts
	•	Rewriting facts to change historical meaning
	•	Mutating history to “fix” outcomes

Guidance:
Think “append-only semantics, LSM-style mechanics.”

⸻

2. Genesis Phase (Identity Bootstrap)

Clarification

Identity creation introduces a bootstrap paradox:
you cannot validate an identity using rules that require the identity to already exist.

v1 Resolution: Genesis Phase

Dharma v1 explicitly defines a Genesis Phase.

Genesis assertions:
	•	establish the first Atlas identity
	•	are allowed only when no Atlas identity exists
	•	are explicitly typed
	•	are non-repeatable
	•	are not upgradeable by contracts

After genesis:
	•	all normal validation rules apply
	•	no further genesis assertions are allowed

This is not a loophole.
It is equivalent to filesystem formatting or database initialization.

⸻

3. Permission Evaluation Model (Latency Guardrail)

Clarification

The PRD defines authority semantics, not execution strategy.

v1 MUST NOT require full WASM execution for every permission check.

Required model (conceptual)

Permission evaluation is layered:
	1.	Declarative permission summaries
	•	fast
	•	cacheable
	•	rejectable at router / ingest layer
	2.	Contract guard evaluation
	•	bounded execution
	•	deterministic
	•	cacheable per (contract version, role, action)
	3.	Full derivation
	•	used only for state computation and acceptance

What MUST hold
	•	No action may be accepted without contract validation.
	•	Routers MAY reject early, but MUST NOT accept without full validation.

Guidance:
Design permission summaries as first-class runtime artifacts.

⸻

4. Key Hierarchy & Rotation (Scalability Guardrail)

Clarification

v1 uses a hierarchical key model:
	•	Domain Root Key
↓
	•	Domain Key Encryption Key (KEK)
↓
	•	Subject Data Keys

What MUST hold
	•	Subject identity is stable across key rotations.
	•	Revocation prevents future decryption via Dharma.
	•	Key rotation does NOT require rewriting subject data.

What is FORBIDDEN
	•	Re-encrypting all subject data on domain key rotation.
	•	Changing subject identity on rotation.

Guidance:
Use envelope encryption and indirection. Expect large domains.

⸻

5. Revocation Semantics (Honesty Clause)

Clarification

Revocation guarantees future access prevention, not retroactive erasure.

What MUST be guaranteed
	•	Revoked identities cannot decrypt new epochs.
	•	Revoked identities cannot access subject data via Dharma.

What MUST be communicated
	•	Previously decrypted plaintext may still exist outside the system.

UI Requirement

The UI MUST explicitly state this limitation when revoking access.

⸻

6. Emergency Freeze (v1 Scope)

Clarification

v1 includes an Emergency Freeze mechanism.

Behavior
	•	When a domain is frozen:
	•	no new facts may be accepted
	•	read access remains unchanged
	•	Freeze events are explicit and logged.

Purpose
	•	suspected compromise
	•	insider threat
	•	operational containment

Out of scope
	•	partial freeze by action type (v2+)

⸻

7. Identity & Domain Compromise

v1 Security Posture (Intentional)
	•	Device key compromise:
	•	device key can be revoked
	•	user/domain continues
	•	Domain admin key compromise:
	•	domain is considered unsafe to operate
	•	no recovery in v1

Important framing

This is a deliberate v1 constraint, not an oversight.

Future recovery mechanisms (social recovery, threshold keys) are:
	•	anticipated
	•	out of scope for v1

⸻

8. Snapshotting & Replay

Clarification

Replay from genesis is a semantic capability, not an operational requirement.

What MUST hold
	•	Snapshots must be derivable from facts + rules.
	•	Snapshots must not change meaning.
	•	Snapshots must not become sources of truth.

Guidance

Snapshots are:
	•	accelerators
	•	checkpoints
	•	caches

They are not authority.

⸻

9. Scope Boundaries (v1 Discipline)

The following are explicitly out of scope for v1:
	•	Subject-level cryptographic sharing independent of domains
	•	Social recovery / quorum-based admin keys
	•	Automated GDPR workflows
	•	Global peer discovery
	•	Permission inference or heuristics

Do not “sneak” these in.

⸻

10. Implementation Posture

Developers are expected to:
	•	preserve semantic guarantees
	•	choose pragmatic implementations
	•	design for evolution without breaking invariants

Developers must NOT:
	•	weaken ownership or authority semantics for convenience
	•	introduce hidden mutation
	•	add implicit trust paths
	•	rely on infrastructure as authority

⸻

Final Reminder

Dharma v1 is intentionally strict at the semantic layer and flexible at the operational layer.

If you ever feel forced to violate an invariant to make something “work”:
	•	stop
	•	escalate
	•	clarify the model

That is the correct response.
