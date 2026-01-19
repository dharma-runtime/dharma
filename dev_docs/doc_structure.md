DHARMA Documentation Architecture (World-Class)

The Golden Rule

Each document answers exactly one question.
Why, What, How, or What exactly is allowed — never all at once.

⸻

0. The Front Door (Orientation)

Audience: intelligent but new
Goal: “I understand what this is and why it’s different.”

0.1 What Is DHARMA? (1 page, timeless)
	•	One-sentence definition
	•	The 5 non-negotiable ideas:
	•	assertions, not mutations
	•	humans commit, machines propose
	•	append-only truth
	•	deterministic replay
	•	versioned meaning
	•	What DHARMA is not

No diagrams yet. No APIs. No hype.

⸻

0.2 Why DHARMA Exists (Essay)
	•	Why CRUD breaks
	•	Why workflows rot
	•	Why audit is bolted on
	•	Why distributed systems lie
	•	Why “event sourcing” almost worked

This is philosophical and deliberately opinionated.
This is where you attract the right people and repel the wrong ones.

⸻

1. The Mental Model (Foundations)

Audience: builders who think in systems
Goal: “I can reason about DHARMA without memorizing it.”

1.1 The Laws of DHARMA
	•	What is truth
	•	What is an assertion
	•	What is derived state
	•	What cannot happen (forbidden states)
	•	What time means

This is your constitution.

⸻

1.2 Core Concepts (Glossary, but narrative)
	•	Assertion
	•	Subject
	•	Envelope
	•	Contract
	•	Package
	•	Profile
	•	Acceptance
	•	Pending
	•	Receipt
	•	Replay

Each term:
	•	definition
	•	example
	•	one invariant it obeys

⸻

1.3 The DHARMA Lifecycle

From:

intent → assertion → validation → acceptance → derivation → effects → receipts → replay

This is the mental execution trace.

⸻

2. The Builder’s Path (Paved Road)

Audience: people who want to build now
Goal: “I shipped something real in an hour.”

2.1 Quickstart: Build a System in 15 Minutes
	•	./dh new ecommerce
	•	./dh dev
	•	open Workspace
	•	create a rule
	•	see API + admin update
	•	submit an order

No theory. No detours.

⸻

2.2 Your First Domain Package
	•	Define schema
	•	Define actions
	•	Define invariants
	•	Define policies
	•	Run tests
	•	See failures with tickets

This mirrors Ash’s best docs — concrete and empowering.

⸻

2.3 Versioning & Iteration
	•	How versioning is etched into data
	•	How to upgrade safely
	•	How to simulate before switching
	•	How rollback actually works in DHARMA terms

This is where DHARMA feels adult.

⸻

3. The Standard Library (Interoperability Layer)

Audience: serious adopters, enterprises
Goal: “I can interoperate without coordination.”

3.1 Stdlib Philosophy
	•	What stdlib is
	•	What it must never become
	•	Compatibility vs innovation
	•	Additive extensions only

⸻

3.2 Canonical Constructs

Each construct gets its own page:
	•	std.party
	•	std.money
	•	std.invoice
	•	std.payment
	•	std.shipment
	•	std.schedule

Each page includes:
	•	semantic definition
	•	required fields
	•	invariants
	•	standard verbs
	•	extension rules
	•	compliance levels

This becomes a reference people cite.

⸻

3.3 Interoperability Guide
	•	Sending objects across organizations
	•	Validating foreign assertions
	•	Handling unknown extensions
	•	Responding with receipts

This is where DHARMA becomes a network, not a tool.

⸻

4. The Runtime (How It Actually Works)

Audience: engineers who need guarantees
Goal: “I trust this.”

4.1 dhd: The Core
	•	What dhd does
	•	What it will never do
	•	Determinism guarantees
	•	Storage model
	•	Replication model (explicit)

No code yet. Just invariants.

⸻

4.2 Execution Semantics
	•	Validation
	•	Ordering
	•	Concurrency model
	•	Replay semantics
	•	Time semantics

This is precise and boring — that’s good.

⸻

4.3 Scheduler & Time
	•	One-shot vs recurring
	•	Cancel/reschedule as truth
	•	No mutable cron
	•	Guarantees and limits

⸻

5. Security, Privacy, Audit (Built-In, Explained)

Audience: auditors, security teams
Goal: “This is safer than what we have.”

5.1 Identity & Signatures
	•	What a signature means
	•	Who can assert what
	•	Device vs user vs org identities

⸻

5.2 Privacy & Visibility
	•	What data is visible where
	•	Field-level vs subject-level rules
	•	Zero-trust assumptions

⸻

5.3 Audit & Forensics
	•	How to prove what happened
	•	Replay as evidence
	•	Failure tickets as artifacts

This section should feel court-ready.

⸻

6. Testing & Correctness (Your Killer Differentiator)

Audience: senior engineers
Goal: “This system does not lie.”

6.1 Testing Philosophy
	•	Why testing is part of the runtime
	•	Why failures are first-class
	•	Why tickets exist

⸻

6.2 ./dh test
	•	Modes
	•	Dashboard
	•	Shrinking
	•	Reproduction

⸻

6.3 Property Catalog
	•	List of properties
	•	What each means
	•	What a violation looks like

This is where you earn TigerBeetle-level respect.

⸻

7. Workspace (The Universal App)

Audience: power users, operators
Goal: “I can live in this.”

7.1 Workspace Philosophy
	•	Buffers, not pages
	•	Commands over clicks
	•	Truth lens, not CRUD

⸻

7.2 Using the Admin
	•	Inspect truth
	•	Review pending assertions
	•	Simulate changes
	•	Time-travel

⸻

7.3 Extending the Workspace
	•	UI descriptors
	•	Custom views
	•	Domain-specific admin

⸻

8. APIs & Integration

Audience: frontend and integration devs
Goal: “DHARMA fits my stack.”

8.1 GraphQL Generation
	•	How schemas map
	•	How policies apply
	•	Versioning guarantees

⸻

8.2 REST / Webhooks / Email
	•	Gateway model
	•	Receipts
	•	Idempotency

⸻

8.3 Mobile & Edge
	•	Embedded core
	•	Node profiles
	•	Offline semantics

⸻

9. Reference (Precise, Boring, Complete)

Audience: implementers, reviewers
	•	DSL grammar
	•	Type system
	•	Error codes
	•	CLI reference
	•	Config reference

No narrative. Just facts.

⸻

10. The Living Appendix
	•	Design decisions
	•	Rejected ideas (important)
	•	Compatibility promises
	•	Roadmap (non-binding)
	•	Manifesto / Core invariants

⸻

The secret that makes it world-class

Every page should answer:

What must never change?
What may evolve?

If readers always know which is which, they will trust you.
