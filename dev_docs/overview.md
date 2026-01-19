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
