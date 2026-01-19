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
*   **Layer 1 (Storage):** DHARMA uses IPFS-style content addressing for its low-level object store (`<storage_root>/objects/`).
*   **Layer 2 (Logic):** DHARMA adds Identity, Signatures, Causality, and Business Logic on top.

**Analogy:**
*   **IPFS** is the file system (EXT4/NTFS).
*   **DHARMA** is the database and application server running on top of it.
