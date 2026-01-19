# Task 30: Semantic IDs (Decoupling Identity from Encryption)

## Goal
Separate the **Semantic Identity** of an assertion (what it says) from its **Transport Identity** (how it is encrypted).
This allows re-encryption (key rotation) without breaking the dependency graph (`prev`/`refs`).

## Specification

### 1. Two IDs
*   **`AssertionID` (Semantic):** `SHA256(CanonicalCBOR(Header + Body))`.
    *   This is the "True ID".
    *   Used in `header.prev`, `header.refs`.
    *   Used in DHARMA-Q foreign keys.
*   **`EnvelopeID` (Transport):** `SHA256(EncryptedBytes)`.
    *   Used as filename in `data/objects/`.
    *   Used in Sync `INV/GET` messages.

### 2. The Linkage
The `AssertionHeader` currently contains `prev: ObjectId`.
*   **Change:** `prev` and `refs` must explicitly be `AssertionID`.
*   **Verification:**
    1.  Decrypt Envelope -> Plaintext.
    2.  Hash Plaintext -> Calculated AssertionID.
    3.  Check if Calculated ID matches the reference (if we are looking it up).

### 3. Storage Index
The Store needs a mapping to find the *current* envelope for a given assertion.
*   **Index:** `AssertionID -> EnvelopeID`.
*   **Behavior:**
    *   On Ingest: Compute both IDs. Update Index.
    *   On Re-Key: Decrypt old envelope, Re-encrypt with new key -> New EnvelopeID. Update Index. `AssertionID` remains unchanged. History is preserved.

## Implementation Steps
1.  **Rename types:** Rename `ObjectId` to `EnvelopeId`. Create `AssertionId`.
2.  **Update Structures:** Update `AssertionHeader` to use `AssertionId`.
3.  **Update Store:** Add `indexes/semantic.idx` (persistent map of `AssertionId -> EnvelopeId`).
4.  **Update Sync:** Sync negotiates `EnvelopeId` (blobs), but validation logic uses `AssertionId`.
