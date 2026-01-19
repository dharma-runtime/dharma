# Core Concepts (Current)

This glossary maps conceptual terms to concrete structures in the codebase.

---

### Subject
The atomic unit of truth. A 32-byte ID representing a ledger for a single "thing".
- **Code:** `SubjectId`
- **Invariant:** Every assertion belongs to exactly one subject.

### Assertion
A signed statement of fact.
- **Code:** `AssertionPlaintext`
- **Header:** protocol version, subject, author, seq, prev, refs, schema, contract, ver
- **Body:** typed CBOR

### Assertion ID
The **hash of the canonical assertion payload** (header + body), not the envelope.
- **Code:** `crypto::assertion_id`

### Envelope
Encrypted wrapper around an assertion or artifact.
- **Code:** `AssertionEnvelope`
- **Envelope ID:** hash of the envelope bytes (`crypto::envelope_id`)

### Contract (DHL)
Deterministic rules compiled to Wasm.
- **Code:** `ContractEngine` + `RuntimeVm`
- **Logic:** `validate` + `apply` + invariants

### Schema
Typed structure for assertions or CQRS state.
- **Generic Schema:** `SchemaManifest`
- **CQRS Schema:** `CqrsSchema`

### Lens (Data Version)
A versioned interpretation of the same subject history.
- **Field:** `header.ver`
- **CLI:** `--lens <ver>`

### Pending / Accepted / Rejected
- **Accepted:** valid and applied.
- **Pending:** missing dependencies.
- **Rejected:** invalid or unauthorized.

### Replay
Deterministic reconstruction of derived state from accepted assertions.
- **Optimization:** snapshots.

---

If you want a deeper walk-through, see [Lifecycle](03_lifecycle.md).

