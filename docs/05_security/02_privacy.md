# Privacy & Visibility (Current)

DHARMA provides privacy through **encryption** and **overlays**.

---

## 1) Envelope Encryption

Assertions and artifacts are encrypted into envelopes using ChaCha20-Poly1305.

- Payloads are end-to-end encrypted.
- Metadata such as subject IDs may still be visible unless additional transport protections are used.

---

## 2) Field Visibility

DHL supports `public` and `private` fields.

- Public fields go to the base assertion.
- Private fields go to **overlay assertions**.

---

## 3) Overlays

- Overlays are stored separately from base logs.
- Replication is gated by overlay policies (`overlays.policy`).
- Overlays reference their base assertions and are merged at replay.

---

## 4) Current Limitations

- No capability tokens yet.
- No metadata-hiding transport by default.

