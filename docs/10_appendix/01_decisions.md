# Design Decisions (Current)

This document records core architectural decisions and their rationale.

---

## 1) Canonical CBOR
- **Decision:** All structured data is encoded as canonical CBOR.
- **Why:** Deterministic signatures and replay across platforms.

## 2) Content Addressing
- **Decision:** Objects are addressed by SHA-256 hashes.
- **Why:** Immutability, deduplication, and integrity.

## 3) Append-Only Logs
- **Decision:** Subject history is append-only (`log.bin`).
- **Why:** Auditability and deterministic replay.

## 4) No Floats
- **Decision:** Floats are not supported in the kernel.
- **Why:** Avoid cross-CPU nondeterminism.

## 5) Minimal Kernel
- **Decision:** Core does not include domain logic.
- **Why:** Auditability and long-term stability.

