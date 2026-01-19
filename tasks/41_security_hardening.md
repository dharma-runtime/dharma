# Task 41: Security Hardening (Audit Remediation)

## Goal
Address the critical and high-risk vulnerabilities identified in the Initial Security Audit. 
Ensure the DHARMA kernel is resilient against Denial-of-Service (DoS), Resource Exhaustion (OOM), and Cryptographic failures.

## Vulnerability Remediation Plan

### 1. Wasm Runtime Safety (Critical)
- [ ] **Fuel Metering (Task 39):** Configure `wasmi` to enforce an instruction limit per execution.
- [ ] **Memory Limits:** Set a hard maximum on the number of Wasm memory pages allowed (e.g., 640KB).
- [ ] **Stack Limits:** Enforce Wasm stack size limits to prevent stack overflow crashes.

### 2. Network & IO Resilience (Critical)
- [ ] **Frame Size Limits:** Update `dharma-core/src/net/codec.rs` to reject any frame exceeding a defined threshold (e.g., 1MB).
- [ ] **Connection Timeouts:** Implement strict timeouts for the Noise handshake and sync exchange.
- [ ] **Pending Queue GC:** Implement a cleanup mechanism for "Pending" assertions that never resolve their dependencies (preventing disk exhaustion).

### 3. Cryptographic Hardening (High)
- [ ] **Nonce Audit:** Review `dharma-core/src/envelope.rs`. Ensure nonces are never reused with the same key.
- [ ] **XChaCha20 Migration:** Consider migrating from standard ChaCha20-Poly1305 to XChaCha20-Poly1305 (192-bit nonces) for safer random nonce generation.
- [ ] **Drift Limits:** Enforce that `assertion.ts` is within a reasonable window of host time (e.g., +/- 5 minutes) during ingest.

### 4. Supply Chain & Tooling
- [ ] **Dependency Audit:** Run `cargo audit` and address any identified vulnerabilities.
- [ ] **Panic Prevention:** Audit `dharma-core` for `unwrap()` or `expect()` calls on untrusted data; replace with `DharmaError`.

## Success Criteria
- [ ] `./dh test --chaos` includes a "Wasm Infinite Loop" test case that fails gracefully (fuel error) instead of hanging.
- [ ] Large network frames are dropped at the codec level without allocation.
- [ ] `cargo audit` passes in CI.
