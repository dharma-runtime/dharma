# Task 18: Error Granularity (Refine DharmaError)

## Goal
Split the monolithic `DharmaError` into actionable categories to improve retry logic and user feedback.

## ADR Dependency (DHA-55)
- Reference: `dev_docs/adr/ADR-0071-runtime-storage-migration.md`
- Risk register: `dev_docs/adr/ADR-0071-risk-register.md`
- Error taxonomy must map cleanly to ADR retryable vs fatal vs compensating-action-required classes.

## Why
- **Sync Logic:** `NetworkError` should trigger retry. `ValidationError` (bad sig) should ban the peer.
- **UX:** "IO Error" is useless. "Disk Full" or "Permission Denied" is helpful.

## Specification

### New Error Hierarchy
```rust
pub enum DharmaError {
    // Permanent Failures (Data is bad)
    Crypto(String),
    Validation(String),
    Contract(String),
    Schema(String),

    // Transient Failures (Retryable)
    Network(String), // Timeout, Reset
    LockBusy,        // File locked

    // Environmental
    Io(std::io::Error),
    Config(String),
    
    // Logic
    NotFound(String),
}
```

### Implementation Steps
1.  Refactor `src/error.rs`.
2.  Review all `?` usages. Map `std::io::Error` to specific variants where possible.
3.  Update `net/sync.rs` to handle `Validation` errors by dropping/banning the peer, and `Network` errors by backing off.
4.  Update REPL to print friendly messages ("Network glitch, retrying..." vs "Invalid data received").
