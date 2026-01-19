# Task 22: Capability Tokens (Authorization)

## Goal
Implement `CapToken`, the bearer token used to authorize Fabric requests and oracle access.
This replaces "sessions" or "ACL checks" at the data layer and enforces **domain sovereignty**.

## Why
- **Stateless:** Providers don't need to look up permissions in a DB. The token *is* the permission.
- **Delegatable:** Tokens can be attenuated and passed to workers.
- **Sovereign Control:** Domain owners define authorization levels and feature flags.

## Specification

### 1. CapToken Structure
Signed CBOR object.
```rust
struct CapToken {
    v: u8,                  // 1
    id: [u8; 32],           // Unique Token ID (nonce)
    issuer: IdentityKey,    // Authority signing this
    domain: String,         // e.g. "corp.ph.cmdv"
    level: String,          // e.g. "public", "partner", "admin"
    subject: Option<SubjectId>, // Specific subject (optional)
    scopes: Vec<Scope>,     // e.g., ["table:invoice", "namespace:com.acme.*"]
    ops: Vec<Op>,           // e.g., [Read, Execute]
    actions: Vec<String>,   // Allowed action names (optional allowlist)
    queries: Vec<String>,   // Allowed query names (predefined only)
    flags: Vec<Flag>,       // Feature flags (replication, custom query)
    oracles: Vec<OracleClaim>, // Oracle claims (optional)
    constraints: Vec<Constraint>, // e.g., { "row_limit": 100, "valid_until": ts }
    nbf: u64,               // Not Before
    exp: u64,               // Expires At
    sig: [u8; 64],          // Ed25519 Signature
}

enum Scope {
    Table(String),
    Namespace(String),
    Subject(SubjectId),
}

enum Op {
    Read, Write, Execute
}

enum Flag {
    AllowReplication,
    AllowCustomQuery,
}

struct OracleClaim {
    name: String,        // oracle name/topic
    mode: OracleMode,    // InputOnly | RequestReply | OutputOnly
    timing: OracleTiming, // Sync | Async
    domain: String,
}
```

### 2. Validation Logic
1.  **Crypto:** Verify `sig` against `issuer`.
2.  **Time:** `now >= nbf` AND `now < exp`.
3.  **Match:** Check if requested operation/scope matches token grants.
4.  **Domain:** Token domain must match request domain.
5.  **Level/Flags:** Enforce replication/custom-query flags.
6.  **Action/Query Allowlist:** if present, restrict to allowed names only.

### 3. Token Minting (Authority)
- The "Authority" (usually the user's identity or an Org Controller) signs these tokens.
- **REPL Integration:** `auth issue --scope ...` command.
- **Atlas Policy:** Domain policy defines valid `level` values and default flags.

## Implementation Steps
1.  Define `CapToken` in `src/fabric/auth.rs`.
2.  Implement `verify()` method.
3.  Implement `check_access(&self, op, scope) -> Result`.
