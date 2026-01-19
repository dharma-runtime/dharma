# Completed Tasks (Reference)

This file documents the scope of completed work so each DONE item in todo.md can link to a deep description.

## Boot sequence: identity check, passphrase unlock, self head verification
- Entry path checks data dir + identity files.
- Unlocks keystore, loads identity, scans subject history to verify latest head.
- Emits status/usage messages per boot state.

## Identity init/export, encrypted keystore, config persistence
- Generates ed25519 identity + subject id.
- Writes identity key encrypted with Argon2.
- Stores dharma.toml with identity binding.
- Supports identity export guarded by unlock.

## Blocking TCP server/client, frame codec, handshake, sync loop
- Blocking TCP listener + client connect.
- Length-prefixed frame codec.
- Noise-lite handshake with X25519 + HKDF + HMAC.
- Encrypted session with ChaCha20-Poly1305.

## Frontier index (tips) for base assertions
- Builds tips by scanning assertions and removing referenced prevs.
- Tracks known object ids for sync diffing.

## Ingest pipeline: signature + structural validation + store commit
- Decodes assertion (envelope or plaintext).
- Verifies signature and structural chain.
- Writes assertions to filesystem store.

## DHL parser + compiler skeleton; wasm validate/reduce for Int/Bool/Enum/Identity
- Markdown + dh code block parsing.
- AST + schema/wasm codegen for basic types and expressions.

## ABI layout reserved (base 0x0000, overlay 0x1000, args 0x2000, context 0x3000)
- Fixed memory map for public/private state and args.
- Wasm codegen uses offsets consistently.

## Sidecar overlays: separate overlay log, split assertions, replay merge in runtime
- Overlay assertions stored separately from base log.
- Runtime merges base + overlay state for execution.

## Overlay replication with policy gating (overlays.policy; legacy allowlist fallback)
- Overlay inventory + GET/OBJ gating by policy.
- Fallback to overlays.allow if policy file is missing.

## Structured overlay policy per subject/namespace
- Policy parser supports subject/namespace/peer scoping.
- Allow/deny evaluation during sync.

## Recursive parent merge on compile for `extends`
- DHL extends merges parent schema/logic before compile.
