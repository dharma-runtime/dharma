# Task 13: Zero-Bloat Crypto Handshake (Noise_XX)

## Goal
Replace the custom `src/net/handshake.rs` implementation with a standard **Noise_XX** pattern (Noise Protocol Framework).
This must be done **without adding new heavy dependencies** (like `rustls` or `snow`).
We will use the existing cryptographic primitives already in the project:
- `x25519-dalek` (Diffie-Hellman)
- `chacha20poly1305` (AEAD Encryption)
- `hkdf` / `sha2` (Hashing and Key Derivation)

## Why
- **Security:** Noise_XX provides mutual authentication, forward secrecy, and identity hiding.
- **Size:** Manual implementation avoids the overhead of generic libraries (~300KB for rustls, ~50KB for snow).
- **Correctness:** Standard patterns are easier to audit than custom rolling.

## Specification (Noise_XX)

The handshake consists of 3 messages.

**Notation:**
- `e`: ephemeral key pair
- `s`: static identity key pair
- `dh(u, r)`: Diffie-Hellman (ECDH) between key `u` (ours) and `r` (remote)
- `mix_hash(data)`: `h = SHA256(h || data)`
- `mix_key(data)`: derive new `ck` and `k` from `ck` and `data` using HKDF.
- `encrypt(k, n, ad, plaintext)`: ChaCha20Poly1305 encryption.

**Protocol State:**
- `h`: Handshake hash (32 bytes). Initialized to `SHA256("Noise_XX_25519_ChaChaPoly_SHA256")`.
- `ck`: Chaining key (32 bytes). Initialized to `h`.
- `k`: Encryption key (32 bytes). Empty if no key yet.
- `n`: Nonce (u64). Reset to 0 when `k` changes.

### Message 1: Alice (Client) -> Bob (Server)
**Pattern:** `-> e`
1. Generate ephemeral key pair `e`.
2. Write `e.public` (32 bytes) to buffer.
3. `mix_hash(e.public)`.
4. `mix_hash(payload)` (payload is empty).
5. Send buffer.

### Message 2: Bob (Server) -> Alice (Client)
**Pattern:** `<- e, ee, s, es`
1. Read Alice's `e.public`.
2. `mix_hash(e.public)` (Alice's).
3. Generate ephemeral key pair `e`.
4. Write `e.public` (32 bytes) to buffer.
5. `mix_hash(e.public)` (Bob's).
6. **DH(e, e):** Bob's `e` + Alice's `e`.
7. `mix_key(dh_output)`.
8. Encrypt Bob's static public key `s.public` using `k`. Write ciphertext to buffer.
9. `mix_hash(ciphertext)`.
10. **DH(s, e):** Bob's static `s` + Alice's ephemeral `e`.
11. `mix_key(dh_output)`.
12. Encrypt payload (empty) using `k`. Write ciphertext (just tag) to buffer.
13. `mix_hash(ciphertext)`.
14. Send buffer.

### Message 3: Alice (Client) -> Bob (Server)
**Pattern:** `-> s, se`
1. Read Bob's message. Process `e`, `ee`, decrypt `s`.
2. Encrypt Alice's static public key `s.public` using `k`. Write ciphertext.
3. `mix_hash(ciphertext)`.
4. **DH(s, e):** Alice's static `s` + Bob's ephemeral `e`.
5. `mix_key(dh_output)`.
6. **DH(s, s):** Alice's static `s` + Bob's static `s`.
7. `mix_key(dh_output)`.
8. Encrypt payload (empty) using `k`. Write ciphertext.
9. `mix_hash(ciphertext)`.
10. Send buffer.

### Transport Phase
1. Split `ck` into two keys: `k_send`, `k_recv`.
2. Handshake complete. Use `k_send` and `k_recv` for subsequent frames.

## Implementation Steps

1.  **Create `src/net/noise.rs`:**
    - Define `HandshakeState` struct.
    - Implement `initialize_initiator` and `initialize_responder`.
    - Implement `write_message` and `read_message`.
2.  **Helpers:**
    - `mix_hash`, `mix_key`, `encrypt_and_hash`, `decrypt_and_hash`.
3.  **Integration:**
    - Update `src/net/handshake.rs` to use `src/net/noise.rs`.
    - Retain the `Session` struct but initialize it with the split transport keys.
4.  **Verification:**
    - Ensure 1MB binary limit is respected.
    - Test roundtrip connection.

## References
- [Noise Protocol Framework](http://noiseprotocol.org/noise.html)
