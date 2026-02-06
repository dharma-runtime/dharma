# Task 64: Dharma Vault (Glacier Tiered Storage)

## Goal
Deliver **bank/CIA‑grade durability and integrity** for Dharma history while keeping the **UX simple**:
- **Infinite history without infinite local storage**
- **Client‑side privacy** (providers never see plaintext or keys)
- **Cryptographic verifiability** (cold chunks proven against hot log checkpoints)
- **Seamless restore** (new device can fully recover with minimal prompts)

---

## 1) Core Philosophy (Non‑Negotiable)
- **Compress locally** (bandwidth/storage efficient).
- **Encrypt locally** (zero trust to any storage provider).
- **Verify remotely** (every cold chunk anchored by a signed Checkpoint in the hot log).
- **Cloud is dumb storage** (blob store; no trust in provider).

---

## 2) Threat Model (Explicit)
We must tolerate:
- **Malicious storage** (tamper, reorder, replace, replay, delete).
- **Partial corruption** (bit rot, truncated objects).
- **Key compromise on a single device** (limit blast radius).
- **Cold‑only restore** (fresh node from remote chunks).

We must prevent:
- **Undetected tampering** (hash + checkpoint verification).
- **Chunk replay/swap** across subjects or ranges (AAD binds subject + range).
- **Silent data loss** (no prune until verified).

### Side-channel leakage (documented trade-off)
Storage providers can still observe:
- Upload timing
- Chunk sizes
- Access patterns

Mitigations (optional, configurable):
- Fixed-size padding
- Dummy uploads
- Batch uploads (delay + group)

---

## 3) Configuration & Keys Strategy (Hybrid Model)

### A) Boot Config (Local Only)
Needed before any sync or vault access.
Stored in `~/.dharma/config.toml` and OS Keychain / Secure Enclave:
- **Identity Root Key** (OS Keychain / hardware‑backed).
- **Vault Credentials** (AWS keys, Arweave wallet, peer token).
- **Vault Pointer** (e.g., `s3://bucket/prefix`).

### B) Synced Config (Encrypted Subject)
Stored in encrypted subject (e.g., `sys.config`) and synced:
- UI preferences, aliases, trusted peers, integrations.
- **Never contains secrets** needed to access vault storage.

### Recovery UX (Simple Wizard)
Wizard prompt:
1) “Do you have a backup?”
   - **No** → Start fresh
   - **Dharma Cloud** → Login
   - **Private S3** → Endpoint + Access Key
   - **Friend / Peer** → Scan QR or paste peer token

---

## 4) Crypto & Key Derivation (Bulletproof)

### Vault Master Key
- **VMK** derived from identity root key via HKDF with salt:
```
VMK = HKDF(root_key, salt=identity_id, info="dharma:vault:master")
```
- Salt prevents identical passphrases from producing identical VMKs.

### Subject Vault Key
- Per subject and epoch:
```
SVK = HKDF(VMK, "dharma:vault:subject" || subject_id || epoch)
```

### Chunk Key (CIA‑grade)
- Each chunk uses a unique key:
```
CK = HKDF(SVK, "dharma:vault:chunk" || seq_start || seq_end || chunk_salt)
```
- `chunk_salt` is a random 32‑byte value stored in the header.

### Encryption
- **XChaCha20‑Poly1305**
- **AAD** = subject_id || seq_start || seq_end || schema_id || contract_id  
  Prevents replay or chunk substitution.

---

## 5) `.dhbox` Cold Chunk Format (Versioned)
**Dharma Box** = cold archive segment.

### Trigger
- Threshold: `N assertions` OR `~10MB` log.

### Chunk sizing (configurable)
Default values:
```
[vault]
chunk_size_mb = 10
chunk_assertions = 10_000
```
- Mobile / low-bandwidth: 1-5 MB chunks
- Enterprise / fast links: 25-50 MB chunks

### Pipeline
1) Serialize → CBOR
2) Compress → Zstd‑19 (with optional trained dictionary)
3) Encrypt → XChaCha20‑Poly1305
4) Hash → BLAKE3 of ciphertext

### Compression dictionary training (required)
Train a Zstd dictionary on real assertion samples to improve compression ratios.
Expected gain: **20-40% smaller .dhbox** (faster upload/restore).

Sample flow:
```
// Collect sample assertions
let samples: Vec<Vec<u8>> = collect_sample_assertions();

// Train dictionary (32KB)
let dict = zstd::dict::from_samples(&samples, 32_768)?;

// Compress with dictionary
let compressed = zstd::encode_all(&data, 19, &dict)?;
```

Dictionary storage options:
- **Inline** in `.dhbox` header (<=32KB).
- **Referenced by hash** (dedup across chunks).

### Format (v1)
```
Header:
  magic = "DHBOX"
  version = 1
  subject_id = 32 bytes
  seq_start, seq_end = u64
  assertion_count = u32
  schema_id = 32 bytes
  contract_id = 32 bytes
  snapshot_hash = 32 bytes
  merkle_root = 32 bytes
  chunk_salt = 32 bytes
  dict_hash = 32 bytes (optional)
  dict_len = u32 (optional)
  dict_inline = bytes (optional)
  compression = enum
  encryption = enum
  nonce = 24 bytes

Body:
  ciphertext = Encrypt(Compress(CBOR({ assertions[], snapshot })))
```

### Merkle proofs (partial verification)
`merkle_root` enables verifying a single assertion without downloading the full chunk.
- Proof size: ~log2(N) hashes
- Use for: "prove this fact exists" without full restore

---

## 6) `sys.vault.Checkpoint` Assertion (Anchoring)

### Contract (`contracts/std/sys_vault.dhl`)
```dhl
type StorageDriver = Enum(Local, S3, Arweave, IPFS, Filecoin, Peer)
type EncryptionAlg = Enum(XChaCha20_Poly1305)
type CompressionAlg = Enum(Zstd_19)

struct VaultShard
    public driver: StorageDriver
    public location: Text(len=256)
    public hash: Hash
    public size: Int
    public shard_index: Int
    public shard_total: Int

struct VaultRef
    public driver: StorageDriver
    public location: Text(len=256)
    public hash: Hash            // BLAKE3 ciphertext hash
    public size: Int
    public compression: CompressionAlg
    public encryption: EncryptionAlg
    public format_version: Int
    public subject: Hash
    public seq_start: Int
    public seq_end: Int
    public snapshot_hash: Hash
    public merkle_root: Hash
    public dict_hash: Hash?
    public dict_size: Int?
    public shards: List<VaultShard>?

action Checkpoint(
    start_seq: Int,
    end_seq: Int,
    state_root: Hash,
    vault: VaultRef
)
```

**Verification Rule:** Checkpoint must match `.dhbox` header + hash exactly.  
**No prune until verified.**

**Monotonicity Rule:**  
- `start_seq >= last_checkpoint.end_seq`  
- `end_seq > start_seq`

---

## 7) Vault Lifecycle (State Machine)
```
Pending -> Uploading -> Uploaded -> Verified -> Pruned
```
- Upload failure → retry, no prune.
- Verify failure → abort prune, mark corrupt.
- Checkpoint failure → keep chunk locally.

---

## 8) Storage Drivers (Hard Security)

### A) S3Driver (Enterprise)
Security profile: **Paranoid**
- **Object Lock** (Compliance mode; 7‑year retention)
- **Block Public Access**
- **IAM policy**: Put/Get only (no delete)
- **Versioning** enabled

### B) ArweaveDriver (Permanent)
Security profile: **Immutable**
- Encrypted client‑side
- Public storage, but ciphertext useless without keys
- Bundling via Irys/Bundlr for instant uploads

### C) PeerDriver (Friend Mode)
- Sync `.dhbox` over Dharma Noise
- Stored at `~/.dharma/vault/peers/<your_id>/`
- Peer never gets keys

### D) LocalDriver (Fallback / Air‑gapped)
- Secondary disk / NAS / external drive

### E) Erasure Coding (Optional, Ultra‑Paranoid)
For critical data, split a chunk into shards with k-of-n recovery.
Example: 3-of-5 shards across mixed providers:
- Shard 1 -> S3
- Shard 2 -> Arweave
- Shard 3 -> Peer
- Shard 4 -> Local backup
- Shard 5 -> Second S3 region

Recovery: any 3 shards reconstruct the original chunk.
Trade‑off: higher storage + bandwidth cost.

---

## 9) Vault Sync Optimization (Bandwidth‑Efficient)
Peers exchange **checkpoint ranges**, not full blobs:
```
Node A: "I have chunks 1-100"
Node B: "I have chunks 1-95, 97-100"
Node A: "Send chunk 96"
```
Range-based sync mirrors git pack logic:
- Advertise seq ranges and chunk hashes
- Transfer only missing chunks
- Optional: request merkle proof for single assertion

---

## 10) UX: The “Safety Slider”

Level 1: **Standard** (Free)
- Hot: Local device
- Cold: Encrypted backup to Dharma relay (cap 100MB)
- Setup: Zero config

Level 2: **Secure** (Friend Mode)
- Hot: Local
- Cold: Mirror to trusted peer device
- Setup: Scan QR

Level 3: **Sovereign** (Pro)
- Hot: Local
- Cold: Arweave (identity/contracts) + S3 (logs)
- Setup: “Login with AWS” or “Connect Wallet”

---

## 11) Implementation Plan

1. **Core (`dharma-core/src/vault`)**
   - `VaultSegment` struct
   - Compression/encryption pipeline
   - Zstd dictionary training + caching
   - Configurable chunk sizing
   - Merkle root + snapshot hash
   - `VaultDriver` trait

2. **Drivers (`dharma-core/src/vault/drivers`)**
   - `LocalDriver`
   - `S3Driver` (AWS SDK)
   - `ArweaveDriver` (Irys/Bundlr)
   - `PeerDriver`
   - Optional erasure coding wrapper (k-of-n)

3. **Contract**
   - Add `contracts/std/sys_vault.dhl`

4. **Runtime Integration**
   - Archival trigger in `store::state`
   - Background job with state machine
   - Checkpoint commit on success
   - Prune local hot log after verify
   - Merkle proof API for partial verification

5. **CLI**
   - `dh vault setup s3`
   - `dh vault setup arweave`
   - `dh vault archive`
   - `dh vault restore`
   - `dh vault verify <subject> <seq>`

---

## 11.1) Execution Subtasks (Split)
- **64.1 Vault Core + .dhbox Format** (tasks/64_1_vault_core_format.md)
- **64.2 Vault Drivers** (tasks/64_2_vault_drivers.md)
- **64.3 sys.vault Contract + Checkpoint Rules** (tasks/64_3_sys_vault_contract.md)
- **64.4 Runtime Archival + Restore Pipeline** (tasks/64_4_vault_runtime_integration.md)
- **64.5 Vault CLI + Recovery UX** (tasks/64_5_vault_cli_ux.md)
- **64.6 Vault Sync + Merkle Proofs** (tasks/64_6_vault_sync_merkle.md)
- **64.7 Vault Test Plan + Harness** (tasks/64_7_vault_tests.md)

---

## 12) Test Plan (CIA‑grade)

### Format / Crypto
- Deterministic `.dhbox` header parsing
- Corrupt ciphertext → hash mismatch
- Replay attempt (AAD mismatch) rejected
- Dictionary training improves compression ratio (>=20% on sample set)
- Dict hash mismatch rejects decode

### Storage Drivers
- S3 put/get + immutability policy check
- Arweave upload + retrieval
- PeerDriver sync and restore
- Erasure coding: reconstruct with any k-of-n shards

### Checkpoint Integrity
- Checkpoint mismatch → abort prune
- Missing chunk → restore fails
- Tampered chunk → restore fails
- Monotonic checkpoint enforcement (no rollback / no gaps)

### Restore / Replay
- Cold‑only recovery reproduces identical frontiers
- Mixed restore + hot log merge produces identical state
- Merkle proof verifies single assertion without full download

### Sync Optimization
- Range-based checkpoint diff returns only missing chunks
- Peer requests chunk by hash; no redundant transfer

### Failure Modes
- Interrupted upload → retry
- Corrupt chunk → redownload or fail safe
- Key mismatch → hard reject (no partial decode)
- Chunk size config honored (1MB, 10MB, 50MB)

---

## 13) Success Criteria
- 10k assertions → 1 `.dhbox`.
- `.dhbox` uploaded to mock S3.
- Hot log pruned only after verified checkpoint.
- New node restores and converges to identical frontier/state.
