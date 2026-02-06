# Identity & Keys (Current)

DHARMA identity is a subject with its own assertion history, backed by cryptographic keys.

---

## 1) Key Types

- **Root key**: long-lived authority for an identity
- **Device key**: operational signing key
- **Subject key**: symmetric key for envelope encryption

Keys are stored in an encrypted keystore (Argon2).

---

## 2) Identity Lifecycle

1) `identity init <alias>`
2) Generates keys + subject
3) Writes `core.genesis` and `identity.profile`
4) Stores encrypted keystore

---

## 3) Delegation (Current)

Delegation is expressed by `iam.delegate` assertions and checked in ingest.

- Delegates are granted a scope string.
- Revocations use `iam.revoke` or `iam.delegate.revoke`.

> Note: scopes support `all`, exact match, hierarchical prefixes (for example `finance` matches `finance.approve`), and glob wildcards (`*`, `?`). Legacy `chat` scope still matches actions containing `chat`.

---

## 4) Verification Status

There is no global registry yet. Identities are **local and unverified** by default.

---

## 5) Planned

- Verified handle registry (Atlas)
- Capability tokens
- Device key rotation UX
