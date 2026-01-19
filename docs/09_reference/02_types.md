# Type System (Current Runtime)

This document describes **how types are represented today** in the DHARMA compiler and runtime.

---

## 1) Primitive Types

| Type | Stored As | Notes |
| --- | --- | --- |
| `Int` | i64 (8 bytes) | Signed 64-bit integer |
| `Bool` | u8 (1 byte) | `0` or `1` |
| `Text(len=N)` | 4-byte length + N bytes | Default `N = 64` |
| `Timestamp` | i64 (8 bytes) | Seconds since epoch |
| `Duration` | i64 (8 bytes) | Seconds |
| `Decimal(scale=K)` | i64 mantissa | Fixed point (K digits) |
| `Ratio` | 16 bytes | `(num:i64, den:i64)` |
| `Currency` | Text | Treated as `Text(len=64)` today |
| `GeoPoint` | 8 bytes | Two i32 components (interpretation left to contract; commonly lat_e7/lon_e7) |

> **Note:** `GeoPoint` literals are not supported in DHL expressions. Action args accept `lat,lon` integer pairs.

---

## 2) Identity & References

| Type | Stored As | Notes |
| --- | --- | --- |
| `Identity` | 32 bytes | Ed25519 public key bytes |
| `Ref<T>` | 32 bytes | SubjectId (no runtime enforcement of `T`) |

---

## 3) Enums

```
Enum(Open, Done)
```

- Stored as a u32 index into the variant list.
- Enum literals must use the `'Variant` syntax.

---

## 4) Optional Types

```
Timestamp?
Text(len=64)?
```

- Encoded as `1 byte presence flag + value bytes`.
- `null` is accepted for optional action args and defaults.

---

## 5) Collections

### `List<T>`

Lists are encoded as:

- `u32 length`
- `length * sizeof(T)` bytes

List capacity is derived from `DEFAULT_COLLECTION_BYTES` (currently 128 bytes) and the element size.

### `Map<K, V>`

Maps are encoded as:

- `u32 length`
- repeated `(key, value)` pairs

Map capacity is derived from key/value sizes and `DEFAULT_COLLECTION_BYTES`.

> **Limitations:** Action arguments do not yet accept list/map literals. Mutations are supported in `apply` via `push/remove/set` syntax.

---

## 6) Determinism Guarantees

- Floating point types are **not supported**.
- All text is length-prefixed with fixed max length.
- Collections have fixed maximum capacities.

---

## 7) Unsupported Types (Planned)

The following appear in draft contracts but are **not implemented** in the compiler/runtime yet:

- `ObjectId`
- `PubKey`
- `delete(map, key)`
