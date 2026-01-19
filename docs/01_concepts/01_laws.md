# The Laws of DHARMA (Current + Intended)

These laws describe the invariants the kernel enforces **today**, and the ones it is designed to enforce as the system matures.

---

## Law 1: Truth is a Vector, Not a Value
The current state is derived by replaying accepted assertions. There is no in-place mutation.

**Current:** enforced by append-only logs and deterministic replay.

---

## Law 2: No Signature, No Truth
Every assertion must be signed, and signatures are verified before acceptance.

**Current:** signature is verified during ingest before an assertion is written to the subject log.

---

## Law 3: Logic Must Be Deterministic
Contracts must produce identical results on every node.

**Current:** no floats in kernel; Wasm runs in fixed memory layout.
**Gap:** `now()` currently reads local system time in REPL; deterministic env injection is planned.

---

## Law 4: Time Is an Assertion (Eventually)
Time must be a declared input, not an ambient system dependency.

**Current:** `header.ts` exists but is often `None`; REPL uses local time for `now()`.
**Planned:** `Env` abstraction will drive deterministic time (Task 12).

---

## Law 5: Sovereignty Is Local
A node chooses what to accept, what to sync, and which overlays to reveal.

**Current:** overlay policies exist; peer trust policy can block peers.

