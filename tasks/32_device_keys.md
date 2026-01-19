# Task 32: Device Key Delegation (Identity V2)

## Goal
Clarify "Who is signing?". Move from "Identity Key signs everything" to "Device Keys sign, Identity Key delegates".

## Why
-   **Security:** If a laptop is stolen, you revoke the Laptop Key. You don't have to rotate your Root Identity Key (which would change your Subject ID).
-   **Concurrency:** Device A and Device B can sign independently without sequence collisions (if we scope sequence to device, or use the DAG).

## Specification

### 1. Identity Structure
*   **Root Key:** The `Subject ID` derivation key. Used *only* to sign `iam.delegate` assertions. kept in cold storage (or high security).
*   **Device Key:** Ephemeral/Hardware-bound key. Used to sign daily assertions.

### 2. Delegation Assertion (`iam.delegate`)
*   **Subject:** The Identity.
*   **Body:** `delegate: PubKey`, `scope: "all" | "chat"`, `expires: Timestamp`.
*   **Signed By:** Root Key (or an existing Admin Device Key).

### 3. Validation Logic
When verifying `Action A` signed by `Key K`:
1.  Verify `Ed25519(Action, K)`.
2.  Look up Identity Subject state.
3.  Verify `K` is in `state.delegates` and `scope` allows `Action A`.

## Implementation Steps
1.  **DHL:** Update `std.iam` to support this logic (already drafted in `contracts/std/iam.dhl`).
2.  **Keystore:** Support multiple keys (`root`, `device`).
3.  **Runtime:** Update `context.signer` resolution. It should return the **Identity ID**, not the Device Key. The Device Key is an implementation detail of the signature.
