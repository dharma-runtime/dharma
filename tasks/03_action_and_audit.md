# Task: Actions and Auditing

## Objective
Implement the safe mutation workflow (Dry-Run / Commit) and auditing tools.

## Requirements
- **Actions**:
  - `dryrun action <ActionName> [k=v...]`:
    - Simulate execution without writing.
    - Show validation result, authority check, and **state diff**.
  - `commit action <ActionName> [k=v...]`:
    - Execute and persist.
    - If `profile=highsec` or `confirmations=on`, show a "Transaction Card" and await `yes`.
- **Auditing**:
  - `why <path>`:
    - Trace back which assertions contributed to a value.
    - Requires tracking "provenance" during reduction (VM instrumentation).
  - `prove <object_id>`:
    - Re-run the full validation pipeline for an object and report every step (sig, schema, contract).
  - `authority <ActionName>`:
    - Explain why the current user is allowed to perform an action (contract rules).
  - `diff`:
    - `--at <tipA> <tipB>`: Compare two states.
    - `--lens <v1> <lens2>`: Compare interpretations.

## Implementation Details
- **VM Instrumentation**: Modify `RuntimeVm` to optionally log read/write access to memory/state to support `why`.
- **Diff Logic**: Implement a JSON-diff or recursive map diff utility.
- **Transaction Card**: specific UI renderer for the confirmation step.
