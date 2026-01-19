# Task: State, History, and Navigation

## Objective
Implement commands for inspecting the state and history of the current subject.

## Requirements
- **State Display**:
  - `state`: Render derived state using the current contract/lens.
  - Support `--json` flag for machine consumption.
  - Support `--at <object_id>` for time-travel (requires replaying up to a specific point).
  - Support `--lens <ver>` to switch interpretation dynamically.
- **History**:
  - `tail [n]`: Show recent `n` accepted assertions.
  - `log [n]`: Verbose history with headers.
  - `show <object_id>`: Decode and display a specific assertion (header, body, signature).
- **Status**:
  - `status`: Show frontier tips, pending count, rejected count, snapshot status.
  - `status --verbose`: Detailed health report.

## Implementation Details
- Leverage `src/store/state.rs` for state loading.
- Enhance `RuntimeVm` to support "stop-at" replay.
- Implement pretty-printers for assertions and state (using generic CBOR to JSON/Text conversion).
