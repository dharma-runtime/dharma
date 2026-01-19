# Task: REPL Extended Commands

## Objective
Implement the REPL commands in the user guide that go beyond core REPL + state/history + action/audit.

## Requirements
- Overlay commands:
  - overlay status/list/enable/disable/show
  - overlay status must indicate base-only vs merged
- Peers and sync commands:
  - peers, peers --verbose
  - sync now, sync subject [id]
  - connect <addr>
  - discover on/off/status
- Package commands (surface-level wiring to package manager):
  - pkg list/show/install/verify/pin/remove
- Indexing commands:
  - index status/build/drop
  - find/vfind/gfind
  - open
- Export/import + maintenance:
  - export/import bundles
  - check, gc, snapshot list/make/prune

## Implementation Details
- Use existing network and store modules; add REPL shims.
- Keep output stable and aligned with user_guide examples.
- Any command that requires unlocked identity should check lock state.

## Acceptance Criteria
- All listed commands exist in REPL and return structured output.
- Help text lists the commands grouped by category.
