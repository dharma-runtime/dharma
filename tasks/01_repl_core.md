# Task: REPL Core Implementation

## Objective
Implement the interactive Read-Eval-Print Loop (REPL) for DHARMA, acting as the primary user interface as described in the User Guide.

## Requirements
- **Library**: Use `rustyline` or `reedline` for input handling (history, editing).
- **Entry Point**: Add `dh repl` command to `src/main.rs`.
- **Shell State**: Maintain a session state containing:
  - Current identity (unlocked/locked).
  - Current subject (SubjectId + Alias).
  - Current lens (Data Version).
  - Current profile (Home/Pro/HighSec).
  - Overlay settings.
- **Commands to Implement**:
  - `help`: Context-aware help.
  - `exit` / `quit`: Graceful shutdown.
  - `clear`: Clear screen.
  - `:set`: Key-value configuration (`profile`, `json`, `color`, `confirmations`).
  - `version`: Show build info.
  - `alias`: `set`, `rm`, `list` (persist aliases in `dharma.toml` or separate `aliases.toml`).
  - `subjects`: List local subjects (`recent`, `mine`).
  - `use`: Switch context to a subject (by ID or alias).
  - `pwd`: Print current context.
  - `identity`: `status`, `unlock`, `lock`, `whoami` (integrate existing logic).

## Implementation Details
- Create `src/repl/mod.rs` and submodules.
- Define a `ReplContext` struct.
- Implement command parsers (nom or simple string splitting).
- Ensure "Golden Transcript 01" flow works (init -> subjects -> use).
