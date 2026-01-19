# Task 29: REPL Polish (TUI & Colors)

## Goal
Elevate the DHARMA REPL from a "raw socket" feel to a "modern developer tool" feel.
Prioritize readability, interactivity, and aesthetics without sacrificing speed.

## Scope (CLI Only)
These changes apply **only** to `dharma-cli`. The Kernel (`dharma-runtime`) remains silent and raw.

## Specification

### 1. The "Powerline" Prompt
Replace the single line `user@dh >` with a structured, two-line status dashboard.

**Layout:**
```text
┌─[👤 Identity]─[📄 Subject]─[👓 Lens]───[📡 Network]─[🛡️ Profile]
└─> 
```

**Components:**
- **Identity:** Alias (Green) + Lock Status (`🔓`/`🔒`).
- **Subject:** Alias or Hash (Blue) + Dirty Flag (`*`).
- **Lens:** Version number (`v1`).
- **Network:** Status (`Online`/`Offline`) + Peer Count (`👥 3`).
- **Profile:** Current config (`Home`/`Pro`/`HighSec`).

### 2. Rich Output Rendering
- **JSON:** Syntax highlighting for `state --json` and `show`.
- **Tables:** `tabled` formatting for DHARMA-Q results and `subjects` list.
- **Diffs:** Visual diffs (Green `+`, Red `-`) for state changes.

### 3. Interactive Prompts (`inquire` crate)
- **Select:** `use` command offers fuzzy-searchable list.
- **Confirm:** `commit` transaction card allows interactive review.
- **Password:** Secure masked input for `identity unlock`.

### 4. Progress Feedback
- **Spinners:** `indicatif` spinners for long-running `sync` or `index build`.

## Implementation Steps
1.  **Dependencies:** Add `crossterm`, `tabled`, `inquire`, `indicatif`, `syntect` (optional for high-quality highlights) to `dharma-cli/Cargo.toml`.
2.  **Prompt Logic:** Rewrite `ReplContext::prompt()` to generate the two-line ANSI string.
3.  **Refactor Commands:** Update `handle_state`, `handle_use` to use rich rendering.
4.  **Transaction Card:** Implement a beautiful boxed summary for `commit action`.
