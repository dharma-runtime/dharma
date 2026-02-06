# Task 46: REPL UX Overhaul (Intelligent Shell)

## Goal
Transform the REPL into the primary, ergonomic interface for DHARMA users.
Focus on discoverability, autocomplete, and streamlined workflows.

## Specification

### 1. Entry Point
-   `dh` (no args) -> Launches REPL.
-   **Auto-Unlock:** On startup, if identity exists but is locked, prompt for password immediately.
    ```text
    Welcome to DHARMA v1.0.
    Identity: JeffDean (0xabc...)
    Password: ****
    dh>
    ```

### 2. Command Hierarchy (Grammar)

#### IDENTITY (`id`)
- `id init <name> <email> <password>`
- `id status`
- `id unlock <password>`
- `id lock`
- `id whoami`
- `id export`

#### ALIAS (`alias`)
- `alias set <name> <value>`
- `alias rm <name>`
- `alias ls`

#### CONFIG (`conf`)
- `conf show`

#### CONTRACTS (`ct`)
- `ct ls`
- `ct info <contract>`
- `ct schema <contract>`
- `ct actions <contract>`
- `ct reactors <contract>`

#### PACKAGES (`pkg`)
- `pkg ls`
- `pkg search`
- `pkg local`
- `pkg installed`
- `pkg show <package>`
- `pkg install <package>`
- `pkg uninstall <package>`
- `pkg verify`
- `pkg pin <package>`
- `pkg build <path>`
- `pkg publish <package>`

#### SUBJECTS (`ls`)
- `ls` (List all)
- `ls recent`
- `ls mine`
- `ls c <contract>`

#### NETWORK (`net`)
- `net peers [--json|--verbose]`
- `net connect <addr>`
- `net sync [now|subject <id>]`

#### ACTIVE SUBJECT (Context)
- `new <contract>`
- `use <id|alias>`
- `do <Action> [k=v...]`
- `try <Action> [k=v...]`
- `can [Action] [k=v...]`
- `why [path]`
- `state`
- `info`

#### SESSION (Global)
- `tail [n]`
- `log [n]`
- `show <id> [--json|--raw]`
- `overlay <status|list|enable|disable|show>`
- `pwd`
- `version`
- `help [command]`
- `exit`

### 3. Intelligent Autocomplete
Implement `rustyline::Helper` traits (`Completer`, `Hinter`, `Validator`) to provide context-aware suggestions.

#### Dynamic Completion Logic
| Context | Input Trigger | Suggestion Source |
| :--- | :--- | :--- |
| **Global** | `use ` | Local Aliases + Recent Subject IDs |
| **Global** | `new `, `ct *`, `ls c ` | Installed Contracts (`dharma.toml` or Store) |
| **Global** | `pkg install ` | Registry Index (Cached) |
| **Subject** | `do `, `try `, `can ` | Actions defined in the Current Subject's Contract |
| **Action** | `do Transfer ` | Argument names (`amount=`, `to=`) |
| **Network** | `net connect ` | Known Peer Addresses |

#### Hints
-   Show faint text hints for arguments:
    -   `do Transfer` -> `(amount: Int, to: Identity)`

### 4. Implementation Steps
1.  **Refactor Parser:** Move from simple string splitting to a recursive command parser (or `clap` with `multicall` if feasible, but likely custom `nom` or `match` logic is lighter).
2.  **Context Struct:** Enhance `ReplContext` to hold `current_subject`, `history`, and `identity_cache`.
3.  **Completer:** Create a `DharmaCompleter` struct that holds references to `Store` and `State`.
4.  **Startup Flow:** Modify `main.rs` to detect no-args mode and trigger the unlock loop.
5.  **Help System:** `help <command>` should traverse the hierarchy (e.g. `help id unlock`).

## Success Criteria
-   User can perform a full workflow (Create -> Action -> Sync) relying mostly on `<TAB>` completion.
-   The REPL feels "alive" (hints appear as you type).
-   `dh` command opens the shell immediately.

---

## Implementation Checklist (Command-by-Command)
Legend: [x] implemented as spec, [~] partially implemented / mismatch, [ ] missing

### Entry Point & Startup
- [x] `dh` (no args) launches REPL. (`dharma-cli/src/lib.rs`)
- [x] Auto-unlock prompt when identity exists but locked. (`dharma-cli/src/repl/mod.rs`)
- [~] Auto-unlock retry loop on failure (spec implies prompt-on-start; current behavior errors and continues locked). (`dharma-cli/src/repl/mod.rs`)

### IDENTITY (`id`)
- [~] `id init <name> <email> <password>` (email not stored/used; password optional; alias only). (`dharma-cli/src/repl/core.rs`, `dharma-core/src/identity_store.rs`)
- [x] `id status` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `id unlock <password>` (implemented; password optional prompt). (`dharma-cli/src/repl/core.rs`)
- [x] `id lock` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `id whoami` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `id export` (implemented). (`dharma-cli/src/repl/core.rs`)

### ALIAS (`alias`)
- [x] `alias set <name> <value>` (implemented; value defaults to current subject). (`dharma-cli/src/repl/core.rs`)
- [x] `alias rm <name>` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `alias ls` (implemented as `alias list`; `alias ls` normalized). (`dharma-cli/src/repl/core.rs`)

### CONFIG (`conf`)
- [x] `conf show` (implemented as `config show`). (`dharma-cli/src/repl/core.rs`)

### CONTRACTS (`ct`)
- [x] `ct ls` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `ct info <contract>` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `ct schema <contract>` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `ct actions <contract>` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `ct reactors <contract>` (implemented). (`dharma-cli/src/repl/core.rs`)

### PACKAGES (`pkg`)
- [x] `pkg ls` (implemented as `pkg list`). (`dharma-cli/src/repl/core.rs`)
- [x] `pkg search` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `pkg local` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `pkg installed` (implemented as `pkg list`). (`dharma-cli/src/repl/core.rs`)
- [x] `pkg show <package>` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `pkg install <package>` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `pkg uninstall <package>` (implemented as `pkg remove`). (`dharma-cli/src/repl/core.rs`)
- [x] `pkg verify` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `pkg pin <package>` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `pkg build <path>` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `pkg publish <package>` (implemented). (`dharma-cli/src/repl/core.rs`)

### SUBJECTS (`ls`)
- [x] `ls` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `ls recent` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `ls mine` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `ls c <contract>` (implemented as `subjects contract`). (`dharma-cli/src/repl/core.rs`)

### NETWORK (`net`)
- [~] `net peers [--json|--verbose]` (works via normalization to `peers`; `net` itself is not a namespace command). (`dharma-cli/src/repl/core.rs`)
- [~] `net connect <addr>` (works via normalization to `connect`). (`dharma-cli/src/repl/core.rs`)
- [~] `net sync [now|subject <id>]` (works via normalization to `sync`). (`dharma-cli/src/repl/core.rs`)

### ACTIVE SUBJECT (Context)
- [x] `new <contract>` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `use <id|alias>` (implemented; interactive selector when no args). (`dharma-cli/src/repl/core.rs`)
- [x] `do <Action> [k=v...]` (implemented as `commit action`). (`dharma-cli/src/repl/core.rs`)
- [x] `try <Action> [k=v...]` (implemented as `dryrun action`). (`dharma-cli/src/repl/core.rs`)
- [x] `can [Action] [k=v...]` (implemented as `authority`). (`dharma-cli/src/repl/core.rs`)
- [x] `why [path]` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `state` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `info` (implemented). (`dharma-cli/src/repl/core.rs`)

### SESSION (Global)
- [x] `tail [n]` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `log [n]` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `show <id> [--json|--raw]` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `overlay <status|list|enable|disable|show>` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `pwd` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `version` (implemented). (`dharma-cli/src/repl/core.rs`)
- [x] `help [command]` (implemented; single-token dispatch). (`dharma-cli/src/repl/core.rs`)
- [~] `help <command>` hierarchical (spec calls for `help id unlock`; current implementation only uses first token). (`dharma-cli/src/repl/core.rs`)
- [x] `exit` (implemented). (`dharma-cli/src/repl/core.rs`)

### Autocomplete & Hints
- [x] Global command completion (top-level commands). (`dharma-cli/src/repl/mod.rs`)
- [x] `use` completes aliases + recent subjects. (`dharma-cli/src/repl/mod.rs`)
- [x] `new`, `ct *`, `ls c` complete contract names. (`dharma-cli/src/repl/mod.rs`)
- [x] `do`/`try`/`can` complete action names and arg keys. (`dharma-cli/src/repl/mod.rs`)
- [~] Action hints show arg signature; action list is based on current lens schema, not the active subject’s contract. (`dharma-cli/src/repl/mod.rs`)
- [~] `net connect` peer address suggestions (not implemented). (`dharma-cli/src/repl/mod.rs`)
- [~] `pkg install` registry suggestions (not implemented). (`dharma-cli/src/repl/mod.rs`)

### Validation / UX polish
- [~] Input validation: `Validator` is a stub (always valid). (`dharma-cli/src/repl/mod.rs`)
