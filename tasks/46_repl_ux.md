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
