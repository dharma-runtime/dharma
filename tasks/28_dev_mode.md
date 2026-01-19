# Task 28: Dev Mode (Hot Reload + Live CEL)

## Goal
Enable a "Live Coding" experience where developers can interact with DHL contracts and state in real-time, with hot-reloading of schema changes.

## Why
- **Feedback Loop:** Compiling and running a full node to test a logic change is slow.
- **Exploration:** Users want to query state (`state.amount`) without typing `dh state --json | jq ...`.

## Specification

### 1. The CEL Interpreter (`dharma-core`)
Implement a Tree-Walk Interpreter for the Common Expression Language (CEL).
- **Input:** `Expr` AST + `Context` (State, Args).
- **Output:** `Value`.
- **Usage:**
    - DHARMA-Q (Filter predicates).
    - REPL (Live interaction).
    - DHL (Optional "Debug Execution").

### 2. REPL "Direct Mode"
Modify `src/repl/core.rs`:
- If input is not a command (`:` or word), try to parse as CEL.
- If valid CEL, evaluate against current subject state.
- Print result.

**Example:**
```bash
> state.status
'Open
> len(state.items)
5
```

### 3. Hot Reloading
- **Command:** `dh repl --dev <file.dhl>`
- **Logic:**
    - On startup, compile `file.dhl`.
    - Create a temporary in-memory subject using this schema.
    - **Loop:** Check file modification time (`mtime`) before every prompt.
    - **Change:** If changed, recompile.
        - If compile succeeds: Hot-swap the schema/contract. Replay history to migrate state (best effort).
        - If compile fails: Print error, keep old schema.

### 4. "Draft" Assertions
- In Dev Mode, assertions generated are **Ephemeral**.
- They are not written to disk (or written to a temp `.dharma/dev` dir).
- Allows "Sandbox" play without polluting the main chain.

### Architecture
-   **Server:** `dh lsp`. Standard JSON-RPC over stdio.
-   **Analysis:** Uses `dharma-core` Parser and TypeChecker.

## 6. Tree-sitter Grammar
**Goal:** Provide high-performance incremental parsing for IDEs.

### Features
-   **Markdown Injection:** Seamless parsing of `dhl` blocks within `.md` or `.dhl` files.
-   **Full Logic Coverage:** Parsers for `aggregate`, `action`, `flow`, `reactor`, and all CEL operators.
-   **Queries:** Support for DHARMA-Q pipeline syntax.

## Implementation Steps
1.  **Interpreter:** Implement `eval(expr, context)` in `dharma-core`.
2.  **REPL Loop:** Hook `eval` into the main input loop.
3.  **Watcher:** Add simple mtime check in `ReplContext`.
4.  **Grammar:** Create `tree-sitter-dharma` repository/module.
5.  **LSP Skeleton:** Implement `dh lsp` command and basic `textDocument/didChange` handler in `dharma-cli`.
6.  **Sandbox:** Implement `MemoryStore` for the Dev Mode backend.
