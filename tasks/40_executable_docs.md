# Task 40: Executable Documentation (Literate Testing)

## Goal
Ensure that all documentation (CLI commands, REPL sessions, DHL examples) is **executable and correct**.
If the documentation says a command works, `dh test --doc` must prove it.

## Scope
1.  **CLI Commands:** Verify arguments, flags, and output formats.
2.  **REPL Sessions:** Simulate a stateful REPL session (input/output matching).
3.  **DHL Examples:** Compile and run every ````dh` block in the specs.

## Implementation Plan

### 1. The Runner (`dharma-test --doc`)
-   Create a runner that accepts a list of Markdown files.
-   Parse CommonMark to extract code blocks.
-   **Directives:** Support annotations in comments to guide the runner:
    -   `<!-- fail -->`: Expect exit code != 0.
    -   `<!-- env: KEY=VAL -->`: Set environment variables.
    -   `<!-- session: name -->`: Link multiple blocks into a stateful session.

### 2. CLI Testing (Bash Blocks)
-   Execute ````bash` blocks as shell commands.
-   Create a temporary `DHARMA_ROOT` for isolation.
-   Compare STDOUT/STDERR with the content following the command.

### 3. REPL Testing (Interactive Session)
-   **Mechanism:** Spawn `dh repl` using a PTY.
-   **Syntax:** Use `dh-repl` blocks with a `> ` prompt for inputs:
    ```dh-repl
    > :ls
    No subjects found.
    > :use default
    Switched to default.
    ```
-   **Logic:**
    1.  Verify the language ID is `dh-repl`.
    2.  Identify lines starting with `> ` as user inputs.
    3.  Treat all other lines as expected output.
    4.  Compare character-by-bit (or regex for dynamic IDs).

### 4. DHL Testing (Dharma Blocks)
-   Extract ````dhl` blocks.
-   Attempt to `dh compile` them.
-   If they contain `test { ... }` (future feature) or assertions, execute them.

## Target Documents
-   `docs/user_guide.md` (The primary flow).
-   `docs/language.md` (Syntax checks).
-   `docs/02_tutorial/01_quickstart.md` (The Hello World).

## Success Criteria
-   `./dh test --doc` runs successfully on the codebase.
-   Modifying `docs/02_tutorial/01_quickstart.md` with a typo causes the test to fail.
