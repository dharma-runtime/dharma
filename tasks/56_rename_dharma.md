# Task 56: Rename Project to Dharma + New Binaries

## Goal
Rename the project to **dharma** and update binary names + startup branding across the entire repo.

## Requirements
- **Executable rename**
  - `pact` -> `dh`
  - `pactd` -> `dhd`
- **Startup banner**
  - When `dh` or `dhd` starts, print:
```
       ____                              
  ____╱ ╱ ╱_  ____ __________ ___  ____ _
 ╱ __  ╱ __ ╲╱ __ `╱ ___╱ __ `__ ╲╱ __ `╱
╱ ╱_╱ ╱ ╱ ╱ ╱ ╱_╱ ╱ ╱  ╱ ╱ ╱ ╱ ╱ ╱ ╱_╱ ╱ 
╲__,_╱_╱ ╱_╱╲__,_╱_╱  ╱_╱ ╱_╱ ╱_╱╲__,_╱  
                                         
```
- **Version string**: current version is `0.1-alpha` (displayed where version text appears).

## Scope
Update **all references** to the old name (`pact`, `pactd`) in:
- Source code (binary names, help text, CLI usage, logging output).
- Build config (Cargo, scripts, test harnesses).
- Docs (README, user guides, tutorial, dev docs).
- Examples (shell commands, REPL snippets, package instructions).

## Implementation Checklist
1. **Binary names**
   - Update Cargo bin targets to `dh` and `dhd`.
   - Update any invocation references in code and scripts.
2. **Startup banner**
   - Print the ASCII art banner on startup for both `dh` and `dhd`.
   - Ensure it appears before other startup logs.
3. **Version output**
   - Update `--version` output to show `0.1-alpha`.
   - Update any embedded version text in docs and help output.
4. **Docs & examples**
   - Replace `pact` -> `dh`, `pactd` -> `dhd`.
   - Ensure tutorials and CLI usage align with new binary names.
5. **Search/verify**
   - Use `rg` to search for legacy binary names across the repo and update all occurrences.
   - Make sure branding appears consistent in docs + code.

## Success Criteria
- Running `dh` or `dhd` prints the dharma banner and uses the new binary names.
- All repo references updated to dharma terminology.
- `--version` reports `0.1-alpha`.
