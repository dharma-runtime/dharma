# Task 64.5: Vault CLI + Recovery UX

## Goal
Provide a simple, safe CLI and setup wizard for vault configuration, archiving, and restore.

## Why
The vault must be accessible to non-technical users while still offering enterprise options.

## Scope
- CLI commands for setup, archive, restore, verify.
- Recovery wizard flow.
- Config persistence in `~/.dharma/config.toml` and OS keychain.

## Out of Scope
- Driver internals (Task 64.2).
- Core format (Task 64.1).
- Runtime scheduling (Task 64.4).

## Specification

### 1) CLI Commands
- `dh vault setup s3`
- `dh vault setup arweave`
- `dh vault setup peer`
- `dh vault archive`
- `dh vault restore`
- `dh vault verify <subject> <seq>`

### 2) Recovery Wizard
Prompt:
1) Do you have a backup?
   - No (start fresh)
   - Dharma Cloud
   - Private S3
   - Friend/Peer

### 3) Safety Slider UX
- Standard (relay backup, 100MB cap).
- Secure (peer backup).
- Sovereign (S3 + Arweave).

## Implementation Steps
1. Add `vault` CLI subcommands.
2. Wire setup to config + keychain.
3. Implement interactive recovery wizard.
4. Add status output (current driver, last checkpoint).

## Test Plan (Detailed)

### Unit Tests
- `vault_config_roundtrip`:
  - Write config -> reload -> fields preserved.
- `vault_setup_requires_credentials`:
  - Missing AWS keys => error.

### Integration Tests
- `vault_archive_cli_roundtrip`:
  - Archive -> verify -> restore using LocalDriver.
- `vault_restore_cli_prompt_paths`:
  - Each wizard choice hits expected code path.

### UX Tests
- Ensure prompts are clear and safe defaults are used.
- Non-technical flow should complete with minimal inputs.

## Acceptance Criteria
- CLI commands available and documented.
- Recovery wizard flows are deterministic and safe.
