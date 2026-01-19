# Task: Package Management

## Objective
Implement the `pkg` command suite to manage schemas, contracts, and artifacts.

## Requirements
- **Commands**:
  - `pkg list`: Show installed packages.
  - `pkg show <name>`: Show details (versions, dependencies).
  - `pkg install <name>`: Fetch artifacts from a registry.
  - `pkg verify <name>`: check signatures and hashes.
- **Registry Support**:
  - Define a "Registry" subject structure (std.registry).
  - Implement artifact fetching (syncing) from registry subjects.
  - Validate package signatures against trusted publishers.

## Implementation Details
- Create `src/pkg/mod.rs`.
- Define the directory structure for installed packages (beyond the current flat `schemas/` and `contracts/` folders).
- Implement dependency resolution logic.
