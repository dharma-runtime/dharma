# Task: Registry + Package Distribution

## Objective
Implement registry subjects and package install/verify flow described in README.

## Requirements
- Registry subject assertions (sys.package) with artifact hashes.
- Fetch artifacts by object id; verify hash matches.
- Map package versions -> schema/contract/reactor artifacts.
- Update dharma.toml mappings + dependency resolution.

## Implementation Details
- Extend store to fetch artifacts from sync peers.
- Add package cache under data/packages or data/objects.
- Implement package dependency resolution with pinned hashes.

## Acceptance Criteria
- pkg install downloads artifacts and registers mappings.
- pkg verify validates publisher + artifact hash chain.
