# Task 42: Configuration System

## Objective
Introduce a unified configuration system with global + project scopes to control
identity, storage, network, registry, and VM limits.

## Requirements
- **Config locations**
  - Global: `~/.dharma/config.toml` (created on first run if missing)
  - Global legacy: `~/.config/dharma/dharma.toml` (read if present)
  - Project: `./dharma.toml` (optional; overrides global)
- **Merge order:** legacy global -> global -> project (last wins).
- **Minimal TOML parser** (no heavy deps): sections, nested sections, strings,
  ints, bools, string arrays.
- **Defaults** for all keys (see below).
- **Apply limits**
  - VM limits: fuel + memory bytes
  - Network frame size limit
  - Connection timeouts (connect/read/write)

## dharma.toml schema (v1)
```toml
[identity]
default_key = "id(0xabc...)"
keystore_path = "./data/keystore"

[network]
listen_port = 4000
peers = ["tcp://dharma.p2p.io:4000", "tcp://relay.corp.com:4000"]
max_peers = 50
max_frame_size = 1048576
connect_timeout_ms = 5000
read_timeout_ms = 5000
write_timeout_ms = 5000

[storage]
path = "./data"
snapshot_interval = 1000
prune_pending_hours = 24

[profile]
mode = "embedded" # or "server"

[registry]
url = "https://registry.dharma.systems"

[registry.pins]
"std.finance" = "1.2.0"

[vm]
fuel = 1000000
memory_bytes = 655360
```

## Acceptance Criteria
- Running any `dh` command creates `~/.dharma/config.toml` if missing.
- Config values override runtime defaults:
  - VM fuel + memory
  - Frame size limit
  - Socket timeouts
- Storage path uses `storage.path`.
- Tests updated or added for config parsing + default creation.
