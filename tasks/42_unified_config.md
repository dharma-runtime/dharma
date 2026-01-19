# Task 42: Unified Configuration System

## Goal
Implement a robust, hierarchical configuration system for DHARMA that unifies local file settings, environment variables, and CLI flags. 

## Specification

### 1. Configuration Sources (Priority Order)
1.  **CLI Flags:** Highest priority. Overrides everything.
2.  **Environment Variables:** Prefix `DHARMA_`.
    -   Format: `DHARMA_<SECTION>_<KEY>=VALUE` (e.g., `DHARMA_NETWORK_PORT=4000`).
    -   Nested keys use double underscore: `DHARMA_STORAGE__PATH=/tmp/dharma`.
3.  **Local Config File:** `./dharma.toml` in the current working directory (Project Mode).
4.  **Global Config File:** `~/.dharma/config.toml` (User Mode).
5.  **Defaults:**
    -   `identity.keystore_path`: `~/.dharma/keystore`
    -   `storage.path`: `~/.dharma/data` (unless `./dharma.toml` overrides)

### 2. Proposed `dharma.toml` Structure
```toml
[identity]
default_key = "id(0x...)"
keystore_path = "~/.dharma/keystore"

[network]
port = 4000
peers = ["tcp://peer1.dharma.io:4000"]
max_frame_size = 1048576

[storage]
# If running as a project, use "./data". If global node, use "~/.dharma/data".
path = "~/.dharma/data"
snapshot_interval = 1000

[profile]
mode = "embedded" # or "server"

[vm.limits]
max_fuel = 1_000_000
max_memory_pages = 10
max_stack_depth = 128
max_table_size = 100
```

### 3. Implementation Steps
1.  **Define `Config` Struct:** Create `dharma-core/src/config.rs` with `serde` derive.
2.  **Unify Loading:** Create a `load_config()` function that merges all sources.
3.  **Refactor Existing Logic:** Replace ad-hoc parsing in `dharma-cli` (e.g., in `reactor.rs` and `action.rs`) with the new unified config.
4.  **Env Mapping:** Use a crate like `config-rs` or implement a custom mapper for `DHARMA_` variables.

## Success Criteria
-   Running `DHARMA_NETWORK_PORT=5000 dh serve` uses port 5000 regardless of `dharma.toml`.
-   Configuration is easily accessible via `env.config()` within the kernel.
-   The system remains <1MB (use lightweight TOML parser if needed).
