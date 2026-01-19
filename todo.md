# TODO

## Done
- [x] Boot sequence: identity check, passphrase unlock, self head verification (tasks/00_done.md)
- [x] Identity init/export, encrypted keystore, config persistence (tasks/00_done.md)
- [x] Blocking TCP server/client, frame codec, handshake, sync loop (tasks/00_done.md)
- [x] Frontier index (tips) for base assertions (tasks/00_done.md)
- [x] Ingest pipeline: signature + structural validation + store commit (tasks/00_done.md)
- [x] DHL parser + compiler skeleton; wasm validate/reduce for Int/Bool/Enum/Identity (tasks/00_done.md)
- [x] ABI layout reserved (base 0x0000, overlay 0x1000, args 0x2000, context 0x3000) (tasks/00_done.md)
- [x] Sidecar overlays: separate overlay log, split assertions, replay merge in runtime (tasks/00_done.md)
- [x] Overlay replication with policy gating (overlays.policy; legacy allowlist fallback) (tasks/00_done.md)
- [x] Structured overlay policy per subject/namespace (tasks/00_done.md)
- [x] Recursive parent merge on compile for `extends` (tasks/00_done.md)

## Roadmap aligned to README.md + user_guide.md (target behavior)

### Phase 0: Architecture & Safety (Immediate)
- [x] Task 27: Workspace Refactoring (Split Kernel vs CLI) (tasks/27_workspace_refactor.md)
- [x] **Task 30: Semantic IDs** (Split Identity/Encryption) (tasks/30_semantic_ids.md)
- [x] **Task 31: Explicit DAG** (Fix Ordering Instability) (tasks/31_explicit_dag.md)
- [x] **Task 32: Device Key Delegation** (Fix Identity Forks) (tasks/32_device_keys.md)
- [x] Task 20: Bloat Removal & Float Ban (tasks/20_bloat_removal.md)

### Storage & Core Data Model
- [x] Implement README storage layout (tasks/06_storage_layout.md)
  - data/objects/<object_id>.obj (raw envelopes for assertions/artifacts)
  - data/subjects/<subject_id>/assertions, snapshots, indexes
- [x] Envelope-first persistence for assertions/artifacts; derive subject views from object store (tasks/06_storage_layout.md)
- [x] Snapshot format + save/load per lens (data_ver) and per subject (tasks/06_storage_layout.md)
- [x] Index regeneration (frontier, per subject indexes) from object store (tasks/06_storage_layout.md)
- [x] Task 14: Incremental Indexing & Manifests (Fix O(N) Startup) (tasks/14_incremental_indexing.md)
- [x] **Task 38: Log Framing Checksums** (Detect corruption in log.bin) (tasks/06_storage_layout.md)
- [x] Task 15: Optimize Frontier Index (Fix In-Memory Bloat) (tasks/15_persistent_indexing.md)
- [x] Task 17: Concurrency Control (LockManager) (tasks/17_concurrency_control.md)

### Protocol + Validation Pipeline
- [x] Add header field `ver` (data version) to assertion header; ensure it is signed and encoded (tasks/07_validation_pipeline.md)
- [x] Lens routing: interpret assertions by `ver`, support multiple installed schema/contract versions (tasks/07_validation_pipeline.md)
- [x] Deterministic validation pipeline in ingest: canonical CBOR -> sig -> schema -> contract (tasks/07_validation_pipeline.md)
- [x] Deterministic replay ordering (deps graph + lexicographic tie-break) (tasks/07_validation_pipeline.md)
- [x] Pending handling: missing deps/artifacts => PENDING, never guessed state (tasks/07_validation_pipeline.md)
- [x] Task 18: Error Granularity (Refine DharmaError) (tasks/18_error_granularity.md)

### REPL (User Guide scope)
- [x] Add `dh repl` command entrypoint (tasks/01_repl_core.md)
- [x] Core REPL loop + history (rustyline); commands: help, exit/quit, clear, version, :set (tasks/01_repl_core.md)
- [ ] **Task 46: REPL UX Overhaul** (Nested commands, Autocomplete, Intelligent Shell) (tasks/46_repl_ux.md)
- [x] Identity commands: status/init/unlock/lock/whoami/export (with confirmation policies) (tasks/01_repl_core.md)
- [x] Subject navigation: subjects/use/pwd/alias (tasks/01_repl_core.md)
- [x] State/history: state (--json/--raw/--at/--lens), tail, log, show, status (tasks/02_state_and_history.md)
- [x] Audit: why, prove, diff (tasks/03_action_and_audit.md)
- [x] Action pipeline: dryrun action, commit action, authority, highsec transaction card (tasks/03_action_and_audit.md)
- [x] Overlay commands: overlay status/list/enable/disable/show (tasks/08_repl_extended.md)
- [x] Peers & sync: peers, sync now, sync subject, connect, discover on/off/status (tasks/08_repl_extended.md)
- [ ] Task 28: Dev Mode (Hot Reload + Live CEL) (tasks/28_dev_mode.md)
- [x] Task 29: REPL Polish (TUI & Colors) (tasks/29_repl_polish.md)
- [x] Indexing commands: index status/build/drop, find, open (tasks/05_search_indexing.md)
- [ ] Vector/graph search commands: vfind/gfind (tasks/05_search_indexing.md)
- [ ] Export/import bundles; maintenance: check/gc/snapshot (tasks/08_repl_extended.md)

### Network/Sync (DHARMA-SYNC/1)
- [x] Ensure hello/inv/get/obj/err message framing matches README (capabilities + suite info) (tasks/09_sync_protocol.md)
- [x] Implement Noise_XX handshake (manual, zero-bloat) using existing crypto primitives (tasks/13_noise_handshake.md)
- [x] Subscription/interest filtering for inventory (tasks/09_sync_protocol.md)
- [x] Task 16: Sync Robustness (Range/Merkle Sync) (tasks/16_sync_robustness.md)
- [x] Peer trust/ban enforcement in sync loop (tasks/09_sync_protocol.md)
- [x] Bind peer SubjectId to a verified identity assertion (not just signature proof) (tasks/09_sync_protocol.md)
- [x] Overlay disclosure by org/role ACLs (beyond subject/namespace policy) (tasks/09_sync_protocol.md)

### Packages & Registry
- [x] Package commands: pkg list/show/install/verify/pin/remove (tasks/04_package_management.md)
- [x] Registry subject + sys.package assertions + artifact fetch (tasks/10_registry.md)
- [ ] Registry publisher ACL checks (registry scope authorization) (tasks/33_registry_publisher_acl.md)
- [ ] Artifact fetch/verify; map versions -> schema/contract/reactor (tasks/10_registry.md)
- [ ] `dharma.toml` registry mappings + dependency resolution (tasks/10_registry.md)

### Compiler + Runtime
- [x] DHL v2 features: has_role, concat, lists, ACLs (tasks/11_compiler_runtime.md)
- [x] **Task 39: Wasm Fuel Metering** (Prevent DoS) (tasks/11_compiler_runtime.md)
- [x] Reactor daemon implementation (subscribe to ingest, execute reactor wasm, emit signed assertions) (tasks/11_compiler_runtime.md)
- [ ] **Task 26: DHL Enhancements** (Expressions, Collections, DHARMA-Q Integration) (tasks/26_dhl_bpm_features.md)
- [ ] **Task 43: DHL Aspects** (Mixins for Code Reuse) (tasks/43_dhl_aspects.md)

### Testing + Conformance
- [x] **Task 12: Testing & Conformance** (Harness built, Core refactor pending) (tasks/12_testing_conformance.md)
- [ ] **Task 57: Relay Sync Integration Harness** (Deterministic multi-node sync via relay) (tasks/57_relay_sync_harness.md)
- [ ] **Task 40: Executable Documentation** (Literate Testing for UX) (tasks/40_executable_docs.md)
- [ ] **Task 38: Log Framing Checksums** (Detect corruption in log.bin) (tasks/06_storage_layout.md)
- [ ] **Task 48: Operations Tooling** (Doctor, GC, Backup) (tasks/48_ops_tooling.md)
- [ ] **Task 39: Wasm Fuel Metering** (Prevent DoS) (tasks/11_compiler_runtime.md)
- [ ] **Task 41: Security Hardening** (Audit Remediation) (tasks/41_security_hardening.md)
- [ ] **Task 42: Unified Configuration System** (TOML + Env Overrides) (tasks/42_unified_config.md)
- [ ] Reproducible build + size budget checks (<1MB) (tasks/12_testing_conformance.md)

### Configuration & Profiles
- [x] **Task 42: Configuration System** (Global + Project dharma.toml, profiles, limits) (tasks/42_configuration_system.md)

### DHARMA-Q (Query Engine)
- [x] Task 19: Implement DHARMA-Q (Embedded Module: Engine + Recursive Boolean Planner) (tasks/19_dharma_q_spec.md)
- [x] Task 19.5: Dynamic Column Projection (Query DHL Fields) (tasks/19_5_dynamic_columns.md)
- [ ] **Task 49: DHARMA-Q Contract Tables (Live State Index)** (tasks/49_dharmaq_contract_tables.md)
- [ ] **Task 51: DHARMA-Q Group-By + Aggregations** (tasks/51_dharmaq_groupby_aggregates.md)
- [ ] **Task 52: DHARMA-Q Benchmark Tool** (tasks/52_dharmaq_benchmark_tool.md)
- [ ] **Task 53: DHARMA-Q Performance Optimization** (tasks/53_dharmaq_performance_optimization.md)
- [ ] **Task 54: DHARMA-Q Dual Store + Index Planner** (tasks/54_dharmaq_dual_store_planner.md)
- [ ] **Task 55: DHL Build Output Directory** (tasks/55_dhl_build_output_dir.md)
- [ ] **Task 56: Rename Project to Dharma + New Binaries** (tasks/56_rename_dharma.md)

### DHARMA-FABRIC (Execution Plane)
- [x] Task 21: Fabric Types & Ads (ShardMap, Advertisements) (tasks/21_fabric_types.md)
- [x] Task 22: Capability Tokens (Authorization) (tasks/22_capability_tokens.md)
- [x] Task 23: Fabric Router (Client) (tasks/23_fabric_router.md)
- [x] Task 24: Fabric Execution Protocol (Fast/Wide Path) (tasks/24_fabric_execution.md)
- [x] Task 25: Directory & Relay System (V1 Discovery/Atlas) (tasks/25_directory_relay.md)

## Phase 5: The Standard Library
- [x] **Task 44: Standard Library Expansion** (The OS of Business) (tasks/44_stdlib_expansion.md)

### Phase 6: DHARMA-Workspace (GUI)
- [ ] **Task 33: Workspace Core** (GPUI Setup, Buffer Model) (docs/workspace.md)
- [ ] **Task 34: DHL Renderer** (View Block Implementation) (docs/workspace.md)

### Phase 7: Developer Experience (IDE)
- [ ] **Task 46: REPL UX Overhaul** (Nested commands, Autocomplete, Intelligent Shell) (tasks/46_repl_ux.md)
- [ ] **Task 47: Semantic Types & Rich Inputs** (Email, Markdown, IPFS Integration) (tasks/47_semantic_types.md)
- [ ] **Task 50: DHARMA-WEB** (Dioxus Fullstack Integration) (tasks/50_dharma_web_dioxus.md)
- [ ] **Task 35: Tree-sitter Grammar** (DHL + CEL) (tasks/28_dev_mode.md)
- [ ] **Task 36: DHARMA LSP** (Verification & Types) (tasks/28_dev_mode.md)

### Phase 8: Database Maturity (HTAP)
- [ ] **Task 54: DHARMA-Q Dual Store** (Row+Column Store, DHL Indexes, Planner) (tasks/54_dharmaq_dual_store_planner.md)

## REPL & User Tools
- [x] **Task 45: REPL Contract Discovery + New Subject** (tasks/45_repl_contracts_commands.md)
- [x] **Task 46: REPL UX Overhaul (Intelligent Shell)** (tasks/46_repl_ux.md)
