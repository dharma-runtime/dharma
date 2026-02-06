# Done

## Foundations / Kernel
- Boot sequence: identity check, passphrase unlock, self head verification (tasks/00_done.md)
- Identity init/export, encrypted keystore, config persistence (tasks/00_done.md)
- Blocking TCP server/client, frame codec, handshake, sync loop (tasks/00_done.md)
- Frontier index (tips) for base assertions (tasks/00_done.md)
- Ingest pipeline: signature + structural validation + store commit (tasks/00_done.md)
- DHL parser + compiler skeleton; wasm validate/reduce for Int/Bool/Enum/Identity (tasks/00_done.md)
- ABI layout reserved (base 0x0000, overlay 0x1000, args 0x2000, context 0x3000) (tasks/00_done.md)
- Sidecar overlays: separate overlay log, split assertions, replay merge in runtime (tasks/00_done.md)
- Overlay replication with policy gating (overlays.policy; legacy allowlist fallback) (tasks/00_done.md)
- Structured overlay policy per subject/namespace (tasks/00_done.md)
- Recursive parent merge on compile for `extends` (tasks/00_done.md)
- Task 20: Bloat Removal & Float Ban (tasks/20_bloat_removal.md)
- Task 27: Workspace Refactoring (Split Kernel vs CLI) (tasks/27_workspace_refactor.md)
- Task 30: Semantic IDs (Split Identity/Encryption) (tasks/30_semantic_ids.md)
- Task 31: Explicit DAG (Fix Ordering Instability) (tasks/31_explicit_dag.md)
- Task 32: Device Key Delegation (Fix Identity Forks) (tasks/32_device_keys.md)

## Storage & Data Model
- Implement README storage layout (tasks/06_storage_layout.md)
- Envelope-first persistence for assertions/artifacts; derive subject views from object store (tasks/06_storage_layout.md)
- Snapshot format + save/load per lens (data_ver) and per subject (tasks/06_storage_layout.md)
- Index regeneration (frontier, per subject indexes) from object store (tasks/06_storage_layout.md)
- Task 14: Incremental Indexing & Manifests (Fix O(N) Startup) (tasks/14_incremental_indexing.md)
- Task 15: Optimize Frontier Index (Fix In-Memory Bloat) (tasks/15_persistent_indexing.md)
- Task 17: Concurrency Control (LockManager) (tasks/17_concurrency_control.md)
- Task 38: Log Framing Checksums (Detect corruption in log.bin) (tasks/06_storage_layout.md)
- Task 64.1: Vault Core + .dhbox Format (tasks/64_1_vault_core_format.md)

## Validation Pipeline
- Add header field `ver` (data version) to assertion header; ensure it is signed and encoded (tasks/07_validation_pipeline.md)
- Lens routing: interpret assertions by `ver`, support multiple installed schema/contract versions (tasks/07_validation_pipeline.md)
- Deterministic validation pipeline in ingest: canonical CBOR -> sig -> schema -> contract (tasks/07_validation_pipeline.md)
- Deterministic replay ordering (deps graph + lexicographic tie-break) (tasks/07_validation_pipeline.md)
- Pending handling: missing deps/artifacts => PENDING, never guessed state (tasks/07_validation_pipeline.md)
- Task 18: Error Granularity (Refine DharmaError) (tasks/18_error_granularity.md)

## REPL / CLI
- Add `dh repl` command entrypoint (tasks/01_repl_core.md)
- Core REPL loop + history (rustyline); commands: help, exit/quit, clear, version, :set (tasks/01_repl_core.md)
- Identity commands: status/init/unlock/lock/whoami/export (with confirmation policies) (tasks/01_repl_core.md)
- Subject navigation: subjects/use/pwd/alias (tasks/01_repl_core.md)
- State/history: state (--json/--raw/--at/--lens), tail, log, show, status (tasks/02_state_and_history.md)
- Audit: why, prove, diff (tasks/03_action_and_audit.md)
- Action pipeline: dryrun action, commit action, authority, highsec transaction card (tasks/03_action_and_audit.md)
- Overlay commands: overlay status/list/enable/disable/show (tasks/08_repl_extended.md)
- Peers & sync: peers, sync now, sync subject, connect, discover on/off/status (tasks/08_repl_extended.md)
- Task 29: REPL Polish (TUI & Colors) (tasks/29_repl_polish.md)
- Indexing commands: index status/build/drop, find, open (tasks/05_search_indexing.md)
- Task 45: REPL Contract Discovery + New Subject (tasks/45_repl_contracts_commands.md)

## Network / Sync
- Ensure hello/inv/get/obj/err message framing matches README (capabilities + suite info) (tasks/09_sync_protocol.md)
- Implement Noise_XX handshake (manual, zero-bloat) using existing crypto primitives (tasks/13_noise_handshake.md)
- Subscription/interest filtering for inventory (tasks/09_sync_protocol.md)
- Task 16: Sync Robustness (Range/Merkle Sync) (tasks/16_sync_robustness.md)
- Peer trust/ban enforcement in sync loop (tasks/09_sync_protocol.md)
- Bind peer SubjectId to a verified identity assertion (not just signature proof) (tasks/09_sync_protocol.md)
- Overlay disclosure by org/role ACLs (beyond subject/namespace policy) (tasks/09_sync_protocol.md)

## Network PRD v1 (Identity, Domains, Ownership, Keys)
- Task 58: Atlas Identity + Genesis Phase + Lifecycle (tasks/58_atlas_identity_genesis.md)
- Task 58.1: Atlas Identity Schema & Types (tasks/58_1_atlas_identity_schema.md)
- Task 58.2: Genesis Phase Enforcement (tasks/58_2_atlas_identity_genesis_ingest.md)
- Task 58.3: Identity Lifecycle + Verification (tasks/58_3_atlas_identity_lifecycle.md)
- Task 58.4: Local Handle Persistence & UX Guardrails (tasks/58_4_local_handle_cli.md)
- Task 59: std.atlas.domain Contract + Membership + Hierarchy (tasks/59_atlas_domain_contract.md)
- Task 59.1: Domain Contract Schema + Types (tasks/59_1_domain_contract_schema.md)
- Task 59.2: Domain Membership State Evaluation (tasks/59_2_domain_membership_state.md)
- Task 59.3: Acting Context Enforcement (tasks/59_3_acting_context_enforcement.md)
- Task 59.4: Directory Integration for Domains (tasks/59_4_directory_integration.md)
- Task 60: Ownership, Attribution, Sharing & Transfer (tasks/60_ownership_sharing_revocation.md)
- Task 60.1: Ownership & Attribution Metadata (tasks/60_1_ownership_metadata.md)
- Task 60.2: Sharing & Revocation Assertions (tasks/60_2_sharing_assertions.md)
- Task 60.3: Fabric Enforcement of Ownership/Sharing (tasks/60_3_fabric_enforcement.md)
- Task 60.4: Ownership Transfer Rules (tasks/60_4_transfer_rules.md)
- Task 61: Permission Summaries & Fast Reject (tasks/61_permission_summaries.md)
- Task 61.1: Permission Summary Artifact (tasks/61_1_permission_summary_artifact.md)
- Task 61.2: Router Cache + Fast Reject (tasks/61_2_router_cache_fast_reject.md)
- Task 61.3: Permission Summary Tests & Benchmarks (tasks/61_3_permission_summary_tests.md)
- Task 62: Domain Key Hierarchy, Rotation & Epochs (tasks/62_key_hierarchy_rotation.md)
- Task 62.1: Key Hierarchy Primitives (tasks/62_1_key_hierarchy_primitives.md)
- Task 62.2: Key Rotation + Epoch Usage (tasks/62_2_epoch_rotation.md)
- Task 62.3: Revocation + Key Distribution (tasks/62_3_revocation_distribution.md)
- Task 63: Emergency Freeze + Compromise Handling (tasks/63_emergency_freeze_compromise.md)
- Task 63.1: Emergency Freeze (tasks/63_1_emergency_freeze.md)
- Task 63.2: Device Key Revocation Enforcement (tasks/63_2_device_revocation.md)
- Task 63.3: Domain Compromise Handling (tasks/63_3_domain_compromise.md)
- Task 57b: IAM Contact-Gated Visibility (Fabric testbed) (tasks/57_iam_contact_visibility.md)

## Protocol Interfaces
- Task 65: Protocol Interfaces & Implementations (tasks/65_protocol_interfaces.md)
- Task 65.1: Protocol Interface Registry (tasks/65_1_protocol_interface_registry.md)
- Task 65.2: DHL `implements` + Compiler Validation (tasks/65_2_dhl_implements_validation.md)
- Task 65.3: Contacts Protocol Interface + Resolver (tasks/65_3_contacts_protocol_interface.md)
- Task 65.4: IAM Protocol Interface + Resolver (tasks/65_4_iam_protocol_interface.md)
- Task 65.5: Atlas Identity Protocol Interface (tasks/65_5_atlas_identity_protocol_interface.md)
- Task 65.6: Atlas Domain Protocol Interface (tasks/65_6_atlas_domain_protocol_interface.md)

## Packages & Registry
- Package commands: pkg list/show/install/verify/pin/remove (tasks/04_package_management.md)
- Registry subject + sys.package assertions + artifact fetch (tasks/10_registry.md)

## Compiler & Runtime
- DHL v2 features: has_role, concat, lists, ACLs (tasks/11_compiler_runtime.md)
- Task 39: Wasm Fuel Metering (Prevent DoS) (tasks/11_compiler_runtime.md)
- Reactor daemon implementation (subscribe to ingest, execute reactor wasm, emit signed assertions) (tasks/11_compiler_runtime.md)

## Testing / Ops
- Task 12: Testing & Conformance (Harness built, Core refactor pending) (tasks/12_testing_conformance.md)
- Task 48: Operations Tooling (Doctor, GC, Backup) (tasks/48_ops_tooling.md)

## Configuration
- Task 42: Configuration System (Global + Project dharma.toml, profiles, limits) (tasks/42_configuration_system.md)

## DHARMA-Q (Base)
- Task 19: Implement DHARMA-Q (Embedded Module: Engine + Recursive Boolean Planner) (tasks/19_dharma_q_spec.md)
- Task 19.5: Dynamic Column Projection (Query DHL Fields) (tasks/19_5_dynamic_columns.md)

## Fabric (Execution Plane)
- Task 21: Fabric Types & Ads (ShardMap, Advertisements) (tasks/21_fabric_types.md)
- Task 22: Capability Tokens (Authorization) (tasks/22_capability_tokens.md)
- Task 23: Fabric Router (Client) (tasks/23_fabric_router.md)
- Task 24: Fabric Execution Protocol (Fast/Wide Path) (tasks/24_fabric_execution.md)
- Task 25: Directory & Relay System (V1 Discovery/Atlas) (tasks/25_directory_relay.md)

## Standard Library
- Task 44: Standard Library Expansion (The OS of Business) (tasks/44_stdlib_expansion.md)

## Branding / Packaging
- Task 56: Rename Project to Dharma + New Binaries (tasks/56_rename_dharma.md)
