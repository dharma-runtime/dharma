# Task 57: Relay Sync Integration Harness

## Goal
Provide a deterministic, repeatable integration harness that spins up a relay + multiple nodes, creates and updates subjects, runs sync, and asserts convergence (subjects, assertions, frontiers) across all nodes.

## Why
Manual relay testing is clunky and unreliable. We need a standard test path that proves relay sync and identity verification in a controlled environment and produces useful traces on failure.

## Scope
Implement in `dharma-test` (preferred) or `tests/` integration tests with:
- Relay server (in-process) on loopback, ephemeral port.
- 2–4 nodes with isolated temp storage.
- Deterministic scenario scripts.
- Convergence assertions (frontier + assertion IDs).
- Structured trace on failure.

## Requirements
### A) Harness API
Provide a minimal helper layer (module or helper structs) usable by multiple tests:

```
struct TestNode {
  data_dir: TempDir,
  identity: IdentityState,
  store: Store,
  index: FrontierIndex,
}

impl TestNode {
  fn new(name: &str) -> Self;
  fn start_server(&self, relay: bool, port: u16) -> JoinHandle<()>;
  fn connect_and_sync(&mut self, addr: &str, subject: Option<SubjectId>, verbose: bool) -> Result<()>;
  fn create_subject(&mut self, lens: u64) -> SubjectId;
  fn write_action(&mut self, subject: SubjectId, action: &str, args: Value) -> AssertionId;
  fn list_subjects(&self) -> HashSet<SubjectId>;
  fn list_assertions(&self, subject: SubjectId) -> Vec<AssertionId>;
  fn frontier(&self, subject: SubjectId) -> Vec<EnvelopeId>;
}
```

### B) Convergence Assertions
For every scenario:
- Subjects equal across nodes.
- Assertion IDs equal per subject across nodes.
- Frontier (tips) equal per subject across nodes.
- No pending objects in frontier index.

### C) Scenarios
Implement at least these cases:
1. **Baseline relay**: 1 subject, 3 assertions, 3 nodes.
2. **Multi-subject**: 3–5 subjects, multiple assertions each.
3. **Interleaved updates**: A writes → B syncs → A writes → C syncs.
4. **Identity verification**: B syncs A’s identity subject first, then verify “auth verified” on reconnect.

### D) Trace + Timeout
- Add a trace sink in `SyncOptions` (or test-only hook) to capture events.
- If convergence fails or times out, dump trace to `tests/failures/…` using the existing failure format.
- Hard timeout per scenario (e.g., 5s).

### E) CI Integration
Wire this into `dh test --deep` (new property: `P-RELAY-001`, `P-RELAY-002`, …).

## Notes / Hints
- Use loopback (`127.0.0.1`) + ephemeral ports for relay/server.
- Keep the harness deterministic (seed RNG for identity/key generation).
- Avoid global `~/.dharma` paths; always use temp dirs.
- Use `SyncOptions { exit_on_idle: true }` for one-shot sync runs.

## Done When
- New relay integration tests pass locally.
- `dh test --deep` runs relay properties without hanging.
- Failures include a trace section showing hello/inv/get/obj/pending events.
