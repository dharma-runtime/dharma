# Storage Migration Runbook

This runbook is the operator procedure for DHA-62 migration verification across SQLite, Postgres, and ClickHouse.

## 1. Preconditions

1. Ensure services are reachable:
   - Postgres (`storage.postgres.url`)
   - ClickHouse (`storage.clickhouse.url`, `storage.clickhouse.database`)
2. Ensure `dharma.toml` points to the target runtime data directory and backend config.
3. Freeze non-essential write traffic before production validation windows.

## 2. Precheck

Run baseline health checks before migration validation:

```bash
dh doctor
```

## 3. Migration Validation

Validate backend schema/migration invariants:

```bash
dh migrate validate --backend all --strict
```

Expected strict checks:
- SQLite tables/indexes exist (`objects`, `semantic_index`, `cqrs_reverse`, `subject_assertions`, `permission_summaries`, plus required indexes).
- Postgres schema exists and `schema_migrations` contains `0001_init`.
- ClickHouse analytics tables exist and watermark invariant holds: `watermark_seq <= committed_seq`.

Optional machine-readable output:

```bash
dh migrate validate --backend all --strict --json
```

## 4. Cross-Backend Parity + Replay

Run deterministic parity/replay digest checks:

```bash
dh migrate parity --strict
```

What this enforces:
- Subject count parity
- Assertion count parity
- Object count parity
- Deterministic replay digest parity (`replay_hash_hex`)
- Frontier digest parity (`frontier_hash_hex`)

Optional machine-readable output:

```bash
dh migrate parity --strict --json
```

## 5. Rollback Verification Flow

If validation or parity fails:

1. Freeze write admission.
2. Revert read/write routing to last known healthy backend profile.
3. Rebuild/replay from canonical store into target backend.
4. Re-run strict checks:

```bash
dh migrate validate --backend all --strict
dh migrate parity --strict
```

5. Resume writes only after both commands return success.

## 6. Post-Run Evidence

Archive command outputs (text or JSON) with:
- Timestamp
- Environment
- Backend endpoints/schema/table prefix
- Result status and reported issues

These artifacts are required for migration sign-off and rollback drills.
