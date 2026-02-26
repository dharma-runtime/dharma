# ECommerce ERP Go-Live Runbook

This runbook is the operator procedure for DHA-69 go-live gating of the TTM v1 ecommerce/ERP projection runtime.

## 1. Preconditions

1. Ensure project identity is initialized and unlocked.
2. Ensure `dharma.toml` points to the target runtime data directory.
3. Ensure projection writer role is granted to the runtime signer identity.
4. Ensure latest commerce contracts are present under `contracts/std/`.

## 2. Primary Gate Path (Scripted)

Run the deterministic DHA-69 gate script from repository root:

```bash
bash scripts/gates/ecommerce_erp_go_live.sh
```

Optional CI/artifact override path:

```bash
bash scripts/gates/ecommerce_erp_go_live.sh --out-dir var/gates/ecommerce-erp-go-live/manual-run
```

The script runs these gates in fixed order:
1. Compile `commerce_inventory_supplier.dhl`
2. Compile `commerce_logistics_warehouse.dhl`
3. Compile `commerce_inventory_sellable.dhl`
4. Compile `commerce_order_line.dhl`
5. `project_rebuild_populates_commerce_projections`
6. `ecommerce_key_queries_return_expected_rows`
7. `project_watch_applies_incremental_update`

The script exits on first failure and records logs in:
- Default: `var/gates/ecommerce-erp-go-live/<utc-timestamp>/`
- Override: exact `--out-dir` path

## 3. Artifact Directory Contract

A successful run writes:
- `compile_supplier.log`
- `compile_warehouse.log`
- `compile_sellable.log`
- `compile_order_line.log`
- `rebuild.log`
- `key_queries.log`
- `watch.log`
- `summary.txt`

`summary.txt` includes UTC start/end timestamps, commit SHA, per-step pass/fail, and output directory.
`summary.txt` lifecycle contract:
- Run start writes `overall=RUNNING`.
- First failing step appends `<step>=FAIL ...`, `failed_step=<step>`, and `overall=FAIL`, then exits non-zero.
- Fully successful run appends `completed_at_utc=<timestamp>` and `overall=PASS`.

Artifacts under `var/gates/ecommerce-erp-go-live/` are runtime outputs for CI/manual evidence and are not source-controlled.

## 4. Manual Fallback Commands

Use this only if script execution is blocked by environment issues.

Contract compile checks:

```bash
cargo run -p dharma-cli -- compile contracts/std/commerce_inventory_supplier.dhl
cargo run -p dharma-cli -- compile contracts/std/commerce_logistics_warehouse.dhl
cargo run -p dharma-cli -- compile contracts/std/commerce_inventory_sellable.dhl
cargo run -p dharma-cli -- compile contracts/std/commerce_order_line.dhl
```

Projection rebuild command:

```bash
dh project rebuild --scope std.commerce
```

Deterministic rebuild gate test:

```bash
cargo test -p dharma-cli cmd::ops::tests::project_rebuild_populates_commerce_projections -- --exact --nocapture
```

Go-live query gate:

```bash
cargo test -p dharma-cli cmd::ops::tests::ecommerce_key_queries_return_expected_rows -- --exact --nocapture
```

Watch incremental gate:

```bash
cargo test -p dharma-cli cmd::ops::tests::project_watch_applies_incremental_update -- --exact --nocapture
```

## 5. Rollback Procedure

If any gate fails:

1. Stop projection watch workers.
2. Freeze writes to impacted commerce domains.
3. Revert to last known-good runtime build/config profile.
4. Re-run compile + rebuild + key query gates after remediation.
5. Resume writes only after all gates pass.

## 6. Evidence and Sign-Off

Archive the gate artifact directory and include:
- Timestamp and environment metadata
- Operator identity
- Commit SHA
- Required log files listed above
