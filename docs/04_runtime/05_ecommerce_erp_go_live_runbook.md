# ECommerce ERP Go-Live Runbook

This runbook is the operator procedure for DHA-69 go-live gating of the TTM v1 ecommerce/ERP projection runtime.

## 1. Preconditions

1. Ensure project identity is initialized and unlocked.
2. Ensure `dharma.toml` points to the target runtime data directory.
3. Ensure projection writer role is granted to the runtime signer identity.
4. Ensure latest commerce contracts are present under `contracts/std/`.

## 2. Contract Compile Gate

Run canonical contract compile checks:

```bash
cargo run -p dharma-cli -- compile contracts/std/commerce_inventory_supplier.dhl
cargo run -p dharma-cli -- compile contracts/std/commerce_logistics_warehouse.dhl
cargo run -p dharma-cli -- compile contracts/std/commerce_inventory_sellable.dhl
cargo run -p dharma-cli -- compile contracts/std/commerce_order_line.dhl
```

Expected result: all four commands exit `0`.

## 3. Projection Rebuild Gate

Execute full commerce projection rebuild:

```bash
dh project rebuild --scope std.commerce
```

Expected result:
- No `projection runtime not wired yet` error.
- Rebuild summary prints `plans`, `writes`, and `prunes`.
- Projection target tables are queryable after rebuild.

## 4. Go-Live Query Gate

Run the key query validation suite:

```bash
cargo test -p dharma-cli cmd::ops::tests::ecommerce_key_queries_return_expected_rows -- --exact --nocapture
```

This gate validates deterministic rows for:
- `GetProductFacets`
- `GetVariantAvailabilityHint`
- `LinesNeedingAllocation`
- `ListMyInvoices`
- `ReturnsAndCreditsSummary`

## 5. Watch Incremental Gate

Run watch mode incremental update validation:

```bash
cargo test -p dharma-cli cmd::ops::tests::project_watch_applies_incremental_update -- --exact --nocapture
```

Expected result: watch cycle runs without full rebuild failure and preserves/increases projected row count after source change.

## 6. Rollback Procedure

If any gate fails:

1. Stop projection watch workers.
2. Freeze writes to impacted commerce domains.
3. Revert to last known-good runtime build/config profile.
4. Re-run compile + rebuild + key query gates after remediation.
5. Resume writes only after all gates pass.

## 7. Evidence and Sign-Off

Archive the following artifacts for gate approval:
- Contract compile command outputs
- Rebuild command output
- Key query test output
- Watch incremental test output
- Timestamp, environment, operator identity, commit SHA

