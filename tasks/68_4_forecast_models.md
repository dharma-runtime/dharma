# Task 68.4: Forecast Models + Calibration

## Goal
Integrate model engines and quantile calibration.

## Scope
- `gbm_quantile` backend (external allowed).
- Baseline univariate engines (augurs_ets/mstl).
- Calibration via residual quantiles.
- Model artifact store with hashes + lineage.
- Backtest output: `std.commerce.forecast.backtest_result`.

## Dependencies
- Task 67.5: ML service interface (train/predict + artifact resolution).

## Test Plan
- Backtest on synthetic data.
- Quantile coverage checks.
- Deterministic artifact IDs.

## Acceptance Criteria
- At least one multivariate engine produces P50/P80/P95.
- Calibration works for point-forecast engines.
