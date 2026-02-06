# Forecast Pipeline DSL + Runtime Spec (v1)

## 0) Goals
1. Forecasting is declarative, versioned, auditable.
2. Algorithms and covariates selected via DHL.
3. Use DemandKey hierarchy for cold start fallback.
4. Emit quantile forecasts (P50/P80/P95).
5. Integrate external signals (calendar, weather, price, promo, marketing).
6. Reproducible outputs: pipeline version + keyspace version + data versions + model hash.

---

## 1) Concepts
- **Series key**: (DemandKey, location_id, channel_id, bucket_date)
- **Target**: observed demand per bucket
- **Covariates**: holidays, weather, price, promo, marketing
- **Pipeline**: named, versioned config -> target + covariates + features + model + outputs

---

## 2) Required data contracts (inputs)

### 2.1 Observed demand (training truth)
`std.commerce.forecast.observed_demand_bucket`
- key: keyspace_version, level_bits, key_code, location_id, channel_id, bucket_date
- fields: observed_qty, censored, censor_reason, price_effective?, promo_flag?, computed_at, source_run_id?

### 2.2 Calendar bucket
`std.commerce.calendar.bucket`
- key: region_id, bucket_date
- fields: day_of_week, week_of_year, month, is_holiday, holiday_name?, is_eve, is_payday, is_school_holiday?

### 2.3 Weather bucket
`std.commerce.weather.bucket`
- key: location_id, bucket_date
- fields: temp_avg, temp_max, rainfall_mm, humidity_avg, storm_risk_score?

### 2.4 Pricing bucket
`std.commerce.pricing.bucket`
- key: item_id or demand_key, location_id, channel_id, bucket_date
- fields: price_effective, discount_depth, price_changed

### 2.5 Marketing bucket (optional)
`std.commerce.marketing.bucket`
- key: location_id, channel_id, bucket_date
- fields: spend, impressions, clicks, campaign_count, promo_intensity_score

### 2.6 Purchasable/availability bucket (optional)
`std.commerce.availability.purchasable_bucket`
- key: item_id or demand_key, location_id, channel_id, bucket_date
- fields: purchasable_ratio, blocked_reason?

---

## 3) Forecast Pipeline DSL

### 3.1 Top-level syntax
```
forecast_pipeline DemandForecastV1 {
  version = 12

  keyspace = DemandSpace
  base_level_bits = 16
  levels = [4, 8, 12, 16]
  bucket = "day"   // day|week

  target { ... }
  covariates { ... }
  features { ... }
  hierarchy { ... }
  model { ... }
  calibration { ... }
  backtest { ... }
  emit { ... }
}
```

---

## 4) Target block
```
target {
  source std.commerce.forecast.observed_demand_bucket
  key (key_code, location_id, channel_id, bucket_date)
  value observed_qty
  censor censored
}
```

---

## 5) Covariates block
```
covariates {
  use std.commerce.calendar.bucket
    join on (region_id = location.region_id, bucket_date = bucket_date)
    fields [day_of_week, month, is_holiday, is_payday]
    missing = "reject"

  use std.commerce.weather.bucket
    join on (location_id = location_id, bucket_date = bucket_date)
    fields [temp_avg, rainfall_mm]
    missing = "forward_fill:3"

  use std.commerce.pricing.bucket
    join on (item_id = item_id, location_id = location_id, channel_id = channel_id, bucket_date = bucket_date)
    fields [price_effective, discount_depth]
    missing = "null"

  use std.commerce.marketing.bucket
    join on (location_id = location_id, channel_id = channel_id, bucket_date = bucket_date)
    fields [spend, impressions]
    missing = "zero"
}
```

Missing policies: `reject | zero | null | forward_fill:N | back_fill:N | constant:<value>`

---

## 6) Feature block
```
features {
  lags [1, 7, 14, 28]
  rolling_mean [7, 14, 28]
  rolling_std [14, 28]
  onehot [day_of_week, month]
  interactions [ (is_payday * discount_depth), (is_holiday * spend) ]
  normalize { scale_numeric = "standard", clip_outliers = "p99" }
}
```

Required primitives:
- lag(k)
- rolling mean/std/min/max
- onehot
- scaling
- interaction terms
- optional: category hashing

---

## 7) Hierarchy block
```
hierarchy {
  lookback_days = 90
  censor_mode = "mask"
  min_valid_obs { 16: 60, 12: 40, 8: 20, 4: 10 }
  fallback = "parent_prefix"
}
```

Semantics:
- count valid obs in lookback
- choose deepest level meeting threshold
- fallback to parent prefix / global
- record `level_bits_used` and `cold_start_mode`

---

## 8) Model block
```
model {
  engine = "gbm_quantile"    // gbm_quantile|augurs_ets|augurs_mstl|augurs_prophet
  quantiles = [0.5, 0.8, 0.95]

  hyperparams {
    trees = 800
    depth = 8
    learning_rate = 0.05
  }
}
```

Engines:
- **gbm_quantile**: multivariate, native quantiles
- **augurs_ets**: univariate, needs calibration for quantiles
- **augurs_mstl**: univariate + seasonal
- **prophet**: optional

---

## 9) Calibration block
```
calibration {
  method = "residual_quantiles"   // none|residual_quantiles
  cohort = "parent_key"           // per_key|parent_key|global
  window_days = 90
}
```

---

## 10) Backtest block
```
backtest {
  window_days = 28
  metrics [pinball_loss, coverage, bias]
  exclude_censored = true
}
```

Outputs in `std.commerce.forecast.backtest_result`.

---

## 11) Emit block
```
emit {
  upsert std.commerce.forecast.demand_forecast_bucket
    subject = proj_id("forecast", run_id, keyspace_version, key_code, level_bits_used, location_id, channel_id, bucket_date)
    fields {
      p50 = p50
      p80 = p80
      p95 = p95
      model_id = model.engine
      model_version = model_version
      pipeline_version = pipeline.version
      keyspace_version = (embedding_version, quantizer_version)
      level_bits_used = level_bits_used
      cold_start_mode = cold_start_mode
      training_window = (start, end)
      valid_obs = valid_obs
      censored_obs = censored_obs
      computed_at = context.timestamp
      run_id = run_id
    }
}
```

---

## 12) Runtime model
- Compilation emits `.forecast_pipeline` plan
- Runs are **batch** (not request-time)
- Model artifacts stored with hash + lineage
- Serving only reads projections

---

## 13) Determinism requirements
- outputs reproducible from pipeline + keyspace + data + model artifact
- no implicit “latest”
- seeded randomness stored in run

---

## 14) Query surface (internal)
- `GetForecast(key, location, channel, date_range)`
- `GetForecastForItem(item_snapshot_id, ...)`
- `ExplainForecast(run_id, key, bucket_date)`
- `ForecastVsActual(key, date_range)`
- `CoverageReport(pipeline_version, date_range)`

---

## 15) Minimal v1 build plan
1. Forecast DSL parsing + compiled plan
2. observed_demand_bucket projection
3. calendar.bucket (holiday + payday)
4. gbm_quantile engine backend (external allowed)
5. hierarchy fallback
6. demand_forecast_bucket emission
7. backtest + coverage reports
8. weather covariate later
