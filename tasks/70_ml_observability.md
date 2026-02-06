# Task 70: ML Pipeline Observability

## Goal
Provide monitoring, alerts, and lineage visibility for keyspace, forecast, and reco pipelines.

## Scope
- Metrics: key population entropy, forecast coverage, reco CTR/ATC, model staleness.
- Alerts: pipeline failures, missing data sources, drift thresholds.
- Lineage dashboard: pipeline version, keyspace version, model artifact hash.

## Out of Scope
- Model training improvements.
- UI polish beyond minimal dashboards.

## Test Plan
- Synthetic drift injection triggers alerts.
- Metrics completeness on sample runs.

## Acceptance Criteria
- Metrics and alerts are emitted for all pipeline runs.
- Lineage view is queryable by run_id and model_version.
