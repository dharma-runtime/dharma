# Task 51: DHARMA-Q Group-By + Aggregations

## Goal
Add GROUP BY and aggregate functions to DHARMA-Q query execution.

## Scope
- Aggregates: count, sum, min, max, avg
- Group-by on a single column initially (extendable to multiple columns)
- Support numeric + symbol/text group keys
- Work with contract state tables and contract assertion tables

## Requirements
- New query pipeline operators:
  - `group by <col>`
  - `aggregate <exprs>` (e.g. `count(*)`, `sum(amount)`, `min(cost)`, `max(cost)`, `avg(cost)`)
- Output rows containing group keys + aggregate columns
- Deterministic ordering for stable results

## Implementation Notes
- Extend query parser to recognize `group by` + `aggregate` segments.
- Add aggregation execution path in DHARMA-Q engine:
  - scan filtered row set
  - hash aggregate (in-memory), with spill strategy TBD
- Extend schema/type handling for aggregate outputs.

## Success Criteria
- Queries return correct grouped aggregates on a known dataset.
- Works on both `@v<lens>` state tables and `.assertions` tables.
- Reasonable performance on 100M-row benchmark (target in Task 53).
